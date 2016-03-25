/*jslint  node:true */
'use strict';

var express = require('express');
var router = express.Router();
var winston = require('winston');
var log = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({'timestamp': true})
    ]
});
var util = require('util');
var _ = require('lodash');
var redis = require('./lib/redis.js');
var storage = require('./lib/azure-storage.js');
var async = require('async');
var azure = require('azure-storage');
var LZUTF8 = require('./lib/lzutf8');

//var murmurhash = require('murmurhash');

var CONCURRENCY = 10;

require('moment-timezone');


//var SEED = 1234567;
//var TOTAL = 3000000;
//var BUCKET = TOTAL / 100;

var MAX_DAY = 99991231;
var MASK = '000000';


var tableSvc = storage.tableService();

var createTable = function (callback) {
    tableSvc.createTableIfNotExists(storage.viewerSummaryTable, function (error, result, response) {
        callback(error);
    });
};

var connectRedis = function (callback) {
    var client = redis.connect();
    client.on("error", function (err) {
        callback(err);
    });
    client.on("ready", function () {
        callback(null, client);
    });
};

//var connectTable = function (callback) {
//    storage.tableService();
//};

var getGames = function (redisClient, callback) {
    redisClient.hgetall('games', function (err, games) {
        callback(err, redisClient, games);
    });
};

var numFmt = function (num, mask) {
    return (mask + num).slice(-Math.max(mask.length, (num + '').length));
};

var processGroup = function (task, callback) {

    task.redisClient.hgetall(task.key, function (err, pairs) {
        if (err) {
            return callback(err);
        }

        var entGen = azure.TableUtilities.entityGenerator,
            keyParts = task.key.split(':'),
            day = MAX_DAY - parseInt(keyParts[2], 10),
            partitionKey = entGen.String(util.format("%d:%d", day, numFmt(keyParts[3], MASK)));

        var totals = _.reduce(pairs, function (result, dur, key) {
            var parts = key.split(':'),
                channel = parts[1],
                game = task.games[parts[2]] || parts[2],
                viewer = parts[0],
                duration = parseInt(dur, 10);

            result[viewer] = result[viewer] || {views: [], total: 0};
            result[viewer].total = (result[viewer].total || 0) + duration;
            result[viewer].views.push({game: game, channel: channel, duration: duration});

            return result;
        }, {});

        log.info('Processing Group', task.key, _.size(totals));

        async.reduce(_.toPairs(totals), {
            batches: [],
            batch: new storage.TableBatch(),
            count: 0
        }, function (result, item, reducecallback) {
            var viewer = item[0];
            var summary = item[1];

            LZUTF8.compressAsync(JSON.stringify(summary.views), {outputEncoding: "BinaryString"}, function (compressed, error) {
                if (error) {
                    return reducecallback(err);
                }


                // check for 64KB field limit. Skip large rows for now
                if (compressed.length > 65536) {
                    log.warn("Record exceeds 64KB limit", {
                        key: task.key,
                        viewer: viewer,
                        stats: summary.views
                    });
                    return result;
                }

                if (result.batch.size() < 100) {
                    result.count += 1;
                    result.batch.insertOrReplaceEntity({
                        PartitionKey: partitionKey,
                        RowKey: entGen.String(viewer),
                        Views: entGen.String(compressed),
                        Total: entGen.Int32(summary.total)
                    }, {
                        echoContent: false
                    });
                } else {
                    result.batches.push(result.batch);
                    result.batch = new storage.TableBatch();
                    result.count = 0;
                }

                reducecallback(null, result);

            });

        }, function (err, result) {

            if (err){
                return callback(err);
            }

            if (result.count > 0) {
                result.batches.push(result.batch);
            }


            async.eachLimit(result.batches, CONCURRENCY, function (batch, cb) {
                async.retry(3, function (cb1) {
                    tableSvc.executeBatch(storage.viewerSummaryTable, batch, cb1);
                }, function (err) {
                    if (!err) {
                        task.stats.batches += 1;
                        task.stats.records += batch.size();
                    } else {
                        log.error(err);
                        task.errors += 1;
                    }
                    cb(null);
                });
            }, function (err) {

                if (err){
                    return callback(err);
                }

                if (task.errors > 0) {
                    return callback(err);
                }

                // remove from cache
                task.redisClient.del(task.key, function (err) {
                    log.info('Removed key: ', task.key);
                    callback(err);
                });

            });

        });



    });

    //callback(null);
    //setTimeout(callback, 1000);
};

var getKeys = function (date, batch) {
    var keys_pattern = util.format("U:%d:%s:*", batch, date);
    return function (redisClient, games, callback) {
        var stats = {
            batches: 0,
            time: new Date().getTime(),
            records: 0,
            errors: 0
        };
        var seed = 0;
        var done = false;
        var queue = async.queue(processGroup, CONCURRENCY);
        queue.drain = function () {
            if (done) {
                callback(null, redisClient, stats);
            }
        };
        async.doWhilst(
            function (cb) {
                redisClient.scan([seed, 'match', keys_pattern], function (err, data) {
                    if (!err) {
                        seed = parseInt(data[0], 10);
                        _.forEach(data[1], function (key) {
                            queue.push({
                                stats: stats,
                                key: key,
                                games: games,
                                redisClient: redisClient
                            });
                        });
                    }
                    cb(err);
                });

            },
            function () {
                return seed !== 0;
            },

            function (err) {
                if (err) {
                    queue.kill();
                    return callback(err, redisClient, stats);
                }
                if (queue.length() === 0) {
                    return callback(null, redisClient, stats);
                }
                done = true;
            }
        );

    };
};

var cleanup = function (redisClient, stats, callback) {
    stats.time = new Date().getTime() - stats.time;
    redisClient.quit(function (err) {
        callback(err, stats);
    });
};

router.get('/viewers/:date/:batch', function (req, res) {

    if (!req.params.batch) {
        return res.status(400).send({error: "batch parameter is missing"});
    }

    var batch = parseInt(req.params.batch, 10),
        date = req.params.date;

    async.waterfall(
        [createTable,
            connectRedis,
            getGames,
            getKeys(date, batch),
            cleanup],
        function (err, stats) {
            if (err) {
                log.error(err);
                return res.status(502).jsonp({error: err});
            }
            return res.status(200).send(stats);
        }
    );

});


module.exports = router;
