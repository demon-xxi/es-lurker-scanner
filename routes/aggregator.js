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
var redis = require('./../lib/redis.js');
var gatekeeper = require('./../lib/gatekeeper');
var storage = require('./../lib/azure-storage');
var async = require('async');
var LZString = require('lz-string');
var datautil = require('./../lib/datautil');

var READ_CONCURRENCY = 10;

var TABLE_BATCH_SIZE = 50;

require('moment-timezone');


var tableSvc = storage.tableService();

var createTable = function (callback) {
    tableSvc.createTable(storage.viewerSummaryTable, {ignoreIfExists: true}, function (error) {
        callback(error);
    });
};

var connectRedis = function (callback) {
    var client = redis.connect();
    client.on("error", function (err) {
        callback(err, client);
    });
    client.on("ready", function () {
        callback(null, client);
    });
};

var getGames = function (redisClient, callback) {
    redisClient.hgetall('games', function (err, games) {
        callback(err, redisClient, games);
    });
};


var processGroup = function (task, callback) {

    console.time('hgetall>' + task.key);
    console.time('processGroup > ' + task.key);
    task.redisClient.hgetall(task.key, function (err, pairs) {
        console.timeEnd('hgetall>' + task.key);
        if (err) {
            return callback(err);
        }

        var //entGen = azure.TableUtilities.entityGenerator,
            keyParts = task.key.split(':'), // U:hash:day:bucket
            day = datautil.reverseDay(parseInt(keyParts[2], 10)),
        //partitionKey = util.format("%d:%s", day, numFmt(keyParts[3], MASK));
            partitionKey = keyParts[3];

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


        var cargo = async.cargo(function (jobs, cargocb) {

            var batch = tableSvc.startBatch();

            _.each(jobs, function (job) {
                batch.insertOrReplaceEntity(storage.viewerSummaryTable, job);
            });

            batch.commit(function (err, data) {
                if (!err) {
                    task.stats.batches += 1;
                    task.stats.records += jobs.length;
                } else {
                    log.error(err);
                    task.errors += 1;
                }
                cargocb(null, data);
            });

        }, TABLE_BATCH_SIZE);

        var complete = function (err) {
            console.timeEnd('executeBatch > ' + task.key);
            console.timeEnd('processGroup > ' + task.key);
            if (err) {
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
        };

        var done = false;
        cargo.drain = function () {
            if (done) {
                complete(null);
            }
        };

        console.time('executeBatch > ' + task.key);
        console.time('LZString > ' + task.key);
        async.each(_.toPairs(totals), function (item, mapcb) {
            var viewer = item[0];
            var summary = item[1];
            var str = JSON.stringify(summary.views);
            var compressed = LZString.compress(str);
            if (compressed.length > 32768) {
                log.warn("Record exceeds 64KB limit", {
                    key: task.key,
                    viewer: viewer
                    //stats: summary.views
                });
            } else {
                cargo.push({
                    PartitionKey: partitionKey,
                    RowKey: viewer + ':' + day,
                    Views: compressed,
                    Total: parseInt(summary.total, 10)
                });
            }
            mapcb(null, item);
        }, function (err) {
            console.timeEnd('LZString > ' + task.key);
            if (err) {
                return complete(err);
            }

            if (cargo.length() === 0) {
                complete(null);
            } else {
                done = true;
            }


        });


    });

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
        var done = false;
        var queue = async.queue(processGroup, READ_CONCURRENCY);
        queue.drain = function () {
            if (done) {
                callback(null, redisClient, stats);
            }
        };

        redisClient.keys(keys_pattern, function (err, keys) {
            if (err) {
                queue.kill();
                return callback(err, redisClient, stats);
            }

            _.forEach(keys, function (key) {
                queue.push({
                    stats: stats,
                    key: key,
                    games: games,
                    redisClient: redisClient
                });
            });

            if (queue.length() === 0) {
                return callback(null, redisClient, stats);
            }
            done = true;

        });

    };
};


router.get('/viewers/:date/:batch', function (req, res) {

    if (!gatekeeper.allow(req)) {
        return res.status(401).jsonp({error: 'Access Denied.'});
    }

    if (!req.params.batch) {
        return res.status(400).send({error: "batch parameter is missing"});
    }

    var batch = parseInt(req.params.batch, 10),
        date = req.params.date;

    async.waterfall(
        [createTable,
            connectRedis,
            getGames,
            getKeys(date, batch)],
        function (err, redisClient, stats) {

            //redisClient.quit(function (err) {
            //    callback(err, stats);
            //});
            if (redisClient) {
                redisClient.quit();
            }

            if (err) {
                log.error(err);
                return res.status(502).jsonp({error: err});
            }

            stats.time = new Date().getTime() - stats.time;
            return res.status(200).send(stats);
        }
    );

});


module.exports = router;
