/*jslint  node:true */
'use strict';

var express = require('express');
var router = express.Router();
var log = require('winston');
var util = require('util');
var _ = require('lodash');
var redis = require('./lib/redis.js');
var async = require('async');
var murmurhash = require('murmurhash');

var CONCURRENCY = 10;

require('moment-timezone');


var SEED = 1234567;
var TOTAL = 3000000;
var BUCKET = TOTAL / 100;


var connectRedis = function (callback) {
    var client = redis.connect();
    client.on("error", function (err) {
        callback(err);
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

    task.redisClient.hgetall(task.key, function (err, pairs) {
        if (err) {
            return callback(err);
        }
        var totals =_.reduce(pairs, function (result, dur, key) {
            var parts = key.split(':');
            var channel = parts[0];
            var game = task.games[parts[1]] || parts[1];
            var viewer = parts[2];
            var duration = parseInt(dur);

            result[viewer] = result[viewer] || {games: {}, channels: {}};

            result[viewer].games[game] = (result[viewer].games[game] || 0) + duration;
            result[viewer].channels[channel] = (result[viewer].channels[channel] || 0) + duration;

            return result;
        }, {});

        log.info('Processing Group', task.key, totals);

    });

    //callback(null);
    //setTimeout(callback, 1000);
};

var getKeys = function (date, batch) {
    var keys_pattern = util.format("U:%d:%s:*", batch, date);
    return function (redisClient, games, callback) {

        var seed = 0;
        var done = false;
        var queue = async.queue(processGroup, CONCURRENCY);
        queue.drain = function () {
            if (done) {
                callback(null, {});
            }
        };
        async.doWhilst(
            function (cb) {
                redisClient.scan([seed, 'match', keys_pattern], function (err, data) {
                    if (!err) {
                        seed = parseInt(data[0]);
                        _.forEach(data[1], function (key) {
                            queue.push({
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
                    return callback(err);
                }
                if (queue.length() == 0) {
                    return callback(null, {});
                }
                done = true;
            }
        );

    };
};


router.get('/viewers/:date/:batch', function (req, res) {

    if (!req.params.batch) {
        return res.status(400).send({error: "batch parameter is missing"});
    }

    var batch = parseInt(req.params.batch);
    var date = req.params.date;

    async.waterfall(
        [connectRedis,
            getGames,
            getKeys(date, batch)],
        function (err, result) {
            if (err) {
                log.error(err);
                return res.status(502).jsonp({error: err});
            }
            return res.status(204).send({});
        }
    );

});


module.exports = router;
