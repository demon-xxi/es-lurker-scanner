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
var _ = require('lodash');
var azure = require('azure-storage');
var LZUTF8 = require('./../lib/lzutf8');
var redis = require('./../lib/redis');
var periods = require('./../lib/period');
var moment = require('moment');
var async = require('async');
var datautil = require('./../lib/datautil');


var games = null;
var getGames = function (callback) {

    if (games) {
        return callback(null, games);
    }

    var client = redis.connect();
    client.on("error", function (err) {
        log.error("Redis error", err);
        callback(err);
    });

    client.hgetall('games', function (err, result) {
        client.quit(); //async
        games = result;
        callback(err, games);
    });
};

var getCacheStats = function (username, dates, callback, params) {
    var client = redis.connect();
    client.on("error", function (err) {
        log.error("Redis error", err);
        callback(err);
    });

    var results = {};

    async.each(dates, function (date, dcd) {

        var key = "U:" + (datautil.viewershash(username) % 100) + ':'
            + date + ':' + datautil.viewershash36(username);

        var pattern = username + ':*';
        var seed = 0;
        async.doWhilst(function (cb) {
                client.hscan([key, seed, 'match', pattern], function (err, data) {
                    if (err) {
                        return cb(err);
                    }
                    seed = parseInt(data[0], 10);
                    if (data[1].length) {

                        log.info(data[1]);

                        var i = 0;
                        for (; i < data[1].length - 1; i += 2) {
                            var rowkey = data[1][i];
                            results[rowkey] = results[rowkey] || 0;
                            results[rowkey] += parseInt(data[1][i + 1]);
                        }

                            //var obj = datautil.parseStatsKey(rowkey);
                            //obj.game = params.games[obj.game] || obj.game;
                            //
                            //if (!results[rowkey]){
                            //    results[rowkey] = obj;
                            //} else {}
                        //}
                        
                    }
            
                    cb()
                });

            },
            function () {
                return seed !== 0;
            }, function (err) {
                dcd(err)
            }
        );


    }, function (err) {
        client.quit(); //async

        results = _.transform(results, function (result, value, key) {
            var obj = datautil.parseStatsKey(key);
            result.push({
                channel: obj.channel,
                game: params.games[obj.game] || obj.game,
                duration: value});
        }, []);

        results = _.reverse(_.sortBy(results, 'duration'));
        
        log.info(JSON.stringify(results));

        callback(err, results);
    });

};

var getArchiveStats = function (username, from, to, callback) {
    callback(null, username);
};

var mergeStats = function (callback, stats) {
    callback(null, stats.cache/* + stats.archive*/);
};

router.get('/user/:username/stats/:period*?', function (req, res) {

    var period = periods.parse(req.params.period);
    var cacheDates = periods.getCachedDates(period);
    var tableRange = periods.getArchiveDates(period);

    var username = req.params.username;
    if (!username) {
        return res.status(400).send('Username is required specified').end();
    }

    async.auto({
        games: getGames,
        cache: ['games', async.apply(getCacheStats, username, cacheDates)],
        archive: async.apply(getArchiveStats, username, tableRange.from, tableRange.to),
        merge: ['cache', 'archive', mergeStats]
    }, function (err, results) {
        if (err) {
            return res.status(500).jsonp({error: err});
        }
        res.jsonp({
            user: username,
            from: periods.getPeriodStart(period),
            to: periods.getPeriodEnd(period),
            stats: results.merge
        });
    });

});


module.exports = router;