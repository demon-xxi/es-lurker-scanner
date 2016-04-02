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
var redis = require('./../lib/redis');
var periods = require('./../lib/period');
var async = require('async');
var datautil = require('./../lib/datautil');
var storage = require('./../lib/azure-storage');
var Query = require('azure-table-node').Query;
var LZString = require('lz-string');

var tableSvc = storage.tableService();

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

        var key = "U:" + (datautil.viewershash(username) % 1000) + ':'
            + date + ':' + datautil.viewershash36(username);

        var pattern = username + ':*';

        client.multi_hscan(key, pattern, function (key, value, cbi) {
            results[key] = (results[key] || 0) + parseInt(value, 10);
            cbi();
        }, function (err) {
            dcd(err);
        });

    }, function (err) {
        client.quit(); //async

        results = _.transform(results, function (result, value, key) {
            var obj = datautil.parseStatsKey(key);
            result.push({
                channel: obj.channel,
                game: params.games[obj.game] || obj.game,
                duration: value
            });
        }, []);

        results = _.reverse(_.sortBy(results, 'duration'));

        //log.info(JSON.stringify(results));

        callback(err, results);
    });

};

var getArchiveStats = function (username, from, to, callback) {

    //partitionKey = util.format("%d:%s", day, numFmt(keyParts[3], MASK));

    var query = Query.create()
        .where('PartitionKey', '==', datautil.viewershash36(username))
        .and('RowKey', '<=', username + ":" + datautil.reverseDay(from))
        .and('RowKey', '>=', username + ":" + datautil.reverseDay(to));

    tableSvc.queryEntities(storage.viewerSummaryTable, {query: query}, function (error, data) {
        if (error) {
            return callback(error);
        }
        async.map(data, function (itm, cb) {
            try {
                var decompressed = LZString.decompress(itm.Views);
                return cb(null, decompressed ? JSON.parse(decompressed) : []);
            } catch (err) {
                return cb(err);
            }
        }, function (err, results) {
            callback(err, results);
        });
    });


};

var mergeStats = function (callback, stats) {
    var all = _.flatten(_.concat((stats.cache || []), stats.archive));
    var grouped = _.groupBy(all, function (itm) {
        return itm.game + '|' + itm.channel;
    });
    var merged = _.map(grouped, function (p) {
        return {game: p[0].game, channel: p[0].channel, duration: _(p).map('duration').sum()};
    });

    //console.log('MERGED', merged);
    callback(null, merged);
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