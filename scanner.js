/*jslint  node:true */
'use strict';

var express = require('express');
var router = express.Router();
var log = require('winston');
var needle = require('needle-retry');
var util = require('util');
var _ = require('lodash');
var redis = require('./lib/redis.js');
var moment = require('moment');
var crypto = require('crypto');
var async = require('async');

var TMI_URL = 'http://tmi.twitch.tv/group/user/%s/chatters';

var client = redis.connect();
client.on("error", function (err) {
    log.error("Redis error", err);
});

var options = {
    needle: {
        compressed: true,
        json: true
    },
    retry: {
        retries: 5
    }
};

var hash = function (text) {
    return crypto.createHash('md5').update(text).digest('base64').substr(0, 10);
};

var getViewers = function (name, cb) {
    needle.get(util.format(TMI_URL, name), options, function (err, response) {
        if (err || response.statusCode !== 200) {
            return cb(err || response.body, null);
        }

        if (!response.data.chatters) {
            return cb("Invalid server response: " + response.body, null);
        }

        cb(null, _.union(
            response.data.chatters.moderators,
            response.data.chatters.admins,
            response.data.chatters.staff,
            response.data.chatters.viewers
        ));
    });
};

var processChannel = function (channel, viewers, res) {
    var gamehash = channel.game ? hash(channel.game) : '',
        date = moment().tz('America/Los_Angeles').format('YYYYMMDD'),
        timestamp = moment().tz('America/Los_Angeles').unix();

    async.parallel([
        function (callback) {
            client.hset('games', gamehash, channel.game, callback);
        },
        function (callback) {
            //logger.info(channel.name, channel.game);
            var script = "local L = redis.call('GET', 'heartbeat:' .. KEYS[2]); local D = 300; " +
                "if L then D = (KEYS[4] - L) end; " +
                "for i=5, #KEYS do " +
                "redis.call('HINCRBY', 's:' .. KEYS[1] .. ':' .. KEYS[i], KEYS[2] , D); " +
                "redis.call('EXPIRE',  's:' .. KEYS[1] .. ':' .. KEYS[i], 172800); " +
                "redis.call('HINCRBY', 'g:' .. KEYS[1] .. ':' .. KEYS[i], KEYS[3] , D); " +
                "redis.call('EXPIRE',  'g:' .. KEYS[1] .. ':' .. KEYS[i], 172800); " +
                "end; redis.call('SETEX', 'heartbeat:' .. KEYS[2], 660, KEYS[4]); ",
                args = _.flatten([script, viewers.length + 4, date, channel.name, gamehash, timestamp, viewers]);
            client.eval(args, callback);
        }

    ], function (err, results) {
        if (err) {
            log.error(err, results);
            return res.status(500).jsonp({error: err});
        }
        return res.status(204).send({});
    });
};


router.post('/channel/:channel', function (req, res) {
    var channel = req.body;

    getViewers(channel.name, options, function (err, viewers) {
        if (err) {
            log.error(err);
            return res.status(502).jsonp({error: err});
        }

        processChannel(channel, viewers, res);

    });
});


module.exports = router;
