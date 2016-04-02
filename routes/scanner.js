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
var needle = require('needle-retry');
var util = require('util');
var _ = require('lodash');
var redis = require('./../lib/redis');
var datautil = require('./../lib/datautil');
var gatekeeper = require('./../lib/gatekeeper');
var moment = require('moment');
var async = require('async');

var TMI_URL = 'http://tmi.twitch.tv/group/user/%s/chatters';
var SCRIPT_SHA1 = null;

require('moment-timezone');

var client = redis.connect();
client.on("error", function (err) {
    log.error("Redis error", err);
});

client.on("ready", function () {

    var script = "local L = redis.call('GET', 'heartbeat:' .. KEYS[2]); local D = 300; " +
        "if L then D = (KEYS[4] - L) end; " +
        "local H =  (#KEYS-4)/2;" +
        "local T =  #KEYS-H;" +
        "for i=5, T do " +
        "redis.call('HINCRBY', 'U:' .. (tonumber(KEYS[i+H], 36)%1000) .. ':' .. KEYS[1] .. ':' .. KEYS[i+H], KEYS[i] .. ':' .. KEYS[2] .. ':' .. KEYS[3] , D); " +
        "end; redis.call('SETEX', 'heartbeat:' .. KEYS[2], 660, KEYS[4]); ";

    client.script('load', script, function (err, sha) {
        log.error(err);
        SCRIPT_SHA1 = sha;
    });

});


var options = {
    needle: {
        compressed: true,
        json: true
    },
    retry: {
        retries: 10
    }
};


var getViewers = function (name, cb) {
    needle.get(util.format(TMI_URL, name), options, function (err, response) {
        if (err || response.statusCode !== 200) {
            return cb(err || response.body, null);
        }

        if (!response.body.chatters) {
            return cb("Invalid server response: " + response.body, null);
        }

        cb(null, _.union(
            response.body.chatters.moderators,
            response.body.chatters.admins,
            response.body.chatters.staff,
            response.body.chatters.viewers
        ));
    });
};

var processChannel = function (channel, viewers, res) {
    var gamehash = datautil.gamehash36(channel.game),
        date = moment().tz('America/Los_Angeles').format('YYYYMMDD'),
        timestamp = moment().tz('America/Los_Angeles').unix(),
        viewershash = _.map(viewers, datautil.viewershash36);

    async.parallel([
        function (callback) {
            client.hset('games', gamehash, channel.game || "Unknown", callback);
        },
        function (callback) {
            var args = _.flatten([SCRIPT_SHA1, viewers.length * 2 + 4,
                date, channel.name, gamehash, timestamp, viewers, viewershash]);
            client.evalsha(args, function (err, data) {
                if (err) {
                    log.error(err);
                }
                callback(err, data);
            });
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

    if (!gatekeeper.allow(req)) {
        return res.status(401).jsonp({error: 'Access Denied.'});
    }

    var channel = req.body;

    getViewers(channel.name, function (err, viewers) {
        if (err) {
            log.error(err);
            return res.status(502).jsonp({error: err});
        }

        processChannel(channel, viewers, res);

    });
});


module.exports = router;
