/*jslint  node:true */
'use strict';

// increase max sockets
require('http').globalAgent.maxSockets = 100;

var needle = require('./../../../../node_modules/needle-retry');
var winston = require('./../../../../node_modules/winston');
var _ = require('./../../../../node_modules/lodash');
var log = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({'timestamp': true})
    ]
});
var gatekeeper = require('./../../../../lib/gatekeeper');
var async = require('./../../../../node_modules/async');
var util = require('util');

var PAGE = 100;
var TWITCH_PARAL = 50;
var API_PARAL = 50;
var MIN_VIEWERS = 30;

var TWITCH_URL = 'https://api.twitch.tv/kraken/streams?&offset=%d&limit=%d';
var API_URL = 'http://' + (process.env.WEBSITE_HOSTNAME || 'localhost:3000') + '/scan/channel/';


var options = {
    needle: {
        compressed: true,
        json: true,
        open_timeout: 60000,
        read_timeout: 60000
    },
    retry: {
        retries: 5
    }
};

var apiOptions = _.merge(options, {needle: {headers: _.zipObject([gatekeeper.header], [gatekeeper.passcode])}});

var total = 0;
var errors = 0;

var worker = function (data, callback) {
    needle.post(API_URL + data.name, data, apiOptions, function (err, response) {
        if (err || response.statusCode >= 300) {
            log.error("Error posting channel", data.name, err);
            callback(err || response.body);
            errors += 1;
            return;
        }

        total += 1;
        log.info(data);
        callback();
    });

};

console.time("read");
console.time("write");
var queue = async.priorityQueue(worker, API_PARAL);

var enqueueStream = function (stream) {
    if (!stream || !stream.channel || parseInt(stream.viewers, 10) < MIN_VIEWERS) {
        return;
    }

    var data = {
        id: stream.channel._id,
        name: stream.channel.name,
        viewers: parseInt(stream.viewers, 10),
        game: stream.channel.game
    };

    queue.push(data, 1000000000 - stream.viewers);
};

var getStreams = function (url, callback) {
    needle.get(url, options, function (err, response) {
        if (err || response.statusCode !== 200) {
            log.error("Error getting streams list.", url, err);
            callback(err || response.body);
            return;
        }

        log.info(url);
        response.body.streams.forEach(enqueueStream);
        callback();
    });
};


// get a total number of streams
needle.get(util.format(TWITCH_URL, 1000000, 1), options, function (err, response) {
    if (err || response.statusCode != 200) {
        log.error("Error getting streams count.", err);
        return;
    }

    var total = parseInt(response.body._total);
    var urls = [];


    for (var offset = 0; offset < total; offset += PAGE) {
        var url = util.format(TWITCH_URL, offset, PAGE);
        urls.push(url);
    }

    async.eachLimit(urls, TWITCH_PARAL, getStreams, function (err) {
        console.timeEnd("read");

        if (err) {
            log.error(err);
        }

        // setting here to ensure it is called once
        queue.drain = function () {
            log.info("Done!", {success: total, errors: errors});
            console.timeEnd("write");
        };

    });
    //getStreams(url);

});
