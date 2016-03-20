// increase max sockets
require('http').globalAgent.maxSockets = 1000;

var needle = require('./../../../../node_modules/needle-retry');
var log = require('./../../../../node_modules/winston');
var async = require('./../../../../node_modules/async');
var util = require('util');

var PAGE = 100;
var TWITCH_PARAL = 50;
var API_PARAL = 100;
var TWITCH_URL = 'https://api.twitch.tv/kraken/streams?&offset=%d&limit=%d';

var API_URL = 'http://' + (process.env.WEBSITE_HOSTNAME || 'localhost:3000') + '/scan/channel/';


var options = {
    needle:{
        compressed: true,
        json: true
    },
    retry: {
        retries: 5
    }
};


var worker = function (stream, callback) {

    var data = {
        id: stream.channel._id,
        name: stream.channel.name,
        viewers: stream.viewers,
        game: stream.channel.game
    };

    needle.post(API_URL + data.name, data, options, function(err, response){
        if (err || response.statusCode >= 300){
            log.error("Error posting channel", data.name, err);
            callback(err || response.body);
            return;
        }

        log.info({id: stream.channel._id, name: stream.channel.name,
            viewers: stream.viewers , game: stream.game});

        callback();
    });

};

console.time("read");
console.time("write");
var queue = async.priorityQueue(worker, API_PARAL);

var enqueueStream = function(stream){
    if (!stream || !stream.channel) return;
    queue.push(stream, stream.viewers);
};

var getStreams = function (url, callback) {
    needle.get(url, options, function(err, response){
        if (err || response.statusCode != 200){
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
needle.get(util.format(TWITCH_URL, 1000000, 1), options, function(err, response) {
    if (err || response.statusCode != 200){
        log.error("Error getting streams count.", err);
        return;
    }

    var total = parseInt(response.body._total);
    var urls = [];


    for (var offset=0; offset < total; offset += PAGE){
        var url = util.format(TWITCH_URL, offset, PAGE);
        urls.push(url);
    }

    async.eachLimit(urls, TWITCH_PARAL, getStreams, function(err){
        console.timeEnd("read");

        if (err){
            log.error(err);
        }

        // setting here to ensure it is called once
        queue.drain = function(){
            console.timeEnd("write");
        };

    });
    //getStreams(url);

});
