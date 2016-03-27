/*jslint  node:true */
'use strict';

// increase max sockets
require('http').globalAgent.maxSockets = 1000;

var needle = require('./../../../../node_modules/needle-retry');
var winston = require('./../../../../node_modules/winston');
var log = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({'timestamp': true})
    ]
});
var async = require('./../../../../node_modules/async');
var moment = require('./../../../../node_modules/moment');
var _ = require('./../../../../node_modules/lodash');
require('./../../../../node_modules/moment-timezone');
var gatekeeper = require('./../../../../lib/gatekeeper');

var API_PARAL = 2;

var date = moment().subtract(1, 'days').tz('America/Los_Angeles').format('YYYYMMDD');
var API_URL = 'http://' + (process.env.WEBSITE_HOSTNAME || 'localhost:3000') + '/aggregate/viewers/' + date + '/';

var options = {
    needle: {
        open_timeout: 3000000,
        read_timeout: 3000000,
        compressed: true,
        json: true,
        headers: _.zipObject([gatekeeper.header],[gatekeeper.passcode])
    },
    retry: {
        retries: 2
    }
};



var batchs = _.range(100);
var total = {
    batches: 0,
    time: new Date().getTime(),
    records: 0,
    errors: 0
};
async.eachLimit(batchs, API_PARAL, function (batch, callback) {
    needle.get(API_URL + batch, options, function (err, response, body) {
        if (err || response.statusCode >= 300) {
            log.error("Error aggregating batch", batch, err, !!body ? body : (!!response ? response.body : ""));
        } else {
            var stats = response.body;
            log.info("Batch stats", batch, stats);
            total.batches += stats.batches;
            //total.time += stats.time;
            total.records += stats.records;
            total.errors += stats.errors;
        }
        callback();
    });
}, function (err) {
    total.time = new Date().getTime() - total.time;
    log.info("Done!", total, err);
});