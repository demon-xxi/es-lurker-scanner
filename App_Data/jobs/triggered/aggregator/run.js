/*jslint  node:true */
'use strict';

// increase max sockets
require('http').globalAgent.maxSockets = 1000;

var needle = require('./../../../../node_modules/needle-retry');
var log = require('./../../../../node_modules/winston');
var async = require('./../../../../node_modules/async');
var moment = require('./../../../../node_modules/moment');
var _ = require('./../../../../node_modules/lodash');
require('./../../../../node_modules/moment-timezone');

var API_PARAL = 3;

var date = moment().subtract(1, 'days').tz('America/Los_Angeles').format('YYYYMMDD');
var API_URL = 'http://' + (process.env.WEBSITE_HOSTNAME || 'localhost:3000') + '/aggregate/viewers/'+date + '/';

var options = {
    needle: {
        open_timeout: 300000,
        read_timeout: 120000,
        compressed: true,
        json: true
    },
    retry: {
        retries: 2
    }
};

var batchs = _.range(100);

async.eachLimit(batchs, API_PARAL, function (batch, callback) {
    needle.get(API_URL + batch, options, function (err, response) {
        if (err || response.statusCode >= 300) {
            log.error("Error aggregating batch", batch, err, !!response ? response.body : "");
        } else {
            log.info("Batch stats", batch, response.body);
        }
        callback();
    });
}, function (err) {
    log.info("Done!", err);
});