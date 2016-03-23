#!/usr/bin/env node

/*jslint  node:true */
'use strict';

var express = require('express');
var morgan = require('morgan');
var winston = require('winston');
var logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({'timestamp':true})
    ]
});
var bodyParser = require('body-parser');

// increase max sockets
require('http').globalAgent.maxSockets = 1000;

var app = express();

// use winston stream for express logger middleware
var winstonStream = {
    write: function (message) {
        logger.info(message.slice(0, -1));
    }
};

var logformat = (process.env.NODE_ENV == 'production') ? 'short' : 'dev';
app.use(morgan(logformat, {stream: winstonStream}));

app.use(bodyParser.json());

// routes
app.use('/scan', require('./scanner'));
app.use('/aggregate', require('./aggregator'));

app.set('port', process.env.PORT || 3000);

var server = app.listen(app.get('port'), function () {
    console.log('Express server listening on port ' + app.get('port'));
});
