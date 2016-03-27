#!/usr/bin/env node

/*jslint  node:true */
'use strict';

var http = require('http');
var express = require('express');
var morgan = require('morgan');
var winston = require('winston');
var logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({'timestamp': true})
    ]
});


// setting app insights
if (process.env.APPINSIGHTS_INSTRUMENTATIONKEY) {
    var appInsights = require("applicationinsights");
    appInsights.enableVerboseLogging().setup().start();
    logger.info("AppInsights automatic data collection enabled!");
}

var bodyParser = require('body-parser');

// increase max sockets
http.globalAgent.maxSockets = 1000;
http.globalAgent.keepAlive = true;
http.globalAgent.options.keepAlive = true;

// catch the uncaught errors that weren't wrapped in a domain or try catch statement
// do not use this in modules, but only in applications, as otherwise we could have multiple of these bound
process.on('uncaughtException', function (err) {
    // handle the error safely
    logger.error(err);
});

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


var ensureAuthenticated = function(req, res, next) {
    //if (req.header("Authorization") && req.header("Authorization").startsWith("Bearer")) {
        return next();
    //}
    //req.session.redirectUrl = req.url;
    //return res.send(401);
};


// routes
app.use('/scan', require('./routes/scanner'));
app.use('/aggregate', require('./routes/aggregator'));
app.use('/api', ensureAuthenticated, require('./routes/api'));

app.set('port', process.env.PORT || 3000);

var server = app.listen(app.get('port'), function () {
    console.log('Express server listening on port ' + app.get('port'));
});
