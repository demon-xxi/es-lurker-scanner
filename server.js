#!/usr/bin/env node
var express = require('express');
var morgan = require('morgan');
var winston = require('winston');
var bodyParser = require('body-parser');

// increase max sockets
require('http').globalAgent.maxSockets = 1000;

var app = express();

// use winston stream for express logger middleware
var winstonStream = {
    write: function (message) {
        winston.info(message.slice(0, -1));
    }
};

var logformat = (process.env.NODE_ENV == 'production') ? 'short' : 'dev';
app.use(morgan(logformat, {stream: winstonStream}));

app.use(bodyParser.json());

// routes
app.use('/scan', require('./scanner'));

app.set('port', process.env.PORT || 3000);

var server = app.listen(app.get('port'), function() {
    console.log('Express server listening on port ' + app.get('port')   );
});
