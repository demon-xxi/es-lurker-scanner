/*jslint  node:true */
'use strict';

var redis = require('redis');
var REDIS_PORT = process.env.REDIS_PORT || 6379;
var REDIS_SERVER = process.env.REDIS_SERVER || '127.0.0.1';
var REDIS_KEY = process.env.REDIS_KEY || '';

module.exports = {
    connect: function () {
        var options = !REDIS_KEY || REDIS_KEY.length === 0 ? null : {auth_pass: REDIS_KEY};
        return redis.createClient(REDIS_PORT, REDIS_SERVER, options);
    }
};
