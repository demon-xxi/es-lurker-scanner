/*jslint  node:true */
//'use strict';

var redis = require('redis');
var async = require('async');
var REDIS_PORT = process.env.REDIS_PORT || process.env.APPSETTING_REDIS_PORT || 6379;
var REDIS_SERVER = process.env.REDIS_SERVER || process.env.APPSETTING_REDIS_SERVER || '127.0.0.1';
var REDIS_KEY = process.env.REDIS_KEY || process.env.APPSETTING_REDIS_KEY || '';

module.exports = {
    connect: function () {
        var options = !REDIS_KEY || REDIS_KEY.length === 0 ? null : {auth_pass: REDIS_KEY};
        var client = redis.createClient(REDIS_PORT, REDIS_SERVER, options);
        client.multi_scan = function(pattern, itemcb, completecb){
            var client = this;
            var seed = 0;
            async.doWhilst(
                function (cb) {
                    client.scan([seed, 'match', pattern], function (err, data) {
                        seed = data[0];
                        if (err){
                            completecb(err);
                        } else {
                            async.each(data[1], itemcb, cb);
                        }
                    });

                },
                function () {
                    return seed !== 0;
                },

                function (err) {
                    completecb(err);
                }
            );

        };
        return client;
    }
};
