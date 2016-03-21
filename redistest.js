/*jslint  node:true */
'use strict';

var redis = require('redis');
var client = require('./lib/redis.js').connect();
var log = require('winston');
var async = require('async');
var _ = require('lodash');
var murmurhash = require('murmurhash');
var Buffer = require('buffer').Buffer;

client.on("error", function (err) {
    log.error("Redis error", err);
});

console.time("write");
var list = "testlist";

var TOTAL = 5000000;
var ids = _.range(TOTAL);

var w = function (cb) {
    return function (val) {
        cb(null, val);
    };
};

var BUCKET = 3000000 / 100;

async.eachLimit(ids, 1000, function (i, callback) {
    var username = "user" + i;
    var key = Math.floor((murmurhash.v3(username) % TOTAL) % BUCKET);
    client.hset(key, username, 1, w(callback));
}, function(err){
    if (err) log.info(err);
    console.timeEnd("write");
});

//var m = client.multi();

//var MAX_INT32 = Math.pow(2, 31) - 1;
//var BUCKET = 3000000 / 100;
//for (var i = 0; i < TOTAL; i++) {
//    // client.sadd(list, "user"+i );
//    var username = "user" + i;
//    var id = (murmurhash.v3(username) % TOTAL) % BUCKET;
//    var key = Math.floor(id);
//    // log.info(username, key);
//    //const buf = new Buffer(4);
//    //buf.writeInt32LE(key,0);
//    //m.hset(buf, buf, 1);
//    client.hset(key, username, 1);
//}

//m.exec(function (msg) {
//    log.info("Done!");
//    log.info(msg);
//});

//console.timeEnd("write");