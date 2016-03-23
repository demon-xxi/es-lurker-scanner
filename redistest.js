/*jslint  node:true */
'use strict';

//var client = require('./lib/redis.js').connect();
var log = require('winston');
var async = require('async');
var _ = require('lodash');
var murmurhash = require('murmurhash');
var jsonpack = require('jsonpack');
var lzwCompress = require('lzwcompress');
var LZUTF8 = require('./lib/lzutf8');

//client.on("error", function (err) {
//    log.error("Redis error", err);
//});

console.time("write");

var TOTAL = 5000000;
var ids = _.range(TOTAL);

var w = function (cb) {
    return function (val) {
        cb(null, val);
    };
};

var json = [{"duration":88738,"channel":"pianoimproman"},{"duration":79174,"channel":"bobross"},{"duration":33162,"channel":"lirik"},{"duration":22267,"channel":"seriousgaming"},{"duration":4829,"channel":"leahloveschief"},{"duration":4706,"channel":"twitchoffice"},{"duration":2412,"channel":"summit1g"},{"duration":2412,"channel":"itmejp"},{"duration":2087,"channel":"krismpro"},{"duration":1805,"channel":"cohhcarnage"},{"duration":1489,"channel":"lustredust"},{"duration":589,"channel":"ltvictor"},{"duration":300,"channel":"chewiemelodies"},{"duration":292,"channel":"inetkoxtv"}];

var str = JSON.stringify(json);
var jsonc = jsonpack.pack(json);
var compressed = lzwCompress.pack(json);
var lzutf8 = LZUTF8.compress(str, {outputEncoding: "BinaryString"});

log.info(compressed, lzutf8, LZUTF8.decompress(lzutf8, {inputEncoding: "BinaryString"}));
log.info(str.length, jsonc.length, compressed.length, lzutf8.length);


//var BUCKET = 3000000 / 100;
//
//async.eachLimit(ids, 1000, function (i, callback) {
//    var username = "user" + parseInt(i).toString(32);
//    var key = Math.floor((murmurhash.v3(username) % TOTAL) % BUCKET).toString(32);
//    client.hset(key, username, 1, w(callback));
//}, function(err){
//    if (err) log.info(err);
//    console.timeEnd("write");
//    client.quit();
//});
//
//

