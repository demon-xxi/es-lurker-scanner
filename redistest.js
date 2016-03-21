/*jslint  node:true */
'use strict';

var redis = require('redis');
var client = require('./lib/redis.js').connect();
var log = require('winston');
var murmurhash = require('murmurhash');

client.on("error", function (err) {
    log.error("Redis error", err);
});

console.time("write");
var list = "testlist";
for (var i = 0; i < 3000000; i++) {
    // client.sadd(list, "user"+i );
    var username = "user" + i;
    var id = i; //murmurhash.v3(username)
    var key = "b:"+ Math.floor(id/50);
    // log.info(username, key);
    client.hset(key, username, 1);
}
console.timeEnd("write");