/*jslint  node:true */
'use strict';

//var _ = require('lodash');
var murmurhash = require('murmurhash');

var SEED = 1234567;
var TOTAL = 3000000;
var BUCKET = TOTAL / 100;

var viewershash = function (username) {
    return Math.floor((murmurhash.v3(username, SEED) % TOTAL) % BUCKET);
};

var parseStatsKey = function (key) {
    var parts = key.split(':');
    return {
        viewer: parts[0],
        channel: parts[1],
        game: parts[2]
    }
};

module.exports = {

    viewershash: viewershash,

    viewershash36: function (username) {
        return viewershash(username).toString(36);
    },

    parseStatsKey: parseStatsKey

};