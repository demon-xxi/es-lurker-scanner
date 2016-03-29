/*jslint  node:true */
//'use strict';

var _ = require('lodash');
var moment = require('moment');
require('moment-timezone');

var TODAY = 'today',
    YESTERDAY = 'yesterday',
    WEEK = 'week',
    MONTH = 'month';


var allowed = [TODAY, YESTERDAY, WEEK, MONTH];

var parse = function (period) {
    if (!period || !_.includes(allowed, period.toLowerCase())) {
        period = TODAY;
    }
    return period;
};


var getPeriodStart = function (period) {
    switch (period) {
        case YESTERDAY:
            return moment().subtract(1, 'days').tz('America/Los_Angeles').format('YYYYMMDD');
        case WEEK:
            return moment().subtract(1, 'w').tz('America/Los_Angeles').format('YYYYMMDD');
        case MONTH:
            return moment().subtract(1, 'm').tz('America/Los_Angeles').format('YYYYMMDD');
        default :
            return moment().tz('America/Los_Angeles').format('YYYYMMDD');
    }
};

var getPeriodEnd = function (period) {
    return moment().tz('America/Los_Angeles').format('YYYYMMDD');
};


var getCachedDates = function (period) {
    var dates = [getPeriodEnd(period)];
    if (period != TODAY) {
        dates.push(moment().subtract(1, 'days').tz('America/Los_Angeles').format('YYYYMMDD'));
    }
    return dates;
};

var getArchiveDates = function (period) {
    var to = moment().add(1, 'days').tz('America/Los_Angeles').format('YYYYMMDD');
    var from = getPeriodStart(period);

    return {from: from, to: to}
};

module.exports = {

    allowed: allowed,
    parse: parse,
    getCachedDates: getCachedDates,
    getArchiveDates: getArchiveDates,
    getPeriodStart: getPeriodStart,
    getPeriodEnd: getPeriodEnd

};