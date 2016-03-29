/*jslint  node:true */
//"use strict";

var passcode = process.env.APPSETTING_GK_PASSCODE || 'YouShallNotPass';
var header = 'X-GateKeeper-Passcode';

module.exports = {
    header: header,
    passcode: passcode,

    allow: function(req){

        return process.env.NODE_ENV != 'production'
            || req.query[header] == passcode
            || req.get(header) == passcode;

    }
};
