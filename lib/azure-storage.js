/*jslint  node:true */
//'use strict';

//var azure = require('azure-storage');
//var STORAGE_ACCESS_KEY = process.env.STORAGE_ACCESS_KEY
//    || process.env.APPSETTING_STORAGE_ACCESS_KEY || process.env.AZURE_STORAGE_ACCESS_KEY;
//var STORAGE_CONNECTION_STRING = process.env.STORAGE_CONNECTION_STRING
//    || process.env.APPSETTING_STORAGE_CONNECTION_STRING || process.env.AZURE_STORAGE_CONNECTION_STRING
//    || azure.generateDevelopmentStorageCredentials();


var azureTable = require('azure-table-node');
var Agent = require('agentkeepalive').HttpsAgent;

var keepaliveAgent = new Agent({
    maxSockets: 1000,
    timeout: 60000,
    keepAliveTimeout: 30000 // free socket keepalive for 30 seconds
});

module.exports = {
    tableService: function () {
        return azureTable.createClient({agent: keepaliveAgent});
    },
    Query: azureTable.Query,
    //TableBatch: azure.TableBatch,
    //entityGenerator: azure.TableUtilities.entityGenerator,
    viewerSummaryTable: 'ViewerStats'
};
