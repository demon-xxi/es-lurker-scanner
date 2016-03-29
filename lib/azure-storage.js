/*jslint  node:true */
//'use strict';

var azure = require('azure-storage');
var STORAGE_ACCESS_KEY = process.env.STORAGE_ACCESS_KEY
    || process.env.APPSETTING_STORAGE_ACCESS_KEY || process.env.AZURE_STORAGE_ACCESS_KEY;
var STORAGE_CONNECTION_STRING = process.env.STORAGE_CONNECTION_STRING
    || process.env.APPSETTING_STORAGE_CONNECTION_STRING || process.env.AZURE_STORAGE_CONNECTION_STRING ||
    azure.generateDevelopmentStorageCredentials();


module.exports = {
    tableService: function () {
        return azure.createTableService(STORAGE_CONNECTION_STRING, STORAGE_ACCESS_KEY);
    },
    TableBatch: azure.TableBatch,
    entityGenerator: azure.TableUtilities.entityGenerator,
    viewerSummaryTable: 'ViewerStats'
};
