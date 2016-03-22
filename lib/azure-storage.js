/*jslint  node:true */
'use strict';

var azure = require('azure-storage');
var STORAGE_ACCESS_KEY = process.env.APPSETTING_STORAGE_ACCESS_KEY || process.env.AZURE_STORAGE_ACCESS_KEY;
var STORAGE_CONNECTION_STRING = process.env.APPSETTING_STORAGE_CONNECTION_STRING || process.env.AZURE_STORAGE_CONNECTION_STRING ||
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1';



module.exports = {
    tableService: function () {
        return azure.createTableService(STORAGE_CONNECTION_STRING, STORAGE_ACCESS_KEY);
    },
    TableBatch: azure.TableBatch,
    entityGenerator: azure.TableUtilities.entityGenerator,
    viewerSummaryTable: 'ViewerSummary'
};
