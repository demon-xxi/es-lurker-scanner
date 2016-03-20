var express = require('express');
var router = express.Router();
var log = require('winston');
var needle = require('needle-retry');
var util = require('util');
//var async = require('async');

var TMI_URL = 'http://tmi.twitch.tv/group/user/%s/chatters';

var options = {
    needle:{
        compressed: true,
        json: true
    },
    retry: {
        retries: 5
    }
};


router.post('/channel/:channel', function (req, res) {
    var channel = req.body;

    needle.get(util.format(TMI_URL, channel.name), options, function (err, response){
        if (err || response.statusCode != 200){
            log.error("Error getting viewers list.", channel.name, err || response.body);
            return res.status(502).json({
                channel: channel.name,
                status: response.statusCode,
                error: err || response.body
            });
        }

        log.info(channel);
        return res.json(channel);
    });

});

module.exports = router;
