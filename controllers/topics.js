var kafka = require('kafka-node'),
    murmur = require('murmurhash-js'),
    config = require('../config'),

    topics = require('../lib/topics.js'),

    client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId),
    producer = new kafka.HighLevelProducer(client),
    compression = config.kafka.compression || 0,
    seed = 0x9747b28c,

    log = require('../logger.js'),
    logger = log.logger;

module.exports = function (app) {

    app.put('/topics/:topic', function (req, res) {
        logger.debug('put information to topic');

        producer.createTopics([req.params.topic],
            false,
            function (err, data) {
                if (err) {
                    logger.error({error: err, request: req, response: res});
                    return res.status(500).json({ error: err });
                }
                res.json({ message: data });
            });
    });

    app.post('/topics/:topic', function (req, res) {

        var topic = req.params.topic;
        logger.trace({topic:topic}, 'controllers/topics : received post to topic');

        topics.partitions(topic, function (err, data) {
            if (err) {
                logger.error({error: err, request: req, response: res});
                return res.status(500).json({ error: err });
            }

            var numPartitions = data,
                payloads = [];

            logger.trace({records: req.body.records.length}, 'controllers/topics : mapping records.');
            req.body
                .records
                .forEach(function (p) {
                    var hasKey = p.key !== null && typeof p.key !== 'undefined',
                        hasPartition = p.partition !== null && typeof p.partition !== 'undefined',
                        result = {
                            topic: topic,
                            messages: [ hasKey ? new kafka.KeyedMessage(p.key, p.value) : p.value ],
                        };
                    if (hasKey) {
                        result.partition = murmur.murmur2(p.key, seed) % numPartitions;
                    }
                    else if (hasPartition) {
                        result.partition = p.partition;
                    }

                    var existing = payloads.find(function (m) {
                        return m.partition == result.partition;
                    });
                    if (existing) {
                        return existing.messages.push(result.messages[0]);
                    }
                    payloads.push(result);
                });

            logger.trace({payloads: payloads.length}, 'controllers/topics : producing messages...');
            producer.send(payloads, function (err, data) {
                if (err) {
                    logger.error({error: err, request: req, response: res}, 'controllers/topics : error producing messages');
                    return res.status(500).json({error: err});
                }

                var topicResult = data[topic];
                var results = [];
                for (var i in topicResult) {
                    results.push({ partition: i, offset: topicResult[i] });
                }
                logger.trace({results: results.length}, 'controllers/topics : produced messages');
                res.json({ offsets: results });
            });
        });

    });

};
