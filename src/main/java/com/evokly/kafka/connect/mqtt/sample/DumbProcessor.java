package com.evokly.kafka.connect.mqtt.sample;

import com.evokly.kafka.connect.mqtt.MqttMessageProcessor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


/**
 * Copyright 2016 Evokly S.A.
 *
 * <p>See LICENSE file for License
 **/
public class DumbProcessor implements MqttMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(DumbProcessor.class);
    private MqttMessage mMessage;
    private String mTopic;

    @Override
    public MqttMessageProcessor process(String topic, MqttMessage message) {
        log.debug("processing data for topic: {}; with message {}", topic, message);
        this.mTopic = topic;
        this.mMessage = message;
        return this;
    }

    @Override
    public SourceRecord[] getRecords(String kafkaTopic) {
        String actualTopic = mTopic.replace("/", "_");
        if (log.isTraceEnabled()) {
            try {
                log.trace("Processed message {} / {}", mTopic, new String(mMessage.getPayload()));
            } catch (Exception ex) {
                //The new String() _might_ fail if message payloads were not actually text.
                // Ignore those errors since they're only related to trace logging anyway.
            }
        }
        return new SourceRecord[]{new SourceRecord(null, null, actualTopic, null,
                Schema.STRING_SCHEMA, mTopic,
                Schema.STRING_SCHEMA, new String(mMessage.getPayload(), StandardCharsets.UTF_8))};
    }
}
