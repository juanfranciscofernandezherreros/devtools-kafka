package com.devkafka.runner;

import com.devkafka.exception.ErrorMessageException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenericRunnerBean {

    private final SchemaDownload schemaDownload;

    public GenericRunnerBean(SchemaDownload schemaDownload) {
        this.schemaDownload = schemaDownload;
    }

    public void run(String keyPayloadFile, String valuePayloadFile, String schemaRegistry, String topicName, String confluentic, String urlPrefix) throws ErrorMessageException {
        long start = System.currentTimeMillis();
        schemaDownload.exportAndSendAvroMessages(keyPayloadFile, valuePayloadFile, schemaRegistry, topicName, confluentic, urlPrefix);
        log.info("✅ Message sent. Time: " + (System.currentTimeMillis() - start) + " ms");
    }
}
