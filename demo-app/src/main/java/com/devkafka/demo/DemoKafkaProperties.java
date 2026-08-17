package com.devkafka.demo;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Not part of the library itself: GenericRunnerBean.run(...) takes the REST
 * Proxy URL, topic name and payload files as plain method arguments, so
 * this demo app binds its own properties and passes them through.
 */
@Data
@ConfigurationProperties(prefix = "app.kafka")
public class DemoKafkaProperties {

    private String restProxyUrl;
    private String schemaUrlSuffix;
    private String topicName;
    private String keyPayloadFile;
    private String valuePayloadFile;

    /**
     * When false, the runner only lists REST Proxy topics and skips the
     * schema download + sample message send (useful for environments where
     * the Schema Registry isn't configured yet).
     */
    private boolean sendSampleMessage = true;

    /**
     * When true (and sendSampleMessage is false), downloads the key/value
     * schemas for topicName and saves them under schemaOutputDir, without
     * sending a sample message. Ignored if sendSampleMessage is true, since
     * that flow already downloads the schemas as part of sending.
     */
    private boolean downloadSchemaOnly = false;

    /**
     * Directory schemas are saved to when downloadSchemaOnly is true.
     */
    private String schemaOutputDir = "schemas";
}
