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
}
