package com.devkafka.config;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "library.schema")
public class SchemaRegistryProperties {

    private String registryUrl;
    private boolean ignoreSsl = false;
    private String basicTokenBase64;
    private String user;
    private String pass;
}
