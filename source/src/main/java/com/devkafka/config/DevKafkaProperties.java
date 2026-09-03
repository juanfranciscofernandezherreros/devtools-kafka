package com.devkafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * High-level configuration used by {@code KafkaAvroClient}.
 */
@ConfigurationProperties(prefix = "devkafka")
public class DevKafkaProperties {

    private String schemaRegistryUrl;
    private String restProxyUrl;
    private String schemaVersionPath = "/versions/latest";
    private boolean ignoreSsl;

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getRestProxyUrl() {
        return restProxyUrl;
    }

    public void setRestProxyUrl(String restProxyUrl) {
        this.restProxyUrl = restProxyUrl;
    }

    public String getSchemaVersionPath() {
        return schemaVersionPath;
    }

    public void setSchemaVersionPath(String schemaVersionPath) {
        this.schemaVersionPath = schemaVersionPath;
    }

    public boolean isIgnoreSsl() {
        return ignoreSsl;
    }

    public void setIgnoreSsl(boolean ignoreSsl) {
        this.ignoreSsl = ignoreSsl;
    }
}
