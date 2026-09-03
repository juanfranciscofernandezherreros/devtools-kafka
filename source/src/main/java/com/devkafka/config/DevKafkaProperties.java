package com.devkafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Single Spring Boot configuration surface for the library.
 */
@ConfigurationProperties(prefix = "devkafka")
public class DevKafkaProperties {

    private boolean enabled = true;
    private String schemaRegistryUrl;
    private String restProxyUrl;
    private String schemaVersionPath = "/versions/latest";
    private boolean ignoreSsl;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

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
