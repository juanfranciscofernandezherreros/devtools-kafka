package com.devkafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "library.schema")
public class SchemaRegistryProperties {

    private String registryUrl;
    private boolean ignoreSsl;
    private String basicTokenBase64;
    private String user;
    private String pass;

    public String getRegistryUrl() {
        return registryUrl;
    }

    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    public boolean isIgnoreSsl() {
        return ignoreSsl;
    }

    public void setIgnoreSsl(boolean ignoreSsl) {
        this.ignoreSsl = ignoreSsl;
    }

    public String getBasicTokenBase64() {
        return basicTokenBase64;
    }

    public void setBasicTokenBase64(String basicTokenBase64) {
        this.basicTokenBase64 = basicTokenBase64;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }
}
