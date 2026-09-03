package com.devkafka.config;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(SchemaRegistryProperties.class)
public class LibraryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.schema", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SchemaRegistryClientService schemaRegistryClientService(SchemaRegistryProperties properties) {
        return new SchemaRegistryClientService(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.restproxy", name = "enabled", havingValue = "true", matchIfMissing = true)
    public KafkaRestProxyClientService kafkaRestProxyClientService(SchemaRegistryProperties properties) {
        return new KafkaRestProxyClientService(properties);
    }
}
