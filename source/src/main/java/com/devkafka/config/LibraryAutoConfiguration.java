package com.devkafka.config;

import com.devkafka.KafkaAvroClient;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties({DevKafkaProperties.class, SchemaRegistryProperties.class})
public class LibraryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SchemaRegistryClientService schemaRegistryClientService(
            DevKafkaProperties properties,
            SchemaRegistryProperties legacyProperties) {
        return new SchemaRegistryClientService(properties.isIgnoreSsl() || legacyProperties.isIgnoreSsl());
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaRestProxyClientService kafkaRestProxyClientService(
            DevKafkaProperties properties,
            SchemaRegistryProperties legacyProperties) {
        return new KafkaRestProxyClientService(properties.isIgnoreSsl() || legacyProperties.isIgnoreSsl());
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaAvroClient kafkaAvroClient(
            SchemaRegistryClientService schemaRegistryClientService,
            KafkaRestProxyClientService kafkaRestProxyClientService,
            DevKafkaProperties properties) {
        return new KafkaAvroClient(schemaRegistryClientService, kafkaRestProxyClientService, properties);
    }
}
