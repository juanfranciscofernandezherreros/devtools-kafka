package com.devkafka.config;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.runner.GenericRunnerBean;
import com.devkafka.runner.SchemaDownload;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@AutoConfiguration
@EnableConfigurationProperties({SchemaRegistryProperties.class})
public class LibraryAutoConfiguration {

    /**
     * SINGLE RestTemplate exposed by the library.
     */
    @Bean(name = "libraryRestTemplate")
    @ConditionalOnMissingBean(name = "libraryRestTemplate")
    public RestTemplate libreriaRestTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    /**
     * Schema Registry client service.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.schema", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SchemaRegistryClientService schemaRegistryClientService() {
        return new SchemaRegistryClientService();
    }

    /**
     * Schema downloader from Schema Registry.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.schema", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SchemaDownload schemaDownload(SchemaRegistryClientService schemaRegistryClientService) {
        return new SchemaDownload(schemaRegistryClientService);
    }

    /**
     * Generic runner for sending messages.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.runner", name = "enabled", havingValue = "true", matchIfMissing = true)
    public GenericRunnerBean genericRunnerBean(SchemaDownload schemaDownload) {
        return new GenericRunnerBean(schemaDownload);
    }

    /**
     * REST Proxy client (for listing topics, sending messages, etc.).
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "library.restproxy", name = "enabled", havingValue = "true", matchIfMissing = true)
    public KafkaRestProxyClientService kafkaRestProxyClientService() {
        return new KafkaRestProxyClientService();
    }
}
