package com.devkafka.config;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LibraryAutoConfigurationTest {

    private final LibraryAutoConfiguration config = new LibraryAutoConfiguration();
    private final SchemaRegistryProperties properties = new SchemaRegistryProperties();

    @Test
    void createsSchemaRegistryClientService() {
        SchemaRegistryClientService service = config.schemaRegistryClientService(properties);
        assertNotNull(service);
        assertInstanceOf(SchemaRegistryClientService.class, service);
    }

    @Test
    void createsKafkaRestProxyClientService() {
        KafkaRestProxyClientService service = config.kafkaRestProxyClientService(properties);
        assertNotNull(service);
        assertInstanceOf(KafkaRestProxyClientService.class, service);
    }
}
