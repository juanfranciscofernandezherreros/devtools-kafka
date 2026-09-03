package com.devkafka.config;

import com.devkafka.KafkaAvroClient;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LibraryAutoConfigurationTest {

    private final LibraryAutoConfiguration config = new LibraryAutoConfiguration();
    private final DevKafkaProperties properties = new DevKafkaProperties();
    private final SchemaRegistryProperties legacyProperties = new SchemaRegistryProperties();

    @Test
    void createsSchemaRegistryClientService() {
        SchemaRegistryClientService service = config.schemaRegistryClientService(properties, legacyProperties);
        assertNotNull(service);
        assertInstanceOf(SchemaRegistryClientService.class, service);
    }

    @Test
    void createsKafkaRestProxyClientService() {
        KafkaRestProxyClientService service = config.kafkaRestProxyClientService(properties, legacyProperties);
        assertNotNull(service);
        assertInstanceOf(KafkaRestProxyClientService.class, service);
    }

    @Test
    void createsKafkaAvroClient() {
        SchemaRegistryClientService schemaClient = config.schemaRegistryClientService(properties, legacyProperties);
        KafkaRestProxyClientService restClient = config.kafkaRestProxyClientService(properties, legacyProperties);
        KafkaAvroClient client = config.kafkaAvroClient(schemaClient, restClient, properties);
        assertNotNull(client);
        assertInstanceOf(KafkaAvroClient.class, client);
    }
}
