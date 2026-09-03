package com.devkafka.config;

import com.devkafka.KafkaAvroClient;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LibraryAutoConfigurationTest {

    private final LibraryAutoConfiguration config = new LibraryAutoConfiguration();
    private final DevKafkaProperties properties = new DevKafkaProperties();

    @Test
    void devKafkaIsEnabledByDefault() {
        assertTrue(properties.isEnabled());
    }

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

    @Test
    void createsKafkaAvroClient() {
        SchemaRegistryClientService schemaClient = config.schemaRegistryClientService(properties);
        KafkaRestProxyClientService restClient = config.kafkaRestProxyClientService(properties);
        KafkaAvroClient client = config.kafkaAvroClient(schemaClient, restClient, properties);
        assertNotNull(client);
        assertInstanceOf(KafkaAvroClient.class, client);
    }
}
