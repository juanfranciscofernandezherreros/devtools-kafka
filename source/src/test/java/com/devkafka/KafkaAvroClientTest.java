package com.devkafka;

import com.devkafka.api.KafkaAvroMessage;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.DevKafkaProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAvroClientTest {

    private SchemaRegistryClientService schemaRegistryClient;
    private KafkaRestProxyClientService restProxyClient;
    private DevKafkaProperties properties;
    private KafkaAvroClient client;

    @BeforeEach
    void setUp() {
        schemaRegistryClient = mock(SchemaRegistryClientService.class);
        restProxyClient = mock(KafkaRestProxyClientService.class);
        properties = new DevKafkaProperties();
        properties.setSchemaRegistryUrl("http://registry/subjects");
        properties.setRestProxyUrl("http://proxy/topics");
        client = new KafkaAvroClient(schemaRegistryClient, restProxyClient, properties);
    }

    @Test
    void generateDummyUsesTopicKeyAndValueSubjects() {
        when(schemaRegistryClient.getLatestSchema(
                "http://registry/subjects/", "orders-key", "/versions/latest"))
                .thenReturn("\"string\"");
        when(schemaRegistryClient.getLatestSchema(
                "http://registry/subjects/", "orders-value", "/versions/latest"))
                .thenReturn("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

        KafkaAvroMessage message = client.generateDummy("orders");

        assertNotNull(message.key());
        assertNotNull(message.value());
        verify(schemaRegistryClient).getLatestSchema(
                "http://registry/subjects/", "orders-key", "/versions/latest");
        verify(schemaRegistryClient).getLatestSchema(
                "http://registry/subjects/", "orders-value", "/versions/latest");
    }

    @Test
    void sendDownloadsSchemasAndDelegatesToRestProxy() throws Exception {
        when(schemaRegistryClient.getLatestSchema(any(), eq("orders-key"), any()))
                .thenReturn("\"string\"");
        when(schemaRegistryClient.getLatestSchema(any(), eq("orders-value"), any()))
                .thenReturn("\"string\"");

        ObjectMapper mapper = new ObjectMapper();
        client.send("orders", mapper.readTree("\"k1\""), mapper.readTree("\"v1\""));

        verify(restProxyClient).sendAvroMessage(
                eq("http://proxy/topics"),
                eq("orders"),
                eq("\"string\""),
                eq("\"string\""),
                any(),
                any());
    }
}
