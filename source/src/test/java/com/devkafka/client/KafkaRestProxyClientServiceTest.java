package com.devkafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.KafkaRestProxyException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRestProxyClientServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockWebServer mockWebServer;
    private KafkaRestProxyClientService service;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        service = new KafkaRestProxyClientService(new SchemaRegistryProperties());
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    @DisplayName("Returns topic list correctly when receiving 200 OK")
    void testListTopics_success() {
        String jsonResponse = "[\"topic1\", \"topic2\", \"topic3\"]";
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(jsonResponse)
                .addHeader("Content-Type", "application/json"));

        String baseUrl = mockWebServer.url("/topics").toString();
        List<String> topics = service.listTopics(baseUrl);

        assertNotNull(topics);
        assertEquals(3, topics.size());
        assertEquals("topic1", topics.get(0));
        assertEquals("topic3", topics.get(2));
    }

    @Test
    @DisplayName("Throws exception on topic-list HTTP response other than 200")
    void testListTopics_httpError() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(404)
                .setBody("{\"error\": \"not found\"}"));

        String baseUrl = mockWebServer.url("/topics").toString();

        KafkaRestProxyException ex = assertThrows(KafkaRestProxyException.class, () ->
                service.listTopics(baseUrl));

        assertTrue(ex.getMessage().contains("HTTP error 404:"));
    }

    @Test
    @DisplayName("Sends an Avro record with schemas to the requested topic")
    void testSendAvroMessage_success() throws Exception {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"offsets\":[{\"partition\":0,\"offset\":1}]}"));

        String restProxyTopicsUrl = mockWebServer.url("/topics").toString();
        JsonNode key = MAPPER.readTree("{\"id\":1}");
        JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");

        service.sendAvroMessage(
                restProxyTopicsUrl,
                "test-topic",
                "{\"type\":\"string\"}",
                "{\"type\":\"record\"}",
                key,
                value);

        var request = mockWebServer.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("/topics/test-topic", request.getPath());
        assertEquals("application/vnd.kafka.avro.v2+json", request.getHeader("Content-Type"));

        JsonNode body = MAPPER.readTree(request.getBody().readUtf8());
        assertEquals("{\"type\":\"string\"}", body.get("key_schema").asText());
        assertEquals("{\"type\":\"record\"}", body.get("value_schema").asText());
        assertEquals(1, body.get("records").get(0).get("key").get("id").asInt());
        assertEquals("test", body.get("records").get(0).get("value").get("name").asText());
    }

    @Test
    @DisplayName("Throws KafkaRestProxyException when publishing fails")
    void testSendAvroMessage_httpError() throws Exception {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(500)
                .setBody("{\"error\":\"internal server error\"}"));

        String restProxyTopicsUrl = mockWebServer.url("/topics").toString();
        JsonNode key = MAPPER.readTree("{\"id\":1}");
        JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");

        KafkaRestProxyException ex = assertThrows(KafkaRestProxyException.class, () ->
                service.sendAvroMessage(
                        restProxyTopicsUrl,
                        "test-topic",
                        "{\"type\":\"string\"}",
                        "{\"type\":\"record\"}",
                        key,
                        value));

        assertTrue(ex.getMessage().contains("REST Proxy error 500"));
    }
}
