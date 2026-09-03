package com.devkafka.client;

import com.devkafka.exception.KafkaRestProxyException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRestProxyClientServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private MockWebServer server;
    private KafkaRestProxyClientService service;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        service = new KafkaRestProxyClientService(false);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void listsTopics() {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("[\"topic1\",\"topic2\"]"));
        List<String> topics = service.listTopics(server.url("/topics").toString());
        assertEquals(List.of("topic1", "topic2"), topics);
    }

    @Test
    void rejectsTopicListHttpError() {
        server.enqueue(new MockResponse().setResponseCode(404).setBody("not found"));
        KafkaRestProxyException ex = assertThrows(KafkaRestProxyException.class,
                () -> service.listTopics(server.url("/topics").toString()));
        assertTrue(ex.getMessage().contains("HTTP error 404"));
    }

    @Test
    void sendsAvroMessageAndAcceptsAny2xx() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(201).setBody("{\"offsets\":[]}"));
        JsonNode key = MAPPER.readTree("{\"id\":1}");
        JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");

        service.sendAvroMessage(server.url("/topics").toString(), "test-topic",
                "{\"type\":\"string\"}", "{\"type\":\"record\"}", key, value);

        var request = server.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("/topics/test-topic", request.getPath());
        assertEquals("application/vnd.kafka.avro.v2+json", request.getHeader("Content-Type"));
    }

    @Test
    void rejectsPublishHttpError() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("error"));
        JsonNode key = MAPPER.readTree("{\"id\":1}");
        JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");
        KafkaRestProxyException ex = assertThrows(KafkaRestProxyException.class,
                () -> service.sendAvroMessage(server.url("/topics").toString(), "test-topic",
                        "{\"type\":\"string\"}", "{\"type\":\"record\"}", key, value));
        assertTrue(ex.getMessage().contains("REST Proxy error 500"));
    }
}
