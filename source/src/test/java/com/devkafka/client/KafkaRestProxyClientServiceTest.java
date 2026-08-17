package com.devkafka.client;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRestProxyClientServiceTest {

    private static MockWebServer mockWebServer;
    private KafkaRestProxyClientService service;

    @BeforeAll
    static void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
    }

    @AfterAll
    static void stopServer() throws IOException {
        mockWebServer.shutdown();
    }

    @BeforeEach
    void setUp() {
        service = new KafkaRestProxyClientService();
    }

    @Test
    @DisplayName("Returns topic list correctly when receiving 200 OK")
    void testListTopics_success() {
        // Prepare simulated REST Proxy response
        String jsonResponse = "[\"topic1\", \"topic2\", \"topic3\"]";
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(jsonResponse)
                .addHeader("Content-Type", "application/json"));

        // Simulated server URL
        String baseUrl = mockWebServer.url("/topics").toString();

        // Execute the method
        List<String> topics = service.listTopics(baseUrl);

        // Verify result
        assertNotNull(topics);
        assertEquals(3, topics.size());
        assertEquals("topic1", topics.get(0));
        assertEquals("topic3", topics.get(2));
    }

    @Test
    @DisplayName("Throws exception on HTTP response other than 200")
    void testListTopics_httpError() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(404)
                .setBody("{\"error\": \"not found\"}"));

        String baseUrl = mockWebServer.url("/topics").toString();

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                service.listTopics(baseUrl));

        assertTrue(ex.getMessage().contains("HTTP error 404:"));
    }
}
