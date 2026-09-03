package com.devkafka.client;

import com.devkafka.exception.SchemaRegistryException;
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

class SchemaRegistryClientServiceTest {

    private MockWebServer server;
    private SchemaRegistryClientService client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        client = new SchemaRegistryClientService(false);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void getsLatestSchema() {
        server.enqueue(new MockResponse().setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}"));
        String schema = client.getLatestSchema(server.url("/subjects/").toString(), "customer-value", "/versions/latest");
        assertEquals("{\"type\":\"string\"}", schema);
    }

    @Test
    void rejectsSchemaRegistryHttpError() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("error"));
        SchemaRegistryException ex = assertThrows(SchemaRegistryException.class,
                () -> client.getLatestSchema(server.url("/subjects/").toString(), "customer-value", "/versions/latest"));
        assertTrue(ex.getMessage().contains("Schema registry fetch failed"));
    }

    @Test
    void listsSubjects() {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("[\"topic1-key\",\"topic1-value\"]"));
        List<String> subjects = client.listSubjects(server.url("/subjects/").toString());
        assertEquals(List.of("topic1-key", "topic1-value"), subjects);
    }

    @Test
    void listsVersions() {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("[1,2,3]"));
        List<Integer> versions = client.listVersions(server.url("/subjects/").toString(), "customer-value");
        assertEquals(List.of(1, 2, 3), versions);
    }
}
