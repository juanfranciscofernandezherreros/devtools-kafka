package com.devkafka.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.ErrorMessageException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SchemaDownloadTest {

    private MockWebServer mockWebServer;
    private SchemaRegistryClientService mockRegistry;
    private SchemaDownload schemaDownload;

    private File keyFile;
    private File valueFile;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws IOException {
        // Simulated REST Proxy server
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        // Mock of the Schema Registry service
        mockRegistry = Mockito.mock(SchemaRegistryClientService.class);
        schemaDownload = new SchemaDownload(mockRegistry, new SchemaRegistryProperties());

        // Temporary payload files
        keyFile = File.createTempFile("key", ".json");
        valueFile = File.createTempFile("value", ".json");

        try (FileWriter fw = new FileWriter(keyFile)) {
            fw.write("{\"id\": 1}");
        }
        try (FileWriter fw = new FileWriter(valueFile)) {
            fw.write("{\"name\": \"test\"}");
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
        keyFile.delete();
        valueFile.delete();
    }

    @Test
    @DisplayName("✅ Correctly sends an Avro message to the REST Proxy")
    void testExportAndSendAvroMessages_success() throws Exception {
        // 1️⃣ Mock of the schema registry
        when(mockRegistry.getLatestSchema(any(), contains("-key"), any()))
                .thenReturn("{\"type\":\"string\"}");
        when(mockRegistry.getLatestSchema(any(), contains("-value"), any()))
                .thenReturn("{\"type\":\"int\"}");

        // 2️⃣ Simulated REST Proxy response (200 OK)
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"offsets\": [{\"partition\": 0, \"offset\": 1}]}"));

        String baseUrl = mockWebServer.url("/topics").toString();

        // 3️⃣ Execute the method
        schemaDownload.exportAndSendAvroMessages(
                keyFile.getAbsolutePath(),
                valueFile.getAbsolutePath(),
                "http://schema-registry.dev",
                "test-topic",
                baseUrl,
                "http://prefix/"
        );

        // 4️⃣ Verify that the expected mock methods were called
        verify(mockRegistry, times(1))
                .getLatestSchema(any(), eq("test-topic-key"), any());
        verify(mockRegistry, times(1))
                .getLatestSchema(any(), eq("test-topic-value"), any());

        // 5️⃣ Analyze the request received by the MockWebServer
        var recorded = mockWebServer.takeRequest();
        String body = recorded.getBody().readUtf8();

        assertEquals("POST", recorded.getMethod());
        assertTrue(body.contains("key_schema"), "Must contain key_schema");
        assertTrue(body.contains("records"), "Must contain records");

        // 6️⃣ Validate the sent JSON content
        JsonNode payload = mapper.readTree(body);
        assertEquals("{\"type\":\"string\"}", payload.get("key_schema").asText());
        assertEquals("{\"type\":\"int\"}", payload.get("value_schema").asText());

        JsonNode recordNode = payload.get("records").get(0);
        assertEquals(1, recordNode.get("key").get("id").asInt());
        assertEquals("test", recordNode.get("value").get("name").asText());
    }

    @Test
    @DisplayName("⚠️ Throws ErrorMessageException if the REST Proxy returns an HTTP error")
    void testExportAndSendAvroMessages_httpError() {
        when(mockRegistry.getLatestSchema(any(), anyString(), any()))
                .thenReturn("{\"type\":\"string\"}");

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(500)
                .setBody("{\"error\":\"internal server error\"}"));

        String baseUrl = mockWebServer.url("/topics").toString();

        assertThrows(ErrorMessageException.class, () -> {
            schemaDownload.exportAndSendAvroMessages(
                    keyFile.getAbsolutePath(),
                    valueFile.getAbsolutePath(),
                    "http://schema-registry.dev",
                    "test-topic",
                    baseUrl,
                    "http://prefix/"
            );
        });
    }

    @Test
    @DisplayName("🚫 Throws ErrorMessageException if the JSON file cannot be read")
    void testExportAndSendAvroMessages_fileError() {
        // Path is never created on disk: exercises the file-not-found branch on purpose.
        File missingPayloadFile = new File("does-not-exist-on-purpose.json");

        assertThrows(ErrorMessageException.class, () -> {
            schemaDownload.exportAndSendAvroMessages(
                    missingPayloadFile.getAbsolutePath(),
                    missingPayloadFile.getAbsolutePath(),
                    "http://schema-registry.dev",
                    "test-topic",
                    "http://localhost",
                    "http://prefix/"
            );
        });
    }
}
