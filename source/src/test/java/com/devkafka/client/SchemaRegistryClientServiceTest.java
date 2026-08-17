package com.devkafka.client;

import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.SchemaRegistryException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.http.HttpStatus.*;

class SchemaRegistryClientServiceTest {

    private SchemaRegistryClientService client;
    private MockRestServiceServer mockServer;

    @BeforeEach
    void setup() throws Exception {
        client = new SchemaRegistryClientService(new SchemaRegistryProperties());

        // Access the internal RestTemplate via reflection
        Field restTemplateField = SchemaRegistryClientService.class.getDeclaredField("restTemplate");
        restTemplateField.setAccessible(true);
        RestTemplate restTemplate = (RestTemplate) restTemplateField.get(client);

        // Associate the MockRestServiceServer with the real RestTemplate of the service
        mockServer = MockRestServiceServer.createServer(restTemplate);
    }

    @Test
    @DisplayName("✅ Correctly obtains the schema from the Schema Registry")
    void testGetLatestSchema_success() throws Exception {
        String expectedSchemaJson = "{\"schema\": \"{\\\"type\\\":\\\"string\\\"}\"}";
        String baseUrl = "https://registry.dev/";
        String subject = "customer-value";
        String prefix = "/latest";

        mockServer.expect(once(), requestTo(baseUrl + subject + prefix))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(OK)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(expectedSchemaJson));

        String schema = client.getLatestSchema(baseUrl, subject, prefix);

        assertEquals("{\"type\":\"string\"}", schema);
        mockServer.verify();
    }

    @Test
    @DisplayName("⚠️ Throws exception if the server returns HTTP error")
    void testGetLatestSchema_httpError() {
        String baseUrl = "https://registry.dev/";
        String subject = "order-key";
        String prefix = "/latest";

        mockServer.expect(once(), requestTo(baseUrl + subject + prefix))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(INTERNAL_SERVER_ERROR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("{\"error\": \"Internal error\"}"));

        SchemaRegistryException ex = assertThrows(SchemaRegistryException.class, () ->
                client.getLatestSchema(baseUrl, subject, prefix));

        assertTrue(ex.getMessage().contains("Schema registry fetch failed"));
    }

    @Test
    @DisplayName("🚫 Throws exception if the response JSON does not have 'schema' field")
    void testGetLatestSchema_invalidJson() {
        String baseUrl = "https://registry.dev/";
        String subject = "bad-value";
        String prefix = "/latest";

        mockServer.expect(once(), requestTo(baseUrl + subject + prefix))
                .andRespond(withStatus(OK)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("{\"other\":\"field\"}"));

        assertThrows(SchemaRegistryException.class, () ->
                client.getLatestSchema(baseUrl, subject, prefix));
    }
}
