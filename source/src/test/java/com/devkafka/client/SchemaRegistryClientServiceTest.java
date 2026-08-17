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
import java.util.List;

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

    @Test
    @DisplayName("✅ Correctly lists the versions of a subject")
    void testListVersions_success() {
        String baseUrl = "https://registry.dev/";
        String subject = "customer-value";

        mockServer.expect(once(), requestTo(baseUrl + subject + "/versions"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(OK)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("[1, 2, 3]"));

        List<Integer> versions = client.listVersions(baseUrl, subject);

        assertEquals(List.of(1, 2, 3), versions);
        mockServer.verify();
    }

    @Test
    @DisplayName("⚠️ Throws exception if listing versions returns HTTP error")
    void testListVersions_httpError() {
        String baseUrl = "https://registry.dev/";
        String subject = "missing-subject";

        mockServer.expect(once(), requestTo(baseUrl + subject + "/versions"))
                .andRespond(withStatus(NOT_FOUND));

        SchemaRegistryException ex = assertThrows(SchemaRegistryException.class, () ->
                client.listVersions(baseUrl, subject));

        assertTrue(ex.getMessage().contains("Schema registry list-versions failed"));
    }

    @Test
    @DisplayName("✅ Correctly lists every subject in the registry")
    void testListSubjects_success() {
        String baseUrl = "https://registry.dev/";

        mockServer.expect(once(), requestTo("https://registry.dev"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(OK)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("[\"topic1-key\", \"topic1-value\", \"topic2-value\"]"));

        List<String> subjects = client.listSubjects(baseUrl);

        assertEquals(List.of("topic1-key", "topic1-value", "topic2-value"), subjects);
        mockServer.verify();
    }

    @Test
    @DisplayName("⚠️ Throws exception if listing subjects returns HTTP error")
    void testListSubjects_httpError() {
        String baseUrl = "https://registry.dev/";

        mockServer.expect(once(), requestTo("https://registry.dev"))
                .andRespond(withStatus(INTERNAL_SERVER_ERROR));

        SchemaRegistryException ex = assertThrows(SchemaRegistryException.class, () ->
                client.listSubjects(baseUrl));

        assertTrue(ex.getMessage().contains("Schema registry list-subjects failed"));
    }
}
