package com.devkafka.client;

import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.SchemaRegistryException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;

/**
 * Minimal HTTP client for Confluent-compatible Schema Registry APIs.
 */
public class SchemaRegistryClientService {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryClientService.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SchemaRegistryClientService(SchemaRegistryProperties properties) {
        this(properties.isIgnoreSsl());
    }

    public SchemaRegistryClientService(boolean ignoreSsl) {
        this.httpClient = HttpClientFactory.create(ignoreSsl);
    }

    public String getLatestSchema(String schemaRegistry, String subject, String urlPrefix) {
        String url = schemaRegistry + subject + urlPrefix;
        log.info("Searching for schema at URL: {}", url);

        try {
            HttpResponse<String> response = get(url);
            ensureSuccessful(response, "Schema registry fetch failed");

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode schemaNode = root.get("schema");
            if (schemaNode == null || schemaNode.isNull()) {
                throw new SchemaRegistryException("Schema registry fetch failed: response does not contain 'schema'");
            }

            log.info("Schema obtained for [{}]", subject);
            return schemaNode.asText();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryException("Schema registry fetch interrupted", e);
        } catch (IOException e) {
            throw new SchemaRegistryException("Schema registry fetch failed", e);
        }
    }

    public List<String> listSubjects(String schemaRegistry) {
        String url = removeTrailingSlash(schemaRegistry);
        log.info("Listing subjects at URL: {}", url);

        try {
            HttpResponse<String> response = get(url);
            ensureSuccessful(response, "Schema registry list-subjects failed");
            return Arrays.asList(objectMapper.readValue(response.body(), String[].class));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryException("Schema registry list-subjects interrupted", e);
        } catch (IOException e) {
            throw new SchemaRegistryException("Schema registry list-subjects failed", e);
        }
    }

    public List<Integer> listVersions(String schemaRegistry, String subject) {
        String url = schemaRegistry + subject + "/versions";
        log.info("Listing versions at URL: {}", url);

        try {
            HttpResponse<String> response = get(url);
            ensureSuccessful(response, "Schema registry list-versions failed");
            return Arrays.asList(objectMapper.readValue(response.body(), Integer[].class));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaRegistryException("Schema registry list-versions interrupted", e);
        } catch (IOException e) {
            throw new SchemaRegistryException("Schema registry list-versions failed", e);
        }
    }

    private HttpResponse<String> get(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void ensureSuccessful(HttpResponse<String> response, String message) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new SchemaRegistryException(message + ": HTTP " + response.statusCode() + ": " + response.body());
        }
    }

    private static String removeTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
