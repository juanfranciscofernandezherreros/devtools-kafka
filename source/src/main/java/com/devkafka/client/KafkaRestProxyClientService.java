package com.devkafka.client;

import com.devkafka.exception.KafkaRestProxyException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Client for Kafka REST Proxy topic discovery and Avro publication.
 */
public class KafkaRestProxyClientService {

    private static final Logger log = LoggerFactory.getLogger(KafkaRestProxyClientService.class);
    private static final String ACCEPT_HEADER =
            "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json";

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient;

    public KafkaRestProxyClientService(boolean ignoreSsl) {
        this.httpClient = HttpClientFactory.create(ignoreSsl);
    }

    public List<String> listTopics(String restProxyUrl) {
        String endpoint = restProxyUrl.endsWith("/topics") ? restProxyUrl : restProxyUrl + "/topics";
        log.info("Requesting topic list from: {}", endpoint);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Accept", ACCEPT_HEADER)
                .GET()
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new KafkaRestProxyException("HTTP error " + response.statusCode() + ": " + response.body());
            }
            return Arrays.asList(mapper.readValue(response.body(), String[].class));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaRestProxyException("Interrupted while obtaining topic list", e);
        } catch (IOException e) {
            throw new KafkaRestProxyException("Error obtaining topic list", e);
        }
    }

    public void sendAvroMessage(
            String restProxyUrl,
            String topicName,
            String keySchema,
            String valueSchema,
            JsonNode keyPayload,
            JsonNode valuePayload) {

        String endpoint = appendPath(restProxyUrl, topicName);
        ObjectNode payload = mapper.createObjectNode();
        payload.put("key_schema", keySchema);
        payload.put("value_schema", valueSchema);

        ArrayNode records = mapper.createArrayNode();
        ObjectNode record = mapper.createObjectNode();
        record.set("key", keyPayload);
        record.set("value", valuePayload);
        records.add(record);
        payload.set("records", records);

        try {
            String jsonPayload = mapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/vnd.kafka.avro.v2+json")
                    .header("Accept", ACCEPT_HEADER)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
                    .build();

            log.info("Sending Kafka message to topic {} via {}", topicName, endpoint);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new KafkaRestProxyException(
                        "REST Proxy error " + response.statusCode() + ": " + response.body());
            }
            log.info("Kafka message sent successfully to topic {}", topicName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaRestProxyException("Interrupted while sending Kafka message", e);
        } catch (IOException e) {
            throw new KafkaRestProxyException("Error sending Kafka message", e);
        }
    }

    private static String appendPath(String baseUrl, String path) {
        return baseUrl.endsWith("/") ? baseUrl + path : baseUrl + "/" + path;
    }
}
