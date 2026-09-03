package com.devkafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.KafkaRestProxyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

/**
 * Client for the Kafka REST Proxy. Certificate validation is enabled by
 * default; it's only skipped when {@code library.schema.ignore-ssl=true}
 * (local/dev environments with self-signed certs).
 */
@Service
@Slf4j
public class KafkaRestProxyClientService {

    private static final String ACCEPT_HEADER =
            "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json";

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient;

    public KafkaRestProxyClientService(SchemaRegistryProperties properties) {
        this.httpClient = createHttpClient(properties.isIgnoreSsl());
    }

    /**
     * Lists all available topics from the REST Proxy.
     */
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
            int status = response.statusCode();

            if (status != 200) {
                log.error("HTTP error {} when obtaining topics: {}", status, response.body());
                throw new KafkaRestProxyException("HTTP error " + status + ": " + response.body());
            }

            String body = response.body();
            log.debug("Topics JSON received: {}", body);
            return Arrays.asList(mapper.readValue(body, String[].class));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaRestProxyException("Interrupted while obtaining topic list", e);
        } catch (IOException e) {
            log.error("Error obtaining topic list: {}", e.getMessage(), e);
            throw new KafkaRestProxyException("Error obtaining topic list", e);
        }
    }

    /**
     * Sends one Avro key/value record through the Kafka REST Proxy.
     */
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
            log.debug("Payload sent to REST Proxy: {}", jsonPayload);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/vnd.kafka.avro.v2+json")
                    .header("Accept", ACCEPT_HEADER)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
                    .build();

            log.info("Sending Kafka message to topic {} via {}", topicName, endpoint);
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("REST Proxy returned HTTP {}: {}", response.statusCode(), response.body());
                throw new KafkaRestProxyException(
                        "REST Proxy error " + response.statusCode() + ": " + response.body());
            }

            log.info("Kafka message sent successfully to topic {}", topicName);
            log.debug("REST Proxy response: {}", response.body());

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

    private static HttpClient createHttpClient(boolean ignoreSsl) {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder();
            if (ignoreSsl) {
                builder.sslContext(createInsecureSslContext());
            }
            return builder.build();
        } catch (Exception e) {
            throw new KafkaRestProxyException("Error configuring HttpClient", e);
        }
    }

    /**
     * FOR TESTING/DEV ONLY: creates an SSLContext that trusts all certificates.
     */
    private static SSLContext createInsecureSslContext() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(X509Certificate[] c, String a) {
                    }

                    public void checkServerTrusted(X509Certificate[] c, String a) {
                    }
                }
        };

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAllCerts, new SecureRandom());
        log.info("SSLContext without certificate validation (DEV/QA only)");
        return ctx;
    }
}
