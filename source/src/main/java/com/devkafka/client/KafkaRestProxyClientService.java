package com.devkafka.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.UUID;

/**
 * Client for the Kafka REST Proxy. Certificate validation is enabled by
 * default; it's only skipped when {@code library.schema.ignore-ssl=true}
 * (local/dev environments with self-signed certs).
 */
@Service
@Slf4j
public class KafkaRestProxyClientService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean ignoreSsl;

    public KafkaRestProxyClientService(SchemaRegistryProperties properties) {
        this.ignoreSsl = properties.isIgnoreSsl();
    }

    /**
     * Lists all available topics from the REST Proxy.
     */
    public List<String> listTopics(String restProxyUrl) {
        String endpoint = restProxyUrl.endsWith("/topics") ? restProxyUrl : restProxyUrl + "/topics";
        log.info("Requesting topic list from: {}", endpoint);

        HttpClient client;
        HttpRequest request;

        try {
            HttpClient.Builder clientBuilder = HttpClient.newBuilder();
            if (ignoreSsl) {
                clientBuilder.sslContext(createInsecureSslContext());
            }
            client = clientBuilder.build();

            request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json")
                    .GET()
                    .build();
        } catch (Exception e) {
            log.error("Error configuring HttpClient/SSL: {}", e.getMessage());
            throw new KafkaRestProxyException("Error configuring HttpClient", e);
        }

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();

            if (status != 200) {
                log.error("HTTP error {} when obtaining topics: {}", status, response.body());
                throw new KafkaRestProxyException("HTTP error " + status + ": " + response.body());
            }

            String body = response.body();
            log.info("Topics JSON received: {}", body);

            return Arrays.asList(mapper.readValue(body, String[].class));

        } catch (IOException | InterruptedException e) {
            log.error("Error obtaining topic list: {}", e.getMessage(), e);
            throw new KafkaRestProxyException("Error obtaining topic list", e);
        }
    }

    /**
     * Reads back the records currently available on a topic via a
     * throwaway REST Proxy consumer group (created, subscribed, read once
     * and deleted within this call) — useful to confirm what a previous
     * produce actually wrote. Avro key/value are returned already decoded
     * to JSON by the REST Proxy.
     *
     * @param restProxyUrl base REST Proxy URL, with or without a trailing "/topics"
     * @param topicName    topic to read from
     * @return raw JSON array of records (as returned by GET .../records)
     */
    public String consumeLatestMessages(String restProxyUrl, String topicName) {
        String base = restProxyUrl.endsWith("/topics")
                ? restProxyUrl.substring(0, restProxyUrl.length() - "/topics".length())
                : restProxyUrl;
        String groupName = "devkafka-demo-" + UUID.randomUUID();

        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        try {
            if (ignoreSsl) {
                clientBuilder.sslContext(createInsecureSslContext());
            }
        } catch (Exception e) {
            log.error("Error configuring HttpClient/SSL: {}", e.getMessage());
            throw new KafkaRestProxyException("Error configuring HttpClient", e);
        }
        HttpClient client = clientBuilder.build();

        try {
            String createBody = mapper.writeValueAsString(Map.of(
                    "name", "instance-1",
                    "format", "avro",
                    "auto.offset.reset", "earliest"
            ));
            HttpRequest createRequest = HttpRequest.newBuilder()
                    .uri(URI.create(base + "/consumers/" + groupName))
                    .header("Content-Type", "application/vnd.kafka.v2+json")
                    .POST(HttpRequest.BodyPublishers.ofString(createBody, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> createResponse = client.send(createRequest, HttpResponse.BodyHandlers.ofString());
            if (createResponse.statusCode() != 200) {
                throw new KafkaRestProxyException("Error creating consumer: HTTP " + createResponse.statusCode() + ": " + createResponse.body());
            }
            // The REST Proxy advertises base_uri using its own configured
            // host.name (e.g. the container hostname in Docker Compose),
            // which usually isn't reachable from the caller. Keep only the
            // path and re-attach it to the host we actually connected to.
            String advertisedUri = mapper.readTree(createResponse.body()).get("base_uri").asText();
            String baseUri = base + URI.create(advertisedUri).getRawPath();

            String subscribeBody = mapper.writeValueAsString(Map.of("topics", List.of(topicName)));
            HttpRequest subscribeRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUri + "/subscription"))
                    .header("Content-Type", "application/vnd.kafka.v2+json")
                    .POST(HttpRequest.BodyPublishers.ofString(subscribeBody, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> subscribeResponse = client.send(subscribeRequest, HttpResponse.BodyHandlers.ofString());
            if (subscribeResponse.statusCode() / 100 != 2) {
                throw new KafkaRestProxyException("Error subscribing to topic: HTTP " + subscribeResponse.statusCode() + ": " + subscribeResponse.body());
            }

            String records = fetchRecords(client, baseUri);
            if ("[]".equals(records)) {
                // The first poll right after subscribing often returns empty
                // while the consumer group finishes assigning partitions.
                Thread.sleep(1000);
                records = fetchRecords(client, baseUri);
            }

            HttpRequest deleteRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUri))
                    .header("Content-Type", "application/vnd.kafka.v2+json")
                    .DELETE()
                    .build();
            client.send(deleteRequest, HttpResponse.BodyHandlers.discarding());

            return records;
        } catch (IOException | InterruptedException e) {
            log.error("Error consuming messages from [{}]: {}", topicName, e.getMessage());
            throw new KafkaRestProxyException("Error consuming messages", e);
        }
    }

    private String fetchRecords(HttpClient client, String baseUri) throws IOException, InterruptedException {
        HttpRequest recordsRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/records?timeout=3000"))
                .header("Accept", "application/vnd.kafka.avro.v2+json")
                .GET()
                .build();
        HttpResponse<String> response = client.send(recordsRequest, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new KafkaRestProxyException("Error fetching records: HTTP " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    /*───────────────────────────────────────────────────────────────*/
    /* SSL ignored (testing only) */
    /*───────────────────────────────────────────────────────────────*/
    private static SSLContext createInsecureSslContext() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                    public void checkClientTrusted(X509Certificate[] c, String a) {}
                    public void checkServerTrusted(X509Certificate[] c, String a) {}
                }
        };

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAllCerts, new SecureRandom());
        log.info("SSLContext without certificate validation (DEV/QA only)");
        return ctx;
    }
}
