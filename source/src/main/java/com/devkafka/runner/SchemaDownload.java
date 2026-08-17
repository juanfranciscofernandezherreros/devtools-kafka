package com.devkafka.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.ErrorMessageException;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

@Component
@Slf4j
public class SchemaDownload {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final SchemaRegistryClientService schemaRegistryDownloader;
    private final boolean ignoreSsl;

    public SchemaDownload(SchemaRegistryClientService schemaRegistryDownloader, SchemaRegistryProperties properties) {
        this.schemaRegistryDownloader = schemaRegistryDownloader;
        this.ignoreSsl = properties.isIgnoreSsl();
    }

    public void exportAndSendAvroMessages(String keyPayloadFile, String valuePayloadFile, String schemaRegistry, String topicName, String restProxyUrl, String urlPrefix)
            throws ErrorMessageException {
        log.info("DevUK109TargetComponent loaded. Ready to execute Avro message sending API.");
        log.info("File key: {}", keyPayloadFile);
        log.info("File value: {}", valuePayloadFile);
        log.info("Schema Registry: {}", schemaRegistry);
        log.info("Topic name  : {}", topicName);
        log.info("REST Proxy  : {}", restProxyUrl);
        log.info("URL Prefix  : {}", urlPrefix);

        sendAvroMessageWithSchema(keyPayloadFile, valuePayloadFile, schemaRegistry, topicName, restProxyUrl, urlPrefix);
    }

    private void sendAvroMessageWithSchema(String keyFilePath, String valueFilePath, String schemaRegistry, String topicName, String restProxyUrl, String urlPrefix)
            throws ErrorMessageException {
        String fullRestProxyUrl = restProxyUrl + "/" + topicName;

        JsonNode keyPayload = readJsonFile(keyFilePath, "KEY");
        JsonNode valuePayload = readJsonFile(valueFilePath, "VALUE");

        String keySchema = schemaRegistryDownloader.getLatestSchema(schemaRegistry, topicName + "-key", urlPrefix);
        log.info("KEY Schema downloaded:\n{}", keySchema);

        String valueSchema = schemaRegistryDownloader.getLatestSchema(schemaRegistry, topicName + "-value", urlPrefix);
        log.info("VALUE Schema downloaded:\n{}", valueSchema);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("key_schema", keySchema);
        payload.put("value_schema", valueSchema);

        ArrayNode records = mapper.createArrayNode();
        ObjectNode recordNode = mapper.createObjectNode();
        recordNode.set("key", keyPayload);
        recordNode.set("value", valuePayload);
        records.add(recordNode);

        payload.set("records", records);

        try {
            String jsonPayload = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
            log.info("Payload sent to REST Proxy:\n{}", jsonPayload);

            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
            if (ignoreSsl) {
                httpClientBuilder.sslContext(createInsecureSslContext());
            }
            HttpClient httpClient = httpClientBuilder.build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(fullRestProxyUrl))
                    .header("Content-Type", "application/vnd.kafka.avro.v2+json")
                    .header("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
                    .build();

            log.info("Sending to REST Proxy: {}", fullRestProxyUrl);

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                log.info("Message sent successfully");
                log.info("Response ({}): {}", response.statusCode(), response.body());
            } else {
                log.error("Error sending message. Code {}", response.statusCode());
                log.error("Response: {}", response.body());
                throw new ErrorMessageException("REST Proxy error: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            log.error("Error sending message", e);
            throw new ErrorMessageException("Error sending REST Proxy request: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new ErrorMessageException("Unexpected error: " + e.getMessage());
        }
    }

    private JsonNode readJsonFile(String filePath, String type) throws ErrorMessageException {
        log.info("Loading {} from: {}", type, filePath);
        try {
            return mapper.readTree(new File(filePath));
        } catch (IOException e) {
            log.error("Error loading {} payload", type, e);
            throw new ErrorMessageException("Error loading " + type + " payload: " + e.getMessage());
        }
    }

    /**
     * FOR TESTING ONLY: Creates an SSLContext that does not validate certificates.
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
        log.info("SSLContext without certificate validation (TEST)");
        return ctx;
    }
}
