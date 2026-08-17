package com.devkafka.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class KafkaRestProxyClientService {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Lists all available topics from the REST Proxy.
     * Ignores SSL (QA/DEV only).
     */
    public List<String> listTopics(String restProxyUrl) {
        String endpoint = restProxyUrl.endsWith("/topics") ? restProxyUrl : restProxyUrl + "/topics";
        log.info("🌍 Requesting topic list from: {}", endpoint);

        HttpClient client;
        HttpRequest request;

        try {
            client = HttpClient.newBuilder()
                    .sslContext(createInsecureSslContext()) // 🔐 igual que en SchemaDownload
                    .build();

            request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json")
                    .GET()
                    .build();
        } catch (Exception e) {
            log.error("❌ Error configuring HttpClient/SSL: {}", e.getMessage());
            throw new KafkaRestProxyException("Error configuring HttpClient", e);
        }

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();

            if (status != 200) {
                log.error("⚠️ HTTP error {} when obtaining topics: {}", status, response.body());
                throw new KafkaRestProxyException("HTTP error " + status + ": " + response.body());
            }

            String body = response.body();
            log.info("✅ Topics JSON received: {}", body);

            return Arrays.asList(mapper.readValue(body, String[].class));

        } catch (IOException | InterruptedException e) {
            log.error("❌ Error obtaining topic list: {}", e.getMessage(), e);
            throw new KafkaRestProxyException("Error obtaining topic list", e);
        }
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
        log.info("🔐 SSLContext without certificate validation (DEV/QA only)");
        return ctx;
    }
}
