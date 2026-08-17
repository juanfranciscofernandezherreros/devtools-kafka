package com.devkafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Client for the Schema Registry that ignores SSL (QA/DEV only).
 */
@Service
@Slf4j
public class SchemaRegistryClientService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public SchemaRegistryClientService() {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Downloads the latest schema of a subject.
     *
     * @param subject Registry subject.
     * @return Schema as a String (JSON raw).
     */
    public String getLatestSchema(String schemaRegistry, String subject, String urlPrefix) {
        disableSSLVerification();  // Avoid certificate issues

        String url = schemaRegistry + subject + urlPrefix;
        log.info("🌍 Searching for schema at URL: " + url);

        try {
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            JsonNode root = objectMapper.readTree(response.getBody());

            String schema = root.get("schema").asText();
            log.info("✅ Schema obtained for [" + subject + "]");
            return schema;

        } catch (Exception e) {
            log.error("❌ Error obtaining schema [" + subject + "]: " + e.getMessage());
            throw new RuntimeException("Schema registry fetch failed", e);
        }
    }

    /*───────────────────────────────────────────────────────────────*/
    /*  Disable SSL (TESTING only)                         */
    /*───────────────────────────────────────────────────────────────*/
    private static void disableSSLVerification() {
        try {
            TrustManager[] trustAll = new TrustManager[]{
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
            ctx.init(null, trustAll, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((h, s) -> true);

            log.info("⚠️  SSL verification disabled (TEST)");
        } catch (Exception e) {
            log.error("❌ Error disabling SSL verification: " + e.getMessage());
        }
    }
}
