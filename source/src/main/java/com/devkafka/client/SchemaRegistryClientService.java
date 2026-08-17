package com.devkafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.SchemaRegistryException;

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
import java.util.Arrays;
import java.util.List;

/**
 * Client for the Schema Registry. Certificate validation is enabled by
 * default; it's only skipped when {@code library.schema.ignore-ssl=true}
 * (local/dev environments with self-signed certs).
 */
@Service
@Slf4j
public class SchemaRegistryClientService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final boolean ignoreSsl;

    public SchemaRegistryClientService(SchemaRegistryProperties properties) {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
        this.ignoreSsl = properties.isIgnoreSsl();
    }

    /**
     * Downloads the latest schema of a subject.
     *
     * @param subject Registry subject.
     * @return Schema as a String (JSON raw).
     */
    public String getLatestSchema(String schemaRegistry, String subject, String urlPrefix) {
        if (ignoreSsl) {
            disableSSLVerification();
        }

        String url = schemaRegistry + subject + urlPrefix;
        log.info("Searching for schema at URL: " + url);

        try {
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
            JsonNode root = objectMapper.readTree(response.getBody());

            String schema = root.get("schema").asText();
            log.info("Schema obtained for [" + subject + "]");
            return schema;

        } catch (Exception e) {
            log.error("Error obtaining schema [" + subject + "]: " + e.getMessage());
            throw new SchemaRegistryException("Schema registry fetch failed", e);
        }
    }

    /**
     * Lists all published version numbers of a subject.
     *
     * @param subject Registry subject.
     * @return version numbers, oldest first.
     */
    public List<Integer> listVersions(String schemaRegistry, String subject) {
        if (ignoreSsl) {
            disableSSLVerification();
        }

        String url = schemaRegistry + subject + "/versions";
        log.info("Listing versions at URL: " + url);

        try {
            ResponseEntity<Integer[]> response = restTemplate.getForEntity(url, Integer[].class);
            return Arrays.asList(response.getBody());

        } catch (Exception e) {
            log.error("Error listing versions [" + subject + "]: " + e.getMessage());
            throw new SchemaRegistryException("Schema registry list-versions failed", e);
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

            log.info("SSL verification disabled (TEST)");
        } catch (Exception e) {
            log.error("Error disabling SSL verification: " + e.getMessage());
        }
    }
}
