package com.devkafka.client;

import com.devkafka.exception.DevKafkaException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.http.HttpClient;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

final class HttpClientFactory {

    private HttpClientFactory() {
    }

    static HttpClient create(boolean ignoreSsl) {
        try {
            HttpClient.Builder builder = HttpClient.newBuilder();
            if (ignoreSsl) {
                builder.sslContext(insecureSslContext());
            }
            return builder.build();
        } catch (Exception e) {
            throw new HttpClientConfigurationException("Unable to configure HTTP client", e);
        }
    }

    private static SSLContext insecureSslContext() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }
                }
        };

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustAll, new SecureRandom());
        return context;
    }

    private static final class HttpClientConfigurationException extends DevKafkaException {
        private HttpClientConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
