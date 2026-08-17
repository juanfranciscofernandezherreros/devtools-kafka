package com.devkafka.exception;

/**
 * Raised when the Kafka REST Proxy cannot be reached, is misconfigured, or
 * returns an unexpected response.
 */
public class KafkaRestProxyException extends DevKafkaException {

    public KafkaRestProxyException(String message) {
        super(message);
    }

    public KafkaRestProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
