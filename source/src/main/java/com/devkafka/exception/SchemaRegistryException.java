package com.devkafka.exception;

/**
 * Raised when the Schema Registry cannot be reached or returns an
 * unexpected response.
 */
public class SchemaRegistryException extends DevKafkaException {

    public SchemaRegistryException(String message) {
        super(message);
    }

    public SchemaRegistryException(String message, Throwable cause) {
        super(message, cause);
    }
}
