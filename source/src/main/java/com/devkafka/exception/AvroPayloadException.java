package com.devkafka.exception;

/**
 * Raised when an Avro payload cannot be generated or converted to JSON.
 */
public class AvroPayloadException extends DevKafkaException {

    public AvroPayloadException(String message) {
        super(message);
    }

    public AvroPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
