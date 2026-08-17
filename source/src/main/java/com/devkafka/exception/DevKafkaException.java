package com.devkafka.exception;

/**
 * Base type for every unchecked exception raised by this library, so
 * consumers can choose to catch either a specific failure or any of them
 * with a single {@code catch (DevKafkaException e)}.
 */
public abstract class DevKafkaException extends RuntimeException {

    protected DevKafkaException(String message) {
        super(message);
    }

    protected DevKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
