package com.devkafka.exception;

public class ErrorMessageException extends RuntimeException {
    public ErrorMessageException(String message) {
        super(message);
    }
}
