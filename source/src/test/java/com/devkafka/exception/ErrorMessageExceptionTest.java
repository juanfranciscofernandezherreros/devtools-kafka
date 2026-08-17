package com.devkafka.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ErrorMessageExceptionTest {

    @Test
    void testConstructorStoresMessage() {
        // Given
        String expectedMessage = "Test error";

        // When
        ErrorMessageException exception = new ErrorMessageException(expectedMessage);

        // Then
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void testIsRuntimeException() {
        ErrorMessageException exception = new ErrorMessageException("Message");
        assertTrue(exception instanceof RuntimeException);
    }
}
