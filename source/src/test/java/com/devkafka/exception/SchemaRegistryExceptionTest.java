package com.devkafka.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryExceptionTest {

    @Test
    void testConstructorStoresMessage() {
        SchemaRegistryException exception = new SchemaRegistryException("Test error");
        assertEquals("Test error", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorStoresMessageAndCause() {
        Exception cause = new IllegalStateException("root cause");
        SchemaRegistryException exception = new SchemaRegistryException("Test error", cause);

        assertEquals("Test error", exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testIsDevKafkaException() {
        SchemaRegistryException exception = new SchemaRegistryException("Message");
        assertTrue(exception instanceof DevKafkaException);
        assertTrue(exception instanceof RuntimeException);
    }
}
