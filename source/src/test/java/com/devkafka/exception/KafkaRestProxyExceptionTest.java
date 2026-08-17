package com.devkafka.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KafkaRestProxyExceptionTest {

    @Test
    void testConstructorStoresMessage() {
        KafkaRestProxyException exception = new KafkaRestProxyException("Test error");
        assertEquals("Test error", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorStoresMessageAndCause() {
        Exception cause = new IllegalStateException("root cause");
        KafkaRestProxyException exception = new KafkaRestProxyException("Test error", cause);

        assertEquals("Test error", exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testIsDevKafkaException() {
        KafkaRestProxyException exception = new KafkaRestProxyException("Message");
        assertTrue(exception instanceof DevKafkaException);
        assertTrue(exception instanceof RuntimeException);
    }
}
