package com.devkafka.runner;

import com.devkafka.exception.ErrorMessageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GenericRunnerBeanTest {

    private SchemaDownload schemaDownload;
    private GenericRunnerBean runner;

    @BeforeEach
    void setUp() {
        // Create a mock of SchemaDownload
        schemaDownload = Mockito.mock(SchemaDownload.class);
        runner = new GenericRunnerBean(schemaDownload);
    }

    @Test
    @DisplayName("Executes successfully without throwing exceptions")
    void testRunSuccessful() throws Exception {
        // Arrange
        String keyFile = "key.json";
        String valueFile = "value.json";
        String schemaRegistry = "https://schema-registry.dev";
        String topic = "test-topic";
        String confluentic = "https://rest-proxy.dev/topics";
        String prefix = "https://schemas/";

        // Act
        runner.run(keyFile, valueFile, schemaRegistry, topic, confluentic, prefix);

        // Assert
        verify(schemaDownload, times(1))
                .exportAndSendAvroMessages(keyFile, valueFile, schemaRegistry, topic, confluentic, prefix);
    }

    @Test
    @DisplayName("Propagates ErrorMessageException if SchemaDownload fails")
    void testRunThrowsErrorMessageException() throws Exception {
        // Arrange
        doThrow(new ErrorMessageException("Simulated error"))
                .when(schemaDownload)
                .exportAndSendAvroMessages(any(), any(), any(), any(), any(), any());

        // Act + Assert
        assertThrows(ErrorMessageException.class, () -> {
            runner.run("k", "v", "sr", "topic", "proxy", "prefix");
        });

        verify(schemaDownload, times(1))
                .exportAndSendAvroMessages(any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Correctly measures execution time (no failure)")
    void testRunExecutionTimeLogged() throws Exception {
        // No log assertions, but we execute to check that it does not fail
        runner.run("key", "value", "sr", "topic", "proxy", "prefix");

        // The method should have been invoked once
        verify(schemaDownload, times(1))
                .exportAndSendAvroMessages(any(), any(), any(), any(), any(), any());
    }
}
