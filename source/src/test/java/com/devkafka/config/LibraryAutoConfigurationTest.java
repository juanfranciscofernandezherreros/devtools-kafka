package com.devkafka.config;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.runner.GenericRunnerBean;
import com.devkafka.runner.SchemaDownload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Pure unit test (without Spring context) for LibraryAutoConfiguration.
 * All dependencies are mocked.
 */
class LibraryAutoConfigurationTest {

    private LibraryAutoConfiguration config;
    private SchemaRegistryProperties properties;

    @Mock
    private RestTemplateBuilder restTemplateBuilder;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private SchemaRegistryClientService mockSchemaRegistryClientService;

    @Mock
    private KafkaRestProxyClientService mockKafkaRestProxyClientService;

    @Mock
    private SchemaDownload mockSchemaDownload;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        config = new LibraryAutoConfiguration();
        properties = new SchemaRegistryProperties();
        when(restTemplateBuilder.build()).thenReturn(restTemplate);
    }

    @Test
    @DisplayName("Creates a RestTemplate using the provided RestTemplateBuilder")
    void testLibreriaRestTemplate() {
        RestTemplate result = config.libreriaRestTemplate(restTemplateBuilder);
        assertNotNull(result);
        verify(restTemplateBuilder, times(1)).build();
    }

    @Test
    @DisplayName("Creates a SchemaRegistryClientService correctly")
    void testSchemaRegistryClientService() {
        SchemaRegistryClientService service = config.schemaRegistryClientService(properties);
        assertNotNull(service);
        assertInstanceOf(SchemaRegistryClientService.class, service);
    }

    @Test
    @DisplayName("Creates a KafkaRestProxyClientService correctly")
    void testKafkaRestProxyClientService() {
        KafkaRestProxyClientService client = config.kafkaRestProxyClientService(properties);
        assertNotNull(client);
        assertInstanceOf(KafkaRestProxyClientService.class, client);
    }

    @Test
    @DisplayName("Creates SchemaDownload with registry and REST Proxy clients")
    void testSchemaDownload() {
        SchemaDownload download = config.schemaDownload(
                mockSchemaRegistryClientService,
                mockKafkaRestProxyClientService);
        assertNotNull(download);
        assertInstanceOf(SchemaDownload.class, download);
    }

    @Test
    @DisplayName("Creates a GenericRunnerBean injecting SchemaDownload")
    void testGenericRunnerBean() {
        GenericRunnerBean runner = config.genericRunnerBean(mockSchemaDownload);
        assertNotNull(runner);
        assertInstanceOf(GenericRunnerBean.class, runner);
    }
}
