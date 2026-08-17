package com.devkafka.demo;

import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.runner.GenericRunnerBean;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;

@Component
@Slf4j
public class KafkaDemoRunner implements CommandLineRunner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final GenericRunnerBean genericRunnerBean;
    private final KafkaRestProxyClientService restProxyClient;
    private final SchemaRegistryClientService schemaRegistryClient;
    private final DemoKafkaProperties properties;
    private final String schemaRegistryUrl;

    public KafkaDemoRunner(GenericRunnerBean genericRunnerBean,
                            KafkaRestProxyClientService restProxyClient,
                            SchemaRegistryClientService schemaRegistryClient,
                            DemoKafkaProperties properties,
                            @Value("${library.schema.registry-url}") String schemaRegistryUrl) {
        this.genericRunnerBean = genericRunnerBean;
        this.restProxyClient = restProxyClient;
        this.schemaRegistryClient = schemaRegistryClient;
        this.properties = properties;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Existing REST Proxy topics: {}", restProxyClient.listTopics(properties.getRestProxyUrl()));

        if (properties.isSendSampleMessage()) {
            genericRunnerBean.run(
                    properties.getKeyPayloadFile(),
                    properties.getValuePayloadFile(),
                    schemaRegistryUrl,
                    properties.getTopicName(),
                    properties.getRestProxyUrl(),
                    properties.getSchemaUrlSuffix()
            );
            return;
        }

        if (properties.isDownloadSchemaOnly()) {
            downloadSchemas();
            return;
        }

        log.info("app.kafka.send-sample-message=false and app.kafka.download-schema-only=false, nothing else to do.");
    }

    private void downloadSchemas() throws Exception {
        String topicName = properties.getTopicName();
        Path outputDir = Path.of(properties.getSchemaOutputDir());
        Files.createDirectories(outputDir);

        for (String suffix : new String[]{"key", "value"}) {
            String subject = topicName + "-" + suffix;
            String schema = schemaRegistryClient.getLatestSchema(schemaRegistryUrl, subject, properties.getSchemaUrlSuffix());

            Path file = outputDir.resolve(subject + ".avsc");
            Files.writeString(file, prettyPrint(schema) + System.lineSeparator());
            log.info("Downloaded schema for [{}] -> {}", subject, file.toAbsolutePath());
        }
    }

    private static String prettyPrint(String schema) {
        try {
            JsonNode node = MAPPER.readTree(schema);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (Exception e) {
            return schema;
        }
    }
}
