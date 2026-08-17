package com.devkafka.demo;

import com.devkafka.avro.AvroDummyFiller;
import com.devkafka.avro.AvroJsonConverter;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.runner.GenericRunnerBean;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
            String keyFile = properties.getKeyPayloadFile();
            String valueFile = properties.getValuePayloadFile();

            if (properties.isAutoGenerateSample()) {
                Path[] generated = generateSampleFiles();
                keyFile = generated[0].toString();
                valueFile = generated[1].toString();
            }

            genericRunnerBean.run(
                    keyFile,
                    valueFile,
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

    /**
     * Downloads the key/value schemas for topicName (saving each as a
     * .avsc file), generates a random sample matching each one, and writes
     * the samples to disk too, so all three artifacts (schema, generated
     * value, and the send itself) are visible from a single run.
     *
     * @return {keyFile, valueFile}
     */
    private Path[] generateSampleFiles() throws Exception {
        String topicName = properties.getTopicName();
        Path schemaDir = Path.of(properties.getSchemaOutputDir());
        Path sampleDir = schemaDir.resolve("generated");
        Files.createDirectories(schemaDir);
        Files.createDirectories(sampleDir);

        Path keyFile = generateSampleFile(topicName + "-key", schemaDir, sampleDir);
        Path valueFile = generateSampleFile(topicName + "-value", schemaDir, sampleDir);

        return new Path[]{keyFile, valueFile};
    }

    private Path generateSampleFile(String subject, Path schemaDir, Path sampleDir) throws Exception {
        String schemaJson = schemaRegistryClient.getLatestSchema(schemaRegistryUrl, subject, properties.getSchemaUrlSuffix());

        Path schemaFile = writeSchemaFile(subject, schemaJson, schemaDir);
        log.info("Downloaded schema for [{}] -> {}", subject, schemaFile.toAbsolutePath());

        Schema schema = new Schema.Parser().parse(schemaJson);
        Object dummyValue = AvroDummyFiller.generateDummyValue(schema);
        String sampleJson = AvroJsonConverter.toJson(dummyValue, schema);

        Path sampleFile = sampleDir.resolve(subject + "-sample.json");
        Files.writeString(sampleFile, sampleJson);
        log.info("Generated sample for [{}] -> {}", subject, sampleFile.toAbsolutePath());
        return sampleFile;
    }

    private void downloadSchemas() throws Exception {
        Path outputDir = Path.of(properties.getSchemaOutputDir());
        Files.createDirectories(outputDir);

        List<String> subjects;
        if (properties.isDownloadAllSchemas()) {
            subjects = schemaRegistryClient.listSubjects(schemaRegistryUrl);
            log.info("Found {} subjects in the Schema Registry", subjects.size());
        } else {
            String topicName = properties.getTopicName();
            subjects = List.of(topicName + "-key", topicName + "-value");
        }

        for (String subject : subjects) {
            String schemaJson = schemaRegistryClient.getLatestSchema(schemaRegistryUrl, subject, properties.getSchemaUrlSuffix());
            Path file = writeSchemaFile(subject, schemaJson, outputDir);
            log.info("Downloaded schema for [{}] -> {}", subject, file.toAbsolutePath());
        }
    }

    private Path writeSchemaFile(String subject, String schemaJson, Path dir) throws Exception {
        Path file = dir.resolve(subject + ".avsc");
        Files.writeString(file, prettyPrint(schemaJson) + System.lineSeparator());
        return file;
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
