package com.devkafka.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.ErrorMessageException;
import com.devkafka.exception.KafkaRestProxyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
@Slf4j
public class SchemaDownload {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final SchemaRegistryClientService schemaRegistryDownloader;
    private final KafkaRestProxyClientService kafkaRestProxyClientService;

    public SchemaDownload(
            SchemaRegistryClientService schemaRegistryDownloader,
            KafkaRestProxyClientService kafkaRestProxyClientService) {
        this.schemaRegistryDownloader = schemaRegistryDownloader;
        this.kafkaRestProxyClientService = kafkaRestProxyClientService;
    }

    /**
     * Backwards-compatible constructor for direct usages outside Spring.
     * Prefer dependency injection through the two-client constructor.
     */
    public SchemaDownload(
            SchemaRegistryClientService schemaRegistryDownloader,
            SchemaRegistryProperties properties) {
        this(schemaRegistryDownloader, new KafkaRestProxyClientService(properties));
    }

    public void exportAndSendAvroMessages(
            String keyPayloadFile,
            String valuePayloadFile,
            String schemaRegistry,
            String topicName,
            String restProxyUrl,
            String urlPrefix) throws ErrorMessageException {

        log.info("Preparing Avro message for topic {}", topicName);
        log.debug("KEY payload file: {}", keyPayloadFile);
        log.debug("VALUE payload file: {}", valuePayloadFile);

        JsonNode keyPayload = readJsonFile(keyPayloadFile, "KEY");
        JsonNode valuePayload = readJsonFile(valuePayloadFile, "VALUE");

        String keySchema = schemaRegistryDownloader.getLatestSchema(
                schemaRegistry, topicName + "-key", urlPrefix);
        String valueSchema = schemaRegistryDownloader.getLatestSchema(
                schemaRegistry, topicName + "-value", urlPrefix);

        log.debug("KEY schema downloaded for topic {}", topicName);
        log.debug("VALUE schema downloaded for topic {}", topicName);

        try {
            kafkaRestProxyClientService.sendAvroMessage(
                    restProxyUrl,
                    topicName,
                    keySchema,
                    valueSchema,
                    keyPayload,
                    valuePayload);
        } catch (KafkaRestProxyException e) {
            log.error("Error sending message to REST Proxy for topic {}", topicName, e);
            throw new ErrorMessageException("REST Proxy error: " + e.getMessage());
        }
    }

    private JsonNode readJsonFile(String filePath, String type) throws ErrorMessageException {
        log.debug("Loading {} payload from {}", type, filePath);
        try {
            return mapper.readTree(new File(filePath));
        } catch (IOException e) {
            log.error("Error loading {} payload", type, e);
            throw new ErrorMessageException("Error loading " + type + " payload: " + e.getMessage());
        }
    }
}
