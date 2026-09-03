package com.devkafka;

import com.devkafka.api.KafkaAvroMessage;
import com.devkafka.api.KafkaAvroSchemas;
import com.devkafka.avro.AvroDummyFiller;
import com.devkafka.avro.AvroJsonConverter;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.DevKafkaProperties;
import com.devkafka.exception.AvroPayloadException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * High-level facade for the library's main use case:
 * Schema Registry + Avro payload generation + Kafka REST Proxy publishing.
 */
public class KafkaAvroClient {

    private final SchemaRegistryClientService schemaRegistryClient;
    private final KafkaRestProxyClientService restProxyClient;
    private final DevKafkaProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaAvroClient(
            SchemaRegistryClientService schemaRegistryClient,
            KafkaRestProxyClientService restProxyClient,
            DevKafkaProperties properties) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.restProxyClient = restProxyClient;
        this.properties = properties;
    }

    public KafkaAvroSchemas getSchemas(String topic) {
        return getSchemas(topic, required(properties.getSchemaRegistryUrl(), "devkafka.schema-registry-url"),
                properties.getSchemaVersionPath());
    }

    public KafkaAvroSchemas getSchemas(String topic, String schemaRegistryUrl, String schemaVersionPath) {
        String keySchema = schemaRegistryClient.getLatestSchema(
                ensureTrailingSlash(schemaRegistryUrl), topic + "-key", schemaVersionPath);
        String valueSchema = schemaRegistryClient.getLatestSchema(
                ensureTrailingSlash(schemaRegistryUrl), topic + "-value", schemaVersionPath);
        return new KafkaAvroSchemas(keySchema, valueSchema);
    }

    public KafkaAvroMessage generateDummy(String topic) {
        return generateDummy(topic, required(properties.getSchemaRegistryUrl(), "devkafka.schema-registry-url"),
                properties.getSchemaVersionPath());
    }

    public KafkaAvroMessage generateDummy(String topic, String schemaRegistryUrl, String schemaVersionPath) {
        KafkaAvroSchemas schemas = getSchemas(topic, schemaRegistryUrl, schemaVersionPath);
        return generateDummy(schemas);
    }

    public KafkaAvroMessage generateDummy(KafkaAvroSchemas schemas) {
        try {
            Schema keySchema = new Schema.Parser().parse(schemas.keySchema());
            Schema valueSchema = new Schema.Parser().parse(schemas.valueSchema());

            Object keyValue = AvroDummyFiller.generateDummyValue(keySchema);
            Object valueValue = AvroDummyFiller.generateDummyValue(valueSchema);

            JsonNode key = objectMapper.readTree(AvroJsonConverter.toJson(keyValue, keySchema));
            JsonNode value = objectMapper.readTree(AvroJsonConverter.toJson(valueValue, valueSchema));
            return new KafkaAvroMessage(key, value);
        } catch (IOException | RuntimeException e) {
            throw new AvroPayloadException("Unable to generate Avro dummy payload", e);
        }
    }

    public void send(String topic, JsonNode key, JsonNode value) {
        send(topic, key, value,
                required(properties.getSchemaRegistryUrl(), "devkafka.schema-registry-url"),
                required(properties.getRestProxyUrl(), "devkafka.rest-proxy-url"),
                properties.getSchemaVersionPath());
    }

    public void send(String topic, String keyJson, String valueJson) {
        try {
            send(topic, objectMapper.readTree(keyJson), objectMapper.readTree(valueJson));
        } catch (IOException e) {
            throw new AvroPayloadException("Invalid JSON payload", e);
        }
    }

    public void send(
            String topic,
            JsonNode key,
            JsonNode value,
            String schemaRegistryUrl,
            String restProxyUrl,
            String schemaVersionPath) {
        KafkaAvroSchemas schemas = getSchemas(topic, schemaRegistryUrl, schemaVersionPath);
        restProxyClient.sendAvroMessage(
                restProxyUrl,
                topic,
                schemas.keySchema(),
                schemas.valueSchema(),
                key,
                value);
    }

    public KafkaAvroMessage generateAndSend(String topic) {
        KafkaAvroSchemas schemas = getSchemas(topic);
        KafkaAvroMessage message = generateDummy(schemas);
        restProxyClient.sendAvroMessage(
                required(properties.getRestProxyUrl(), "devkafka.rest-proxy-url"),
                topic,
                schemas.keySchema(),
                schemas.valueSchema(),
                message.key(),
                message.value());
        return message;
    }

    private static String required(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + propertyName);
        }
        return value;
    }

    private static String ensureTrailingSlash(String value) {
        return value.endsWith("/") ? value : value + "/";
    }
}
