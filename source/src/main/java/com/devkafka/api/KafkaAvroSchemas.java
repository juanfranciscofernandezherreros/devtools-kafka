package com.devkafka.api;

/**
 * Key/value Avro schemas for a Kafka topic.
 */
public record KafkaAvroSchemas(String keySchema, String valueSchema) {
}
