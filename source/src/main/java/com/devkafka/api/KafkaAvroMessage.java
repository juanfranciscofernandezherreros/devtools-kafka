package com.devkafka.api;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Key/value payload pair ready to be sent through the Kafka REST Proxy.
 */
public record KafkaAvroMessage(JsonNode key, JsonNode value) {
}
