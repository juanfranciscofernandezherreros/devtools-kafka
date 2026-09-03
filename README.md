# devtools-kafka

Java 17 / Spring Boot library specialized in Kafka + Avro + Schema Registry + Kafka REST Proxy testing.

## What the library does

- Downloads key/value schemas from a Confluent-compatible Schema Registry / Apicurio API.
- Generates valid dummy Avro key/value payloads.
- Converts Avro values to JSON.
- Lists Kafka topics through Kafka REST Proxy.
- Publishes Avro key/value messages through Kafka REST Proxy.
- Provides a high-level `KafkaAvroClient` facade so consumers do not need to coordinate the low-level services themselves.
- Provides Spring Boot auto-configuration.

The repository intentionally contains only reusable library code and unit tests. Demo applications, Cucumber scenarios and Kafka/Kubernetes infrastructure belong in consumer/test repositories.

## Maven coordinates

```xml
<dependency>
  <groupId>com.devkafka</groupId>
  <artifactId>rft-devtools-kafka-cucumber</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
```

Build and install locally:

```bash
mvn -f source/pom.xml clean install
```

## Recommended API: KafkaAvroClient

Configure Spring Boot once:

```yaml
devkafka:
  schema-registry-url: https://registry/apis/ccompat/v7/subjects
  rest-proxy-url: https://rest-proxy/topics
  schema-version-path: /versions/latest
  ignore-ssl: false
```

Then inject the facade:

```java
@Autowired
private KafkaAvroClient kafka;
```

### Generate a valid dummy message

```java
KafkaAvroMessage message = kafka.generateDummy("my-topic");
```

The library automatically downloads:

- `my-topic-key`
- `my-topic-value`

and creates a JSON-compatible Avro key/value pair.

### Send an existing message

```java
kafka.send("my-topic", keyJsonNode, valueJsonNode);
```

Or from JSON strings:

```java
kafka.send("my-topic", keyJson, valueJson);
```

The current key/value schemas are downloaded before publication.

### Generate and send in one operation

```java
KafkaAvroMessage sent = kafka.generateAndSend("my-topic");
```

### Read the schemas

```java
KafkaAvroSchemas schemas = kafka.getSchemas("my-topic");
```

### Runtime-selected environments

Tools and test suites that select DEV/INT/QA at runtime can use the explicit overloads instead of fixed Spring properties:

```java
KafkaAvroMessage message = kafka.generateDummy(
    topic,
    schemaRegistryUrl,
    "/versions/latest"
);

kafka.send(
    topic,
    message.key(),
    message.value(),
    schemaRegistryUrl,
    restProxyUrl,
    "/versions/latest"
);
```

## Low-level API

The following types remain public for advanced use cases and backwards compatibility:

- `SchemaRegistryClientService`
- `KafkaRestProxyClientService`
- `AvroDummyFiller`
- `AvroJsonConverter`
- `SchemaRegistryProperties` (legacy direct-client configuration)

The recommended entry point for new consumers is `KafkaAvroClient`.

## SSL

SSL certificate validation is enabled by default. Disable verification only in controlled development/test environments:

```yaml
devkafka:
  ignore-ssl: true
```

Legacy direct client construction using `SchemaRegistryProperties#setIgnoreSsl` is still supported.

## Scope

This library deliberately does **not** include:

- Kafka brokers or Docker Compose
- Kubernetes manifests
- demo applications
- Cucumber features/step definitions
- Spring Kafka producer/consumer clients
- Confluent Java serializers

It is intentionally specialized in **Kafka + Avro + Schema Registry + REST Proxy testing**, rather than trying to be a universal Kafka framework.
