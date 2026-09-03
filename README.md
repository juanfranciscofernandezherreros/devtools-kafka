# devtools-kafka

Minimal Java 17 / Spring Boot library for Kafka integration testing through Confluent-compatible HTTP APIs.

## What the library does

- Downloads schemas from Schema Registry / Apicurio using the Confluent compatibility API.
- Generates dummy Avro values from an Avro `Schema`.
- Converts generated Avro values to JSON.
- Lists Kafka topics through Kafka REST Proxy.
- Publishes Avro key/value messages through Kafka REST Proxy.
- Provides Spring Boot auto-configuration for the two HTTP clients.

The repository intentionally contains only library code and unit tests. Demo applications, Cucumber scenarios and Kafka/Kubernetes infrastructure belong in consumer/test repositories.

## Maven coordinates

```xml
<dependency>
  <groupId>com.devkafka</groupId>
  <artifactId>rft-devtools-kafka-cucumber</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Build and install locally:

```bash
mvn -f source/pom.xml clean install
```

## Main API

### Schema Registry

```java
SchemaRegistryProperties properties = new SchemaRegistryProperties();
properties.setIgnoreSsl(true); // DEV/QA only when required

SchemaRegistryClientService registry = new SchemaRegistryClientService(properties);
String schema = registry.getLatestSchema(
    "https://registry/apis/ccompat/v7/subjects/",
    "my-topic-value",
    "/versions/latest"
);
```

### Generate a dummy Avro payload

```java
Schema avroSchema = new Schema.Parser().parse(schema);
Object dummy = AvroDummyFiller.generateDummyValue(avroSchema);
String json = AvroJsonConverter.toJson(dummy, avroSchema);
```

### Publish through Kafka REST Proxy

```java
KafkaRestProxyClientService restProxy = new KafkaRestProxyClientService(properties);
restProxy.sendAvroMessage(
    "https://rest-proxy/topics",
    "my-topic",
    keySchema,
    valueSchema,
    keyJsonNode,
    valueJsonNode
);
```

## Spring Boot auto-configuration

When the JAR is on the classpath, Spring Boot can expose:

- `SchemaRegistryClientService`
- `KafkaRestProxyClientService`

SSL certificate validation is enabled by default. Disable it only for controlled development/test environments:

```yaml
library:
  schema:
    ignore-ssl: true
```

Optional bean switches:

```yaml
library:
  schema:
    enabled: true
  restproxy:
    enabled: true
```

## Scope

This library deliberately does **not** include:

- Kafka brokers or Docker Compose
- Kubernetes manifests
- demo applications
- Cucumber features/step definitions
- Spring Kafka producer/consumer clients
- Confluent Java serializers

Those concerns should live in the application or test project consuming this JAR.
