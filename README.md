# devtools-kafka

Java 17 / Spring Boot library specialized in **Kafka + Avro + Schema Registry + Kafka REST Proxy testing**.

## 0.3.0

0.3.0 consolidates the public API introduced in 0.2.0 and removes the remaining legacy configuration surface.

- `KafkaAvroClient` is the recommended high-level API.
- `DevKafkaProperties` is the only Spring Boot configuration class.
- `devkafka.enabled` controls the whole auto-configuration and defaults to `true`.
- `SchemaRegistryProperties` and its legacy client constructors were removed.
- REST Proxy publishing accepts any successful HTTP 2xx response.

## Maven coordinates

```xml
<dependency>
  <groupId>com.devkafka</groupId>
  <artifactId>rft-devtools-kafka-cucumber</artifactId>
  <version>0.3.0-SNAPSHOT</version>
</dependency>
```

Build locally:

```bash
mvn -f source/pom.xml clean install
```

## Configuration

```yaml
devkafka:
  enabled: true
  schema-registry-url: https://registry/apis/ccompat/v7/subjects
  rest-proxy-url: https://rest-proxy/topics
  schema-version-path: /versions/latest
  ignore-ssl: false
```

Set `devkafka.enabled=false` to disable all library auto-configuration.

SSL verification is enabled by default. Disable it only in controlled test environments.

## Recommended API

```java
@Autowired
private KafkaAvroClient kafka;
```

Generate a valid dummy key/value pair from the current schemas:

```java
KafkaAvroMessage message = kafka.generateDummy("my-topic");
```

Send existing JSON payloads using the current key/value schemas:

```java
kafka.send("my-topic", keyJsonNode, valueJsonNode);
// or
kafka.send("my-topic", keyJson, valueJson);
```

Generate and send in one operation:

```java
KafkaAvroMessage sent = kafka.generateAndSend("my-topic");
```

Read schemas:

```java
KafkaAvroSchemas schemas = kafka.getSchemas("my-topic");
```

For runtime-selected DEV/INT/QA environments, use the explicit overloads:

```java
KafkaAvroMessage message = kafka.generateDummy(topic, schemaRegistryUrl, "/versions/latest");

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

These utilities remain available when a consumer needs more control:

- `SchemaRegistryClientService`
- `KafkaRestProxyClientService`
- `AvroDummyFiller`
- `AvroJsonConverter`

Direct clients are constructed explicitly with the SSL policy:

```java
SchemaRegistryClientService registry = new SchemaRegistryClientService(false);
KafkaRestProxyClientService restProxy = new KafkaRestProxyClientService(false);
```

## Scope

The library intentionally does **not** include Kafka brokers, Docker Compose, Kubernetes manifests, demo applications, Cucumber step definitions, Spring Kafka producers/consumers, or Confluent Java serializers. Those concerns belong in consumer or test projects.
