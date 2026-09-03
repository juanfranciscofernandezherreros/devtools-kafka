# devtools-kafka

`devtools-kafka` is a Java 17 / Spring Boot library specialized in **Kafka + Avro + Schema Registry + Kafka REST Proxy testing**.

The library is intentionally focused. It does not try to replace Spring Kafka or provide a full Kafka client framework. Its purpose is to make test and tooling workflows around Avro topics simple and reusable.

## Version 0.3.1

Version `0.3.1-SNAPSHOT` is a documentation and consistency release on top of the 0.3.x API.

Highlights:

- the entire repository documentation and user-facing text is standardized in English;
- `KafkaAvroClient` remains the recommended high-level API;
- `DevKafkaProperties` remains the single Spring Boot configuration surface;
- key and value schemas are still downloaded from Schema Registry using the `topic-key` and `topic-value` subject convention;
- dummy Avro payload generation remains available;
- existing JSON payloads can be sent through Kafka REST Proxy using the latest schemas;
- runtime-selected environments remain supported through explicit URL overloads;
- Spring Boot auto-configuration can be disabled with `devkafka.enabled=false`.

See [CHANGELOG.md](CHANGELOG.md) for the version history.

## Maven dependency

```xml
<dependency>
  <groupId>com.devkafka</groupId>
  <artifactId>rft-devtools-kafka-cucumber</artifactId>
  <version>0.3.1-SNAPSHOT</version>
</dependency>
```

Build and install locally:

```bash
mvn -f source/pom.xml clean test
mvn -f source/pom.xml clean install
```

## Spring Boot configuration

```yaml
devkafka:
  enabled: true
  schema-registry-url: https://registry/apis/ccompat/v7/subjects
  rest-proxy-url: https://rest-proxy/topics
  schema-version-path: /versions/latest
  ignore-ssl: false
```

### Configuration properties

| Property | Default | Purpose |
| --- | --- | --- |
| `devkafka.enabled` | `true` | Enables or disables the library auto-configuration. |
| `devkafka.schema-registry-url` | none | Base URL of the Confluent-compatible Schema Registry subjects API. |
| `devkafka.rest-proxy-url` | none | Base Kafka REST Proxy topics URL. |
| `devkafka.schema-version-path` | `/versions/latest` | Schema version suffix used when resolving subjects. |
| `devkafka.ignore-ssl` | `false` | Disables TLS certificate and hostname validation for controlled test environments. |

SSL verification should only be disabled in controlled development or test environments.

## Recommended API: `KafkaAvroClient`

Inject the facade in a Spring Boot application:

```java
@Autowired
private KafkaAvroClient kafka;
```

### Download the current key and value schemas

```java
KafkaAvroSchemas schemas = kafka.getSchemas("my-topic");
```

For the topic `my-topic`, the library requests:

```text
my-topic-key
my-topic-value
```

using the configured schema version path, which defaults to:

```text
/versions/latest
```

The schemas are returned in memory as a `KafkaAvroSchemas` value. The library does not automatically write `.avsc` files to disk; file persistence belongs to the consuming test or tooling project.

### Generate a valid dummy Avro message

```java
KafkaAvroMessage message = kafka.generateDummy("my-topic");
```

This operation:

1. downloads the latest key schema;
2. downloads the latest value schema;
3. parses both Avro schemas;
4. generates compatible dummy values;
5. converts them to JSON-compatible `JsonNode` payloads.

### Send an existing message

With Jackson nodes:

```java
kafka.send("my-topic", keyJsonNode, valueJsonNode);
```

Or with JSON strings:

```java
kafka.send("my-topic", keyJson, valueJson);
```

Before publishing, the library downloads the current key and value schemas and sends the message through Kafka REST Proxy using the Avro REST Proxy media type.

### Generate and send in one operation

```java
KafkaAvroMessage sent = kafka.generateAndSend("my-topic");
```

This combines schema download, dummy generation and REST Proxy publishing.

## Runtime-selected environments

Test suites that select environments such as DEV, INT or QA at runtime do not need fixed Spring properties. Use the explicit overloads:

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

This keeps environment selection in the consuming test project while the library remains environment-agnostic.

## Low-level API

The low-level services remain available when a consumer needs direct control:

- `SchemaRegistryClientService`
- `KafkaRestProxyClientService`
- `AvroDummyFiller`
- `AvroJsonConverter`

Example:

```java
SchemaRegistryClientService registry = new SchemaRegistryClientService(false);
KafkaRestProxyClientService restProxy = new KafkaRestProxyClientService(false);
```

The boolean constructor argument controls whether SSL verification is ignored.

## Schema Registry capabilities

`SchemaRegistryClientService` supports:

- downloading the latest schema for a subject;
- listing subjects;
- listing versions for a subject.

It targets Confluent-compatible Schema Registry APIs, including compatible Apicurio endpoints.

## Kafka REST Proxy capabilities

`KafkaRestProxyClientService` supports:

- listing topics;
- publishing Avro key/value messages;
- accepting successful HTTP `2xx` publication responses.

## Avro capabilities

`AvroDummyFiller` supports dummy generation for common Avro schema types, including:

- records;
- strings;
- integers and longs;
- floats and doubles;
- booleans;
- enums;
- arrays;
- maps;
- unions;
- null values.

`AvroJsonConverter` converts generated Avro values, including primitive key values, to Avro-compatible JSON.

## Error handling

The library exposes domain-specific unchecked exceptions:

- `DevKafkaException` as the base exception;
- `SchemaRegistryException` for Schema Registry failures;
- `KafkaRestProxyException` for Kafka REST Proxy failures;
- `AvroPayloadException` for Avro payload generation or JSON conversion failures.

## Project scope

The repository intentionally does **not** contain:

- Kafka brokers;
- Docker Compose infrastructure;
- Kubernetes manifests;
- demo applications;
- Cucumber feature files or step definitions;
- Spring Kafka producers or consumers;
- Confluent Java serializers.

Those concerns belong in applications or test projects that consume this library.

## Compatibility notes

The 0.3.x line uses the unified `devkafka.*` configuration namespace. Legacy `library.schema.*`, `SchemaRegistryProperties`, `GenericRunnerBean` and `SchemaDownload` are not part of the 0.3.x public API.

For new integrations, use `KafkaAvroClient` unless direct access to a low-level service is specifically required.
