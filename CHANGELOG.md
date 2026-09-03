# Changelog

All notable changes to `devtools-kafka` are documented in this file.

The project follows semantic versioning for the library API.

## 0.3.1-SNAPSHOT

### Documentation and consistency

- Standardized repository documentation and user-facing text in English.
- Expanded the README with configuration, schema resolution, dummy generation, publishing, runtime environment selection, low-level API and error-handling documentation.
- Clarified that schemas are downloaded into memory and are not automatically persisted as `.avsc` files.
- Clarified the `topic-key` / `topic-value` subject convention and `/versions/latest` default schema path.
- Clarified the supported scope of the specialized library.

### Runtime behavior

- No intentional breaking runtime changes from 0.3.0.
- `KafkaAvroClient`, Schema Registry access, Avro dummy generation and Kafka REST Proxy publishing remain unchanged.

## 0.3.0-SNAPSHOT

### Added

- Unified Spring Boot configuration through `DevKafkaProperties` and the `devkafka.*` namespace.
- Global `devkafka.enabled` auto-configuration switch.
- High-level `KafkaAvroClient` workflow for schema retrieval, dummy generation and publishing.
- Runtime URL overloads for dynamically selected environments.

### Changed

- Kafka REST Proxy publishing accepts any successful HTTP `2xx` response.
- SSL configuration is scoped to each HTTP client instead of modifying JVM-wide defaults.

### Removed

- Legacy `SchemaRegistryProperties` configuration.
- Legacy constructors based on `SchemaRegistryProperties`.
- `GenericRunnerBean` and `SchemaDownload` legacy orchestration.
- Cucumber scenarios, demo application, local Kafka infrastructure and Kubernetes manifests from the library repository.
- Unused Spring Kafka, Confluent serializer/client, MapStruct, Commons IO, Lombok and related dependencies.

## 0.2.0-SNAPSHOT

### Added

- `KafkaAvroClient` facade.
- `KafkaAvroMessage` and `KafkaAvroSchemas` typed API values.
- `AvroPayloadException`.
- `DevKafkaProperties` configuration.

### Changed

- Schema Registry and Kafka REST Proxy clients use `java.net.http.HttpClient`.
- The repository was reduced to reusable library code and unit tests.

## 0.1.0-SNAPSHOT

Initial library version with Schema Registry access, Avro dummy generation, Avro-to-JSON conversion and Kafka REST Proxy publishing capabilities.
