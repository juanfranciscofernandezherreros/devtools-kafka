# rft-devtools-kafka-cucumber

Módulo Maven de la librería. Ver el [README de la raíz del repositorio](../README.md)
para la infraestructura Docker local y las properties de ejemplo.

## Requisitos

- Java 17+
- Maven 3.8+

## Build

```bash
mvn clean install
```

## Paquetes

```
src/main/java/com/devkafka/
├── config/      LibraryAutoConfiguration, SchemaRegistryProperties (library.schema.*)
├── client/      SchemaRegistryClientService, KafkaRestProxyClientService
├── runner/      SchemaDownload, GenericRunnerBean
├── avro/        AvroDummyFiller, AvroJsonConverter
└── exception/   ErrorMessageException
```

Los tests replican la misma estructura de paquetes bajo `src/test/java`.

## Dependencias externas relevantes

- Spring Framework / Spring Boot AutoConfiguration
- Spring Kafka
- Confluent Schema Registry Client / Avro Serializer (repositorio `packages.confluent.io`)
- Lombok, MapStruct (procesadores de anotaciones)
