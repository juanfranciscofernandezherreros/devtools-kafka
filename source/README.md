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

## Tests Cucumber (`com.devkafka.cucumber`)

`RunnerSteps.java` ejercita la librería (`KafkaRestProxyClientService`,
`SchemaRegistryClientService`, `SchemaDownload`, `GenericRunnerBean`) contra
un REST Proxy + Schema Registry reales — no reimplementa las llamadas HTTP.

**No se ejecutan** con `mvn test`/`mvn clean install` (el runner se llama
`RunCucumberIT`, no `*Test`, para que Surefire lo ignore por defecto y no
rompa el build cuando no hay Kafka levantado). Para correrlos:

```bash
cd ../local-dev && docker compose up -d && ./register-demo-schemas.sh && cd ../source
mvn test -Dtest=RunCucumberIT -Dsurefire.failIfNoSpecifiedTests=false
```

Escenarios en `src/test/resources/features/runner-steps.feature`: listar
topics + descargar esquema, generar datos dummy desde esquemas y enviarlos,
enviar un mensaje con `GenericRunnerBean`, y consultar todas las versiones
de un subject (`SchemaRegistryClientService.listVersions`, nuevo método
añadido para esta suite).

## Dependencias externas relevantes

- Spring Framework / Spring Boot AutoConfiguration
- Spring Kafka
- Confluent Schema Registry Client / Avro Serializer (repositorio `packages.confluent.io`)
- Lombok, MapStruct (procesadores de anotaciones)
