# rft-devtools-kafka-cucumber

Librería Spring Boot (`AutoConfiguration`) que expone clientes REST para
Kafka REST Proxy y Confluent Schema Registry, junto con utilidades Avro,
para que otros servicios puedan descargar esquemas, generar mensajes de
prueba y enviarlos vía REST Proxy sin reimplementar ese cableado en cada
proyecto.

## Estructura del repositorio

```
.
├── source/             Proyecto Maven (la librería en sí)
│   ├── pom.xml
│   └── src/
├── demo-app/            App Spring Boot runnable que consume la librería
│   ├── pom.xml
│   ├── samples/                 Payloads de ejemplo (key.json, value.json)
│   └── src/main/resources/      application-{local,dev,integration,qa}.yml
├── local-dev/           Infraestructura Kafka local para desarrollo/pruebas
│   ├── docker-compose.yml       Zookeeper + Kafka + Schema Registry + REST Proxy
│   ├── register-demo-schemas.sh Registra los esquemas de ejemplo en el Schema Registry local
│   └── config/                   Properties de ejemplo para apps consumidoras
│       ├── application-local.yml
│       ├── application-dev.yml
│       ├── application-integration.yml
│       └── application-qa.yml
├── .gitlab-ci.yml       Pipeline (build backend Java, imagen "backend")
└── sonar-project.properties
```

## Qué expone la librería

`LibraryAutoConfiguration` registra automáticamente (activable/desactivable
por properties):

| Bean | Paquete | Property para desactivar |
|---|---|---|
| `SchemaRegistryClientService` | `client` | `library.schema.enabled=false` |
| `SchemaDownload` | `runner` | `library.schema.enabled=false` |
| `GenericRunnerBean` | `runner` | `library.runner.enabled=false` |
| `KafkaRestProxyClientService` | `client` | `library.restproxy.enabled=false` |

Paquetes internos (`com.devkafka.*`):

- `config` — auto-configuración y `@ConfigurationProperties` (`library.schema.*`)
- `client` — clientes HTTP hacia Schema Registry y Kafka REST Proxy
- `runner` — orquestación: descarga de esquema + envío de mensaje Avro
- `avro` — utilidades para generar registros Avro de prueba y convertirlos a JSON
- `exception` — excepciones propias de la librería

Certificados TLS: se validan por defecto; solo se ignoran cuando
`library.schema.ignore-ssl=true` (pensado para local/dev con certificados
propios, no para producción).

## Compilar

Requiere Java 17+ y Maven.

```bash
cd source
mvn clean install
```

Genera `source/target/rft-devtools-kafka-cucumber-0.1.0-SNAPSHOT.jar` y
corre la suite de tests (unitarios, sin dependencias externas).

## Levantar Kafka en local

`local-dev/docker-compose.yml` levanta un stack Confluent 7.4.4 (misma
versión que fija `pom.xml`) para poder probar la librería contra
infraestructura real en vez de contra los endpoints QA/DEV que reciben
las URLs como parámetro.

```bash
cd local-dev
docker compose up -d      # arranca Zookeeper, Kafka, Schema Registry y REST Proxy
docker compose ps         # confirma que los 4 servicios estén "healthy"
./register-demo-schemas.sh    # registra los esquemas de ejemplo (solo la primera vez)
docker compose down -v    # para y limpia volúmenes
```

Puertos expuestos en el host:

| Servicio | Puerto |
|---|---|
| Zookeeper | `2181` |
| Kafka (broker externo) | `9092` |
| Schema Registry | `8081` |
| Kafka REST Proxy | `8082` |

## Probarla en marcha: `demo-app`

La librería es un jar sin clase `main`, así que `demo-app/` es una app
Spring Boot mínima que sí se puede arrancar, para ejercitar la librería de
verdad contra un perfil:

```bash
mvn -f source/pom.xml clean install     # instala la librería en el repo Maven local
cd demo-app
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

Según `app.kafka.send-sample-message` / `app.kafka.download-schema-only`
puede listar topics, descargar solo el esquema de un topic, o descargar +
enviar un mensaje Avro de ejemplo. Ver [demo-app/README.md](demo-app/README.md)
para el detalle de cada modo y cómo apuntar a `dev`/`integration`/`qa`.

## Configurar una app consumidora

`GenericRunnerBean.run(keyFile, valueFile, schemaRegistryUrl, topicName,
restProxyUrl, urlPrefix)` recibe las URLs de Schema Registry y REST Proxy
como argumentos, no las resuelve de properties. `local-dev/config/`
contiene plantillas listas para copiar al proyecto consumidor, una por
entorno (`local`, `dev`, `integration`, `qa`), documentando las properties
reales que sí enlaza la librería (`library.schema.*`) y las que debe
definir la app consumidora para pasar a `runner.run(...)`.

## CI

`.gitlab-ci.yml` construye el módulo Java bajo `source/` con la plantilla
backend estándar (`IMAGE_LOCATION: backend`, `JAVA_VERSION: 17`).
