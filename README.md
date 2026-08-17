# rft-devtools-kafka-cucumber

Librería Spring Boot (`AutoConfiguration`) que expone clientes REST para
Kafka REST Proxy y Confluent Schema Registry, junto con utilidades Avro,
para que otros servicios puedan descargar esquemas, generar mensajes de
prueba y enviarlos vía REST Proxy sin reimplementar ese cableado en cada
proyecto.

## Estructura del repositorio

```
.
├── source/            Proyecto Maven (la librería en sí)
│   ├── pom.xml
│   └── src/
├── local-dev/          Infraestructura Kafka local para desarrollo/pruebas
│   ├── docker-compose.yml     Zookeeper + Kafka + Schema Registry + REST Proxy
│   └── config/                 Properties de ejemplo para apps consumidoras
│       ├── application-local.yml
│       └── application-dev.yml
├── .gitlab-ci.yml      Pipeline (build backend Java, imagen "backend")
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

## Compilar

Requiere Java 17+ y Maven.

```bash
cd source
mvn clean install
```

Genera `source/target/rft-devtools-kafka-cucumber-1.0.0-SNAPSHOT.jar` y
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
docker compose down -v    # para y limpia volúmenes
```

Puertos expuestos en el host:

| Servicio | Puerto |
|---|---|
| Zookeeper | `2181` |
| Kafka (broker externo) | `9092` |
| Schema Registry | `8081` |
| Kafka REST Proxy | `8082` |

## Configurar una app consumidora

`GenericRunnerBean.run(keyFile, valueFile, schemaRegistryUrl, topicName,
restProxyUrl, urlPrefix)` recibe las URLs de Schema Registry y REST Proxy
como argumentos, no las resuelve de properties. `local-dev/config/`
contiene dos plantillas listas para copiar al proyecto consumidor:

- `application-local.yml` — apunta al stack de `docker-compose.yml` (`localhost:8081` / `localhost:8082`)
- `application-dev.yml` — plantilla para un entorno DEV/QA compartido (rellenar hosts y credenciales)

Ambas documentan las properties reales que sí enlaza la librería
(`library.schema.*`) y las que debe definir la app consumidora para pasar
a `runner.run(...)`.

## CI

`.gitlab-ci.yml` construye el módulo Java bajo `source/` con la plantilla
backend estándar (`IMAGE_LOCATION: backend`, `JAVA_VERSION: 17`).
