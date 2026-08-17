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
│   ├── register-demo-schemas.ps1/.sh  Registra los esquemas de ejemplo en el Schema Registry local
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
- `client` — clientes HTTP hacia Schema Registry (esquema/versiones/lista
  de subjects) y Kafka REST Proxy
- `runner` — orquestación: descarga de esquema + envío de mensaje Avro
- `avro` — utilidades para generar registros Avro de prueba y convertirlos a JSON
- `exception` — excepciones propias de la librería

Certificados TLS: se validan por defecto; solo se ignoran cuando
`library.schema.ignore-ssl=true` (pensado para local/dev con certificados
propios, no para producción).

## Compilar

Requiere Java 17+ y Maven. Todos los comandos de este README se ejecutan
**desde la raíz del repositorio**, sin necesidad de `cd` a ningún subproyecto
(usan `mvn -f <módulo>/pom.xml` y `docker compose -f local-dev/docker-compose.yml`).

```powershell
mvn -f source/pom.xml clean install
```

Genera `source/target/rft-devtools-kafka-cucumber-0.1.0-SNAPSHOT.jar` y
corre la suite de tests (unitarios, sin dependencias externas).

## Levantar Kafka en local

`local-dev/docker-compose.yml` levanta un stack Confluent 7.4.4 (misma
versión que fija `pom.xml`) para poder probar la librería contra
infraestructura real en vez de contra los endpoints QA/DEV que reciben
las URLs como parámetro.

```powershell
docker compose -f local-dev/docker-compose.yml up -d
docker compose -f local-dev/docker-compose.yml ps
powershell -File local-dev/register-demo-schemas.ps1
```

(En macOS/Linux o Git Bash, usa `bash local-dev/register-demo-schemas.sh`
en vez del `.ps1`.)

Para parar y limpiar volúmenes:

```powershell
docker compose -f local-dev/docker-compose.yml down -v
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

```powershell
mvn -f source/pom.xml clean install
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=local"
```

Según las properties `app.kafka.*` activas puede: listar topics, descargar
solo el esquema de un topic (o **todos** los subjects del registry de una
vez con `download-all-schemas`), o descargar + enviar un mensaje Avro —
con un payload fijo o **generado automáticamente** a partir del esquema
descargado (`auto-generate-sample`, vía `AvroDummyFiller`). Ver
[demo-app/README.md](demo-app/README.md) para la tabla completa de modos
y cómo apuntar a `dev`/`integration`/`qa`.

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
