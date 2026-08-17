# devkafka-demo-app

App Spring Boot mínima que consume `rft-devtools-kafka-cucumber` (el módulo
`source/`) y sí se puede arrancar con un perfil, a diferencia de la
librería en sí. Al arrancar: lista los topics del REST Proxy, descarga el
esquema Avro de `test-topic` y envía un mensaje de ejemplo
(`samples/key.json` + `samples/value.json`).

## Arrancar contra el stack local

```bash
mvn -f ../source/pom.xml clean install && cd ../local-dev && docker compose up -d && ./register-demo-schemas.sh && cd ../demo-app && mvn spring-boot:run -Dspring-boot.run.profiles=local
```

## Arrancar contra dev / integration / qa

Mismo comando cambiando el perfil:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev
mvn spring-boot:run -Dspring-boot.run.profiles=integration
mvn spring-boot:run -Dspring-boot.run.profiles=qa
```

Estos tres perfiles (`src/main/resources/application-{dev,integration,qa}.yml`)
todavía tienen placeholders (`<schema-registry-*-host>`, etc.) — rellénalos
con las URLs reales antes de que funcionen contra esos entornos.

## Modos de ejecución (`app.kafka.*`)

`KafkaDemoRunner` siempre lista los topics del REST Proxy al arrancar.
Además, según las properties activas:

| `send-sample-message` | `auto-generate-sample` | `download-schema-only` | Qué hace |
|---|---|---|---|
| `true` | `false` | (ignorado) | Descarga el esquema key+value y envía los ficheros estáticos `key-payload-file`/`value-payload-file` (por defecto `samples/key.json`/`value.json`) |
| `true` | `true` | (ignorado) | Descarga el esquema key+value, **genera un valor aleatorio que cumple ese esquema** (`AvroDummyFiller`) y lo envía — sin necesitar ficheros de ejemplo fijos |
| `false` | — | `true` | Solo descarga el esquema key+value de `topic-name` a `schema-output-dir` (por defecto `schemas/`), sin enviar nada |
| `false` | — | `false` | Solo lista los topics |

Útil cuando tienes la URL del Schema Registry pero `topic-name` (`test-topic`
por defecto) no existe como subject real en ese entorno — solo listar
topics o solo descargar esquemas no falla aunque el envío del mensaje sí lo
haría.

Ejemplo para descargar solo el esquema de un topic real en `dev`:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev \
  "-Dspring-boot.run.arguments=--app.kafka.topic-name=mi-topic-real --app.kafka.send-sample-message=false --app.kafka.download-schema-only=true"
```

Ejemplo para generar y enviar un mensaje automático (sin ficheros fijos) a
un topic real en `dev`, una vez confirmado que existe como subject:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev \
  "-Dspring-boot.run.arguments=--app.kafka.topic-name=mi-topic-real --app.kafka.auto-generate-sample=true"
```

(`-Dspring-boot.run.arguments=...` pasa argumentos al programa — a
diferencia de `-D<prop>=<valor>` suelto, que Maven **no** reenvía al
proceso hijo de `spring-boot:run`.)

## Jar ejecutable

```bash
mvn clean package && java -jar target/devkafka-demo-app-0.1.0-SNAPSHOT.jar --spring.profiles.active=local
```
