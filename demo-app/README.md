# devkafka-demo-app

App Spring Boot mínima que consume `rft-devtools-kafka-cucumber` (el módulo
`source/`) y sí se puede arrancar con un perfil, a diferencia de la
librería en sí. Al arrancar: lista los topics del REST Proxy, descarga el
esquema Avro de `test-topic` y envía un mensaje de ejemplo
(`samples/key.json` + `samples/value.json`).

Todos los comandos de este README se ejecutan **desde la raíz del
repositorio** (no desde `demo-app/`), usando `mvn -f demo-app/pom.xml` —
así no hace falta `cd` a ningún subproyecto.

## Arrancar contra el stack local

```powershell
mvn -f source/pom.xml clean install
docker compose -f local-dev/docker-compose.yml up -d
powershell -File local-dev/register-demo-schemas.ps1
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=local"
```

## Arrancar contra dev / integration / qa

Mismo comando cambiando el perfil:

```powershell
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=dev"
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=integration"
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=qa"
```

Estos tres perfiles (`src/main/resources/application-{dev,integration,qa}.yml`)
todavía tienen placeholders (`<schema-registry-*-host>`, etc.) — rellénalos
con las URLs reales antes de que funcionen contra esos entornos.

## Modos de ejecución (`app.kafka.*`)

`KafkaDemoRunner` siempre lista los topics del REST Proxy al arrancar.
Además, según las properties activas:

| `send-sample-message` | `auto-generate-sample` | `download-schema-only` | `download-all-schemas` | Qué hace |
|---|---|---|---|---|
| `true` | `false` | (ignorado) | (ignorado) | Descarga el esquema key+value y envía los ficheros estáticos `key-payload-file`/`value-payload-file` (por defecto `samples/key.json`/`value.json`) |
| `true` | `true` | (ignorado) | (ignorado) | Descarga el esquema key+value, **genera un valor aleatorio que cumple ese esquema** (`AvroDummyFiller`) y lo envía — sin necesitar ficheros de ejemplo fijos |
| `false` | — | `true` | `false` | Solo descarga el esquema key+value de `topic-name` a `schema-output-dir` (por defecto `schemas/`), sin enviar nada |
| `false` | — | `true` | `true` | Ignora `topic-name`: lista **todos** los subjects del Schema Registry (`SchemaRegistryClientService.listSubjects`) y descarga cada uno |
| `false` | — | `false` | — | Solo lista los topics |

Útil cuando tienes la URL del Schema Registry pero `topic-name` (`test-topic`
por defecto) no existe como subject real en ese entorno — solo listar
topics o solo descargar esquemas no falla aunque el envío del mensaje sí lo
haría.

Cada vez que se envía un mensaje (las dos primeras filas de la tabla),
`KafkaDemoRunner` guarda el payload exacto enviado (key + value, tanto si
viene de `key-payload-file`/`value-payload-file` como si es el generado con
`auto-generate-sample`) en
`<schema-output-dir>/sent/<topic-name>-sent.json`.

Ejemplo para descargar solo el esquema de un topic real en `dev`:

```powershell
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=dev" `
  "-Dspring-boot.run.arguments=--app.kafka.topic-name=mi-topic-real --app.kafka.send-sample-message=false --app.kafka.download-schema-only=true"
```

Ejemplo para descargar **todos** los esquemas registrados en `dev` de golpe:

```powershell
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=dev" `
  "-Dspring-boot.run.arguments=--app.kafka.send-sample-message=false --app.kafka.download-schema-only=true --app.kafka.download-all-schemas=true"
```

Ejemplo para generar y enviar un mensaje automático (sin ficheros fijos) a
un topic real en `dev`, una vez confirmado que existe como subject:

```powershell
mvn -f demo-app/pom.xml spring-boot:run "-Dspring-boot.run.profiles=dev" `
  "-Dspring-boot.run.arguments=--app.kafka.topic-name=mi-topic-real --app.kafka.auto-generate-sample=true"
```

(`-Dspring-boot.run.arguments=...` pasa argumentos al programa — a
diferencia de `-D<prop>=<valor>` suelto, que Maven **no** reenvía al
proceso hijo de `spring-boot:run`. En PowerShell, el backtick `` ` `` al
final de línea continúa el comando en la siguiente; no dejes espacios
después de él.)

## Jar ejecutable

A diferencia de `spring-boot:run` (que fija el directorio de trabajo en
`demo-app/` automáticamente), `java -jar` usa el directorio desde el que lo
lances — así que las rutas de los payloads hay que darlas relativas a la
raíz del repo:

```powershell
mvn -f demo-app/pom.xml clean package
java -jar demo-app/target/devkafka-demo-app-0.1.0-SNAPSHOT.jar `
  --spring.profiles.active=local `
  --app.kafka.key-payload-file=demo-app/samples/key.json `
  --app.kafka.value-payload-file=demo-app/samples/value.json
```
