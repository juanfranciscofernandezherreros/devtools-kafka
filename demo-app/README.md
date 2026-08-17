# devkafka-demo-app

App Spring Boot mínima que consume `rft-devtools-kafka-cucumber` (el módulo
`source/`) y sí se puede arrancar con un perfil, a diferencia de la
librería en sí. Al arrancar: lista los topics del REST Proxy, descarga el
esquema Avro de `test-topic` y envía un mensaje de ejemplo
(`samples/key.json` + `samples/value.json`).

## Arrancar contra el stack local

```bash
# 1) Instalar la librería en el repo Maven local
mvn -f ../source/pom.xml clean install

# 2) Levantar Kafka local (si no está ya arriba)
cd ../local-dev && docker compose up -d && cd ../demo-app

# 3) Registrar los esquemas de ejemplo en el Schema Registry local (solo la primera vez)
../local-dev/register-demo-schemas.sh

# 4) Arrancar con el perfil "local"
mvn spring-boot:run -Dspring-boot.run.profiles=local
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

## Jar ejecutable

```bash
mvn clean package
java -jar target/devkafka-demo-app-0.1.0-SNAPSHOT.jar --spring.profiles.active=local
```
