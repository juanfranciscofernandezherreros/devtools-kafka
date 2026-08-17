Feature: Kafka REST Proxy and Schema Registry via rft-devtools-kafka-cucumber

  Requiere el stack local levantado y los esquemas de ejemplo registrados
  (local-dev/docker-compose.yml + local-dev/register-demo-schemas.sh).
  No se ejecuta con mvn test/install por defecto: correr con
  `mvn test -Dtest=RunCucumberIT` (ver source/README.md).

  Background:
    Given el REST Proxy "http://localhost:8082/topics"
    And el Schema Registry "http://localhost:8081/subjects/"
    And el URL Prefix "/versions/latest"

  Scenario: Listar topics y descargar el esquema Avro de un topic
    Given establezco la carpeta de salida de esquemas Avro a "runner-steps"
    And configuro la descarga de esquemas Avro a "false"
    When consulto la lista de topics Avro desde el REST Proxy
    Then debo recibir una lista de topics del REST Proxy
    When descargo el esquema Avro del topic "test-topic"
    Then debo tener el esquema Avro (key y value) del topic indicado en el directorio de salida

  Scenario: Generar datos dummy a partir de esquemas descargados y enviarlos
    Given establezco la carpeta de salida de esquemas Avro a "runner-steps-dummy"
    When descargo el esquema Avro del topic "test-topic"
    And genero datos dummy a partir de los esquemas Avro
    And envío los registros dummy Avro al entorno dev

  Scenario: Enviar un mensaje Avro directamente con GenericRunnerBean
    Given un keyPayload "test-data/key.json"
    And un valuePayload "test-data/value.json"
    And el topic "test-topic"
    When ejecuto GenericRunnerBean
    Then el mensaje debe enviarse correctamente

  Scenario: Consultar todas las versiones de los subjects de un topic
    Given el topic "test-topic"
    When consulto todas las versiones de los subjects del topic usando el Schema Registry v6
    Then debo ver una lista de versiones disponibles para cada subject en v6
