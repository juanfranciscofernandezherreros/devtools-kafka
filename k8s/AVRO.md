# Contratos Avro de los microservicios

Los mensajes de negocio usan clave `String` y valor Avro con el formato binario
de Confluent. Los valores se validan contra Apicurio Registry mediante la
estrategia estándar `TopicIdStrategy`, por lo que cada artefacto se llama
`<topic>-value` (equivalente al `TopicNameStrategy` de Confluent y compatible
con los subjects que ya existían).

| Microservicio | Topic | Subject | Registro Avro |
|---|---|---|---|
| `crud-crypto-relay` | `crypto-prices-in` | `crypto-prices-in-value` | `CryptoPriceEvent` |
| `crud-crypto-relay` | `crypto-prices-out` | `crypto-prices-out-value` | `CryptoPriceRelayedEvent` |
| `crud-sales-streams` | `orders-topic1` | `orders-topic1-value` | `OrderEvent` |
| `crud-sales-streams` | `total-sales-topic1` | `total-sales-topic1-value` | `OrderWithTotalEvent` |
| `crud-sensores-stream` | `lecturas-sensores` | `lecturas-sensores-value` | `LecturaSensorEvent` |
| `crud-sensores-stream` | `totales-sensores` | `totales-sensores-value` | `LecturaSensorConTotalEvent` |

Los ficheros de `k8s/schemas/` forman el catálogo central. El Job
`register-avro-schemas` espera a que Apicurio Registry esté disponible (vía su
API compatible con Confluent, `/apis/ccompat/v7`) y registra los seis
contratos después de cada sincronización de Argo CD. El registro es
idempotente: volver a enviar el mismo esquema no crea una versión incompatible.

Cada aplicación recibe estas variables:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka.kafka-shared.svc.cluster.local:9092
SCHEMA_REGISTRY_URL=http://apicurio-registry.kafka-shared.svc.cluster.local:8080/apis/registry/v3
```

Las clases Java no se mantienen a mano. `avro-maven-plugin` las genera durante
`generate-sources` desde los `.avsc` de cada microservicio. Las topologías usan
`io.apicurio.registry.serde.avro.AvroSerde` (dependencia
`io.apicurio:apicurio-registry-avro-serde-kafka`), que incorpora el
identificador del esquema en el mensaje y consulta Apicurio Registry al
deserializarlo — mismo rol que cumplía `SpecificAvroSerde` de Confluent.

## Compatibilidad con mensajes JSON antiguos

Los registros JSON existentes en estos topics no se pueden interpretar como
Avro. Antes de activar la nueva imagen en un entorno con datos históricos se
debe decidir entre vaciar los topics de desarrollo, crear topics versionados o
iniciar los nuevos grupos desde el final. En este clúster local se recomienda
vaciar los seis topics si solo contienen datos de prueba.

## Comprobación

Con el port-forward de Redpanda Console activo, los subjects aparecen en:

```text
http://localhost:8086/schema-registry
```

También se pueden consultar directamente vía la API compatible con Confluent:

```powershell
kubectl port-forward svc/apicurio-registry -n kafka-shared 8081:8080
Invoke-RestMethod http://localhost:8081/apis/ccompat/v7/subjects
```

O explorarlos visualmente en la UI de Apicurio (ver `README.md`).
