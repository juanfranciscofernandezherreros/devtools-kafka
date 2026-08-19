# Kafka compartido para Kubernetes

Este directorio despliega con Argo CD un único stack Kafka para todos los
microservicios del clúster:

- Kafka: `kafka.kafka-shared.svc.cluster.local:9092`
- Apicurio Registry (API nativa v3): `http://apicurio-registry.kafka-shared.svc.cluster.local:8080/apis/registry/v3`
- Apicurio Registry (API compatible Confluent): `http://apicurio-registry.kafka-shared.svc.cluster.local:8080/apis/ccompat/v7`
- Apicurio Registry UI: `http://apicurio-registry-ui.kafka-shared.svc.cluster.local:8080`
- REST Proxy: `http://kafka-rest-proxy.kafka-shared.svc.cluster.local:8082`
- Kafka UI: `http://kafka-ui.kafka-shared.svc.cluster.local:8080`
- Kafka Connect: `http://kafka-connect.kafka-shared.svc.cluster.local:8083`
- Redpanda Console: `http://redpanda-console.kafka-shared.svc.cluster.local:8080`

El stack está pensado para el clúster local de desarrollo: un broker, un
ZooKeeper y factor de replicación 1. Los datos de Kafka y ZooKeeper usan PVC.

Los consumidores deben recibir el broker mediante la variable:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka.kafka-shared.svc.cluster.local:9092
```

No se debe volver a incluir un StatefulSet Kafka dentro de cada microservicio.

## Contratos Avro

Los topics de negocio de `crud-crypto-relay`, `crud-sales-streams` y
`crud-sensores-stream` usan valores Avro y claves String. El catálogo completo,
los subjects y la estrategia de migración desde JSON están documentados en
[`AVRO.md`](AVRO.md). El Job GitOps `register-avro-schemas` registra los seis
contratos automáticamente en Apicurio Registry (vía su API compatible con
Confluent, `/apis/ccompat/v7`) después de la sincronización.

Apicurio Registry usa almacenamiento KafkaSQL: su estado (esquemas, versiones,
metadatos) se persiste en un topic interno del propio Kafka compartido, por lo
que sobrevive a un reinicio del pod sin depender del Job de registro.

## Visualizar los tópicos

Desde Windows:

```powershell
kubectl port-forward svc/kafka-ui -n kafka-shared 8085:8080
```

Después se abre `http://localhost:8085` y se selecciona el clúster
`kafka-compartido`. La interfaz muestra tópicos, mensajes, particiones,
consumer groups y los esquemas registrados.

## Redpanda Console, Apicurio Registry y Kafka Connect

Redpanda Console está configurada mediante un archivo YAML para conectarse a
los tres servicios compartidos: Kafka, Apicurio Registry (vía su API
compatible con Confluent) y Kafka Connect.

## Interfaz web de Apicurio Registry

Apicurio Registry despliega su UI como un componente aparte
(`apicurio-registry-ui`), independiente del backend. Como la UI corre en el
navegador (no en el pod), necesita alcanzar el backend en un puerto local, así
que hacen falta **dos** port-forwards a la vez (en dos terminales):

```powershell
kubectl port-forward svc/apicurio-registry -n kafka-shared 8081:8080
kubectl port-forward svc/apicurio-registry-ui -n kafka-shared 8087:8080
```

Después se abre `http://localhost:8087`. Desde ahí se pueden explorar los
esquemas registrados, sus versiones y metadatos, y editar reglas de
compatibilidad por artefacto o globales.

```powershell
kubectl port-forward svc/redpanda-console -n kafka-shared 8086:8080
```

Después se abre `http://localhost:8086`. Las secciones `Schema Registry` y
`Kafka Connect` permiten administrar los esquemas y conectores. Kafka Connect
se despliega como un worker distribuido y conserva su configuración, offsets y
estado en los tópicos internos `_connect-configs`, `_connect-offsets` y
`_connect-status`.
