# Kafka compartido para Kubernetes

Este directorio despliega con Argo CD un único stack Kafka para todos los
microservicios del clúster:

- Kafka: `kafka.kafka-shared.svc.cluster.local:9092`
- Schema Registry: `http://schema-registry.kafka-shared.svc.cluster.local:8081`
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
contratos automáticamente en Schema Registry después de la sincronización.

## Visualizar los tópicos

Desde Windows:

```powershell
kubectl port-forward svc/kafka-ui -n kafka-shared 8085:8080
```

Después se abre `http://localhost:8085` y se selecciona el clúster
`kafka-compartido`. La interfaz muestra tópicos, mensajes, particiones,
consumer groups y los esquemas registrados.

## Redpanda Console, Schema Registry y Kafka Connect

Redpanda Console está configurada mediante un archivo YAML para conectarse a
los tres servicios compartidos: Kafka, Schema Registry y Kafka Connect.

```powershell
kubectl port-forward svc/redpanda-console -n kafka-shared 8086:8080
```

Después se abre `http://localhost:8086`. Las secciones `Schema Registry` y
`Kafka Connect` permiten administrar los esquemas y conectores. Kafka Connect
se despliega como un worker distribuido y conserva su configuración, offsets y
estado en los tópicos internos `_connect-configs`, `_connect-offsets` y
`_connect-status`.
