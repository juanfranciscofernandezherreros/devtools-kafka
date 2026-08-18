# Kafka compartido para Kubernetes

Este directorio despliega con Argo CD un único stack Kafka para todos los
microservicios del clúster:

- Kafka: `kafka.kafka-shared.svc.cluster.local:9092`
- Schema Registry: `http://schema-registry.kafka-shared.svc.cluster.local:8081`
- REST Proxy: `http://kafka-rest-proxy.kafka-shared.svc.cluster.local:8082`
- Kafka UI: `http://kafka-ui.kafka-shared.svc.cluster.local:8080`
- Redpanda Console: `http://redpanda-console.kafka-shared.svc.cluster.local:8080`

El stack está pensado para el clúster local de desarrollo: un broker, un
ZooKeeper y factor de replicación 1. Los datos de Kafka y ZooKeeper usan PVC.

Los consumidores deben recibir el broker mediante la variable:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka.kafka-shared.svc.cluster.local:9092
```

No se debe volver a incluir un StatefulSet Kafka dentro de cada microservicio.

## Visualizar los tópicos

Desde Windows:

```powershell
kubectl port-forward svc/kafka-ui -n kafka-shared 8085:8080
```

Después se abre `http://localhost:8085` y se selecciona el clúster
`kafka-compartido`. La interfaz muestra tópicos, mensajes, particiones,
consumer groups y los esquemas registrados.

## Visualizar Schema Registry con Redpanda Console

```powershell
kubectl port-forward svc/redpanda-console -n kafka-shared 8086:8080
```

Después se abre `http://localhost:8086`. La sección `Schema Registry` permite
consultar los subjects, versiones y contenido de los esquemas. Redpanda Console
también permite navegar por los tópicos y grupos consumidores del mismo Kafka.
