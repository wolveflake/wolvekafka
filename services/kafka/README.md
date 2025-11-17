# Kafka Demo Helm Chart

A bite-sized Apache Kafka + Zookeeper deployment tuned for DigitalOcean Kubernetes demo clusters.

## Components

- **Kafka** – single-broker StatefulSet with 5Gi optional persistence and lightweight resources.
- **Zookeeper** – single-node Deployment sized for demos; persistence optional.

## Configuration Highlights

See `values.yaml` for every option. Common toggles:

| Key | Default | Description |
| --- | --- | --- |
| `kafka.replicaCount` | `1` | Number of brokers. Keep at 1 for the demo profile. |
| `kafka.heapOpts` | `-Xmx512m -Xms512m` | JVM heap sizing. |
| `kafka.service.external.enabled` | `false` | Creates a LoadBalancer/NodePort to reach Kafka externally. |
| `kafka.persistence.enabled` | `true` | Provision a PVC for Kafka data. |
| `zookeeper.persistence.enabled` | `false` | Adds a PVC for Zookeeper state if you prefer durability. |

## Install

```bash
helm install demo-kafka ./kafka-demo \
  --namespace kafka-demo --create-namespace
```

Override any value with `--set` or a custom `values.yaml` file, e.g. to enable an external LoadBalancer:

```bash
helm install demo-kafka ./kafka-demo \
  --set kafka.service.external.enabled=true \
  --set kafka.service.external.type=LoadBalancer
```

## Verify

```bash
kubectl get pods -n kafka-demo
kubectl port-forward svc/demo-kafka-kafka 9092:9092 -n kafka-demo
```

Then use your favourite Kafka client pointing at `localhost:9092`.
