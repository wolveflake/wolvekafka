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

## Publish Demo Messages from the Shell Pod

The chart ships with a helper script mounted inside the `kafka-shell` pod at `/opt/wolvekafka/demo_producer.py`. Use it to seed a topic from inside the cluster:

```bash
POD=$(kubectl -n kafka-demo get pod -l app.kubernetes.io/component=kafka-shell -o jsonpath='{.items[0].metadata.name}')
kubectl -n kafka-demo exec -it "$POD" -- bash

# Install the python dependency once inside the shell
pip install --user kafka-python

# Publish JSON demo events (KAFKA_BOOTSTRAP_SERVERS is pre-populated)
python3 /opt/wolvekafka/demo_producer.py --topic demo-topic --count 5
```

`KAFKA_BOOTSTRAP_SERVERS` already points the script at the in-cluster broker, so you only need to pass a topic. Use `--count 0` to stream indefinitely, `--message "..."` to tweak the payload, or override `--bootstrap-servers` if you exposed Kafka externally.
