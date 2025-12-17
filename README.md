# WolveKafka Demo Environment

Step-by-step instructions to run the landing page, produce demo data into Kafka, and view the Grafana dashboard via port-forwards.

## Prerequisites
- Access to the `wks-demo` namespace
- `kubectl`, `helm`

## Port-forward the cluster services

In separate terminals:

```bash
kubectl -n wks-demo port-forward svc/demo-kafka-kafka-demo-kafka 9092:9092   # optional
kubectl -n wks-demo port-forward svc/wks-with-instrumentation-prometheus 9090:9090   # optional
kubectl -n wks-demo port-forward svc/grafana 3000:80
kubectl -n wks-demo port-forward svc/lambda-lambda 8081:8081
```

Open [http://127.0.0.1:8081](http://127.0.0.1:8081) to use the lambda UI, and [http://127.0.0.1:3000](http://127.0.0.1:3000) for Grafana (default admin/admin, data source `http://wks-with-instrumentation-prometheus.wks-demo.svc.cluster.local:9090`).

## Deploy or update lambda

To redeploy the lambda image (optionally pinning a digest):

```bash
helm upgrade lambda services/lambda -n wks-demo --reuse-values \
  --set image.digest=sha256:fc6d423f36757610ede60aa305b20e2b0ef2bf57016ecff4cf452046873e73ab \
  --set image.tag="latest"
```

## Enable Grafana embedding

Make sure the Grafana release includes `services/grafana.values.yaml` so `allow_embedding` is true:

```bash
helm upgrade grafana grafana/grafana -n wks-demo -f services/grafana.values.yaml
```

## Producing demo data

In the landing page’s "Seed the demo topic" form:
- Topic: `demo-topic`
- Bootstrap servers: `demo-kafka-kafka-demo-kafka:9092`
- Count/interval/message as desired
- Click **Start producing** and watch the lambda logs (`kubectl -n wks-demo logs deploy/lambda-lambda`).

## Viewing dashboards

With the Grafana port-forward active, paste the `http://127.0.0.1:3000/d-solo/...` links into each panel slot. The URLs persist in local storage, so refreshes retain them.
