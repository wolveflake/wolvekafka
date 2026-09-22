# WolveKafka (WKS)

**Turn any HTTP service into a reliable Kafka consumer.** WolveKafka runs the
consumer group and provides retries, dead-letter queues, batching, weighted
routing, hot reload, and observability. Your application only implements an
HTTP handler, in any language.

```text
Kafka topic ──▶ WKS ──▶ your HTTP handler
                  │
                  └──▶ optional Kafka output or dead-letter topic
```

WKS is deliberately narrow: use it when your business logic already is—or
should be—an independently deployed HTTP service, but you do not want every
service to implement Kafka consumer plumbing.

## Why WKS?

| Without WKS | With WKS |
| --- | --- |
| Kafka client and offset logic in every service | A plain HTTP `POST` handler |
| Custom retry and dead-letter code | Declarative retry and DLQ policies |
| Consumer implementation tied to a client language | Handlers in Python, Node, Go, Rust, or anything with HTTP |
| Redeploy to change consumers | Hot-reload a JSON configuration file |
| Assemble metrics and dashboards yourself | OpenTelemetry metrics and a Grafana dashboard |

## Five-minute quickstart

Prerequisites: Docker with Compose v2 and `curl`. The checked-in quickstart
uses version-pinned Kafka and Python images and builds WKS from this checkout;
it does not depend on an unpublished binary or private registry.

Start Kafka, create `demo-topic`, launch the example handler, and run WKS:

```bash
git clone https://github.com/dguptawolveflake/wolvekafka.git
cd wolvekafka
docker compose -f examples/quickstart/compose.yaml up --build -d

# Wait for the WKS HTTP endpoint. It prints OK when ready.
until curl -fsS http://127.0.0.1:8080/health; do sleep 1; done
```

Produce one message:

```bash
printf '{"hello":"kafka"}\n' | docker compose \
  -f examples/quickstart/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic demo-topic
```

Inspect the handler log:

```bash
docker compose -f examples/quickstart/compose.yaml logs handler
```

You should see output like:

```text
[handler] got demo-topic[0]@0: {"hello":"kafka"}
```

WKS committed the input offset only after the handler returned
`{"messages":[{"status":2}]}`. Its Prometheus metrics are available at
<http://127.0.0.1:8080/metrics>:

```bash
curl -fsS http://127.0.0.1:8080/metrics | grep '^wks_'
```

Stop the quickstart and remove its local volumes:

```bash
docker compose -f examples/quickstart/compose.yaml down -v
```

The complete example lives in [`examples/quickstart`](examples/quickstart).
Edit its [`config.json`](examples/quickstart/config.json) to try batching,
retries, a DLQ, multiple handler targets, or Kafka output.

## Handler contract

WKS sends a batch as protobuf JSON. Kafka byte fields such as `key`, `value`,
and header values are base64 encoded:

```json
{
  "messages": [
    {
      "topic": "demo-topic",
      "partition": 0,
      "offset": "0",
      "headers": [],
      "key": "",
      "value": "eyJoZWxsbyI6ImthZmthIn0K",
      "encoding": 0
    }
  ]
}
```

Return exactly one result for every input message, in the same order:

```json
{"messages":[{"status":2}]}
```

| Status | Meaning |
| --- | --- |
| `2` / `STATUS_SUCCESS` | Processing succeeded; the response may be sent to configured Kafka output. |
| `0` / `STATUS_RETRY` | Retry this record according to the retry policy. |
| `1` / `STATUS_FAILED` | Apply the configured DLQ or explicit drop action. |

Always return an explicit status. An omitted status is protobuf enum value zero,
which means retry—not success. See [delivery, retry, and migration semantics](docs/delivery-and-security.md)
before using WKS with production traffic.

## Configuration

A minimal configuration looks like this:

```json
{
  "cluster": {
    "bootstrap_servers": ["kafka:9092"]
  },
  "consumers": [
    {
      "group_id": "orders-handler",
      "topics": ["orders"],
      "lambdas": [{"url": "http://orders-api:8080/events"}],
      "batch": {
        "max_messages": 100,
        "max_bytes": 1048576,
        "max_wait_ms": 500
      },
      "retry_policy": {
        "exponential_backoff": {
          "initial_backoff_ms": 100,
          "max_backoff_ms": 5000,
          "multiplier": 2
        },
        "dlq": {"topic": "orders.dlq"}
      }
    }
  ]
}
```

Important options:

- `cluster`: bootstrap servers plus optional SASL/PLAIN, SCRAM-SHA-256/512,
  TLS, and mTLS settings.
- `lambdas`: one or more HTTP targets with optional weights. Retries select a
  target again so a failed peer does not permanently pin a batch.
- `batch`: maximum messages, bytes, and fetch wait. Processing is ordered per
  worker.
- `parallel`: worker count for higher throughput; it may reorder processing
  within a partition. It is mutually exclusive with `batch`.
- `retry_policy`: exponential backoff followed by an explicit `dlq` or `drop`
  action. Without a terminal action, unresolved input remains uncommitted.
- Output policy: omit output or select `drop`; use `kafka` to publish successful
  handler responses to another topic.

WKS watches the configuration file and applies valid content changes without a
process restart. Unknown fields are rejected. The protobuf definitions under
[`proto/configs`](proto/configs) are the complete schema; the operational and
security details are in [`docs/delivery-and-security.md`](docs/delivery-and-security.md).

## Run and deploy

From source with Bazel:

```bash
bazel run //go/wks/server -- --config_path=/absolute/path/to/config.json
```

Useful endpoints default to:

- `GET :8080/health` — process health.
- `GET :8080/metrics` — Prometheus metrics.
- `:8090` — gRPC health service.

The repository also contains a [Kubernetes Helm chart](services/wks/README.md)
and a ready-to-import [Grafana dashboard](WKS-Grafana.json). The
[release process](docs/releases.md) produces versioned Linux AMD64/ARM64
binaries, GHCR images, and an OCI chart. The quickstart builds from source and
does not require any published artifact.

## Delivery guarantees

WKS provides **at-least-once delivery**. It commits input offsets only after the
handler result is resolved, required Kafka output succeeds, and any explicit
drop or durable DLQ action completes. A crash or partial output/DLQ write before
the input commit can cause duplicates. Handlers should be idempotent, commonly
using topic/partition/offset or a business key for deduplication.

## When not to use WKS

- For a large catalog of database, storage, and SaaS connectors, use Kafka
  Connect or Redpanda Connect.
- For inline mapping, filtering, enrichment, joins, or windows without an
  external service, use Redpanda Connect, Kafka Streams, or Flink.
- For exactly-once processing, stateful stream processing, or Schema Registry
  integration, use a platform designed for those requirements.

WKS is Kafka-in, HTTP-processing, and optionally Kafka-out. It is not intended
to become a general connector or stream-processing framework.

## Project status

WKS is early-stage software. The core delivery path has unit, race, and
real-Kafka integration coverage, but the project has not yet published a stable
configuration compatibility promise or broad managed-Kafka certification.
Feedback and production experience reports are especially valuable.

## Contributing and support

Issues and pull requests are welcome. Contributor guidelines, security
reporting instructions, public releases, and a support policy are still being
prepared before the repository is made public.

## License

Licensed under Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright 2023–2026 Wolveflake LLC (<https://wolveflake.io>).
