<!-- DRAFT OSS README for WolveKafka (WKS). Replace the existing demo README.md at launch.
     Pre-launch blockers before this is true: wire SASL+TLS (see docs/positioning-vs-competitors.md),
     relicense to Apache-2.0/MIT, and confirm the quickstart end-to-end. -->

# WolveKafka (WKS)

**Turn any HTTP service into a production Kafka consumer.** Retries, dead-letter queues,
batching, weighted routing, and observability — built in. No consumer code, no JVM, no
Kafka Connect cluster.

WKS runs your Kafka consumer groups and, for each message, calls an HTTP endpoint you
write in *any* language. You keep your Kafka cluster and your tooling — WKS doesn't hide
Kafka, it just deletes the boilerplate around it.

```
 Kafka topic ──▶ WKS (consumer group, retries, DLQ, metrics) ──▶ your HTTP handler
                                    │
                                    └──▶ (optional) produce the response to another topic
```

## Why

Writing a *correct* Kafka consumer means hand-rolling offset management, retry/backoff,
dead-lettering, batching, graceful shutdown, and metrics — for every service, in every
language, forever. WKS makes all of that declarative config and lets your code be a plain
HTTP handler:

| Without WKS | With WKS |
|---|---|
| Kafka client + offset commit logic per service | A `POST` handler that returns `{status}` |
| Custom retry/backoff + DLQ wiring | `retry_policy` + `dlq` in config |
| Consumer written in one JVM/Go stack | Handler in Python, Node, Go, Rust… anything that speaks HTTP |
| Redeploy to add/change a consumer | Edit config — **hot-reloaded**, no redeploy |
| Bolt on Prometheus + build dashboards | Metrics + Grafana dashboard included |

## When *not* to use WKS

Honesty up front so you pick the right tool:

- Need **in-stream transforms, routing, or enrichment** without a service (mapping,
  filtering, joins)? Use **[Redpanda Connect / Benthos](https://www.benthos.dev/)**.
- Need a big library of **source/sink connectors** (S3, databases, Elasticsearch, …)?
  Use **Kafka Connect** or Benthos.
- Need **exactly-once** semantics or stateful stream processing (windows, aggregations)?
  Use **Kafka Streams / Flink**.

WKS is for one thing: **your business logic already is (or wants to be) an HTTP service,
and you want it consuming Kafka reliably without writing consumer plumbing.**

## 5-minute quickstart

> Requires Docker. This spins up a single-broker Kafka (Redpanda), a tiny handler, and WKS.

**1. Start a broker** (`docker-compose.yml`):

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command: redpanda start --smp 1 --overprovisioned --node-id 0 --check=false
             --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://redpanda:9092
    ports: ["9092:9092"]
```

```bash
docker compose up -d
```

**2. Write your handler** — any language. It receives a batch of Kafka messages and returns
a status per message. `value` is base64-encoded bytes (Kafka values are bytes); `status`
is `2`=SUCCESS, `0`=RETRY, `1`=FAILED (names like `"STATUS_SUCCESS"` also accepted).

```python
# handler.py  —  python3 -m http.server won't cut it; use Flask/FastAPI or stdlib
import base64, json
from http.server import BaseHTTPRequestHandler, HTTPServer

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        out = []
        for m in req.get("messages", []):
            value = base64.b64decode(m.get("value", "")).decode("utf-8", "replace")
            print(f"got {m['topic']}[{m['partition']}]@{m['offset']}: {value}", flush=True)
            out.append({"status": 2})           # SUCCESS → commit offset
        self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers()
        self.wfile.write(json.dumps({"messages": out}).encode())

HTTPServer(("0.0.0.0", 8081), H).serve_forever()
```

```bash
python3 handler.py    # listens on :8081
```

**3. Configure WKS** (`config.json`) — point it at the broker and your handler:

```json
{
  "cluster": { "bootstrap_servers": ["localhost:9092"] },
  "consumers": [
    {
      "group_id": "quickstart",
      "topics": ["demo-topic"],
      "lambdas": [{ "url": "http://localhost:8081/" }],
      "retry_policy": {
        "exponential_backoff": { "initial_backoff_ms": 100, "max_backoff_ms": 5000, "multiplier": 2 },
        "dlq": { "topic": "demo-topic.dlq" }
      },
      "batch": { "max_messages": 100, "max_wait_ms": 1000 }
    }
  ]
}
```

**4. Run WKS:**

```bash
# via the released binary / image
wks --config ./config.json
# or from source: bazel run //go/wks/server -- --config ./config.json
```

**5. Produce a message and watch it flow:**

```bash
echo '{"hello":"kafka"}' | docker compose exec -T redpanda rpk topic produce demo-topic
```

Your handler prints the message; WKS commits the offset. Send a `{"status":0}` back to see
retry/backoff, and after exhaustion the message lands in `demo-topic.dlq`. That's a
production-shaped consumer — retries, DLQ, batching, metrics — and you wrote ~15 lines.

**6. See the dashboard:** WKS exposes Prometheus metrics (throughput, handler latency,
failures, retries, commit errors, in-flight). Import `WKS-Grafana.json` into Grafana
pointed at your Prometheus to get the prebuilt dashboard.

## Configuration reference (essentials)

- **`cluster.bootstrap_servers`** — your Kafka brokers.
  <!-- LAUNCH BLOCKER: add sasl_username/password + TLS wiring; today only PLAINTEXT works. -->
- **`consumers[]`**
  - `group_id`, `topics[]`
  - `lambdas[]` — `{ url, weight }`; multiple targets are load-balanced by `weight`.
  - `retry_policy` — `exponential_backoff` + action: `drop` or `dlq { topic }`.
  - processing mode — `batch { max_messages, max_bytes, max_wait_ms }` (ordered) **or**
    `parallel { parallelism }` (higher throughput; may reorder within a partition).
  - output — omit/`drop`, or `kafka { topic, copy_headers, skip_empty_message }` to
    produce the handler's response to another topic.

Config is **hot-reloaded**: edit the file (or ConfigMap) and WKS applies changes without a
restart.

## Delivery guarantees

**At-least-once.** Offsets commit only after your handler returns success, so a crash
mid-flight redelivers. Make handlers idempotent (dedupe on `topic/partition/offset` or a
business key).

## Status

Early. Kafka in / Kafka out with an HTTP processing hop is the whole scope today — no
connector library, no in-stream transforms, no Schema Registry. See
[`docs/positioning-vs-competitors.md`](docs/positioning-vs-competitors.md) for an honest
comparison and roadmap. Issues and PRs welcome.

## License

<!-- LAUNCH BLOCKER: relicense repo to Apache-2.0 (recommended) or MIT and strip the
     "CONFIDENTIAL / All Rights Reserved" headers before publishing. -->
TBD (Apache-2.0 intended).
