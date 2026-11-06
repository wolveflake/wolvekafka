# WKS reference

[Back to the quickstart](../README.md#quickstart)

## Handler contract

WKS sends a batch as protobuf JSON. Kafka byte fields such as `key`, `value`,
and header values are base64 encoded:

```json
{
  "messages": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": "0",
      "headers": [],
      "key": "",
      "value": "eyJvcmRlcl9pZCI6IjEyMyJ9",
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

If the handler is overloaded, respond with HTTP `429` (optionally with
`Retry-After`) instead of per-record statuses. WKS waits and retries without
using up the retry budget; see [throttling](delivery-and-security.md#throttling-429-and-retry-after).

Always return an explicit status. An omitted status is protobuf enum value zero,
which means retry—not success. See [delivery, retry, and migration semantics](delivery-and-security.md)
before using WKS with production traffic.

Stripped of HTTP boilerplate, that is the whole of a handler:

```python
request = json.loads(body)
results = []

for message in request["messages"]:
    process(base64.b64decode(message["value"]))  # your logic
    results.append({"status": 2})                # 2 = SUCCESS

return json.dumps({"messages": results})
```

The runnable version is [`examples/quickstart/handler.py`](../examples/quickstart/handler.py).

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
[`proto/configs`](../proto/configs) are the complete schema; the operational and
security details are in [`docs/delivery-and-security.md`](delivery-and-security.md).

## Delivery, ordering, and failure handling

WKS provides **at-least-once delivery**. It commits input offsets only after the
handler result is resolved, required Kafka output succeeds, and any explicit
drop or durable DLQ action completes. A crash or partial output/DLQ write before
the input commit can cause duplicates. Handlers should be idempotent, commonly
using topic/partition/offset or a business key for deduplication.

Ordering depends on the execution mode you choose. In `batch` mode records are
processed in order. `parallel` mode trades that for throughput: workers may
process records from the same partition concurrently, so per-partition order is
not preserved. Pick `batch` when downstream correctness depends on order.

Rebalances, partial handler results, output failures, and restart/replay
behavior are covered by unit, race, and real-Kafka integration tests.

## Observability

WKS exports OpenTelemetry metrics and serves them for Prometheus on
`:8080/metrics`, where they appear with underscore separators:

| Metric | Meaning |
| --- | --- |
| `wks.kafka.messages_processed` | Records successfully processed and committed |
| `wks.kafka.messages_dead_lettered` | Records dead-lettered and committed |
| `wks.kafka.messages_dropped` | Records explicitly dropped and committed |
| `wks.lambda.duration_ms` | Handler invocation latency, as a histogram |
| `wks.lambda.failures`, `wks.lambda.retries` | Handler errors and retry attempts |
| `wks.lambda.throttled` | Handler throttling responses (429, or 503 with `Retry-After`), by `http.status_code` |
| `wks.kafka.commit_errors`, `wks.output.errors` | Offset commit and output policy failures |
| `wks.consumers.configured`, `wks.jobs.inflight` | Active consumer groups and in-flight jobs |
| `wks.config.applied_total` | Configuration reloads applied at runtime |

[`WKS-Grafana.json`](../WKS-Grafana.json) is a ready-to-import dashboard built on
these metrics.

WKS also propagates W3C trace context from Kafka record headers to the handler
request (`traceparent`) and can export spans over OTLP. See
[distributed tracing](observability.md#distributed-tracing).
