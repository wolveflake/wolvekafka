# Observe WKS

[Back to the quickstart](../README.md#quickstart)

```bash
./demo start --observe
./demo retry
```

Open [Grafana](http://127.0.0.1:3000/d/adpmcvw/wks). The optional Compose profile
starts Prometheus, configures a scrape every five seconds from `wks:8080`, and
provisions Grafana's Prometheus data source and the checked-in
[WKS dashboard](../WKS-Grafana.json). No manual dashboard import is needed.

![Local demo dashboard](assets/wks-dashboard.png)

Rate charts need multiple scrapes. A quiet demo has low or zero throughput;
send more events to see activity. The screenshot is a local demonstration,
not a capacity benchmark. No throughput or latency target is claimed.

| Endpoint | Purpose |
| --- | --- |
| <http://127.0.0.1:3000/d/adpmcvw/wks> | Grafana dashboard, anonymous read-only access |
| <http://127.0.0.1:9090/targets> | Verify Prometheus can scrape WKS |
| <http://127.0.0.1:8080/metrics> | Raw Prometheus metrics |
| <http://127.0.0.1:8080/health> | WKS process health, not proof of successful delivery |

The Compose ports bind to loopback. This local demo uses anonymous Grafana
viewing and ephemeral observability storage; configure authentication and
retention for a shared deployment. `./demo stop` stops all profile services.

## Use your own monitoring stack

Configure Prometheus to scrape WKS's `/metrics`, then configure Grafana with
that **Prometheus server** as its data source. Grafana does not directly scrape
the WKS endpoint. Import `WKS-Grafana.json` and bind its panels to your data
source. The example provisioning uses the dashboard's existing data source UID
`bf6mceododw5cf` so it works without editing panel JSON.

See the [Prometheus scrape configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
and [Grafana provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/)
for the corresponding configuration formats. The working local files live in
[`examples/quickstart/observability`](../examples/quickstart/observability).

The [metric reference](reference.md#observability) lists instrumentation names;
the Prometheus endpoint exports names with underscores and counter/unit suffixes.
The dashboard contains queries for the exported names.

## Consumer lag

Lag is how far a consumer group is behind the newest records in Kafka. WKS
reports it two ways, for every partition a WKS process currently owns:

| Prometheus name | Meaning |
| --- | --- |
| `wks_kafka_consumer_lag` | Records written to the partition but not yet committed by the group. |
| `wks_kafka_consumer_lag_seconds` | Age of the oldest of those records, from its Kafka timestamp. Zero when caught up. |
| `wks_kafka_partitions_assigned` | Number of partitions this process owns, per group and topic. |

Records count only when they can still be consumed: a group without a
committed offset is measured from the start of the log, and records already
deleted by retention are not counted.

**How it is measured.** Each worker reads the newest offset of its partitions
from Kafka every `--lag_refresh_interval` (default 15s), and the log start
offset only while some partition has no committed offset. Commits made by WKS
update the gauges immediately. The time lag uses the timestamp of the oldest
record the worker has fetched but not committed, so it keeps growing while a
batch is stuck in retries.

**What to expect.**

- Each process reports only the partitions it owns. Sum across pods for a
  group's total: `sum by (consumer, topic) (wks_kafka_consumer_lag)`.
- After a rebalance, a partition appears under its new owner once that worker
  has read offsets, up to one refresh interval later.
- The time lag is omitted for a partition that has a backlog but whose next
  record has not been fetched yet, for example just after startup. Record lag
  is still reported.
- Record timestamps are set by producers by default, so a producer with a
  wrong clock skews the time lag.
- If offsets cannot be read, WKS logs one warning and keeps the last known
  values until a refresh succeeds.

**Large topics.** One series per partition can be a lot on topics with
thousands of partitions. Start WKS with `--lag_metrics=topic` to report one
series per group and topic instead: record lag is summed and time lag is the
oldest across partitions. `--lag_metrics=off` disables lag reporting and its
offset requests.

The dashboard's **Consumer lag** panels chart both values per group and topic.

## Distributed tracing

WKS creates a `wks.process_batch` span (kind `CONSUMER`) for every batch and
propagates W3C trace context end to end:

- **From Kafka:** WKS reads `traceparent`, `tracestate`, and `baggage` from
  record headers. If every record in the batch carries the same span context
  (always the case for a traced single-record batch), the batch span continues
  that trace as its child. Otherwise the batch span starts a new trace and
  links to each distinct record span context, following the OpenTelemetry
  messaging conventions for batches.
- **To the handler:** the HTTP request carries `traceparent` (plus `tracestate`
  and `baggage` when present) for the batch span, so handler spans join the
  trace. Record headers are still delivered unchanged in the request body, so a
  handler can also continue each record's own trace.

Select the span exporter with the standard OpenTelemetry environment variables:

| Variable | Effect |
| --- | --- |
| `OTEL_TRACES_EXPORTER` | `otlp`, `console`, or `none`. Other values are rejected at startup. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/HTTP endpoint, for example `http://otel-collector:4318`. When set and `OTEL_TRACES_EXPORTER` is unset, OTLP is used. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, … | Other standard OTLP/HTTP exporter settings. |
| `OTEL_SERVICE_NAME` | Service name shown in your tracing backend, for example `wks`. |

Without any of these variables, spans are printed to stdout as before. Set
`OTEL_TRACES_EXPORTER=none` to disable trace export in production if you do not
collect traces.

## Refresh the screenshot

With the observability profile running, send orders and exercise `./demo retry`
and `./demo dlq`. Wait for several scrapes, then run:

```bash
python3 scripts/demo/screenshot.py
```

The capture uses a temporary headless Chrome profile and closes it afterward.
Use `--chrome /path/to/chromium` if Chrome is not found automatically. Review
the saved PNG before committing it; capture real activity rather than changing
dashboard values for the image.
