# WolveKafka (WKS) — honest competitive positioning

Draft for deciding whether/how to open-source WKS. Written from the code, not the pitch.

## What WKS actually is (precisely)

A **declarative Kafka consumer runtime**. You describe consumers in JSON; WKS runs the
consumer groups and, for each message/batch, POSTs it to an HTTP endpoint you own (a
"lambda"), then optionally produces the response to another Kafka topic.

Concretely, from the code:

- **Flow:** `Kafka topic(s) → WKS consumer group → HTTP POST to your handler → (optional) produce response to a Kafka topic`.
- **Handler contract:** batch `LambdaRequest { messages[] }` in, `LambdaResponse { messages[] { status, message } }` out. `status` (`SUCCESS`/`RETRY`/`FAILED`) drives commit vs retry vs DLQ.
- **Delivery:** at-least-once — offsets are committed *after* the handler succeeds (franz-go `MarkCommitRecords` + `CommitMarkedOffsets`).
- **Per-consumer config:** weighted multi-target selection, exponential-backoff retry, DLQ topic or drop, batch mode (size/bytes/wait) or parallel mode (N workers), output drop-or-produce with header copy.
- **Ops:** single Go binary; **hot-reload** of consumer config from a mounted file/ConfigMap (add/change consumers with no redeploy); **OpenTelemetry metrics** + a ready **Grafana dashboard**.
- **Honest framing:** it does not hide Kafka — you bring your cluster, keep your tooling and guarantees.

## The one-line positioning that's actually defensible

> **Turn any HTTP service into a production Kafka consumer** — retries, DLQ, batching,
> weighted routing, and observability built in. No consumer code, no JVM, no Connect cluster.

That is the narrow wedge where WKS wins cleanly. It is **not** a general-purpose stream
processor, and pitching it as one invites a fight it loses (see Redpanda Connect below).

## Head-to-head

### vs Kafka Connect (Confluent) — *WKS's strongest contrast*
Connect is the incumbent declarative Kafka integration layer: a cluster running JVM
connector plugins, hundreds of source/sink connectors, converters, and SMTs.
- **WKS wins:** custom logic is a plain HTTP endpoint in *any* language — no JVM, no
  plugin packaging, no Connect cluster to operate. Single binary. Hot-reload config.
  Metrics/dashboard out of the box. Minutes to a working consumer vs hours wiring Connect.
- **WKS loses:** no connector ecosystem (WKS is Kafka-in / Kafka-out only — no S3, DB,
  Elasticsearch, etc.); no Schema Registry / Avro-Protobuf handling; no SMT transform
  library; far less scale pedigree. Connect's HTTP Sink also exists (though it's a
  licensed/limited connector) — so lead with the JVM-free, any-language, zero-ops story.

### vs Redpanda Connect (formerly Benthos) — *the hardest comp; be honest*
A mature single-binary stream processor: 200+ connectors, rich in-stream processing
(Bloblang mapping, transforms, enrichment, windowing) with **no external service**, great
docs, large community, owned by Redpanda.
- **WKS wins (narrowly):** when your processing *is a real service you want to own and
  scale independently* (a model server, an existing microservice, business logic that
  won't fit in config), WKS's "your logic is a first-class HTTP service, invoked per
  message with weighted routing + retry/DLQ" model is cleaner than expressing it in
  Bloblang. Batteries-included Grafana dashboard is a nicer first-run than Benthos.
- **WKS loses (broadly):** Benthos does Kafka→HTTP→Kafka *too*, plus hundreds of other
  connectors and full in-stream transforms without a network hop. For routing/transform/
  enrichment workloads, Benthos wins decisively on capability, maturity, and community.
  **Do not** position WKS as "a better Benthos." Position it as "for when your logic is a
  service, not a config expression."

### vs cloud FaaS event-source mappings (AWS Lambda–Kafka ESM, etc.)
Managed "Kafka → your function."
- **WKS wins:** cloud-agnostic, any runtime, your handler can be a long-running service
  (not a cold-starting FaaS); works with any Kafka; no per-invocation cloud lock-in.
- **WKS loses:** no serverless scale-to-zero; you operate it.

### vs Numaflow / roll-your-own consumer + KEDA
- **Numaflow** (CNCF, K8s-native pipelines with language-agnostic UDF containers) is the
  closest philosophically; it's heavier/pipeline-oriented, WKS is lighter/single-purpose.
- **Roll-your-own + KEDA:** KEDA scales on lag but you still hand-write the consumer,
  retries, DLQ, and metrics. WKS's whole value is deleting that code. This "we already
  hand-roll consumers and hate it" crowd is the **primary adoption target**.

## Where WKS genuinely wins (the pitch, distilled)

1. **Time-to-first-consumer:** a config block + an HTTP handler in any language, vs writing
   a Kafka client or a Connect plugin.
2. **Language-agnostic custom logic** without JVM or plugin packaging.
3. **Ops UX:** single binary, hot-reload config (no redeploy to add/modify consumers),
   Prometheus + Grafana included.
4. **Declarative reliability:** retry, DLQ, weighted routing, batch/parallel as config.
5. **Honest "it's just Kafka":** keep your cluster, tooling, and guarantees.

## Gaps that will get flamed on HN — fix or disclose *before* launch

1. **🔴 Cannot connect to secured/cloud Kafka.** `sasl_username`/`sasl_password` exist in
   the config schema but are **never wired into the client** — the franz-go client sets
   only `SeedBrokers` + `ConsumerGroup`, no `kgo.SASL(...)` and no TLS. So WKS talks only
   to **plaintext, unauthenticated** Kafka today. It cannot reach Confluent Cloud, MSK,
   Aiven, or Redpanda Cloud. Advertising SASL fields that silently do nothing is worse
   than omitting them. **This is table stakes and must ship in v0.** (SASL/PLAIN + SCRAM +
   TLS/SASL_SSL, ideally mTLS and AWS-MSK-IAM.)
2. **No connector ecosystem** — Kafka-in/Kafka-out only. Say so plainly; it's the scope,
   not a bug, but reviewers will compare to Benthos/Connect connector counts.
3. **No in-stream transforms / Schema Registry** — every transform is an HTTP round-trip;
   no Avro/Protobuf deserialization. Fine for the wedge, a dealbreaker outside it.
4. **At-least-once only** — no exactly-once/transactions. Acceptable, but competitors
   advertise EOS; document the guarantee and the dedup expectation on handlers.
5. **Ordering under `parallel` mode** — parallelism can reorder within a partition;
   document the batch-mode (ordered) vs parallel-mode (throughput, unordered) tradeoff
   explicitly, or users will hit it in prod.
6. **Handler DX friction** — the contract is protojson with base64 `value`/`message` and
   numeric enums. Workable, but a raw-body / plaintext mode would make the "5-minute
   handler in any language" story much stronger. Consider before launch.
7. **Licensing** — every source file carries a `CONFIDENTIAL / All Rights Reserved`
   header. To open-source you must relicense (Apache-2.0 or MIT) and strip those headers
   repo-wide. Non-negotiable pre-launch step.
8. **Maturity/community** — one company, unproven at scale, no docs site. Set expectations
   with a clear "status: early / not yet battle-tested" and a public roadmap.

## Recommendation

Open-sourcing WKS is a legitimate developer-distribution move **if** the company is
committing to a developer-facing wedge. To make the launch land:

- **Position narrowly:** "any HTTP service → production Kafka consumer." Cede transforms
  and connectors to Benthos/Connect explicitly (a "When *not* to use WKS" section earns
  trust and pre-empts the top HN comment).
- **Ship the must-fixes first:** SASL+TLS (so it works with real clusters), relicense, and
  a friction-free handler mode. Without SASL/TLS the launch fails on contact with anyone
  who runs managed Kafka — i.e. most of the audience.
- **Lead the README with a 5-minute quickstart** that beats Connect's and Benthos's
  first-five-minutes, and show the Grafana dashboard as the "wow." (Draft in `README.oss.md`.)
- Then WolveFlow is the natural "now orchestrate these consumers into workflows" upsell —
  a coherent OSS-led funnel: **WKS (adoption) → WolveFlow (orchestration) → Constructor (monetize)**.

The teardown's blunt version: the wedge is real but narrow, and the single most important
thing between here and a launch that generates the interest you want is **making WKS
actually connect to the Kafka clusters your audience runs.**
