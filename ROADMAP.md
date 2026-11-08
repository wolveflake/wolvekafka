# Roadmap

WolveKafka is early-stage. This is a direction of travel, not a release
schedule or a compatibility guarantee. Maintainers will discuss concrete
proposals in issues and adjust priorities based on user evidence.

## Current priorities

- Validate the documented delivery and security behavior against additional
  Kafka deployments, especially managed clusters using SASL and TLS.
- Early-user requests: [per-key ordered fan-out](https://github.com/wolveflake/wolvekafka/issues/96)
  and a [gRPC handler transport](https://github.com/wolveflake/wolvekafka/issues/97).
- Improve the first-run experience with examples and troubleshooting for
  handlers, retries, DLQs, and configuration reloads.
- Keep regression coverage strong for rebalances, partial handler results,
  output failures, and restart/replay behavior.

## Explore with contributors

- Easier handler development without obscuring Kafka's bytes, offsets, or
  at-least-once delivery semantics.
- Better configuration validation and clearer operational diagnostics.
- Additional managed-Kafka authentication mechanisms where users provide
  concrete integration requirements and tests.

## Deliberate non-goals

WKS is not a general connector catalog, in-process stream processor,
Schema Registry client, or exactly-once transaction engine. See
[fit and project status](README.md#fit-and-project-status). Proposals in these areas
need a strong case for fitting the project's narrow Kafka-to-HTTP scope.
