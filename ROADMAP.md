# Roadmap

WolveKafka is early-stage. This is a direction of travel, not a release
schedule or a compatibility guarantee. Maintainers will discuss concrete
proposals in issues and adjust priorities based on user evidence.

## Current priorities

- Validate the documented delivery and security behavior against additional
  Kafka deployments, especially managed clusters using SASL and TLS.
- Make the first public release and its binary, container, chart, and
  verification workflow usable without private registry access.
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
[when not to use WKS](README.md#when-not-to-use-wks). Proposals in these areas
need a strong case for fitting the project's narrow Kafka-to-HTTP scope.
