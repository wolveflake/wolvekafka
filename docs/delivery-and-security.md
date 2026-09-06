# Delivery, Kafka security, and config reload

WKS disables Kafka auto-commit. It holds each batch until handler results are
resolved, successful output is produced, and explicit offset commits succeed.
No later batch can advance past an unresolved batch in that worker. Output or
commit failures retry the current phase without invoking a successful handler
again. Partition rebalances are deferred while a batch is in flight.

Delivery is at least once, not exactly once: a crash after an HTTP side effect,
output write, or DLQ write but before committing input can cause duplicates.
Handlers should deduplicate using topic/partition/offset or a business key.
Partial output writes can also be duplicated on retry.

## HTTP response contract

Return one response entry for every request message, in the same order:

```json
{"messages":[{"status":2},{"status":0},{"status":1}]}
```

- `2` / `STATUS_SUCCESS`: retain the response for optional Kafka output.
- `0` / `STATUS_RETRY`: retry only this unresolved record, retaining successful
  responses from the rest of the batch.
- `1` / `STATUS_FAILED`: do not call the handler again for this record. Apply the
  configured DLQ or explicit drop action immediately. Without either action,
  hold the batch uncommitted until shutdown or a changed configuration is applied.

Invalid JSON, missing/extra response entries, null entries, unknown statuses,
non-2xx HTTP responses, and transport errors fail the attempt. A missing status
uses protobuf's default `STATUS_RETRY`; it is never implicit success.

The exponential schedule includes a retry at each backoff through
`max_backoff_ms`. With a terminal drop/DLQ action, exhausted retries apply that
action only to unresolved records. Without an action, retry continues at the
capped delay (5 seconds without a configured schedule). An action with no
backoff schedule applies after the first failed attempt. Failed DLQ writes are
retried without acknowledging the input or invoking the handler again.

On cancellation, WKS stops processing and leaves unresolved input uncommitted
for replay. A stalled handler or terminal failure can hold its worker's other
partitions; monitor progress and configure a DLQ for poison messages.

## Kafka security

All consumers, output producers, and DLQ producers use the same cluster settings:

```json
{
  "cluster": {
    "bootstrap_servers": ["broker.example.com:9093"],
    "sasl_username": "service-user",
    "sasl_password": "replace-with-secret",
    "sasl_mechanism": "SCRAM-SHA-512",
    "tls": {
      "enabled": true,
      "ca_file": "/etc/kafka/certs/ca.pem",
      "cert_file": "/etc/kafka/certs/client.pem",
      "key_file": "/etc/kafka/certs/client-key.pem"
    }
  }
}
```

Supported mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Credentials
without a mechanism default to `PLAIN`. Both username and password are required.
TLS verifies certificates and hostnames, requires TLS 1.2 or newer, and uses
system roots when `ca_file` is omitted. Client certificate/key files are optional
for mTLS, but must be supplied together. `server_name` optionally overrides the
hostname being verified. Unsupported mechanisms, incomplete credentials,
disabled TLS with TLS options, and invalid certificate files are rejected.

TLS is opt-in to retain compatibility with local plaintext Kafka; enable it for
credentials sent over untrusted networks. Supply credentials through a protected
configuration/Secret file, not a publicly readable ConfigMap. OAuth and AWS IAM
mechanisms are not implemented. Environment-variable interpolation is not supported.

## Reload

Run `wks --config_path /path/to/config.json`. The server checks file contents every
10 seconds, including Kubernetes symlink replacements and files with unchanged
timestamps. Consumer or cluster changes recreate affected workers after the old
workers stop. A cluster-only change (brokers, credentials, or TLS settings)
recreates all consumers and their producers. Invalid updates keep the last valid
configuration. Unknown JSON fields are rejected, including misspelled settings.

Replacing certificate contents at an unchanged path alone does not recreate
clients; change the certificate path in the JSON or restart WKS for rotation.

## Regression tests

```sh
bazel test //go/configs:configs_test //go/kafka:kafka_test //go/wks:wks_test --@rules_go//go/config:race
# With a locally loaded image matching the fixture's /opt/kafka layout:
bazel test //go/itest/wks_e2e:wks_e2e_test \
  --test_env=WKS_TEST_KAFKA_IMAGE=kafka:4.1.0 --test_strategy=standalone
```

The integration fixture uses localhost ports 9092 and 9093. Run its test targets
serially; the fixture creates and removes its own Kafka containers.
