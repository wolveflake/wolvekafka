# Process order events with WKS

[Back to the project](../../README.md)

Run commands from the repository root. You need Git, Docker with Compose v2,
Bash, and curl; Windows users can use WSL2. Python runs inside containers.

```bash
./demo start
./demo send '{"order_id":"123","event":"order.created"}'
```

The first start downloads images and builds WKS. It creates `orders` and
`orders.dlq`, starts handlers A and B, and verifies an event reaches a handler.
`SUCCESS` in the output is the handler result, not an independent confirmation
that the offset has already been committed.

## Change your handler

Edit `process_order` in [handler.py](handler.py). For example:

```python
def process_order(order):
    print(f"Received order {order['order_id']}", flush=True)
    return SUCCESS
```

Then run `./demo restart-handler` and send another order. `./demo logs` shows
your custom output. Ctrl-C stops log following without stopping the stack.
Return `RETRY` for a temporary failure and `FAILED` for a permanent failure.
The example converts malformed order JSON into `FAILED`.

This is a small demonstration HTTP server, not a production web framework.
The `demo` field simulates failures before `process_order` runs; omit it in
ordinary business events. Retry simulation is process-local, so a retry sent
to another handler can fail once there too. Production side effects need
durable idempotency; the in-memory retry set does not provide it.

## Examples

### Recover from a temporary failure

```bash
./demo route a
./demo retry
```

After routing reloads, the same topic/partition/offset appears first with
`RETRY`, then with `SUCCESS`. WKS applies the configured backoff between calls.

### Isolate an invalid order

```bash
./demo dlq
```

The handler returns `FAILED`. WKS writes the original value and key to
`orders.dlq`, with an `x-processing-error` header, before committing the
input offset. The command reads the new dead-letter record from Kafka.

### Change routing without restarting WKS

```bash
./demo route b
# The command waits for WKS to report a completed configuration reload.
./demo send '{"order_id":"125","event":"order.created"}'
./demo route split
./demo send '{"order_id":"126","event":"order.created"}'
./demo route a
```

Look for `[handler B]` after the first reload. `split` gives each handler equal
probability for each invocation; it does not guarantee alternating events.
`./demo logs` shows WKS applying configuration changes.

`route` updates only `.runtime/config.json`, an ignored copy of
[config.json](config.json). The directory is mounted into WKS so atomic file
replacement remains visible. Edit the runtime copy to explore batching,
backoff, targets, or Kafka output. Unknown configuration fields are rejected.
Changes to the checked-in defaults take effect after reset and a new start.

### Recording

The README animation is generated from actual `./demo send` and `./demo retry`
output, not a benchmark or a simulated service response. Its command transcript
is checked in as [an asciicast](../../docs/assets/wks-demo.cast).
With the stack running and routing set to A, maintainers can recapture it using
`python3 scripts/demo/record.py` (Python is needed on the host only for capture).
The animation repeats and compresses idle time for readability.

## Connect your own Kafka

1. Start with [config.json](config.json). Change `cluster.bootstrap_servers`,
   the consumer group and topics, and `lambdas[].url` to your HTTP endpoint.
2. Ensure broker **advertised listeners** and the handler URL are reachable
   from WKS's network. `localhost` inside a container is that container;
   Docker Desktop exposes host services through `host.docker.internal`.
3. Configure your required Kafka authentication and TLS using the
   [security guide](../../docs/delivery-and-security.md). Keep credentials
   out of version control.
4. Run WKS with that file, for example from source:

   ```bash
   bazel run //go/wks/server -- --config_path=/absolute/path/to/config.json
   ```

Your endpoint must implement the [batch contract](../../docs/reference.md#handler-contract),
including base64 byte fields and one explicit status per input, in input order.
An arbitrary existing REST endpoint is not automatically compatible.
For Kubernetes, use the [Helm chart guide](../../services/wks/README.md).

## Observability

`./demo start --observe` also starts Prometheus and Grafana.
Open <http://127.0.0.1:3000/d/adpmcvw/wks> for the provisioned dashboard.
See [the observability guide](../../docs/observability.md) for how it is wired.

## Troubleshooting and lifecycle

| Symptom | Check |
| --- | --- |
| Cannot connect to Docker | Start Docker Desktop or your Docker daemon. |
| Address already in use | Free ports 8080/8090; observability also uses 3000/9090. |
| Slow first build | Image downloads and Go dependencies need network access; later builds use Docker's cache. |
| Startup or delivery timeout | Inspect `./demo logs` and `docker compose -f examples/quickstart/compose.yaml ps`. |
| Dashboard has no data | Send events, wait for a few 5-second scrapes, and check <http://127.0.0.1:9090/targets>. |
| Edited Python has no effect | Run `./demo restart-handler`. |
| Routing command times out | Inspect WKS logs for rejected config and check `/metrics` is reachable. |

`./demo stop` removes containers, retains the Kafka volume and runtime config,
and stops observability services too. Observability history is ephemeral.
`./demo reset --yes` additionally deletes this demo's Kafka volume and runtime
configuration. It does not delete the example source files.

To use Compose directly, initialize the runtime configuration first:

```bash
mkdir -p examples/quickstart/.runtime
# On a fresh checkout only; do not overwrite runtime edits you want to retain.
cp examples/quickstart/config.json examples/quickstart/.runtime/config.json
docker compose -f examples/quickstart/compose.yaml up --build -d
```

The wrapper adds bounded readiness checks, end-to-end delivery verification,
and readable failure diagnostics around these Compose operations.
