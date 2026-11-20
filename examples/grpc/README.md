# gRPC handlers

[Back to the project](../../README.md) ·
[gRPC reference](../../docs/reference.md#grpc-handlers)

Two handlers, one in [Python](python/server.py) and one in [Go](go/main.go),
implement the gRPC `HandlerService` from
[`handler.proto`](../../proto/handler/v1/handler.proto). WolveKafka splits the
`orders` topic between them 50/50 ([config.json](config.json)).

Run from this directory (Docker with Compose v2):

```bash
docker compose up -d --build
for i in 1 2 3 4 5 6; do
  echo "{\"order_id\":\"$i\"}" | docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic orders
done
docker compose logs handler-python handler-go
```

Each order shows up in one of the two handlers:

```text
handler-python-1  | [python] orders[0]@0: {"order_id":"1"}
handler-go-1      | 2026/11/20 06:51:15 [go] orders[0]@1: {"order_id":"2"}
```

WolveKafka picks a handler per batch, not per message. The loop sends each
order separately so they land in different batches; orders sent together
arrive at one handler as one batch.

`docker compose down` stops everything.

## Write your own

A gRPC handler implements one method. It receives a batch and returns one
`ResponseMessage` per input message, in order, with `STATUS_SUCCESS`,
`STATUS_RETRY`, or `STATUS_FAILED`, exactly as an HTTP handler does.

1. Copy `proto/handler/v1/handler.proto`. It has no imports.
2. Generate code for your language. The Dockerfiles show the commands:
   `grpc_tools.protoc` for [Python](python/Dockerfile), `protoc` with
   `protoc-gen-go` and `protoc-gen-go-grpc` for [Go](go/Dockerfile). Go users
   map the file into their own module with `--go_opt=Mhandler.proto=...`.
3. Point a lambda at it: `{"url": "grpc://my-handler:9090"}`, or `grpcs://` for
   TLS.

To slow WolveKafka down, return `RESOURCE_EXHAUSTED` (optionally with a
`RetryInfo` delay). See the [status mapping](../../docs/reference.md#grpc-handlers)
and the deployment notes there: run handlers behind a headless Service and set a
maximum connection age, as both examples do.
