# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0
"""A WolveKafka handler as a gRPC service."""
from concurrent import futures

import grpc

import handler_pb2
import handler_pb2_grpc


class Handler(handler_pb2_grpc.HandlerServiceServicer):
    def Handle(self, request, context):
        results = []
        for message in request.messages:
            print(f"[python] {message.topic}[{message.partition}]@{message.offset}: "
                  f"{message.value.decode()}", flush=True)  # your business logic
            results.append(handler_pb2.ResponseMessage(status=handler_pb2.STATUS_SUCCESS))
        return handler_pb2.LambdaResponse(messages=results)


# Recycling connections lets WolveKafka pick up new handler replicas.
server = grpc.server(futures.ThreadPoolExecutor(max_workers=8),
                     options=[("grpc.max_connection_age_ms", 5 * 60 * 1000)])
handler_pb2_grpc.add_HandlerServiceServicer_to_server(Handler(), server)
server.add_insecure_port("[::]:9090")
server.start()
print("[python] gRPC handler listening on :9090", flush=True)
server.wait_for_termination()
