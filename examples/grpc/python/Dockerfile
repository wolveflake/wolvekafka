# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

# Build context is the repository root, so the image uses the same
# handler.proto that WolveKafka is built from.
FROM python:3.13.7-slim-bookworm
WORKDIR /app
COPY examples/grpc/python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/handler/v1/handler.proto /proto/
RUN python -m grpc_tools.protoc -I /proto --python_out=. --grpc_python_out=. handler.proto
COPY examples/grpc/python/server.py .
CMD ["python", "-u", "server.py"]
