# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

# Build context is the repository root, so the image uses the same
# handler.proto that WolveKafka is built from.
FROM golang:1.25.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.8 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
WORKDIR /src
COPY examples/grpc/go/go.mod examples/grpc/go/go.sum ./
RUN go mod download
COPY proto/handler/v1/handler.proto /proto/
# Map handler.proto into this module instead of WolveKafka's go_package.
RUN mkdir handlerv1 && protoc -I /proto \
    --go_out=handlerv1 --go_opt=paths=source_relative --go_opt=Mhandler.proto=example.com/grpc-handler/handlerv1 \
    --go-grpc_out=handlerv1 --go-grpc_opt=paths=source_relative --go-grpc_opt=Mhandler.proto=example.com/grpc-handler/handlerv1 \
    handler.proto
COPY examples/grpc/go/main.go .
RUN CGO_ENABLED=0 go build -o /out/handler .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/handler /handler
ENTRYPOINT ["/handler"]
