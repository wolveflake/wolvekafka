# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.25.1-bookworm AS build

WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/* \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6

COPY go.mod go.sum ./
RUN go mod download

COPY go ./go
COPY proto ./proto
RUN protoc -I . --go_out=. --go_opt=module=wolvekafka \
    proto/configs/cluster.proto \
    proto/configs/consumer.proto \
    proto/configs/configs.proto \
    proto/model/kafka_msg.proto
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wks ./go/wks/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wks /usr/local/bin/wks
ENTRYPOINT ["/usr/local/bin/wks"]
