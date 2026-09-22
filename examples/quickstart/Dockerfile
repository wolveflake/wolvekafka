# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

# The same build produces local quickstart images, release binaries, and GHCR
# manifests. Both base images and the protoc distribution are digest-pinned.
FROM --platform=$BUILDPLATFORM golang:1.25.1-bookworm@sha256:c423747fbd96fd8f0b1102d947f51f9b266060217478e5f9bf86f145969562ee AS build

ARG TARGETOS
ARG TARGETARCH
ARG BUILDARCH
ARG VERSION=dev
ARG VCS_REF=unknown

WORKDIR /src
ADD https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-linux-x86_64.zip /tmp/protoc-amd64.zip
ADD https://github.com/protocolbuffers/protobuf/releases/download/v33.0/protoc-33.0-linux-aarch_64.zip /tmp/protoc-arm64.zip
COPY scripts/release/extract-protoc.go /tmp/extract-protoc.go
RUN echo 'd99c011b799e9e412064244f0be417e5d76c9b6ace13a2ac735330fa7d57ad8f  /tmp/protoc-amd64.zip' | sha256sum -c - \
    && echo '4b96bc91f8b54d829b8c3ca2207ff1ceb774843321e4fa5a68502faece584272  /tmp/protoc-arm64.zip' | sha256sum -c - \
    && go run /tmp/extract-protoc.go "/tmp/protoc-${BUILDARCH}.zip" /usr/local \
    && test -f /usr/local/include/google/protobuf/any.proto \
    && rm /tmp/protoc-*.zip \
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
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false \
    -ldflags="-s -w -buildid= -X main.version=$VERSION -X main.commit=$VCS_REF" \
    -o /out/wks ./go/wks/server

FROM scratch AS binary
COPY --from=build /out/wks /wks

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG VERSION=dev
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="WolveKafka" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$VCS_REF" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/dguptawolveflake/wolvekafka"
COPY --from=build /out/wks /usr/local/bin/wks
ENTRYPOINT ["/usr/local/bin/wks"]
