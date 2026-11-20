// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/handler/v1"
)

const (
	// grpcHandleMethod is the method every gRPC handler implements; it is
	// pinned by handler.proto and contract_test.go.
	grpcHandleMethod = "/wolvekafka.handler.v1.HandlerService/Handle"

	// grpcMaxResponseBytes bounds a handler response. Responses carry at most
	// one small result per input record, plus optional output payloads.
	grpcMaxResponseBytes = 64 << 20

	// grpcServiceConfig spreads calls across every address DNS returns for
	// the target, rather than pinning a worker to one handler instance.
	grpcServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`
)

// grpcTransport calls HandlerService.Handle. It keeps one client connection
// per target address, opened on first use and closed with the job.
type grpcTransport struct {
	tls bool

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newGRPCTransport(useTLS bool) *grpcTransport {
	return &grpcTransport{tls: useTLS, conns: map[string]*grpc.ClientConn{}}
}

// grpcTargetAddress returns host:port from a grpc:// or grpcs:// URL.
func grpcTargetAddress(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid gRPC handler url: %w", err)
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("gRPC handler url %q must be scheme://host:port only", u.Redacted())
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("gRPC handler url %q must include a host and port", u.Redacted())
	}
	return u.Host, nil
}

func (t *grpcTransport) conn(rawURL string) (*grpc.ClientConn, error) {
	addr, err := grpcTargetAddress(rawURL)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}
	creds := insecure.NewCredentials()
	if t.tls {
		host, _, _ := net.SplitHostPort(addr)
		creds = credentials.NewTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
	}
	// The dns resolver returns every handler instance (a headless Service in
	// Kubernetes), and round_robin spreads calls across them.
	conn, err := grpc.NewClient("dns:///"+addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxResponseBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for %s: %w", addr, err)
	}
	t.conns[addr] = conn
	return conn, nil
}

func (t *grpcTransport) Send(ctx context.Context, target *configs_pb.Lambda, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
	conn, err := t.conn(target.GetUrl())
	if err != nil {
		return nil, err
	}
	// Let the handler join the batch's trace (traceparent, tracestate, baggage).
	md := metadata.MD{}
	otel.GetTextMapPropagator().Inject(ctx, metadataCarrier(md))
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp := &model_pb.LambdaResponse{}
	if err := conn.Invoke(ctx, grpcHandleMethod, req, resp); err != nil {
		if ctx.Err() != nil {
			// Rebalance, shutdown, or timeout_ms: not a handler verdict.
			return nil, fmt.Errorf("gRPC handler call interrupted: %w", ctx.Err())
		}
		return nil, classifyGRPCError(err, time.Now())
	}
	return resp, nil
}

// classifyGRPCError maps a gRPC status onto WKS's outcomes, matching HTTP:
// RESOURCE_EXHAUSTED is throttling (like 429); UNAVAILABLE with a RetryInfo
// delay is throttling (like 503 with Retry-After); anything else is a failed
// attempt that advances the retry schedule.
func classifyGRPCError(err error, now time.Time) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("gRPC handler call failed: %w", err)
	}
	failure := fmt.Errorf("gRPC handler call failed with %s: %s", st.Code(), st.Message())
	retryAfter, hasRetryAfter := grpcRetryDelay(st)
	switch {
	case st.Code() == codes.ResourceExhausted:
	case st.Code() == codes.Unavailable && hasRetryAfter:
	case st.Code() == codes.Unimplemented, st.Code() == codes.Unauthenticated, st.Code() == codes.PermissionDenied:
		// Retrying cannot fix these; say so plainly in the log.
		return fmt.Errorf("gRPC handler is misconfigured or rejected WKS (%s): %s", st.Code(), st.Message())
	default:
		return failure
	}
	return &throttledError{transport: transportGRPC, status: int(st.Code()), retryAfter: retryAfter, err: failure}
}

// grpcRetryDelay reads the standard google.rpc.RetryInfo detail, capped like
// HTTP Retry-After.
func grpcRetryDelay(st *status.Status) (time.Duration, bool) {
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.RetryInfo)
		if !ok || info.GetRetryDelay() == nil {
			continue
		}
		delay := info.GetRetryDelay().AsDuration()
		if delay < 0 {
			return 0, false
		}
		if delay > maxRetryAfter {
			delay = maxRetryAfter
		}
		return delay, true
	}
	return 0, false
}

func (t *grpcTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var errs []error
	for addr, conn := range t.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing gRPC connection to %s: %w", addr, err))
		}
	}
	t.conns = map[string]*grpc.ClientConn{}
	return errors.Join(errs...)
}

// metadataCarrier adapts gRPC metadata to an OpenTelemetry propagator.
type metadataCarrier metadata.MD

func (c metadataCarrier) Get(key string) string {
	if v := metadata.MD(c).Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c metadataCarrier) Set(key, value string) { metadata.MD(c).Set(strings.ToLower(key), value) }

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
