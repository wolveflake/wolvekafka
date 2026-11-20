// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/handler/v1"
)

type grpcHandleFunc func(ctx context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error)

// startGRPCHandler serves HandlerService on a local port and returns its
// grpc:// URL. It registers the service by hand, as any handler could.
func startGRPCHandler(t *testing.T, fn grpcHandleFunc) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "wolvekafka.handler.v1.HandlerService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Handle",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req := &model_pb.LambdaRequest{}
				if err := dec(req); err != nil {
					return nil, err
				}
				return fn(ctx, req)
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return "grpc://" + lis.Addr().String()
}

func allSucceeded(req *model_pb.LambdaRequest) *model_pb.LambdaResponse {
	resp := &model_pb.LambdaResponse{}
	for range req.GetMessages() {
		resp.Messages = append(resp.Messages, &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_SUCCESS})
	}
	return resp
}

func throttledStatus(t *testing.T, code codes.Code, delay time.Duration) error {
	st, err := status.New(code, "busy").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	require.NoError(t, err)
	return st.Err()
}

func grpcJob(t *testing.T, url string, timeoutMs uint32) *ProcessingJob {
	job := &ProcessingJob{
		consumerCfg:    retryTestPolicy(),
		lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: url, TimeoutMs: timeoutMs}}),
		transports:     newHandlerTransports(),
	}
	t.Cleanup(func() { _ = job.transports.Close() })
	return job
}

func TestGRPCHandlerReceivesBatchAndTraceContext(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	var gotTraceparent string
	var gotMessages int
	url := startGRPCHandler(t, func(ctx context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if v := md.Get("traceparent"); len(v) > 0 {
			gotTraceparent = v[0]
		}
		gotMessages = len(req.GetMessages())
		return allSucceeded(req), nil
	})
	job := grpcJob(t, url, 0)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	result, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Value: []byte("a")}, {Value: []byte("b")}}})

	require.NoError(t, err)
	require.Len(t, result.Response.Messages, 2)
	require.Equal(t, 2, gotMessages)
	require.Contains(t, gotTraceparent, sc.TraceID().String())
}

func TestGRPCResourceExhaustedIsThrottledNotFailed(t *testing.T) {
	var calls atomic.Int32
	url := startGRPCHandler(t, func(_ context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		// More throttles than the retry schedule allows failures: a busy
		// handler must not push records to the drop action.
		if calls.Add(1) <= 4 {
			return nil, throttledStatus(t, codes.ResourceExhausted, 10*time.Millisecond)
		}
		return allSucceeded(req), nil
	})
	job := grpcJob(t, url, 0)

	result, err := defaultSendEventFunc(job, context.Background(), &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.EqualValues(t, 5, calls.Load())
	require.Zero(t, result.Dropped)
	require.Len(t, result.Response.Messages, 1)
}

func TestGRPCCallHonorsTimeoutMs(t *testing.T) {
	var serverDeadline atomic.Int64 // nanoseconds; written by the server goroutine
	var calls atomic.Int32
	url := startGRPCHandler(t, func(ctx context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		if calls.Add(1) == 1 {
			deadline, _ := ctx.Deadline()
			serverDeadline.Store(int64(time.Until(deadline)))
			<-ctx.Done() // never answers
			return nil, ctx.Err()
		}
		return allSucceeded(req), nil
	})
	job := grpcJob(t, url, 200)

	start := time.Now()
	result, err := defaultSendEventFunc(job, context.Background(), &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load(), "the timed-out call is retried")
	require.Positive(t, serverDeadline.Load(), "the deadline reaches the handler")
	require.LessOrEqual(t, time.Duration(serverDeadline.Load()), 200*time.Millisecond)
	require.Less(t, time.Since(start), 3*time.Second)
	require.Len(t, result.Response.Messages, 1)
}

func TestGRPCCancellationIsNotAHandlerFailure(t *testing.T) {
	started := make(chan struct{})
	url := startGRPCHandler(t, func(ctx context.Context, _ *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	tr := newGRPCTransport(false)
	defer tr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()

	_, err := tr.Send(ctx, &configs_pb.Lambda{Url: url}, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.ErrorIs(t, err, context.Canceled)
	var throttled *throttledError
	require.False(t, errors.As(err, &throttled))
}

func TestClassifyGRPCError(t *testing.T) {
	now := time.Now()
	var throttled *throttledError

	err := classifyGRPCError(throttledStatus(t, codes.ResourceExhausted, 2*time.Second), now)
	require.ErrorAs(t, err, &throttled)
	require.Equal(t, 2*time.Second, throttled.retryAfter)
	require.Equal(t, "gRPC ResourceExhausted", throttled.describe())

	err = classifyGRPCError(status.Error(codes.ResourceExhausted, "busy"), now)
	require.ErrorAs(t, err, &throttled, "RESOURCE_EXHAUSTED throttles even without RetryInfo, like 429")
	require.Zero(t, throttled.retryAfter)

	err = classifyGRPCError(throttledStatus(t, codes.Unavailable, time.Hour), now)
	require.ErrorAs(t, err, &throttled, "UNAVAILABLE with RetryInfo is like 503 with Retry-After")
	require.Equal(t, maxRetryAfter, throttled.retryAfter, "long delays are capped")

	for _, code := range []codes.Code{codes.Unavailable, codes.Internal, codes.Unknown, codes.Aborted, codes.DeadlineExceeded, codes.InvalidArgument} {
		err = classifyGRPCError(status.Error(code, "boom"), now)
		require.False(t, errors.As(err, &throttled), code.String())
		require.ErrorContains(t, err, code.String())
	}
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unauthenticated, codes.PermissionDenied} {
		err = classifyGRPCError(status.Error(code, "nope"), now)
		require.False(t, errors.As(err, &throttled))
		require.ErrorContains(t, err, "misconfigured")
	}
}

func TestGRPCTransportReusesAndClosesConnections(t *testing.T) {
	url := startGRPCHandler(t, func(_ context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		return allSucceeded(req), nil
	})
	tr := newGRPCTransport(false)
	first, err := tr.conn(url)
	require.NoError(t, err)
	second, err := tr.conn(url + "/")
	require.NoError(t, err)
	require.Same(t, first, second)
	require.NoError(t, tr.Close())
	require.Empty(t, tr.conns)

	_, err = tr.conn("grpc://handler")
	require.ErrorContains(t, err, "host and port")
}

func TestTransportForGRPCURLs(t *testing.T) {
	require.Equal(t, transportGRPC, transportForURL("grpc://handler:9090"))
	require.Equal(t, transportGRPCS, transportForURL("GRPCS://handler:443"))
}
