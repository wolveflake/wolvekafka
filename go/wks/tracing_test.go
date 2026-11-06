// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

const (
	traceA = "4bf92f3577b34da6a3ce929d0e0e4736"
	spanA  = "00f067aa0ba902b7"
	traceB = "0af7651916cd43dd8448eb211c80319c"
	spanB  = "b7ad6b7169203331"
)

var testPropagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

func tracedMsg(traceID, spanID string, extra ...kafka.Header) *kafka.Message {
	headers := append([]kafka.Header{{Key: "traceparent", Value: []byte("00-" + traceID + "-" + spanID + "-01")}}, extra...)
	return &kafka.Message{Topic: "orders", Headers: headers}
}

func TestBatchSpanParentContinuesSharedTrace(t *testing.T) {
	msgs := []*kafka.Message{
		tracedMsg(traceA, spanA, kafka.Header{Key: "baggage", Value: []byte("tenant=acme")}),
		tracedMsg(traceA, spanA),
	}
	type key struct{}
	base, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "kept"))

	ctx, links := batchSpanParent(base, testPropagator, msgs)

	require.Nil(t, links)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsRemote())
	require.Equal(t, traceA, sc.TraceID().String())
	require.Equal(t, spanA, sc.SpanID().String())
	require.Equal(t, "acme", baggage.FromContext(ctx).Member("tenant").Value())
	require.Equal(t, "kept", ctx.Value(key{}))
	cancel()
	require.Error(t, ctx.Err(), "parent context must keep the batch context's cancellation")
}

func TestBatchSpanParentLinksMixedTraces(t *testing.T) {
	msgs := []*kafka.Message{
		tracedMsg(traceA, spanA),
		tracedMsg(traceB, spanB),
		tracedMsg(traceA, spanA),
		{Topic: "orders"}, // untraced
		nil,
	}
	base := context.Background()

	ctx, links := batchSpanParent(base, testPropagator, msgs)

	require.Equal(t, base, ctx)
	require.Len(t, links, 2)
	require.Equal(t, traceA, links[0].SpanContext.TraceID().String())
	require.Equal(t, traceB, links[1].SpanContext.TraceID().String())
}

func TestBatchSpanParentLinksWhenSomeRecordsAreUntraced(t *testing.T) {
	ctx, links := batchSpanParent(context.Background(), testPropagator,
		[]*kafka.Message{tracedMsg(traceA, spanA), {Topic: "orders"}})

	require.False(t, trace.SpanContextFromContext(ctx).IsValid())
	require.Len(t, links, 1)
}

func TestBatchSpanParentIgnoresUntracedAndMalformedHeaders(t *testing.T) {
	msgs := []*kafka.Message{
		{Topic: "orders"},
		{Topic: "orders", Headers: []kafka.Header{{Key: "traceparent", Value: []byte("not-a-traceparent")}}},
	}
	ctx, links := batchSpanParent(context.Background(), testPropagator, msgs)

	require.Empty(t, links)
	require.False(t, trace.SpanContextFromContext(ctx).IsValid())
}

func TestRecordHeaderCarrierMatchesKeysCaseInsensitively(t *testing.T) {
	carrier := recordHeaderCarrier{{Key: "TraceParent", Value: []byte("v")}, {Key: "other", Value: []byte("x")}}
	require.Equal(t, "v", carrier.Get("traceparent"))
	require.Equal(t, "", carrier.Get("missing"))
	require.Equal(t, []string{"TraceParent", "other"}, carrier.Keys())
	carrier.Set("traceparent", "ignored")
	require.Equal(t, "v", carrier.Get("traceparent"))
}

func TestBatchSpanUsesRecordParentOrLinks(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := &instrumentation{tracer: provider.Tracer("test")}

	parentCtx, links := batchSpanParent(context.Background(), testPropagator, []*kafka.Message{tracedMsg(traceA, spanA)})
	_, span := inst.startBatchSpan(parentCtx, "g", 0, 1, 10, links)
	span.End()

	parentCtx, links = batchSpanParent(context.Background(), testPropagator,
		[]*kafka.Message{tracedMsg(traceA, spanA), tracedMsg(traceB, spanB)})
	_, span = inst.startBatchSpan(parentCtx, "g", 0, 2, 20, links)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 2)

	child := spans[0]
	require.Equal(t, trace.SpanKindConsumer, child.SpanKind())
	require.Equal(t, traceA, child.SpanContext().TraceID().String())
	require.Equal(t, spanA, child.Parent().SpanID().String())
	require.Empty(t, child.Links())

	linked := spans[1]
	require.False(t, linked.Parent().IsValid())
	require.NotEqual(t, traceA, linked.SpanContext().TraceID().String())
	require.NotEqual(t, traceB, linked.SpanContext().TraceID().String())
	require.Len(t, linked.Links(), 2)
}

func TestHandlerRequestCarriesTraceContext(t *testing.T) {
	previousProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(testPropagator)
	t.Cleanup(func() { otel.SetTextMapPropagator(previousProp) })

	previousClient := lambdaHTTPClient
	t.Cleanup(func() { lambdaHTTPClient = previousClient })
	var got http.Header
	lambdaHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		got = req.Header.Clone()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"messages":[{"status":2}]}`))}, nil
	})}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, _ = batchSpanParent(ctx, testPropagator, []*kafka.Message{tracedMsg(traceA, spanA)})
	job := &ProcessingJob{consumerCfg: &configs_pb.Consumer{}, lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://handler/"}})}

	_, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.Equal(t, "00-"+traceA+"-"+spanA+"-01", got.Get("traceparent"))
	require.Equal(t, "application/json", got.Get("Content-Type"))
}

func TestHandlerRequestWithoutTraceHasNoTraceparent(t *testing.T) {
	previousProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(testPropagator)
	t.Cleanup(func() { otel.SetTextMapPropagator(previousProp) })

	previousClient := lambdaHTTPClient
	t.Cleanup(func() { lambdaHTTPClient = previousClient })
	var got http.Header
	lambdaHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		got = req.Header.Clone()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"messages":[{"status":2}]}`))}, nil
	})}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	job := &ProcessingJob{consumerCfg: &configs_pb.Consumer{}, lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://handler/"}})}

	_, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.Empty(t, got.Get("traceparent"))
}
