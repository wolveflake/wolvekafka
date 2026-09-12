// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRetryReselectsHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	job := &ProcessingJob{consumerCfg: retryTestPolicy(), lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://failed/"}})}
	previous := lambdaHTTPClient
	t.Cleanup(func() { lambdaHTTPClient = previous })
	calls := 0
	lambdaHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			require.Equal(t, "failed", req.URL.Host)
			job.lambdaSelector = NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://healthy/"}})
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
		}
		require.Equal(t, "healthy", req.URL.Host)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"messages":[{"status":2}]}`))}, nil
	})}
	result, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Len(t, result.Response.Messages, 1)
}

func TestMissingAndZeroStatusNeverBecomeSuccess(t *testing.T) {
	for _, body := range []string{`{"messages":[{}]}`, `{"messages":[{"status":0}]}`} {
		t.Run(body, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			response, err := ParseLambdaResponse([]byte(body))
			require.NoError(t, err)
			policy := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: &configs_pb.Consumer{}})
			result, err := policy.ExecuteBatch(ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}}, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) { return response, nil })
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, result)
		})
	}
}

func TestTerminalRecordsUseOneBatchedDLQRetryLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := retryTestPolicy()
	cfg.RetryPolicy.Action = &configs_pb.RetryPolicy_Dlq{Dlq: &configs_pb.DLQAction{Topic: "dead", RetryPolicy: &configs_pb.DLQAction_ExponentialBackoff{ExponentialBackoff: &configs_pb.ExponentialBackoff{InitialBackoffMs: 1, MaxBackoffMs: 1}}}}
	writes, calls := 0, 0
	producer := &kafka.Producer{ProduceBatchFunc: func(messages []*kafka.Message) error {
		writes++
		require.Len(t, messages, 1000)
		require.Equal(t, "dead", messages[0].Topic)
		if writes == 1 {
			return fmt.Errorf("broker unavailable")
		}
		return nil
	}}
	policy := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: cfg, DLQProducer: producer})
	req, resp := &model_pb.LambdaRequest{}, &model_pb.LambdaResponse{}
	for i := 0; i < 1000; i++ {
		req.Messages = append(req.Messages, &model_pb.KafkaMessage{})
		resp.Messages = append(resp.Messages, &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_FAILED})
	}
	result, err := policy.ExecuteBatch(ctx, req, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		return resp, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, writes)
	require.Equal(t, 1, calls)
	require.Equal(t, 1000, result.DeadLettered)
	require.Empty(t, result.Response.Messages)
}

func TestOutcomeMetricsSeparateSuccessDLQAndDrop(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(ctx)
	meter := provider.Meter("test")
	metrics := &instrumentation{messagesProcessed: buildInt64Counter(meter, "success", ""), messagesDeadLettered: buildInt64Counter(meter, "dlq", ""), messagesDropped: buildInt64Counter(meter, "drop", "")}
	metrics.recordBatchOutcomes(ctx, "group", 0, &BatchResult{Response: &model_pb.LambdaResponse{Messages: make([]*model_pb.ResponseMessage, 7)}, DeadLettered: 2, Dropped: 1})
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &collected))
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
				counts[metric.Name] += point.Value
			}
		}
	}
	require.Equal(t, map[string]int64{"success": 7, "dlq": 2, "drop": 1}, counts)
}

func TestStopConsumersCancelsAllGroupsAndBoundsWait(t *testing.T) {
	var refs []*consumerRef
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		wait := &sync.WaitGroup{}
		wait.Add(1)
		defer wait.Done()
		refs = append(refs, &consumerRef{jobs: []*ProcessingJob{{ctx: ctx, ctxCancel: cancel}}, wait: wait})
	}
	start := time.Now()
	require.Error(t, stopConsumers(refs, 20*time.Millisecond))
	require.Less(t, time.Since(start), time.Second)
	for _, ref := range refs {
		require.ErrorIs(t, ref.jobs[0].ctx.Err(), context.Canceled)
	}
}

func TestOwnershipErrorsExitCommitRetry(t *testing.T) {
	job := &ProcessingJob{log: zap.NewNop().Sugar()}
	for _, ownershipErr := range []error{kerr.IllegalGeneration, kerr.UnknownMemberID, kerr.RebalanceInProgress, kerr.FencedInstanceID} {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		calls := 0
		err := job.retryUntilResolved(ctx, func() error { calls++; return fmt.Errorf("commit: %w", ownershipErr) })
		cancel()
		require.ErrorIs(t, err, ownershipErr)
		require.Equal(t, 1, calls)
	}
}

func TestReloadReleasesStateLockWhileWaitingForOldWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobCtx, jobCancel := context.WithCancel(ctx)
	oldCfg := reloadTestConfig(t, "old:9092")
	wait := &sync.WaitGroup{}
	wait.Add(1)
	var release sync.Once
	defer release.Do(wait.Done)
	service := &WolveKafkaService{ctx: ctx, ctxCancel: cancel, log: zap.NewNop().Sugar()}
	service.consumers.Store(map[string]*consumerRef{"g": {consumerCfg: oldCfg.Consumers()[0], clusterCfg: oldCfg.Cluster(), jobs: []*ProcessingJob{{ctx: jobCtx, ctxCancel: jobCancel}}, wait: wait}})
	newCfg := reloadTestConfig(t, "new:9092")
	done := make(chan error, 1)
	go func() { done <- service.updateConfig(newCfg) }()
	select {
	case <-jobCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("reload did not cancel old jobs")
	}
	unlocked := make(chan struct{})
	go func() { service.mu.Lock(); service.mu.Unlock(); close(unlocked) }()
	select {
	case <-unlocked:
	case <-time.After(time.Second):
		t.Fatal("reload held service state lock while waiting")
	}
	cancel()
	release.Do(wait.Done)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reload did not stop after cancellation")
	}
}
