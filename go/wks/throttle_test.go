// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		value string
		want  time.Duration
		ok    bool
	}{
		{"", 0, false},
		{"soon", 0, false},
		{"-1", 0, false},
		{"0", 0, true},
		{" 2 ", 2 * time.Second, true},
		{"999999999", maxRetryAfter, true},
		{now.Add(10 * time.Second).Format(http.TimeFormat), 10 * time.Second, true},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
		{now.Add(24 * time.Hour).Format(http.TimeFormat), maxRetryAfter, true},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.value, now)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestThrottleFromResponse(t *testing.T) {
	now := time.Now()
	base := errors.New("handler busy")
	header := func(v string) http.Header {
		h := http.Header{}
		if v != "" {
			h.Set("Retry-After", v)
		}
		return h
	}

	got := throttleFromResponse(http.StatusTooManyRequests, header(""), now, base)
	require.NotNil(t, got, "429 is throttling even without Retry-After")
	require.Zero(t, got.retryAfter)
	require.ErrorIs(t, got, base)

	got = throttleFromResponse(http.StatusTooManyRequests, header("3"), now, base)
	require.Equal(t, 3*time.Second, got.retryAfter)

	got = throttleFromResponse(http.StatusServiceUnavailable, header("1"), now, base)
	require.NotNil(t, got)
	require.Equal(t, time.Second, got.retryAfter)

	require.Nil(t, throttleFromResponse(http.StatusServiceUnavailable, header(""), now, base),
		"503 without Retry-After stays an ordinary failure")
	require.Nil(t, throttleFromResponse(http.StatusInternalServerError, header("5"), now, base))
}

func TestThrottlingDoesNotExhaustRetryBudget(t *testing.T) {
	// One-step schedule with a drop action: an ordinary failure would drop the
	// record after the first retry.
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()})
	req := &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Offset: 1}}}
	calls := 0
	result, err := r.ExecuteBatch(context.Background(), req, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		if calls <= 5 {
			return nil, &throttledError{status: http.StatusTooManyRequests, err: fmt.Errorf("busy")}
		}
		return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_SUCCESS}}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 6, calls)
	require.Zero(t, result.Dropped)
	require.Len(t, result.Response.Messages, 1)
}

func TestOrdinaryFailuresStillExhaustRetryBudget(t *testing.T) {
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()})
	req := &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Offset: 1}}}
	calls := 0
	result, err := r.ExecuteBatch(context.Background(), req, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		return nil, fmt.Errorf("lambda request failed with status 500")
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, result.Dropped)
}

func TestThrottlingHonorsRetryAfter(t *testing.T) {
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()}) // 1ms backoff
	req := &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Offset: 1}}}
	calls := 0
	start := time.Now()
	_, err := r.ExecuteBatch(context.Background(), req, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		if calls == 1 {
			return nil, &throttledError{status: http.StatusTooManyRequests, retryAfter: 80 * time.Millisecond, err: fmt.Errorf("busy")}
		}
		return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_SUCCESS}}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
}

func TestThrottledWaitIsCancellable(t *testing.T) {
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.ExecuteBatch(ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}},
		func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
			return nil, &throttledError{status: http.StatusTooManyRequests, retryAfter: time.Minute, err: fmt.Errorf("busy")}
		})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second, "a rebalance or shutdown must interrupt Retry-After waits")
}

func TestHandler429IsRetriedWithoutDropping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job := &ProcessingJob{consumerCfg: retryTestPolicy(), lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://handler/"}})}
	previous := lambdaHTTPClient
	t.Cleanup(func() { lambdaHTTPClient = previous })
	calls := 0
	lambdaHTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls <= 3 {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"0"}},
				Body: io.NopCloser(strings.NewReader("slow down"))}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"messages":[{"status":2}]}`))}, nil
	})}

	result, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.Equal(t, 4, calls)
	require.Zero(t, result.Dropped)
	require.Len(t, result.Response.Messages, 1)
}
