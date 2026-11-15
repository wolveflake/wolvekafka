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
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

func TestLambdaTimeoutDefaultsTo30Seconds(t *testing.T) {
	require.Equal(t, 30*time.Second, lambdaTimeout(&configs_pb.Lambda{Url: "http://handler/"}))
	require.Equal(t, 2*time.Minute, lambdaTimeout(&configs_pb.Lambda{Url: "http://handler/", TimeoutMs: 120000}))
}

func TestSlowHandlerTimesOutAtConfiguredTimeoutAndIsRetried(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job := &ProcessingJob{consumerCfg: retryTestPolicy(), lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://handler/", TimeoutMs: 50}})}
	previous := lambdaHTTPClient
	t.Cleanup(func() { lambdaHTTPClient = previous })
	calls := 0
	var firstDeadline time.Duration
	lambdaHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			deadline, ok := req.Context().Deadline()
			require.True(t, ok, "each request must carry the lambda deadline")
			firstDeadline = time.Until(deadline)
			<-req.Context().Done() // a handler that never answers
			return nil, req.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"messages":[{"status":2}]}`))}, nil
	})}

	start := time.Now()
	result, err := defaultSendEventFunc(job, ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}})

	require.NoError(t, err)
	require.Equal(t, 2, calls, "the timed-out attempt is retried")
	require.LessOrEqual(t, firstDeadline, 50*time.Millisecond)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Zero(t, result.Dropped)
	require.Len(t, result.Response.Messages, 1)
}
