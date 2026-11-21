// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"wolvekafka/go/kafka"
)

func TestJobReadinessStates(t *testing.T) {
	job := &ProcessingJob{}
	ok, reason := job.readiness()
	require.False(t, ok)
	require.Equal(t, "connecting to Kafka", reason)

	job.connectErr.Store("dial tcp: connection refused")
	_, reason = job.readiness()
	require.Equal(t, "connecting to Kafka: dial tcp: connection refused", reason)

	job.liveConsumer.Store(&kafka.Consumer{}) // a stub without a group protocol counts as joined
	ok, _ = job.readiness()
	require.True(t, ok)

	job.stopped.Store(true)
	ok, reason = job.readiness()
	require.False(t, ok)
	require.Equal(t, "worker stopped", reason)
}

func TestReadyHandlerReportsEachConsumer(t *testing.T) {
	joined := &ProcessingJob{}
	joined.liveConsumer.Store(&kafka.Consumer{})
	connecting := &ProcessingJob{}
	connecting.connectErr.Store("no brokers")

	svc := &WolveKafkaService{}
	svc.consumers.Store(map[string]*consumerRef{
		"orders":   {jobs: []*ProcessingJob{joined, joined}},
		"payments": {jobs: []*ProcessingJob{joined, connecting}},
	})

	rec := httptest.NewRecorder()
	svc.ReadyHandler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	var body struct {
		Ready     bool                         `json:"ready"`
		Consumers map[string]ConsumerReadiness `json:"consumers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.False(t, body.Ready)
	require.Equal(t, ConsumerReadiness{Ready: true, Workers: 2, ReadyWorkers: 2}, body.Consumers["orders"])
	require.Equal(t, ConsumerReadiness{Ready: false, Workers: 2, ReadyWorkers: 1, Reason: "connecting to Kafka: no brokers"}, body.Consumers["payments"])

	svc.consumers.Store(map[string]*consumerRef{"orders": {jobs: []*ProcessingJob{joined}}})
	rec = httptest.NewRecorder()
	svc.ReadyHandler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// No consumers configured: nothing to wait for.
	svc.consumers.Store(map[string]*consumerRef{})
	ready, _ := svc.Readiness()
	require.True(t, ready)
}
