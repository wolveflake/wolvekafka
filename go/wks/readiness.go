// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"encoding/json"
	"net/http"
	"sort"
)

// ConsumerReadiness is one consumer's entry in the /ready response.
type ConsumerReadiness struct {
	Ready        bool   `json:"ready"`
	Workers      int    `json:"workers"`
	ReadyWorkers int    `json:"ready_workers"`
	Reason       string `json:"reason,omitempty"`
}

// Readiness reports whether every configured consumer has connected to Kafka
// and every one of its workers has joined the consumer group. Workers without
// assigned partitions still count once they are group members.
func (w *WolveKafkaService) Readiness() (bool, map[string]ConsumerReadiness) {
	consumers, _ := w.consumers.Load().(map[string]*consumerRef)
	names := make([]string, 0, len(consumers))
	for name := range consumers {
		names = append(names, name)
	}
	sort.Strings(names)

	ready := true
	out := make(map[string]ConsumerReadiness, len(consumers))
	for _, name := range names {
		ref := consumers[name]
		status := ConsumerReadiness{Workers: len(ref.jobs)}
		for _, job := range ref.jobs {
			ok, reason := job.readiness()
			if ok {
				status.ReadyWorkers++
			} else if status.Reason == "" {
				status.Reason = reason
			}
		}
		status.Ready = status.ReadyWorkers == status.Workers
		ready = ready && status.Ready
		out[name] = status
	}
	return ready, out
}

// ReadyHandler serves /ready: 200 when Readiness is true, otherwise 503, with
// a JSON body listing each consumer and the first reason it is not ready.
func (w *WolveKafkaService) ReadyHandler() http.HandlerFunc {
	return func(rw http.ResponseWriter, _ *http.Request) {
		ready, consumers := w.Readiness()
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Cache-Control", "no-store")
		if !ready {
			rw.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(rw).Encode(struct {
			Ready     bool                         `json:"ready"`
			Consumers map[string]ConsumerReadiness `json:"consumers"`
		}{ready, consumers})
	}
}
