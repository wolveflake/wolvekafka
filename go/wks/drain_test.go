// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"wolvekafka/go/configs"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/handler/v1"
)

type drainStub struct {
	events  chan kafka.Event
	commits chan int64
}

func (s *drainStub) Poll(ms int) kafka.Event {
	select {
	case msg := <-s.events:
		return msg
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}
func (s *drainStub) Close() error { return nil }
func (s *drainStub) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	s.commits <- msg.Offset
	return nil, nil
}

// drainService runs one worker whose handler blocks until release is closed
// (or its batch context is cancelled) and reports each batch's first offset.
func drainService(t *testing.T, stub *drainStub, release <-chan struct{}) (*WolveKafkaService, chan int64) {
	t.Helper()
	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster: &configs_pb.Cluster{BootstrapServers: []string{"localhost:9092"}},
		Consumers: []*configs_pb.Consumer{{
			GroupId: "g", Topics: []string{"in"}, Lambdas: []*configs_pb.Lambda{{Url: "http://handler/"}},
			ConsumerType: &configs_pb.Consumer_Parallel{Parallel: &configs_pb.ParallelConsumer{Parallelism: 1}},
		}},
	})
	require.NoError(t, err)
	started := make(chan int64, 4)
	svc, err := NewWolveKafkaService(context.Background(), WolveKafkaParams{
		Config: cfg,
		newConsumerFunc: func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error) {
			params.Client = stub
			return kafka.NewConsumer(ctx, params)
		},
		sendEventFunc: func(_ *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*BatchResult, error) {
			started <- req.Messages[0].Offset
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			resp := &model_pb.LambdaResponse{}
			for range req.Messages {
				resp.Messages = append(resp.Messages, &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_SUCCESS})
			}
			return &BatchResult{Request: req, Response: resp}, nil
		},
	})
	require.NoError(t, err)
	return svc, started
}

func TestShutdownLetsInFlightBatchFinishAndCommit(t *testing.T) {
	stub := &drainStub{events: make(chan kafka.Event, 4), commits: make(chan int64, 4)}
	release := make(chan struct{})
	svc, started := drainService(t, stub, release)
	stub.events <- &kafka.Message{Topic: "in", Offset: 7}
	select {
	case offset := <-started:
		require.EqualValues(t, 7, offset)
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not reach the handler")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- svc.Shutdown(10 * time.Second) }()
	require.Eventually(t, func() bool {
		ready, consumers := svc.Readiness()
		return !ready && consumers["g"].Reason == "draining for shutdown"
	}, 2*time.Second, 10*time.Millisecond, "draining workers report not ready")
	// Records that arrive during the drain are not started.
	stub.events <- &kafka.Message{Topic: "in", Offset: 8}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown finished before the in-flight batch")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case offset := <-stub.commits:
		require.EqualValues(t, 7, offset, "the in-flight batch commits")
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight batch was not committed")
	}
	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after the batch")
	}
	select {
	case offset := <-started:
		t.Fatalf("started offset %d during drain", offset)
	default:
	}
}

func TestShutdownAbandonsBatchAfterDrainTimeout(t *testing.T) {
	stub := &drainStub{events: make(chan kafka.Event, 4), commits: make(chan int64, 4)}
	svc, started := drainService(t, stub, make(chan struct{})) // never released
	stub.events <- &kafka.Message{Topic: "in", Offset: 7}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not reach the handler")
	}

	start := time.Now()
	require.NoError(t, svc.Shutdown(200*time.Millisecond))
	require.Less(t, time.Since(start), 5*time.Second, "the drain timeout bounds shutdown")
	select {
	case offset := <-stub.commits:
		t.Fatalf("committed offset %d of an abandoned batch", offset)
	default:
	}
}

func TestShutdownOfIdleWorkersIsImmediate(t *testing.T) {
	stub := &drainStub{events: make(chan kafka.Event, 1), commits: make(chan int64, 1)}
	svc, _ := drainService(t, stub, make(chan struct{}))
	time.Sleep(100 * time.Millisecond) // let the worker start polling
	start := time.Now()
	require.NoError(t, svc.Shutdown(10*time.Second))
	require.Less(t, time.Since(start), 2*time.Second)
}
