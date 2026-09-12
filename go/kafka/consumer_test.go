// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCustomPoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name: "test-consumer",
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	// Simulate a single message flowing through the stub client.
	topic := "test-topic"
	fakeConsumer.enqueue(&kgo.Record{
		Topic:     topic,
		Partition: 0,
		Offset:    1,
		Value:     []byte("test-message"),
	})

	select {
	case msgs := <-msgChan:
		assert.Equal(t, 1, len(msgs))
	case <-ctx.Done():
		t.Fatal("timeout sending message to msgChan")
	}
}

func TestPollBatchesByMessageCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name:             "batch-by-count",
		PollDuration:     10 * time.Millisecond,
		BatchMaxMessages: 2,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	// Feed two messages; they should be delivered together because of the count threshold.
	fakeConsumer.enqueue(&kgo.Record{Value: []byte("one")})
	fakeConsumer.enqueue(&kgo.Record{Value: []byte("two")})

	select {
	case batch := <-msgChan:
		require.Len(t, batch, 2, "expected both messages in the same batch")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for batched messages")
	}

	require.NoError(t, consumer.Close())
}

func TestPollBatchesBySize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name:          "batch-by-size",
		PollDuration:  10 * time.Millisecond,
		BatchMaxBytes: 4,
		BatchMaxWait:  time.Second,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	payload := []byte("four")
	fakeConsumer.enqueue(&kgo.Record{Value: payload})
	fakeConsumer.enqueue(&kgo.Record{Value: []byte("next")})

	select {
	case batch := <-msgChan:
		require.Len(t, batch, 1, "size threshold should flush single message")
		require.Equal(t, payload, batch[0].Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for size-based flush")
	}

	require.NoError(t, consumer.Close())
}

func TestPollBatchesByWaitDuration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name:             "batch-by-wait",
		PollDuration:     5 * time.Millisecond,
		BatchMaxMessages: 10,
		BatchMaxBytes:    1024,
		BatchMaxWait:     50 * time.Millisecond,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	start := time.Now()
	fakeConsumer.enqueue(&kgo.Record{Value: []byte("delayed")})

	select {
	case batch := <-msgChan:
		require.Len(t, batch, 1, "timeout should flush partial batch")
		if elapsed := time.Since(start); elapsed+5*time.Millisecond < consumer.batchMaxWait {
			t.Fatalf("batch flushed too early: %v < %v", elapsed, consumer.batchMaxWait)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for wait-duration flush")
	}

	require.NoError(t, consumer.Close())
}

func TestPollContinuesAfterTransientError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name:         "transient-error",
		PollDuration: 5 * time.Millisecond,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	fakeConsumer.enqueue(errors.New("temporary broker outage"))
	fakeConsumer.enqueue(&kgo.Record{Value: []byte("after-error")})

	select {
	case batch, ok := <-msgChan:
		require.True(t, ok, "poll channel should remain open after transient errors")
		require.Len(t, batch, 1)
		require.Equal(t, []byte("after-error"), batch[0].Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message after transient error")
	}

	require.NoError(t, consumer.Close())
}

func TestPollStopsWhenKafkaClientClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeConsumer := newFakeKafkaConsumer()
	defer fakeConsumer.Close()

	consumer := newConsumer(ctx, ConsumerParams{
		Name:         "client-closed",
		PollDuration: 5 * time.Millisecond,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	fakeConsumer.enqueue(errKafkaClientClosed)

	select {
	case _, ok := <-msgChan:
		require.False(t, ok, "poll channel should close after the Kafka client closes")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for poll channel to close")
	}
}

type fakeKafkaConsumer struct {
	events chan Event
	closed bool
}

func newFakeKafkaConsumer() *fakeKafkaConsumer {
	return &fakeKafkaConsumer{
		events: make(chan Event, 10),
	}
}

func (f *fakeKafkaConsumer) enqueue(event Event) {
	f.events <- event
}

func (f *fakeKafkaConsumer) Poll(ms int) Event {
	select {
	case ev := <-f.events:
		return ev
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}

func (f *fakeKafkaConsumer) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.events)
	return nil
}

func (f *fakeKafkaConsumer) CommitMessage(msg *Message) ([]TopicPartition, error) {
	return nil, nil
}
