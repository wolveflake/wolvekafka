///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2024 [wolveflake LLC], All Rights Reserved.
///
/// NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical concepts contained
/// herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
/// Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
/// from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees, managers or contractors who have executed
/// Confidentiality and Non-disclosure agreements explicitly covering such access.
///
/// The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
/// information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
/// OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
/// LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
/// TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
///

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	fakeConsumer.enqueue(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		Value:          []byte("test-message"),
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
		PoolDuration:     10 * time.Millisecond,
		BatchMaxMessages: 2,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	// Feed two messages; they should be delivered together because of the count threshold.
	fakeConsumer.enqueue(&kafka.Message{Value: []byte("one")})
	fakeConsumer.enqueue(&kafka.Message{Value: []byte("two")})

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
		PoolDuration:  10 * time.Millisecond,
		BatchMaxBytes: 4,
		BatchMaxWait:  time.Second,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	payload := []byte("four")
	fakeConsumer.enqueue(&kafka.Message{Value: payload})
	fakeConsumer.enqueue(&kafka.Message{Value: []byte("next")})

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
		PoolDuration:     5 * time.Millisecond,
		BatchMaxMessages: 10,
		BatchMaxBytes:    1024,
		BatchMaxWait:     50 * time.Millisecond,
	})
	consumer.kafkaConsumer = fakeConsumer

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	start := time.Now()
	fakeConsumer.enqueue(&kafka.Message{Value: []byte("delayed")})

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

type fakeKafkaConsumer struct {
	events chan kafka.Event
	closed bool
}

func newFakeKafkaConsumer() *fakeKafkaConsumer {
	return &fakeKafkaConsumer{
		events: make(chan kafka.Event, 10),
	}
}

func (f *fakeKafkaConsumer) enqueue(event kafka.Event) {
	f.events <- event
}

func (f *fakeKafkaConsumer) Poll(ms int) kafka.Event {
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

func (f *fakeKafkaConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return nil, nil
}
