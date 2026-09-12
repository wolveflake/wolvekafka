// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerProduceUsesProducerFunc(t *testing.T) {
	ctx := context.Background()
	msg := &kgo.Record{
		Value: []byte("payload"),
	}

	var captured *Message
	producer := newProducer(ctx, ProducerParams{
		Name: "test-producer",
		ProducerFunc: func(m *Message) error {
			captured = m
			return nil
		},
	})

	err := producer.Produce(ctx, msg)

	require.NoError(t, err)
	assert.Equal(t, msg, captured)
}

func TestProducerProducePropagatesProducerFuncError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("produce failure")
	producer := newProducer(ctx, ProducerParams{
		Name: "test-producer",
		ProducerFunc: func(m *Message) error {
			return expectedErr
		},
	})

	err := producer.Produce(ctx, &kgo.Record{})

	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

func TestProducerCloseCancelsContextWithoutKafkaProducer(t *testing.T) {
	ctx := context.Background()
	producer := newProducer(ctx, ProducerParams{Name: "test-producer"})

	err := producer.Close()
	require.NoError(t, err)

	select {
	case <-producer.ctx.Done():
	default:
		t.Fatalf("producer context should be canceled after Close")
	}
}
