// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

// TestRetryPolicyExecuteWithRetrySuccess ensures transient failures are retried until success.
func TestRetryPolicyExecuteWithRetrySuccess(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     4,
					Multiplier:       2,
				},
			},
		},
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        1,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	_, err := retryPolicy.ExecuteWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error %d", attempts)
		}
		return &model_pb.LambdaResponse{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

// TestRetryPolicyExecuteWithRetryExhausted verifies errors bubble out when retries are exhausted.
func TestRetryPolicyExecuteWithRetryExhausted(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     4,
					Multiplier:       2,
				},
			},
		},
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        2,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := retryPolicy.ExecuteWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})
	require.Error(t, err)
	require.Equal(t, expectedAttempts, attempts)
}

// TestRetryPolicyExecuteWithRetryDropAction confirms drop policy swallows final failure.
func TestRetryPolicyExecuteWithRetryDropAction(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     4,
					Multiplier:       2,
				},
			},
			Action: &configs_pb.RetryPolicy_Drop{
				Drop: &configs_pb.DropAction{},
			},
		},
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        3,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := retryPolicy.ExecuteWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})
	require.NoError(t, err)
	require.Equal(t, expectedAttempts, attempts)
}

// TestRetryPolicyExecuteWithRetryDLQAction confirms DLQ policy sends messages to DLQ after retries exhausted.
func TestRetryPolicyExecuteWithRetryDLQAction(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     4,
					Multiplier:       2,
				},
			},
			Action: &configs_pb.RetryPolicy_Dlq{
				Dlq: &configs_pb.DLQAction{
					Topic: "test-dlq-topic",
					RetryPolicy: &configs_pb.DLQAction_ExponentialBackoff{
						ExponentialBackoff: &configs_pb.ExponentialBackoff{
							InitialBackoffMs: 1,
							MaxBackoffMs:     2,
							Multiplier:       2,
						},
					},
				},
			},
		},
	}

	var dlqMessages []*kafka.Message
	mockProducer := func(msg *kafka.Message) error {
		dlqMessages = append(dlqMessages, msg)
		return nil
	}

	ctx := context.Background()
	dlqProducer := &kafka.Producer{
		ProducerFunc: mockProducer,
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        4,
		DLQProducer:  dlqProducer,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := retryPolicy.ExecuteWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})

	// Should succeed because DLQ succeeded
	require.NoError(t, err)
	require.Equal(t, expectedAttempts, attempts)
	require.Len(t, dlqMessages, 2) // Both messages should be sent to DLQ

	// Verify DLQ messages
	for i, msg := range dlqMessages {
		require.Equal(t, "test-dlq-topic", msg.Topic)
		require.Equal(t, lambdaReq.Messages[i].Key, msg.Key)
		require.Equal(t, lambdaReq.Messages[i].Value, msg.Value)

		// Check for error header
		foundErrorHeader := false
		for _, header := range msg.Headers {
			if header.Key == "x-processing-error" {
				foundErrorHeader = true
				require.Contains(t, string(header.Value), "always fail")
				break
			}
		}
		require.True(t, foundErrorHeader, "DLQ message should have error header")
	}
}

// TestRetryPolicyExecuteWithRetryDLQActionWithRetries confirms DLQ retries work correctly.
func TestRetryPolicyExecuteWithRetryDLQActionWithRetries(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     2,
					Multiplier:       2,
				},
			},
			Action: &configs_pb.RetryPolicy_Dlq{
				Dlq: &configs_pb.DLQAction{
					Topic: "test-dlq-topic",
					RetryPolicy: &configs_pb.DLQAction_ExponentialBackoff{
						ExponentialBackoff: &configs_pb.ExponentialBackoff{
							InitialBackoffMs: 1,
							MaxBackoffMs:     4,
							Multiplier:       2,
						},
					},
				},
			},
		},
	}

	var dlqAttempts atomic.Int32
	mockProducer := func(msg *kafka.Message) error {
		attempt := dlqAttempts.Add(1)
		if attempt < 3 {
			return fmt.Errorf("DLQ send failed attempt %d", attempt)
		}
		return nil
	}

	ctx := context.Background()
	dlqProducer := &kafka.Producer{
		ProducerFunc: mockProducer,
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        5,
		DLQProducer:  dlqProducer,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := retryPolicy.ExecuteWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		return nil, errors.New("processing failed")
	})

	// Should succeed because DLQ eventually succeeded
	require.NoError(t, err)
	require.Equal(t, int32(3), dlqAttempts.Load())
}

// TestRetryPolicyExecuteWithRetryDLQActionFailure confirms failure when DLQ exhausts retries.
func TestRetryPolicyExecuteWithRetryDLQActionFailure(t *testing.T) {
	consumerCfg := &configs_pb.Consumer{
		RetryPolicy: &configs_pb.RetryPolicy{
			RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{
				ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 1,
					MaxBackoffMs:     2,
					Multiplier:       2,
				},
			},
			Action: &configs_pb.RetryPolicy_Dlq{
				Dlq: &configs_pb.DLQAction{
					Topic: "test-dlq-topic",
					RetryPolicy: &configs_pb.DLQAction_ExponentialBackoff{
						ExponentialBackoff: &configs_pb.ExponentialBackoff{
							InitialBackoffMs: 1,
							MaxBackoffMs:     2,
							Multiplier:       2,
						},
					},
				},
			},
		},
	}

	var dlqAttempts atomic.Int32
	mockProducer := func(msg *kafka.Message) error {
		dlqAttempts.Add(1)
		return errors.New("DLQ send always fails")
	}

	ctx := context.Background()
	dlqProducer := &kafka.Producer{
		ProducerFunc: mockProducer,
	}

	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  consumerCfg,
		ConsumerName: "test-consumer",
		JobId:        6,
		DLQProducer:  dlqProducer,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := retryPolicy.ExecuteWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		return nil, errors.New("processing failed")
	})

	// Should fail because both processing and DLQ failed
	require.Error(t, err)
	require.Contains(t, err.Error(), "processing failed and DLQ send failed")

	// DLQ should have attempted retries
	expectedDLQAttempts := 1 + len(buildBackoffSchedule(&configs_pb.ExponentialBackoff{
		InitialBackoffMs: 1,
		MaxBackoffMs:     2,
		Multiplier:       2,
	}))
	require.Equal(t, int32(expectedDLQAttempts), dlqAttempts.Load())
}

// TestRetryPolicyExecuteWithRetryNoPolicy makes sure missing policy returns the original error.
func TestRetryPolicyExecuteWithRetryNoPolicy(t *testing.T) {
	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  &configs_pb.Consumer{},
		ConsumerName: "test-consumer",
		JobId:        7,
	})

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := retryPolicy.ExecuteWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		return nil, errors.New("fail once")
	})
	require.EqualError(t, err, "fail once")
}

// TestBuildBackoffScheduleDefaults validates defaults when proto leaves fields unset.
func TestBuildBackoffScheduleDefaults(t *testing.T) {
	schedule := buildBackoffSchedule(&configs_pb.ExponentialBackoff{})
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		5 * time.Second,
	}, schedule)
}

// TestBuildBackoffScheduleMultiplierFloor ensures multiplier < 1 is clamped.
func TestBuildBackoffScheduleMultiplierFloor(t *testing.T) {
	schedule := buildBackoffSchedule(&configs_pb.ExponentialBackoff{
		InitialBackoffMs: 10,
		MaxBackoffMs:     15,
		Multiplier:       1,
	})
	require.Equal(t, []time.Duration{
		10 * time.Millisecond,
		15 * time.Millisecond,
	}, schedule)
}

// TestSleepWithContextCancelled checks context cancellation is honoured.
func TestSleepWithContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sleepWithContext(ctx, time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

// TestRetryPolicyClose validates cleanup of DLQ producer.
func TestRetryPolicyClose(t *testing.T) {
	// Close with nil producer should not error
	retryPolicy := NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  &configs_pb.Consumer{},
		ConsumerName: "test-consumer",
		JobId:        8,
	})
	err := retryPolicy.Close()
	require.NoError(t, err)
}
