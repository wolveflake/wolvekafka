///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2025 [wolveflake LLC], All Rights Reserved.
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

package wks

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

// TestExecuteWithRetrySuccess ensures transient failures are retried until success.
func TestExecuteWithRetrySuccess(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        1,
		log:          zap.NewNop().Sugar(),
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	_, err := job.executeWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error %d", attempts)
		}
		return &model_pb.LambdaResponse{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

// TestExecuteWithRetryExhausted verifies errors bubble out when retries are exhausted.
func TestExecuteWithRetryExhausted(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        2,
		log:          zap.NewNop().Sugar(),
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := job.executeWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})
	require.Error(t, err)
	require.Equal(t, expectedAttempts, attempts)
}

// TestExecuteWithRetryDropAction confirms drop policy swallows final failure.
func TestExecuteWithRetryDropAction(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        3,
		log:          zap.NewNop().Sugar(),
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := job.executeWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})
	require.NoError(t, err)
	require.Equal(t, expectedAttempts, attempts)
}

// TestExecuteWithRetryDLQAction confirms DLQ policy sends messages to DLQ after retries exhausted.
func TestExecuteWithRetryDLQAction(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        4,
		log:          zap.NewNop().Sugar(),
		dlqProducer:  dlqProducer,
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	_, err := job.executeWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		attempts++
		return nil, errors.New("always fail")
	})

	// Should succeed because DLQ succeeded
	require.NoError(t, err)
	require.Equal(t, expectedAttempts, attempts)
	require.Len(t, dlqMessages, 2) // Both messages should be sent to DLQ

	// Verify DLQ messages
	for i, msg := range dlqMessages {
		require.Equal(t, "test-dlq-topic", *msg.TopicPartition.Topic)
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

// TestExecuteWithRetryDLQActionWithRetries confirms DLQ retries work correctly.
func TestExecuteWithRetryDLQActionWithRetries(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        5,
		log:          zap.NewNop().Sugar(),
		dlqProducer:  dlqProducer,
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := job.executeWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		return nil, errors.New("processing failed")
	})

	// Should succeed because DLQ eventually succeeded
	require.NoError(t, err)
	require.Equal(t, int32(3), dlqAttempts.Load())
}

// TestExecuteWithRetryDLQActionFailure confirms failure when DLQ exhausts retries.
func TestExecuteWithRetryDLQActionFailure(t *testing.T) {
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

	job := &ProcessingJob{
		consumerCfg:  consumerCfg,
		consumerName: "test-consumer",
		jobId:        6,
		log:          zap.NewNop().Sugar(),
		dlqProducer:  dlqProducer,
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := job.executeWithRetry(ctx, lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
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

// TestExecuteWithRetryNoPolicy makes sure missing policy returns the original error.
func TestExecuteWithRetryNoPolicy(t *testing.T) {
	job := &ProcessingJob{
		consumerCfg:  &configs_pb.Consumer{},
		consumerName: "test-consumer",
		jobId:        7,
		log:          zap.NewNop().Sugar(),
	}

	lambdaReq := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}

	_, err := job.executeWithRetry(context.Background(), lambdaReq, func(ctx context.Context) (*model_pb.LambdaResponse, error) {
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
