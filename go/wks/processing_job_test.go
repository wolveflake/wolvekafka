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

/*
import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	configs_pb "wolvekafka/proto/configs"
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

	attempts := 0
	err := job.executeWithRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("transient error %d", attempts)
		}
		return nil
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

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	err := job.executeWithRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		return errors.New("always fail")
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

	attempts := 0
	expectedAttempts := 1 + len(buildBackoffSchedule(consumerCfg.GetRetryPolicy().GetExponentialBackoff()))
	err := job.executeWithRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		return errors.New("always fail")
	})
	require.NoError(t, err)
	require.Equal(t, expectedAttempts, attempts)
}

// TestDefaultSendEventFuncRetries exercises HTTP retry behaviour end-to-end.
func TestDefaultSendEventFuncRetries(t *testing.T) {
	var callCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.ReadAll(r.Body)
		attempt := callCount.Add(1)
		if attempt < 3 {
			http.Error(w, "fail", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

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
		Lambdas: []*configs_pb.Lambda{
			{Url: ts.URL},
		},
	}

	job := &ProcessingJob{
		consumerCfg:    consumerCfg,
		consumerName:   "test-consumer",
		jobId:          4,
		log:            zap.NewNop().Sugar(),
		lambdaSelector: NewLambdaSelector(consumerCfg.GetLambdas()),
	}

	topic := "topic"
	msgs := []*kafka.Message{
		{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 0,
				Offset:    0,
			},
			Value: []byte("payload"),
		},
	}

	err := defaultSendEventFunc(job, context.Background(), msgs)
	require.NoError(t, err)
	require.Equal(t, int32(3), callCount.Load())
}

// TestExecuteWithRetryNoPolicy makes sure missing policy returns the original error.
func TestExecuteWithRetryNoPolicy(t *testing.T) {
	job := &ProcessingJob{
		consumerCfg:  &configs_pb.Consumer{},
		consumerName: "test-consumer",
		jobId:        5,
		log:          zap.NewNop().Sugar(),
	}

	err := job.executeWithRetry(context.Background(), func(ctx context.Context) error {
		return errors.New("fail once")
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
*/
