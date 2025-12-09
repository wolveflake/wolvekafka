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
	"fmt"
	"time"

	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

const (
	defaultRetryInitialBackoff = 100 * time.Millisecond
	defaultRetryMaxBackoff     = 5 * time.Second
	defaultRetryMultiplier     = 2.0
	maxRetryAttemptsCap        = 32
)

// RetryPolicy handles retry logic for lambda requests and DLQ operations.
type RetryPolicy struct {
	// Consumer configuration with retry policy settings.
	consumerCfg *configs_pb.Consumer

	// Consumer name for logging.
	consumerName string

	// Job ID for logging.
	jobId int

	// DLQ producer for sending failed messages.
	dlqProducer *kafka.Producer

	// Logger instance.
	log *zap.SugaredLogger

	// Metrics recorder (optional).
	metrics *instrumentation
}

// RetryPolicyParams contains parameters for creating a RetryPolicy.
type RetryPolicyParams struct {
	ConsumerCfg  *configs_pb.Consumer
	ConsumerName string
	JobId        int
	DLQProducer  *kafka.Producer
	Metrics      *instrumentation
}

// NewRetryPolicy creates a new RetryPolicy instance.
func NewRetryPolicy(params RetryPolicyParams) *RetryPolicy {
	return &RetryPolicy{
		consumerCfg:  params.ConsumerCfg,
		consumerName: params.ConsumerName,
		jobId:        params.JobId,
		dlqProducer:  params.DLQProducer,
		metrics:      params.Metrics,
		log:          logger.S(),
	}
}

// ExecuteWithRetry runs the provided function until it succeeds or retry policy rules are exhausted.
func (r *RetryPolicy) ExecuteWithRetry(
	ctx context.Context,
	lambdaReq *model_pb.LambdaRequest,
	fn func(context.Context) (*model_pb.LambdaResponse, error)) (*model_pb.LambdaResponse, error) {

	lambdaResponse, lastErr := fn(ctx)
	if lastErr == nil {
		return lambdaResponse, nil
	}

	retryPolicy := r.consumerCfg.GetRetryPolicy()
	if retryPolicy == nil {
		return lambdaResponse, lastErr
	}

	var backoffDurations []time.Duration
	if exponential := retryPolicy.GetExponentialBackoff(); exponential != nil {
		backoffDurations = buildBackoffSchedule(exponential)
	}

	if len(backoffDurations) == 0 {
		return lambdaResponse, lastErr
	}

	totalAttempts := 1 + len(backoffDurations)
	r.log.Warnf("[%v:%v] send attempt 1/%d failed: %v", r.consumerName, r.jobId, totalAttempts, lastErr)

	for idx, backoff := range backoffDurations {
		if err := sleepWithContext(ctx, backoff); err != nil {
			return lambdaResponse, err
		}

		attempt := idx + 2
		if backoff > 0 {
			r.log.Warnf("[%v:%v] retrying message batch attempt %d/%d after %v", r.consumerName, r.jobId, attempt, totalAttempts, backoff)
		} else {
			r.log.Warnf("[%v:%v] retrying message batch attempt %d/%d", r.consumerName, r.jobId, attempt, totalAttempts)
		}
		if r.metrics != nil {
			r.metrics.recordLambdaRetry(ctx, r.consumerName, r.jobId, attempt, lastErr)
		}

		lambdaResponse, lastErr = fn(ctx)
		if lastErr == nil {
			return lambdaResponse, nil
		}
	}

	// Handle action after retries are exhausted
	return r.handleExhaustedRetries(ctx, lambdaReq, lambdaResponse, lastErr, totalAttempts)
}

// handleExhaustedRetries handles the action to take after retries are exhausted.
func (r *RetryPolicy) handleExhaustedRetries(
	ctx context.Context,
	lambdaReq *model_pb.LambdaRequest,
	lambdaResponse *model_pb.LambdaResponse,
	lastErr error,
	totalAttempts int) (*model_pb.LambdaResponse, error) {

	retryPolicy := r.consumerCfg.GetRetryPolicy()
	if retryPolicy == nil {
		r.log.Errorf("[%v:%v] exhausted %d attempts sending message batch: %v", r.consumerName, r.jobId, totalAttempts, lastErr)
		return lambdaResponse, lastErr
	}

	// Handle drop action
	if retryPolicy.GetDrop() != nil {
		r.log.Warnf("[%v:%v] dropping message batch after %d attempts due to: %v", r.consumerName, r.jobId, totalAttempts, lastErr)
		return nil, nil
	}

	// Handle DLQ action
	if dlqAction := retryPolicy.GetDlq(); dlqAction != nil {
		r.log.Warnf("[%v:%v] sending message batch to DLQ after %d attempts due to: %v", r.consumerName, r.jobId, totalAttempts, lastErr)
		if err := r.sendToDLQ(ctx, lambdaReq, dlqAction, lastErr); err != nil {
			r.log.Errorf("[%v:%v] failed to send to DLQ: %v", r.consumerName, r.jobId, err)
			return lambdaResponse, fmt.Errorf("processing failed and DLQ send failed: %w", err)
		}
		// Message successfully sent to DLQ, return nil error to allow commit
		return nil, nil
	}

	r.log.Errorf("[%v:%v] exhausted %d attempts sending message batch: %v", r.consumerName, r.jobId, totalAttempts, lastErr)
	return lambdaResponse, lastErr
}

// sendToDLQ sends a failed message batch to the DLQ topic with retry logic.
func (r *RetryPolicy) sendToDLQ(ctx context.Context, lambdaReq *model_pb.LambdaRequest, dlqAction *configs_pb.DLQAction, processingErr error) error {
	if r.dlqProducer == nil {
		return fmt.Errorf("DLQ producer not initialized")
	}

	if dlqAction == nil || dlqAction.GetTopic() == "" {
		return fmt.Errorf("DLQ topic not configured")
	}

	dlqTopic := dlqAction.GetTopic()

	// Convert lambda request messages to DLQ messages
	for _, msg := range lambdaReq.GetMessages() {
		dlqMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &dlqTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   msg.GetKey(),
			Value: msg.GetValue(),
		}

		// Copy headers from original message
		for _, header := range msg.GetHeaders() {
			dlqMsg.Headers = append(dlqMsg.Headers, kafka.Header{
				Key:   header.GetKey(),
				Value: header.GetValue(),
			})
		}

		// Add error metadata header
		dlqMsg.Headers = append(dlqMsg.Headers, kafka.Header{
			Key:   "x-processing-error",
			Value: []byte(processingErr.Error()),
		})

		// Send with DLQ retry policy
		sendFunc := func(ctx context.Context) error {
			return r.dlqProducer.Produce(ctx, dlqMsg)
		}

		if err := r.executeWithDLQRetry(ctx, sendFunc, dlqAction); err != nil {
			r.log.Errorf("[%v:%v] failed to send message to DLQ topic %s: %v", r.consumerName, r.jobId, dlqTopic, err)
			return err
		}

		r.log.Infof("[%v:%v] successfully sent message to DLQ topic %s", r.consumerName, r.jobId, dlqTopic)
	}

	return nil
}

// executeWithDLQRetry runs the provided function with DLQ-specific retry logic.
func (r *RetryPolicy) executeWithDLQRetry(ctx context.Context, fn func(context.Context) error, dlqAction *configs_pb.DLQAction) error {
	lastErr := fn(ctx)
	if lastErr == nil {
		return nil
	}

	retryPolicy := dlqAction.GetRetryPolicy()
	if retryPolicy == nil {
		return lastErr
	}

	var backoffDurations []time.Duration
	if exponential := retryPolicy.(*configs_pb.DLQAction_ExponentialBackoff); exponential != nil {
		backoffDurations = buildBackoffSchedule(exponential.ExponentialBackoff)
	}

	if len(backoffDurations) == 0 {
		return lastErr
	}

	totalAttempts := 1 + len(backoffDurations)
	r.log.Warnf("[%v:%v] DLQ send attempt 1/%d failed: %v", r.consumerName, r.jobId, totalAttempts, lastErr)

	for idx, backoff := range backoffDurations {
		if err := sleepWithContext(ctx, backoff); err != nil {
			return err
		}

		attempt := idx + 2
		r.log.Warnf("[%v:%v] retrying DLQ send attempt %d/%d after %v", r.consumerName, r.jobId, attempt, totalAttempts, backoff)

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
	}

	r.log.Errorf("[%v:%v] exhausted %d attempts sending to DLQ: %v", r.consumerName, r.jobId, totalAttempts, lastErr)
	return lastErr
}

// Close cleans up resources held by the retry policy.
func (r *RetryPolicy) Close() error {
	if r.dlqProducer == nil {
		return nil
	}

	err := r.dlqProducer.Close()
	if err != nil {
		r.log.Errorf("[%v:%v] error closing DLQ producer: %v", r.consumerName, r.jobId, err)
		return err
	}
	return nil
}

// buildBackoffSchedule transforms the proto configuration into a bounded sequence of backoff durations.
func buildBackoffSchedule(exp *configs_pb.ExponentialBackoff) []time.Duration {
	if exp == nil {
		return nil
	}

	initial := time.Duration(exp.GetInitialBackoffMs()) * time.Millisecond
	if initial <= 0 {
		initial = defaultRetryInitialBackoff
	}

	maxBackoff := time.Duration(exp.GetMaxBackoffMs()) * time.Millisecond
	if maxBackoff <= 0 {
		maxBackoff = defaultRetryMaxBackoff
	}
	if initial > maxBackoff {
		maxBackoff = initial
	}

	multiplier := exp.GetMultiplier()
	if multiplier <= 1 {
		multiplier = defaultRetryMultiplier
	}

	durations := make([]time.Duration, 0, 8)
	current := initial
	for len(durations) < maxRetryAttemptsCap {
		durations = append(durations, current)
		if current >= maxBackoff {
			break
		}

		next := time.Duration(float64(current) * multiplier)
		if next <= current {
			next = current
		}
		if next > maxBackoff {
			next = maxBackoff
		}
		current = next
	}

	return durations
}

// sleepWithContext waits for the requested duration or returns early if the context is canceled.
func sleepWithContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
