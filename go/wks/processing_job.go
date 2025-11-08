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

package wks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

var (
	// interval to recreate consumer in case of failure.
	consumerRecreationInterval = 10 * time.Second
)

const (
	defaultRetryInitialBackoff = 100 * time.Millisecond
	defaultRetryMaxBackoff     = 5 * time.Second
	defaultRetryMultiplier     = 2.0
	maxRetryAttemptsCap        = 32
)

type sendEventFunc func(c *ProcessingJob, ctx context.Context, msgs []*kafka.Message) error

type consumerConfigCtxKey struct{}

type ProcessingJob struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// consumer name.
	consumerName string
	jobId        int

	// consumers map for dynamic reconfiguration.
	consumers atomic.Value // map[string]*consumerRef

	// callback function to send events.
	sendEventFunc sendEventFunc

	// logger instance.
	log *zap.SugaredLogger

	// consumer instance.
	kafkaConsumer   *kafka.Consumer
	newConsumerFunc func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error)

	// configuration part.
	consumerCfg *configs_pb.Consumer
	clusterCfg  *configs_pb.Cluster

	lambdaSelector *LambdaSelector
}

func defaultSendEventFunc(c *ProcessingJob, ctx context.Context, msgs []*kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	lambda := c.lambdaSelector.Select()
	lambdaReq := kafkaMsgsToLambdaRequest(msgs)

	// convert lambda request to JSON payload.
	payload, err := ConvertLambdaRequestToJson(lambdaReq)
	if err != nil {
		return fmt.Errorf("failed to marshal lambda request: %w", err)
	}

	sendOnce := func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, lambda.GetUrl(), bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("failed to create http request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to send lambda request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if readErr != nil {
				return fmt.Errorf("lambda request failed with status %d and read error: %v", resp.StatusCode, readErr)
			}
			return fmt.Errorf("lambda request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	return c.executeWithRetry(ctx, sendOnce)
}

// executeWithRetry runs the provided function until it succeeds or retry policy rules are exhausted.
func (c *ProcessingJob) executeWithRetry(ctx context.Context, fn func(context.Context) error) error {
	lastErr := fn(ctx)
	if lastErr == nil {
		return nil
	}

	retryPolicy := c.consumerCfg.GetRetryPolicy()
	if retryPolicy == nil {
		return lastErr
	}

	var backoffDurations []time.Duration
	if exponential := retryPolicy.GetExponentialBackoff(); exponential != nil {
		backoffDurations = buildBackoffSchedule(exponential)
	}

	if len(backoffDurations) == 0 {
		return lastErr
	}

	totalAttempts := 1 + len(backoffDurations)
	c.log.Warnf("[%v:%v] send attempt 1/%d failed: %v", c.consumerName, c.jobId, totalAttempts, lastErr)

	for idx, backoff := range backoffDurations {
		if err := sleepWithContext(ctx, backoff); err != nil {
			return err
		}

		attempt := idx + 2
		if backoff > 0 {
			c.log.Warnf("[%v:%v] retrying message batch attempt %d/%d after %v", c.consumerName, c.jobId, attempt, totalAttempts, backoff)
		} else {
			c.log.Warnf("[%v:%v] retrying message batch attempt %d/%d", c.consumerName, c.jobId, attempt, totalAttempts)
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
	}

	if retryPolicy.GetDrop() != nil {
		c.log.Warnf("[%v:%v] dropping message batch after %d attempts due to: %v", c.consumerName, c.jobId, totalAttempts, lastErr)
		return nil
	}

	c.log.Errorf("[%v:%v] exhausted %d attempts sending message batch: %v", c.consumerName, c.jobId, totalAttempts, lastErr)
	return lastErr
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

// createKafkaConsumer creates a new Kafka consumer with retry logic.
func (c *ProcessingJob) createKafkaConsumer(consumerParams kafka.ConsumerParams) (*kafka.Consumer, error) {
	for {
		kafkaConsumer, err := c.newConsumerFunc(c.ctx, consumerParams)
		if err != nil {
			c.log.Errorf("failed to create kafka consumer for %s:%v, reason: %v", c.consumerName, c.jobId, err)

			select {
			case <-c.ctx.Done():
				return nil, fmt.Errorf("stopping consumer job for %s:%v", c.consumerName, c.jobId)
			case <-time.After(consumerRecreationInterval):
			}
			continue
		}

		return kafkaConsumer, nil
	}
}

func (c *ProcessingJob) Execute() {
	c.log.Infof("[%v:%v] starting consumer job...", c.consumerName, c.jobId)

	defer c.done()

	c.lambdaSelector = NewLambdaSelector(c.consumerCfg.GetLambdas())
	c.log.Infof("[%v:%v] Total lambda weigts: %v, lambdas: %v", c.consumerName, c.jobId, c.lambdaSelector.totalWeight, c.lambdaSelector.lambdas)

	// use the default send event function if not provided.
	if c.sendEventFunc == nil {
		c.sendEventFunc = defaultSendEventFunc
	}

	// start polling kafka messages.
	consumerParams := kafka.ConsumerParams{
		Name:         c.consumerName,
		Cluster:      c.clusterCfg,
		PoolDuration: time.Millisecond * 500,
		Topics:       c.consumerCfg.GetTopics(),
	}
	kafkaConsumer, err := c.createKafkaConsumer(consumerParams)
	if err != nil {
		c.log.Errorf("[%v:%v] failed to create kafka consumer: %v", c.consumerName, c.jobId, err)
		return
	}
	c.kafkaConsumer = kafkaConsumer

	msgChan, err := c.kafkaConsumer.Poll()
	if err != nil {
		c.log.Errorf("[%v:%v] error polling messages: %v", c.consumerName, c.jobId, err)
	}

	sendCtx := context.WithValue(c.ctx, consumerConfigCtxKey{}, c.consumerCfg)

	for {
		select {
		case <-c.ctx.Done():
			c.log.Infof("[%v:%v] stopping consumer job", c.consumerName, c.jobId)
			return
		case msgs, ok := <-msgChan:
			if !ok {
				c.log.Infof("[%v:%v] message channel closed", c.consumerName, c.jobId)
				c.ctxCancel()
				return
			}

			for _, msg := range msgs {
				c.log.Infof(
					"[%v:%v] Consumed message from topic %s [%d] at offset %v",
					c.consumerName,
					c.jobId,
					*msg.TopicPartition.Topic,
					msg.TopicPartition.Partition,
					msg.TopicPartition.Offset)
			}

			err = c.sendEventFunc(c, sendCtx, msgs)
			c.log.Infof("[%v:%v] Processed with status: %v", c.consumerName, c.jobId, err)
			if err != nil {
				continue
			}

			// You can process the message here
			// For automatic offset committing, no explicit commit is needed here with "enable.auto.commit": "true"
			// For manual offset committing, you would use consumer.CommitMessage(e)
			for _, msg := range msgs {
				_, err := c.kafkaConsumer.CommitMessage(msg)
				if err != nil {
					c.log.Errorf("[%v:%v] error committing message offset: %v", c.consumerName, c.jobId, err)
				} else {
					c.log.Infof("[%v:%v] committed message offset: %v", c.consumerName, c.jobId, msg.TopicPartition.Offset)
				}
			}
		}
	}
}

func (c *ProcessingJob) Cancel() {
	defer c.done()
}

func (c *ProcessingJob) done() {
	c.log.Infof("[%v:%v] consumer job has been stopped", c.consumerName, c.jobId)

	// cancel job context.
	c.ctxCancel()

	err := c.kafkaConsumer.Close()
	if err != nil {
		c.log.Errorf("[%v:%v] error closing consumer: %v", c.consumerName, c.jobId, err)
	}
}
