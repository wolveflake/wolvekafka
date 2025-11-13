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
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
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

type sendEventFunc func(c *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error)

type consumerConfigCtxKey struct{}

type ProcessingJob struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// consumer name.
	consumerName string
	jobId        int

	metrics *instrumentation

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

	// lambda response msg handler.
	outputPolicy *OutputPolicy

	shutdownOnce sync.Once
}

func defaultSendEventFunc(c *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
	if len(req.GetMessages()) == 0 {
		return nil, nil
	}

	lambda := c.lambdaSelector.Select()

	// convert lambda request to JSON payload.
	payload, err := ConvertLambdaRequestToJson(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal lambda request: %w", err)
	}

	sendOnce := func(ctx context.Context) (*model_pb.LambdaResponse, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, lambda.GetUrl(), bytes.NewReader(payload))
		if err != nil {

			return nil, fmt.Errorf("failed to create http request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send lambda request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("lambda request failed with status %d and read error: %v", resp.StatusCode)
		}

		// read & parse response.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("lambda request failed with status %d and read error: %v", resp.StatusCode, err)
		}
		respMsgs, err := ParseLambdaResponse(body)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal lambda response: %w", err)
		}

		return respMsgs, nil
	}

	return c.executeWithRetry(ctx, sendOnce)
}

// executeWithRetry runs the provided function until it succeeds or retry policy rules are exhausted.
func (c *ProcessingJob) executeWithRetry(
	ctx context.Context,
	fn func(context.Context) (*model_pb.LambdaResponse, error)) (*model_pb.LambdaResponse, error) {

	lambdaResponse, lastErr := fn(ctx)
	if lastErr == nil {
		return lambdaResponse, nil
	}

	retryPolicy := c.consumerCfg.GetRetryPolicy()
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
	c.log.Warnf("[%v:%v] send attempt 1/%d failed: %v", c.consumerName, c.jobId, totalAttempts, lastErr)

	for idx, backoff := range backoffDurations {
		if err := sleepWithContext(ctx, backoff); err != nil {
			return lambdaResponse, err
		}

		attempt := idx + 2
		if backoff > 0 {
			c.log.Warnf("[%v:%v] retrying message batch attempt %d/%d after %v", c.consumerName, c.jobId, attempt, totalAttempts, backoff)
		} else {
			c.log.Warnf("[%v:%v] retrying message batch attempt %d/%d", c.consumerName, c.jobId, attempt, totalAttempts)
		}
		if c.metrics != nil {
			c.metrics.recordLambdaRetry(ctx, c.consumerName, c.jobId, attempt, lastErr)
		}

		lambdaResponse, lastErr = fn(ctx)
		if lastErr == nil {
			return lambdaResponse, nil
		}
	}

	if retryPolicy.GetDrop() != nil {
		c.log.Warnf("[%v:%v] dropping message batch after %d attempts due to: %v", c.consumerName, c.jobId, totalAttempts, lastErr)
		return nil, nil
	}

	c.log.Errorf("[%v:%v] exhausted %d attempts sending message batch: %v", c.consumerName, c.jobId, totalAttempts, lastErr)
	return lambdaResponse, lastErr
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

	if c.metrics != nil {
		c.metrics.jobStarted(c.ctx, c.consumerName, c.jobId)
	}

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

	if err := c.ensureOutputProducer(); err != nil {
		c.log.Errorf("[%v:%v] failed to create output kafka producer: %v", c.consumerName, c.jobId, err)
		return
	}

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

			// convert kafka messages to lambda request.
			lambdaReq := kafkaMsgsToLambdaRequest(msgs)
			batchBytes := approximateBatchBytes(msgs)
			processCtx := sendCtx
			span := trace.SpanFromContext(processCtx)
			if c.metrics != nil {
				processCtx, span = c.metrics.startBatchSpan(sendCtx, c.consumerName, c.jobId, len(msgs), batchBytes)
			}

			func(ctx context.Context, span trace.Span) {
				defer span.End()

				if c.metrics != nil {
					c.metrics.recordBatch(ctx, c.consumerName, c.jobId, len(msgs), batchBytes)
				}

				start := time.Now()
				lambdaResp, err := c.sendEventFunc(c, ctx, lambdaReq)
				dur := time.Since(start)
				c.log.Infof("[%v:%v] Processed with status: %v", c.consumerName, c.jobId, err)
				if c.metrics != nil {
					c.metrics.recordLambdaResult(ctx, c.consumerName, c.jobId, dur, err)
				}
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					return
				}

				var batchErr error

				// process lambda response.
				if lambdaResp != nil {
					if err := c.outputPolicy.Process(ctx, lambdaReq, lambdaResp); err != nil {
						batchErr = err
						if c.metrics != nil {
							c.metrics.recordOutputError(ctx, c.consumerName, c.jobId, err)
						}
						c.log.Errorf("[%v:%v] error handling lambda response: %v", c.consumerName, c.jobId, err)
						span.RecordError(err)
					}
				}

				successfulCommits := 0
				for _, msg := range msgs {
					_, err := c.kafkaConsumer.CommitMessage(msg)
					if err != nil {
						batchErr = err
						if c.metrics != nil {
							c.metrics.recordCommitError(ctx, c.consumerName, c.jobId, err)
						}
						c.log.Errorf("[%v:%v] error committing message offset: %v", c.consumerName, c.jobId, err)
						span.RecordError(err)
					} else {
						successfulCommits++
						c.log.Infof("[%v:%v] committed message offset: %v", c.consumerName, c.jobId, msg.TopicPartition.Offset)
					}
				}

				if successfulCommits > 0 && c.metrics != nil {
					c.metrics.recordMessagesProcessed(ctx, c.consumerName, c.jobId, successfulCommits)
				}

				if batchErr != nil {
					span.SetStatus(codes.Error, batchErr.Error())
					return
				}

				span.SetStatus(codes.Ok, "processed")
			}(processCtx, span)
		}
	}
}

func (c *ProcessingJob) Cancel() {
	defer c.done()
}

func (c *ProcessingJob) done() {
	c.shutdownOnce.Do(func() {
		c.log.Infof("[%v:%v] consumer job has been stopped", c.consumerName, c.jobId)

		// cancel job context.
		c.ctxCancel()

		if c.metrics != nil {
			c.metrics.jobFinished(c.ctx, c.consumerName, c.jobId)
		}

		if c.kafkaConsumer != nil {
			if err := c.kafkaConsumer.Close(); err != nil {
				c.log.Errorf("[%v:%v] error closing consumer: %v", c.consumerName, c.jobId, err)
			}
		}

		if c.outputPolicy != nil {
			if err := c.outputPolicy.Close(); err != nil {
				c.log.Errorf("[%v:%v] error closing output policy: %v", c.consumerName, c.jobId, err)
			}
		}
	})
}

func approximateBatchBytes(msgs []*kafka.Message) int64 {
	var total int64
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		total += int64(len(msg.Value) + len(msg.Key))
		for _, header := range msg.Headers {
			total += int64(len(header.Key) + len(header.Value))
		}
	}
	return total
}

func (c *ProcessingJob) ensureOutputProducer() error {
	if c.outputPolicy == nil {
		return nil
	}
	if c.consumerCfg.GetKafka() == nil {
		return nil
	}
	if c.outputPolicy.kafkaProducer != nil {
		return nil
	}

	producerName := fmt.Sprintf("%s-output-%d", c.consumerName, c.jobId)
	producer, err := kafka.NewProducer(c.ctx, kafka.ProducerParams{
		Name:    producerName,
		Cluster: c.clusterCfg,
	})
	if err != nil {
		return err
	}

	c.outputPolicy.kafkaProducer = producer
	if c.outputPolicy.log == nil {
		c.outputPolicy.log = c.log
	}
	return nil
}
