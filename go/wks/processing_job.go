// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

var (
	// interval to recreate consumer in case of failure.
	consumerRecreationInterval = 10 * time.Second

	// defaultLambdaTimeout is the HTTP client timeout for lambda invocations.
	defaultLambdaTimeout = 30 * time.Second

	// lambdaHTTPClient is a shared HTTP client with a reasonable timeout.
	lambdaHTTPClient = &http.Client{
		Timeout: defaultLambdaTimeout,
	}
)

type sendEventFunc func(c *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*BatchResult, error)

type consumerConfigCtxKey struct{}

type ProcessingJob struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// consumer name.
	consumerName string
	jobId        int

	metrics *instrumentation

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

	// retry policy handler.
	retryPolicy *RetryPolicy

	shutdownOnce sync.Once
}

func defaultSendEventFunc(c *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*BatchResult, error) {
	if len(req.GetMessages()) == 0 {
		return nil, nil
	}

	sendOnce := func(ctx context.Context, batch *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		lambda := c.lambdaSelector.Select()
		payload, err := ConvertLambdaRequestToJson(batch)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal lambda request: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, lambda.GetUrl(), bytes.NewReader(payload))
		if err != nil {

			return nil, fmt.Errorf("failed to create http request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		// Let the handler join the batch's trace (traceparent, tracestate, baggage).
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

		resp, err := lambdaHTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send lambda request: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("lambda request failed with status %d and read error: %w", resp.StatusCode, err)
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			reason := strings.TrimSpace(string(body))
			if reason == "" {
				reason = resp.Status
			}
			err := fmt.Errorf("lambda request failed with status %d: %s", resp.StatusCode, reason)
			if throttled := throttleFromResponse(resp.StatusCode, resp.Header, time.Now(), err); throttled != nil {
				return nil, throttled
			}
			return nil, err
		}

		respMsgs, err := ParseLambdaResponse(body)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal lambda response: %w", err)
		}

		return respMsgs, nil
	}

	if c.retryPolicy == nil {
		c.retryPolicy = NewRetryPolicy(RetryPolicyParams{ConsumerCfg: c.consumerCfg, ConsumerName: c.consumerName, JobId: c.jobId, Metrics: c.metrics})
	}
	return c.retryPolicy.ExecuteBatch(ctx, req, sendOnce)
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
		Name:            c.consumerName,
		Cluster:         c.clusterCfg,
		PollDuration:    time.Millisecond * 500,
		Topics:          c.consumerCfg.GetTopics(),
		RequireBatchAck: true,
	}
	if batchCfg := c.consumerCfg.GetBatch(); batchCfg != nil {
		if v := batchCfg.GetMaxMessages(); v > 0 {
			consumerParams.BatchMaxMessages = int(v)
		}
		if v := batchCfg.GetMaxBytes(); v > 0 {
			consumerParams.BatchMaxBytes = int(v)
		}
		if v := batchCfg.GetMaxWaitMs(); v > 0 {
			consumerParams.BatchMaxWait = time.Duration(v) * time.Millisecond
		}
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

	if err := c.ensureRetryPolicy(); err != nil {
		c.log.Errorf("[%v:%v] failed to initialize retry policy: %v", c.consumerName, c.jobId, err)
		return
	}

	msgChan, err := c.kafkaConsumer.Poll()
	if err != nil {
		c.log.Errorf("[%v:%v] error polling messages: %v", c.consumerName, c.jobId, err)
		return
	}

	for {
		var msgs []*kafka.Message
		var ok bool

		select {
		case <-c.ctx.Done():
			// Leave unfinished input uncommitted for replay on the next owner.
			return
		case msgs, ok = <-msgChan:
			if !ok {
				c.log.Infof("[%v:%v] message channel closed", c.consumerName, c.jobId)
				c.ctxCancel()
				return
			}
		}

		if len(msgs) > 0 {
			first := msgs[0]
			last := msgs[len(msgs)-1]
			c.log.Infof("[%v:%v] received batch of %d messages (%s[%d] offsets %v..%v)",
				c.consumerName, c.jobId, len(msgs), first.Topic, first.Partition, first.Offset, last.Offset)
		}

		sendCtx := context.WithValue(c.kafkaConsumer.BatchContext(), consumerConfigCtxKey{}, c.consumerCfg)
		// convert kafka messages to lambda request.
		lambdaReq := kafkaMsgsToLambdaRequest(msgs)
		batchBytes := approximateBatchBytes(msgs)
		// Continue (or link to) the producers' traces carried in record headers.
		processCtx, links := batchSpanParent(sendCtx, otel.GetTextMapPropagator(), msgs)
		span := trace.SpanFromContext(processCtx)
		if c.metrics != nil {
			processCtx, span = c.metrics.startBatchSpan(processCtx, c.consumerName, c.jobId, len(msgs), batchBytes, links)
		}

		processed := func(ctx context.Context, span trace.Span) bool {
			defer span.End()

			if c.metrics != nil {
				c.metrics.recordBatch(ctx, c.consumerName, c.jobId, len(msgs), batchBytes)
			}

			start := time.Now()
			var result *BatchResult
			err := c.retryUntilResolved(ctx, func() error {
				var err error
				result, err = c.sendEventFunc(c, ctx, lambdaReq)
				if err == nil && result == nil {
					return fmt.Errorf("handler returned no batch result")
				}
				return err
			})
			dur := time.Since(start)
			c.log.Infof("[%v:%v] Processed with status: %v", c.consumerName, c.jobId, err)
			if c.metrics != nil {
				c.metrics.recordLambdaResult(ctx, c.consumerName, c.jobId, dur, err)
			}
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return false
			}

			// process lambda response.
			if len(result.Response.GetMessages()) > 0 {
				if err := c.retryUntilResolved(ctx, func() error {
					err := c.outputPolicy.Process(ctx, result.Request, result.Response)
					if err != nil && c.metrics != nil {
						c.metrics.recordOutputError(ctx, c.consumerName, c.jobId, err)
					}
					return err
				}); err != nil {
					c.log.Errorf("[%v:%v] error handling lambda response: %v", c.consumerName, c.jobId, err)
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					return false
				}
			}

			// Commit only the highest offset per topic-partition.
			type tpKey struct {
				topic     string
				partition int32
			}
			highest := make(map[tpKey]*kafka.Message)
			for _, msg := range msgs {
				key := tpKey{topic: msg.Topic, partition: msg.Partition}
				if prev, ok := highest[key]; !ok || msg.Offset > prev.Offset {
					highest[key] = msg
				}
			}

			var batchErr error
			successfulCommits := 0
			for _, msg := range highest {
				err := c.retryUntilResolved(ctx, func() error {
					_, err := c.kafkaConsumer.CommitMessage(msg)
					if err != nil && c.metrics != nil {
						c.metrics.recordCommitError(ctx, c.consumerName, c.jobId, err)
					}
					return err
				})
				if err != nil {
					batchErr = err
					c.log.Errorf("[%v:%v] error committing message offset: %v", c.consumerName, c.jobId, err)
					span.RecordError(err)
				} else {
					successfulCommits++
					c.log.Debugf("[%v:%v] committed offset %v for %s[%d]", c.consumerName, c.jobId, msg.Offset, msg.Topic, msg.Partition)
				}
			}

			if successfulCommits > 0 && batchErr == nil && c.metrics != nil {
				c.metrics.recordBatchOutcomes(ctx, c.consumerName, c.jobId, result)
			}

			if batchErr != nil {
				span.SetStatus(codes.Error, batchErr.Error())
				return false
			}

			span.SetStatus(codes.Ok, "processed")
			return true
		}(processCtx, span)
		if !processed {
			if c.ctx.Err() != nil {
				return
			}
			c.kafkaConsumer.AbortBatch()
			continue
		}
		c.kafkaConsumer.CompleteBatch()
	}
}

// Retry the current phase without reading another input batch. In particular,
// output and commit failures do not invoke a successful handler again.
func (c *ProcessingJob) retryUntilResolved(ctx context.Context, fn func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			if kafka.IsOwnershipError(err) {
				if c.kafkaConsumer != nil {
					c.kafkaConsumer.RequestRejoin()
				}
				return err
			}
			c.log.Errorf("[%s:%d] holding current batch after failure: %v", c.consumerName, c.jobId, err)
		}
		if err := sleepWithContext(ctx, time.Second); err != nil {
			return err
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

		if c.retryPolicy != nil {
			if err := c.retryPolicy.Close(); err != nil {
				c.log.Errorf("[%v:%v] error closing retry policy: %v", c.consumerName, c.jobId, err)
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

func (c *ProcessingJob) ensureRetryPolicy() error {
	if c.retryPolicy != nil {
		return nil
	}

	retryCfg := c.consumerCfg.GetRetryPolicy()
	if retryCfg == nil {
		return nil
	}

	// Create DLQ producer if needed
	var dlqProducer *kafka.Producer
	if dlqAction := retryCfg.GetDlq(); dlqAction != nil && dlqAction.GetTopic() != "" {
		producerName := fmt.Sprintf("%s-dlq-%d", c.consumerName, c.jobId)
		producer, err := kafka.NewProducer(c.ctx, kafka.ProducerParams{
			Name:    producerName,
			Cluster: c.clusterCfg,
		})
		if err != nil {
			return fmt.Errorf("failed to create DLQ producer: %w", err)
		}
		dlqProducer = producer
		c.log.Infof("[%v:%v] DLQ producer initialized for topic: %s", c.consumerName, c.jobId, dlqAction.GetTopic())
	}

	c.retryPolicy = NewRetryPolicy(RetryPolicyParams{
		ConsumerCfg:  c.consumerCfg,
		ConsumerName: c.consumerName,
		JobId:        c.jobId,
		DLQProducer:  dlqProducer,
		Metrics:      c.metrics,
	})

	return nil
}
