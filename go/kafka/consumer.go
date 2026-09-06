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
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
	configs_pb "wolvekafka/proto/configs"
)

const (
	// defaultPollDurationMs is the default duration (in milliseconds) for polling Kafka messages.
	defaultPollDurationMs = 100
	// defaultBatchMaxMessages flushes after each message unless overridden.
	defaultBatchMaxMessages = 1
	// defaultBatchMaxBytes is an ample cap to prevent unbounded batch growth.
	defaultBatchMaxBytes = 1 << 20 // 1 MiB
	// defaultBatchMaxWait bounds how long a partial batch can wait before being emitted.
	defaultBatchMaxWait = 500 * time.Millisecond
)

var errKafkaClientClosed = errors.New("kafka client closed")

// KafkaConsumerClient is the minimal surface area required from a Kafka consumer implementation.
type KafkaConsumerClient interface {
	Poll(int) Event
	Close() error
	CommitMessage(*Message) ([]TopicPartition, error)
}

type Consumer struct {
	// consumer name.
	name string

	// subscribed topics.
	topics []string

	ctx       context.Context
	cancel    context.CancelFunc
	batchDone chan struct{}

	// polling duration in milliseconds.
	pollDurationMs int

	// Kafka consumer instance.
	kafkaConsumer KafkaConsumerClient

	// franz-go client (when not using test stub).
	franzClient *kgo.Client

	// logger.
	log *zap.SugaredLogger

	// batch configuration.
	batchMaxMessages int
	batchMaxBytes    int
	batchMaxWait     time.Duration
}

type ConsumerParams struct {
	// consumer name (for logging and metrics).
	Name string

	// Kafka consumer configuration.
	Cluster *configs_pb.Cluster

	// polling duration.
	PollDuration time.Duration

	// topics to subscribe to.
	Topics []string

	// batch configuration for Poll().
	BatchMaxMessages int
	BatchMaxBytes    int
	BatchMaxWait     time.Duration

	// Client allows injecting a Kafka consumer implementation (for testing).
	Client KafkaConsumerClient
	// RequireBatchAck holds partition ownership until CompleteBatch is called.
	RequireBatchAck bool
}

// NewConsumer creates a new Kafka consumer and subscribes it to the specified topics.
func NewConsumer(ctx context.Context, params ConsumerParams) (*Consumer, error) {
	consumer := newConsumer(ctx, params)

	if params.Client != nil {
		consumer.kafkaConsumer = params.Client
		return consumer, nil
	}

	if params.Cluster == nil {
		consumer.cancel()
		return nil, fmt.Errorf("cluster configuration is required")
	}

	opts, err := ConnectionOptions(params.Cluster)
	if err != nil {
		consumer.cancel()
		return nil, err
	}
	opts = append(opts,
		kgo.ConsumerGroup(params.Name),
		kgo.ConsumeTopics(params.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if params.RequireBatchAck {
		opts = append(opts, kgo.BlockRebalanceOnPoll())
	}

	franzClient, err := kgo.NewClient(opts...)
	if err != nil {
		consumer.cancel()
		consumer.log.Errorf("Failed to create %v consumer: %v", params.Name, err)
		return nil, err
	}

	consumer.franzClient = franzClient
	consumer.kafkaConsumer = &franzConsumerAdapter{client: franzClient, ctx: consumer.ctx}

	return consumer, nil
}

// newConsumer creates a new Consumer instance without initializing the Kafka consumer.
func newConsumer(ctx context.Context, params ConsumerParams) *Consumer {
	pollDurationMs := params.PollDuration.Milliseconds()
	if pollDurationMs <= 0 {
		pollDurationMs = defaultPollDurationMs
	}

	batchMaxMessages := params.BatchMaxMessages
	if batchMaxMessages <= 0 {
		batchMaxMessages = defaultBatchMaxMessages
	}

	batchMaxBytes := params.BatchMaxBytes
	if batchMaxBytes <= 0 {
		batchMaxBytes = defaultBatchMaxBytes
	}

	batchMaxWait := params.BatchMaxWait
	if batchMaxWait <= 0 {
		batchMaxWait = defaultBatchMaxWait
	}

	log := logger.S()
	ctx, cancel := context.WithCancel(ctx)
	var batchDone chan struct{}
	if params.RequireBatchAck {
		batchDone = make(chan struct{})
	}

	return &Consumer{
		ctx:              ctx,
		cancel:           cancel,
		batchDone:        batchDone,
		name:             params.Name,
		topics:           append([]string(nil), params.Topics...),
		pollDurationMs:   int(pollDurationMs),
		log:              log,
		batchMaxMessages: batchMaxMessages,
		batchMaxBytes:    batchMaxBytes,
		batchMaxWait:     batchMaxWait,
	}
}

// Poll polls messages from Kafka and delivers them in batches bounded by the configured limits.
func (c *Consumer) Poll() (chan []*Message, error) {
	if c.kafkaConsumer == nil {
		return nil, fmt.Errorf("kafka consumer is not initialized")
	}

	msgChan := make(chan []*Message, 1)

	go func() {
		defer close(msgChan)

		batch := make([]*Message, 0, c.batchMaxMessages)
		batchBytes := 0
		var flushDeadline time.Time
		pollDegraded := false
		recoverablePollErrors := 0

		flush := func() {
			if len(batch) == 0 {
				return
			}

			out := make([]*Message, len(batch))
			copy(out, batch)

			select {
			case <-c.ctx.Done():
				// drop if context is cancelled.
			case msgChan <- out:
				if c.batchDone != nil {
					select {
					case <-c.ctx.Done():
					case <-c.batchDone:
					}
					if c.franzClient != nil {
						c.franzClient.AllowRebalance()
					}
				}
			}

			batch = batch[:0]
			batchBytes = 0
			flushDeadline = time.Time{}
		}

		for {
			if err := c.ctx.Err(); err != nil {
				flush()
				return
			}

			event := c.kafkaConsumer.Poll(c.pollDurationMs)
			if event != nil {
				switch e := event.(type) {
				case *Message:
					if pollDegraded {
						c.log.Infof(
							"Kafka poll recovered for consumer %q topics=%v after %d recoverable poll errors; delivering records again",
							c.name,
							c.topics,
							recoverablePollErrors,
						)
						pollDegraded = false
						recoverablePollErrors = 0
					}

					batch = append(batch, e)
					batchBytes += approximateMessageSize(e)

					if len(batch) == 1 && c.batchMaxWait > 0 {
						flushDeadline = time.Now().Add(c.batchMaxWait)
					}

					if (c.batchMaxMessages > 0 && len(batch) >= c.batchMaxMessages) ||
						(c.batchMaxBytes > 0 && batchBytes >= c.batchMaxBytes) {
						flush()
					}
				case error:
					flush()
					if errors.Is(e, errKafkaClientClosed) || c.ctx.Err() != nil {
						return
					}

					recoverablePollErrors++
					if !pollDegraded {
						c.log.Errorf(
							"Kafka poll failed for consumer %q topics=%v; worker will keep polling and should rejoin when Kafka is available: %v",
							c.name,
							c.topics,
							e,
						)
						pollDegraded = true
					} else if recoverablePollErrors%10 == 0 {
						c.log.Warnf(
							"Kafka poll still failing for consumer %q topics=%v after %d recoverable poll errors; last error: %v",
							c.name,
							c.topics,
							recoverablePollErrors,
							e,
						)
					}
					time.Sleep(c.pollBackoff())
				default:
					// ignore other events
				}
			}

			if len(batch) > 0 && c.batchMaxWait > 0 && !flushDeadline.IsZero() && time.Now().After(flushDeadline) {
				flush()
			}
			if len(batch) == 0 && c.batchDone != nil && c.franzClient != nil {
				c.franzClient.AllowRebalance()
			}
		}
	}()

	return msgChan, nil
}

func (c *Consumer) pollBackoff() time.Duration {
	backoff := time.Duration(c.pollDurationMs) * time.Millisecond
	if backoff <= 0 {
		return time.Duration(defaultPollDurationMs) * time.Millisecond
	}
	if backoff > time.Second {
		return time.Second
	}
	return backoff
}

func approximateMessageSize(msg *Message) int {
	if msg == nil {
		return 0
	}

	size := len(msg.Value) + len(msg.Key)
	for _, h := range msg.Headers {
		size += len(h.Key) + len(h.Value)
	}

	return size
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() error {
	c.cancel()
	if c.kafkaConsumer == nil {
		return nil
	}
	return c.kafkaConsumer.Close()
}

// CompleteBatch releases the polling loop only after every record is resolved
// and its offset committed. Cancellation leaves the batch available for replay.
func (c *Consumer) CompleteBatch() {
	if c.batchDone != nil {
		select {
		case c.batchDone <- struct{}{}:
		case <-c.ctx.Done():
		}
	}
}

// CommitMessage commits the offset of the provided message.
func (c *Consumer) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if c.kafkaConsumer == nil {
		return nil, fmt.Errorf("kafka consumer is not initialized")
	}
	return c.kafkaConsumer.CommitMessage(msg)
}

// franzConsumerAdapter adapts franz-go client to the KafkaConsumerClient interface.
type franzConsumerAdapter struct {
	client *kgo.Client
	ctx    context.Context
}

func (f *franzConsumerAdapter) Poll(ms int) Event {
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(ms)*time.Millisecond)
	defer cancel()

	fetches := f.client.PollRecords(ctx, 1)
	if fetches.IsClientClosed() {
		return errKafkaClientClosed
	}

	// A fetch can contain records and errors from different partitions. Deliver
	// records first; returning an error must never discard fetched records.
	iter := fetches.RecordIter()
	if !iter.Done() {
		return iter.Next()
	}
	errs := fetches.Errors()
	for _, fetchErr := range errs {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}
		return fetchErr.Err
	}

	return nil
}

func (f *franzConsumerAdapter) Close() error {
	f.client.CloseAllowingRebalance()
	return nil
}

func (f *franzConsumerAdapter) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is required")
	}

	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	if err := f.client.CommitRecords(ctx, msg); err != nil {
		return nil, err
	}

	topic := msg.Topic
	return []TopicPartition{
		{
			Topic:     &topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		},
	}, nil
}
