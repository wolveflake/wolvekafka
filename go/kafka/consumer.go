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
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
	configs_pb "wolvekafka/proto/configs"
)

const (
	// defaultPoolDurationMs is the default duration (in milliseconds) for polling Kafka messages.
	defaultPoolDurationMs = 100
	// defaultBatchMaxMessages flushes after each message unless overridden.
	defaultBatchMaxMessages = 1
	// defaultBatchMaxBytes is an ample cap to prevent unbounded batch growth.
	defaultBatchMaxBytes = 1 << 20 // 1 MiB
	// defaultBatchMaxWait bounds how long a partial batch can wait before being emitted.
	defaultBatchMaxWait = 500 * time.Millisecond
)

// KafkaConsumerClient is the minimal surface area required from a Kafka consumer implementation.
type KafkaConsumerClient interface {
	Poll(int) kafka.Event
	Close() error
	CommitMessage(*Message) ([]TopicPartition, error)
}

type Consumer struct {
	// consumer name.
	name string

	ctx context.Context

	// polling duration in milliseconds.
	poolDurationMs int

	// Kafka consumer instance.
	kafkaConsumer KafkaConsumerClient

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
	PoolDuration time.Duration

	// topics to subscribe to.
	Topics []string

	// batch configuration for Poll().
	BatchMaxMessages int
	BatchMaxBytes    int
	BatchMaxWait     time.Duration

	// Client allows injecting a Kafka consumer implementation (for testing).
	Client KafkaConsumerClient
}

// NewConsumer creates a new Kafka consumer and subscribes it to the specified topics.
func NewConsumer(ctx context.Context, params ConsumerParams) (*Consumer, error) {
	consumer := newConsumer(ctx, params)

	if params.Client != nil {
		consumer.kafkaConsumer = params.Client
		return consumer, nil
	}

	if params.Cluster == nil {
		return nil, fmt.Errorf("cluster configuration is required")
	}

	bootstrapServers := ""
	for _, server := range params.Cluster.BootstrapServers {
		bootstrapServers += "," + server
	}
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          params.Name,
		"auto.offset.reset": "earliest",
	}

	kafkaConsumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		consumer.log.Errorf("Failed to create %v consumer: %v", params.Name, err)
		return nil, err
	}

	if err := kafkaConsumer.SubscribeTopics(params.Topics, nil); err != nil {
		consumer.log.Errorf("Failed to subscribe to %v topics, %s consumer: %v", params.Topics, params.Name, err)
		return nil, err
	}

	consumer.kafkaConsumer = kafkaConsumer

	return consumer, nil
}

// newConsumer creates a new Consumer instance without initializing the Kafka consumer.
func newConsumer(ctx context.Context, params ConsumerParams) *Consumer {
	poolDurationMs := params.PoolDuration.Milliseconds()
	if poolDurationMs <= 0 {
		poolDurationMs = defaultPoolDurationMs
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

	return &Consumer{
		ctx:              ctx,
		name:             params.Name,
		poolDurationMs:   int(poolDurationMs),
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

			event := c.kafkaConsumer.Poll(c.poolDurationMs)
			if event != nil {
				switch e := event.(type) {
				case *kafka.Message:
					batch = append(batch, e)
					batchBytes += approximateMessageSize(e)

					if len(batch) == 1 && c.batchMaxWait > 0 {
						flushDeadline = time.Now().Add(c.batchMaxWait)
					}

					if (c.batchMaxMessages > 0 && len(batch) >= c.batchMaxMessages) ||
						(c.batchMaxBytes > 0 && batchBytes >= c.batchMaxBytes) {
						flush()
					}
				case kafka.Error:
					c.log.Errorf("Kafka poll error: %v", e)
					if e.Code() == kafka.ErrAllBrokersDown {
						flush()
						return
					}
				default:
					// ignore other events
				}
			}

			if len(batch) > 0 && c.batchMaxWait > 0 && !flushDeadline.IsZero() && time.Now().After(flushDeadline) {
				flush()
			}
		}
	}()

	return msgChan, nil
}

func approximateMessageSize(msg *kafka.Message) int {
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
	if c.kafkaConsumer == nil {
		return nil
	}
	return c.kafkaConsumer.Close()
}

// CommitMessage commits the offset of the provided message.
func (c *Consumer) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if c.kafkaConsumer == nil {
		return nil, fmt.Errorf("kafka consumer is not initialized")
	}
	return c.kafkaConsumer.CommitMessage(msg)
}
