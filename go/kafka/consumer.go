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
)

type PollFunc func() (chan []*Message, error)
type CommitMessageFunc func(msg *Message) ([]TopicPartition, error)

type Consumer struct {
	// consumer name.s
	name string

	ctx context.Context

	// polling duration in milliseconds.
	poolDurationMs int

	// Kafka consumer instance.
	kafkaConsumer *kafka.Consumer

	// logger.
	log *zap.SugaredLogger

	// polling function (mostly for testing purposes).
	pollFunc PollFunc
	// commit message function (mostly for testing purposes).
	commitMessageFunc CommitMessageFunc
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

	// polling function (mostly for testing purposes), but if is set, the Kafka consumer is not initialized.
	PollFunc PollFunc
	// commit message function (mostly for testing purposes).
	CommitMessageFunc CommitMessageFunc
}

// NewConsumer creates a new Kafka consumer and subscribes it to the specified topics.
func NewConsumer(ctx context.Context, params ConsumerParams) (*Consumer, error) {
	consumer := newConsumer(ctx, params)

	// skip Kafka consumer initialization if a custom polling function is provided.
	if params.PollFunc != nil {
		return consumer, nil
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

	// create Kafka consumer.
	kafkaConsumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		consumer.log.Errorf("Failed to create %v consumer: %v", params.Name, err)
		return nil, err
	}

	// subscribe to topics.
	err = kafkaConsumer.SubscribeTopics(params.Topics, nil)
	if err != nil {
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

	log := logger.S()

	return &Consumer{
		ctx:               ctx,
		name:              params.Name,
		poolDurationMs:    defaultPoolDurationMs,
		log:               log,
		pollFunc:          params.PollFunc,
		commitMessageFunc: params.CommitMessageFunc,
	}
}

// Poll polls a message from Kafka.
func (c *Consumer) Poll() (chan []*Message, error) {
	if c.pollFunc != nil {
		return c.pollFunc()
	}

	return nil, fmt.Errorf("not implemented")
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() error {
	// no instance in tests.
	if c.kafkaConsumer == nil {
		return nil
	}
	return c.kafkaConsumer.Close()
}

// CommitMessage commits the offset of the provided message.
func (c *Consumer) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if c.commitMessageFunc != nil {
		return c.commitMessageFunc(msg)
	}
	return c.kafkaConsumer.CommitMessage(msg)
}
