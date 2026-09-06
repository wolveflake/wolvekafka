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

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
	configs_pb "wolvekafka/proto/configs"
)

type ProducerFunc func(msg *Message) error

type Producer struct {
	// producer name.
	name string

	ctx       context.Context
	ctxCancel context.CancelFunc

	// Kafka producer instance.
	kafkaProducer *kgo.Client

	// logger.
	log *zap.SugaredLogger

	// polling function (mostly for testing purposes).
	ProducerFunc ProducerFunc
}

type ProducerParams struct {
	// producer name (for logging and metrics).
	Name string

	// Kafka configuration.
	Cluster *configs_pb.Cluster

	// polling function (mostly for testing purposes).
	ProducerFunc ProducerFunc
}

func NewProducer(ctx context.Context, params ProducerParams) (*Producer, error) {
	producer := newProducer(ctx, params)

	if params.Cluster == nil {
		producer.ctxCancel()
		return nil, fmt.Errorf("cluster configuration is required")
	}

	// create Kafka producer.
	opts, err := ConnectionOptions(params.Cluster)
	if err != nil {
		producer.ctxCancel()
		return nil, err
	}
	opts = append(opts,
		kgo.ClientID(params.Name),
		kgo.RequiredAcks(kgo.AllISRAcks()), // Require all in-sync replicas to acknowledge
	)

	kafkaProducer, err := kgo.NewClient(opts...)
	if err != nil {
		producer.ctxCancel()
		producer.log.Errorf("Failed to create %v producer: %v", params.Name, err)
		return nil, err
	}
	producer.kafkaProducer = kafkaProducer

	go func() {
		select {
		case <-producer.ctx.Done():
			producer.log.Errorf("Context has been cancelled, producer: %v", params.Name)
			producer.Close()
		}
	}()

	return producer, nil
}

// newProducer creates a new Producer instance without initializing the Kafka producer.
func newProducer(ctx context.Context, params ProducerParams) *Producer {
	producerCtx, producerCancel := context.WithCancel(ctx)
	log := logger.S()

	return &Producer{
		ctx:          producerCtx,
		ctxCancel:    producerCancel,
		name:         params.Name,
		log:          log,
		ProducerFunc: params.ProducerFunc,
	}
}

// Produce sends a message to Kafka.
func (c *Producer) Produce(ctx context.Context, msg *Message) error {
	if c.ProducerFunc != nil {
		return c.ProducerFunc(msg)
	}

	if c.kafkaProducer == nil {
		return fmt.Errorf("kafka producer is not initialized")
	}

	if msg == nil {
		return fmt.Errorf("message is required")
	}

	// Use ProduceSync for synchronous production with delivery guarantees
	result := c.kafkaProducer.ProduceSync(ctx, msg)
	if err := result.FirstErr(); err != nil {
		return err
	}

	return nil
}

// ProduceBatch sends multiple messages to Kafka in a single synchronous call.
func (c *Producer) ProduceBatch(ctx context.Context, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}

	if c.ProducerFunc != nil {
		for _, msg := range msgs {
			if err := c.ProducerFunc(msg); err != nil {
				return err
			}
		}
		return nil
	}

	if c.kafkaProducer == nil {
		return fmt.Errorf("kafka producer is not initialized")
	}

	result := c.kafkaProducer.ProduceSync(ctx, msgs...)
	if err := result.FirstErr(); err != nil {
		return err
	}

	return nil
}

// Close closes the Kafka producer.
func (c *Producer) Close() error {
	c.ctxCancel()

	// no instance in tests.
	if c.kafkaProducer == nil {
		return nil
	}

	c.kafkaProducer.Close()
	return nil
}
