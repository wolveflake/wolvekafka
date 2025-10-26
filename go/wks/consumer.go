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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
)

const (
	// defaultPoolDurationMs is the default duration (in milliseconds) for polling Kafka messages.
	defaultPoolDurationMs = 100
)

type Consumer struct {
	// consumer name.s
	name string

	// polling duration in milliseconds.
	poolDurationMs int

	// Kafka consumer instance.
	kafkaConsumer *kafka.Consumer

	// logger.
	log *zap.SugaredLogger
}

type ConsumerParams struct {
	// consumer name (for logging and metrics).
	Name string

	// Kafka consumer configuration.
	ConfigMap *kafka.ConfigMap

	// polling duration.
	PoolDuration time.Duration

	// topics to subscribe to.
	Topics []string
}

func NewConsumer(ctx context.Context, params ConsumerParams) (*Consumer, error) {
	consumer := newConsumer(ctx, params)

	// create Kafka consumer.
	kafkaConsumer, err := kafka.NewConsumer(params.ConfigMap)
	if err != nil {
		consumer.log.Errorf("Failed to create %v consumer: %v", params.Name, err)
		return nil, err
	}

	// subscribe to topics.
	err = consumer.SubscribeTopics(params.Topics, nil)
	if err != nil {
		consumer.log.Errorf("Failed to subscribe to %v topics, %s consumer: %v", consumer.params.Topics, params.Name, err)
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
		name:           params.Name,
		poolDurationMs: defaultPoolDurationMs,
		log:            log,
	}
}
