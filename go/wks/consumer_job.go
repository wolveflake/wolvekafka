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
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	configs_pb "wolvekafka/proto/configs"
)

var (
	// interval to recreate consumer in case of failure.
	consumerRecreationInterval = 10 * time.Second
)

type sendEventFunc func(ctx context.Context, msgs []*kafka.Message) error

type ConsumerJob struct {
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
	kafkaConsumer   *Consumer
	newConsumerFunc func(ctx context.Context, params ConsumerParams) (*Consumer, error)

	// configuration part.
	consumerCfg *configs_pb.Consumer
	clusterCfg  *configs_pb.Cluster
}

func defaultSendEventFunc(ctx context.Context, msgs []*kafka.Message) error {

	return nil
}

// createKafkaConsumer creates a new Kafka consumer with retry logic.
func (c *ConsumerJob) createKafkaConsumer(consumerParams ConsumerParams) (*Consumer, error) {
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

func (c *ConsumerJob) Execute() {
	defer c.done()

	c.log.Infof("[%v:%v] starting consumer job...", c.consumerName, c.jobId)

	// start polling kafka messages.
	consumerParams := ConsumerParams{
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

	for {
		select {
		case <-c.ctx.Done():
			c.log.Infof("[%v:%v] stopping consumer job", c.consumerName, c.jobId)
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

			err = c.sendEventFunc(c.ctx, msgs)
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

func (c *ConsumerJob) Cancel() {
	defer c.done()
}

func (c *ConsumerJob) done() {
	c.log.Infof("[%v:%v] consumer job has been stopped", c.consumerName, c.jobId)

	// cancel job context.
	c.ctxCancel()

	err := c.kafkaConsumer.Close()
	if err != nil {
		c.log.Errorf("[%v:%v] error closing consumer: %v", c.consumerName, c.jobId, err)
	}
}
