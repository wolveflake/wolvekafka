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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type sendEventFunc func(ctx context.Context, msgs []*kafka.Message) error

type ConsumerJob struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// consumer name.
	name string

	// kafka consumer instance.
	consumer *Consumer

	// callback function to send events.
	sendEventFunc sendEventFunc

	// logger instance.
	log *zap.SugaredLogger
}

func defaultSendEventFunc(ctx context.Context, msgs []*kafka.Message) error {

	return nil
}

func (c *ConsumerJob) Execute() {
	c.log.Infof("[%v] starting consumer job", c.name)
	// start polling kafka messages.
	msgChan, err := c.consumer.Poll()
	if err != nil {
		c.log.Errorf("[%v] error polling messages: %v", c.name, err)
	}

	for {
		select {
		case <-c.ctx.Done():
			c.log.Info("[%v] stopping consumer job", c.name)
		case msgs, ok := <-msgChan:
			if !ok {
				c.log.Infof("[%v] message channel closed", c.name)
				c.ctxCancel()
				return
			}

			for _, msg := range msgs {
				c.log.Infof(
					"[%v] Consumed message from topic %s [%d] at offset %v",
					c.name,
					*msg.TopicPartition.Topic,
					msg.TopicPartition.Partition,
					msg.TopicPartition.Offset)
			}

			err = c.sendEventFunc(c.ctx, msgs)
			c.log.Infof("[%v] Processed with status: %v", c.name, err)
			if err != nil {
				continue
			}

			// You can process the message here
			// For automatic offset committing, no explicit commit is needed here with "enable.auto.commit": "true"
			// For manual offset committing, you would use consumer.CommitMessage(e)
			for _, msg := range msgs {
				_, err := c.consumer.CommitMessage(msg)
				if err != nil {
					c.log.Errorf("[%v] error committing message offset: %v", c.name, err)
				} else {
					c.log.Infof("[%v] committed message offset: %v", c.name, msg.TopicPartition.Offset)
				}
			}
		}
	}
}

func (c *ConsumerJob) Cancel() {
	defer c.done()
}

func (c *ConsumerJob) done() {
	c.log.Info("[%v] consumer job has been stopped", c.name)

	// cancel job context.
	c.ctxCancel()

	err := c.consumer.Close()
	if err != nil {
		c.log.Errorf("[%v] error closing consumer: %v", c.name, err)
	}
}
