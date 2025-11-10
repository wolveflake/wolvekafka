///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2025 [wolveflake LLC], All Rights Reserved.
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

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"

	"go.uber.org/zap"
)

var (
	emptyJsonMessageValue = []byte("{}")
)

type OutputPolicy struct {
	kafkaProducer *kafka.Producer

	consumerCfg *configs_pb.Consumer

	// logger instance.
	log *zap.SugaredLogger
}

type OutputPolicyParams struct {
	KafkaProducer *kafka.Producer

	// configuration part.
	ConsumerCfg *configs_pb.Consumer
}

func NewOutputPolicy(params OutputPolicyParams) *OutputPolicy {
	return &OutputPolicy{
		consumerCfg: params.ConsumerCfg,
	}
}

func (o *OutputPolicy) executeDrop(ctx context.Context, req *model_pb.LambdaRequest, resp *model_pb.LambdaResponse) error {
	return nil
}

func (o *OutputPolicy) executeKafka(ctx context.Context, req *model_pb.LambdaRequest, resp *model_pb.LambdaResponse) error {

	if o.kafkaProducer == nil {
		return fmt.Errorf("kafka producer is not initialized")
	}

	policy := o.consumerCfg.GetKafka()

	// construct kafka message.
	msgs := make([]*kafka.Message, len(resp.GetMessages()))

	for i, resp := range resp.GetMessages() {
		// extracting meta information from request for logging.
		reqMsg := req.Messages[i]
		topic := reqMsg.GetTopic()
		partition := reqMsg.GetPartition()
		offset := reqMsg.GetOffset()

		// skipping empty message.
		if len(resp.GetMessage()) == 0 && policy.GetSkipEmptyMessage() {
			o.log.Infof("Skipping empty response: topic: %v, partition: %v, offset: %v", topic, partition, offset)
			continue
		}

		outMsg, err := ResponseMessageToKafkaMessage(policy, resp)
		if err != nil {
			o.log.Infof(
				"failed to construct kafka message for response based on request: topic: %v, partition: %v, offset: %v, err: %v",
				topic,
				partition,
				offset,
				err)
			return err
		}

		// copy headers if it is needed.
		if policy.GetCopyHeaders() {
			outMsg.Headers = HeadersToKafkaHeaders(reqMsg.GetHeaders())
		}

		msgs = append(msgs, outMsg)
	}

	// produce generated messages.
	// TODO(13): switch to batch API.
	for i, msg := range msgs {
		err := o.kafkaProducer.Produce(ctx, msg)
		if err != nil {
			o.log.Infof(
				"failed to produce message [%d/%d]: err: %v", i, len(msgs), err)
			return err
		}
	}

	return nil
}

func (o *OutputPolicy) Process(ctx context.Context, req *model_pb.LambdaRequest, resp *model_pb.LambdaResponse) error {
	// drop policy is the default.
	if o.consumerCfg.GetOutputPolicy() == nil {
		return o.executeDrop(ctx, req, resp)
	}

	switch policy := o.consumerCfg.GetOutputPolicy().(type) {
	case *configs_pb.Consumer_Drop:
		return o.executeDrop(ctx, req, resp)
	case *configs_pb.Consumer_Kafka:
		return o.executeKafka(ctx, req, resp)
	default:
		return fmt.Errorf("unsupported output policy: %v", policy)
	}
}

func (o *OutputPolicy) Close() error {
	if o.kafkaProducer == nil {
		return nil
	}

	err := o.kafkaProducer.Close()
	if err != nil {
		o.log.Errorw("failed to close kafka producer", "error", err)
		return err
	}
	return nil
}
