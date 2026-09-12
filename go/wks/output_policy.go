// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"

	"wolvekafka/go/common/logger"
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
		consumerCfg:   params.ConsumerCfg,
		kafkaProducer: params.KafkaProducer,
		log:           logger.S(),
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

	if len(resp.GetMessages()) != len(req.GetMessages()) {
		return fmt.Errorf("response message count (%d) does not match request message count (%d)",
			len(resp.GetMessages()), len(req.GetMessages()))
	}

	// construct kafka message.
	msgs := make([]*kafka.Message, 0, len(resp.GetMessages()))

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

	// produce generated messages in a single batch.
	if err := o.kafkaProducer.ProduceBatch(ctx, msgs); err != nil {
		o.log.Errorf("failed to produce batch of %d messages: %v", len(msgs), err)
		return err
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
