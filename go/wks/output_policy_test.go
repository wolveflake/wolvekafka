// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputPolicyProcessDefaultsToDrop(t *testing.T) {
	ctx := context.Background()
	policy := &OutputPolicy{
		consumerCfg: &configs_pb.Consumer{},
		log:         zap.NewNop().Sugar(),
	}

	err := policy.Process(ctx, &model_pb.LambdaRequest{}, &model_pb.LambdaResponse{})
	require.NoError(t, err)
}

func TestOutputPolicyProcessKafkaProducesMessages(t *testing.T) {
	ctx := context.Background()
	var produced []*kafka.Message

	outputCfg := &configs_pb.OutputKafkaPolicy{
		Topic:       "output-topic",
		CopyHeaders: true,
	}

	policy := &OutputPolicy{
		consumerCfg: &configs_pb.Consumer{
			OutputPolicy: &configs_pb.Consumer_Kafka{
				Kafka: outputCfg,
			},
		},
		kafkaProducer: &kafka.Producer{
			ProducerFunc: func(msg *kafka.Message) error {
				produced = append(produced, msg)
				return nil
			},
		},
		log: zap.NewNop().Sugar(),
	}

	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{
				Topic:     "input-topic",
				Partition: 1,
				Offset:    10,
				Headers: []*model_pb.Header{
					{Key: "x-header", Value: []byte("value")},
				},
				Key:   []byte("key"),
				Value: []byte("value"),
			},
		},
	}

	resp := &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{
				Status:  model_pb.ResponseStatus_STATUS_SUCCESS,
				Message: []byte("payload"),
			},
		},
	}

	err := policy.Process(ctx, req, resp)
	require.NoError(t, err)

	nonNil := filterNonNilMessages(produced)
	require.Len(t, nonNil, 1)
	assert.Equal(t, []byte("payload"), nonNil[0].Value)
	require.Len(t, nonNil[0].Headers, len(req.Messages[0].Headers))
	assert.Equal(t, req.Messages[0].Headers[0].GetKey(), nonNil[0].Headers[0].Key)
	assert.Equal(t, req.Messages[0].Headers[0].GetValue(), nonNil[0].Headers[0].Value)
}

func TestOutputPolicyProcessKafkaSkipsEmptyMessagesWhenConfigured(t *testing.T) {
	ctx := context.Background()
	var produced []*kafka.Message

	outputCfg := &configs_pb.OutputKafkaPolicy{
		Topic:            "output-topic",
		SkipEmptyMessage: true,
		CopyHeaders:      false,
	}

	policy := &OutputPolicy{
		consumerCfg: &configs_pb.Consumer{
			OutputPolicy: &configs_pb.Consumer_Kafka{
				Kafka: outputCfg,
			},
		},
		kafkaProducer: &kafka.Producer{
			ProducerFunc: func(msg *kafka.Message) error {
				produced = append(produced, msg)
				return nil
			},
		},
		log: zap.NewNop().Sugar(),
	}

	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{
				Topic:     "input-topic",
				Partition: 0,
				Offset:    1,
			},
		},
	}
	resp := &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{
				Status:  model_pb.ResponseStatus_STATUS_SUCCESS,
				Message: nil,
			},
		},
	}

	err := policy.Process(ctx, req, resp)
	require.NoError(t, err)

	nonNil := filterNonNilMessages(produced)
	assert.Len(t, nonNil, 0)
}

func TestOutputPolicyProcessKafkaResponseMessageError(t *testing.T) {
	ctx := context.Background()

	outputCfg := &configs_pb.OutputKafkaPolicy{
		Topic: "output-topic",
	}

	policy := &OutputPolicy{
		consumerCfg: &configs_pb.Consumer{
			OutputPolicy: &configs_pb.Consumer_Kafka{
				Kafka: outputCfg,
			},
		},
		kafkaProducer: &kafka.Producer{
			ProducerFunc: func(msg *kafka.Message) error {
				t.Fatalf("unexpected produce call")
				return nil
			},
		},
		log: zap.NewNop().Sugar(),
	}

	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Topic: "input-topic"},
		},
	}
	resp := &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{
				Status:   model_pb.ResponseStatus_STATUS_SUCCESS,
				Message:  nil,
				Encoding: model_pb.MessageEncoding_ENCODING_DEFAULT,
			},
		},
	}

	err := policy.Process(ctx, req, resp)
	require.Error(t, err)
}

func TestOutputPolicyProcessKafkaProducerErrorPropagates(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("produce failed")

	outputCfg := &configs_pb.OutputKafkaPolicy{
		Topic: "output-topic",
	}

	policy := &OutputPolicy{
		consumerCfg: &configs_pb.Consumer{
			OutputPolicy: &configs_pb.Consumer_Kafka{
				Kafka: outputCfg,
			},
		},
		kafkaProducer: &kafka.Producer{
			ProducerFunc: func(msg *kafka.Message) error {
				if msg == nil {
					return nil
				}
				return expectedErr
			},
		},
		log: zap.NewNop().Sugar(),
	}

	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{Topic: "input-topic"},
		},
	}
	resp := &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{Message: []byte("payload")},
		},
	}

	err := policy.Process(ctx, req, resp)
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

func filterNonNilMessages(msgs []*kafka.Message) []*kafka.Message {
	nonNil := make([]*kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg != nil {
			nonNil = append(nonNil, msg)
		}
	}
	return nonNil
}
