// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"

	"time"
	"wolvekafka/go/kafka"

	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

var (
	// Configure protojson to match the legacy jsonpb behaviour we relied on.
	marshaler = protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseEnumNumbers:  true,
	}
	// Allow round-tripping JSON back into the proto message.
	unmarshaler = protojson.UnmarshalOptions{}
)

// kafkaMsgToProto converts a single Kafka message to its protobuf representation.
func kafkaMsgToProto(msg *kafka.Message) *model_pb.KafkaMessage {
	headers := make([]*model_pb.Header, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		headers = append(headers, &model_pb.Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return &model_pb.KafkaMessage{
		Topic:     msg.Topic,
		Partition: int32(msg.Partition),
		Offset:    int64(msg.Offset),
		Headers:   headers,
		Key:       msg.Key,
		Value:     msg.Value,
		Encoding:  model_pb.MessageEncoding_ENCODING_DEFAULT,
	}
}

// kafkaMsgsToLambdaRequest converts a slice of Kafka messages to a LambdaRequest protobuf message.
func kafkaMsgsToLambdaRequest(msgs []*kafka.Message) *model_pb.LambdaRequest {
	lambdaReq := &model_pb.LambdaRequest{
		Messages: make([]*model_pb.KafkaMessage, 0, len(msgs)),
	}
	for _, msg := range msgs {
		lambdaReq.Messages = append(lambdaReq.Messages, kafkaMsgToProto(msg))
	}

	return lambdaReq
}

// ConvertLambdaRequestToJson converts a LambdaRequest protobuf message to its JSON representation.
func ConvertLambdaRequestToJson(lambdaReq *model_pb.LambdaRequest) ([]byte, error) {
	return marshaler.Marshal(lambdaReq)
}

// ParseLambdaRequestJson parses a JSON representation of a LambdaRequest into its protobuf message.
func ParseLambdaRequestJson(payload []byte) (*model_pb.LambdaRequest, error) {
	result := &model_pb.LambdaRequest{}
	if err := unmarshaler.Unmarshal(payload, result); err != nil {
		return nil, err
	}
	return result, nil
}

// ParseLambdaResponse parses a JSON representation of a LambdaResponse into its protobuf message.
func ParseLambdaResponse(payload []byte) (*model_pb.LambdaResponse, error) {
	result := &model_pb.LambdaResponse{}
	if err := unmarshaler.Unmarshal(payload, result); err != nil {
		return nil, err
	}
	return result, nil
}

// ResponseMessageToKafkaMessage constructs kafka message based on response messages and output policy.
func ResponseMessageToKafkaMessage(policy *configs_pb.OutputKafkaPolicy, msg *model_pb.ResponseMessage) (*kafka.Message, error) {
	value := msg.GetMessage()
	if value == nil || len(value) == 0 {
		if msg.GetEncoding() != model_pb.MessageEncoding_ENCODING_JSON {
			return nil, fmt.Errorf("response message is empty and doesn't have default value")
		}
		value = emptyJsonMessageValue
	}

	// Construct the kafka.Message
	return &kafka.Message{
		Topic:     policy.Topic,
		Partition: kafka.PartitionAny,
		Value:     value,
		Timestamp: time.Now(), // Optional: Message timestamp
	}, nil
}

func HeadersToKafkaHeaders(headers []*model_pb.Header) []kafka.Header {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for _, header := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   header.GetKey(),
			Value: header.GetValue(),
		})
	}

	return kafkaHeaders
}
