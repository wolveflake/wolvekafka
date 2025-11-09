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
	"google.golang.org/protobuf/encoding/protojson"
	"wolvekafka/go/kafka"

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
	var topic string
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	headers := make([]*model_pb.Header, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		headers = append(headers, &model_pb.Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return &model_pb.KafkaMessage{
		Topic:     topic,
		Partition: int32(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
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
