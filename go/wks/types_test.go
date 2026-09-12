// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

func TestSimpleConversion(t *testing.T) {
	req := &model_pb.LambdaRequest{
		Messages: []*model_pb.KafkaMessage{
			{
				Topic:     "test_topic",
				Partition: 0,
				Offset:    12345,
				Headers: []*model_pb.Header{
					{Key: "header1", Value: []byte("value1")},
					{Key: "header2", Value: []byte("value2")},
				},
				Key:   []byte("test_key"),
				Value: []byte("test_value"),
			},
		},
	}
	lambdaReqJson, err := ConvertLambdaRequestToJson(req)
	assert.NoError(t, err)
	assert.NotEmpty(t, lambdaReqJson)

	parsedReq, err := ParseLambdaRequestJson(lambdaReqJson)
	assert.NoError(t, err)
	assert.Equal(t, len(req.Messages), len(parsedReq.Messages))
	assert.Equal(t, req.Messages[0].Topic, parsedReq.Messages[0].Topic)
	assert.Equal(t, req.Messages[0].Partition, parsedReq.Messages[0].Partition)
	assert.Equal(t, req.Messages[0].Offset, parsedReq.Messages[0].Offset)
	assert.Equal(t, len(req.Messages[0].Headers), len(parsedReq.Messages[0].Headers))
	assert.Equal(t, string(req.Messages[0].Headers[0].Value), string(parsedReq.Messages[0].Headers[0].Value))
	assert.Equal(t, string(req.Messages[0].Key), string(parsedReq.Messages[0].Key))
	assert.Equal(t, string(req.Messages[0].Value), string(parsedReq.Messages[0].Value))
}

func TestKafkaMsgToProto(t *testing.T) {
	topic := "input-topic"
	headers := []kafka.Header{
		{Key: "header-1", Value: []byte("value-1")},
	}

	msg := &kafka.Message{
		Topic:     topic,
		Partition: 3,
		Offset:    42,
		Headers:   headers,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	protoMsg := kafkaMsgToProto(msg)

	require.NotNil(t, protoMsg)
	assert.Equal(t, topic, protoMsg.GetTopic())
	assert.Equal(t, int32(3), protoMsg.GetPartition())
	assert.Equal(t, int64(42), protoMsg.GetOffset())
	require.Len(t, protoMsg.GetHeaders(), len(headers))
	assert.Equal(t, headers[0].Key, protoMsg.GetHeaders()[0].GetKey())
	assert.Equal(t, headers[0].Value, protoMsg.GetHeaders()[0].GetValue())
	assert.Equal(t, msg.Key, protoMsg.GetKey())
	assert.Equal(t, msg.Value, protoMsg.GetValue())
	assert.Equal(t, model_pb.MessageEncoding_ENCODING_DEFAULT, protoMsg.GetEncoding())
}

func TestKafkaMsgsToLambdaRequest(t *testing.T) {
	topicA := "topic-a"
	topicB := "topic-b"
	msgs := []*kafka.Message{
		{
			Topic:     topicA,
			Partition: 0,
			Offset:    10,
			Key:       []byte("key-a"),
			Value:     []byte("value-a"),
		},
		{
			Topic:     topicB,
			Partition: 1,
			Offset:    20,
			Key:       []byte("key-b"),
			Value:     []byte("value-b"),
		},
	}

	lambdaReq := kafkaMsgsToLambdaRequest(msgs)

	require.Len(t, lambdaReq.Messages, len(msgs))
	for i, kafkaMsg := range msgs {
		protoMsg := lambdaReq.Messages[i]
		require.NotNil(t, protoMsg)
		assert.Equal(t, kafkaMsg.Topic, protoMsg.GetTopic())
		assert.Equal(t, kafkaMsg.Partition, protoMsg.GetPartition())
		assert.Equal(t, int64(kafkaMsg.Offset), protoMsg.GetOffset())
		assert.Equal(t, kafkaMsg.Key, protoMsg.GetKey())
		assert.Equal(t, kafkaMsg.Value, protoMsg.GetValue())
	}
}

func TestParseLambdaResponse(t *testing.T) {
	resp := &model_pb.LambdaResponse{
		Messages: []*model_pb.ResponseMessage{
			{
				Status:   model_pb.ResponseStatus_STATUS_SUCCESS,
				Message:  []byte("ok"),
				Encoding: model_pb.MessageEncoding_ENCODING_JSON,
			},
		},
	}

	payload, err := marshaler.Marshal(resp)
	require.NoError(t, err)

	parsed, err := ParseLambdaResponse(payload)
	require.NoError(t, err)

	require.Len(t, parsed.GetMessages(), 1)
	assert.Equal(t, resp.Messages[0].GetStatus(), parsed.Messages[0].GetStatus())
	assert.Equal(t, resp.Messages[0].GetEncoding(), parsed.Messages[0].GetEncoding())
	assert.Equal(t, resp.Messages[0].GetMessage(), parsed.Messages[0].GetMessage())
}

func TestResponseMessageToKafkaMessage(t *testing.T) {
	policy := &configs_pb.OutputKafkaPolicy{Topic: "output-topic"}
	respMsg := &model_pb.ResponseMessage{
		Message: []byte("payload"),
	}

	msg, err := ResponseMessageToKafkaMessage(policy, respMsg)
	require.NoError(t, err)

	require.NotNil(t, msg)
	assert.Equal(t, policy.Topic, msg.Topic)
	assert.Equal(t, kafka.PartitionAny, msg.Partition)
	assert.Equal(t, respMsg.GetMessage(), msg.Value)
}

func TestResponseMessageToKafkaMessageUsesDefaultJsonPayload(t *testing.T) {
	policy := &configs_pb.OutputKafkaPolicy{Topic: "output-topic"}
	respMsg := &model_pb.ResponseMessage{
		Encoding: model_pb.MessageEncoding_ENCODING_JSON,
	}

	msg, err := ResponseMessageToKafkaMessage(policy, respMsg)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, emptyJsonMessageValue, msg.Value)
}

func TestResponseMessageToKafkaMessageEmptyNonJsonError(t *testing.T) {
	policy := &configs_pb.OutputKafkaPolicy{Topic: "output-topic"}
	respMsg := &model_pb.ResponseMessage{}

	msg, err := ResponseMessageToKafkaMessage(policy, respMsg)
	require.Error(t, err)
	assert.Nil(t, msg)
}

func TestHeadersToKafkaHeaders(t *testing.T) {
	headers := []*model_pb.Header{
		{Key: "key-1", Value: []byte("value-1")},
		{Key: "key-2", Value: nil},
	}

	kHeaders := HeadersToKafkaHeaders(headers)

	require.Len(t, kHeaders, len(headers))
	for i, header := range headers {
		assert.Equal(t, header.GetKey(), kHeaders[i].Key)
		assert.Equal(t, header.GetValue(), kHeaders[i].Value)
	}

	assert.Len(t, HeadersToKafkaHeaders(nil), 0)
}
