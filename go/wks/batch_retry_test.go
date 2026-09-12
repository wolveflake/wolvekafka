// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

func retryTestPolicy() *configs_pb.Consumer {
	return &configs_pb.Consumer{RetryPolicy: &configs_pb.RetryPolicy{
		RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{ExponentialBackoff: &configs_pb.ExponentialBackoff{InitialBackoffMs: 1, MaxBackoffMs: 1}},
		Action:      &configs_pb.RetryPolicy_Drop{Drop: &configs_pb.DropAction{}},
	}}
}

func TestBatchRetriesOnlyUnresolvedMessages(t *testing.T) {
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()})
	req := &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Offset: 1}, {Offset: 2}, {Offset: 3}}}
	calls := 0
	resp, err := r.ExecuteBatch(context.Background(), req, func(ctx context.Context, sub *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		if calls == 1 {
			require.Len(t, sub.Messages, 3)
			return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{
				{Status: model_pb.ResponseStatus_STATUS_SUCCESS, Message: []byte("kept")},
				{Status: model_pb.ResponseStatus_STATUS_RETRY},
				{Status: model_pb.ResponseStatus_STATUS_FAILED},
			}}, nil
		}
		require.Len(t, sub.Messages, 1)
		require.EqualValues(t, 2, sub.Messages[0].Offset)
		return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_SUCCESS}}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, []byte("kept"), resp.Response.Messages[0].Message)
	require.Len(t, resp.Response.Messages, 2)
	require.Equal(t, 1, resp.Dropped)
	require.EqualValues(t, 2, resp.Request.Messages[1].Offset)
}

func TestMalformedResponsesCannotAcknowledgeSuccess(t *testing.T) {
	for _, resp := range []*model_pb.LambdaResponse{nil, {}, {Messages: []*model_pb.ResponseMessage{nil}}, {Messages: []*model_pb.ResponseMessage{{Status: 99}}}} {
		require.Error(t, validateBatchResponse(&model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}}, resp))
	}
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: retryTestPolicy()})
	calls := 0
	resp, err := r.ExecuteBatch(context.Background(), &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}}, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		return &model_pb.LambdaResponse{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Empty(t, resp.Response.Messages)
	require.Equal(t, 1, resp.Dropped)
}

func TestFailedWithoutTerminalPolicyRemainsUncommitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: &configs_pb.Consumer{}})
	calls := 0
	_, err := r.ExecuteBatch(ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}}}, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_FAILED}}}, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}

func TestDLQFailureRetainsInputWithoutReinvokingHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := retryTestPolicy()
	cfg.RetryPolicy.Action = &configs_pb.RetryPolicy_Dlq{Dlq: &configs_pb.DLQAction{Topic: "dead"}}
	sends, calls := 0, 0
	r := NewRetryPolicy(RetryPolicyParams{ConsumerCfg: cfg, DLQProducer: &kafka.Producer{ProducerFunc: func(msg *kafka.Message) error {
		sends++
		require.Equal(t, "dead", msg.Topic)
		if sends == 1 {
			return fmt.Errorf("broker unavailable")
		}
		return nil
	}}})
	_, err := r.ExecuteBatch(ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{Value: []byte("failed")}}}, func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		calls++
		return &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_FAILED}}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, 2, sends)
}
