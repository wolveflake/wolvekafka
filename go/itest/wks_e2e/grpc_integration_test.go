// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks_e2e

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"wolvekafka/go/configs"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/wks"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/wolvekafka/handler/v1"
)

// A gRPC handler gets the same delivery semantics as an HTTP one: throttling
// does not spend the retry budget, RETRY is redelivered, FAILED goes to the
// DLQ, and the input offset is committed only once every record is resolved.
func TestGRPCHandlerDeliverySemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	topic := fmt.Sprintf("grpc-e2e-%d", time.Now().UnixNano())
	dlq := topic + ".dlq"
	group := fmt.Sprintf("grpc-e2e-group-%d", time.Now().UnixNano())
	client, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()))
	require.NoError(t, err)
	defer client.Close()
	admin := kadm.NewClient(client)
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic, dlq)
	require.NoError(t, err)
	require.NoError(t, created.Error())

	var mu sync.Mutex
	calls := 0
	seen := map[string]int{}
	handle := func(_ context.Context, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			st, _ := status.New(codes.ResourceExhausted, "warming up").
				WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(100 * time.Millisecond)})
			return nil, st.Err()
		}
		resp := &model_pb.LambdaResponse{}
		for _, msg := range req.GetMessages() {
			value := string(msg.GetValue())
			seen[value]++
			result := model_pb.ResponseStatus_STATUS_SUCCESS
			switch {
			case value == "retry-once" && seen[value] == 1:
				result = model_pb.ResponseStatus_STATUS_RETRY
			case value == "reject":
				result = model_pb.ResponseStatus_STATUS_FAILED
			}
			resp.Messages = append(resp.Messages, &model_pb.ResponseMessage{Status: result})
		}
		return resp, nil
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "wolvekafka.handler.v1.HandlerService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Handle",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				req := &model_pb.LambdaRequest{}
				if err := dec(req); err != nil {
					return nil, err
				}
				return handle(ctx, req)
			},
		}},
	}, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster: &configs_pb.Cluster{BootstrapServers: []string{fixture.BootstrapHost()}},
		Consumers: []*configs_pb.Consumer{{
			GroupId: group,
			Topics:  []string{topic},
			Lambdas: []*configs_pb.Lambda{{Url: "grpc://" + lis.Addr().String()}},
			RetryPolicy: &configs_pb.RetryPolicy{
				RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{ExponentialBackoff: &configs_pb.ExponentialBackoff{
					InitialBackoffMs: 50, MaxBackoffMs: 200, Multiplier: 2,
				}},
				Action: &configs_pb.RetryPolicy_Dlq{Dlq: &configs_pb.DLQAction{Topic: dlq}},
			},
		}},
	})
	require.NoError(t, err)
	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	defer service.Close()

	values := []string{"ok", "retry-once", "reject"}
	for _, v := range values {
		require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(v)}).FirstErr())
	}

	require.Eventually(t, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		o, ok := offsets.Lookup(topic, 0)
		return ok && o.At == int64(len(values))
	}, 60*time.Second, 250*time.Millisecond, "every record resolved and committed")

	dlqReader, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()),
		kgo.ConsumeTopics(dlq), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer dlqReader.Close()
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 20*time.Second)
	defer fetchCancel()
	var dead []string
	for len(dead) == 0 && fetchCtx.Err() == nil {
		dlqReader.PollFetches(fetchCtx).EachRecord(func(r *kgo.Record) { dead = append(dead, string(r.Value)) })
	}
	require.Equal(t, []string{"reject"}, dead)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, seen["retry-once"], "RETRY is redelivered once, then succeeds")
	require.Equal(t, 1, seen["ok"], "a successful record is not resent")
}
