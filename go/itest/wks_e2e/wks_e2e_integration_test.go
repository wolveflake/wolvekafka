// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks_e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"

	"wolvekafka/go/configs"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/wks"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

func TestWolveKafkaServiceEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Boot a real Kafka via Docker.
	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	// Capture requests hitting the lambda endpoint for validation.
	lambdaReqCh := make(chan *model_pb.LambdaRequest, 1)
	lambdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		req, err := wks.ParseLambdaRequestJson(body)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
			return
		}
		select {
		case lambdaReqCh <- req:
		default:
		}

		respPayload, err := protojson.Marshal(&model_pb.LambdaResponse{
			Messages: []*model_pb.ResponseMessage{
				{Status: model_pb.ResponseStatus_STATUS_SUCCESS},
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal response: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(respPayload); err != nil {
			t.Logf("failed to write response: %v", err)
		}
	}))
	defer lambdaServer.Close()

	topic := fmt.Sprintf("wks-e2e-topic-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("wks-e2e-group-%d", time.Now().UnixNano())
	createTestTopics(t, ctx, fixture.BootstrapHost(), topic)

	cfgProto := &configs_pb.Config{
		Cluster: &configs_pb.Cluster{
			BootstrapServers: []string{fixture.BootstrapHost()},
		},
		Consumers: []*configs_pb.Consumer{
			{
				GroupId: groupID,
				Topics:  []string{topic},
				Lambdas: []*configs_pb.Lambda{
					{Url: lambdaServer.URL},
				},
			},
		},
	}

	cfg, err := configs.NewConfigFromProto(cfgProto)
	require.NoError(t, err)

	// Spin up the full service with real consumers.
	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	require.NotNil(t, service)
	defer service.Close()

	// Produce a message the service should consume.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(fixture.BootstrapHost()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		producer.Close()
	})

	messageValue := []byte("wks end-to-end payload")

	produceCtx, produceCancel := context.WithTimeout(ctx, 10*time.Second)
	defer produceCancel()

	results := producer.ProduceSync(produceCtx, &kgo.Record{
		Topic: topic,
		Value: messageValue,
	})
	require.NoError(t, results.FirstErr())

	record := results[0]
	producedPartition := record.Record.Partition
	producedOffset := record.Record.Offset

	select {
	case req := <-lambdaReqCh:
		require.NotNil(t, req)
		require.Equal(t, 1, len(req.GetMessages()))
		require.Equal(t, topic, req.Messages[0].GetTopic())
		require.Equal(t, messageValue, req.Messages[0].GetValue())
	case <-time.After(45 * time.Second):
		t.Fatal("timed out waiting for lambda invocation")
	}

	// Verify the offset committed in Kafka advanced to include our message.
	commitVerifierClient, err := kgo.NewClient(
		kgo.SeedBrokers(fixture.BootstrapHost()),
		kgo.ConsumerGroup(groupID),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		commitVerifierClient.Close()
	})

	adminClient := kadm.NewClient(commitVerifierClient)

	require.Eventually(t, func() bool {
		offsetsCtx, offsetsCancel := context.WithTimeout(ctx, 10*time.Second)
		defer offsetsCancel()

		offsets, err := adminClient.FetchOffsets(offsetsCtx, groupID)
		if err != nil {
			t.Logf("FetchOffsets() error: %v", err)
			return false
		}

		topicOffsets, ok := offsets.Lookup(topic, producedPartition)
		if !ok {
			return false
		}

		return topicOffsets.At == producedOffset+1
	}, 15*time.Second, 500*time.Millisecond, "expected committed offset to advance")
}

func TestWolveKafkaServiceKafkaOutputFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Kafka fixture covers both input and output topics.
	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	inputTopic := fmt.Sprintf("wks-e2e-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("wks-e2e-output-%d", time.Now().UnixNano())
	groupInput := fmt.Sprintf("wks-e2e-input-group-%d", time.Now().UnixNano())
	groupOutput := fmt.Sprintf("wks-e2e-output-group-%d", time.Now().UnixNano())
	createTestTopics(t, ctx, fixture.BootstrapHost(), inputTopic, outputTopic)

	lambda1Payload := []byte(`{"from":"lambda1"}`)
	// First lambda returns JSON which the output policy should forward to the next topic.
	lambda1ReqCh := make(chan *model_pb.LambdaRequest, 1)
	lambda1Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		req, err := wks.ParseLambdaRequestJson(body)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
			return
		}
		select {
		case lambda1ReqCh <- req:
		default:
		}

		respBytes, err := protojson.Marshal(&model_pb.LambdaResponse{
			Messages: []*model_pb.ResponseMessage{
				{
					Status:   model_pb.ResponseStatus_STATUS_SUCCESS,
					Message:  lambda1Payload,
					Encoding: model_pb.MessageEncoding_ENCODING_JSON,
				},
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("marshal error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(respBytes); err != nil {
			t.Logf("lambda1 write error: %v", err)
		}
	}))
	defer lambda1Server.Close()

	// Second lambda observes the produced output topic.
	lambda2ReqCh := make(chan *model_pb.LambdaRequest, 1)
	lambda2Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		req, err := wks.ParseLambdaRequestJson(body)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
			return
		}
		select {
		case lambda2ReqCh <- req:
		default:
		}

		respBytes, err := protojson.Marshal(&model_pb.LambdaResponse{
			Messages: []*model_pb.ResponseMessage{
				{Status: model_pb.ResponseStatus_STATUS_SUCCESS},
			},
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("marshal error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(respBytes); err != nil {
			t.Logf("lambda2 write error: %v", err)
		}
	}))
	defer lambda2Server.Close()

	// Configure two consumers: first outputs to Kafka, second watches resulting topic.
	cfgProto := &configs_pb.Config{
		Cluster: &configs_pb.Cluster{
			BootstrapServers: []string{fixture.BootstrapHost()},
		},
		Consumers: []*configs_pb.Consumer{
			{
				GroupId: groupInput,
				Topics:  []string{inputTopic},
				Lambdas: []*configs_pb.Lambda{
					{Url: lambda1Server.URL},
				},
				OutputPolicy: &configs_pb.Consumer_Kafka{
					Kafka: &configs_pb.OutputKafkaPolicy{
						Topic: outputTopic,
					},
				},
			},
			{
				GroupId: groupOutput,
				Topics:  []string{outputTopic},
				Lambdas: []*configs_pb.Lambda{
					{Url: lambda2Server.URL},
				},
			},
		},
	}

	cfg, err := configs.NewConfigFromProto(cfgProto)
	require.NoError(t, err)

	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	require.NotNil(t, service)
	defer service.Close()

	// Inject a message into the first consumer's topic to drive the pipeline.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(fixture.BootstrapHost()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		producer.Close()
	})

	produceCtx, produceCancel := context.WithTimeout(ctx, 10*time.Second)
	defer produceCancel()

	results := producer.ProduceSync(produceCtx, &kgo.Record{
		Topic: inputTopic,
		Value: []byte("input-message"),
	})
	require.NoError(t, results.FirstErr())

	select {
	case <-lambda1ReqCh:
	case <-time.After(45 * time.Second):
		t.Fatal("lambda1 was not invoked")
	}

	select {
	case req := <-lambda2ReqCh:
		require.NotNil(t, req)
		require.Equal(t, outputTopic, req.Messages[0].GetTopic())
		require.Equal(t, lambda1Payload, req.Messages[0].GetValue())
	case <-time.After(60 * time.Second):
		t.Fatal("timeout waiting for lambda2 invocation")
	}
}

func createTestTopics(t *testing.T, ctx context.Context, broker string, topics ...string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()
	created, err := kadm.NewClient(client).CreateTopics(ctx, 1, 1, nil, topics...)
	require.NoError(t, err)
	require.NoError(t, created.Error())
}
