package wks_e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
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

	// Produce a message the service should consume.
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers": fixture.BootstrapHost(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		producer.Close()
	})

	messageValue := []byte("wks end-to-end payload")
	deliveryChan := make(chan ckafka.Event, 1)
	require.NoError(t, producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: messageValue,
	}, deliveryChan))

	var producedPartition int32
	var producedOffset ckafka.Offset
	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*ckafka.Message)
		require.True(t, ok, "unexpected delivery event type %T", ev)
		require.NoError(t, msg.TopicPartition.Error)
		producedPartition = msg.TopicPartition.Partition
		producedOffset = msg.TopicPartition.Offset
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery report")
	}

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
	commitVerifier, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": fixture.BootstrapHost(),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = commitVerifier.Close()
	})

	require.Eventually(t, func() bool {
		partitions, err := commitVerifier.Committed([]ckafka.TopicPartition{{
			Topic:     &topic,
			Partition: producedPartition,
		}}, 10_000)
		if err != nil {
			t.Logf("Committed() error: %v", err)
			return false
		}
		if len(partitions) == 0 {
			return false
		}
		return partitions[0].Offset == producedOffset+1
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

	// Inject a message into the first consumer's topic to drive the pipeline.
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers": fixture.BootstrapHost(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		producer.Close()
	})

	deliveryChan := make(chan ckafka.Event, 1)
	require.NoError(t, producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &inputTopic,
			Partition: ckafka.PartitionAny,
		},
		Value: []byte("input-message"),
	}, deliveryChan))

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*ckafka.Message)
		require.True(t, ok, "unexpected delivery event type %T", ev)
		require.NoError(t, msg.TopicPartition.Error)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery report")
	}

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
