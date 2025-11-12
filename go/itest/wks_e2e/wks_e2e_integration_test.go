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

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

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

	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	require.NotNil(t, service)

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
