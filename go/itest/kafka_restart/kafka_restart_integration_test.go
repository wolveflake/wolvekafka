package kafka_restart

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

func TestWolveKafkaServiceRejoinsAfterBrokerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	lambdaReqCh := make(chan *model_pb.LambdaRequest, 4)
	lambdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		_, _ = w.Write(respPayload)
	}))
	defer lambdaServer.Close()

	topic := fmt.Sprintf("wks-rejoin-topic-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("wks-rejoin-group-%d", time.Now().UnixNano())
	createTopic(t, fixture.BootstrapHost(), topic)

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
				ConsumerType: &configs_pb.Consumer_Parallel{
					Parallel: &configs_pb.ParallelConsumer{Parallelism: 1},
				},
			},
		},
	}

	cfg, err := configs.NewConfigFromProto(cfgProto)
	require.NoError(t, err)

	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, service.Close())
	})

	firstPartition, firstOffset := produceMessage(t, fixture.BootstrapHost(), topic, []byte("before restart"))
	requireLambdaPayload(t, lambdaReqCh, []byte("before restart"))
	waitCommittedOffset(t, ctx, fixture.BootstrapHost(), groupID, topic, firstPartition, firstOffset+1)

	fixture.Restart(ctx)

	secondPartition, secondOffset := produceMessage(t, fixture.BootstrapHost(), topic, []byte("after restart"))
	requireLambdaPayload(t, lambdaReqCh, []byte("after restart"))
	waitCommittedOffset(t, ctx, fixture.BootstrapHost(), groupID, topic, secondPartition, secondOffset+1)
}

func createTopic(t *testing.T, bootstrap, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	require.NoError(t, err)
	defer client.Close()

	adminClient := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = adminClient.CreateTopic(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
}

func produceMessage(t *testing.T, bootstrap, topic string, payload []byte) (int32, int64) {
	t.Helper()

	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results := producer.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: payload,
	})
	require.NoError(t, results.FirstErr())

	record := results[0].Record
	return record.Partition, record.Offset
}

func requireLambdaPayload(t *testing.T, lambdaReqCh <-chan *model_pb.LambdaRequest, payload []byte) {
	t.Helper()

	select {
	case req := <-lambdaReqCh:
		require.NotNil(t, req)
		require.Len(t, req.GetMessages(), 1)
		require.Equal(t, payload, req.GetMessages()[0].GetValue())
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for lambda payload %q", string(payload))
	}
}

func waitCommittedOffset(t *testing.T, ctx context.Context, bootstrap, groupID, topic string, partition int32, wantOffset int64) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	adminClient := kadm.NewClient(client)

	require.Eventually(t, func() bool {
		offsetCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		offsets, err := adminClient.FetchOffsets(offsetCtx, groupID)
		if err != nil {
			t.Logf("FetchOffsets(%s) failed: %v", groupID, err)
			return false
		}

		topicOffset, ok := offsets.Lookup(topic, partition)
		if !ok {
			return false
		}
		return topicOffset.At >= wantOffset
	}, 30*time.Second, 500*time.Millisecond)
}
