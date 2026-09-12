package wks_e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

// A partial fetch must reach the worker without a second poll. Rebalancing
// cancels its retry context and replays it even if this sole member retains
// the partition in a cooperative rebalance. Close must also join a live poll.
func TestPartialFetchRebalancesReplaysAndCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)
	client, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()))
	require.NoError(t, err)
	defer client.Close()
	topic := fmt.Sprintf("rebalance-%d", time.Now().UnixNano())
	created, err := kadm.NewClient(client).CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, created.Error())
	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("replay")}).FirstErr())
	consumer, err := kafka.NewConsumer(ctx, kafka.ConsumerParams{Name: topic, Cluster: &configs_pb.Cluster{BootstrapServers: []string{fixture.BootstrapHost()}}, Topics: []string{topic}, RequireBatchAck: true, BatchMaxMessages: 10, BatchMaxWait: 5 * time.Second})
	require.NoError(t, err)
	defer consumer.Close()
	batches, err := consumer.Poll()
	require.NoError(t, err)
	receive := func() []*kafka.Message {
		select {
		case batch := <-batches:
			require.Len(t, batch, 1)
			require.EqualValues(t, 0, batch[0].Offset)
			return batch
		case <-time.After(10 * time.Second):
			t.Fatal("partial fetch was not delivered or replayed")
			return nil
		}
	}
	for i := 0; i < 3; i++ {
		receive()
		batchCtx := consumer.BatchContext()
		consumer.RequestRejoin()
		select {
		case <-batchCtx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("rebalance did not cancel batch processing")
		}
		consumer.AbortBatch()
	}
	batch := receive()
	_, err = consumer.CommitMessage(batch[0])
	require.NoError(t, err)
	consumer.CompleteBatch()
	consumer.RequestRejoin()
	done := make(chan error, 1)
	go func() { done <- consumer.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close raced polling and did not finish")
	}
}
