package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/stretchr/testify/require"

	kafka_fixture "wolvekafka/go/itest/fixture"
	kafka_pkg "wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

func TestConsumerReceivesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	bootstrap := fixture.BootstrapHost()
	cluster := &configs_pb.Cluster{
		BootstrapServers: []string{bootstrap},
	}

	topic := fmt.Sprintf("itest-consumer-topic-%d", time.Now().UnixNano())
	payload := []byte("consumer integration payload")

	consumer, err := kafka_pkg.NewConsumer(ctx, kafka_pkg.ConsumerParams{
		Name:         fmt.Sprintf("itest-consumer-%d", time.Now().UnixNano()),
		Cluster:      cluster,
		Topics:       []string{topic},
		PoolDuration: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, consumer.Close())
	})

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	produceMessage(t, bootstrap, topic, payload)

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		t.Fatalf("context cancelled before receiving message: %v", ctx.Err())
	case <-timeout.C:
		t.Fatalf("timed out waiting for message on %s", topic)
	case msgs, ok := <-msgChan:
		if !ok {
			t.Fatalf("message channel closed before receiving message")
		}

		msg := msgs[0]
		require.Equal(t, topic, msg.Topic)
		require.Equal(t, payload, msg.Value)

		_, err := consumer.CommitMessage(msg)
		require.NoError(t, err)
		return
	}
}

func produceMessage(t *testing.T, bootstrap, topic string, payload []byte) {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
	)
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := producer.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: payload,
	})

	require.NoError(t, result.FirstErr())
}
