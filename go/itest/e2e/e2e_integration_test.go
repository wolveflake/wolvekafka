package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kafka_fixture "wolvekafka/go/itest/fixture"
	kafka "wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

func TestProducerAndConsumerEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	cluster := &configs_pb.Cluster{
		BootstrapServers: []string{fixture.BootstrapHost()},
	}

	topic := fmt.Sprintf("e2e-topic-%d", time.Now().UnixNano())
	payload := []byte("end-to-end kafka payload")

	consumer, err := kafka.NewConsumer(ctx, kafka.ConsumerParams{
		Name:         fmt.Sprintf("e2e-consumer-%d", time.Now().UnixNano()),
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

	producer, err := kafka.NewProducer(ctx, kafka.ProducerParams{
		Name:    fmt.Sprintf("e2e-producer-%d", time.Now().UnixNano()),
		Cluster: cluster,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, producer.Close())
	})

	err = producer.Produce(ctx, &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	})
	require.NoError(t, err)

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
		require.Equal(t, topic, *msg.TopicPartition.Topic)
		require.Equal(t, payload, msg.Value)

		_, err := consumer.CommitMessage(msg)
		require.NoError(t, err)
		return
	}
}
