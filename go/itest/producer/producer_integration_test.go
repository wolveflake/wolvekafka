package producer

import (
	"context"
	"fmt"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"

	kafka_fixture "wolvekafka/go/itest/fixture"
	kafka_pkg "wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

func TestProducerPublishesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)

	cluster := &configs_pb.Cluster{
		BootstrapServers: []string{fixture.BootstrapHost()},
	}

	producer, err := kafka_pkg.NewProducer(ctx, kafka_pkg.ProducerParams{
		Name:    fmt.Sprintf("itest-producer-%d", time.Now().UnixNano()),
		Cluster: cluster,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, producer.Close())
	})

	topic := fmt.Sprintf("itest-producer-topic-%d", time.Now().UnixNano())
	payload := []byte("producer integration payload")

	require.NoError(t, producer.Produce(ctx, &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: payload,
	}))

	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": fixture.BootstrapHost(),
		"group.id":          fmt.Sprintf("itest-consumer-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = consumer.Close()
	})

	require.NoError(t, consumer.SubscribeTopics([]string{topic}, nil))

	deadline := time.Now().Add(30 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			t.Fatalf("timed out waiting for message on %s", topic)
		}

		msg, err := consumer.ReadMessage(timeout)
		if err != nil {
			if kafkaErr, ok := err.(ckafka.Error); ok && kafkaErr.Code() == ckafka.ErrTimedOut {
				continue
			}
			t.Fatalf("failed to read message: %v", err)
		}

		require.Equal(t, topic, *msg.TopicPartition.Topic)
		require.Equal(t, payload, msg.Value)
		break
	}
}
