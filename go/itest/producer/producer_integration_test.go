package producer

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

	require.NoError(t, producer.Produce(ctx, &kgo.Record{
		Topic:     topic,
		Partition: -1, // any partition
		Value:     payload,
	}))

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(fixture.BootstrapHost()),
		kgo.ConsumerGroup(fmt.Sprintf("itest-consumer-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		consumer.Close()
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			t.Fatalf("timed out waiting for message on %s", topic)
		}

		fetchCtx, fetchCancel := context.WithTimeout(ctx, timeout)
		fetches := consumer.PollFetches(fetchCtx)
		fetchCancel()

		if fetches.IsClientClosed() {
			t.Fatal("client closed unexpectedly")
		}

		if len(fetches.Errors()) > 0 {
			t.Fatalf("fetch errors: %v", fetches.Errors())
		}

		iter := fetches.RecordIter()
		if !iter.Done() {
			msg := iter.Next()
			require.Equal(t, topic, msg.Topic)
			require.Equal(t, payload, msg.Value)
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}
