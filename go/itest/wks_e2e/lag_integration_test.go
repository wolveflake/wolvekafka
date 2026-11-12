// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks_e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	kafka_fixture "wolvekafka/go/itest/fixture"
	kafka_pkg "wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

// The consumer must report the backlog of a partition it owns, keep reporting
// it while a batch is held, and report zero once everything is committed.
func TestConsumerReportsLagFromKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)
	bootstrap := fixture.BootstrapHost()

	topic := fmt.Sprintf("itest-lag-topic-%d", time.Now().UnixNano())
	const records = 5
	produceRecords(t, bootstrap, topic, records, time.Now().Add(-time.Minute))

	consumer, err := kafka_pkg.NewConsumer(ctx, kafka_pkg.ConsumerParams{
		Name:               fmt.Sprintf("itest-lag-%d", time.Now().UnixNano()),
		Cluster:            &configs_pb.Cluster{BootstrapServers: []string{bootstrap}},
		Topics:             []string{topic},
		PollDuration:       100 * time.Millisecond,
		BatchMaxMessages:   1,
		RequireBatchAck:    true,
		LagRefreshInterval: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	msgChan, err := consumer.Poll()
	require.NoError(t, err)

	var first []*kgo.Record
	select {
	case first = <-msgChan:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the first batch")
	}
	require.Len(t, first, 1)

	// While the first batch is held, nothing is committed: the whole backlog
	// counts, and the oldest waiting record is at least a minute old.
	waitForLag(t, consumer, func(p kafka_pkg.PartitionLag) bool {
		return p.Lag == records && p.LagSeconds >= 60
	})

	_, err = consumer.CommitMessage(first[0])
	require.NoError(t, err)
	consumer.CompleteBatch()
	for i := 1; i < records; i++ {
		select {
		case batch := <-msgChan:
			_, err := consumer.CommitMessage(batch[len(batch)-1])
			require.NoError(t, err)
			consumer.CompleteBatch()
		case <-ctx.Done():
			t.Fatalf("timed out waiting for batch %d", i)
		}
	}

	waitForLag(t, consumer, func(p kafka_pkg.PartitionLag) bool {
		return p.Lag == 0 && p.LagSeconds == 0
	})
}

func waitForLag(t *testing.T, consumer *kafka_pkg.Consumer, ok func(kafka_pkg.PartitionLag) bool) {
	t.Helper()
	var last []kafka_pkg.PartitionLag
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		last = consumer.Lag(time.Now())
		if len(last) == 1 && ok(last[0]) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("lag never reached the expected state; last report: %+v", last)
}

func produceRecords(t *testing.T, bootstrap, topic string, count int, timestamp time.Time) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < count; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Value:     []byte(fmt.Sprintf("lag-%d", i)),
			Timestamp: timestamp,
		})
		require.NoError(t, result.FirstErr())
	}
}
