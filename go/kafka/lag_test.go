// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var lagNow = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func lagRecord(topic string, partition int32, offset int64, age time.Duration) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Offset: offset, Timestamp: lagNow.Add(-age)}
}

func onlyLag(t *testing.T, tracker *lagTracker) PartitionLag {
	t.Helper()
	snapshot := tracker.snapshot(lagNow)
	require.Len(t, snapshot, 1)
	return snapshot[0]
}

func TestLagUnknownUntilEndOffsetIsRead(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})

	assert.Equal(t, PartitionLag{Topic: "orders", Partition: 0, Lag: -1, LagSeconds: -1}, onlyLag(t, tracker))
}

func TestLagCountsRecordsAfterCommitAndAgesOldestPendingRecord(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 110}, nil, map[topicPartition]int64{{"orders", 0}: 100})
	tracker.fetched([]*kgo.Record{
		lagRecord("orders", 0, 100, 90*time.Second),
		lagRecord("orders", 0, 101, 60*time.Second),
	})

	got := onlyLag(t, tracker)
	assert.Equal(t, int64(10), got.Lag)
	assert.InDelta(t, 90, got.LagSeconds, 0.001)

	// Committing the first record moves the oldest waiting record forward.
	tracker.committed(lagRecord("orders", 0, 100, 0))
	got = onlyLag(t, tracker)
	assert.Equal(t, int64(9), got.Lag)
	assert.InDelta(t, 60, got.LagSeconds, 0.001)
}

func TestLagIsZeroWhenCaughtUp(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 5}, nil, nil)
	tracker.fetched([]*kgo.Record{lagRecord("orders", 0, 4, time.Hour)})
	tracker.committed(lagRecord("orders", 0, 4, 0))

	assert.Equal(t, PartitionLag{Topic: "orders", Partition: 0, Lag: 0, LagSeconds: 0}, onlyLag(t, tracker))
}

func TestLagWithoutCommitStartsAtLogStart(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	_, needStart := tracker.wanted()
	require.True(t, needStart, "a partition without a commit needs the log start offset")

	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 50}, map[topicPartition]int64{{"orders", 0}: 20}, nil)
	assert.Equal(t, int64(30), onlyLag(t, tracker).Lag)
}

func TestLagIgnoresRecordsDeletedByRetention(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.updateOffsets(
		map[topicPartition]int64{{"orders", 0}: 500},
		map[topicPartition]int64{{"orders", 0}: 400},
		map[topicPartition]int64{{"orders", 0}: 100},
	)

	assert.Equal(t, int64(100), onlyLag(t, tracker).Lag)
}

func TestLagKeepsOwnCommitOverOlderKafkaCommit(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 100}, nil, map[topicPartition]int64{{"orders", 0}: 10})
	tracker.committed(lagRecord("orders", 0, 89, 0))

	// A later refresh may still carry the commit read when the group joined.
	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 100}, nil, map[topicPartition]int64{{"orders", 0}: 10})
	assert.Equal(t, int64(10), onlyLag(t, tracker).Lag)
}

func TestLagTimeUnknownAfterRewindUntilRefetch(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.updateOffsets(map[topicPartition]int64{{"orders", 0}: 3}, nil, map[topicPartition]int64{{"orders", 0}: 0})
	records := []*kgo.Record{lagRecord("orders", 0, 0, time.Minute), lagRecord("orders", 0, 1, time.Minute)}
	tracker.fetched(records)
	tracker.rewound(records)

	got := onlyLag(t, tracker)
	assert.Equal(t, int64(3), got.Lag)
	assert.Equal(t, float64(-1), got.LagSeconds)

	tracker.fetched(records)
	assert.InDelta(t, 60, onlyLag(t, tracker).LagSeconds, 0.001)
}

func TestLagFetchedSkipsAlreadyPendingOffsets(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0}})
	tracker.fetched([]*kgo.Record{lagRecord("orders", 0, 5, 0), lagRecord("orders", 0, 6, 0)})
	tracker.fetched([]*kgo.Record{lagRecord("orders", 0, 6, 0), lagRecord("orders", 0, 7, 0)})

	state := tracker.parts[topicPartition{"orders", 0}]
	require.Len(t, state.pending, 3)
	assert.Equal(t, []int64{5, 6, 7}, []int64{state.pending[0].offset, state.pending[1].offset, state.pending[2].offset})
}

func TestLagRevokeForgetsPartitions(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"orders": {0, 1}, "payments": {3}})
	tracker.revoke(map[string][]int32{"orders": {1}})

	snapshot := tracker.snapshot(lagNow)
	require.Len(t, snapshot, 2)
	assert.Equal(t, "orders", snapshot[0].Topic)
	assert.Equal(t, int32(0), snapshot[0].Partition)
	assert.Equal(t, "payments", snapshot[1].Topic)

	// A commit for a revoked partition must not bring it back.
	tracker.committed(lagRecord("orders", 1, 9, 0))
	assert.Len(t, tracker.snapshot(lagNow), 2)
}

func TestLagWantedListsTopicsOnce(t *testing.T) {
	tracker := newLagTracker()
	tracker.assign(map[string][]int32{"payments": {0, 1}, "orders": {2}})
	tracker.updateOffsets(nil, nil, map[topicPartition]int64{
		{"payments", 0}: 1, {"payments", 1}: 1, {"orders", 2}: 1,
	})

	topics, needStart := tracker.wanted()
	assert.Equal(t, []string{"orders", "payments"}, topics)
	assert.False(t, needStart)
}

func TestConsumerLagDisabledByDefault(t *testing.T) {
	consumer := newConsumer(t.Context(), ConsumerParams{Name: "no-lag"})
	assert.Nil(t, consumer.Lag(lagNow))
}
