package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestFetchErrorsLoggedAlongsideRecords(t *testing.T) {
	core, observed := observer.New(zap.ErrorLevel)
	record := &kgo.Record{Topic: "healthy", Value: []byte("keep")}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{
		{Topic: "healthy", Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{record}}}},
		{Topic: "denied", Partitions: []kgo.FetchPartition{{Err: kerr.TopicAuthorizationFailed}}},
		{Topic: "timeout", Partitions: []kgo.FetchPartition{{Err: context.DeadlineExceeded}}},
	}}}
	logFetchErrors(zap.New(core).Sugar(), fetches)
	require.Equal(t, []*kgo.Record{record}, fetches.Records())
	require.Equal(t, 1, observed.Len())
	require.Contains(t, observed.All()[0].Message, "denied")
}

func TestReplayOffsetsRewindsEachPartitionToEarliestFetchedRecord(t *testing.T) {
	offsets := replayOffsets([]*Message{{Topic: "a", Partition: 0, Offset: 8, LeaderEpoch: 2}, {Topic: "a", Partition: 0, Offset: 5, LeaderEpoch: 1}, {Topic: "a", Partition: 1, Offset: 20, LeaderEpoch: 3}})
	require.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 5}, offsets["a"][0])
	require.Equal(t, kgo.EpochOffset{Epoch: 3, Offset: 20}, offsets["a"][1])
}
