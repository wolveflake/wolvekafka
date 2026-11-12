// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"sort"
	"sync"
	"time"
)

// PartitionLag reports how far one partition owned by a consumer is behind
// the newest record in Kafka.
type PartitionLag struct {
	Topic     string
	Partition int32

	// Lag is the number of records not yet committed, or -1 when the newest
	// offset has not been read from Kafka yet.
	Lag int64

	// LagSeconds is the age of the oldest record not yet committed, or -1 when
	// it is unknown. It is zero when the partition is caught up.
	LagSeconds float64
}

type topicPartition struct {
	topic     string
	partition int32
}

type pendingRecord struct {
	offset    int64
	timestamp time.Time
}

// partitionState offsets are "next offset" positions, matching Kafka's
// convention: end is the high watermark and committed is the offset the group
// resumes from. -1 means unknown.
type partitionState struct {
	end       int64
	start     int64
	committed int64

	// Records fetched but not yet committed, in offset order. The first entry
	// is the oldest record still waiting, which gives the time lag.
	pending []pendingRecord
}

// lagTracker follows the partitions one consumer owns. Assignment callbacks,
// the poll loop, commits, the offset refresher and metric collection all call
// into it from different goroutines.
type lagTracker struct {
	mu    sync.Mutex
	parts map[topicPartition]*partitionState
}

func newLagTracker() *lagTracker {
	return &lagTracker{parts: make(map[topicPartition]*partitionState)}
}

func newPartitionState() *partitionState {
	return &partitionState{end: -1, start: -1, committed: -1}
}

// stateLocked returns the partition's state, creating it if needed.
func (t *lagTracker) stateLocked(tp topicPartition) *partitionState {
	state, ok := t.parts[tp]
	if !ok {
		state = newPartitionState()
		t.parts[tp] = state
	}
	return state
}

func (t *lagTracker) assign(assigned map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			t.stateLocked(topicPartition{topic, partition})
		}
	}
}

// revoke forgets partitions this consumer no longer owns, so another owner
// reports them instead.
func (t *lagTracker) revoke(revoked map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, partitions := range revoked {
		for _, partition := range partitions {
			delete(t.parts, topicPartition{topic, partition})
		}
	}
}

func (t *lagTracker) fetched(records []*Message) {
	if len(records) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, record := range records {
		state := t.stateLocked(topicPartition{record.Topic, record.Partition})
		if n := len(state.pending); n > 0 && record.Offset <= state.pending[n-1].offset {
			continue
		}
		state.pending = append(state.pending, pendingRecord{offset: record.Offset, timestamp: record.Timestamp})
	}
}

// committed records a successful commit of msg, which resolves every fetched
// record of that partition up to and including msg.
func (t *lagTracker) committed(msg *Message) {
	if msg == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.parts[topicPartition{msg.Topic, msg.Partition}]
	if !ok {
		return
	}
	if next := msg.Offset + 1; next > state.committed {
		state.committed = next
	}
	drop := 0
	for drop < len(state.pending) && state.pending[drop].offset <= msg.Offset {
		drop++
	}
	state.pending = state.pending[drop:]
}

// rewound clears fetched records that will be fetched again after a replay.
func (t *lagTracker) rewound(records []*Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, record := range records {
		if state, ok := t.parts[topicPartition{record.Topic, record.Partition}]; ok {
			state.pending = nil
		}
	}
}

// wanted returns the topics to request offsets for, and whether any partition
// still lacks a committed offset, in which case the log start offset is needed
// to know where consumption begins.
func (t *lagTracker) wanted() (topics []string, needStart bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	seen := make(map[string]bool)
	for tp, state := range t.parts {
		if !seen[tp.topic] {
			seen[tp.topic] = true
			topics = append(topics, tp.topic)
		}
		if state.committed < 0 {
			needStart = true
		}
	}
	sort.Strings(topics)
	return topics, needStart
}

// updateOffsets applies offsets read from Kafka. Only partitions this
// consumer still owns are updated. A committed offset from Kafka only fills
// in a partition that has none yet: commits made by this consumer are newer.
func (t *lagTracker) updateOffsets(end, start, committed map[topicPartition]int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for tp, state := range t.parts {
		if offset, ok := end[tp]; ok && offset >= 0 {
			state.end = offset
		}
		if offset, ok := start[tp]; ok && offset >= 0 {
			state.start = offset
		}
		if offset, ok := committed[tp]; ok && offset >= 0 && state.committed < 0 {
			state.committed = offset
		}
	}
}

func (t *lagTracker) snapshot(now time.Time) []PartitionLag {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PartitionLag, 0, len(t.parts))
	for tp, state := range t.parts {
		out = append(out, state.lag(tp, now))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func (s *partitionState) lag(tp topicPartition, now time.Time) PartitionLag {
	result := PartitionLag{Topic: tp.topic, Partition: tp.partition, Lag: -1, LagSeconds: -1}
	if s.end < 0 {
		return result
	}
	// Without a committed offset the group starts at the beginning of the log.
	// Records before the log start were deleted by retention and can no longer
	// be consumed, so they do not count as lag.
	position := s.committed
	if s.start >= 0 && position < s.start {
		position = s.start
	}
	if position < 0 {
		return result
	}
	result.Lag = s.end - position
	if result.Lag <= 0 {
		result.Lag = 0
		result.LagSeconds = 0
		return result
	}
	if len(s.pending) > 0 && !s.pending[0].timestamp.IsZero() {
		age := now.Sub(s.pending[0].timestamp).Seconds()
		if age < 0 {
			age = 0
		}
		result.LagSeconds = age
	}
	return result
}
