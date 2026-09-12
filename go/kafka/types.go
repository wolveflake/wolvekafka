// Copyright 2023-2026 wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// Type aliases for Kafka types.

type Message = kgo.Record
type Header = kgo.RecordHeader

// TopicPartition represents a topic and partition.
type TopicPartition struct {
	Topic     *string
	Partition int32
	Offset    int64
	Error     error
}

// Event is used for compatibility with the previous Kafka client interface.
type Event interface{}

const PartitionAny int32 = -1
