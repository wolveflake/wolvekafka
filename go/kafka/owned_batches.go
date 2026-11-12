// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// One fetch owns one rebalance hold. Never poll again with a partial batch:
// PollRecords can wait on a rebalance condition that does not observe its ctx.
func (c *Consumer) pollOwnedBatches() chan []*Message {
	out := make(chan []*Message)
	go func() {
		defer close(out)
		defer close(c.pollDone)
		defer c.franzClient.AllowRebalance()
		for c.ctx.Err() == nil {
			ctx, cancel := context.WithCancel(c.ctx)
			c.stateMu.Lock()
			c.batchCtx, c.batchCancel = ctx, cancel
			c.stateMu.Unlock()
			pollCtx, pollCancel := context.WithTimeout(ctx, c.batchMaxWait)
			fetches := c.franzClient.PollRecords(pollCtx, c.batchMaxMessages)
			pollCancel()
			logFetchErrors(c.log, fetches)
			records := fetches.Records()
			if c.lag != nil {
				c.lag.fetched(records)
			}
			aborted := false
			for start := 0; start < len(records) && !aborted; {
				end, size := start, 0
				for end < len(records) && (end == start || size+approximateMessageSize(records[end]) <= c.batchMaxBytes) {
					size += approximateMessageSize(records[end])
					end++
				}
				select {
				case out <- records[start:end]:
					// Wait for the worker even on rebalance cancellation; it must stop
					// producing/committing before we rewind or release ownership.
					select {
					case complete := <-c.batchDone:
						aborted = !complete
					case <-c.ctx.Done():
						aborted = true
					}
				case <-ctx.Done():
					aborted = true
				}
				start = end
			}
			if aborted && c.ctx.Err() == nil {
				// Cooperative rebalances can retain partitions, so releasing the
				// hold alone is insufficient to replay uncommitted fetched records.
				c.franzClient.SetOffsets(replayOffsets(records))
				if c.lag != nil {
					c.lag.rewound(records)
				}
			}
			cancel()
			c.franzClient.AllowRebalance()
			if fetches.IsClientClosed() {
				return
			}
			if len(records) == 0 {
				timer := time.NewTimer(c.pollBackoff())
				select {
				case <-timer.C:
				case <-c.ctx.Done():
				}
				timer.Stop()
			}
		}
	}()
	return out
}

func replayOffsets(records []*Message) map[string]map[int32]kgo.EpochOffset {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for _, record := range records {
		if offsets[record.Topic] == nil {
			offsets[record.Topic] = make(map[int32]kgo.EpochOffset)
		}
		old, ok := offsets[record.Topic][record.Partition]
		if !ok || record.Offset < old.Offset {
			offsets[record.Topic][record.Partition] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
		}
	}
	return offsets
}

func logFetchErrors(log *zap.SugaredLogger, fetches kgo.Fetches) {
	for _, e := range fetches.Errors() {
		if !errors.Is(e.Err, context.Canceled) && !errors.Is(e.Err, context.DeadlineExceeded) {
			log.Errorf("Kafka fetch failed for %s[%d]: %v", e.Topic, e.Partition, e.Err)
		}
	}
}
