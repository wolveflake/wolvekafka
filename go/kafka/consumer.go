// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"wolvekafka/go/common/logger"
	configs_pb "wolvekafka/proto/configs"
)

const (
	// defaultPollDurationMs is the default duration (in milliseconds) for polling Kafka messages.
	defaultPollDurationMs = 100
	// defaultBatchMaxMessages flushes after each message unless overridden.
	defaultBatchMaxMessages = 1
	// defaultBatchMaxBytes is an ample cap to prevent unbounded batch growth.
	defaultBatchMaxBytes = 1 << 20 // 1 MiB
	// defaultBatchMaxWait bounds how long a partial batch can wait before being emitted.
	defaultBatchMaxWait = 500 * time.Millisecond
	// maxLagRequestTimeout bounds one offset refresh so a slow broker cannot
	// pile up requests.
	maxLagRequestTimeout = 10 * time.Second
)

var errKafkaClientClosed = errors.New("kafka client closed")

// KafkaConsumerClient is the minimal surface area required from a Kafka consumer implementation.
type KafkaConsumerClient interface {
	Poll(int) Event
	Close() error
	CommitMessage(*Message) ([]TopicPartition, error)
}

type Consumer struct {
	// consumer name.
	name string

	// subscribed topics.
	topics []string

	ctx         context.Context
	cancel      context.CancelFunc
	batchDone   chan bool
	stateMu     sync.Mutex
	batchCtx    context.Context
	batchCancel context.CancelFunc
	pollStarted bool
	closed      bool
	pollDone    chan struct{}
	closeOnce   sync.Once
	closeErr    error

	// polling duration in milliseconds.
	pollDurationMs int

	// Kafka consumer instance.
	kafkaConsumer KafkaConsumerClient

	// franz-go client (when not using test stub).
	franzClient *kgo.Client

	// logger.
	log *zap.SugaredLogger

	// batch configuration.
	batchMaxMessages int
	batchMaxBytes    int
	batchMaxWait     time.Duration

	// lag follows owned partitions for lag metrics; nil when disabled.
	lag                *lagTracker
	lagRefreshInterval time.Duration
	lagDone            chan struct{}
}

type ConsumerParams struct {
	// consumer name (for logging and metrics).
	Name string

	// Kafka consumer configuration.
	Cluster *configs_pb.Cluster

	// polling duration.
	PollDuration time.Duration

	// topics to subscribe to.
	Topics []string

	// batch configuration for Poll().
	BatchMaxMessages int
	BatchMaxBytes    int
	BatchMaxWait     time.Duration

	// Client allows injecting a Kafka consumer implementation (for testing).
	Client KafkaConsumerClient
	// RequireBatchAck holds partition ownership until CompleteBatch is called.
	RequireBatchAck bool
	// LagRefreshInterval is how often the newest offsets of owned partitions
	// are read from Kafka for lag reporting. Zero disables lag tracking.
	LagRefreshInterval time.Duration
}

// NewConsumer creates a new Kafka consumer and subscribes it to the specified topics.
func NewConsumer(ctx context.Context, params ConsumerParams) (*Consumer, error) {
	consumer := newConsumer(ctx, params)

	if params.Client != nil {
		consumer.kafkaConsumer = params.Client
		return consumer, nil
	}

	if params.Cluster == nil {
		consumer.cancel()
		return nil, fmt.Errorf("cluster configuration is required")
	}

	opts, err := ConnectionOptions(params.Cluster)
	if err != nil {
		consumer.cancel()
		return nil, err
	}
	opts = append(opts,
		kgo.ConsumerGroup(params.Name),
		kgo.ConsumeTopics(params.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if consumer.lag != nil {
		opts = append(opts,
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
				consumer.lag.assign(assigned)
			}),
			kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
				consumer.lag.revoke(revoked)
			}),
			kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
				consumer.lag.revoke(lost)
			}),
		)
	}
	if params.RequireBatchAck {
		opts = append(opts, kgo.BlockRebalanceOnPoll(), kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
			consumer.stateMu.Lock()
			defer consumer.stateMu.Unlock()
			if consumer.batchCancel != nil {
				consumer.batchCancel()
			}
		}))
	}

	franzClient, err := kgo.NewClient(opts...)
	if err != nil {
		consumer.cancel()
		consumer.log.Errorf("Failed to create %v consumer: %v", params.Name, err)
		return nil, err
	}

	consumer.franzClient = franzClient
	consumer.kafkaConsumer = &franzConsumerAdapter{client: franzClient, ctx: consumer.ctx}
	if consumer.lag != nil {
		consumer.lagDone = make(chan struct{})
		go consumer.refreshLagLoop()
	}

	return consumer, nil
}

// newConsumer creates a new Consumer instance without initializing the Kafka consumer.
func newConsumer(ctx context.Context, params ConsumerParams) *Consumer {
	pollDurationMs := params.PollDuration.Milliseconds()
	if pollDurationMs <= 0 {
		pollDurationMs = defaultPollDurationMs
	}

	batchMaxMessages := params.BatchMaxMessages
	if batchMaxMessages <= 0 {
		batchMaxMessages = defaultBatchMaxMessages
	}

	batchMaxBytes := params.BatchMaxBytes
	if batchMaxBytes <= 0 {
		batchMaxBytes = defaultBatchMaxBytes
	}

	batchMaxWait := params.BatchMaxWait
	if batchMaxWait <= 0 {
		batchMaxWait = defaultBatchMaxWait
	}

	log := logger.S()
	ctx, cancel := context.WithCancel(ctx)
	var batchDone chan bool
	if params.RequireBatchAck {
		batchDone = make(chan bool)
	}

	consumer := &Consumer{
		ctx:              ctx,
		cancel:           cancel,
		batchDone:        batchDone,
		pollDone:         make(chan struct{}),
		name:             params.Name,
		topics:           append([]string(nil), params.Topics...),
		pollDurationMs:   int(pollDurationMs),
		log:              log,
		batchMaxMessages: batchMaxMessages,
		batchMaxBytes:    batchMaxBytes,
		batchMaxWait:     batchMaxWait,
	}
	if params.LagRefreshInterval > 0 {
		consumer.lag = newLagTracker()
		consumer.lagRefreshInterval = params.LagRefreshInterval
	}
	return consumer
}

// Poll polls messages from Kafka and delivers them in batches bounded by the configured limits.
func (c *Consumer) Poll() (chan []*Message, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed || c.pollStarted {
		return nil, fmt.Errorf("consumer closed or already polling")
	}
	if c.kafkaConsumer == nil {
		return nil, fmt.Errorf("kafka consumer is not initialized")
	}
	c.pollStarted = true
	if c.franzClient != nil && c.batchDone != nil {
		return c.pollOwnedBatches(), nil
	}

	msgChan := make(chan []*Message, 1)

	go func() {
		defer close(c.pollDone)
		defer close(msgChan)

		batch := make([]*Message, 0, c.batchMaxMessages)
		batchBytes := 0
		var flushDeadline time.Time
		pollDegraded := false
		recoverablePollErrors := 0

		flush := func() {
			if len(batch) == 0 {
				return
			}

			out := make([]*Message, len(batch))
			copy(out, batch)

			select {
			case <-c.ctx.Done():
				// drop if context is cancelled.
			case msgChan <- out:
				if c.batchDone != nil {
					select {
					case <-c.ctx.Done():
					case <-c.batchDone:
					}
					if c.franzClient != nil {
						c.franzClient.AllowRebalance()
					}
				}
			}

			batch = batch[:0]
			batchBytes = 0
			flushDeadline = time.Time{}
		}

		for {
			if err := c.ctx.Err(); err != nil {
				flush()
				return
			}

			event := c.kafkaConsumer.Poll(c.pollDurationMs)
			if event != nil {
				switch e := event.(type) {
				case *Message:
					if pollDegraded {
						c.log.Infof(
							"Kafka poll recovered for consumer %q topics=%v after %d recoverable poll errors; delivering records again",
							c.name,
							c.topics,
							recoverablePollErrors,
						)
						pollDegraded = false
						recoverablePollErrors = 0
					}

					batch = append(batch, e)
					batchBytes += approximateMessageSize(e)
					if c.lag != nil {
						c.lag.fetched([]*Message{e})
					}

					if len(batch) == 1 && c.batchMaxWait > 0 {
						flushDeadline = time.Now().Add(c.batchMaxWait)
					}

					if (c.batchMaxMessages > 0 && len(batch) >= c.batchMaxMessages) ||
						(c.batchMaxBytes > 0 && batchBytes >= c.batchMaxBytes) {
						flush()
					}
				case error:
					flush()
					if errors.Is(e, errKafkaClientClosed) || c.ctx.Err() != nil {
						return
					}

					recoverablePollErrors++
					if !pollDegraded {
						c.log.Errorf(
							"Kafka poll failed for consumer %q topics=%v; worker will keep polling and should rejoin when Kafka is available: %v",
							c.name,
							c.topics,
							e,
						)
						pollDegraded = true
					} else if recoverablePollErrors%10 == 0 {
						c.log.Warnf(
							"Kafka poll still failing for consumer %q topics=%v after %d recoverable poll errors; last error: %v",
							c.name,
							c.topics,
							recoverablePollErrors,
							e,
						)
					}
					time.Sleep(c.pollBackoff())
				default:
					// ignore other events
				}
			}

			if len(batch) > 0 && c.batchMaxWait > 0 && !flushDeadline.IsZero() && time.Now().After(flushDeadline) {
				flush()
			}
			if len(batch) == 0 && c.batchDone != nil && c.franzClient != nil {
				c.franzClient.AllowRebalance()
			}
		}
	}()

	return msgChan, nil
}

func (c *Consumer) pollBackoff() time.Duration {
	backoff := time.Duration(c.pollDurationMs) * time.Millisecond
	if backoff <= 0 {
		return time.Duration(defaultPollDurationMs) * time.Millisecond
	}
	if backoff > time.Second {
		return time.Second
	}
	return backoff
}

func approximateMessageSize(msg *Message) int {
	if msg == nil {
		return 0
	}

	size := len(msg.Value) + len(msg.Key)
	for _, h := range msg.Headers {
		size += len(h.Key) + len(h.Value)
	}

	return size
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.stateMu.Lock()
		c.closed = true
		c.cancel()
		started := c.pollStarted
		c.stateMu.Unlock()
		if started {
			<-c.pollDone
		}
		if c.lagDone != nil {
			<-c.lagDone
		}
		if c.franzClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			c.franzClient.LeaveGroupContext(ctx)
			cancel()
		}
		if c.kafkaConsumer != nil {
			c.closeErr = c.kafkaConsumer.Close()
		}
	})
	return c.closeErr
}

// CompleteBatch releases the polling loop only after every record is resolved
// and its offset committed. Cancellation leaves the batch available for replay.
func (c *Consumer) CompleteBatch() {
	c.finishBatch(true)
}

// AbortBatch rewinds local fetch positions before releasing ownership for replay.
func (c *Consumer) AbortBatch() { c.finishBatch(false) }

func (c *Consumer) finishBatch(complete bool) {
	if c.batchDone != nil {
		select {
		case c.batchDone <- complete:
		case <-c.ctx.Done():
		}
	}
}

func (c *Consumer) BatchContext() context.Context {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.batchCtx != nil {
		return c.batchCtx
	}
	return c.ctx
}

func (c *Consumer) RequestRejoin() {
	if c.franzClient != nil {
		c.franzClient.ForceRebalance()
	}
}

func IsOwnershipError(err error) bool {
	return errors.Is(err, kerr.IllegalGeneration) || errors.Is(err, kerr.UnknownMemberID) || errors.Is(err, kerr.RebalanceInProgress) || errors.Is(err, kerr.FencedInstanceID)
}

// CommitMessage commits the offset of the provided message.
func (c *Consumer) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if c.kafkaConsumer == nil {
		return nil, fmt.Errorf("kafka consumer is not initialized")
	}
	committed, err := c.kafkaConsumer.CommitMessage(msg)
	if err == nil && c.lag != nil {
		c.lag.committed(msg)
	}
	return committed, err
}

// Lag reports the partitions this consumer currently owns and how far each is
// behind. It returns nil when lag tracking is disabled.
func (c *Consumer) Lag(now time.Time) []PartitionLag {
	if c == nil || c.lag == nil {
		return nil
	}
	return c.lag.snapshot(now)
}

func (c *Consumer) refreshLagLoop() {
	defer close(c.lagDone)
	ticker := time.NewTicker(c.lagRefreshInterval)
	defer ticker.Stop()
	failing := false
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		err := c.refreshLag()
		switch {
		case err != nil && c.ctx.Err() != nil:
			return
		case err != nil && !failing:
			c.log.Warnf("Lag metrics for consumer %q are stale: could not read partition offsets: %v", c.name, err)
			failing = true
		case err == nil && failing:
			c.log.Infof("Lag metrics for consumer %q recovered", c.name)
			failing = false
		}
	}
}

// refreshLag reads the newest offset of every owned partition, and the log
// start offset when some partition has no committed offset yet. It uses list
// offset requests on the consumer's own client and never touches the group.
func (c *Consumer) refreshLag() error {
	topics, needStart := c.lag.wanted()
	if len(topics) == 0 {
		return nil
	}
	timeout := c.lagRefreshInterval
	if timeout > maxLagRequestTimeout {
		timeout = maxLagRequestTimeout
	}
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()
	admin := kadm.NewClient(c.franzClient)

	ends, err := admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	end, err := listedOffsets(ends)
	var start map[topicPartition]int64
	if needStart {
		starts, startErr := admin.ListStartOffsets(ctx, topics...)
		if startErr != nil {
			return startErr
		}
		var partialErr error
		start, partialErr = listedOffsets(starts)
		err = errors.Join(err, partialErr)
	}
	committed := make(map[topicPartition]int64)
	for topic, partitions := range c.franzClient.CommittedOffsets() {
		for partition, offset := range partitions {
			committed[topicPartition{topic, partition}] = offset.Offset
		}
	}
	// Apply whatever was read even if some partitions failed.
	c.lag.updateOffsets(end, start, committed)
	return err
}

func listedOffsets(listed kadm.ListedOffsets) (map[topicPartition]int64, error) {
	out := make(map[topicPartition]int64)
	var errs []error
	listed.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			errs = append(errs, fmt.Errorf("%s[%d]: %w", o.Topic, o.Partition, o.Err))
			return
		}
		out[topicPartition{o.Topic, o.Partition}] = o.Offset
	})
	return out, errors.Join(errs...)
}

// franzConsumerAdapter adapts franz-go client to the KafkaConsumerClient interface.
type franzConsumerAdapter struct {
	client *kgo.Client
	ctx    context.Context
}

func (f *franzConsumerAdapter) Poll(ms int) Event {
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(ms)*time.Millisecond)
	defer cancel()

	fetches := f.client.PollRecords(ctx, 1)
	logFetchErrors(logger.S(), fetches)
	if fetches.IsClientClosed() {
		return errKafkaClientClosed
	}

	// A fetch can contain records and errors from different partitions. Deliver
	// records first; returning an error must never discard fetched records.
	iter := fetches.RecordIter()
	if !iter.Done() {
		return iter.Next()
	}
	errs := fetches.Errors()
	for _, fetchErr := range errs {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}
		return fetchErr.Err
	}

	return nil
}

func (f *franzConsumerAdapter) Close() error {
	f.client.CloseAllowingRebalance()
	return nil
}

func (f *franzConsumerAdapter) CommitMessage(msg *Message) ([]TopicPartition, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is required")
	}

	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	if err := f.client.CommitRecords(ctx, msg); err != nil {
		return nil, err
	}

	topic := msg.Topic
	return []TopicPartition{
		{
			Topic:     &topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		},
	}, nil
}
