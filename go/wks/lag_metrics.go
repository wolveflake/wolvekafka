// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"wolvekafka/go/kafka"
)

// Lag metric granularity values.
const (
	// LagByPartition reports one series per consumer, topic, and partition.
	LagByPartition = "partition"
	// LagByTopic adds partitions up per consumer and topic, bounding series
	// count on topics with very many partitions.
	LagByTopic = "topic"
	// LagOff disables lag tracking and the offset requests it makes.
	LagOff = "off"

	defaultLagRefreshInterval = 15 * time.Second
)

// LagMetricsOptions controls consumer lag reporting.
type LagMetricsOptions struct {
	// Granularity is LagByPartition (the default), LagByTopic, or LagOff.
	Granularity string
	// RefreshInterval is how often each worker reads the newest offsets of
	// its partitions from Kafka. Defaults to 15 seconds.
	RefreshInterval time.Duration
}

func (o LagMetricsOptions) withDefaults() (LagMetricsOptions, error) {
	switch o.Granularity {
	case "":
		o.Granularity = LagByPartition
	case LagByPartition, LagByTopic, LagOff:
	default:
		return o, fmt.Errorf("unknown lag metrics granularity %q: use %q, %q, or %q", o.Granularity, LagByPartition, LagByTopic, LagOff)
	}
	if o.RefreshInterval < 0 {
		return o, fmt.Errorf("lag refresh interval must not be negative")
	}
	if o.RefreshInterval == 0 {
		o.RefreshInterval = defaultLagRefreshInterval
	}
	return o, nil
}

// refreshInterval is the interval workers pass to their Kafka consumer; zero
// turns lag tracking off.
func (o LagMetricsOptions) refreshInterval() time.Duration {
	if o.Granularity == LagOff {
		return 0
	}
	return o.RefreshInterval
}

// lagSource is implemented by *kafka.Consumer.
type lagSource interface {
	Lag(now time.Time) []kafka.PartitionLag
}

type lagRegistration struct {
	consumer string
	source   lagSource
}

// lagMetrics exposes lag as gauges read at collection time. Each worker
// registers its Kafka consumer; partitions are owned by one worker at a time,
// so per-partition series never collide within a process.
type lagMetrics struct {
	byTopic atomic.Bool

	mu      sync.Mutex
	nextID  uint64
	sources map[uint64]lagRegistration

	now func() time.Time
}

func newLagMetrics(meter metric.Meter) (*lagMetrics, error) {
	l := &lagMetrics{sources: make(map[uint64]lagRegistration), now: time.Now}
	lag, err := meter.Int64ObservableGauge("wks.kafka.consumer_lag",
		metric.WithDescription("Records in owned partitions not yet committed by the consumer group."),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	lagSeconds, err := meter.Float64ObservableGauge("wks.kafka.consumer_lag_seconds",
		metric.WithDescription("Age of the oldest record in owned partitions not yet committed; zero when caught up."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	assigned, err := meter.Int64ObservableGauge("wks.kafka.partitions_assigned",
		metric.WithDescription("Partitions currently assigned to this process, per consumer group and topic."),
		metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		l.observe(o, lag, lagSeconds, assigned)
		return nil
	}, lag, lagSeconds, assigned)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *lagMetrics) setGranularity(granularity string) {
	if l != nil {
		l.byTopic.Store(granularity == LagByTopic)
	}
}

// register adds a worker's consumer and returns a function that removes it.
func (l *lagMetrics) register(consumer string, source lagSource) func() {
	if l == nil || source == nil {
		return func() {}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	id := l.nextID
	l.nextID++
	l.sources[id] = lagRegistration{consumer: consumer, source: source}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.sources, id)
		})
	}
}

type lagKey struct {
	consumer  string
	topic     string
	partition int32 // -1 when reporting per topic
}

type lagValue struct {
	records    int64
	seconds    float64
	hasRecords bool
	hasSeconds bool
}

func (l *lagMetrics) observe(o metric.Observer, lag metric.Int64Observable, lagSeconds metric.Float64Observable, assigned metric.Int64Observable) {
	l.mu.Lock()
	registrations := make([]lagRegistration, 0, len(l.sources))
	for _, r := range l.sources {
		registrations = append(registrations, r)
	}
	l.mu.Unlock()

	byTopic := l.byTopic.Load()
	now := l.now()
	values := make(map[lagKey]*lagValue)
	counts := make(map[lagKey]int64)
	for _, r := range registrations {
		for _, p := range r.source.Lag(now) {
			counts[lagKey{consumer: r.consumer, topic: p.Topic, partition: -1}]++
			key := lagKey{consumer: r.consumer, topic: p.Topic, partition: p.Partition}
			if byTopic {
				key.partition = -1
			}
			v := values[key]
			if v == nil {
				v = &lagValue{}
				values[key] = v
			}
			if p.Lag >= 0 {
				v.records += p.Lag
				v.hasRecords = true
			}
			// Per topic, report the oldest waiting record across partitions.
			if p.LagSeconds >= 0 && (!v.hasSeconds || p.LagSeconds > v.seconds) {
				v.seconds = p.LagSeconds
				v.hasSeconds = true
			}
		}
	}

	for key, count := range counts {
		o.ObserveInt64(assigned, count, metric.WithAttributes(lagAttrs(key)...))
	}
	for key, v := range values {
		attrs := metric.WithAttributes(lagAttrs(key)...)
		if v.hasRecords {
			o.ObserveInt64(lag, v.records, attrs)
		}
		if v.hasSeconds {
			o.ObserveFloat64(lagSeconds, v.seconds, attrs)
		}
	}
}

func lagAttrs(key lagKey) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("consumer", key.consumer),
		attribute.String("topic", key.topic),
	}
	if key.partition >= 0 {
		attrs = append(attrs, attribute.Int("partition", int(key.partition)))
	}
	return attrs
}
