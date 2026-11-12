// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"wolvekafka/go/kafka"
)

type fakeLagSource []kafka.PartitionLag

func (f fakeLagSource) Lag(time.Time) []kafka.PartitionLag { return f }

func newTestLagMetrics(t *testing.T) (*lagMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	lag, err := newLagMetrics(provider.Meter("test"))
	require.NoError(t, err)
	return lag, reader
}

// collectGauges returns gauge values keyed by metric name, then by a label
// string such as "consumer=a,topic=t,partition=0".
func collectGauges(t *testing.T, reader *sdkmetric.ManualReader) map[string]map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]map[string]float64{}
	label := func(set attribute.Set) string {
		s := ""
		for _, key := range []attribute.Key{"consumer", "topic", "partition"} {
			if v, ok := set.Value(key); ok {
				if s != "" {
					s += ","
				}
				s += string(key) + "=" + v.Emit()
			}
		}
		return s
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			values := map[string]float64{}
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					values[label(p.Attributes)] = float64(p.Value)
				}
			case metricdata.Gauge[float64]:
				for _, p := range data.DataPoints {
					values[label(p.Attributes)] = p.Value
				}
			default:
				t.Fatalf("unexpected data type %T for %s", m.Data, m.Name)
			}
			out[m.Name] = values
		}
	}
	return out
}

func TestLagMetricsPerPartition(t *testing.T) {
	lag, reader := newTestLagMetrics(t)
	lag.register("orders-handler", fakeLagSource{
		{Topic: "orders", Partition: 0, Lag: 12, LagSeconds: 30},
		{Topic: "orders", Partition: 1, Lag: 0, LagSeconds: 0},
	})
	// A second worker of the same group owns a different partition.
	lag.register("orders-handler", fakeLagSource{
		{Topic: "orders", Partition: 2, Lag: 5, LagSeconds: -1},
		{Topic: "orders", Partition: 3, Lag: -1, LagSeconds: -1},
	})

	got := collectGauges(t, reader)
	assert.Equal(t, map[string]float64{
		"consumer=orders-handler,topic=orders,partition=0": 12,
		"consumer=orders-handler,topic=orders,partition=1": 0,
		"consumer=orders-handler,topic=orders,partition=2": 5,
	}, got["wks.kafka.consumer_lag"], "partitions with unknown lag are omitted")
	assert.Equal(t, map[string]float64{
		"consumer=orders-handler,topic=orders,partition=0": 30,
		"consumer=orders-handler,topic=orders,partition=1": 0,
	}, got["wks.kafka.consumer_lag_seconds"], "unknown time lag is omitted")
	assert.Equal(t, map[string]float64{
		"consumer=orders-handler,topic=orders": 4,
	}, got["wks.kafka.partitions_assigned"], "assigned partitions count even when lag is unknown")
}

func TestLagMetricsPerTopicSumsRecordsAndTakesOldestAge(t *testing.T) {
	lag, reader := newTestLagMetrics(t)
	lag.setGranularity(LagByTopic)
	lag.register("orders-handler", fakeLagSource{
		{Topic: "orders", Partition: 0, Lag: 12, LagSeconds: 30},
		{Topic: "orders", Partition: 1, Lag: 8, LagSeconds: 90},
		{Topic: "refunds", Partition: 0, Lag: 0, LagSeconds: 0},
	})
	lag.register("orders-handler", fakeLagSource{
		{Topic: "orders", Partition: 2, Lag: 5, LagSeconds: 10},
	})

	got := collectGauges(t, reader)
	assert.Equal(t, map[string]float64{
		"consumer=orders-handler,topic=orders":  25,
		"consumer=orders-handler,topic=refunds": 0,
	}, got["wks.kafka.consumer_lag"])
	assert.Equal(t, map[string]float64{
		"consumer=orders-handler,topic=orders":  90,
		"consumer=orders-handler,topic=refunds": 0,
	}, got["wks.kafka.consumer_lag_seconds"])
}

func TestLagMetricsStopReportingUnregisteredWorkers(t *testing.T) {
	lag, reader := newTestLagMetrics(t)
	unregister := lag.register("orders-handler", fakeLagSource{{Topic: "orders", Partition: 0, Lag: 3, LagSeconds: 1}})
	require.Len(t, collectGauges(t, reader)["wks.kafka.consumer_lag"], 1)

	unregister()
	unregister() // safe to call twice
	assert.Empty(t, collectGauges(t, reader)["wks.kafka.consumer_lag"])
}

func TestLagMetricsNilIsSafe(t *testing.T) {
	var lag *lagMetrics
	lag.setGranularity(LagByTopic)
	lag.register("orders-handler", fakeLagSource{})()

	var metrics *instrumentation
	metrics.registerLagSource("orders-handler", fakeLagSource{})()
}

func TestLagMetricsOptions(t *testing.T) {
	opts, err := LagMetricsOptions{}.withDefaults()
	require.NoError(t, err)
	assert.Equal(t, LagByPartition, opts.Granularity)
	assert.Equal(t, 15*time.Second, opts.refreshInterval())

	opts, err = LagMetricsOptions{Granularity: LagOff, RefreshInterval: time.Second}.withDefaults()
	require.NoError(t, err)
	assert.Zero(t, opts.refreshInterval(), "off disables offset requests")

	_, err = LagMetricsOptions{Granularity: "consumer"}.withDefaults()
	assert.ErrorContains(t, err, "unknown lag metrics granularity")

	_, err = LagMetricsOptions{RefreshInterval: -time.Second}.withDefaults()
	assert.Error(t, err)
}

func TestNewServiceRejectsUnknownLagGranularity(t *testing.T) {
	_, err := NewWolveKafkaService(context.Background(), WolveKafkaParams{LagMetrics: LagMetricsOptions{Granularity: "bogus"}})
	assert.ErrorContains(t, err, "unknown lag metrics granularity")
}
