// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	"wolvekafka/go/common/logger"
)

// instrumentation centralizes all OpenTelemetry handles used by the WKS pipeline.
type instrumentation struct {
	tracer trace.Tracer

	configApplied        metric.Int64Counter
	consumersConfigured  metric.Int64UpDownCounter
	jobsInflight         metric.Int64UpDownCounter
	batchMessages        metric.Int64Histogram
	batchBytes           metric.Int64Histogram
	messagesProcessed    metric.Int64Counter
	messagesDeadLettered metric.Int64Counter
	messagesDropped      metric.Int64Counter
	lambdaDurationMs     metric.Float64Histogram
	lambdaFailures       metric.Int64Counter
	lambdaRetries        metric.Int64Counter
	kafkaCommitErrors    metric.Int64Counter
	outputErrors         metric.Int64Counter
}

var (
	instOnce sync.Once
	inst     *instrumentation
)

func getInstrumentation() *instrumentation {
	instOnce.Do(func() {
		meter := otel.Meter("wolvekafka/wks")
		inst = &instrumentation{
			tracer:               otel.Tracer("wolvekafka/wks/processor"),
			configApplied:        buildInt64Counter(meter, "wks.config.applied_total", "Number of configuration refreshes applied at runtime."),
			consumersConfigured:  buildInt64UpDownCounter(meter, "wks.consumers.configured", "Count of active consumer groups managed by the service."),
			jobsInflight:         buildInt64UpDownCounter(meter, "wks.jobs.inflight", "Number of processing jobs currently running."),
			batchMessages:        buildInt64Histogram(meter, "wks.kafka.batch.messages", "Number of Kafka records delivered per batch."),
			batchBytes:           buildInt64Histogram(meter, "wks.kafka.batch.bytes", "Approximate size of Kafka batches in bytes.", metric.WithUnit("By")),
			messagesProcessed:    buildInt64Counter(meter, "wks.kafka.messages_processed", "Count of Kafka records that were successfully processed and committed."),
			messagesDeadLettered: buildInt64Counter(meter, "wks.kafka.messages_dead_lettered", "Count of input records dead-lettered and committed."),
			messagesDropped:      buildInt64Counter(meter, "wks.kafka.messages_dropped", "Count of input records explicitly dropped and committed."),
			lambdaDurationMs:     buildFloat64Histogram(meter, "wks.lambda.duration_ms", "End-to-end duration spent invoking downstream lambdas.", metric.WithUnit("ms")),
			lambdaFailures:       buildInt64Counter(meter, "wks.lambda.failures", "Number of lambda invocations that returned an error."),
			lambdaRetries:        buildInt64Counter(meter, "wks.lambda.retries", "Number of lambda retry attempts triggered after a failure."),
			kafkaCommitErrors:    buildInt64Counter(meter, "wks.kafka.commit_errors", "Count of failures while committing Kafka offsets."),
			outputErrors:         buildInt64Counter(meter, "wks.output.errors", "Number of failures while executing output policies."),
		}
	})
	return inst
}

func (i *instrumentation) recordBatchOutcomes(ctx context.Context, consumer string, jobID int, result *BatchResult) {
	if i == nil || result == nil {
		return
	}
	i.recordMessagesProcessed(ctx, consumer, jobID, len(result.Response.GetMessages()))
	opts := metric.WithAttributes(i.jobAttrs(consumer, jobID)...)
	i.messagesDeadLettered.Add(ctx, int64(result.DeadLettered), opts)
	i.messagesDropped.Add(ctx, int64(result.Dropped), opts)
}

func buildInt64Counter(m metric.Meter, name, desc string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	instrumentOpts := append([]metric.Int64CounterOption{metric.WithDescription(desc)}, opts...)
	counter, err := m.Int64Counter(name, instrumentOpts...)
	if err != nil {
		logger.S().Warnf("failed to create counter %s: %v", name, err)
		fallback, _ := noop.NewMeterProvider().Meter("noop").Int64Counter(name)
		return fallback
	}
	return counter
}

func buildInt64UpDownCounter(m metric.Meter, name, desc string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter {
	instrumentOpts := append([]metric.Int64UpDownCounterOption{metric.WithDescription(desc)}, opts...)
	gauge, err := m.Int64UpDownCounter(name, instrumentOpts...)
	if err != nil {
		logger.S().Warnf("failed to create updown counter %s: %v", name, err)
		fallback, _ := noop.NewMeterProvider().Meter("noop").Int64UpDownCounter(name)
		return fallback
	}
	return gauge
}

func buildInt64Histogram(m metric.Meter, name, desc string, opts ...metric.Int64HistogramOption) metric.Int64Histogram {
	instrumentOpts := append([]metric.Int64HistogramOption{metric.WithDescription(desc)}, opts...)
	hist, err := m.Int64Histogram(name, instrumentOpts...)
	if err != nil {
		logger.S().Warnf("failed to create histogram %s: %v", name, err)
		fallback, _ := noop.NewMeterProvider().Meter("noop").Int64Histogram(name)
		return fallback
	}
	return hist
}

func buildFloat64Histogram(m metric.Meter, name, desc string, opts ...metric.Float64HistogramOption) metric.Float64Histogram {
	instrumentOpts := append([]metric.Float64HistogramOption{metric.WithDescription(desc)}, opts...)
	hist, err := m.Float64Histogram(name, instrumentOpts...)
	if err != nil {
		logger.S().Warnf("failed to create histogram %s: %v", name, err)
		fallback, _ := noop.NewMeterProvider().Meter("noop").Float64Histogram(name)
		return fallback
	}
	return hist
}

func (i *instrumentation) recordConfigApplied(ctx context.Context, consumerCount int) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	i.configApplied.Add(ctx, 1, metric.WithAttributes(attribute.Int("consumer_count", consumerCount)))
}

func (i *instrumentation) recordConsumerLifecycle(ctx context.Context, consumer string, delta int64) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	i.consumersConfigured.Add(ctx, delta, metric.WithAttributes(attribute.String("consumer", consumer)))
}

func (i *instrumentation) jobStarted(ctx context.Context, consumer string, jobID int) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	i.jobsInflight.Add(ctx, 1, metric.WithAttributes(i.jobAttrs(consumer, jobID)...))
}

func (i *instrumentation) jobFinished(ctx context.Context, consumer string, jobID int) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	i.jobsInflight.Add(ctx, -1, metric.WithAttributes(i.jobAttrs(consumer, jobID)...))
}

func (i *instrumentation) recordBatch(ctx context.Context, consumer string, jobID int, messageCount int, batchBytes int64) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	attrs := metric.WithAttributes(i.jobAttrs(consumer, jobID)...)
	i.batchMessages.Record(ctx, int64(messageCount), attrs)
	i.batchBytes.Record(ctx, batchBytes, attrs)
}

func (i *instrumentation) recordMessagesProcessed(ctx context.Context, consumer string, jobID int, messageCount int) {
	if i == nil || messageCount <= 0 {
		return
	}
	ctx = ensureContext(ctx)
	i.messagesProcessed.Add(ctx, int64(messageCount), metric.WithAttributes(i.jobAttrs(consumer, jobID)...))
}

func (i *instrumentation) recordLambdaResult(ctx context.Context, consumer string, jobID int, duration time.Duration, err error) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	attrs := metric.WithAttributes(i.jobAttrs(consumer, jobID)...)
	i.lambdaDurationMs.Record(ctx, float64(duration.Milliseconds()), attrs)
	if err != nil {
		i.lambdaFailures.Add(ctx, 1, attrs)
	}
}

func (i *instrumentation) recordLambdaRetry(ctx context.Context, consumer string, jobID int, attempt int, err error) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	attrs := append(i.jobAttrs(consumer, jobID), attribute.Int("attempt", attempt))
	i.lambdaRetries.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (i *instrumentation) recordCommitError(ctx context.Context, consumer string, jobID int, err error) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	attrs := append(i.jobAttrs(consumer, jobID), attribute.String("error", err.Error()))
	i.kafkaCommitErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (i *instrumentation) recordOutputError(ctx context.Context, consumer string, jobID int, err error) {
	if i == nil {
		return
	}
	ctx = ensureContext(ctx)
	attrs := append(i.jobAttrs(consumer, jobID), attribute.String("error", err.Error()))
	i.outputErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (i *instrumentation) startBatchSpan(ctx context.Context, consumer string, jobID int, messageCount int, batchBytes int64, links []trace.Link) (context.Context, trace.Span) {
	ctx = ensureContext(ctx)
	tracer := i.tracer
	if tracer == nil {
		tracer = otel.Tracer("wolvekafka/wks/processor")
	}
	attrs := append(i.jobAttrs(consumer, jobID),
		attribute.Int("batch.messages", messageCount),
		attribute.Int64("batch.bytes", batchBytes),
	)
	return tracer.Start(ctx, "wks.process_batch",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
		trace.WithLinks(links...))
}

func (i *instrumentation) jobAttrs(consumer string, jobID int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("consumer", consumer),
		attribute.Int("job_id", jobID),
	}
}

func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
