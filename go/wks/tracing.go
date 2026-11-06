// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"wolvekafka/go/kafka"
)

// recordHeaderCarrier exposes Kafka record headers to an OpenTelemetry
// propagator. It is read-only: WKS never rewrites input record headers.
type recordHeaderCarrier []kafka.Header

var _ propagation.TextMapCarrier = recordHeaderCarrier(nil)

func (c recordHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c recordHeaderCarrier) Set(string, string) {}

func (c recordHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}

// batchSpanParent derives the parent context and links for a batch span from
// the trace context carried in the records' headers, following the
// OpenTelemetry messaging conventions for batch processing:
//
//   - If every record carries the same span context (always the case for a
//     traced single-record batch), the batch span continues that trace.
//   - Otherwise the batch span starts a new trace and links to each distinct
//     record span context.
//
// The returned context always derives from ctx, so its cancellation and values
// are preserved.
func batchSpanParent(ctx context.Context, prop propagation.TextMapPropagator, msgs []*kafka.Message) (context.Context, []trace.Link) {
	type spanKey struct {
		traceID trace.TraceID
		spanID  trace.SpanID
	}
	var (
		links      []trace.Link
		seen       = map[spanKey]struct{}{}
		untraced   bool
		firstMsgIx = -1
	)
	for i, msg := range msgs {
		if msg == nil {
			continue
		}
		sc := trace.SpanContextFromContext(prop.Extract(context.Background(), recordHeaderCarrier(msg.Headers)))
		if !sc.IsValid() {
			untraced = true
			continue
		}
		key := spanKey{traceID: sc.TraceID(), spanID: sc.SpanID()}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if firstMsgIx < 0 {
			firstMsgIx = i
		}
		links = append(links, trace.Link{SpanContext: sc})
	}
	if len(links) == 1 && !untraced {
		// Re-extract onto ctx so baggage from the record travels with the parent.
		return prop.Extract(ctx, recordHeaderCarrier(msgs[firstMsgIx].Headers)), nil
	}
	return ctx, links
}
