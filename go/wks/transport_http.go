// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

// lambdaHTTPClient is shared by all HTTP targets; each request carries its own
// deadline from the target's timeout_ms.
var lambdaHTTPClient = &http.Client{}

// httpTransport POSTs the batch as JSON and parses a JSON LambdaResponse.
type httpTransport struct{}

func (t *httpTransport) Send(ctx context.Context, target *configs_pb.Lambda, batch *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
	payload, err := ConvertLambdaRequestToJson(batch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal lambda request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.GetUrl(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Let the handler join the batch's trace (traceparent, tracestate, baggage).
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := lambdaHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send lambda request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("lambda request failed with status %d and read error: %w", resp.StatusCode, err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		reason := strings.TrimSpace(string(body))
		if reason == "" {
			reason = resp.Status
		}
		err := fmt.Errorf("lambda request failed with status %d: %s", resp.StatusCode, reason)
		if throttled := throttleFromResponse(resp.StatusCode, resp.Header, time.Now(), err); throttled != nil {
			return nil, throttled
		}
		return nil, err
	}

	respMsgs, err := ParseLambdaResponse(body)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal lambda response: %w", err)
	}
	return respMsgs, nil
}

// Close is a no-op: the HTTP client and its connection pool are shared by all
// jobs and outlive any one of them.
func (t *httpTransport) Close() error { return nil }
