// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/handler/v1"
)

// Transport names, used in metrics, spans, and logs.
const (
	transportHTTP  = "http"
	transportGRPC  = "grpc"
	transportGRPCS = "grpcs"
)

// handlerTransport is the mechanics of reaching a handler: connecting,
// encoding the batch, and decoding the response. Everything that decides what
// to do with the response (per-record status, retries, dead-lettering, and
// commits) sits above it and is shared by every transport.
//
// Send must honor ctx: rebalances, reloads, shutdowns, and the target's
// timeout_ms all end an in-flight call by cancelling it. Send reports a busy
// handler with a *throttledError so it is retried without spending the
// records' retry budget.
type handlerTransport interface {
	Send(ctx context.Context, target *configs_pb.Lambda, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error)
	// Close releases connections the transport owns. It is called when the
	// job stops, on rebalance, reload, and shutdown.
	Close() error
}

// transportForURL returns the name of the transport that serves a target URL.
// Anything that is not another transport's scheme stays on HTTP, which reports
// malformed and unsupported URLs when the request is made.
func transportForURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return transportHTTP
	}
	switch strings.ToLower(u.Scheme) {
	case "grpc":
		return transportGRPC
	case "grpcs":
		return transportGRPCS
	default:
		return transportHTTP
	}
}

// handlerTransports holds one transport of each kind for a processing job,
// created on first use. A job delivers one batch at a time, so it needs no
// locking.
type handlerTransports struct {
	byName map[string]handlerTransport
}

func newHandlerTransports() *handlerTransports {
	return &handlerTransports{byName: map[string]handlerTransport{}}
}

// forTarget returns the transport for a target and the transport's name.
func (t *handlerTransports) forTarget(target *configs_pb.Lambda) (handlerTransport, string, error) {
	name := transportForURL(target.GetUrl())
	if tr, ok := t.byName[name]; ok {
		return tr, name, nil
	}
	var tr handlerTransport
	switch name {
	case transportHTTP:
		tr = &httpTransport{}
	case transportGRPC:
		tr = newGRPCTransport(false)
	case transportGRPCS:
		tr = newGRPCTransport(true)
	default:
		return nil, name, fmt.Errorf("unsupported handler transport %q", name)
	}
	t.byName[name] = tr
	return tr, name, nil
}

// Close closes every transport created so far.
func (t *handlerTransports) Close() error {
	var errs []error
	for name, tr := range t.byName {
		if err := tr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s transport: %w", name, err))
		}
	}
	t.byName = map[string]handlerTransport{}
	return errors.Join(errs...)
}

// targetAddress returns a target's host for spans and logs, leaving out user
// info, path, and query, which may carry credentials.
func targetAddress(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}
