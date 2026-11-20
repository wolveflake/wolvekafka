// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

type fakeTransport struct {
	targets  []string
	deadline time.Duration
	closed   int
	err      error
}

func (f *fakeTransport) Send(ctx context.Context, target *configs_pb.Lambda, req *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error) {
	f.targets = append(f.targets, target.GetUrl())
	if deadline, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(deadline)
	}
	if f.err != nil {
		return nil, f.err
	}
	resp := &model_pb.LambdaResponse{}
	for range req.GetMessages() {
		resp.Messages = append(resp.Messages, &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_SUCCESS})
	}
	return resp, nil
}

func (f *fakeTransport) Close() error {
	f.closed++
	return f.err
}

func TestTransportForURL(t *testing.T) {
	for _, u := range []string{"http://handler:8080/", "HTTPS://handler/", "handler:8080", "::bad", ""} {
		require.Equal(t, transportHTTP, transportForURL(u), u)
	}
}

func TestTargetAddressOmitsCredentialsAndPath(t *testing.T) {
	require.Equal(t, "handler:8080", targetAddress("https://user:secret@handler:8080/events?token=x"))
	require.Equal(t, "", targetAddress("not a url"))
}

func TestSendGoesThroughTheTargetsTransportWithItsDeadline(t *testing.T) {
	fake := &fakeTransport{}
	transports := newHandlerTransports()
	transports.byName[transportHTTP] = fake
	job := &ProcessingJob{
		consumerCfg:    retryTestPolicy(),
		lambdaSelector: NewLambdaSelector([]*configs_pb.Lambda{{Url: "http://handler/", TimeoutMs: 2000}}),
		transports:     transports,
	}

	result, err := defaultSendEventFunc(job, context.Background(), &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{{}, {}}})

	require.NoError(t, err)
	require.Len(t, result.Response.Messages, 2)
	require.Equal(t, []string{"http://handler/"}, fake.targets)
	require.Greater(t, fake.deadline, time.Second)
	require.LessOrEqual(t, fake.deadline, 2*time.Second)
}

func TestHandlerTransportsReuseAndClose(t *testing.T) {
	transports := newHandlerTransports()
	target := &configs_pb.Lambda{Url: "http://handler/"}
	first, name, err := transports.forTarget(target)
	require.NoError(t, err)
	require.Equal(t, transportHTTP, name)
	second, _, err := transports.forTarget(&configs_pb.Lambda{Url: "https://other/"})
	require.NoError(t, err)
	require.Same(t, first, second, "one transport per kind is shared by its targets")

	fake := &fakeTransport{err: errors.New("close failed")}
	transports.byName[transportHTTP] = fake
	require.ErrorContains(t, transports.Close(), "close failed")
	require.Equal(t, 1, fake.closed)
	require.Empty(t, transports.byName)
}

func TestThrottledErrorDescribesItsTransport(t *testing.T) {
	require.Equal(t, "HTTP 429", (&throttledError{status: 429}).describe())
	require.Equal(t, "HTTP 503", (&throttledError{transport: transportHTTP, status: 503}).describe())
}
