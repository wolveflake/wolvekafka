// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"wolvekafka/go/configs"
	"wolvekafka/go/kafka"
	model_pb "wolvekafka/proto/model"
)

func TestBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assert.NotNil(t, ctx)

	// start a test HTTP server to act as a lambda endpoint
	lambdaReqChan := make(chan *model_pb.LambdaRequest, 1)

	jsonCfg := fmt.Sprintf(`{
		"cluster": {
			"bootstrap_servers": [
				"broker1:9092"
			]
		},
		"consumers": [
			{
				"group_id": "group1",
				"topics": ["topic1", "topic2"],
				"lambdas": [
					{
						"url": "http://127.0.0.1/lambda"
					}
				]
			}
		]
	}`)

	cfgProto, err := configs.ParseConfigsJson([]byte(jsonCfg))
	assert.NoError(t, err)
	assert.NotNil(t, cfgProto)
	cfg, err := configs.NewConfigFromProto(cfgProto)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	clientSource := newStubKafkaClientSource()
	wks, err := NewWolveKafkaService(ctx, WolveKafkaParams{
		Config: cfg,
		newConsumerFunc: func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error) {
			params.Cluster = cfg.Cluster()
			params.Client = clientSource.NewClient()
			return kafka.NewConsumer(ctx, params)
		},
		sendEventFunc: func(c *ProcessingJob, ctx context.Context, req *model_pb.LambdaRequest) (*BatchResult, error) {
			lambdaReqChan <- req
			return &BatchResult{Request: req, Response: &model_pb.LambdaResponse{
				Messages: []*model_pb.ResponseMessage{
					&model_pb.ResponseMessage{
						Status: model_pb.ResponseStatus_STATUS_SUCCESS,
					},
				},
			}}, nil
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, wks)

	topic := "topic1"
	// send a test message
	testMsg := &kafka.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Value:     []byte("test message"),
	}
	clientSource.Emit(testMsg)

	// wait for the lambda to be invoked
	select {
	case req := <-lambdaReqChan:
		assert.NotNil(t, req)
		assert.Equal(t, 1, len(req.GetMessages()))
		assert.Equal(t, "test message", string(req.Messages[0].Value))
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for lambda invocation")
	}
}

func TestHttpLambda(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assert.NotNil(t, ctx)

	lambdaReqChan := make(chan *model_pb.LambdaRequest, 1)
	// start a test HTTP server to act as a lambda endpoint
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// read the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		lambdaReq, err := ParseLambdaRequestJson(body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse lambda request: %v", err), http.StatusBadRequest)
			return
		}
		lambdaReqChan <- lambdaReq
		w.WriteHeader(http.StatusOK)
	}))

	jsonCfg := fmt.Sprintf(`{
		"cluster": {
			"bootstrap_servers": [
				"broker1:9092"
			]
		},
		"consumers": [
			{
				"group_id": "group1",
				"topics": ["topic1", "topic2"],
				"lambdas": [
					{
						"url": "%s"
					}
				]
			}
		]
	}`, ts.URL)

	cfgProto, err := configs.ParseConfigsJson([]byte(jsonCfg))
	assert.NoError(t, err)
	assert.NotNil(t, cfgProto)
	cfg, err := configs.NewConfigFromProto(cfgProto)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	clientSource := newStubKafkaClientSource()
	wks, err := NewWolveKafkaService(ctx, WolveKafkaParams{
		Config: cfg,
		newConsumerFunc: func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error) {
			params.Cluster = cfg.Cluster()
			params.Client = clientSource.NewClient()
			return kafka.NewConsumer(ctx, params)
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, wks)

	topic := "topic1"
	// send a test message
	testMsg := &kafka.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Value:     []byte("test message"),
	}
	clientSource.Emit(testMsg)

	// wait for the lambda to be invoked
	select {
	case lambdaReq := <-lambdaReqChan:
		assert.NotNil(t, lambdaReq)
		msgs := lambdaReq.GetMessages()
		assert.Equal(t, 1, len(msgs))
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for lambda invocation")
	}
}

type stubKafkaClientSource struct {
	events chan kafka.Event
}

func newStubKafkaClientSource() *stubKafkaClientSource {
	return &stubKafkaClientSource{
		events: make(chan kafka.Event, 16),
	}
}

func (s *stubKafkaClientSource) Emit(msg *kafka.Message) {
	s.events <- msg
}

func (s *stubKafkaClientSource) NewClient() kafka.KafkaConsumerClient {
	return &stubKafkaClient{
		source: s,
	}
}

type stubKafkaClient struct {
	source *stubKafkaClientSource
}

func (c *stubKafkaClient) Poll(ms int) kafka.Event {
	select {
	case ev := <-c.source.events:
		return ev
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}

func (c *stubKafkaClient) Close() error {
	return nil
}

func (c *stubKafkaClient) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return []kafka.TopicPartition{}, nil
}
