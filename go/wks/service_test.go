///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2024 [wolveflake LLC], All Rights Reserved.
///
/// NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical concepts contained
/// herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
/// Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
/// from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees, managers or contractors who have executed
/// Confidentiality and Non-disclosure agreements explicitly covering such access.
///
/// The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
/// information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
/// OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
/// LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
/// TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
///

package wks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"

	"wolvekafka/go/configs"
	model_pb "wolvekafka/proto/model"
)

func TestBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assert.NotNil(t, ctx)

	// start a test HTTP server to act as a lambda endpoint
	lambdaReqChan := make(chan []*kafka.Message, 1)

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
				"retry_policy": {
					"max_retries": 3,
					"initial_backoff_ms": 100,
					"max_backoff_ms": 1000,
					"drop": {}
				},
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

	msgChan := make(chan []*kafka.Message, 1)
	pollFunc := func() (chan []*kafka.Message, error) {
		return msgChan, nil
	}
	wks, err := NewWolveKafkaService(ctx, WolveKafkaParams{
		Config: cfg,
		newConsumerFunc: func(ctx context.Context, params ConsumerParams) (*Consumer, error) {
			return newConsumer(ctx, ConsumerParams{
				Cluster:  cfg.Cluster(),
				pollFunc: pollFunc,
				commitMessageFunc: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
					return []kafka.TopicPartition{}, nil
				},
			}), nil
		},
		sendEventFunc: func(c *ProcessingJob, ctx context.Context, msgs []*kafka.Message) error {
			lambdaReqChan <- msgs
			return nil
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, wks)

	topic := "topic1"
	// send a test message
	testMsg := &kafka.Message{

		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
		Value: []byte("test message"),
	}
	msgChan <- []*kafka.Message{testMsg}

	// wait for the lambda to be invoked
	select {
	case msgs := <-lambdaReqChan:
		assert.NotNil(t, msgs)
		assert.Equal(t, 1, len(msgs))
		assert.Equal(t, "test message", string(msgs[0].Value))
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
				"retry_policy": {
					"max_retries": 3,
					"initial_backoff_ms": 100,
					"max_backoff_ms": 1000,
					"drop": {}
				},
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

	msgChan := make(chan []*kafka.Message, 1)
	pollFunc := func() (chan []*kafka.Message, error) {
		return msgChan, nil
	}
	wks, err := NewWolveKafkaService(ctx, WolveKafkaParams{
		Config: cfg,
		newConsumerFunc: func(ctx context.Context, params ConsumerParams) (*Consumer, error) {
			return newConsumer(ctx, ConsumerParams{
				Cluster:  cfg.Cluster(),
				pollFunc: pollFunc,
				commitMessageFunc: func(msg *kafka.Message) ([]kafka.TopicPartition, error) {
					return []kafka.TopicPartition{}, nil
				},
			}), nil
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, wks)

	topic := "topic1"
	// send a test message
	testMsg := &kafka.Message{

		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
		Value: []byte("test message"),
	}
	msgChan <- []*kafka.Message{testMsg}

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
