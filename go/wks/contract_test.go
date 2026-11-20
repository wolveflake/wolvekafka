// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	model_pb "wolvekafka/proto/wolvekafka/handler/v1"
)

// The JSON sent to HTTP handlers is a public contract. A change here breaks
// deployed handlers, so it must be deliberate (see handler.proto).
func TestHandlerRequestJSONIsPinned(t *testing.T) {
	req := &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{
		{
			Topic: "orders", Partition: 3, Offset: 42,
			Headers: []*model_pb.Header{{Key: "traceparent", Value: []byte("00-abc")}},
			Key:     []byte("k1"), Value: []byte(`{"id":1}`),
		},
		{Topic: "orders", Partition: 3, Offset: 43},
	}}
	payload, err := ConvertLambdaRequestToJson(req)
	require.NoError(t, err)
	var compact bytes.Buffer
	require.NoError(t, json.Compact(&compact, payload))
	require.Equal(t,
		`{"messages":[`+
			`{"topic":"orders","partition":3,"offset":"42","headers":[{"key":"traceparent","value":"MDAtYWJj"}],"key":"azE=","value":"eyJpZCI6MX0=","encoding":0},`+
			`{"topic":"orders","partition":3,"offset":"43","headers":[],"key":"","value":"","encoding":0}`+
			`]}`,
		compact.String())
}

func TestHandlerResponseJSONIsPinned(t *testing.T) {
	resp, err := ParseLambdaResponse([]byte(`{"messages":[{"status":2},{"status":1,"message":"eyJvayI6ZmFsc2V9","encoding":1},{"status":0}]}`))
	require.NoError(t, err)
	require.Equal(t, model_pb.ResponseStatus_STATUS_SUCCESS, resp.Messages[0].GetStatus())
	require.Equal(t, model_pb.ResponseStatus_STATUS_FAILED, resp.Messages[1].GetStatus())
	require.Equal(t, []byte(`{"ok":false}`), resp.Messages[1].GetMessage())
	require.Equal(t, model_pb.MessageEncoding_ENCODING_JSON, resp.Messages[1].GetEncoding())
	require.Equal(t, model_pb.ResponseStatus_STATUS_RETRY, resp.Messages[2].GetStatus())
}

// gRPC handlers are addressed by this method name, compiled into their code.
func TestHandlerServiceMethodNameIsPinned(t *testing.T) {
	services := model_pb.File_proto_wolvekafka_handler_v1_handler_proto.Services()
	require.Equal(t, 1, services.Len())
	method := services.Get(0).Methods().ByName("Handle")
	require.NotNil(t, method)
	require.Equal(t, "wolvekafka.handler.v1.HandlerService.Handle", string(method.FullName()))
	require.Equal(t, "wolvekafka.handler.v1.LambdaRequest", string(method.Input().FullName()))
	require.Equal(t, "wolvekafka.handler.v1.LambdaResponse", string(method.Output().FullName()))
}
