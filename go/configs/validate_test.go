// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	configs_pb "wolvekafka/proto/configs"
)

func TestValidateLambdaTimeout(t *testing.T) {
	consumer := func(timeoutMs uint32) []*configs_pb.Consumer {
		return []*configs_pb.Consumer{{
			GroupId: "orders",
			Topics:  []string{"orders"},
			Lambdas: []*configs_pb.Lambda{{Url: "http://handler/", TimeoutMs: timeoutMs}},
		}}
	}
	assert.NoError(t, ValidateConsumers(consumer(0)), "omitted timeout uses the default")
	assert.NoError(t, ValidateConsumers(consumer(MaxLambdaTimeoutMs)))
	assert.ErrorContains(t, ValidateConsumers(consumer(MaxLambdaTimeoutMs+1)), "exceeds the maximum")
}

func TestValidateGRPCLambdaURL(t *testing.T) {
	consumer := func(url string) []*configs_pb.Consumer {
		return []*configs_pb.Consumer{{
			GroupId: "orders",
			Topics:  []string{"orders"},
			Lambdas: []*configs_pb.Lambda{{Url: url}},
		}}
	}
	for _, ok := range []string{"grpc://handler:9090", "grpcs://handler.example.com:443", "GRPC://10.0.0.1:9090/", "http://handler:8080/events"} {
		assert.NoError(t, ValidateConsumers(consumer(ok)), ok)
	}
	for _, bad := range []string{"grpc://handler", "grpc://handler:9090/events", "grpcs://user:pw@handler:443", "grpc://handler:9090?x=1", "grpc://:9090"} {
		assert.Error(t, ValidateConsumers(consumer(bad)), bad)
	}
}
