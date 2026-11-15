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
