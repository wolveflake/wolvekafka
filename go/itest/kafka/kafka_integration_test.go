// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kafka_itest "wolvekafka/go/itest/fixture"
)

func TestKafkaStartupAndConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fixture := kafka_itest.NewDockerFixture(t)
	fixture.Start(ctx)

	require.NoError(t, fixture.VerifyConnection())
}
