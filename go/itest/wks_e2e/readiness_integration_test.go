// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks_e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"wolvekafka/go/configs"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/wks"
	configs_pb "wolvekafka/proto/configs"
)

func readinessService(t *testing.T, ctx context.Context, broker, group string, workers uint32) *wks.WolveKafkaService {
	t.Helper()
	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster: &configs_pb.Cluster{BootstrapServers: []string{broker}},
		Consumers: []*configs_pb.Consumer{{
			GroupId:      group,
			Topics:       []string{group + "-in"},
			Lambdas:      []*configs_pb.Lambda{{Url: "http://127.0.0.1:1/"}},
			ConsumerType: &configs_pb.Consumer_Parallel{Parallel: &configs_pb.ParallelConsumer{Parallelism: workers}},
		}},
	})
	require.NoError(t, err)
	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close() })
	return service
}

// Readiness turns true only after every worker joins the group, including
// workers that get no partitions, and stays false when Kafka is unreachable.
func TestReadinessReflectsGroupMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	unreachable := readinessService(t, ctx, "127.0.0.1:1", fmt.Sprintf("ready-down-%d", time.Now().UnixNano()), 2)

	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)
	group := fmt.Sprintf("ready-%d", time.Now().UnixNano())
	createTestTopics(t, ctx, fixture.BootstrapHost(), group+"-in")
	service := readinessService(t, ctx, fixture.BootstrapHost(), group, 3)

	require.Eventually(t, func() bool {
		ready, consumers := service.Readiness()
		return ready && consumers[group].ReadyWorkers == 3
	}, 60*time.Second, 250*time.Millisecond, "all three workers join, though the topic has one partition")

	require.Eventually(t, func() bool {
		ready, consumers := unreachable.Readiness()
		for _, c := range consumers {
			if !ready && !c.Ready && strings.Contains(c.Reason, "connect to 127.0.0.1:1") {
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "an unreachable cluster is not ready, and the reason names the broker")
}
