package wks

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"wolvekafka/go/configs"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
)

func reloadTestService(t *testing.T, cfg *configs.Config) *WolveKafkaService {
	t.Helper()
	source := newStubKafkaClientSource()
	s, err := NewWolveKafkaService(context.Background(), WolveKafkaParams{Config: cfg,
		newConsumerFunc: func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error) {
			params.Client = source.NewClient()
			params.PollDuration = time.Millisecond
			return kafka.NewConsumer(ctx, params)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func reloadTestConfig(t *testing.T, broker string) *configs.Config {
	t.Helper()
	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster:   &configs_pb.Cluster{BootstrapServers: []string{broker}},
		Consumers: []*configs_pb.Consumer{{GroupId: "g", Topics: []string{"in"}, Lambdas: []*configs_pb.Lambda{{Url: "http://localhost"}}, ConsumerType: &configs_pb.Consumer_Parallel{Parallel: &configs_pb.ParallelConsumer{Parallelism: 1}}}},
	})
	require.NoError(t, err)
	return cfg
}

func TestClusterOnlyReloadReplacesConsumers(t *testing.T) {
	cfg := reloadTestConfig(t, "old:9092")
	s := reloadTestService(t, cfg)
	before := s.consumers.Load().(map[string]*consumerRef)["g"]
	require.NoError(t, s.updateConfig(cfg))
	require.Same(t, before, s.consumers.Load().(map[string]*consumerRef)["g"])
	updated := reloadTestConfig(t, "new:9092")
	require.NoError(t, s.updateConfig(updated))
	after := s.consumers.Load().(map[string]*consumerRef)["g"]
	require.NotSame(t, before, after)
	require.ErrorIs(t, before.jobs[0].ctx.Err(), context.Canceled)
	require.Equal(t, []string{"new:9092"}, after.jobs[0].clusterCfg.BootstrapServers)
	updated.Cluster().SaslUsername = "user"
	updated.Cluster().SaslPassword = "password"
	require.NoError(t, s.updateConfig(updated))
	require.NotSame(t, after, s.consumers.Load().(map[string]*consumerRef)["g"])
	current := s.consumers.Load().(map[string]*consumerRef)["g"]
	updated.Cluster().Tls = &configs_pb.TLS{Enabled: true, CaFile: "/missing/ca.pem"}
	require.Error(t, s.updateConfig(updated))
	require.Same(t, current, s.consumers.Load().(map[string]*consumerRef)["g"])
}

func TestWatcherFollowsSymlinkAndKeepsLastValidConfig(t *testing.T) {
	dir := t.TempDir()
	first, second, link := filepath.Join(dir, "first"), filepath.Join(dir, "second"), filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(first, []byte(`{"cluster":{"bootstrap_servers":["old:9092"]},"consumers":[]}`), 0600))
	require.NoError(t, os.Symlink(first, link))
	cfg, err := LoadConfigFromFile(link)
	require.NoError(t, err)
	s := reloadTestService(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); s.WatchConfig(ctx, link, 5*time.Millisecond) }()
	require.NoError(t, os.WriteFile(second, []byte(`{"cluster":{"bootstrap_servers":["new:9092"]},"consumers":[{"group_id":"new","topics":["in"],"lambdas":[{"url":"http://localhost"}],"parallel":{"parallelism":1}}]}`), 0600))
	info, err := os.Stat(first)
	require.NoError(t, err)
	require.NoError(t, os.Chtimes(second, info.ModTime(), info.ModTime()))
	replacement := filepath.Join(dir, "replacement")
	require.NoError(t, os.Symlink(second, replacement))
	require.NoError(t, os.Rename(replacement, link))
	require.Eventually(t, func() bool { return s.consumers.Load().(map[string]*consumerRef)["new"] != nil }, time.Second, 5*time.Millisecond)
	ref := s.consumers.Load().(map[string]*consumerRef)["new"]
	require.NoError(t, os.WriteFile(second, []byte(`{"broken":`), 0600))
	require.Never(t, func() bool { return s.consumers.Load().(map[string]*consumerRef)["new"] != ref }, 30*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, s.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher survived service shutdown")
	}
}
