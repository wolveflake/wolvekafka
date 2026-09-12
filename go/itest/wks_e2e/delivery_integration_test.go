package wks_e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"wolvekafka/go/configs"
	kafka_fixture "wolvekafka/go/itest/fixture"
	"wolvekafka/go/wks"
	configs_pb "wolvekafka/proto/configs"
)

// Exercise the public HTTP path with a real broker. Waiting longer than the
// default auto-commit interval makes the original premature-commit bug visible.
func TestFailedInputIsNotSkippedAndReplaysAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := kafka_fixture.NewDockerFixture(t)
	fixture.Start(ctx)
	client, err := kgo.NewClient(kgo.SeedBrokers(fixture.BootstrapHost()))
	require.NoError(t, err)
	defer client.Close()
	admin := kadm.NewClient(client)
	topic, group := fmt.Sprintf("delivery-%d", time.Now().UnixNano()), fmt.Sprintf("group-%d", time.Now().UnixNano())
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, created.Error())
	var healthy atomic.Bool
	observed := make(chan int64, 64)
	handler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		req, err := wks.ParseLambdaRequestJson(body)
		if err != nil || len(req.GetMessages()) != 1 {
			http.Error(w, "bad batch", 400)
			return
		}
		offset := req.Messages[0].GetOffset()
		observed <- offset
		if offset == 0 && !healthy.Load() {
			io.WriteString(w, `{"messages":[{"status":0}]}`)
			return
		}
		io.WriteString(w, `{"messages":[{"status":2}]}`)
	}))
	defer handler.Close()
	cfg, err := configs.NewConfigFromProto(&configs_pb.Config{
		Cluster: &configs_pb.Cluster{BootstrapServers: []string{fixture.BootstrapHost()}},
		Consumers: []*configs_pb.Consumer{{GroupId: group, Topics: []string{topic}, Lambdas: []*configs_pb.Lambda{{Url: handler.URL}},
			ConsumerType: &configs_pb.Consumer_Parallel{Parallel: &configs_pb.ParallelConsumer{Parallelism: 1}},
			RetryPolicy:  &configs_pb.RetryPolicy{RetryPolicy: &configs_pb.RetryPolicy_ExponentialBackoff{ExponentialBackoff: &configs_pb.ExponentialBackoff{InitialBackoffMs: 100, MaxBackoffMs: 1000, Multiplier: 2}}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("first")}, &kgo.Record{Topic: topic, Value: []byte("second")}).FirstErr())
	service, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	defer service.Close()
	select {
	case offset := <-observed:
		require.EqualValues(t, 0, offset)
	case <-time.After(30 * time.Second):
		t.Fatal("handler not invoked")
	}
	// The second message must not pass the failed first message, even after
	// the default 5-second automatic commit interval would have elapsed.
	timer := time.NewTimer(6 * time.Second)
	waiting := true
	for waiting {
		select {
		case offset := <-observed:
			require.EqualValues(t, 0, offset)
		case <-timer.C:
			waiting = false
		}
	}
	offsets, err := admin.FetchOffsets(ctx, group)
	require.NoError(t, err)
	if offset, ok := offsets.Lookup(topic, 0); ok {
		require.LessOrEqual(t, offset.At, int64(0), "failed input must not be committed")
	}
	require.NoError(t, service.Close())
	for len(observed) > 0 {
		<-observed
	}
	healthy.Store(true)
	restarted, err := wks.NewWolveKafkaService(ctx, wks.WolveKafkaParams{Config: cfg})
	require.NoError(t, err)
	defer restarted.Close()
	for _, expected := range []int64{0, 1} {
		select {
		case offset := <-observed:
			require.Equal(t, expected, offset)
		case <-time.After(30 * time.Second):
			t.Fatal("failed input was not replayed in order")
		}
	}
	require.Eventually(t, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		offset, ok := offsets.Lookup(topic, 0)
		return ok && offset.At == 2
	}, 10*time.Second, 100*time.Millisecond)

	// The same worker must resume after a broker restart without rewinding its
	// successful commits or skipping the next record.
	fixture.Restart(ctx)
	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("after broker restart")}).FirstErr())
	select {
	case offset := <-observed:
		require.EqualValues(t, 2, offset)
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not rejoin after broker restart")
	}
	require.Eventually(t, func() bool {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		offset, ok := offsets.Lookup(topic, 0)
		return ok && offset.At == 3
	}, 10*time.Second, 100*time.Millisecond)
}
