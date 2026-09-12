package wks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"wolvekafka/go/kafka"
	configs_pb "wolvekafka/proto/configs"
	model_pb "wolvekafka/proto/model"
)

type deliveryStub struct {
	events   chan kafka.Event
	commits  chan int64
	attempts int
}

func (s *deliveryStub) Poll(ms int) kafka.Event {
	select {
	case msg := <-s.events:
		return msg
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}
func (s *deliveryStub) Close() error { return nil }
func (s *deliveryStub) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	s.attempts++
	s.commits <- msg.Offset
	if s.attempts == 1 {
		return nil, fmt.Errorf("commit unavailable")
	}
	return nil, nil
}

func TestJobHoldsBatchAcrossHandlerOutputAndCommitFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stub := &deliveryStub{events: make(chan kafka.Event, 2), commits: make(chan int64, 4)}
	stub.events <- &kafka.Message{Topic: "in", Offset: 1}
	stub.events <- &kafka.Message{Topic: "in", Offset: 2}
	cluster := &configs_pb.Cluster{BootstrapServers: []string{"localhost:9092"}}
	cfg := &configs_pb.Consumer{GroupId: "g", Topics: []string{"in"}, Lambdas: []*configs_pb.Lambda{{Url: "http://localhost"}}, OutputPolicy: &configs_pb.Consumer_Kafka{Kafka: &configs_pb.OutputKafkaPolicy{Topic: "out"}}}
	producer, err := kafka.NewProducer(ctx, kafka.ProducerParams{Name: "out", Cluster: cluster})
	require.NoError(t, err)
	outputAttempts := make(chan struct{}, 4)
	releaseOutput := make(chan struct{})
	outputs := 0
	producer.ProducerFunc = func(*kafka.Message) error {
		outputs++
		outputAttempts <- struct{}{}
		if outputs == 1 {
			return fmt.Errorf("output unavailable")
		}
		select {
		case <-releaseOutput:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	sent := make(chan int64, 4)
	sends := 0
	job := &ProcessingJob{ctx: ctx, ctxCancel: cancel, consumerName: "g", log: zap.NewNop().Sugar(), consumerCfg: cfg, clusterCfg: cluster,
		outputPolicy: NewOutputPolicy(OutputPolicyParams{ConsumerCfg: cfg, KafkaProducer: producer}),
		newConsumerFunc: func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error) {
			params.Client = stub
			return kafka.NewConsumer(ctx, params)
		},
		sendEventFunc: func(_ *ProcessingJob, _ context.Context, req *model_pb.LambdaRequest) (*BatchResult, error) {
			sends++
			sent <- req.Messages[0].Offset
			if sends == 1 {
				return nil, fmt.Errorf("handler unavailable")
			}
			return &BatchResult{Request: req, Response: &model_pb.LambdaResponse{Messages: []*model_pb.ResponseMessage{{Status: model_pb.ResponseStatus_STATUS_SUCCESS, Message: []byte("out")}}}}, nil
		},
	}
	done := make(chan struct{})
	go func() { defer close(done); job.Execute() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("job did not stop")
		}
	})
	for i := 0; i < 2; i++ {
		select {
		case offset := <-sent:
			require.EqualValues(t, 1, offset)
		case <-ctx.Done():
			t.Fatal("handler was not retried")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-outputAttempts:
		case <-ctx.Done():
			t.Fatal("output was not retried")
		}
	}
	select {
	case <-sent:
		t.Fatal("advanced input or repeated successful handler while output failed")
	default:
	}
	select {
	case <-stub.commits:
		t.Fatal("committed before output succeeded")
	default:
	}
	close(releaseOutput)
	for i := 0; i < 2; i++ {
		select {
		case offset := <-stub.commits:
			require.EqualValues(t, 1, offset)
		case <-ctx.Done():
			t.Fatal("commit was not retried")
		}
	}
	select {
	case offset := <-sent:
		require.EqualValues(t, 2, offset)
	case <-ctx.Done():
		t.Fatal("next input did not resume")
	}
}
