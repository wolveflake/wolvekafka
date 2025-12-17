package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"wolvekafka/go/common/logger"
)

const (
	defaultMessageCount = 10
	maxMessageCount     = 200
	defaultInterval     = 500 * time.Millisecond
	defaultMessage      = "Hello from the WolveKafka demo!"
)

type demoProducerHandler struct {
	defaultBootstrap string
}

type demoProducerRequest struct {
	Topic            string  `json:"topic"`
	BootstrapServers string  `json:"bootstrap_servers"`
	Count            int     `json:"count"`
	IntervalSeconds  float64 `json:"interval_seconds"`
	Message          string  `json:"message"`
}

type demoProducerResponse struct {
	Produced int              `json:"produced"`
	Results  []produceSummary `json:"results"`
}

type produceSummary struct {
	DemoID    string `json:"demo_id"`
	Sequence  int    `json:"sequence"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func newDemoProducerHandler() http.HandlerFunc {
	handler := &demoProducerHandler{defaultBootstrap: strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))}
	return handler.handle
}

func (h *demoProducerHandler) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req demoProducerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.S().Warnf("demo producer: invalid payload: %v", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	resp, err := h.produce(r.Context(), req)
	if err != nil {
		logger.S().Warnf("demo producer failed: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.S().Warnf("demo producer: failed to encode response: %v", err)
	}
}

func (h *demoProducerHandler) produce(ctx context.Context, req demoProducerRequest) (*demoProducerResponse, error) {
	bootstrap := strings.TrimSpace(req.BootstrapServers)
	if bootstrap == "" {
		bootstrap = h.defaultBootstrap
	}
	if bootstrap == "" {
		return nil, errors.New("bootstrap_servers missing")
	}
	if req.Topic == "" {
		return nil, errors.New("topic is required")
	}

	count := req.Count
	if count <= 0 {
		count = defaultMessageCount
	}
	if count > maxMessageCount {
		return nil, fmt.Errorf("count exceeds limit (%d)", maxMessageCount)
	}

	interval := defaultInterval
	if req.IntervalSeconds > 0 {
		interval = time.Duration(req.IntervalSeconds * float64(time.Second))
	}

	message := req.Message
	if message == "" {
		message = defaultMessage
	}

	brokers := splitBrokers(bootstrap)
	if len(brokers) == 0 {
		return nil, errors.New("no bootstrap brokers provided")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("init kafka producer: %w", err)
	}
	defer client.Close()

	results := make([]produceSummary, 0, count)
	for i := 0; i < count; i++ {
		payload := newDemoPayload(message, i+1)
		value, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}

		record := kgo.Record{Topic: req.Topic, Value: value}
		if err := client.ProduceSync(ctx, &record).FirstErr(); err != nil {
			return nil, fmt.Errorf("produce message: %w", err)
		}

		results = append(results, produceSummary{
			DemoID:    payload.DemoID,
			Sequence:  payload.Sequence,
			Partition: int32(record.Partition),
			Offset:    int64(record.Offset),
		})

		if i+1 < count && interval > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(interval):
			}
		}
	}

	return &demoProducerResponse{Produced: len(results), Results: results}, nil
}

func newDemoPayload(message string, seq int) demoPayload {
	return demoPayload{
		DemoID:    uuid.NewString(),
		Sequence:  seq,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

type demoPayload struct {
	DemoID    string `json:"demo_id"`
	Sequence  int    `json:"sequence"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

func splitBrokers(bootstrap string) []string {
	parts := strings.Split(bootstrap, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if host := strings.TrimSpace(part); host != "" {
			brokers = append(brokers, host)
		}
	}
	return brokers
}
