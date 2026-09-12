///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2025 [wolveflake LLC], All Rights Reserved.
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
	"sync"
	"sync/atomic"
	"time"
	"wolvekafka/go/common/logger"
	"wolvekafka/go/configs"
	"wolvekafka/go/kafka"

	"github.com/dropbox/godropbox/executor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	configs_pb "wolvekafka/proto/configs"
)

const (
	defaultNumWorkers = 10
)

type WolveKafkaService struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// service parameters.
	params *WolveKafkaParams

	// logger.
	log *zap.SugaredLogger

	metrics *instrumentation

	updateMu  sync.Mutex // serialize updates without blocking Close during teardown.
	mu        sync.Mutex
	consumers atomic.Value // map[string]*consumerRef

	sendEventFunc   sendEventFunc
	newConsumerFunc func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error)
}

// WolveKafkaParams holds parameters for WKS service.
type WolveKafkaParams struct {
	Config *configs.Config

	// event sending function (mostly for testing purposes).
	sendEventFunc sendEventFunc
	// function to create new consumer (mostly for testing purposes).
	newConsumerFunc func(ctx context.Context, params kafka.ConsumerParams) (*kafka.Consumer, error)
}

type consumerRef struct {
	jobs        []*ProcessingJob
	executor    *executor.WorkPoolExecutor
	consumerCfg *configs_pb.Consumer
	clusterCfg  *configs_pb.Cluster
	wait        *sync.WaitGroup
}

// NewWolveKafkaService creates a new WKS service instance.
func NewWolveKafkaService(ctx context.Context, params WolveKafkaParams) (*WolveKafkaService, error) {
	srvCtx, cancelFunc := context.WithCancel(ctx)
	metrics := getInstrumentation()

	// set default functions if not provided.
	newConsumerFunc := kafka.NewConsumer
	if params.newConsumerFunc != nil {
		logger.S().Infof("using new consumer callback function provided in params...")
		newConsumerFunc = params.newConsumerFunc
	}
	sendEventFunc := defaultSendEventFunc
	if params.sendEventFunc != nil {
		logger.S().Infof("using send event callback function provided in params...")
		sendEventFunc = params.sendEventFunc
	}
	s := &WolveKafkaService{
		ctx:             srvCtx,
		ctxCancel:       cancelFunc,
		log:             logger.S(),
		params:          &params,
		metrics:         metrics,
		newConsumerFunc: newConsumerFunc,
		sendEventFunc:   sendEventFunc,
	}
	s.consumers.Store(make(map[string]*consumerRef))

	// initial configuration.
	if err := s.updateConfig(params.Config); err != nil {
		cancelFunc()
		return nil, err
	}

	return s, nil
}

// updateConfig updates the service configuration.
func (w *WolveKafkaService) updateConfig(config *configs.Config) error {
	w.updateMu.Lock()
	defer w.updateMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	if config == nil {
		return fmt.Errorf("configuration is required")
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if _, err := kafka.ConnectionOptions(config.Cluster()); err != nil {
		return err
	}
	clusterCfg := proto.Clone(config.Cluster()).(*configs_pb.Cluster)

	if w.metrics != nil {
		w.metrics.recordConfigApplied(w.ctx, len(config.Consumers()))
	}

	currentConsumers := make(map[string]*consumerRef)
	for name, ref := range w.consumers.Load().(map[string]*consumerRef) {
		currentConsumers[name] = ref
	}
	newConsumers := make(map[string]*consumerRef)
	desired := make(map[string]*configs_pb.Consumer)
	for _, cfg := range config.Consumers() {
		desired[cfg.GetGroupId()] = cfg
	}
	var stopping []*consumerRef
	for name, ref := range currentConsumers {
		if !proto.Equal(ref.consumerCfg, desired[name]) || !proto.Equal(ref.clusterCfg, clusterCfg) {
			stopping = append(stopping, ref)
		}
	}
	w.mu.Unlock()
	err := stopConsumers(stopping, 5*time.Second)
	w.mu.Lock()
	if err != nil {
		return err
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}

	for _, consumerCfg := range config.Consumers() {
		consumerCfg = proto.Clone(consumerCfg).(*configs_pb.Consumer)
		consumerName := consumerCfg.GetGroupId()
		if existingConsumer, exists := currentConsumers[consumerName]; exists {
			if proto.Equal(existingConsumer.consumerCfg, consumerCfg) && proto.Equal(existingConsumer.clusterCfg, clusterCfg) {
				// Config unchanged — keep existing consumer.
				newConsumers[consumerName] = existingConsumer
				delete(currentConsumers, consumerName)
				continue
			}
			// Config changed — stop old consumer so it gets recreated below.
			w.log.Infof("config changed for consumer %s, recreating", consumerName)
			existingConsumer.executor.Configure(executor.WorkPoolExecutorParams{NumWorkers: 0})
			delete(currentConsumers, consumerName)
		}

		// create new consumer.
		w.log.Infof("creating new consumer: %s", consumerName)
		numWorkers := defaultNumWorkers
		if p := consumerCfg.GetParallel(); p != nil && p.GetParallelism() > 0 {
			numWorkers = int(p.GetParallelism())
		}
		workPoolParams := executor.WorkPoolExecutorParams{
			NumWorkers: numWorkers,
		}
		workPool := executor.NewWorkPoolExecutor()
		err := workPool.Configure(workPoolParams)
		if err != nil {
			w.log.Errorf("failed to configure work pool for consumer %s: %s", consumerName, err)
		} else {
			jobs := make([]*ProcessingJob, workPoolParams.NumWorkers)
			wait := &sync.WaitGroup{}
			// generate consumer job and submit to work pool
			w.log.Infof("starting workers: %s, size: %v", consumerName, workPoolParams.NumWorkers)
			for idx := 0; idx < workPoolParams.NumWorkers; idx++ {
				jobCtx, jobCtxCancel := context.WithCancel(w.ctx)
				job := &ProcessingJob{
					ctx:       jobCtx,
					ctxCancel: jobCtxCancel,
					// consumer name.
					consumerName: consumerCfg.GetGroupId(),
					// mostly for logging.
					jobId: idx,
					// callback function to send events.
					sendEventFunc: w.sendEventFunc,
					// logger instance.
					log:         w.log,
					consumerCfg: consumerCfg,
					clusterCfg:  clusterCfg,

					// output policy.
					outputPolicy: NewOutputPolicy(OutputPolicyParams{
						ConsumerCfg: consumerCfg,
					}),

					newConsumerFunc: w.newConsumerFunc,
					metrics:         w.metrics,
				}
				workPool.ProcessWithWaitGroup(job, wait)
				jobs[idx] = job
			}
			if w.metrics != nil {
				w.metrics.recordConsumerLifecycle(w.ctx, consumerName, 1)
			}
			newConsumers[consumerName] = &consumerRef{
				jobs:        jobs,
				executor:    workPool,
				consumerCfg: consumerCfg,
				clusterCfg:  clusterCfg,
				wait:        wait,
			}
		}
	}

	// Shutdown removed consumers.
	for consumerName, exec := range currentConsumers {
		w.log.Infof("stopping consumer: %s, jobs count: %v", consumerName, len(exec.jobs))
		if w.metrics != nil {
			w.metrics.recordConsumerLifecycle(w.ctx, consumerName, -1)
		}
		exec.executor.Configure(executor.WorkPoolExecutorParams{
			NumWorkers: 0,
		})
	}

	w.consumers.Store(newConsumers)
	return nil
}

// Cancel every group before waiting, with one shared teardown budget.
func stopConsumers(refs []*consumerRef, timeout time.Duration) error {
	if len(refs) == 0 {
		return nil
	}
	for _, ref := range refs {
		for _, job := range ref.jobs {
			job.ctxCancel()
		}
	}
	done := make(chan struct{})
	go func() {
		for _, ref := range refs {
			if ref.wait != nil {
				ref.wait.Wait()
			}
		}
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("consumer shutdown exceeded %v; refusing to start overlapping workers", timeout)
	}
}

// Close stops all managed consumers and releases resources.
func (w *WolveKafkaService) Close() error {
	if w == nil {
		return nil
	}
	if w.ctxCancel != nil {
		w.ctxCancel()
	}
	w.mu.Lock()
	consumers := w.consumers.Load().(map[string]*consumerRef)
	w.mu.Unlock()
	var refs []*consumerRef
	for _, ref := range consumers {
		if ref == nil {
			continue
		}
		refs = append(refs, ref)
		if ref.executor != nil {
			_ = ref.executor.Configure(executor.WorkPoolExecutorParams{NumWorkers: 0})
		}
	}
	return stopConsumers(refs, 5*time.Second)
}
