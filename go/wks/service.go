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

	"github.com/dropbox/godropbox/executor"
	"go.uber.org/zap"
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

	mu        sync.Mutex   // one config update at a time.
	consumers atomic.Value // map[string]*consumerRef

	sendEventFunc   sendEventFunc
	newConsumerFunc func(ctx context.Context, params ConsumerParams) (*Consumer, error)
}

// WolveKafkaParams holds parameters for WKS service.
type WolveKafkaParams struct {
	Config *configs.Config

	// event sending function (mostly for testing purposes).
	sendEventFunc sendEventFunc
	// function to create new consumer (mostly for testing purposes).
	newConsumerFunc func(ctx context.Context, params ConsumerParams) (*Consumer, error)
}

type consumerRef struct {
	jobs     []*ConsumerJob
	executor *executor.WorkPoolExecutor
}

// NewWolveKafkaService creates a new WKS service instance.
func NewWolveKafkaService(ctx context.Context, params WolveKafkaParams) (*WolveKafkaService, error) {
	srvCtx, cancelFunc := context.WithCancel(ctx)

	// set default functions if not provided.
	newConsumerFunc := NewConsumer
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
		newConsumerFunc: newConsumerFunc,
		sendEventFunc:   sendEventFunc,
	}
	s.consumers.Store(make(map[string]*consumerRef))

	// initial configuration.
	s.updateConfig(params.Config)

	return s, nil
}

// updateConfig updates the service configuration.
func (w *WolveKafkaService) updateConfig(config *configs.Config) {
	w.mu.Lock()
	defer w.mu.Unlock()

	currentConsumers := w.consumers.Load().(map[string]*consumerRef)
	newConsumers := make(map[string]*consumerRef)

	for _, consumerCfg := range config.Consumers() {
		consumerName := consumerCfg.GetGroupId()
		if existingConsumer, exists := currentConsumers[consumerName]; exists {
			newConsumers[consumerName] = existingConsumer
			delete(currentConsumers, consumerName)
			// TODO(13): check if configuration has changed and update if necessary.
			continue
		}

		// create new consumer.
		w.log.Infof("creating new consumer: %s", consumerName)
		workPoolParams := executor.WorkPoolExecutorParams{
			NumWorkers: defaultNumWorkers,
		}
		workPool := executor.NewWorkPoolExecutor()
		err := workPool.Configure(workPoolParams)
		if err != nil {
			w.log.Errorf("failed to configure work pool for consumer %s: %s", consumerName, err)
		} else {
			jobs := make([]*ConsumerJob, workPoolParams.NumWorkers)
			// generate consumer job and submit to work pool
			w.log.Infof("starting workers: %s, size: %v", consumerName, workPoolParams.NumWorkers)
			for idx := 0; idx < workPoolParams.NumWorkers; idx++ {
				jobCtx, jobCtxCancel := context.WithCancel(w.ctx)
				consumerParams := ConsumerParams{
					Name:         consumerName,
					Cluster:      config.Cluster(),
					PoolDuration: time.Millisecond * 500,
					Topics:       consumerCfg.GetTopics(),
				}
				kafkaConsumer, err := w.newConsumerFunc(jobCtx, consumerParams)
				if err != nil {
					w.log.Errorf("failed to create kafka consumer for %s: %s", consumerName, err)
					continue
				}
				job := &ConsumerJob{
					ctx:       jobCtx,
					ctxCancel: jobCtxCancel,
					// consumer name.
					name: fmt.Sprintf("%s-%d", consumerName, idx),
					// kafka consumer instance.
					consumer: kafkaConsumer,
					// callback function to send events.
					sendEventFunc: w.sendEventFunc,
					// logger instance.
					log: w.log,
				}
				workPool.Process(job)
				jobs[idx] = job
			}
			newConsumers[consumerName] = &consumerRef{
				jobs:     jobs,
				executor: workPool,
			}
		}
	}

	// Shutdown removed consumers.
	for consumerName, exec := range currentConsumers {
		w.log.Infof("stopping consumer: %s, jobs count: %v", consumerName, len(exec.jobs))
		w.stopConsumerJobs(exec)
		exec.executor.Configure(executor.WorkPoolExecutorParams{
			NumWorkers: 0,
		})
	}

	w.consumers.Store(newConsumers)
}

// stopConsumerJobs stops all jobs for a given consumer.
func (w *WolveKafkaService) stopConsumerJobs(consumerRef *consumerRef) {
	for _, job := range consumerRef.jobs {
		job.ctxCancel()
	}
}
