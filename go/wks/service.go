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
	"sync"
	"sync/atomic"
	"time"
	"wolvekafka/go/common/logger"
	"wolvekafka/go/configs"

	"github.com/dropbox/godropbox/executor"
	"go.uber.org/zap"
)

const (
	defaultNumWorkers   = 10
	defaultQueueTimeout = time.Second * 10
)

type WolveKafkaService struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// service parameters.
	params *WolveKafkaParams

	// logger.
	log *zap.SugaredLogger

	mu        sync.Mutex   // one update at a time.
	consumers atomic.Value // map[string]*executor.WorkPoolExecutor
}

// WolveKafkaParams holds parameters for WKS service.
type WolveKafkaParams struct {
	config *configs.Config

	// event sending function (mostly for testing purposes).
	sendEventFunc sendEventFunc
}

// NewWolveKafkaService creates a new WKS service instance.
func NewWolveKafkaService(ctx context.Context, params WolveKafkaParams) (*WolveKafkaService, error) {
	srvCtx, cancelFunc := context.WithCancel(ctx)

	s := &WolveKafkaService{
		ctx:       srvCtx,
		ctxCancel: cancelFunc,
		log:       logger.S(),
		params:    &params,
	}
	s.consumers.Store(make(map[string]*executor.WorkPoolExecutor))

	// initial configuration.
	s.updateConfig(params.config)

	return s, nil
}

func (w *WolveKafkaService) Serve() error {
	return nil
}

func (w *WolveKafkaService) updateConfig(config *configs.Config) {
	w.mu.Lock()
	defer w.mu.Unlock()

	currentConsumers := w.consumers.Load().(map[string]*executor.WorkPoolExecutor)
	newConsumers := make(map[string]executor.Executor)

	for _, consumerCfg := range config.Consumers() {
		consumerName := consumerCfg.GetGroupId()
		if existingConsumer, exists := currentConsumers[consumerName]; exists {
			newConsumers[consumerName] = existingConsumer
			delete(currentConsumers, consumerName)
		} else {
			workPoolParams := executor.WorkPoolExecutorParams{
				NumWorkers:   defaultNumWorkers,
				QueueTimeout: defaultQueueTimeout,
			}
			workPool := executor.NewWorkPoolExecutor()
			err := workPool.Configure(workPoolParams)
			if err != nil {
				w.log.Errorf("failed to configure work pool for consumer %s: %s", consumerName, err)
			} else {
				newConsumers[consumerName] = workPool
			}
		}
	}

	// Shutdown removed consumers.
	for consumerName, exec := range currentConsumers {
		w.log.Infof("stopping consumer %s", consumerName)
		exec.Configure(executor.WorkPoolExecutorParams{
			NumWorkers: 0,
		})
	}

	w.consumers.Store(newConsumers)
}
