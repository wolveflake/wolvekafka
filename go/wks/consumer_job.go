///
/// wolveflake LLC ("COMPANY") CONFIDENTIAL
/// Unpublished Copyright (c) 2023-2024 [wolveflake LLC], All Rights Reserved.
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
	"time"

	"go.uber.org/zap"
)

type sendEventFunc func(ctx context.Context, event []*Event) error

type ConsumerJob struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	// consumer name.
	name string

	// events to send to consumer for processing.
	event []*Event

	// priority of the job (low value -> bigger priority).
	priority uint16

	sendEventFunc sendEventFunc

	// logger instance.
	log *zap.SugaredLogger

	// timestamp of the job creation.
	jobCreationStartTime time.Time
	/// timestamp of the job execution.
	jobExecutionStartTime time.Time
}

func defaultSendEventFunc(ctx context.Context, event []*Event) error {
	// TODO(13): implement default event sending logic.
	return nil
}

func newConsumerJob(ctx context.Context, event []*Event, priority uint16) *ConsumerJob {
	ctxJob, ctxCancel := context.WithCancel(ctx)

	return &ConsumerJob{
		ctx:           ctxJob,
		ctxCancel:     ctxCancel,
		event:         event,
		priority:      priority,
		sendEventFunc: defaultSendEventFunc,
		// creation time.
		jobCreationStartTime: time.Now(),
	}
}

func (e *ConsumerJob) Priority() uint16 {
	return e.priority
}

func (e *ConsumerJob) Execute() {
	defer e.done()

	// record execution start time to report processing time metrics.
	e.jobExecutionStartTime = time.Now()

	err := e.sendEventFunc(e.ctx, e.event)
	eventsLogs := make([]string, 0, len(e.event))
	for _, ev := range e.event {
		eventsLogs = append(eventsLogs, ev.msg.String())
	}
	e.log.Infof("Processed events with error: %v, event: %+v", err, eventsLogs)
}

func (e *ConsumerJob) Cancel() {
	defer e.done()
}

func (e *ConsumerJob) done() {
	// TODO(13): record metrics.

	// cancel job context.
	e.ctxCancel()
}
