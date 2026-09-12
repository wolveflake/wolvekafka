// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"context"
	"fmt"
	"time"

	model_pb "wolvekafka/proto/model"
)

// BatchResult explicitly separates successful output from terminal dispositions.
// Request and Response contain only successes, aligned in original input order.
type BatchResult struct {
	Request      *model_pb.LambdaRequest
	Response     *model_pb.LambdaResponse
	DeadLettered int
	Dropped      int
}

// ExecuteBatch retains successful results and retries only unresolved records.
// A terminal result is acknowledged only after an explicit drop or durable DLQ
// write. Without a terminal action, RETRY keeps retrying and FAILED pauses until
// cancellation (rebalance, restart, or config reload), leaving input uncommitted.
func (r *RetryPolicy) ExecuteBatch(ctx context.Context, req *model_pb.LambdaRequest,
	fn func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error),
) (*BatchResult, error) {
	result := &BatchResult{Request: &model_pb.LambdaRequest{}, Response: &model_pb.LambdaResponse{}}
	successes := make([]*model_pb.ResponseMessage, len(req.GetMessages()))
	pending := make([]int, len(successes))
	for i := range pending {
		pending[i] = i
	}
	policy := r.consumerCfg.GetRetryPolicy()
	schedule := buildBackoffSchedule(policy.GetExponentialBackoff())
	hasAction := policy.GetDrop() != nil || policy.GetDlq() != nil
	for attempt := 0; len(pending) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		subset := &model_pb.LambdaRequest{}
		for _, i := range pending {
			subset.Messages = append(subset.Messages, req.Messages[i])
		}
		resp, err := fn(ctx, subset)
		if err == nil {
			err = validateBatchResponse(subset, resp)
		}
		var retry, terminal []int
		if err != nil {
			retry = append(retry, pending...)
		} else {
			for j, msg := range resp.Messages {
				i := pending[j]
				switch msg.GetStatus() {
				case model_pb.ResponseStatus_STATUS_SUCCESS:
					successes[i] = msg
				case model_pb.ResponseStatus_STATUS_RETRY:
					retry = append(retry, i)
				case model_pb.ResponseStatus_STATUS_FAILED:
					terminal = append(terminal, i)
				}
			}
			err = fmt.Errorf("handler requested retry")
		}
		if attempt >= len(schedule) && hasAction {
			terminal = append(terminal, retry...)
			retry = nil
		}
		if len(terminal) > 0 {
			failed := &model_pb.LambdaRequest{}
			for _, i := range terminal {
				failed.Messages = append(failed.Messages, req.Messages[i])
			}
			if terminalErr := r.resolveTerminal(ctx, failed, fmt.Errorf("terminal handler failure or exhausted retries: %w", err)); terminalErr != nil {
				return nil, terminalErr
			}
			if policy.GetDrop() != nil {
				result.Dropped += len(terminal)
			} else {
				result.DeadLettered += len(terminal)
			}
		}
		if len(retry) == 0 {
			break
		}
		delay := defaultRetryMaxBackoff
		if attempt < len(schedule) {
			delay = schedule[attempt]
		} else if len(schedule) > 0 {
			delay = schedule[len(schedule)-1]
		}
		if r.metrics != nil {
			r.metrics.recordLambdaRetry(ctx, r.consumerName, r.jobId, attempt+2, err)
		}
		r.log.Warnf("[%s:%d] retaining %d unresolved records: %v", r.consumerName, r.jobId, len(retry), err)
		if err := sleepWithContext(ctx, delay); err != nil {
			return nil, err
		}
		pending = retry
	}
	for i, msg := range successes {
		if msg != nil {
			result.Request.Messages = append(result.Request.Messages, req.Messages[i])
			result.Response.Messages = append(result.Response.Messages, msg)
		}
	}
	return result, nil
}

func validateBatchResponse(req *model_pb.LambdaRequest, resp *model_pb.LambdaResponse) error {
	if resp == nil || len(resp.GetMessages()) != len(req.GetMessages()) {
		return fmt.Errorf("response message count (%d) does not match request message count (%d)", len(resp.GetMessages()), len(req.GetMessages()))
	}
	for i, msg := range resp.Messages {
		if msg == nil {
			return fmt.Errorf("response message %d is null", i)
		}
		switch msg.GetStatus() {
		case model_pb.ResponseStatus_STATUS_SUCCESS, model_pb.ResponseStatus_STATUS_RETRY, model_pb.ResponseStatus_STATUS_FAILED:
		default:
			return fmt.Errorf("response message %d has unknown status %d", i, msg.GetStatus())
		}
	}
	return nil
}

func (r *RetryPolicy) resolveTerminal(ctx context.Context, req *model_pb.LambdaRequest, cause error) error {
	policy := r.consumerCfg.GetRetryPolicy()
	if policy.GetDrop() != nil {
		return nil
	}
	if policy.GetDlq() == nil {
		r.log.Errorf("[%s:%d] terminal handler failure without drop/DLQ policy; holding uncommitted batch until rebalance, restart, or config reload", r.consumerName, r.jobId)
		<-ctx.Done()
		return ctx.Err()
	}
	schedule := buildBackoffSchedule(policy.GetDlq().GetExponentialBackoff())
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.sendToDLQOnce(ctx, req, policy.GetDlq(), cause); err == nil {
			return nil
		} else {
			r.log.Errorf("[%s:%d] retaining input after DLQ failure: %v", r.consumerName, r.jobId, err)
		}
		delay := time.Second
		if len(schedule) > 0 {
			index := attempt
			if index >= len(schedule) {
				index = len(schedule) - 1
			}
			delay = schedule[index]
		}
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
}
