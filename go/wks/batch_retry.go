package wks

import (
	"context"
	"fmt"
	"time"

	model_pb "wolvekafka/proto/model"
)

// ExecuteBatch retains successful results and retries only unresolved records.
// A terminal result is acknowledged only after an explicit drop or durable DLQ
// write. Without a terminal action, RETRY keeps retrying and FAILED pauses until
// cancellation (restart or config reload), leaving the input uncommitted.
func (r *RetryPolicy) ExecuteBatch(ctx context.Context, req *model_pb.LambdaRequest,
	fn func(context.Context, *model_pb.LambdaRequest) (*model_pb.LambdaResponse, error),
) (*model_pb.LambdaResponse, error) {
	result := &model_pb.LambdaResponse{Messages: make([]*model_pb.ResponseMessage, len(req.GetMessages()))}
	pending := make([]int, len(result.Messages))
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
		var retry []int
		if err != nil {
			retry = append(retry, pending...)
		} else {
			for j, msg := range resp.Messages {
				i := pending[j]
				switch msg.GetStatus() {
				case model_pb.ResponseStatus_STATUS_SUCCESS:
					result.Messages[i] = msg
				case model_pb.ResponseStatus_STATUS_RETRY:
					retry = append(retry, i)
				case model_pb.ResponseStatus_STATUS_FAILED:
					if err := r.resolveTerminal(ctx, req.Messages[i], fmt.Errorf("handler returned STATUS_FAILED")); err != nil {
						return nil, err
					}
					result.Messages[i] = &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_FAILED}
				}
			}
			err = fmt.Errorf("handler requested retry")
		}
		if len(retry) == 0 {
			break
		}
		if attempt >= len(schedule) && hasAction {
			for _, i := range retry {
				if terminalErr := r.resolveTerminal(ctx, req.Messages[i], err); terminalErr != nil {
					return nil, terminalErr
				}
				result.Messages[i] = &model_pb.ResponseMessage{Status: model_pb.ResponseStatus_STATUS_FAILED}
			}
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

func (r *RetryPolicy) resolveTerminal(ctx context.Context, msg *model_pb.KafkaMessage, cause error) error {
	policy := r.consumerCfg.GetRetryPolicy()
	if policy.GetDrop() != nil {
		return nil
	}
	if policy.GetDlq() == nil {
		r.log.Errorf("[%s:%d] terminal handler failure without drop/DLQ policy; holding uncommitted batch until restart or config reload", r.consumerName, r.jobId)
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.sendToDLQ(ctx, &model_pb.LambdaRequest{Messages: []*model_pb.KafkaMessage{msg}}, policy.GetDlq(), cause); err == nil {
			return nil
		} else {
			r.log.Errorf("[%s:%d] retaining input after DLQ failure: %v", r.consumerName, r.jobId, err)
		}
		if err := sleepWithContext(ctx, time.Second); err != nil {
			return err
		}
	}
}
