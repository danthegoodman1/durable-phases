package shardengine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
)

type JournalOperation struct {
	Op      string                  `json:"op"`
	Input   json.RawMessage         `json:"input,omitempty"`
	Session *durable.OpenShardInput `json:"session,omitempty"`
}

type ClaimReadyActivationsOperationInput struct {
	ShardIDs []int                        `json:"shardIds"`
	Input    durable.ClaimShardTasksInput `json:"input"`
}

type HeartbeatActivationsOperationInput struct {
	ActivationIDs []string      `json:"activationIds"`
	WorkerID      string        `json:"workerId"`
	Now           time.Time     `json:"now"`
	Lease         time.Duration `json:"lease"`
}

type ReleaseActivationsOperationInput struct {
	ActivationIDs []string `json:"activationIds"`
	WorkerID      string   `json:"workerId"`
}

type HeartbeatDispatchShardOperationInput struct {
	Now   time.Time     `json:"now"`
	Lease time.Duration `json:"lease"`
}

func NewJournalOperation(op string, input any) (JournalOperation, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return JournalOperation{}, err
	}
	return JournalOperation{Op: op, Input: raw}, nil
}

func NewSessionJournalOperation(op string, session durable.OpenShardInput, input any) (JournalOperation, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return JournalOperation{}, err
	}
	return JournalOperation{Op: op, Input: raw, Session: &session}, nil
}

func ApplyJournalOperation(ctx context.Context, engine *Provider, operation JournalOperation) error {
	return engine.replay(func() error {
		switch operation.Op {
		case "claimShard":
			input, err := decodeJournalInput[durable.ClaimDispatchShardInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.ClaimShard(ctx, input)
			return err
		case "heartbeatDispatchShard":
			if operation.Session == nil {
				return fmt.Errorf("heartbeatDispatchShard journal operation missing session")
			}
			input, err := decodeJournalInput[HeartbeatDispatchShardOperationInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.OpenShard(*operation.Session).Heartbeat(ctx, input.Now, input.Lease)
		case "releaseDispatchShard":
			input, err := decodeJournalInput[durable.ReleaseDispatchShardInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.OpenShard(durable.OpenShardInput{ShardID: input.ShardID, OwnerID: input.OwnerID}).Release(ctx)
		case "createInstance":
			input, err := decodeJournalInput[durable.CreateInstanceInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.CreateInstance(ctx, input)
			return err
		case "createChildInstance":
			input, err := decodeJournalInput[durable.CreateChildInstanceInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.CreateChildInstance(ctx, input)
			return err
		case "cancelChild":
			input, err := decodeJournalInput[durable.CancelChildInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.CancelChild(ctx, input)
		case "appendSignal":
			input, err := decodeJournalInput[durable.AppendSignalInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.AppendSignal(ctx, input)
			return err
		case "claimReadyActivations":
			input, err := decodeJournalInput[ClaimReadyActivationsOperationInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.ClaimReadyActivations(ctx, input.ShardIDs, input.Input)
			return err
		case "claimShardTasks":
			if operation.Session == nil {
				return fmt.Errorf("claimShardTasks journal operation missing session")
			}
			input, err := decodeJournalInput[durable.ClaimShardTasksInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.OpenShard(*operation.Session).ClaimTasks(ctx, input)
			return err
		case "heartbeatActivations":
			input, err := decodeJournalInput[HeartbeatActivationsOperationInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.HeartbeatActivations(ctx, input.ActivationIDs, input.WorkerID, input.Now, input.Lease)
		case "releaseActivations":
			input, err := decodeJournalInput[ReleaseActivationsOperationInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.ReleaseActivations(ctx, input.ActivationIDs, input.WorkerID)
		case "getOrReserveEffect":
			input, err := decodeJournalInput[durable.ReserveEffectInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.GetOrReserveEffect(ctx, input)
			return err
		case "heartbeatEffect":
			input, err := decodeJournalInput[durable.HeartbeatEffectInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.HeartbeatEffect(ctx, input)
		case "completeEffect":
			input, err := decodeJournalInput[durable.CompleteEffectInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.CompleteEffect(ctx, input)
		case "failEffect":
			input, err := decodeJournalInput[durable.FailEffectInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.FailEffect(ctx, input)
			return err
		case "commitActivations":
			input, err := decodeJournalInput[[]durable.CommitCheckpointInput](operation.Input)
			if err != nil {
				return err
			}
			_, err = engine.CommitActivations(ctx, input)
			return err
		case "recordActivationFailures":
			input, err := decodeJournalInput[[]durable.RecordActivationFailureInput](operation.Input)
			if err != nil {
				return err
			}
			return engine.RecordActivationFailures(ctx, input)
		default:
			return fmt.Errorf("unknown journal operation: %s", operation.Op)
		}
	})
}

func OperationTime(operation JournalOperation) time.Time {
	switch operation.Op {
	case "claimShard":
		if input, err := decodeJournalInput[durable.ClaimDispatchShardInput](operation.Input); err == nil {
			return input.Now
		}
	case "heartbeatDispatchShard":
		if input, err := decodeJournalInput[HeartbeatDispatchShardOperationInput](operation.Input); err == nil {
			return input.Now
		}
	case "createInstance":
		if input, err := decodeJournalInput[durable.CreateInstanceInput](operation.Input); err == nil {
			return input.Now
		}
	case "createChildInstance":
		if input, err := decodeJournalInput[durable.CreateChildInstanceInput](operation.Input); err == nil {
			return input.Now
		}
	case "cancelChild":
		if input, err := decodeJournalInput[durable.CancelChildInput](operation.Input); err == nil {
			return input.Now
		}
	case "appendSignal":
		if input, err := decodeJournalInput[durable.AppendSignalInput](operation.Input); err == nil {
			return input.ReceivedAt
		}
	case "claimReadyActivations":
		if input, err := decodeJournalInput[ClaimReadyActivationsOperationInput](operation.Input); err == nil {
			return input.Input.Now
		}
	case "claimShardTasks":
		if input, err := decodeJournalInput[durable.ClaimShardTasksInput](operation.Input); err == nil {
			return input.Now
		}
	case "heartbeatActivations":
		if input, err := decodeJournalInput[HeartbeatActivationsOperationInput](operation.Input); err == nil {
			return input.Now
		}
	case "getOrReserveEffect":
		if input, err := decodeJournalInput[durable.ReserveEffectInput](operation.Input); err == nil {
			return input.Now
		}
	case "heartbeatEffect":
		if input, err := decodeJournalInput[durable.HeartbeatEffectInput](operation.Input); err == nil {
			return input.Now
		}
	case "completeEffect":
		if input, err := decodeJournalInput[durable.CompleteEffectInput](operation.Input); err == nil {
			return input.Now
		}
	case "failEffect":
		if input, err := decodeJournalInput[durable.FailEffectInput](operation.Input); err == nil {
			return input.Now
		}
	case "commitActivations":
		if input, err := decodeJournalInput[[]durable.CommitCheckpointInput](operation.Input); err == nil && len(input) > 0 {
			return input[0].Now
		}
	case "recordActivationFailures":
		if input, err := decodeJournalInput[[]durable.RecordActivationFailureInput](operation.Input); err == nil && len(input) > 0 {
			return input[0].Now
		}
	}
	return time.Now().UTC()
}

func decodeJournalInput[T any](raw json.RawMessage) (T, error) {
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (p *Provider) replay(fn func() error) error {
	p.mu.Lock()
	previous := p.replaying
	p.replaying = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.replaying = previous
		p.mu.Unlock()
	}()
	return fn()
}
