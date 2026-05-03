package shardengine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
)

type Snapshot struct {
	Instances                 []durable.PersistedInstance       `json:"instances"`
	Signals                   []durable.SignalRecord            `json:"signals"`
	Children                  []durable.ChildRecord             `json:"children"`
	Tasks                     []Task                            `json:"tasks"`
	EffectsByActivation       map[string][]durable.EffectRecord `json:"effectsByActivation"`
	ClaimedSequenceEpochs     map[string]int64                  `json:"claimedSequenceEpochs"`
	CompletedActivationClaims []CompletedActivationClaim        `json:"completedActivationClaims"`
	ShardLeases               []durable.ShardLease              `json:"shardLeases"`
	SignalCounter             int64                             `json:"signalCounter"`
	ChildCounter              int64                             `json:"childCounter"`
}

type CompletedActivationClaim struct {
	ActivationID        string `json:"activationId"`
	WorkflowID          string `json:"workflowId"`
	RunID               string `json:"runId"`
	Sequence            int64  `json:"sequence"`
	Kind                string `json:"kind"`
	OwnerID             string `json:"ownerId,omitempty"`
	CompletedBySequence int64  `json:"completedBySequence"`
}

type Task struct {
	TaskID          string               `json:"taskId"`
	ActivationID    string               `json:"activationId"`
	WorkflowName    string               `json:"workflowName"`
	WorkflowVersion int                  `json:"workflowVersion"`
	WorkflowID      string               `json:"workflowId"`
	RunID           string               `json:"runId"`
	PartitionShard  int                  `json:"partitionShard"`
	Sequence        int64                `json:"sequence"`
	Kind            string               `json:"kind"`
	WaitName        string               `json:"waitName,omitempty"`
	Wait            *durable.DurableWait `json:"wait,omitempty"`
	Event           *durable.ReadyEvent  `json:"event,omitempty"`
	ReadyAt         time.Time            `json:"readyAt"`
	SortKey         string               `json:"sortKey"`
	ClaimOwnerID    string               `json:"claimOwnerId,omitempty"`
	ClaimEpoch      int64                `json:"claimEpoch,omitempty"`
	LeaseUntil      time.Time            `json:"leaseUntil,omitempty"`
	BlockedUntil    time.Time            `json:"blockedUntil,omitempty"`
}

type Provider struct {
	mu                        sync.Mutex
	instances                 map[string]durable.PersistedInstance
	signals                   map[string]durable.SignalRecord
	children                  map[string]durable.ChildRecord
	tasks                     map[string]Task
	taskIDsByRef              map[string]map[string]struct{}
	taskIDsByShard            map[int]map[string]struct{}
	taskIDsByActivation       map[string]map[string]struct{}
	effectsByActivation       map[string][]durable.EffectRecord
	claimedSequenceEpochs     map[string]int64
	completedActivationClaims []CompletedActivationClaim
	shardLeases               map[int]durable.ShardLease
	signalCounter             int64
	childCounter              int64
	replaying                 bool
}

func New() *Provider {
	p := &Provider{}
	p.init()
	return p
}

func FromSnapshot(snapshot Snapshot) *Provider {
	p := New()
	_ = p.Restore(snapshot)
	return p
}

func (p *Provider) init() {
	p.instances = map[string]durable.PersistedInstance{}
	p.signals = map[string]durable.SignalRecord{}
	p.children = map[string]durable.ChildRecord{}
	p.tasks = map[string]Task{}
	p.taskIDsByRef = map[string]map[string]struct{}{}
	p.taskIDsByShard = map[int]map[string]struct{}{}
	p.taskIDsByActivation = map[string]map[string]struct{}{}
	p.effectsByActivation = map[string][]durable.EffectRecord{}
	p.claimedSequenceEpochs = map[string]int64{}
	p.completedActivationClaims = nil
	p.shardLeases = map[int]durable.ShardLease{}
	p.signalCounter = 0
	p.childCounter = 0
}

func (p *Provider) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

func (p *Provider) snapshotLocked() Snapshot {
	out := Snapshot{
		EffectsByActivation:   map[string][]durable.EffectRecord{},
		ClaimedSequenceEpochs: map[string]int64{},
		SignalCounter:         p.signalCounter,
		ChildCounter:          p.childCounter,
	}
	for _, instance := range p.instances {
		out.Instances = append(out.Instances, clone(instance))
	}
	sort.Slice(out.Instances, func(i, j int) bool {
		return refKey(out.Instances[i].WorkflowID, out.Instances[i].RunID) < refKey(out.Instances[j].WorkflowID, out.Instances[j].RunID)
	})
	for _, signal := range p.signals {
		out.Signals = append(out.Signals, clone(signal))
	}
	sort.Slice(out.Signals, func(i, j int) bool {
		return out.Signals[i].SignalID < out.Signals[j].SignalID
	})
	for _, child := range p.children {
		out.Children = append(out.Children, clone(child))
	}
	sort.Slice(out.Children, func(i, j int) bool {
		return out.Children[i].ChildRecordID < out.Children[j].ChildRecordID
	})
	for _, task := range p.tasks {
		out.Tasks = append(out.Tasks, clone(task))
	}
	sort.Slice(out.Tasks, func(i, j int) bool {
		return out.Tasks[i].TaskID < out.Tasks[j].TaskID
	})
	for activationID, effects := range p.effectsByActivation {
		out.EffectsByActivation[activationID] = clone(effects)
	}
	for key, epoch := range p.claimedSequenceEpochs {
		out.ClaimedSequenceEpochs[key] = epoch
	}
	out.CompletedActivationClaims = clone(p.completedActivationClaims)
	for _, lease := range p.shardLeases {
		out.ShardLeases = append(out.ShardLeases, clone(lease))
	}
	sort.Slice(out.ShardLeases, func(i, j int) bool {
		return out.ShardLeases[i].ShardID < out.ShardLeases[j].ShardID
	})
	return out
}

func (p *Provider) Restore(snapshot Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init()
	for _, instance := range snapshot.Instances {
		p.instances[refKey(instance.WorkflowID, instance.RunID)] = clone(instance)
	}
	for _, signal := range snapshot.Signals {
		p.signals[signal.SignalID] = clone(signal)
	}
	for _, child := range snapshot.Children {
		p.children[child.ChildRecordID] = clone(child)
	}
	for _, task := range snapshot.Tasks {
		cp := clone(task)
		p.tasks[cp.TaskID] = cp
		addIndex(p.taskIDsByRef, refKey(cp.WorkflowID, cp.RunID), cp.TaskID)
		addIndexInt(p.taskIDsByShard, cp.PartitionShard, cp.TaskID)
		addIndex(p.taskIDsByActivation, cp.ActivationID, cp.TaskID)
	}
	for activationID, effects := range snapshot.EffectsByActivation {
		p.effectsByActivation[activationID] = clone(effects)
	}
	for key, epoch := range snapshot.ClaimedSequenceEpochs {
		p.claimedSequenceEpochs[key] = epoch
	}
	p.completedActivationClaims = clone(snapshot.CompletedActivationClaims)
	for _, lease := range snapshot.ShardLeases {
		p.shardLeases[lease.ShardID] = clone(lease)
	}
	p.signalCounter = snapshot.SignalCounter
	p.childCounter = snapshot.ChildCounter
	return nil
}

func (p *Provider) ClaimShard(_ context.Context, input durable.ClaimDispatchShardInput) (*durable.ShardLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	existing, ok := p.shardLeases[input.ShardID]
	if ok && existing.OwnerID != input.OwnerID && existing.LeaseUntil.After(input.Now) {
		return nil, nil
	}
	epoch := existing.LeaseEpoch + 1
	if ok && existing.OwnerID == input.OwnerID && existing.LeaseUntil.After(input.Now) {
		epoch = existing.LeaseEpoch
	}
	lease := durable.ShardLease{
		ShardID:    input.ShardID,
		OwnerID:    input.OwnerID,
		LeaseUntil: input.Now.Add(input.Lease),
		LeaseEpoch: epoch,
	}
	p.shardLeases[input.ShardID] = lease
	out := clone(lease)
	return &out, nil
}

func (p *Provider) OpenShard(input durable.OpenShardInput) durable.ShardDurabilitySession {
	return &session{provider: p, shardID: input.ShardID, ownerID: input.OwnerID, leaseEpoch: input.LeaseEpoch}
}

func (p *Provider) CreateInstance(_ context.Context, input durable.CreateInstanceInput) (durable.InstanceRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.createInstanceLocked(input)
}

func (p *Provider) createInstanceLocked(input durable.CreateInstanceInput) (durable.InstanceRef, error) {
	key := refKey(input.WorkflowID, input.RunID)
	if _, ok := p.instances[key]; ok {
		switch input.ConflictPolicy {
		case durable.ConflictFail:
			return durable.InstanceRef{}, fmt.Errorf("workflow instance already exists: %s/%s", input.WorkflowID, input.RunID)
		case durable.ConflictTerminateExisting:
			p.deleteInstanceRecordsLocked(input.WorkflowID, input.RunID)
		default:
			return durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID}, nil
		}
	}
	instance := durable.PersistedInstance{
		WorkflowName:    input.WorkflowName,
		WorkflowVersion: input.WorkflowVersion,
		WorkflowID:      input.WorkflowID,
		RunID:           input.RunID,
		PartitionShard:  input.PartitionShard,
		Sequence:        0,
		Status:          "running",
		Common:          clone(input.Common),
		Phase:           ptr(clone(input.Phase)),
		Waits:           clone(input.Waits),
		CreatedAt:       input.Now,
		UpdatedAt:       input.Now,
		Parent:          clonePtr(input.Parent),
	}
	p.instances[key] = instance
	p.replaceTasksForInstanceLocked(instance)
	return durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID}, nil
}

func (p *Provider) CreateChildInstance(_ context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.replaying {
		if err := p.requireShardOwnerForRefLocked(input.WorkflowID, input.RunID, input.WorkerID, input.LeaseNow); err != nil {
			return durable.ChildHandleAny{}, err
		}
	}
	parent := p.instances[refKey(input.ParentWorkflowID, input.ParentRunID)]
	if parent.Status != "running" {
		return durable.ChildHandleAny{}, fmt.Errorf("unknown running parent: %s/%s", input.ParentWorkflowID, input.ParentRunID)
	}
	for _, child := range p.children {
		if child.ParentWorkflowID == input.ParentWorkflowID && child.ParentRunID == input.ParentRunID && child.ActivationID == input.ActivationID && child.Key == input.Key && input.ConflictPolicy != durable.ConflictTerminateExisting {
			if input.ConflictPolicy == durable.ConflictFail {
				return durable.ChildHandleAny{}, fmt.Errorf("child workflow already exists for activation key: %s", input.Key)
			}
			return childHandle(child), nil
		}
	}
	if conflict := p.validateChildStartsLocked(input.ParentWorkflowID, input.ParentRunID, input.ActivationID, []durable.CheckpointChildStart{childStartFromCreate(input)}); conflict != nil {
		return durable.ChildHandleAny{}, conflict.err
	}
	p.writeChildStartLocked(childStartFromCreate(input), input.Now, input.ParentWorkflowID, input.ParentRunID, input.ActivationID)
	return durable.ChildHandleAny{WorkflowName: input.WorkflowName, WorkflowVersion: input.WorkflowVersion, WorkflowID: input.WorkflowID, RunID: input.RunID}, nil
}

func (p *Provider) CancelChild(_ context.Context, input durable.CancelChildInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.replaying {
		if err := p.requireShardOwnerForRefLocked(input.ParentWorkflowID, input.ParentRunID, input.WorkerID, input.Now); err != nil {
			return err
		}
	}
	for id, child := range p.children {
		if child.ParentWorkflowID == input.ParentWorkflowID && child.ParentRunID == input.ParentRunID && child.WorkflowID == input.WorkflowID && child.RunID == input.RunID {
			if child.Status != "started" {
				return nil
			}
			child.Status = "failed"
			child.CompletedAt = input.Now
			child.Error = durable.SerializedError{Name: "ChildCanceled", Message: "Child canceled by parent"}
			p.children[id] = child
			childInstance := p.instances[refKey(input.WorkflowID, input.RunID)]
			if childInstance.Status == "running" {
				childInstance.Status = "canceled"
				childInstance.CancelReason = "Child canceled by parent"
				childInstance.Waits = nil
				childInstance.UpdatedAt = input.Now
				p.instances[refKey(input.WorkflowID, input.RunID)] = childInstance
				p.deleteTasksForRefLocked(input.WorkflowID, input.RunID)
			}
			if parent, ok := p.instances[refKey(input.ParentWorkflowID, input.ParentRunID)]; ok && parent.Status == "running" {
				p.refreshChildTasksForInstanceLocked(parent)
			}
			return nil
		}
	}
	return nil
}

func (p *Provider) LoadInstance(_ context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	instance, ok := p.instances[refKey(ref.WorkflowID, ref.RunID)]
	if !ok {
		return nil, nil
	}
	out := clone(instance)
	if options.IncludeEffects {
		for activationID, effects := range p.effectsByActivation {
			if strings.HasPrefix(activationID, ref.WorkflowID+"/"+ref.RunID+"/") {
				out.Effects = append(out.Effects, clone(effects)...)
			}
		}
	}
	return &out, nil
}

func (p *Provider) ShardForRef(ref durable.InstanceRef) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	instance, ok := p.instances[refKey(ref.WorkflowID, ref.RunID)]
	if !ok {
		return 0, false
	}
	return instance.PartitionShard, true
}

func (p *Provider) ShardForActivation(activationID string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tasks := p.tasksForActivationLocked(activationID)
	if len(tasks) == 0 {
		return 0, false
	}
	return tasks[0].PartitionShard, true
}

func (p *Provider) AppendSignal(_ context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.instances[refKey(input.WorkflowID, input.RunID)]; !ok {
		return durable.SignalRecord{}, fmt.Errorf("cannot signal unknown workflow: %s/%s", input.WorkflowID, input.RunID)
	}
	p.signalCounter++
	signal := durable.SignalRecord{
		SignalID:   fmt.Sprintf("signal-%d", p.signalCounter),
		WorkflowID: input.WorkflowID,
		RunID:      input.RunID,
		Type:       input.Type,
		Payload:    clone(input.Payload),
		ReceivedAt: input.ReceivedAt,
	}
	p.signals[signal.SignalID] = signal
	instance := p.instances[refKey(input.WorkflowID, input.RunID)]
	if instance.Status == "running" {
		p.refreshSignalTasksForInstanceLocked(instance)
	}
	return clone(signal), nil
}

func (p *Provider) ClaimReadyActivations(ctx context.Context, shardIDs []int, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	var output durable.ClaimShardTasksResult
	for _, shardID := range shardIDs {
		session := p.OpenShard(durable.OpenShardInput{ShardID: shardID})
		result, err := session.ClaimTasks(ctx, input)
		if err != nil {
			continue
		}
		output.Claims = append(output.Claims, result.Claims...)
		output.NextWakeAt = earliest(output.NextWakeAt, result.NextWakeAt)
		if len(output.Claims) >= input.Limit {
			output.Claims = output.Claims[:input.Limit]
			break
		}
	}
	return output, nil
}

func (p *Provider) HeartbeatActivations(_ context.Context, activationIDs []string, workerID string, now time.Time, lease time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireActivityTimeoutsLocked(now, timeoutFilter{ownerID: workerID})
	for _, activationID := range activationIDs {
		task := p.findClaimedTaskLocked(activationID, workerID, now)
		if task == nil {
			return fmt.Errorf("lost activation lease: %s", activationID)
		}
		task.LeaseUntil = now.Add(lease)
		p.tasks[task.TaskID] = *task
	}
	return nil
}

func (p *Provider) ReleaseActivations(_ context.Context, activationIDs []string, workerID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, activationID := range activationIDs {
		p.releaseActivationLocked(activationID, workerID)
	}
	return nil
}

func (p *Provider) GetOrReserveEffect(_ context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireActivityTimeoutsLocked(input.Now, timeoutFilter{activationID: input.ActivationID, ownerID: input.WorkerID})
	if effect := p.effectForLocked(input.ActivationID, input.Key); effect != nil {
		switch effect.Status {
		case "completed":
			return durable.EffectReservation{Status: "completed", Result: clone(effect.Result)}, nil
		case "failed":
			return durable.EffectReservation{Status: "failed", Error: effect.Error}, nil
		}
	}
	task := p.findClaimedTaskLocked(input.ActivationID, input.WorkerID, input.Now)
	if task == nil {
		return durable.EffectReservation{}, fmt.Errorf("lost activation lease: %s", input.ActivationID)
	}
	existing := p.effectForLocked(input.ActivationID, input.Key)
	options := normalizeEffectOptions(input)
	attempt := 1
	if existing != nil && existing.Attempt > 0 {
		attempt = existing.Attempt
	}
	effect := durable.EffectRecord{
		EffectID:               firstNonEmpty(effectIDFor(input.ActivationID, input.Key), ""),
		ActivationID:           input.ActivationID,
		Key:                    input.Key,
		IdempotencyKey:         fmt.Sprintf("%s/%s/%s/%s", input.WorkflowID, input.RunID, input.ActivationID, input.Key),
		Status:                 "pending",
		Attempt:                attempt,
		AttemptID:              attemptIDFor(input.ActivationID, input.Key, attempt, input.WorkerID, input.Now),
		AttemptOwnerID:         input.WorkerID,
		AttemptStartedAt:       input.Now,
		StartToCloseTimeout:    options.startToCloseTimeout,
		HeartbeatTimeout:       options.heartbeatTimeout,
		MaxAttempts:            options.maxAttempts,
		MaxElapsed:             options.maxElapsed,
		InitialInterval:        options.initialInterval,
		MaxInterval:            options.maxInterval,
		BackoffCoefficient:     options.backoffCoefficient,
		FirstAttemptStartedAt:  input.Now,
		NonRetryableErrorNames: clone(options.nonRetryableErrorNames),
	}
	if existing != nil {
		effect.EffectID = existing.EffectID
		effect.IdempotencyKey = existing.IdempotencyKey
		effect.FirstAttemptStartedAt = existing.FirstAttemptStartedAt
		effect.HeartbeatDetails = clone(existing.HeartbeatDetails)
		effect.LastFailure = existing.LastFailure
	}
	if effect.StartToCloseTimeout > 0 {
		effect.StartToCloseDeadline = input.Now.Add(effect.StartToCloseTimeout)
	}
	if effect.HeartbeatTimeout > 0 {
		effect.HeartbeatDeadline = input.Now.Add(effect.HeartbeatTimeout)
	}
	p.upsertEffectLocked(input.ActivationID, effect)
	task.BlockedUntil = time.Time{}
	p.tasks[task.TaskID] = *task
	return durable.EffectReservation{
		Status:           "reserved",
		EffectID:         effect.EffectID,
		IdempotencyKey:   effect.IdempotencyKey,
		Attempt:          effect.Attempt,
		AttemptID:        effect.AttemptID,
		HeartbeatDetails: clone(effect.HeartbeatDetails),
	}, nil
}

func (p *Provider) HeartbeatEffect(_ context.Context, input durable.HeartbeatEffectInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireActivityTimeoutsLocked(input.Now, timeoutFilter{activationID: input.ActivationID, ownerID: input.WorkerID})
	effect, err := p.mutableEffectLocked(input.ActivationID, input.WorkerID, input.EffectID, input.AttemptID, input.Now)
	if err != nil {
		return err
	}
	effect.HeartbeatAt = input.Now
	effect.HeartbeatDetails = clone(input.Details)
	if effect.HeartbeatTimeout > 0 {
		effect.HeartbeatDeadline = input.Now.Add(effect.HeartbeatTimeout)
	}
	p.upsertEffectLocked(input.ActivationID, *effect)
	return nil
}

func (p *Provider) CompleteEffect(_ context.Context, input durable.CompleteEffectInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireActivityTimeoutsLocked(input.Now, timeoutFilter{activationID: input.ActivationID, ownerID: input.WorkerID})
	effect, err := p.mutableEffectLocked(input.ActivationID, input.WorkerID, input.EffectID, input.AttemptID, input.Now)
	if err != nil {
		return err
	}
	effect.Status = "completed"
	effect.Result = clone(input.Result)
	effect.Error = durable.SerializedError{}
	effect.AttemptOwnerID = ""
	effect.AttemptID = ""
	effect.AttemptStartedAt = time.Time{}
	effect.StartToCloseDeadline = time.Time{}
	effect.HeartbeatDeadline = time.Time{}
	p.upsertEffectLocked(input.ActivationID, *effect)
	return nil
}

func (p *Provider) FailEffect(_ context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireActivityTimeoutsLocked(input.Now, timeoutFilter{activationID: input.ActivationID, ownerID: input.WorkerID})
	effect, err := p.mutableEffectLocked(input.ActivationID, input.WorkerID, input.EffectID, input.AttemptID, input.Now)
	if err != nil {
		return durable.FailEffectResult{}, err
	}
	retryable := true
	if input.Retryable != nil {
		retryable = *input.Retryable
	}
	decision := retryDecision(*effect, input.Error, input.Now, retryable)
	effect.Error = input.Error
	effect.LastFailure = input.Error
	effect.AttemptID = ""
	effect.AttemptOwnerID = ""
	effect.AttemptStartedAt = time.Time{}
	effect.StartToCloseDeadline = time.Time{}
	effect.HeartbeatDeadline = time.Time{}
	if decision.Status == "retry_scheduled" {
		effect.Status = "pending"
		effect.NextAttemptAt = decision.NextAttemptAt
		effect.Attempt = decision.NextAttempt
		if task := p.firstTaskForActivationLocked(input.ActivationID); task != nil {
			task.BlockedUntil = decision.NextAttemptAt
			task.ClaimOwnerID = ""
			task.ClaimEpoch = 0
			task.LeaseUntil = time.Time{}
			p.tasks[task.TaskID] = *task
			delete(p.claimedSequenceEpochs, sequenceKeyForTask(*task))
		}
		p.upsertEffectLocked(input.ActivationID, *effect)
		return decision, nil
	}
	effect.Status = "failed"
	p.upsertEffectLocked(input.ActivationID, *effect)
	return durable.FailEffectResult{Status: "failed"}, nil
}

func (p *Provider) CommitActivations(_ context.Context, inputs []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := durable.CommitActivationsResult{Results: make([]durable.CommitCheckpointResult, 0, len(inputs))}
	for _, input := range inputs {
		result := p.commitOneLocked(input)
		result.ActivationID = input.ActivationID
		out.Results = append(out.Results, result)
	}
	return out, nil
}

func (p *Provider) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	result, err := p.CommitActivations(ctx, []durable.CommitCheckpointInput{input})
	if err != nil {
		return durable.CommitCheckpointResult{}, err
	}
	if len(result.Results) == 0 {
		return durable.CommitCheckpointResult{OK: false, Sequence: -1, Reason: "missing_commit_result"}, nil
	}
	out := result.Results[0]
	out.ActivationID = ""
	return out, nil
}

func (p *Provider) RecordActivationFailures(_ context.Context, inputs []durable.RecordActivationFailureInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, input := range inputs {
		task := p.findClaimedTaskLocked(input.ActivationID, input.WorkerID, input.Now)
		if task == nil {
			return fmt.Errorf("lost activation lease: %s", input.ActivationID)
		}
		effects := checkpointEffectsToRecords(input.WorkflowID, input.RunID, input.ActivationID, input.Now, input.Effects)
		p.effectsByActivation[input.ActivationID] = effects
		for _, effect := range effects {
			if !effect.NextAttemptAt.IsZero() {
				task.BlockedUntil = effect.NextAttemptAt
			}
		}
		if input.ReleaseActivation {
			task.ClaimOwnerID = ""
			task.ClaimEpoch = 0
			task.LeaseUntil = time.Time{}
			delete(p.claimedSequenceEpochs, sequenceKeyForTask(*task))
		}
		p.tasks[task.TaskID] = *task
	}
	return nil
}

func (p *Provider) ListInstances(_ context.Context, options durable.LoadInstanceOptions) ([]durable.PersistedInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]durable.PersistedInstance, 0, len(p.instances))
	for _, instance := range p.instances {
		cp := clone(instance)
		if options.IncludeEffects {
			for activationID, effects := range p.effectsByActivation {
				if strings.HasPrefix(activationID, instance.WorkflowID+"/"+instance.RunID+"/") {
					cp.Effects = append(cp.Effects, clone(effects)...)
				}
			}
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return refKey(out[i].WorkflowID, out[i].RunID) < refKey(out[j].WorkflowID, out[j].RunID)
	})
	return out, nil
}

func (p *Provider) ListSignals(_ context.Context) ([]durable.SignalRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]durable.SignalRecord, 0, len(p.signals))
	for _, signal := range p.signals {
		out = append(out, clone(signal))
	}
	sort.Slice(out, func(i, j int) bool {
		return sortKey(out[i].ReceivedAt, out[i].SignalID) < sortKey(out[j].ReceivedAt, out[j].SignalID)
	})
	return out, nil
}

func (p *Provider) ListChildren(_ context.Context) ([]durable.ChildRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]durable.ChildRecord, 0, len(p.children))
	for _, child := range p.children {
		out = append(out, clone(child))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChildRecordID < out[j].ChildRecordID })
	return out, nil
}

func (p *Provider) Close(context.Context) error {
	return nil
}

func (p *Provider) claimTasksForSessionLocked(s *session, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	if input.Limit <= 0 {
		return durable.ClaimShardTasksResult{}, fmt.Errorf("limit must be positive")
	}
	if s.ownerID == "" {
		return durable.ClaimShardTasksResult{}, fmt.Errorf("shard %d is not opened with an owner", s.shardID)
	}
	lease, err := p.requireShardLeaseLocked(s.shardID, s.ownerID, input.Now)
	if err != nil {
		return durable.ClaimShardTasksResult{}, err
	}
	if s.leaseEpoch != 0 && s.leaseEpoch != lease.LeaseEpoch {
		return durable.ClaimShardTasksResult{}, fmt.Errorf("lost shard lease: %d", s.shardID)
	}
	p.expireActivityTimeoutsLocked(input.Now, timeoutFilter{shardID: &s.shardID, ownerID: s.ownerID})
	p.refreshMigrationTasksLocked(s.shardID, input.Now, input.Workflows)
	candidates := p.tasksForShardLocked(s.shardID)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].SortKey < candidates[j].SortKey })
	var out durable.ClaimShardTasksResult
	claimedSequences := map[string]struct{}{}
	for _, task := range candidates {
		if len(out.Claims) >= input.Limit {
			break
		}
		if task.ReadyAt.After(input.Now) || (!task.BlockedUntil.IsZero() && task.BlockedUntil.After(input.Now)) {
			continue
		}
		if p.taskHasUnexpiredPendingAttemptLocked(task, input.Now) {
			continue
		}
		if version, ok := input.Workflows[task.WorkflowName]; !ok || task.WorkflowVersion > version {
			continue
		}
		sequenceKey := sequenceKeyForTask(task)
		if _, ok := claimedSequences[sequenceKey]; ok || p.hasCompetingCurrentClaimLocked(task, lease.LeaseEpoch) {
			continue
		}
		task.ClaimOwnerID = s.ownerID
		task.ClaimEpoch = lease.LeaseEpoch
		task.LeaseUntil = input.Now.Add(input.Lease)
		p.tasks[task.TaskID] = task
		p.claimedSequenceEpochs[sequenceKey] = lease.LeaseEpoch
		claimedSequences[sequenceKey] = struct{}{}
		instance, ok := p.instances[refKey(task.WorkflowID, task.RunID)]
		if !ok || instance.Status != "running" || instance.Sequence != task.Sequence {
			p.deleteTaskLocked(task.TaskID)
			continue
		}
		out.Claims = append(out.Claims, durable.ClaimedActivationWithInstance{
			Activation: activationFromTask(task),
			Instance:   clone(instance),
			Effects:    clone(p.effectsByActivation[task.ActivationID]),
			Lease:      durable.ActivationClaimLease{Scope: "shard", ShardID: s.shardID, Epoch: lease.LeaseEpoch},
		})
	}
	out.NextWakeAt = p.nextWakeAtLocked(s.shardID, input.Now, input.Workflows)
	return out, nil
}

func (p *Provider) commitOneLocked(input durable.CommitCheckpointInput) durable.CommitCheckpointResult {
	instance, ok := p.instances[refKey(input.WorkflowID, input.RunID)]
	conflict := func(reason string, sequence int64, retryable *bool, err durable.SerializedError) durable.CommitCheckpointResult {
		return durable.CommitCheckpointResult{OK: false, Sequence: sequence, Reason: reason, Retryable: retryable, Error: err}
	}
	if !ok || instance.Status != "running" {
		return conflict("not_running", -1, nil, durable.SerializedError{})
	}
	if instance.Sequence != input.ExpectedSequence {
		return conflict("stale_sequence", instance.Sequence, nil, durable.SerializedError{})
	}
	task := p.findTaskClaimedByShardEpochLocked(input.ActivationID, input.WorkerID)
	if task == nil || task.WorkflowID != input.WorkflowID || task.RunID != input.RunID || task.Sequence != input.ExpectedSequence {
		return conflict("lost_shard_task_lease", instance.Sequence, nil, durable.SerializedError{})
	}
	lease := p.shardLeases[task.PartitionShard]
	if !p.replaying && (lease.OwnerID != input.WorkerID || lease.LeaseEpoch != task.ClaimEpoch || lease.LeaseUntil.Before(input.Now)) {
		return conflict("lost_shard_task_lease", instance.Sequence, nil, durable.SerializedError{})
	}
	if !taskMatchesCommit(*task, input) {
		return conflict("activation_event_mismatch", instance.Sequence, nil, durable.SerializedError{})
	}
	if input.ConsumeSignalID != "" {
		signal, ok := p.signals[input.ConsumeSignalID]
		if !ok || signal.WorkflowID != input.WorkflowID || signal.RunID != input.RunID || signal.ConsumedBySequence != nil {
			return conflict("signal_not_consumable", instance.Sequence, nil, durable.SerializedError{})
		}
	}
	if input.ConsumeChildRecordID != "" {
		child, ok := p.children[input.ConsumeChildRecordID]
		if !ok || child.ParentWorkflowID != input.WorkflowID || child.ParentRunID != input.RunID || child.DeliveredBySequence != nil {
			return conflict("child_not_consumable", instance.Sequence, nil, durable.SerializedError{})
		}
	}
	if childConflict := p.validateChildStartsLocked(input.WorkflowID, input.RunID, input.ActivationID, input.ChildStarts); childConflict != nil {
		f := false
		return conflict(childConflict.reason, instance.Sequence, &f, durable.SerializedError{Name: "ChildStartConflict", Message: childConflict.err.Error()})
	}
	previous := clone(instance)
	nextSequence := instance.Sequence + 1
	updated := p.nextInstanceLocked(instance, input, nextSequence)
	p.instances[refKey(input.WorkflowID, input.RunID)] = updated
	if input.ConsumeSignalID != "" {
		signal := p.signals[input.ConsumeSignalID]
		signal.ConsumedBySequence = &nextSequence
		p.signals[input.ConsumeSignalID] = signal
	}
	if input.ConsumeChildRecordID != "" {
		child := p.children[input.ConsumeChildRecordID]
		child.DeliveredBySequence = &nextSequence
		p.children[input.ConsumeChildRecordID] = child
	}
	effects := checkpointEffectsToRecords(input.WorkflowID, input.RunID, input.ActivationID, input.Now, input.Effects)
	for _, effect := range effects {
		p.upsertEffectLocked(input.ActivationID, effect)
	}
	for _, start := range input.ChildStarts {
		p.writeChildStartLocked(start, input.Now, input.WorkflowID, input.RunID, input.ActivationID)
	}
	p.updateParentChildRecordLocked(previous, input)
	p.applyParentClosePolicyLocked(previous, input, nextSequence)
	p.deleteTasksForRefLocked(input.WorkflowID, input.RunID)
	p.replaceTasksForInstanceLocked(updated)
	p.completedActivationClaims = append(p.completedActivationClaims, CompletedActivationClaim{
		ActivationID:        input.ActivationID,
		WorkflowID:          input.WorkflowID,
		RunID:               input.RunID,
		Sequence:            input.ExpectedSequence,
		Kind:                task.Kind,
		OwnerID:             input.WorkerID,
		CompletedBySequence: nextSequence,
	})
	return durable.CommitCheckpointResult{OK: true, Sequence: nextSequence}
}

func (p *Provider) nextInstanceLocked(current durable.PersistedInstance, input durable.CommitCheckpointInput, sequence int64) durable.PersistedInstance {
	base := durable.PersistedInstance{
		WorkflowName:    current.WorkflowName,
		WorkflowVersion: input.WorkflowVersion,
		WorkflowID:      current.WorkflowID,
		RunID:           current.RunID,
		PartitionShard:  current.PartitionShard,
		Sequence:        sequence,
		CreatedAt:       current.CreatedAt,
		UpdatedAt:       input.Now,
		Parent:          clonePtr(current.Parent),
	}
	switch input.Next.Status {
	case "running":
		base.Status = "running"
		base.Common = clone(input.Next.Common)
		base.Phase = clonePtr(input.Next.Phase)
		base.Waits = clone(input.Waits)
	case "completed":
		base.Status = "completed"
		base.Output = clone(input.Next.Output)
	case "canceled":
		base.Status = "canceled"
		base.CancelReason = input.Next.Reason
	default:
		base.Status = "failed"
		base.Error = input.Next.Error
	}
	return base
}

func (p *Provider) replaceTasksForInstanceLocked(instance durable.PersistedInstance) {
	p.deleteTasksForRefLocked(instance.WorkflowID, instance.RunID)
	if instance.Status != "running" {
		return
	}
	for _, wait := range instance.Waits {
		if wait.Kind == "" {
			continue
		}
		p.insertTaskForWaitLocked(instance, wait)
	}
}

func (p *Provider) refreshSignalTasksForInstanceLocked(instance durable.PersistedInstance) {
	for _, task := range p.tasksForRefLocked(instance.WorkflowID, instance.RunID) {
		if task.Sequence == instance.Sequence && task.Kind == "event" && task.Event != nil && task.Event.Kind == "signal" {
			p.deleteTaskLocked(task.TaskID)
		}
	}
	for _, wait := range instance.Waits {
		if wait.Kind == "signal" {
			p.insertTaskForWaitLocked(instance, wait)
		}
	}
}

func (p *Provider) refreshChildTasksForInstanceLocked(instance durable.PersistedInstance) {
	for _, task := range p.tasksForRefLocked(instance.WorkflowID, instance.RunID) {
		if task.Sequence == instance.Sequence && task.Kind == "event" && task.Event != nil && task.Event.Kind == "child" {
			p.deleteTaskLocked(task.TaskID)
		}
	}
	for _, wait := range instance.Waits {
		if wait.Kind == "child" {
			p.insertTaskForWaitLocked(instance, wait)
		}
	}
}

func (p *Provider) insertTaskForWaitLocked(instance durable.PersistedInstance, wait durable.DurableWait) {
	switch wait.Kind {
	case "run":
		readyAt := wait.ReadyAt
		if readyAt.IsZero() {
			readyAt = instance.UpdatedAt
		}
		p.insertTaskLocked(instance, taskInput{kind: "run", eventID: wait.Name, readyAt: readyAt, sortKey: sortKey(readyAt, "run", wait.Name, instance.WorkflowID, instance.RunID)})
	case "timer":
		p.insertTaskLocked(instance, taskInput{
			kind: "event", eventID: wait.Name + ":" + wait.FireAt.Format(time.RFC3339Nano), readyAt: wait.FireAt, wait: &wait,
			event:   &durable.ReadyEvent{Kind: "timer", FiredAt: wait.FireAt, OccurredAt: wait.FireAt},
			sortKey: sortKey(wait.FireAt, "timer", wait.Name, wait.FireAt.Format(time.RFC3339Nano)),
		})
	case "signal":
		var signals []durable.SignalRecord
		for _, signal := range p.signals {
			if signal.WorkflowID == instance.WorkflowID && signal.RunID == instance.RunID && signal.Type == wait.Type && signal.ConsumedBySequence == nil {
				signals = append(signals, signal)
			}
		}
		sort.Slice(signals, func(i, j int) bool {
			return sortKey(signals[i].ReceivedAt, signals[i].Type, signals[i].SignalID) < sortKey(signals[j].ReceivedAt, signals[j].Type, signals[j].SignalID)
		})
		if len(signals) == 0 {
			return
		}
		signal := signals[0]
		p.insertTaskLocked(instance, taskInput{
			kind: "event", eventID: signal.SignalID, readyAt: signal.ReceivedAt, wait: &wait,
			event:      &durable.ReadyEvent{Kind: "signal", SignalID: signal.SignalID, Payload: clone(signal.Payload), OccurredAt: signal.ReceivedAt, ConsumeSignalID: signal.SignalID},
			taskSuffix: wait.Name,
			sortKey:    sortKey(signal.ReceivedAt, "signal", wait.Name, signal.SignalID),
		})
	case "child":
		var children []durable.ChildRecord
		for _, child := range p.children {
			if child.ParentWorkflowID == instance.WorkflowID && child.ParentRunID == instance.RunID &&
				child.WorkflowName == wait.WorkflowName && child.WorkflowVersion == wait.WorkflowVersion &&
				child.WorkflowID == wait.WorkflowID && child.RunID == wait.RunID &&
				(child.Status == "completed" || child.Status == "failed") && child.DeliveredBySequence == nil {
				children = append(children, child)
			}
		}
		sort.Slice(children, func(i, j int) bool {
			leftAt, rightAt := children[i].CompletedAt, children[j].CompletedAt
			if leftAt.IsZero() {
				leftAt = instance.UpdatedAt
			}
			if rightAt.IsZero() {
				rightAt = instance.UpdatedAt
			}
			return sortKey(leftAt, children[i].ChildRecordID) < sortKey(rightAt, children[j].ChildRecordID)
		})
		if len(children) == 0 {
			return
		}
		child := children[0]
		occurredAt := child.CompletedAt
		if occurredAt.IsZero() {
			occurredAt = instance.UpdatedAt
		}
		event := &durable.ChildEvent{OK: child.Status == "completed", Output: clone(child.Output), Error: child.Error}
		p.insertTaskLocked(instance, taskInput{
			kind: "event", eventID: child.ChildRecordID, readyAt: occurredAt, wait: &wait,
			event:   &durable.ReadyEvent{Kind: "child", ChildRecordID: child.ChildRecordID, OccurredAt: occurredAt, Event: event},
			sortKey: sortKey(occurredAt, "child", wait.Name, child.ChildRecordID),
		})
	}
}

type taskInput struct {
	kind       string
	eventID    string
	readyAt    time.Time
	sortKey    string
	wait       *durable.DurableWait
	event      *durable.ReadyEvent
	taskSuffix string
}

func (p *Provider) insertTaskLocked(instance durable.PersistedInstance, input taskInput) {
	activationID := activationIDFromParts(instance.WorkflowID, instance.RunID, instance.Sequence, input.kindOrEvent(), input.eventID)
	taskID := activationID
	if input.taskSuffix != "" {
		taskID += "/" + input.taskSuffix
	}
	p.deleteTaskLocked(taskID)
	task := Task{
		TaskID:          taskID,
		ActivationID:    activationID,
		WorkflowName:    instance.WorkflowName,
		WorkflowVersion: instance.WorkflowVersion,
		WorkflowID:      instance.WorkflowID,
		RunID:           instance.RunID,
		PartitionShard:  instance.PartitionShard,
		Sequence:        instance.Sequence,
		Kind:            input.kind,
		ReadyAt:         input.readyAt,
		SortKey:         input.sortKey,
		Wait:            clonePtr(input.wait),
		Event:           clonePtr(input.event),
	}
	if input.wait != nil {
		task.WaitName = input.wait.Name
	}
	p.tasks[taskID] = task
	addIndex(p.taskIDsByRef, refKey(task.WorkflowID, task.RunID), taskID)
	addIndexInt(p.taskIDsByShard, task.PartitionShard, taskID)
	addIndex(p.taskIDsByActivation, task.ActivationID, taskID)
}

func (i taskInput) kindOrEvent() string {
	if i.event != nil {
		return i.event.Kind
	}
	return i.kind
}

func (p *Provider) refreshMigrationTasksLocked(shardID int, now time.Time, workflows map[string]int) {
	for _, instance := range p.instances {
		if instance.PartitionShard != shardID || instance.Status != "running" {
			continue
		}
		version, ok := workflows[instance.WorkflowName]
		if !ok || instance.WorkflowVersion >= version {
			for _, task := range p.tasksForRefLocked(instance.WorkflowID, instance.RunID) {
				if task.Kind == "migration" && task.Sequence == instance.Sequence {
					p.deleteTaskLocked(task.TaskID)
				}
			}
			continue
		}
		hasCurrent := false
		for _, task := range p.tasksForRefLocked(instance.WorkflowID, instance.RunID) {
			if task.Sequence == instance.Sequence {
				hasCurrent = true
				break
			}
		}
		if !hasCurrent {
			p.insertTaskLocked(instance, taskInput{kind: "migration", eventID: fmt.Sprintf("migration-%d", version), readyAt: now, sortKey: sortKey(now, "migration", instance.WorkflowName, instance.WorkflowID, instance.RunID)})
		}
	}
}

func (p *Provider) deleteTasksForRefLocked(workflowID, runID string) {
	for id := range p.taskIDsByRef[refKey(workflowID, runID)] {
		p.deleteTaskLocked(id)
	}
}

func (p *Provider) deleteTaskLocked(taskID string) {
	task, ok := p.tasks[taskID]
	if !ok {
		return
	}
	delete(p.claimedSequenceEpochs, sequenceKeyForTask(task))
	delete(p.tasks, taskID)
	deleteIndex(p.taskIDsByRef, refKey(task.WorkflowID, task.RunID), taskID)
	deleteIndexInt(p.taskIDsByShard, task.PartitionShard, taskID)
	deleteIndex(p.taskIDsByActivation, task.ActivationID, taskID)
}

func (p *Provider) tasksForShardLocked(shardID int) []Task {
	return p.tasksFromIndexInt(p.taskIDsByShard, shardID)
}

func (p *Provider) tasksForRefLocked(workflowID, runID string) []Task {
	return p.tasksFromIndex(p.taskIDsByRef, refKey(workflowID, runID))
}

func (p *Provider) tasksForActivationLocked(activationID string) []Task {
	return p.tasksFromIndex(p.taskIDsByActivation, activationID)
}

func (p *Provider) tasksFromIndex(index map[string]map[string]struct{}, key string) []Task {
	var out []Task
	for id := range index[key] {
		if task, ok := p.tasks[id]; ok {
			out = append(out, task)
		}
	}
	return out
}

func (p *Provider) tasksFromIndexInt(index map[int]map[string]struct{}, key int) []Task {
	var out []Task
	for id := range index[key] {
		if task, ok := p.tasks[id]; ok {
			out = append(out, task)
		}
	}
	return out
}

func (p *Provider) findClaimedTaskLocked(activationID, workerID string, now time.Time) *Task {
	for _, task := range p.tasksForActivationLocked(activationID) {
		if task.ActivationID == activationID && task.ClaimOwnerID == workerID && !task.LeaseUntil.Before(now) {
			cp := task
			return &cp
		}
	}
	if p.replaying {
		if tasks := p.tasksForActivationLocked(activationID); len(tasks) > 0 {
			return &tasks[0]
		}
	}
	return nil
}

func (p *Provider) findTaskClaimedByShardEpochLocked(activationID, workerID string) *Task {
	for _, task := range p.tasksForActivationLocked(activationID) {
		if task.ActivationID == activationID && task.ClaimOwnerID == workerID && task.ClaimEpoch != 0 {
			cp := task
			return &cp
		}
	}
	if p.replaying {
		if tasks := p.tasksForActivationLocked(activationID); len(tasks) > 0 {
			return &tasks[0]
		}
	}
	return nil
}

func (p *Provider) firstTaskForActivationLocked(activationID string) *Task {
	tasks := p.tasksForActivationLocked(activationID)
	if len(tasks) == 0 {
		return nil
	}
	return &tasks[0]
}

func (p *Provider) hasCompetingCurrentClaimLocked(task Task, epoch int64) bool {
	key := sequenceKeyForTask(task)
	existing, ok := p.claimedSequenceEpochs[key]
	if !ok {
		return false
	}
	if existing != epoch {
		delete(p.claimedSequenceEpochs, key)
		return false
	}
	return true
}

func (p *Provider) releaseActivationLocked(activationID, workerID string) {
	for _, task := range p.tasksForActivationLocked(activationID) {
		if task.ClaimOwnerID == workerID {
			delete(p.claimedSequenceEpochs, sequenceKeyForTask(task))
			task.ClaimOwnerID = ""
			task.ClaimEpoch = 0
			task.LeaseUntil = time.Time{}
			p.tasks[task.TaskID] = task
		}
	}
}

func (p *Provider) requireShardLeaseLocked(shardID int, ownerID string, now time.Time) (durable.ShardLease, error) {
	if p.replaying {
		return durable.ShardLease{ShardID: shardID, OwnerID: ownerID, LeaseUntil: now, LeaseEpoch: p.shardLeases[shardID].LeaseEpoch}, nil
	}
	lease, ok := p.shardLeases[shardID]
	if !ok || lease.OwnerID != ownerID || lease.LeaseUntil.Before(now) {
		return durable.ShardLease{}, fmt.Errorf("lost shard lease: %d", shardID)
	}
	return lease, nil
}

func (p *Provider) requireShardOwnerForRefLocked(workflowID, runID, workerID string, now time.Time) error {
	instance, ok := p.instances[refKey(workflowID, runID)]
	shardID := 0
	if ok {
		shardID = instance.PartitionShard
	}
	_, err := p.requireShardLeaseLocked(shardID, workerID, now)
	return err
}

func (p *Provider) nextWakeAtLocked(shardID int, now time.Time, workflows map[string]int) time.Time {
	var times []time.Time
	for _, task := range p.tasksForShardLocked(shardID) {
		version, ok := workflows[task.WorkflowName]
		if !ok || task.WorkflowVersion > version {
			continue
		}
		if !task.BlockedUntil.IsZero() && task.BlockedUntil.After(now) {
			times = append(times, task.BlockedUntil)
		} else if task.ReadyAt.After(now) {
			times = append(times, task.ReadyAt)
		}
		for _, effect := range p.effectsByActivation[task.ActivationID] {
			for _, candidate := range []time.Time{effect.NextAttemptAt, effect.StartToCloseDeadline, effect.HeartbeatDeadline} {
				if !candidate.IsZero() && candidate.After(now) {
					times = append(times, candidate)
				}
			}
		}
	}
	if len(times) == 0 {
		return time.Time{}
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	return times[0]
}

func (p *Provider) taskHasUnexpiredPendingAttemptLocked(task Task, now time.Time) bool {
	for _, effect := range p.effectsByActivation[task.ActivationID] {
		if effect.Status != "pending" || effect.AttemptID == "" {
			continue
		}
		deadline := earliest(effect.StartToCloseDeadline, effect.HeartbeatDeadline)
		if !deadline.IsZero() && deadline.After(now) {
			return true
		}
	}
	return false
}

type childStartConflict struct {
	reason string
	err    error
}

func (p *Provider) validateChildStartsLocked(workflowID, runID, activationID string, starts []durable.CheckpointChildStart) *childStartConflict {
	seenKeys := map[string]struct{}{}
	seenRefs := map[string]durable.CheckpointChildStart{}
	for _, start := range starts {
		if _, ok := seenKeys[start.Key]; ok {
			return &childStartConflict{reason: "duplicate_child_start_key", err: fmt.Errorf("duplicate child start key: %s", start.Key)}
		}
		seenKeys[start.Key] = struct{}{}
		childRef := refKey(start.WorkflowID, start.RunID)
		if _, ok := seenRefs[childRef]; ok && start.ConflictPolicy != durable.ConflictTerminateExisting {
			return &childStartConflict{reason: "duplicate_child_start_instance", err: fmt.Errorf("duplicate child start instance: %s/%s", start.WorkflowID, start.RunID)}
		}
		seenRefs[childRef] = start
		for _, child := range p.children {
			if child.ParentWorkflowID == workflowID && child.ParentRunID == runID && child.ActivationID == activationID && child.Key == start.Key && start.ConflictPolicy != durable.ConflictTerminateExisting {
				return &childStartConflict{reason: "existing_child_activation_key", err: fmt.Errorf("existing child activation key: %s", start.Key)}
			}
		}
		if _, ok := p.instances[childRef]; ok && start.ConflictPolicy != durable.ConflictTerminateExisting {
			return &childStartConflict{reason: "existing_child_instance", err: fmt.Errorf("child workflow instance already exists: %s/%s", start.WorkflowID, start.RunID)}
		}
	}
	return nil
}

func (p *Provider) writeChildStartLocked(start durable.CheckpointChildStart, now time.Time, parentWorkflowID, parentRunID, activationID string) {
	if start.ConflictPolicy == durable.ConflictTerminateExisting {
		var existingRefs []durable.InstanceRef
		for _, child := range p.children {
			if child.ParentWorkflowID == parentWorkflowID && child.ParentRunID == parentRunID && child.ActivationID == activationID && child.Key == start.Key {
				existingRefs = append(existingRefs, durable.InstanceRef{WorkflowID: child.WorkflowID, RunID: child.RunID})
			}
		}
		for _, ref := range existingRefs {
			p.deleteInstanceRecordsLocked(ref.WorkflowID, ref.RunID)
		}
		p.deleteInstanceRecordsLocked(start.WorkflowID, start.RunID)
	}
	p.childCounter++
	childRecordID := fmt.Sprintf("child-%d", p.childCounter)
	childInstance := durable.PersistedInstance{
		WorkflowName:    start.WorkflowName,
		WorkflowVersion: start.WorkflowVersion,
		WorkflowID:      start.WorkflowID,
		RunID:           start.RunID,
		PartitionShard:  start.PartitionShard,
		Sequence:        0,
		Status:          "running",
		Common:          clone(start.Common),
		Phase:           ptr(clone(start.Phase)),
		Waits:           clone(start.Waits),
		CreatedAt:       now,
		UpdatedAt:       now,
		Parent:          &durable.InstanceParent{WorkflowID: parentWorkflowID, RunID: parentRunID, ChildRecordID: childRecordID},
	}
	p.instances[refKey(start.WorkflowID, start.RunID)] = childInstance
	parentClose := string(start.ParentClosePolicy)
	if parentClose == "" {
		parentClose = string(durable.ParentCloseCancel)
	}
	p.children[childRecordID] = durable.ChildRecord{
		ChildRecordID:     childRecordID,
		ParentWorkflowID:  parentWorkflowID,
		ParentRunID:       parentRunID,
		ActivationID:      activationID,
		Key:               start.Key,
		WorkflowName:      start.WorkflowName,
		WorkflowVersion:   start.WorkflowVersion,
		WorkflowID:        start.WorkflowID,
		RunID:             start.RunID,
		Status:            "started",
		ParentClosePolicy: parentClose,
	}
	p.replaceTasksForInstanceLocked(childInstance)
}

func (p *Provider) updateParentChildRecordLocked(previous durable.PersistedInstance, input durable.CommitCheckpointInput) {
	if previous.Parent == nil || previous.Parent.ChildRecordID == "" || input.Next.Status == "running" {
		return
	}
	child := p.children[previous.Parent.ChildRecordID]
	if child.Status != "started" {
		return
	}
	child.CompletedAt = input.Now
	if input.Next.Status == "completed" {
		child.Status = "completed"
		child.Output = clone(input.Next.Output)
		child.Error = durable.SerializedError{}
	} else {
		child.Status = "failed"
		if input.Next.Status == "failed" {
			child.Error = input.Next.Error
		} else {
			child.Error = durable.SerializedError{Name: "ChildCanceled", Message: firstNonEmpty(input.Next.Reason, "child canceled")}
		}
	}
	p.children[child.ChildRecordID] = child
	if parent, ok := p.instances[refKey(child.ParentWorkflowID, child.ParentRunID)]; ok {
		p.refreshChildTasksForInstanceLocked(parent)
	}
}

func (p *Provider) applyParentClosePolicyLocked(previous durable.PersistedInstance, input durable.CommitCheckpointInput, deliveredBySequence int64) {
	if input.Next.Status != "failed" && input.Next.Status != "canceled" {
		return
	}
	for _, child := range p.children {
		if child.ParentWorkflowID == previous.WorkflowID && child.ParentRunID == previous.RunID && child.Status == "started" {
			if child.ParentClosePolicy == string(durable.ParentCloseAbandon) {
				child.Status = "abandoned"
				child.DeliveredBySequence = &deliveredBySequence
				p.children[child.ChildRecordID] = child
				continue
			}
			p.cancelChildTreeForParentCloseLocked(child, input.Now, deliveredBySequence)
		}
	}
}

func (p *Provider) cancelChildTreeForParentCloseLocked(child durable.ChildRecord, now time.Time, deliveredBySequence int64) {
	if instance, ok := p.instances[refKey(child.WorkflowID, child.RunID)]; ok && instance.Status == "running" {
		instance.Status = "canceled"
		instance.CancelReason = "Child canceled because parent closed"
		instance.Waits = nil
		instance.UpdatedAt = now
		p.instances[refKey(child.WorkflowID, child.RunID)] = instance
		p.deleteTasksForRefLocked(child.WorkflowID, child.RunID)
	}
	child.Status = "failed"
	child.CompletedAt = now
	child.Error = durable.SerializedError{Name: "ParentClosed", Message: "Child canceled because parent closed"}
	child.DeliveredBySequence = &deliveredBySequence
	p.children[child.ChildRecordID] = child
	for _, descendant := range p.children {
		if descendant.ParentWorkflowID == child.WorkflowID && descendant.ParentRunID == child.RunID && descendant.Status == "started" {
			if descendant.ParentClosePolicy == string(durable.ParentCloseAbandon) {
				descendant.Status = "abandoned"
				descendant.DeliveredBySequence = &deliveredBySequence
				p.children[descendant.ChildRecordID] = descendant
			} else {
				p.cancelChildTreeForParentCloseLocked(descendant, now, deliveredBySequence)
			}
		}
	}
}

func (p *Provider) deleteInstanceRecordsLocked(workflowID, runID string) {
	delete(p.instances, refKey(workflowID, runID))
	p.deleteTasksForRefLocked(workflowID, runID)
	for id, signal := range p.signals {
		if signal.WorkflowID == workflowID && signal.RunID == runID {
			delete(p.signals, id)
		}
	}
	for id, child := range p.children {
		if (child.ParentWorkflowID == workflowID && child.ParentRunID == runID) || (child.WorkflowID == workflowID && child.RunID == runID) {
			delete(p.children, id)
		}
	}
}

func (p *Provider) effectForLocked(activationID, key string) *durable.EffectRecord {
	for _, effect := range p.effectsByActivation[activationID] {
		if effect.Key == key {
			cp := effect
			return &cp
		}
	}
	return nil
}

func (p *Provider) upsertEffectLocked(activationID string, next durable.EffectRecord) {
	effects := p.effectsByActivation[activationID]
	for i, effect := range effects {
		if effect.Key == next.Key {
			effects[i] = next
			p.effectsByActivation[activationID] = effects
			return
		}
	}
	p.effectsByActivation[activationID] = append(effects, next)
}

func (p *Provider) mutableEffectLocked(activationID, workerID, effectID, attemptID string, now time.Time) (*durable.EffectRecord, error) {
	if p.findClaimedTaskLocked(activationID, workerID, now) == nil {
		return nil, fmt.Errorf("lost activation lease: %s", activationID)
	}
	for _, effect := range p.effectsByActivation[activationID] {
		if effect.EffectID == effectID && effect.AttemptID == attemptID && effect.AttemptOwnerID == workerID && effect.Status == "pending" {
			cp := effect
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("lost effect attempt: %s/%s", effectID, attemptID)
}

type timeoutFilter struct {
	activationID string
	ownerID      string
	shardID      *int
}

func (p *Provider) expireActivityTimeoutsLocked(now time.Time, filter timeoutFilter) {
	for activationID, effects := range p.effectsByActivation {
		if filter.activationID != "" && activationID != filter.activationID {
			continue
		}
		task := p.firstTaskForActivationLocked(activationID)
		if filter.shardID != nil && (task == nil || task.PartitionShard != *filter.shardID) {
			continue
		}
		if filter.ownerID != "" && (task == nil || task.ClaimOwnerID != filter.ownerID || !p.shardLeaseMatchesTaskLocked(*task, filter.ownerID, now)) {
			continue
		}
		changed := false
		for i := range effects {
			effect := &effects[i]
			if effect.Status != "pending" || effect.AttemptID == "" {
				continue
			}
			timeoutKind := ""
			if !effect.StartToCloseDeadline.IsZero() && !effect.StartToCloseDeadline.After(now) {
				timeoutKind = "start_to_close"
			} else if !effect.HeartbeatDeadline.IsZero() && !effect.HeartbeatDeadline.After(now) {
				timeoutKind = "heartbeat"
			}
			if timeoutKind == "" {
				continue
			}
			err := durable.SerializedError{Name: "ActivityTimeoutError", Message: fmt.Sprintf("activity %s failed due to %s timeout", effect.Key, timeoutKind)}
			decision := retryDecision(*effect, err, now, true)
			effect.TimedOutAt = now
			effect.TimeoutKind = timeoutKind
			effect.LastFailure = err
			effect.Error = err
			effect.AttemptID = ""
			effect.AttemptOwnerID = ""
			effect.AttemptStartedAt = time.Time{}
			effect.StartToCloseDeadline = time.Time{}
			effect.HeartbeatDeadline = time.Time{}
			if task != nil {
				task.ClaimOwnerID = ""
				task.ClaimEpoch = 0
				task.LeaseUntil = time.Time{}
				delete(p.claimedSequenceEpochs, sequenceKeyForTask(*task))
			}
			if decision.Status == "retry_scheduled" {
				effect.Status = "pending"
				effect.NextAttemptAt = decision.NextAttemptAt
				effect.Attempt = decision.NextAttempt
				if task != nil {
					task.BlockedUntil = decision.NextAttemptAt
				}
			} else {
				effect.Status = "failed"
				if task != nil {
					task.BlockedUntil = time.Time{}
				}
			}
			changed = true
		}
		if changed {
			p.effectsByActivation[activationID] = effects
			if task != nil {
				p.tasks[task.TaskID] = *task
			}
		}
	}
}

func (p *Provider) shardLeaseMatchesTaskLocked(task Task, ownerID string, now time.Time) bool {
	if p.replaying {
		return true
	}
	lease := p.shardLeases[task.PartitionShard]
	return lease.OwnerID == ownerID && lease.LeaseEpoch == task.ClaimEpoch && !lease.LeaseUntil.Before(now)
}

func activationFromTask(task Task) durable.ClaimedActivation {
	out := durable.ClaimedActivation{
		Kind:           task.Kind,
		ActivationID:   task.ActivationID,
		WorkflowName:   task.WorkflowName,
		WorkflowID:     task.WorkflowID,
		RunID:          task.RunID,
		Sequence:       task.Sequence,
		ActivationTime: task.ReadyAt,
		LeaseUntil:     task.LeaseUntil,
	}
	if out.LeaseUntil.IsZero() {
		out.LeaseUntil = task.ReadyAt
	}
	if task.Kind == "event" {
		out.WaitName = task.WaitName
		out.Wait = clonePtr(task.Wait)
		out.Event = clonePtr(task.Event)
	}
	return out
}

func taskMatchesCommit(task Task, input durable.CommitCheckpointInput) bool {
	if task.Kind != "event" {
		return input.ConsumeSignalID == "" && input.ConsumeChildRecordID == ""
	}
	if task.Event == nil {
		return false
	}
	switch task.Event.Kind {
	case "signal":
		return input.ConsumeSignalID == task.Event.ConsumeSignalID && input.ConsumeChildRecordID == ""
	case "child":
		return input.ConsumeChildRecordID == task.Event.ChildRecordID && input.ConsumeSignalID == ""
	default:
		return input.ConsumeSignalID == "" && input.ConsumeChildRecordID == ""
	}
}

func checkpointEffectsToRecords(workflowID, runID, activationID string, now time.Time, mutations []durable.CheckpointEffectMutation) []durable.EffectRecord {
	var out []durable.EffectRecord
	for _, mutation := range mutations {
		status := mutation.Status
		if status == "retry_scheduled" {
			status = "pending"
		}
		attempt := mutation.Attempt
		if attempt == 0 {
			attempt = 1
		}
		if mutation.Status == "retry_scheduled" && mutation.NextAttempt > 0 {
			attempt = mutation.NextAttempt
		}
		out = append(out, durable.EffectRecord{
			EffectID:               fmt.Sprintf("effect-%s-%s", durable.SafeID(activationID), durable.SafeID(mutation.Key)),
			ActivationID:           activationID,
			Key:                    mutation.Key,
			IdempotencyKey:         firstNonEmpty(mutation.IdempotencyKey, fmt.Sprintf("%s/%s/%s/%s", workflowID, runID, activationID, mutation.Key)),
			Status:                 status,
			Attempt:                attempt,
			AttemptID:              fmt.Sprintf("attempt-%s-%s-%d", durable.SafeID(activationID), durable.SafeID(mutation.Key), attempt),
			FirstAttemptStartedAt:  firstNonZero(mutation.FirstAttemptStartedAt, now),
			MaxAttempts:            firstNonZeroInt(mutation.MaxAttempts, 3),
			MaxElapsed:             mutation.MaxElapsed,
			InitialInterval:        firstNonZeroDuration(mutation.InitialInterval, time.Second),
			MaxInterval:            firstNonZeroDuration(mutation.MaxInterval, 30*time.Second),
			BackoffCoefficient:     firstNonZeroFloat(mutation.BackoffCoefficient, 2),
			NonRetryableErrorNames: clone(mutation.NonRetryableErrorNames),
			NextAttemptAt:          mutation.NextAttemptAt,
			Result:                 clone(mutation.Result),
			Error:                  mutation.Error,
			LastFailure:            mutation.Error,
			HeartbeatDetails:       clone(mutation.HeartbeatDetails),
		})
	}
	return out
}

func childStartFromCreate(input durable.CreateChildInstanceInput) durable.CheckpointChildStart {
	return durable.CheckpointChildStart{
		Key:               input.Key,
		WorkflowName:      input.WorkflowName,
		WorkflowVersion:   input.WorkflowVersion,
		WorkflowID:        input.WorkflowID,
		RunID:             input.RunID,
		PartitionShard:    input.PartitionShard,
		Common:            input.Common,
		Phase:             input.Phase,
		Waits:             input.Waits,
		ParentClosePolicy: input.ParentClosePolicy,
		ConflictPolicy:    input.ConflictPolicy,
	}
}

func childHandle(record durable.ChildRecord) durable.ChildHandleAny {
	return durable.ChildHandleAny{WorkflowName: record.WorkflowName, WorkflowVersion: record.WorkflowVersion, WorkflowID: record.WorkflowID, RunID: record.RunID}
}

func refKey(workflowID, runID string) string {
	return workflowID + "\x00" + runID
}

func activationIDFromParts(workflowID, runID string, sequence int64, kind, eventID string) string {
	return fmt.Sprintf("%s/%s/%d/%s/%s", workflowID, runID, sequence, kind, eventID)
}

func sequenceKeyForTask(task Task) string {
	return fmt.Sprintf("%s\x00%s\x00%d", task.WorkflowID, task.RunID, task.Sequence)
}

func sortKey(parts ...any) string {
	strs := make([]string, 0, len(parts))
	for _, part := range parts {
		switch v := part.(type) {
		case time.Time:
			strs = append(strs, v.UTC().Format(time.RFC3339Nano))
		default:
			strs = append(strs, fmt.Sprint(v))
		}
	}
	return strings.Join(strs, "\x00")
}

func addIndex(index map[string]map[string]struct{}, key, value string) {
	if index[key] == nil {
		index[key] = map[string]struct{}{}
	}
	index[key][value] = struct{}{}
}

func addIndexInt(index map[int]map[string]struct{}, key int, value string) {
	if index[key] == nil {
		index[key] = map[string]struct{}{}
	}
	index[key][value] = struct{}{}
}

func deleteIndex(index map[string]map[string]struct{}, key, value string) {
	delete(index[key], value)
	if len(index[key]) == 0 {
		delete(index, key)
	}
}

func deleteIndexInt(index map[int]map[string]struct{}, key int, value string) {
	delete(index[key], value)
	if len(index[key]) == 0 {
		delete(index, key)
	}
}

func clone[T any](value T) T {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

func ptr[T any](value T) *T {
	return &value
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	cp := clone(*value)
	return &cp
}

func earliest(values ...time.Time) time.Time {
	var out time.Time
	for _, value := range values {
		if value.IsZero() {
			continue
		}
		if out.IsZero() || value.Before(out) {
			out = value
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonZero(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

func firstNonZeroInt(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func firstNonZeroDuration(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

func firstNonZeroFloat(value, fallback float64) float64 {
	if value == 0 {
		return fallback
	}
	return value
}

func effectIDFor(activationID, key string) string {
	return fmt.Sprintf("effect-%s-%s", durable.SafeID(activationID), durable.SafeID(key))
}

func attemptIDFor(activationID, key string, attempt int, workerID string, now time.Time) string {
	return fmt.Sprintf("attempt-%s-%s-%d-%s-%s", durable.SafeID(activationID), durable.SafeID(key), attempt, durable.SafeID(workerID), durable.SafeID(now.Format(time.RFC3339Nano)))
}

type normalizedEffectOptions struct {
	startToCloseTimeout    time.Duration
	heartbeatTimeout       time.Duration
	maxAttempts            int
	maxElapsed             time.Duration
	initialInterval        time.Duration
	maxInterval            time.Duration
	backoffCoefficient     float64
	nonRetryableErrorNames []string
}

func normalizeEffectOptions(input durable.ReserveEffectInput) normalizedEffectOptions {
	retry := input.Options.Retry
	return normalizedEffectOptions{
		startToCloseTimeout:    input.Options.StartToCloseTimeout,
		heartbeatTimeout:       input.Options.HeartbeatTimeout,
		maxAttempts:            firstNonZeroInt(firstNonZeroInt(retry.MaxAttempts, input.MaxAttempts), 3),
		maxElapsed:             retry.MaxElapsed,
		initialInterval:        firstNonZeroDuration(retry.InitialInterval, time.Second),
		maxInterval:            firstNonZeroDuration(retry.MaxInterval, 30*time.Second),
		backoffCoefficient:     firstNonZeroFloat(retry.BackoffCoefficient, 2),
		nonRetryableErrorNames: clone(input.Options.NonRetryableErrorNames),
	}
}

func retryDecision(effect durable.EffectRecord, err durable.SerializedError, now time.Time, retryable bool) durable.FailEffectResult {
	if !retryable {
		return durable.FailEffectResult{Status: "failed"}
	}
	maxAttempts := firstNonZeroInt(effect.MaxAttempts, 3)
	nextAttempt := effect.Attempt + 1
	if nextAttempt > maxAttempts {
		return durable.FailEffectResult{Status: "failed"}
	}
	if effect.MaxElapsed > 0 && !effect.FirstAttemptStartedAt.IsZero() && now.Sub(effect.FirstAttemptStartedAt) >= effect.MaxElapsed {
		return durable.FailEffectResult{Status: "failed"}
	}
	for _, name := range effect.NonRetryableErrorNames {
		if name != "" && (name == err.Name || strings.Contains(err.Name, name)) {
			return durable.FailEffectResult{Status: "failed"}
		}
	}
	interval := firstNonZeroDuration(effect.InitialInterval, time.Second)
	coefficient := firstNonZeroFloat(effect.BackoffCoefficient, 2)
	for i := 2; i < nextAttempt; i++ {
		interval = time.Duration(float64(interval) * coefficient)
	}
	maxInterval := firstNonZeroDuration(effect.MaxInterval, 30*time.Second)
	if interval > maxInterval {
		interval = maxInterval
	}
	return durable.FailEffectResult{Status: "retry_scheduled", NextAttemptAt: now.Add(interval), NextAttempt: nextAttempt}
}

type session struct {
	provider   *Provider
	shardID    int
	ownerID    string
	leaseEpoch int64
}

func (s *session) ShardID() int      { return s.shardID }
func (s *session) OwnerID() string   { return s.ownerID }
func (s *session) LeaseEpoch() int64 { return s.leaseEpoch }

func (s *session) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.InstanceRef, error) {
	if input.PartitionShard != s.shardID {
		return durable.InstanceRef{}, fmt.Errorf("shard session %d cannot write shard %d", s.shardID, input.PartitionShard)
	}
	return s.provider.CreateInstance(ctx, input)
}
func (s *session) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	return s.provider.CreateChildInstance(ctx, input)
}
func (s *session) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	return s.provider.CancelChild(ctx, input)
}
func (s *session) ReadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return s.provider.LoadInstance(ctx, ref, options)
}
func (s *session) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	return s.provider.AppendSignal(ctx, input)
}
func (s *session) ClaimTasks(_ context.Context, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	return s.provider.claimTasksForSessionLocked(s, input)
}
func (s *session) Heartbeat(_ context.Context, now time.Time, lease time.Duration) error {
	if s.ownerID == "" {
		return nil
	}
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	current, ok := s.provider.shardLeases[s.shardID]
	if !ok || current.OwnerID != s.ownerID || current.LeaseUntil.Before(now) {
		return fmt.Errorf("lost shard lease: %d", s.shardID)
	}
	current.LeaseUntil = now.Add(lease)
	s.provider.shardLeases[s.shardID] = current
	return nil
}
func (s *session) Release(_ context.Context) error {
	if s.ownerID == "" {
		return nil
	}
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	current := s.provider.shardLeases[s.shardID]
	if current.OwnerID == s.ownerID {
		current.LeaseUntil = time.Unix(0, 0).UTC()
		s.provider.shardLeases[s.shardID] = current
	}
	return nil
}
func (s *session) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	return s.provider.GetOrReserveEffect(ctx, input)
}
func (s *session) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	return s.provider.HeartbeatEffect(ctx, input)
}
func (s *session) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	return s.provider.CompleteEffect(ctx, input)
}
func (s *session) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	return s.provider.FailEffect(ctx, input)
}
func (s *session) CommitActivations(ctx context.Context, input []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	return s.provider.CommitActivations(ctx, input)
}
func (s *session) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	return s.provider.CommitCheckpoint(ctx, input)
}
func (s *session) RecordActivationFailures(ctx context.Context, input []durable.RecordActivationFailureInput) error {
	return s.provider.RecordActivationFailures(ctx, input)
}
func (s *session) ReleaseActivation(_ context.Context, activationID string, workerID string) error {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	s.provider.releaseActivationLocked(activationID, workerID)
	return nil
}
