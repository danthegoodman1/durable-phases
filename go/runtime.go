package durable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type RuntimeOptions struct {
	WorkerID                  string
	ShardCount                int
	DispatchShardIDs          []int
	MaxConcurrentActivations  int
	ActivationPrefetchLimit   int
	ActivationCommitBatchSize int
	ActivationCommitMaxDelay  time.Duration
	DispatchLease             time.Duration
	ActivationLease           time.Duration
	LeaseHeartbeatInterval    time.Duration
	MinPollInterval           time.Duration
	MaxPollInterval           time.Duration
	Clock                     func() time.Time
	Workflows                 []Workflow
	Observability             Observability
}

type StartOptions struct {
	WorkflowID            string
	WorkflowIDReusePolicy WorkflowIDReusePolicy
}

type DrainOptions struct {
	MaxActivations            int
	MaxConcurrentActivations  int
	ActivationPrefetchLimit   int
	ActivationCommitBatchSize int
	ActivationCommitMaxDelay  time.Duration
}

type RunShardStepOptions struct {
	ShardID                   int
	MaxActivations            int
	MaxConcurrentActivations  int
	ActivationPrefetchLimit   int
	ActivationCommitBatchSize int
	ActivationCommitMaxDelay  time.Duration
}

type RunWorkerOptions struct {
	MaxActivations            int
	StopWhenIdle              bool
	IdleSleep                 time.Duration
	MaxConcurrentActivations  int
	ActivationPrefetchLimit   int
	ActivationCommitBatchSize int
	ActivationCommitMaxDelay  time.Duration
}

type DrainResult struct {
	Activations int
	NextWakeAt  time.Time
}

type RunShardStepResult struct {
	ShardID      int
	ClaimedShard bool
	Activations  int
	NextWakeAt   time.Time
}

type Runtime struct {
	provider  DurabilityProvider
	mu        sync.RWMutex
	workflows map[string]Workflow
	options   RuntimeOptions
}

func NewRuntime(provider DurabilityProvider, options RuntimeOptions) (*Runtime, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider is required")
	}
	if options.WorkerID == "" {
		options.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	if options.ShardCount == 0 {
		options.ShardCount = 1
	}
	if options.MaxConcurrentActivations == 0 {
		options.MaxConcurrentActivations = 4
	}
	if options.ActivationPrefetchLimit == 0 {
		options.ActivationPrefetchLimit = 32
	}
	if options.ActivationCommitBatchSize == 0 {
		options.ActivationCommitBatchSize = 64
	}
	if options.ActivationCommitMaxDelay == 0 {
		options.ActivationCommitMaxDelay = 5 * time.Millisecond
	}
	if options.DispatchLease == 0 {
		options.DispatchLease = 30 * time.Second
	}
	if options.ActivationLease == 0 {
		options.ActivationLease = 30 * time.Second
	}
	if options.LeaseHeartbeatInterval == 0 {
		options.LeaseHeartbeatInterval = minDuration(options.DispatchLease, options.ActivationLease) / 3
		if options.LeaseHeartbeatInterval <= 0 {
			options.LeaseHeartbeatInterval = time.Second
		}
	}
	if options.MinPollInterval == 0 {
		options.MinPollInterval = 10 * time.Millisecond
	}
	if options.MaxPollInterval == 0 {
		options.MaxPollInterval = time.Second
	}
	if options.Clock == nil {
		options.Clock = func() time.Time { return time.Now().UTC() }
	}
	if len(options.DispatchShardIDs) == 0 {
		options.DispatchShardIDs = make([]int, options.ShardCount)
		for i := 0; i < options.ShardCount; i++ {
			options.DispatchShardIDs[i] = i
		}
	}
	if err := validateShardIDs(options.DispatchShardIDs, options.ShardCount); err != nil {
		return nil, err
	}
	r := &Runtime{
		provider:  provider,
		workflows: map[string]Workflow{},
		options:   options,
	}
	r.Register(options.Workflows...)
	return r, nil
}

func (r *Runtime) Register(workflows ...Workflow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, workflow := range workflows {
		if workflow != nil {
			r.workflows[workflow.Name()] = workflow
		}
	}
}

func (r *Runtime) Start(ctx context.Context, workflow Workflow, input JSON, options StartOptions) (StartWorkflowResult, error) {
	r.Register(workflow)
	start, err := workflow.Initial(ctx, input)
	if err != nil {
		return StartWorkflowResult{}, err
	}
	now := r.now()
	workflowID := options.WorkflowID
	if workflowID == "" {
		workflowID = fmt.Sprintf("%s-%d", workflow.Name(), now.UnixNano())
	}
	runID := uuid.NewString()
	reusePolicy := options.WorkflowIDReusePolicy
	if reusePolicy == "" {
		reusePolicy = WorkflowIDReusePolicyNotRunning
	}
	partitionShard := WorkflowPartitionShard(workflowID, runID, r.options.ShardCount)
	waits, err := workflow.MaterializeWaits(ctx, start.Common, start.Phase, now)
	if err != nil {
		return StartWorkflowResult{}, err
	}
	session := r.provider.OpenShard(OpenShardInput{ShardID: partitionShard})
	ref, err := session.CreateInstance(ctx, CreateInstanceInput{
		WorkflowName:          workflow.Name(),
		WorkflowVersion:       workflow.Version(),
		WorkflowID:            workflowID,
		RunID:                 runID,
		PartitionShard:        partitionShard,
		Common:                start.Common,
		Phase:                 start.Phase,
		Waits:                 waits,
		Now:                   now,
		ConflictPolicy:        ConflictFail,
		WorkflowIDReusePolicy: reusePolicy,
	})
	if err == nil {
		r.log("info", "workflow.start", map[string]any{"workflowName": workflow.Name(), "workerId": r.options.WorkerID})
		r.count("durable.workflow.start", map[string]any{"workflowName": workflow.Name(), "workerId": r.options.WorkerID})
	}
	return ref, err
}

func (r *Runtime) Signal(ctx context.Context, workflow Workflow, run WorkflowRunRef, typ string, payload JSON) (SignalRecord, error) {
	r.Register(workflow)
	ref := run.InstanceReference()
	session := r.provider.OpenShard(OpenShardInput{ShardID: WorkflowPartitionShard(ref.WorkflowID, ref.RunID, r.options.ShardCount)})
	signal, err := session.AppendSignal(ctx, AppendSignalInput{
		WorkflowID: ref.WorkflowID,
		RunID:      ref.RunID,
		Type:       typ,
		Payload:    payload,
		ReceivedAt: r.now(),
	})
	if err == nil {
		r.log("info", "workflow.signal", map[string]any{"workflowName": workflow.Name(), "type": typ, "workerId": r.options.WorkerID})
		r.count("durable.workflow.signal", map[string]any{"workflowName": workflow.Name(), "workerId": r.options.WorkerID})
	}
	return signal, err
}

func (r *Runtime) Query(ctx context.Context, workflow Workflow, run WorkflowRunRef, name string) (JSON, error) {
	r.Register(workflow)
	ref := run.InstanceReference()
	instance, err := r.provider.LoadInstance(ctx, ref, LoadInstanceOptions{})
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, fmt.Errorf("unknown workflow instance: %s/%s", ref.WorkflowID, ref.RunID)
	}
	return workflow.Query(ctx, name, QueryContext{Sequence: instance.Sequence, Snapshot: snapshotFromInstance(*instance)})
}

func (r *Runtime) GetWorkflowRuns(ctx context.Context, input GetWorkflowRunsInput) (GetWorkflowRunsResult, error) {
	return r.provider.GetWorkflowRuns(ctx, input)
}

func (r *Runtime) ShardForRef(run WorkflowRunRef) int {
	ref := run.InstanceReference()
	return WorkflowPartitionShard(ref.WorkflowID, ref.RunID, r.options.ShardCount)
}

func (r *Runtime) Drain(ctx context.Context, options DrainOptions) (DrainResult, error) {
	shardSessions, err := r.claimShardSessions(ctx)
	if err != nil {
		return DrainResult{}, err
	}
	defer func() {
		for _, session := range shardSessions {
			_ = session.Release(context.Background())
		}
	}()
	return r.drainOwnedShards(ctx, shardSessions, drainSettings{
		maxActivations:            positive(options.MaxActivations, 100),
		maxConcurrentActivations:  positive(options.MaxConcurrentActivations, r.options.MaxConcurrentActivations),
		activationPrefetchLimit:   positive(options.ActivationPrefetchLimit, r.options.ActivationPrefetchLimit),
		activationCommitBatchSize: positive(options.ActivationCommitBatchSize, r.options.ActivationCommitBatchSize),
		activationCommitMaxDelay:  defaultDuration(options.ActivationCommitMaxDelay, r.options.ActivationCommitMaxDelay),
	})
}

func (r *Runtime) RunShardStep(ctx context.Context, options RunShardStepOptions) (RunShardStepResult, error) {
	if err := validateShardID(options.ShardID, r.options.ShardCount, "shard id"); err != nil {
		return RunShardStepResult{}, err
	}
	result := RunShardStepResult{ShardID: options.ShardID}
	session, claimed, err := r.claimShardSession(ctx, options.ShardID)
	if err != nil {
		return result, err
	}
	if !claimed {
		return result, nil
	}
	result.ClaimedShard = true
	defer func() {
		_ = session.Release(context.Background())
	}()
	drainResult, err := r.drainOwnedShards(ctx, []ShardDurabilitySession{session}, drainSettings{
		maxActivations:            positive(options.MaxActivations, 100),
		maxConcurrentActivations:  positive(options.MaxConcurrentActivations, r.options.MaxConcurrentActivations),
		activationPrefetchLimit:   positive(options.ActivationPrefetchLimit, r.options.ActivationPrefetchLimit),
		activationCommitBatchSize: positive(options.ActivationCommitBatchSize, r.options.ActivationCommitBatchSize),
		activationCommitMaxDelay:  defaultDuration(options.ActivationCommitMaxDelay, r.options.ActivationCommitMaxDelay),
	})
	result.Activations = drainResult.Activations
	result.NextWakeAt = drainResult.NextWakeAt
	if err != nil {
		return result, err
	}
	return result, nil
}

func (r *Runtime) RunWorker(ctx context.Context, options RunWorkerOptions) (DrainResult, error) {
	idleSleep := defaultDuration(options.IdleSleep, r.options.MinPollInterval)
	if idleSleep <= 0 {
		idleSleep = time.Millisecond
	}
	var total DrainResult
	for {
		if ctx.Err() != nil {
			return total, nil
		}
		maxActivations := options.MaxActivations
		if maxActivations > 0 && total.Activations >= maxActivations {
			return total, nil
		}
		remaining := 100
		if maxActivations > 0 {
			remaining = maxActivations - total.Activations
		}
		result, err := r.Drain(ctx, DrainOptions{
			MaxActivations:            remaining,
			MaxConcurrentActivations:  options.MaxConcurrentActivations,
			ActivationPrefetchLimit:   options.ActivationPrefetchLimit,
			ActivationCommitBatchSize: options.ActivationCommitBatchSize,
			ActivationCommitMaxDelay:  options.ActivationCommitMaxDelay,
		})
		if err != nil {
			return total, err
		}
		total.Activations += result.Activations
		total.NextWakeAt = earliestTime(total.NextWakeAt, result.NextWakeAt)
		if result.Activations > 0 {
			idleSleep = defaultDuration(options.IdleSleep, r.options.MinPollInterval)
			continue
		}
		if options.StopWhenIdle {
			return total, nil
		}
		delay := idleSleep
		if !result.NextWakeAt.IsZero() {
			until := time.Until(result.NextWakeAt)
			if until > 0 && until < delay {
				delay = until
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return total, nil
		case <-timer.C:
		}
		idleSleep *= 2
		if idleSleep > r.options.MaxPollInterval {
			idleSleep = r.options.MaxPollInterval
		}
	}
}

type drainSettings struct {
	maxActivations            int
	maxConcurrentActivations  int
	activationPrefetchLimit   int
	activationCommitBatchSize int
	activationCommitMaxDelay  time.Duration
}

func (r *Runtime) drainOwnedShards(ctx context.Context, sessions []ShardDurabilitySession, settings drainSettings) (DrainResult, error) {
	if len(sessions) == 0 || settings.maxActivations <= 0 {
		return DrainResult{}, nil
	}
	var result DrainResult
	for result.Activations < settings.maxActivations {
		batch, nextWakeAt, err := r.claimBatch(ctx, sessions, minInt(settings.activationPrefetchLimit, settings.maxActivations-result.Activations))
		if err != nil {
			return result, err
		}
		result.NextWakeAt = earliestTime(result.NextWakeAt, nextWakeAt)
		if len(batch) == 0 {
			return result, nil
		}
		var commits []preparedActivation
		for _, claim := range batch {
			prepared, err := r.prepareActivationCommit(ctx, claim)
			if err != nil {
				_ = claim.Session.ReleaseActivation(ctx, claim.Claim.Activation.ActivationID, r.options.WorkerID)
				return result, err
			}
			if prepared != nil {
				commits = append(commits, *prepared)
			} else {
				_ = claim.Session.ReleaseActivation(ctx, claim.Claim.Activation.ActivationID, r.options.WorkerID)
			}
			result.Activations++
			if result.Activations >= settings.maxActivations {
				break
			}
		}
		if err := r.commitPrepared(ctx, commits, settings.activationCommitBatchSize); err != nil {
			return result, err
		}
	}
	return result, nil
}

type runtimeClaim struct {
	Session ShardDurabilitySession
	Claim   ClaimedActivationWithInstance
}

func (r *Runtime) claimBatch(ctx context.Context, sessions []ShardDurabilitySession, limit int) ([]runtimeClaim, time.Time, error) {
	if limit <= 0 {
		return nil, time.Time{}, nil
	}
	versions := r.workflowVersions()
	var out []runtimeClaim
	var nextWakeAt time.Time
	for _, session := range sessions {
		if len(out) >= limit {
			break
		}
		result, err := session.ClaimTasks(ctx, ClaimShardTasksInput{
			Workflows:  versions,
			ShardCount: r.options.ShardCount,
			Now:        r.now(),
			Lease:      r.options.ActivationLease,
			Limit:      limit - len(out),
		})
		if err != nil {
			return nil, time.Time{}, err
		}
		nextWakeAt = earliestTime(nextWakeAt, result.NextWakeAt)
		for _, claim := range result.Claims {
			out = append(out, runtimeClaim{Session: session, Claim: claim})
		}
	}
	return out, nextWakeAt, nil
}

type preparedActivation struct {
	session ShardDurabilitySession
	input   CommitCheckpointInput
}

func (r *Runtime) prepareActivationCommit(ctx context.Context, claim runtimeClaim) (*preparedActivation, error) {
	activation := claim.Claim.Activation
	instance := claim.Claim.Instance
	workflow, err := r.workflow(instance.WorkflowName)
	if err != nil {
		return nil, err
	}
	if instance.Status != "running" || instance.Sequence != activation.Sequence || instance.Phase == nil {
		return nil, nil
	}
	if activation.Kind == "migration" {
		next, err := r.migrateSnapshot(ctx, workflow, instance)
		if err != nil {
			return nil, err
		}
		waits, err := waitsForStatus(ctx, workflow, next, r.now())
		if err != nil {
			return nil, err
		}
		return &preparedActivation{session: claim.Session, input: CommitCheckpointInput{
			WorkflowID:       instance.WorkflowID,
			RunID:            instance.RunID,
			ExpectedSequence: instance.Sequence,
			ActivationID:     activation.ActivationID,
			WorkerID:         r.options.WorkerID,
			WorkflowVersion:  workflow.Version(),
			Next:             next,
			Waits:            waits,
			Now:              r.now(),
		}}, nil
	}
	dctx := &Context{
		runtime:              r,
		provider:             r.provider,
		workflowID:           instance.WorkflowID,
		runID:                instance.RunID,
		partitionShard:       instance.PartitionShard,
		sequence:             instance.Sequence,
		activationID:         activation.ActivationID,
		workerID:             r.options.WorkerID,
		shardCount:           r.options.ShardCount,
		commitEffects:        map[string]CheckpointEffectMutation{},
		commitChildren:       map[string]CheckpointChildStart{},
		commitChildRefs:      map[string]string{},
		commitChildWorkflows: map[string]string{},
	}
	var transition Transition
	if activation.Kind == "run" {
		transition, err = workflow.DispatchRun(ctx, dctx, instance.Common, *instance.Phase)
	} else {
		if activation.Event == nil {
			return nil, fmt.Errorf("event activation missing event")
		}
		transition, err = workflow.DispatchEvent(ctx, dctx, instance.Common, *instance.Phase, activation.WaitName, *activation.Event)
	}
	if err != nil {
		return nil, err
	}
	next, err := r.applyTransition(workflow, instance, transition)
	if err != nil {
		return nil, err
	}
	waits, err := waitsForStatus(ctx, workflow, next, r.now())
	if err != nil {
		return nil, err
	}
	var consumeSignalID, consumeChildRecordID string
	if activation.Event != nil {
		if activation.Event.Kind == "signal" {
			consumeSignalID = activation.Event.ConsumeSignalID
		}
		if activation.Event.Kind == "child" {
			consumeChildRecordID = activation.Event.ChildRecordID
		}
	}
	return &preparedActivation{session: claim.Session, input: CommitCheckpointInput{
		WorkflowID:           instance.WorkflowID,
		RunID:                instance.RunID,
		ExpectedSequence:     instance.Sequence,
		ActivationID:         activation.ActivationID,
		WorkerID:             r.options.WorkerID,
		WorkflowVersion:      workflow.Version(),
		Next:                 next,
		Waits:                waits,
		Now:                  r.now(),
		ConsumeSignalID:      consumeSignalID,
		ConsumeChildRecordID: consumeChildRecordID,
		Effects:              mapValues(dctx.commitEffects),
		ChildStarts:          mapValues(dctx.commitChildren),
	}}, nil
}

func (r *Runtime) commitPrepared(ctx context.Context, prepared []preparedActivation, batchSize int) error {
	if len(prepared) == 0 {
		return nil
	}
	groups := map[ShardDurabilitySession][]CommitCheckpointInput{}
	for _, item := range prepared {
		groups[item.session] = append(groups[item.session], item.input)
	}
	for session, inputs := range groups {
		for len(inputs) > 0 {
			n := minInt(positive(batchSize, len(inputs)), len(inputs))
			result, err := session.CommitActivations(ctx, inputs[:n])
			if err != nil {
				return err
			}
			for _, one := range result.Results {
				if one.OK || (one.Retryable != nil && *one.Retryable) {
					continue
				}
				if one.Error.Message != "" {
					return errors.New(one.Error.Message)
				}
				return fmt.Errorf("activation commit failed: %s", one.Reason)
			}
			inputs = inputs[n:]
		}
	}
	return nil
}

func (r *Runtime) claimShardSessions(ctx context.Context) ([]ShardDurabilitySession, error) {
	var sessions []ShardDurabilitySession
	for _, shardID := range r.options.DispatchShardIDs {
		session, claimed, err := r.claimShardSession(ctx, shardID)
		if err != nil {
			return nil, err
		}
		if claimed {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}

func (r *Runtime) claimShardSession(ctx context.Context, shardID int) (ShardDurabilitySession, bool, error) {
	lease, err := r.provider.ClaimShard(ctx, ClaimDispatchShardInput{
		ShardID: shardID,
		OwnerID: r.options.WorkerID,
		Now:     r.now(),
		Lease:   r.options.DispatchLease,
	})
	if err != nil {
		return nil, false, err
	}
	if lease == nil {
		return nil, false, nil
	}
	return r.provider.OpenShard(OpenShardInput{ShardID: lease.ShardID, OwnerID: lease.OwnerID, LeaseUntil: lease.LeaseUntil, LeaseEpoch: lease.LeaseEpoch}), true, nil
}

func (r *Runtime) applyTransition(workflow Workflow, instance PersistedInstance, transition Transition) (InstanceStatus, error) {
	switch transition.Kind {
	case TransitionStay:
		if instance.Phase == nil {
			return InstanceStatus{}, fmt.Errorf("running workflow has no phase")
		}
		if transition.Phase.Name == "" {
			transition.Phase.Name = instance.Phase.Name
		}
		if transition.Phase.Name != instance.Phase.Name {
			return InstanceStatus{}, fmt.Errorf("stay transition cannot change phase from %s to %s", instance.Phase.Name, transition.Phase.Name)
		}
		return Running(instance.Common, transition.Phase), nil
	case TransitionGo:
		return Running(instance.Common, transition.Phase), nil
	case TransitionComplete:
		return InstanceStatus{Status: "completed", Output: transition.Output}, nil
	case TransitionCancel:
		return InstanceStatus{Status: "canceled", Reason: transition.Reason}, nil
	case TransitionFail:
		return InstanceStatus{Status: "failed", Error: transition.Error}, nil
	default:
		return InstanceStatus{}, fmt.Errorf("unknown transition kind: %s", transition.Kind)
	}
}

func (r *Runtime) migrateSnapshot(ctx context.Context, workflow Workflow, instance PersistedInstance) (InstanceStatus, error) {
	common := instance.Common
	phase := *instance.Phase
	for version := instance.WorkflowVersion; version < workflow.Version(); version++ {
		result, err := workflow.Migrate(ctx, version, MigrationArgs{Common: common, Phase: phase, FromVersion: version, ToVersion: version + 1})
		if err != nil {
			return InstanceStatus{}, err
		}
		if result != nil {
			if result.Common != nil {
				common = result.Common
			}
			if result.Phase != nil {
				phase = *result.Phase
			}
		}
	}
	return Running(common, phase), nil
}

func waitsForStatus(ctx context.Context, workflow Workflow, status InstanceStatus, now time.Time) ([]DurableWait, error) {
	if status.Status != "running" || status.Phase == nil {
		return nil, nil
	}
	return workflow.MaterializeWaits(ctx, status.Common, *status.Phase, now)
}

func snapshotFromInstance(instance PersistedInstance) InstanceStatus {
	switch instance.Status {
	case "running":
		return InstanceStatus{Status: "running", Common: instance.Common, Phase: instance.Phase}
	case "completed":
		return InstanceStatus{Status: "completed", Output: instance.Output}
	case "canceled":
		return InstanceStatus{Status: "canceled", Reason: instance.CancelReason}
	default:
		return InstanceStatus{Status: "failed", Error: instance.Error}
	}
}

func (r *Runtime) workflow(name string) (Workflow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	workflow, ok := r.workflows[name]
	if !ok {
		return nil, fmt.Errorf("unknown workflow: %s", name)
	}
	return workflow, nil
}

func (r *Runtime) workflowVersions() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]int{}
	for name, workflow := range r.workflows {
		out[name] = workflow.Version()
	}
	return out
}

func (r *Runtime) now() time.Time {
	return r.options.Clock().UTC()
}

func (r *Runtime) log(level, event string, fields map[string]any) {
	logger := r.options.Observability.Logger
	if logger == nil {
		return
	}
	defer func() { _ = recover() }()
	switch level {
	case "debug":
		logger.Debug(event, fields)
	case "info":
		logger.Info(event, fields)
	case "warn":
		logger.Warn(event, fields)
	case "error":
		logger.Error(event, fields)
	}
}

func (r *Runtime) count(name string, tags map[string]any) {
	if r.options.Observability.Metrics == nil {
		return
	}
	defer func() { _ = recover() }()
	r.options.Observability.Metrics.Counter(name, 1, tags)
}

func validateShardIDs(ids []int, shardCount int) error {
	seen := map[int]struct{}{}
	for _, id := range ids {
		if err := validateShardID(id, shardCount, "dispatch shard id"); err != nil {
			return err
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("dispatch shard ids must not contain duplicates")
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validateShardID(id, shardCount int, label string) error {
	if id < 0 || id >= shardCount {
		return fmt.Errorf("%s %d outside 0..%d", label, id, shardCount-1)
	}
	return nil
}

func mapValues[T any](input map[string]T) []T {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]T, 0, len(input))
	for _, key := range keys {
		out = append(out, input[key])
	}
	return out
}

func positive(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func defaultDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func earliestTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}

type Context struct {
	runtime              *Runtime
	provider             DurabilityProvider
	workflowID           string
	runID                string
	partitionShard       int
	sequence             int64
	activationID         string
	workerID             string
	shardCount           int
	commitEffects        map[string]CheckpointEffectMutation
	commitChildren       map[string]CheckpointChildStart
	commitChildRefs      map[string]string
	commitChildWorkflows map[string]string
}

func (c *Context) Now() time.Time {
	return c.runtime.now()
}

func (c *Context) Activity(ctx context.Context, key string, fn func(context.Context) (JSON, error), options ActivityOptions) (JSON, error) {
	return c.ActivityWithContext(ctx, key, options, func(ctx context.Context, _ ActivityContext) (JSON, error) {
		return fn(ctx)
	})
}

func (c *Context) ActivityWithContext(ctx context.Context, key string, options ActivityOptions, fn func(context.Context, ActivityContext) (JSON, error)) (JSON, error) {
	if options.Durability == "" {
		options.Durability = ActivityCheckpoint
	}
	if options.Durability == ActivityCheckpoint && options.StartToCloseTimeout == 0 && options.HeartbeatTimeout == 0 {
		if existing, ok := c.commitEffects[key]; ok {
			if existing.Status == "completed" {
				return existing.Result, nil
			}
			if existing.Status == "failed" {
				return nil, errors.New(existing.Error.Message)
			}
		}
		result, err := fn(ctx, ActivityContext{
			IdempotencyKey: fmt.Sprintf("%s/%s/%s/%s", c.workflowID, c.runID, c.activationID, key),
			Attempt:        1,
			Signal:         ctx.Done(),
		})
		if err != nil {
			c.commitEffects[key] = CheckpointEffectMutation{Key: key, Status: "failed", Error: SerializeError(err)}
			return nil, err
		}
		c.commitEffects[key] = CheckpointEffectMutation{Key: key, Status: "completed", Result: result}
		return result, nil
	}
	reservation, err := c.provider.GetOrReserveEffect(ctx, ReserveEffectInput{
		WorkflowID: c.workflowID, RunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, Key: key, Now: c.Now(), Options: options, MaxAttempts: options.Retry.MaxAttempts,
	})
	if err != nil {
		return nil, err
	}
	switch reservation.Status {
	case "completed":
		return reservation.Result, nil
	case "failed":
		return nil, errors.New(reservation.Error.Message)
	}
	actx := ActivityContext{
		IdempotencyKey:       reservation.IdempotencyKey,
		Attempt:              reservation.Attempt,
		LastHeartbeatDetails: reservation.HeartbeatDetails,
		Signal:               ctx.Done(),
		heartbeat: func(ctx context.Context, details JSON) error {
			return c.provider.HeartbeatEffect(ctx, HeartbeatEffectInput{
				WorkflowID: c.workflowID, RunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, EffectID: reservation.EffectID, AttemptID: reservation.AttemptID, Now: c.Now(), Details: details,
			})
		},
	}
	_ = actx
	result, err := fn(ctx, actx)
	if err != nil {
		_, failErr := c.provider.FailEffect(ctx, FailEffectInput{
			WorkflowID: c.workflowID, RunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, EffectID: reservation.EffectID, AttemptID: reservation.AttemptID, Error: SerializeError(err), Now: c.Now(),
		})
		if failErr != nil {
			return nil, failErr
		}
		return nil, err
	}
	if err := c.provider.CompleteEffect(ctx, CompleteEffectInput{
		WorkflowID: c.workflowID, RunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, EffectID: reservation.EffectID, AttemptID: reservation.AttemptID, Result: result, Now: c.Now(),
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func Activity[T any](ctx context.Context, dctx *Context, key string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	raw, err := dctx.Activity(ctx, key, func(ctx context.Context) (JSON, error) {
		value, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return ToJSON(value)
	}, ActivityOptions{})
	if err != nil {
		return zero, err
	}
	return DecodeJSON[T](raw)
}

func ActivityWithOptions[T any](ctx context.Context, dctx *Context, key string, options ActivityOptions, fn func(context.Context, ActivityContext) (T, error)) (T, error) {
	var zero T
	raw, err := dctx.ActivityWithContext(ctx, key, options, func(ctx context.Context, activity ActivityContext) (JSON, error) {
		value, err := fn(ctx, activity)
		if err != nil {
			return nil, err
		}
		return ToJSON(value)
	})
	if err != nil {
		return zero, err
	}
	return DecodeJSON[T](raw)
}

func (c *Context) ChildStart(ctx context.Context, key string, workflow Workflow, input JSON, options ChildOptions) (ChildHandleAny, error) {
	c.runtime.Register(workflow)
	start, err := workflow.Initial(ctx, input)
	if err != nil {
		return ChildHandleAny{}, err
	}
	now := c.Now()
	workflowID := options.WorkflowID
	if workflowID == "" {
		workflowID = defaultChildWorkflowID(c.workflowID, c.runID, c.sequence, key, c.shardCount, c.partitionShard)
	}
	reusePolicy := options.WorkflowIDReusePolicy
	if reusePolicy == "" {
		reusePolicy = WorkflowIDReusePolicyNotRunning
	}
	if reusePolicy != WorkflowIDReusePolicyAlways {
		if existingKey, ok := c.commitChildWorkflows[workflowID]; ok {
			if existing, ok := c.commitChildren[existingKey]; ok {
				return ChildHandleAny{WorkflowName: existing.WorkflowName, WorkflowVersion: existing.WorkflowVersion, WorkflowID: existing.WorkflowID, RunID: existing.RunID, Created: false}, nil
			}
		}
	}
	var latest *PersistedInstance
	if reusePolicy != WorkflowIDReusePolicyAlways {
		runs, err := c.provider.GetWorkflowRuns(ctx, GetWorkflowRunsInput{ID: workflowID, Direction: WorkflowRunDirectionDesc, Limit: 1})
		if err != nil {
			return ChildHandleAny{}, err
		}
		if len(runs.Runs) > 0 {
			latest = &runs.Runs[0]
		}
	}
	created := latest == nil || ShouldCreateWorkflowRun(reusePolicy, latest.Status)
	runID := uuid.NewString()
	if !created {
		runID = latest.RunID
	}
	partitionShard := WorkflowPartitionShard(workflowID, runID, c.shardCount)
	waits, err := workflow.MaterializeWaits(ctx, start.Common, start.Phase, now)
	if err != nil {
		return ChildHandleAny{}, err
	}
	if options.ParentClosePolicy == "" {
		options.ParentClosePolicy = ParentCloseCancel
	}
	if options.Durability == "" {
		options.Durability = ActivityCheckpoint
	}
	if options.Durability == ActivityCheckpoint {
		start := CheckpointChildStart{
			Key: key, WorkflowName: workflow.Name(), WorkflowVersion: workflow.Version(), WorkflowID: workflowID, RunID: runID, PartitionShard: partitionShard,
			Common: start.Common, Phase: start.Phase, Waits: waits, ParentClosePolicy: options.ParentClosePolicy, ConflictPolicy: ConflictFail, WorkflowIDReusePolicy: reusePolicy, Created: created,
		}
		refKey := workflowID + "\x00" + runID
		if existingKey, ok := c.commitChildRefs[refKey]; ok {
			if existing, ok := c.commitChildren[existingKey]; ok {
				return ChildHandleAny{WorkflowName: existing.WorkflowName, WorkflowVersion: existing.WorkflowVersion, WorkflowID: existing.WorkflowID, RunID: existing.RunID, Created: existing.Created}, nil
			}
		}
		c.commitChildren[key] = start
		c.commitChildRefs[refKey] = key
		c.commitChildWorkflows[workflowID] = key
		return ChildHandleAny{WorkflowName: workflow.Name(), WorkflowVersion: workflow.Version(), WorkflowID: workflowID, RunID: runID, Created: created}, nil
	}
	return c.provider.CreateChildInstance(ctx, CreateChildInstanceInput{
		CreateInstanceInput: CreateInstanceInput{
			WorkflowName: workflow.Name(), WorkflowVersion: workflow.Version(), WorkflowID: workflowID, RunID: runID, PartitionShard: partitionShard,
			Common: start.Common, Phase: start.Phase, Waits: waits, Now: now, ConflictPolicy: ConflictFail, WorkflowIDReusePolicy: reusePolicy,
		},
		ParentWorkflowID: c.workflowID, ParentRunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, LeaseNow: now, Key: key, ParentClosePolicy: options.ParentClosePolicy,
	})
}

func ChildStart[I any, O any](ctx context.Context, dctx *Context, key string, workflow Workflow, input I, options ChildOptions) (ChildHandle[O], error) {
	jsonInput, err := ToJSON(input)
	if err != nil {
		return ChildHandle[O]{}, err
	}
	raw, err := dctx.ChildStart(ctx, key, workflow, jsonInput, options)
	if err != nil {
		return ChildHandle[O]{}, err
	}
	return ChildHandle[O]{WorkflowName: raw.WorkflowName, WorkflowVersion: raw.WorkflowVersion, WorkflowID: raw.WorkflowID, RunID: raw.RunID, Created: raw.Created}, nil
}

func (c *Context) ChildCancel(ctx context.Context, handle ChildHandleAny) error {
	refKey := handle.WorkflowID + "\x00" + handle.RunID
	if key, ok := c.commitChildRefs[refKey]; ok {
		delete(c.commitChildRefs, refKey)
		if start, ok := c.commitChildren[key]; ok && c.commitChildWorkflows[start.WorkflowID] == key {
			delete(c.commitChildWorkflows, start.WorkflowID)
		}
		delete(c.commitChildren, key)
		return nil
	}
	return c.provider.CancelChild(ctx, CancelChildInput{
		ParentWorkflowID: c.workflowID, ParentRunID: c.runID, ActivationID: c.activationID, WorkerID: c.workerID, WorkflowID: handle.WorkflowID, RunID: handle.RunID, Now: c.Now(),
	})
}

func defaultChildWorkflowID(parentWorkflowID, parentRunID string, sequence int64, key string, shardCount, parentShard int) string {
	base := fmt.Sprintf("%s__%s__%d__%s", parentWorkflowID, parentRunID, sequence, SafeID(key))
	if WorkflowPartitionShard(base, "run-1", shardCount) == parentShard {
		return base
	}
	for i := 0; i < 4096; i++ {
		candidate := fmt.Sprintf("%s__shard_%d", base, i)
		if WorkflowPartitionShard(candidate, "run-1", shardCount) == parentShard {
			return candidate
		}
	}
	panic("could not find shard-affine child workflow id")
}
