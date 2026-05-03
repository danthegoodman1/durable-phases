package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"time"
)

type JSON = any

type SerializedError struct {
	Name    string `json:"name,omitempty"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable error"
	}
	return e.Err.Error()
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}

func SerializeError(err error) SerializedError {
	if err == nil {
		return SerializedError{}
	}
	return SerializedError{
		Name:    reflect.TypeOf(err).String(),
		Message: err.Error(),
	}
}

func IsNonRetryable(err error) bool {
	var target NonRetryableError
	return errors.As(err, &target)
}

type InstanceRef struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type WorkflowContract[I any, O any] struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type WorkflowContractAny struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func (c WorkflowContract[I, O]) Any() WorkflowContractAny {
	return WorkflowContractAny{Name: c.Name, Version: c.Version}
}

type PhaseSnapshot struct {
	Name string `json:"name"`
	Data JSON   `json:"data"`
}

type Start struct {
	Common JSON          `json:"common,omitempty"`
	Phase  PhaseSnapshot `json:"phase"`
}

type TransitionKind string

const (
	TransitionStay     TransitionKind = "stay"
	TransitionGo       TransitionKind = "go"
	TransitionComplete TransitionKind = "complete"
	TransitionCancel   TransitionKind = "cancel"
	TransitionFail     TransitionKind = "fail"
)

type Transition struct {
	Kind   TransitionKind  `json:"kind"`
	Phase  PhaseSnapshot   `json:"phase,omitempty"`
	Output JSON            `json:"output,omitempty"`
	Reason string          `json:"reason,omitempty"`
	Error  SerializedError `json:"error,omitempty"`
}

func Stay(phase PhaseSnapshot) Transition {
	return Transition{Kind: TransitionStay, Phase: phase}
}

func Go(phase PhaseSnapshot) Transition {
	return Transition{Kind: TransitionGo, Phase: phase}
}

func Complete(output JSON) Transition {
	return Transition{Kind: TransitionComplete, Output: output}
}

func Cancel(reason string) Transition {
	return Transition{Kind: TransitionCancel, Reason: reason}
}

func Fail(err error) Transition {
	return Transition{Kind: TransitionFail, Error: SerializeError(err)}
}

type InstanceStatus struct {
	Status string          `json:"status"`
	Common JSON            `json:"common,omitempty"`
	Phase  *PhaseSnapshot  `json:"phase,omitempty"`
	Output JSON            `json:"output,omitempty"`
	Reason string          `json:"reason,omitempty"`
	Error  SerializedError `json:"error,omitempty"`
}

func Running(common JSON, phase PhaseSnapshot) InstanceStatus {
	return InstanceStatus{Status: "running", Common: common, Phase: &phase}
}

type InstanceParent struct {
	WorkflowID    string `json:"workflowId"`
	RunID         string `json:"runId"`
	ChildRecordID string `json:"childRecordId"`
}

type PersistedInstance struct {
	WorkflowName    string          `json:"workflowName"`
	WorkflowVersion int             `json:"workflowVersion"`
	WorkflowID      string          `json:"workflowId"`
	RunID           string          `json:"runId"`
	PartitionShard  int             `json:"partitionShard"`
	Sequence        int64           `json:"sequence"`
	Status          string          `json:"status"`
	Common          JSON            `json:"common,omitempty"`
	Phase           *PhaseSnapshot  `json:"phase,omitempty"`
	Output          JSON            `json:"output,omitempty"`
	Error           SerializedError `json:"error,omitempty"`
	CancelReason    string          `json:"cancelReason,omitempty"`
	Waits           []DurableWait   `json:"waits"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	Parent          *InstanceParent `json:"parent,omitempty"`
	Effects         []EffectRecord  `json:"effects,omitempty"`
}

type LoadInstanceOptions struct {
	IncludeEffects bool `json:"includeEffects,omitempty"`
}

type DurableWait struct {
	Kind            string    `json:"kind"`
	Name            string    `json:"name"`
	ReadyAt         time.Time `json:"readyAt,omitempty"`
	Type            string    `json:"type,omitempty"`
	Scope           string    `json:"scope,omitempty"`
	FireAt          time.Time `json:"fireAt,omitempty"`
	WorkflowName    string    `json:"workflowName,omitempty"`
	WorkflowVersion int       `json:"workflowVersion,omitempty"`
	WorkflowID      string    `json:"workflowId,omitempty"`
	RunID           string    `json:"runId,omitempty"`
}

func RunWait(readyAt time.Time) DurableWait {
	return DurableWait{Kind: "run", Name: "__run", ReadyAt: readyAt}
}

func SignalWait(name, typ string, global bool) DurableWait {
	scope := "phase"
	if global {
		scope = "global"
	}
	return DurableWait{Kind: "signal", Name: name, Type: typ, Scope: scope}
}

func TimerWait(name string, fireAt time.Time) DurableWait {
	if fireAt.IsZero() {
		return DurableWait{}
	}
	return DurableWait{Kind: "timer", Name: name, FireAt: fireAt}
}

func ChildWait(name string, handle ChildHandleAny) DurableWait {
	return DurableWait{
		Kind:            "child",
		Name:            name,
		WorkflowName:    handle.WorkflowName,
		WorkflowVersion: handle.WorkflowVersion,
		WorkflowID:      handle.WorkflowID,
		RunID:           handle.RunID,
	}
}

type SignalRecord struct {
	SignalID           string    `json:"signalId"`
	WorkflowID         string    `json:"workflowId"`
	RunID              string    `json:"runId"`
	Type               string    `json:"type"`
	Payload            JSON      `json:"payload"`
	ReceivedAt         time.Time `json:"receivedAt"`
	ConsumedBySequence *int64    `json:"consumedBySequence,omitempty"`
}

type ChildHandle[O any] struct {
	WorkflowName    string `json:"workflowName"`
	WorkflowVersion int    `json:"workflowVersion"`
	WorkflowID      string `json:"workflowId"`
	RunID           string `json:"runId"`
}

type ChildHandleAny struct {
	WorkflowName    string `json:"workflowName"`
	WorkflowVersion int    `json:"workflowVersion"`
	WorkflowID      string `json:"workflowId"`
	RunID           string `json:"runId"`
}

func (h ChildHandle[O]) Any() ChildHandleAny {
	return ChildHandleAny{
		WorkflowName:    h.WorkflowName,
		WorkflowVersion: h.WorkflowVersion,
		WorkflowID:      h.WorkflowID,
		RunID:           h.RunID,
	}
}

type ChildEvent struct {
	OK     bool            `json:"ok"`
	Output JSON            `json:"output,omitempty"`
	Error  SerializedError `json:"error,omitempty"`
}

type ChildRecord struct {
	ChildRecordID       string          `json:"childRecordId"`
	ParentWorkflowID    string          `json:"parentWorkflowId"`
	ParentRunID         string          `json:"parentRunId"`
	ActivationID        string          `json:"activationId"`
	Key                 string          `json:"key"`
	WorkflowName        string          `json:"workflowName"`
	WorkflowVersion     int             `json:"workflowVersion"`
	WorkflowID          string          `json:"workflowId"`
	RunID               string          `json:"runId"`
	Status              string          `json:"status"`
	ParentClosePolicy   string          `json:"parentClosePolicy"`
	CompletedAt         time.Time       `json:"completedAt,omitempty"`
	Output              JSON            `json:"output,omitempty"`
	Error               SerializedError `json:"error,omitempty"`
	DeliveredBySequence *int64          `json:"deliveredBySequence,omitempty"`
}

type ConflictPolicy string

const (
	ConflictUseExisting       ConflictPolicy = "use_existing"
	ConflictFail              ConflictPolicy = "fail"
	ConflictTerminateExisting ConflictPolicy = "terminate_existing"
)

type ParentClosePolicy string

const (
	ParentCloseCancel  ParentClosePolicy = "cancel"
	ParentCloseAbandon ParentClosePolicy = "abandon"
)

type ActivityDurability string

const (
	ActivityCheckpoint ActivityDurability = "checkpoint"
	ActivityEager      ActivityDurability = "eager"
)

type ActivityOptions struct {
	Durability             ActivityDurability `json:"durability,omitempty"`
	StartToCloseTimeout    time.Duration      `json:"startToCloseTimeout,omitempty"`
	HeartbeatTimeout       time.Duration      `json:"heartbeatTimeout,omitempty"`
	Retry                  RetryPolicy        `json:"retry,omitempty"`
	NonRetryableErrorNames []string           `json:"nonRetryableErrorNames,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts        int           `json:"maxAttempts,omitempty"`
	MaxElapsed         time.Duration `json:"maxElapsed,omitempty"`
	InitialInterval    time.Duration `json:"initialInterval,omitempty"`
	MaxInterval        time.Duration `json:"maxInterval,omitempty"`
	BackoffCoefficient float64       `json:"backoffCoefficient,omitempty"`
}

type ActivityContext struct {
	IdempotencyKey       string
	Attempt              int
	LastHeartbeatDetails JSON
	Signal               <-chan struct{}
	heartbeat            func(context.Context, JSON) error
}

func (c ActivityContext) Heartbeat(ctx context.Context, details JSON) error {
	if c.heartbeat == nil {
		return nil
	}
	return c.heartbeat(ctx, details)
}

type ChildOptions struct {
	WorkflowID        string
	Durability        ActivityDurability
	ParentClosePolicy ParentClosePolicy
	ConflictPolicy    ConflictPolicy
}

func DefaultChildOptions() ChildOptions {
	return ChildOptions{
		Durability:        ActivityCheckpoint,
		ParentClosePolicy: ParentCloseCancel,
		ConflictPolicy:    ConflictUseExisting,
	}
}

type QueryContext struct {
	Sequence int64
	Snapshot InstanceStatus
}

type MigrationArgs struct {
	Common      JSON
	Phase       PhaseSnapshot
	FromVersion int
	ToVersion   int
}

type MigrationResult struct {
	Common JSON
	Phase  *PhaseSnapshot
}

type Workflow interface {
	Name() string
	Version() int
	Initial(context.Context, JSON) (Start, error)
	MaterializeWaits(context.Context, JSON, PhaseSnapshot, time.Time) ([]DurableWait, error)
	DispatchRun(context.Context, *Context, JSON, PhaseSnapshot) (Transition, error)
	DispatchEvent(context.Context, *Context, JSON, PhaseSnapshot, string, ReadyEvent) (Transition, error)
	Query(context.Context, string, QueryContext) (JSON, error)
	Migrate(context.Context, int, MigrationArgs) (*MigrationResult, error)
}

type ReadyEvent struct {
	Kind            string      `json:"kind"`
	SignalID        string      `json:"signalId,omitempty"`
	Payload         JSON        `json:"payload,omitempty"`
	OccurredAt      time.Time   `json:"occurredAt"`
	ConsumeSignalID string      `json:"consumeSignalId,omitempty"`
	FiredAt         time.Time   `json:"firedAt,omitempty"`
	ChildRecordID   string      `json:"childRecordId,omitempty"`
	Event           *ChildEvent `json:"event,omitempty"`
}

type CreateInstanceInput struct {
	WorkflowName    string          `json:"workflowName"`
	WorkflowVersion int             `json:"workflowVersion"`
	WorkflowID      string          `json:"workflowId"`
	RunID           string          `json:"runId"`
	PartitionShard  int             `json:"partitionShard"`
	Common          JSON            `json:"common"`
	Phase           PhaseSnapshot   `json:"phase"`
	Waits           []DurableWait   `json:"waits"`
	Now             time.Time       `json:"now"`
	Parent          *InstanceParent `json:"parent,omitempty"`
	ConflictPolicy  ConflictPolicy  `json:"conflictPolicy,omitempty"`
}

type CreateChildInstanceInput struct {
	CreateInstanceInput
	ParentWorkflowID  string            `json:"parentWorkflowId"`
	ParentRunID       string            `json:"parentRunId"`
	ActivationID      string            `json:"activationId"`
	WorkerID          string            `json:"workerId"`
	LeaseNow          time.Time         `json:"leaseNow"`
	Key               string            `json:"key"`
	ParentClosePolicy ParentClosePolicy `json:"parentClosePolicy,omitempty"`
}

type CancelChildInput struct {
	ParentWorkflowID string    `json:"parentWorkflowId"`
	ParentRunID      string    `json:"parentRunId"`
	ActivationID     string    `json:"activationId"`
	WorkerID         string    `json:"workerId"`
	WorkflowID       string    `json:"workflowId"`
	RunID            string    `json:"runId"`
	Now              time.Time `json:"now"`
}

type AppendSignalInput struct {
	WorkflowID string    `json:"workflowId"`
	RunID      string    `json:"runId"`
	Type       string    `json:"type"`
	Payload    JSON      `json:"payload"`
	ReceivedAt time.Time `json:"receivedAt"`
}

type DispatchShardLease struct {
	ShardID    int       `json:"shardId"`
	OwnerID    string    `json:"ownerId"`
	LeaseUntil time.Time `json:"leaseUntil"`
	LeaseEpoch int64     `json:"leaseEpoch,omitempty"`
}

type ShardLease = DispatchShardLease

type OpenShardInput struct {
	ShardID    int       `json:"shardId"`
	OwnerID    string    `json:"ownerId,omitempty"`
	LeaseUntil time.Time `json:"leaseUntil,omitempty"`
	LeaseEpoch int64     `json:"leaseEpoch,omitempty"`
}

type ClaimDispatchShardInput struct {
	ShardID int           `json:"shardId"`
	OwnerID string        `json:"ownerId"`
	Now     time.Time     `json:"now"`
	Lease   time.Duration `json:"lease"`
}

type HeartbeatDispatchShardInput = ClaimDispatchShardInput

type ReleaseDispatchShardInput struct {
	ShardID int    `json:"shardId"`
	OwnerID string `json:"ownerId"`
}

type ClaimedActivation struct {
	Kind           string       `json:"kind"`
	ActivationID   string       `json:"activationId"`
	WorkflowName   string       `json:"workflowName"`
	WorkflowID     string       `json:"workflowId"`
	RunID          string       `json:"runId"`
	Sequence       int64        `json:"sequence"`
	ActivationTime time.Time    `json:"activationTime"`
	WaitName       string       `json:"waitName,omitempty"`
	Wait           *DurableWait `json:"wait,omitempty"`
	Event          *ReadyEvent  `json:"event,omitempty"`
	LeaseUntil     time.Time    `json:"leaseUntil"`
}

type ActivationClaimLease struct {
	Scope   string `json:"scope"`
	ShardID int    `json:"shardId,omitempty"`
	Epoch   int64  `json:"epoch,omitempty"`
}

type ClaimedActivationWithInstance struct {
	Activation ClaimedActivation    `json:"activation"`
	Instance   PersistedInstance    `json:"instance"`
	Effects    []EffectRecord       `json:"effects"`
	Lease      ActivationClaimLease `json:"lease"`
}

type ClaimShardTasksInput struct {
	Workflows  map[string]int `json:"workflows"`
	ShardCount int            `json:"shardCount,omitempty"`
	Now        time.Time      `json:"now"`
	Lease      time.Duration  `json:"lease"`
	Limit      int            `json:"limit"`
}

type ClaimShardTasksResult struct {
	Claims     []ClaimedActivationWithInstance `json:"claims"`
	NextWakeAt time.Time                       `json:"nextWakeAt,omitempty"`
}

type EffectRecord struct {
	EffectID               string          `json:"effectId"`
	ActivationID           string          `json:"activationId"`
	Key                    string          `json:"key"`
	IdempotencyKey         string          `json:"idempotencyKey"`
	Status                 string          `json:"status"`
	Attempt                int             `json:"attempt,omitempty"`
	AttemptID              string          `json:"attemptId,omitempty"`
	AttemptOwnerID         string          `json:"attemptOwnerId,omitempty"`
	AttemptStartedAt       time.Time       `json:"attemptStartedAt,omitempty"`
	StartToCloseTimeout    time.Duration   `json:"startToCloseTimeout,omitempty"`
	StartToCloseDeadline   time.Time       `json:"startToCloseDeadline,omitempty"`
	HeartbeatTimeout       time.Duration   `json:"heartbeatTimeout,omitempty"`
	HeartbeatDeadline      time.Time       `json:"heartbeatDeadline,omitempty"`
	MaxAttempts            int             `json:"maxAttempts,omitempty"`
	MaxElapsed             time.Duration   `json:"maxElapsed,omitempty"`
	InitialInterval        time.Duration   `json:"initialInterval,omitempty"`
	MaxInterval            time.Duration   `json:"maxInterval,omitempty"`
	BackoffCoefficient     float64         `json:"backoffCoefficient,omitempty"`
	FirstAttemptStartedAt  time.Time       `json:"firstAttemptStartedAt,omitempty"`
	NextAttemptAt          time.Time       `json:"nextAttemptAt,omitempty"`
	LastFailure            SerializedError `json:"lastFailure,omitempty"`
	NonRetryableErrorNames []string        `json:"nonRetryableErrorNames,omitempty"`
	TimedOutAt             time.Time       `json:"timedOutAt,omitempty"`
	TimeoutKind            string          `json:"timeoutKind,omitempty"`
	Result                 JSON            `json:"result,omitempty"`
	Error                  SerializedError `json:"error,omitempty"`
	HeartbeatAt            time.Time       `json:"heartbeatAt,omitempty"`
	HeartbeatDetails       JSON            `json:"heartbeatDetails,omitempty"`
}

type EffectReservation struct {
	Status           string          `json:"status"`
	EffectID         string          `json:"effectId,omitempty"`
	IdempotencyKey   string          `json:"idempotencyKey,omitempty"`
	Attempt          int             `json:"attempt,omitempty"`
	AttemptID        string          `json:"attemptId,omitempty"`
	HeartbeatDetails JSON            `json:"heartbeatDetails,omitempty"`
	Result           JSON            `json:"result,omitempty"`
	Error            SerializedError `json:"error,omitempty"`
}

type ReserveEffectInput struct {
	WorkflowID   string          `json:"workflowId"`
	RunID        string          `json:"runId"`
	ActivationID string          `json:"activationId"`
	WorkerID     string          `json:"workerId"`
	Key          string          `json:"key"`
	Now          time.Time       `json:"now"`
	Options      ActivityOptions `json:"options,omitempty"`
	MaxAttempts  int             `json:"maxAttempts,omitempty"`
}

type HeartbeatEffectInput struct {
	WorkflowID   string    `json:"workflowId"`
	RunID        string    `json:"runId"`
	ActivationID string    `json:"activationId"`
	WorkerID     string    `json:"workerId"`
	EffectID     string    `json:"effectId"`
	AttemptID    string    `json:"attemptId"`
	Now          time.Time `json:"now"`
	Details      JSON      `json:"details,omitempty"`
}

type CompleteEffectInput struct {
	WorkflowID   string    `json:"workflowId"`
	RunID        string    `json:"runId"`
	ActivationID string    `json:"activationId"`
	WorkerID     string    `json:"workerId"`
	EffectID     string    `json:"effectId"`
	AttemptID    string    `json:"attemptId"`
	Result       JSON      `json:"result"`
	Now          time.Time `json:"now"`
}

type FailEffectInput struct {
	WorkflowID   string          `json:"workflowId"`
	RunID        string          `json:"runId"`
	ActivationID string          `json:"activationId"`
	WorkerID     string          `json:"workerId"`
	EffectID     string          `json:"effectId"`
	AttemptID    string          `json:"attemptId"`
	Error        SerializedError `json:"error"`
	Now          time.Time       `json:"now"`
	Retryable    *bool           `json:"retryable,omitempty"`
}

type FailEffectResult struct {
	Status        string    `json:"status"`
	NextAttemptAt time.Time `json:"nextAttemptAt,omitempty"`
	NextAttempt   int       `json:"nextAttempt,omitempty"`
}

type CheckpointEffectMutation struct {
	Key                    string          `json:"key"`
	Status                 string          `json:"status"`
	Result                 JSON            `json:"result,omitempty"`
	Error                  SerializedError `json:"error,omitempty"`
	Retryable              *bool           `json:"retryable,omitempty"`
	NextAttemptAt          time.Time       `json:"nextAttemptAt,omitempty"`
	NextAttempt            int             `json:"nextAttempt,omitempty"`
	HeartbeatDetails       JSON            `json:"heartbeatDetails,omitempty"`
	Attempt                int             `json:"attempt,omitempty"`
	IdempotencyKey         string          `json:"idempotencyKey,omitempty"`
	FirstAttemptStartedAt  time.Time       `json:"firstAttemptStartedAt,omitempty"`
	MaxAttempts            int             `json:"maxAttempts,omitempty"`
	MaxElapsed             time.Duration   `json:"maxElapsed,omitempty"`
	InitialInterval        time.Duration   `json:"initialInterval,omitempty"`
	MaxInterval            time.Duration   `json:"maxInterval,omitempty"`
	BackoffCoefficient     float64         `json:"backoffCoefficient,omitempty"`
	NonRetryableErrorNames []string        `json:"nonRetryableErrorNames,omitempty"`
}

type CheckpointChildStart struct {
	Key               string            `json:"key"`
	WorkflowName      string            `json:"workflowName"`
	WorkflowVersion   int               `json:"workflowVersion"`
	WorkflowID        string            `json:"workflowId"`
	RunID             string            `json:"runId"`
	PartitionShard    int               `json:"partitionShard"`
	Common            JSON              `json:"common"`
	Phase             PhaseSnapshot     `json:"phase"`
	Waits             []DurableWait     `json:"waits"`
	ParentClosePolicy ParentClosePolicy `json:"parentClosePolicy,omitempty"`
	ConflictPolicy    ConflictPolicy    `json:"conflictPolicy,omitempty"`
}

type CommitCheckpointInput struct {
	WorkflowID           string                     `json:"workflowId"`
	RunID                string                     `json:"runId"`
	ExpectedSequence     int64                      `json:"expectedSequence"`
	ActivationID         string                     `json:"activationId"`
	WorkerID             string                     `json:"workerId"`
	WorkflowVersion      int                        `json:"workflowVersion"`
	Next                 InstanceStatus             `json:"next"`
	Waits                []DurableWait              `json:"waits"`
	Now                  time.Time                  `json:"now"`
	ConsumeSignalID      string                     `json:"consumeSignalId,omitempty"`
	ConsumeChildRecordID string                     `json:"consumeChildRecordId,omitempty"`
	Effects              []CheckpointEffectMutation `json:"effects,omitempty"`
	ChildStarts          []CheckpointChildStart     `json:"childStarts,omitempty"`
}

type CommitCheckpointResult struct {
	OK           bool            `json:"ok"`
	Sequence     int64           `json:"sequence"`
	Reason       string          `json:"reason,omitempty"`
	Retryable    *bool           `json:"retryable,omitempty"`
	Error        SerializedError `json:"error,omitempty"`
	ActivationID string          `json:"activationId,omitempty"`
}

type CommitActivationsResult struct {
	Results []CommitCheckpointResult `json:"results"`
}

type RecordActivationFailureInput struct {
	WorkflowID        string                     `json:"workflowId"`
	RunID             string                     `json:"runId"`
	ActivationID      string                     `json:"activationId"`
	WorkerID          string                     `json:"workerId"`
	Now               time.Time                  `json:"now"`
	Effects           []CheckpointEffectMutation `json:"effects"`
	ReleaseActivation bool                       `json:"releaseActivation,omitempty"`
}

type DurabilityProvider interface {
	ClaimShard(context.Context, ClaimDispatchShardInput) (*ShardLease, error)
	OpenShard(OpenShardInput) ShardDurabilitySession
	CreateInstance(context.Context, CreateInstanceInput) (InstanceRef, error)
	CreateChildInstance(context.Context, CreateChildInstanceInput) (ChildHandleAny, error)
	CancelChild(context.Context, CancelChildInput) error
	LoadInstance(context.Context, InstanceRef, LoadInstanceOptions) (*PersistedInstance, error)
	AppendSignal(context.Context, AppendSignalInput) (SignalRecord, error)
	ClaimReadyActivations(context.Context, []int, ClaimShardTasksInput) (ClaimShardTasksResult, error)
	HeartbeatActivations(context.Context, []string, string, time.Time, time.Duration) error
	ReleaseActivations(context.Context, []string, string) error
	GetOrReserveEffect(context.Context, ReserveEffectInput) (EffectReservation, error)
	HeartbeatEffect(context.Context, HeartbeatEffectInput) error
	CompleteEffect(context.Context, CompleteEffectInput) error
	FailEffect(context.Context, FailEffectInput) (FailEffectResult, error)
	CommitActivations(context.Context, []CommitCheckpointInput) (CommitActivationsResult, error)
	CommitCheckpoint(context.Context, CommitCheckpointInput) (CommitCheckpointResult, error)
	RecordActivationFailures(context.Context, []RecordActivationFailureInput) error
	ListInstances(context.Context, LoadInstanceOptions) ([]PersistedInstance, error)
	ListSignals(context.Context) ([]SignalRecord, error)
	ListChildren(context.Context) ([]ChildRecord, error)
	Close(context.Context) error
}

type ShardDurabilitySession interface {
	ShardID() int
	OwnerID() string
	LeaseEpoch() int64
	CreateInstance(context.Context, CreateInstanceInput) (InstanceRef, error)
	CreateChildInstance(context.Context, CreateChildInstanceInput) (ChildHandleAny, error)
	CancelChild(context.Context, CancelChildInput) error
	ReadInstance(context.Context, InstanceRef, LoadInstanceOptions) (*PersistedInstance, error)
	AppendSignal(context.Context, AppendSignalInput) (SignalRecord, error)
	ClaimTasks(context.Context, ClaimShardTasksInput) (ClaimShardTasksResult, error)
	Heartbeat(context.Context, time.Time, time.Duration) error
	Release(context.Context) error
	GetOrReserveEffect(context.Context, ReserveEffectInput) (EffectReservation, error)
	HeartbeatEffect(context.Context, HeartbeatEffectInput) error
	CompleteEffect(context.Context, CompleteEffectInput) error
	FailEffect(context.Context, FailEffectInput) (FailEffectResult, error)
	CommitActivations(context.Context, []CommitCheckpointInput) (CommitActivationsResult, error)
	CommitCheckpoint(context.Context, CommitCheckpointInput) (CommitCheckpointResult, error)
	RecordActivationFailures(context.Context, []RecordActivationFailureInput) error
	ReleaseActivation(context.Context, string, string) error
}

type DurableLogger interface {
	Debug(string, map[string]any)
	Info(string, map[string]any)
	Warn(string, map[string]any)
	Error(string, map[string]any)
}

type DurableMetrics interface {
	Counter(string, float64, map[string]any)
	Histogram(string, float64, map[string]any)
	Gauge(string, float64, map[string]any)
}

type Observability struct {
	Logger  DurableLogger
	Metrics DurableMetrics
}

func WorkflowPartitionShard(workflowID, runID string, shardCount int) int {
	if shardCount <= 0 {
		panic("shardCount must be positive")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(workflowID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(runID))
	return int(h.Sum32() % uint32(shardCount))
}

func CloneJSON[T any](value T) (T, error) {
	var zero T
	if any(value) == nil {
		return value, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	if err := json.Unmarshal(data, &zero); err != nil {
		return zero, err
	}
	return zero, nil
}

func MustCloneJSON[T any](value T) T {
	out, err := CloneJSON(value)
	if err != nil {
		panic(err)
	}
	return out
}

func ToJSON(value any) (JSON, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func DecodeJSON[T any](value any) (T, error) {
	var out T
	data, err := json.Marshal(value)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

func SafeID(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func ValidateSignalPayload[T any](payload any) (T, error) {
	return DecodeJSON[T](payload)
}

func Errf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
