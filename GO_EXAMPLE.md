# Durable Phase Workflow Example In Go

This is the section 15 workflow translated into a Go-shaped SDK.

Decision: Go should use **code generation** for workflow authoring. The runtime is a good fit for Go, but the authoring DX is poor if users hand-write phase unions, ready-event types, wait specs, and dispatch switches. A `go generate` step should produce that boilerplate from ordinary Go declarations.

The user-authored code below is the source of truth. Generated files provide the runtime metadata and dispatch layer.

## Runtime-Facing Types

```go
package workflows

import (
	"context"
	"time"
)

type WorkflowContract[I any, O any] struct {
	Name    string
	Version int
}

type WorkflowContractAny struct {
	Name    string
	Version int
}

func (c WorkflowContract[I, O]) Any() WorkflowContractAny {
	return WorkflowContractAny{Name: c.Name, Version: c.Version}
}

type Start[C any, P any] struct {
	Common C
	Phase  P
}

type TransitionKind string

const (
	TransitionStay     TransitionKind = "stay"
	TransitionGo       TransitionKind = "go"
	TransitionComplete TransitionKind = "complete"
	TransitionCancel   TransitionKind = "cancel"
)

type Transition[O any, P any] struct {
	Kind   TransitionKind
	Phase  P
	Output O
	Reason string
	Error  SerializedError
}

func Stay[O any, P any](phase P) Transition[O, P] {
	return Transition[O, P]{Kind: TransitionStay, Phase: phase}
}

func Go[O any, P any](phase P) Transition[O, P] {
	return Transition[O, P]{Kind: TransitionGo, Phase: phase}
}

func Complete[O any, P any](output O) Transition[O, P] {
	return Transition[O, P]{Kind: TransitionComplete, Output: output}
}

func Cancel[O any, P any](reason string) Transition[O, P] {
	return Transition[O, P]{Kind: TransitionCancel, Reason: reason}
}

type SerializedError struct {
	Message string
}

type SnapshotStatus string

const (
	SnapshotRunning   SnapshotStatus = "running"
	SnapshotCompleted SnapshotStatus = "completed"
	SnapshotCanceled  SnapshotStatus = "canceled"
	SnapshotFailed    SnapshotStatus = "failed"
)

type InstanceSnapshot[C any, P any, O any] struct {
	Status       SnapshotStatus
	Common       C
	Phase        P
	Output       O
	CancelReason string
	Error        SerializedError
}

type QueryContext[C any, P any, O any] struct {
	Snapshot InstanceSnapshot[C, P, O]
	Sequence int64
}

type DurableContext interface {
	Now() time.Time

	Activity(
		ctx context.Context,
		key string,
		options ActivityOptions,
		fn func(context.Context, ActivityContext) (any, error),
	) (any, error)

	StartChild(
		ctx context.Context,
		key string,
		contract WorkflowContractAny,
		input any,
		options ChildOptions,
	) (ChildHandleAny, error)
}

type ActivityOptions struct {
	StartToCloseTimeout time.Duration
	HeartbeatTimeout    time.Duration
	Retry               RetryPolicy
}

type RetryPolicy struct {
	MaxAttempts     int
	InitialInterval time.Duration
}

type ActivityContext interface {
	IdempotencyKey() string
	LastHeartbeatDetails() any
	Heartbeat(ctx context.Context, details any) error
}

func Activity[T any](
	ctx context.Context,
	dctx DurableContext,
	key string,
	fn func(context.Context) (T, error),
) (T, error) {
	raw, err := dctx.Activity(ctx, key, ActivityOptions{}, func(ctx context.Context, activity ActivityContext) (any, error) {
		return fn(ctx)
	})
	if err != nil {
		var zero T
		return zero, err
	}

	return raw.(T), nil
}

func ActivityWithOptions[T any](
	ctx context.Context,
	dctx DurableContext,
	key string,
	options ActivityOptions,
	fn func(context.Context, ActivityContext) (T, error),
) (T, error) {
	raw, err := dctx.Activity(ctx, key, options, func(ctx context.Context, activity ActivityContext) (any, error) {
		return fn(ctx, activity)
	})
	if err != nil {
		var zero T
		return zero, err
	}

	return raw.(T), nil
}

type ChildHandle[O any] struct {
	WorkflowName    string
	WorkflowVersion int
	WorkflowID      string
	RunID           string
}

type ChildHandleAny struct {
	WorkflowName    string
	WorkflowVersion int
	WorkflowID      string
	RunID           string
}

type ChildEvent[O any] struct {
	OK     bool
	Output O
	Error  SerializedError
}

type ChildOptions struct {
	WorkflowID        string
	ParentClosePolicy ParentClosePolicy
	ConflictPolicy    ConflictPolicy
}

type ParentClosePolicy string

const (
	ParentCloseCancel  ParentClosePolicy = "cancel"
	ParentCloseAbandon ParentClosePolicy = "abandon"
	ParentCloseWait    ParentClosePolicy = "wait"
)

type ConflictPolicy string

const (
	ConflictUseExisting       ConflictPolicy = "use_existing"
	ConflictFail              ConflictPolicy = "fail"
	ConflictTerminateExisting ConflictPolicy = "terminate_existing"
)

func DefaultChildOptions() ChildOptions {
	return ChildOptions{
		ParentClosePolicy: ParentCloseCancel,
		ConflictPolicy:    ConflictUseExisting,
	}
}

func ChildStart[I any, O any](
	ctx context.Context,
	dctx DurableContext,
	key string,
	contract WorkflowContract[I, O],
	input I,
	options ChildOptions,
) (ChildHandle[O], error) {
	raw, err := dctx.StartChild(ctx, key, contract.Any(), input, options)
	if err != nil {
		return ChildHandle[O]{}, err
	}

	return ChildHandle[O]{
		WorkflowName:    raw.WorkflowName,
		WorkflowVersion: raw.WorkflowVersion,
		WorkflowID:      raw.WorkflowID,
		RunID:           raw.RunID,
	}, nil
}
```

Queries are typed snapshot reads. They load one committed sequence and do not create activations, consume signals, run activities, or checkpoint.

Activity heartbeats are effect metadata only. They help retry or resume long-running activities but never update phase data.

## Domain Types

```go
package workflows

import (
	"context"
	"time"
)

type Document struct {
	ID   string
	Kind string
}

type PartialDocumentSet struct {
	Documents []Document
}

type CompleteDocumentSet struct {
	Documents []Document
}

type KycJob struct {
	ID string
}

type KycPollResult struct {
	Done  bool
	Value KycOutput
}

type KycInput struct {
	CustomerID string
	Documents  CompleteDocumentSet
}

type KycStatus string

const (
	KycApproved KycStatus = "approved"
	KycFailed   KycStatus = "failed"
)

type KycOutput struct {
	Status     KycStatus
	ContractID string
	Reason     string
}

type OnboardingInput struct {
	CustomerID string
}

type OnboardingOutput struct {
	CustomerID  string
	ActivatedAt time.Time
}

type CancelEvent struct {
	Reason string
}

type EmptyEvent struct{}

type KycSummary struct {
	Sequence      int64
	Status        SnapshotStatus
	Phase         string
	ProviderJobID string
}

type OnboardingProgress struct {
	Sequence          int64
	Status            SnapshotStatus
	Phase             string
	DocumentsReceived int
	NextReminderAt    *time.Time
}

type KycSubmitHeartbeat struct {
	Step string
}

func EmptyDocumentSet() PartialDocumentSet {
	return PartialDocumentSet{Documents: []Document{}}
}

func AddDocument(documents PartialDocumentSet, document Document) PartialDocumentSet {
	documents.Documents = append(documents.Documents, document)
	return documents
}

func HasAllRequiredDocuments(documents PartialDocumentSet) bool {
	return len(documents.Documents) > 0
}

func ToCompleteDocumentSet(documents PartialDocumentSet) CompleteDocumentSet {
	return CompleteDocumentSet{Documents: documents.Documents}
}

func MinutesFromNow(minutes int) time.Time {
	return time.Now().Add(time.Duration(minutes) * time.Minute)
}

func DaysFromNow(days int) time.Time {
	return time.Now().Add(time.Duration(days) * 24 * time.Hour)
}
```

## Codegen Model

```go
package workflows

//go:generate durable-gen
```

The generator reads `durable` comments and emits:

```text
workflow contracts
phase union types
ready-event union types
durable wait specs
query specs
run/event dispatch functions
schema registration metadata
```

The string values in comments are durable identifiers stored by the runtime. The developer writes each name once, next to the handler that owns it.

## KYC Child Workflow

```go
package workflows

import (
	"context"
	"time"
)

//durable:workflow name=kyc version=1 input=KycInput output=KycOutput common=KycCommon
type KycWorkflow struct{}

var KycWorkflowContract = WorkflowContract[KycInput, KycOutput]{
	Name:    "kyc",
	Version: 1,
}

type KycCommon struct {
	CustomerID string
}

type KycSubmitting struct {
	Documents CompleteDocumentSet
}

type KycWaitingForProvider struct {
	ProviderJobID string
	NextPollAt    time.Time
}

func (KycWorkflow) Initial(input KycInput) Start[KycCommon, KycPhase] {
	return Start[KycCommon, KycPhase]{
		Common: KycCommon{
			CustomerID: input.CustomerID,
		},
		Phase: KycSubmitting{
			Documents: input.Documents,
		},
	}
}

//durable:query name=summary output=KycSummary
func (KycWorkflow) Summary(ctx QueryContext[KycCommon, KycPhase, KycOutput]) KycSummary {
	summary := KycSummary{
		Sequence: ctx.Sequence,
		Status:   ctx.Snapshot.Status,
	}

	if ctx.Snapshot.Status != SnapshotRunning {
		return summary
	}

	switch phase := ctx.Snapshot.Phase.(type) {
	case KycSubmitting:
		summary.Phase = "submitting"
	case KycWaitingForProvider:
		summary.Phase = "waiting_for_provider"
		summary.ProviderJobID = phase.ProviderJobID
	}

	return summary
}

//durable:phase name=submitting run
func (KycWorkflow) Submitting(
	ctx context.Context,
	dctx DurableContext,
	common KycCommon,
	data KycSubmitting,
) (Transition[KycOutput, KycPhase], error) {
	job, err := ActivityWithOptions(
		ctx,
		dctx,
		"submit_kyc",
		ActivityOptions{
			HeartbeatTimeout: time.Minute,
		},
		func(ctx context.Context, activity ActivityContext) (KycJob, error) {
			return SubmitKycWithHeartbeat(ctx, common.CustomerID, data.Documents, activity)
		},
	)
	if err != nil {
		return Transition[KycOutput, KycPhase]{}, err
	}

	return Go[KycOutput, KycPhase](KycWaitingForProvider{
		ProviderJobID: job.ID,
		NextPollAt:    MinutesFromNow(10),
	}), nil
}

//durable:phase name=waiting_for_provider state=KycWaitingForProvider
type KycWaitingForProviderHandlers struct{}

//durable:signal name=provider_webhook payload=KycOutput
func (KycWaitingForProviderHandlers) ProviderWebhook(
	event KycOutput,
) (Transition[KycOutput, KycPhase], error) {
	return Complete[KycOutput, KycPhase](event), nil
}

//durable:timer name=poll_due at=NextPollAt
func (KycWaitingForProviderHandlers) PollDue(
	ctx context.Context,
	dctx DurableContext,
	data KycWaitingForProvider,
) (Transition[KycOutput, KycPhase], error) {
	result, err := Activity(ctx, dctx, "poll_kyc", func(ctx context.Context) (KycPollResult, error) {
		return PollKycProvider(ctx, data.ProviderJobID)
	})
	if err != nil {
		return Transition[KycOutput, KycPhase]{}, err
	}

	if result.Done {
		return Complete[KycOutput, KycPhase](result.Value), nil
	}

	return Stay[KycOutput, KycPhase](KycWaitingForProvider{
		ProviderJobID: data.ProviderJobID,
		NextPollAt:    MinutesFromNow(10),
	}), nil
}
```

## Parent Onboarding Workflow

```go
package workflows

import (
	"context"
	"time"
)

//durable:workflow name=customer_onboarding version=1 input=OnboardingInput output=OnboardingOutput common=OnboardingCommon
type OnboardingWorkflow struct{}

var OnboardingWorkflowContract = WorkflowContract[OnboardingInput, OnboardingOutput]{
	Name:    "customer_onboarding",
	Version: 1,
}

type OnboardingCommon struct {
	CustomerID string
}

type WaitingForDocuments struct {
	Documents      PartialDocumentSet
	NextReminderAt time.Time
}

type WaitingForKyc struct {
	Kyc ChildHandle[KycOutput]
}

type WaitingForSignature struct {
	ContractID     string
	NextReminderAt time.Time
}

func (OnboardingWorkflow) Initial(input OnboardingInput) Start[OnboardingCommon, OnboardingPhase] {
	return Start[OnboardingCommon, OnboardingPhase]{
		Common: OnboardingCommon{
			CustomerID: input.CustomerID,
		},
		Phase: WaitingForDocuments{
			Documents:      EmptyDocumentSet(),
			NextReminderAt: DaysFromNow(7),
		},
	}
}

//durable:query name=progress output=OnboardingProgress
func (OnboardingWorkflow) Progress(ctx QueryContext[OnboardingCommon, OnboardingPhase, OnboardingOutput]) OnboardingProgress {
	progress := OnboardingProgress{
		Sequence: ctx.Sequence,
		Status:   ctx.Snapshot.Status,
	}

	if ctx.Snapshot.Status != SnapshotRunning {
		return progress
	}

	switch phase := ctx.Snapshot.Phase.(type) {
	case WaitingForDocuments:
		progress.Phase = "waiting_for_documents"
		progress.DocumentsReceived = len(phase.Documents.Documents)
		progress.NextReminderAt = &phase.NextReminderAt
	case WaitingForKyc:
		progress.Phase = "waiting_for_kyc"
	case WaitingForSignature:
		progress.Phase = "waiting_for_signature"
		progress.NextReminderAt = &phase.NextReminderAt
	}

	return progress
}

//durable:global_signal name=customer_canceled payload=CancelEvent
func (OnboardingWorkflow) CustomerCanceled(
	event CancelEvent,
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	return Cancel[OnboardingOutput, OnboardingPhase](event.Reason), nil
}

//durable:phase name=waiting_for_documents state=WaitingForDocuments
type WaitingForDocumentsHandlers struct{}

//durable:signal name=document_uploaded payload=Document
func (WaitingForDocumentsHandlers) DocumentUploaded(
	ctx context.Context,
	dctx DurableContext,
	common OnboardingCommon,
	data WaitingForDocuments,
	event Document,
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	documents := AddDocument(data.Documents, event)

	if !HasAllRequiredDocuments(documents) {
		return Stay[OnboardingOutput, OnboardingPhase](WaitingForDocuments{
			Documents:      documents,
			NextReminderAt: data.NextReminderAt,
		}), nil
	}

	kyc, err := ChildStart(
		ctx,
		dctx,
		"kyc",
		KycWorkflowContract,
		KycInput{
			CustomerID: common.CustomerID,
			Documents:  ToCompleteDocumentSet(documents),
		},
		DefaultChildOptions(),
	)
	if err != nil {
		return Transition[OnboardingOutput, OnboardingPhase]{}, err
	}

	return Go[OnboardingOutput, OnboardingPhase](WaitingForKyc{
		Kyc: kyc,
	}), nil
}

//durable:timer name=reminder_due at=NextReminderAt
func (WaitingForDocumentsHandlers) ReminderDue(
	ctx context.Context,
	dctx DurableContext,
	common OnboardingCommon,
	data WaitingForDocuments,
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	_, err := Activity(ctx, dctx, "send_document_reminder", func(ctx context.Context) (struct{}, error) {
		return struct{}{}, SendDocumentReminder(ctx, common.CustomerID)
	})
	if err != nil {
		return Transition[OnboardingOutput, OnboardingPhase]{}, err
	}

	return Stay[OnboardingOutput, OnboardingPhase](WaitingForDocuments{
		Documents:      data.Documents,
		NextReminderAt: DaysFromNow(7),
	}), nil
}

//durable:phase name=waiting_for_kyc state=WaitingForKyc
type WaitingForKycHandlers struct{}

//durable:child name=kyc_finished handle=Kyc
func (WaitingForKycHandlers) KycFinished(
	event ChildEvent[KycOutput],
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	if !event.OK {
		return Cancel[OnboardingOutput, OnboardingPhase]("KYC workflow failed"), nil
	}

	if event.Output.Status == KycFailed {
		return Cancel[OnboardingOutput, OnboardingPhase](event.Output.Reason), nil
	}

	return Go[OnboardingOutput, OnboardingPhase](WaitingForSignature{
		ContractID:     event.Output.ContractID,
		NextReminderAt: DaysFromNow(3),
	}), nil
}

//durable:phase name=waiting_for_signature state=WaitingForSignature
type WaitingForSignatureHandlers struct{}

//durable:signal name=contract_signed payload=EmptyEvent
func (WaitingForSignatureHandlers) ContractSigned(
	ctx context.Context,
	dctx DurableContext,
	common OnboardingCommon,
	data WaitingForSignature,
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	_, err := Activity(ctx, dctx, "activate_account", func(ctx context.Context) (struct{}, error) {
		return struct{}{}, ActivateAccount(ctx, common.CustomerID, data.ContractID)
	})
	if err != nil {
		return Transition[OnboardingOutput, OnboardingPhase]{}, err
	}

	return Complete[OnboardingOutput, OnboardingPhase](OnboardingOutput{
		CustomerID:  common.CustomerID,
		ActivatedAt: dctx.Now(),
	}), nil
}

//durable:timer name=reminder_due at=NextReminderAt
func (WaitingForSignatureHandlers) ReminderDue(
	ctx context.Context,
	dctx DurableContext,
	common OnboardingCommon,
	data WaitingForSignature,
) (Transition[OnboardingOutput, OnboardingPhase], error) {
	_, err := Activity(ctx, dctx, "send_signature_reminder", func(ctx context.Context) (struct{}, error) {
		return struct{}{}, SendSignatureReminder(ctx, common.CustomerID, data.ContractID)
	})
	if err != nil {
		return Transition[OnboardingOutput, OnboardingPhase]{}, err
	}

	return Stay[OnboardingOutput, OnboardingPhase](WaitingForSignature{
		ContractID:     data.ContractID,
		NextReminderAt: DaysFromNow(3),
	}), nil
}
```

## Generated Shape

`durable-gen` emits code that should not be hand-written:

```go
// Code generated by durable-gen. DO NOT EDIT.

type KycPhase interface {
	isKycPhase()
}

func (KycSubmitting) isKycPhase()         {}
func (KycWaitingForProvider) isKycPhase() {}

type OnboardingPhase interface {
	isOnboardingPhase()
}

func (WaitingForDocuments) isOnboardingPhase() {}
func (WaitingForKyc) isOnboardingPhase()       {}
func (WaitingForSignature) isOnboardingPhase() {}

// Also generated:
// - wait-spec materialization
// - ready-event types
// - run/event dispatch functions
// - query registration
// - workflow registration metadata
```

## Activity Stubs

```go
package workflows

import "context"

func SubmitKycWithHeartbeat(
	ctx context.Context,
	customerID string,
	documents CompleteDocumentSet,
	activity ActivityContext,
) (KycJob, error) {
	if err := activity.Heartbeat(ctx, KycSubmitHeartbeat{Step: "submitted_to_provider"}); err != nil {
		return KycJob{}, err
	}

	panic("not implemented")
}

func PollKycProvider(ctx context.Context, providerJobID string) (KycPollResult, error) {
	panic("not implemented")
}

func SendDocumentReminder(ctx context.Context, customerID string) error {
	panic("not implemented")
}

func ActivateAccount(ctx context.Context, customerID string, contractID string) error {
	panic("not implemented")
}

func SendSignatureReminder(ctx context.Context, customerID string, contractID string) error {
	panic("not implemented")
}
```