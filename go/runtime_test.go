package durable_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	postgresprovider "github.com/danthegoodman1/durable-phases/go/providers/postgres"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
	"github.com/jackc/pgx/v5/pgxpool"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time          { return c.now }
func (c *manualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type testWorkflow struct {
	name    string
	version int
}

func (w testWorkflow) Name() string {
	if w.name == "" {
		return "test_workflow"
	}
	return w.name
}
func (w testWorkflow) Version() int {
	if w.version == 0 {
		return 1
	}
	return w.version
}
func (w testWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{"label": "ok"}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
}
func (w testWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	switch phase.Name {
	case "boot":
		return []durable.DurableWait{durable.RunWait(now)}, nil
	case "waiting":
		return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
	default:
		return nil, nil
	}
}
func (w testWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Go(durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{"ready": true}}), nil
}
func (w testWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName == "finish" {
		return durable.Complete(event.Payload), nil
	}
	return durable.Fail(durable.Errf("unexpected wait %s", waitName)), nil
}
func (w testWorkflow) Query(_ context.Context, name string, ctx durable.QueryContext) (durable.JSON, error) {
	return map[string]any{"name": name, "sequence": float64(ctx.Sequence), "status": ctx.Snapshot.Status}, nil
}
func (w testWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type directSignalWorkflow struct{}

func (w directSignalWorkflow) Name() string { return "direct_signal_workflow" }
func (w directSignalWorkflow) Version() int { return 1 }
func (w directSignalWorkflow) Initial(_ context.Context, input durable.JSON) (durable.Start, error) {
	label := "unset"
	if raw, ok := input.(map[string]any); ok {
		if value, ok := raw["label"].(string); ok {
			label = value
		}
	}
	return durable.Start{Common: map[string]any{"label": label}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}}, nil
}
func (w directSignalWorkflow) MaterializeWaits(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
	return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
}
func (w directSignalWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected run")), nil
}
func (w directSignalWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, common durable.JSON, _ durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "finish" {
		return durable.Fail(durable.Errf("unexpected wait %s", waitName)), nil
	}
	return durable.Complete(map[string]any{"label": common.(map[string]any)["label"], "payload": event.Payload}), nil
}
func (w directSignalWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w directSignalWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type dynamicSignalWorkflow struct{}

type dynamicSignalState struct {
	JobID          string            `json:"jobId"`
	Pending        []string          `json:"pending"`
	Approvals      map[string]string `json:"approvals"`
	ProviderStatus string            `json:"providerStatus"`
}

func (w dynamicSignalWorkflow) Name() string { return "dynamic_signal_workflow" }
func (w dynamicSignalWorkflow) Version() int { return 1 }
func (w dynamicSignalWorkflow) Initial(_ context.Context, input durable.JSON) (durable.Start, error) {
	raw, err := durable.DecodeJSON[struct {
		JobID     string   `json:"jobId"`
		Approvers []string `json:"approvers"`
	}](input)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{Phase: durable.PhaseSnapshot{Name: "waiting", Data: dynamicSignalState{
		JobID: raw.JobID, Pending: raw.Approvers, Approvals: map[string]string{}, ProviderStatus: "pending",
	}}}, nil
}
func (w dynamicSignalWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	if phase.Name != "waiting" {
		return nil, nil
	}
	data, err := durable.DecodeJSON[dynamicSignalState](phase.Data)
	if err != nil {
		return nil, err
	}
	waits := []durable.DurableWait{
		durable.SignalWaitWithOptions(
			"provider:"+data.JobID,
			"provider_result:"+data.JobID,
			false,
			durable.SignalWaitOptions{Handler: "provider_result", Meta: map[string]any{"jobId": data.JobID}},
		),
	}
	for _, approverID := range data.Pending {
		waits = append(waits, durable.SignalWaitWithOptions(
			"approval:"+approverID,
			"approval:"+approverID,
			false,
			durable.SignalWaitOptions{Handler: "approval", Meta: approverID},
		))
	}
	return waits, nil
}
func (w dynamicSignalWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected run")), nil
}
func (w dynamicSignalWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	data, err := durable.DecodeJSON[dynamicSignalState](phase.Data)
	if err != nil {
		return durable.Transition{}, err
	}
	switch waitName {
	case "provider_result":
		meta, err := durable.WaitMeta[map[string]string](event)
		if err != nil {
			return durable.Transition{}, err
		}
		payload, err := durable.DecodeJSON[struct {
			Status string `json:"status"`
		}](event.Payload)
		if err != nil {
			return durable.Transition{}, err
		}
		if meta["jobId"] != data.JobID {
			return durable.Fail(durable.Errf("unexpected provider meta")), nil
		}
		data.ProviderStatus = payload.Status
		return durable.Stay(durable.PhaseSnapshot{Name: "waiting", Data: data}), nil
	case "approval":
		approverID, err := durable.WaitMeta[string](event)
		if err != nil {
			return durable.Transition{}, err
		}
		payload, err := durable.DecodeJSON[struct {
			Decision string `json:"decision"`
		}](event.Payload)
		if err != nil {
			return durable.Transition{}, err
		}
		data.Approvals[approverID] = payload.Decision
		nextPending := data.Pending[:0]
		for _, id := range data.Pending {
			if id != approverID {
				nextPending = append(nextPending, id)
			}
		}
		data.Pending = nextPending
		if len(data.Pending) == 0 {
			return durable.Complete(map[string]any{"providerStatus": data.ProviderStatus, "approvals": data.Approvals}), nil
		}
		return durable.Stay(durable.PhaseSnapshot{Name: "waiting", Data: data}), nil
	default:
		return durable.Fail(durable.Errf("unexpected wait %s", waitName)), nil
	}
}
func (w dynamicSignalWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w dynamicSignalWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type duplicateDynamicSignalWorkflow struct{}

func (w duplicateDynamicSignalWorkflow) Name() string { return "duplicate_dynamic_signal_workflow" }
func (w duplicateDynamicSignalWorkflow) Version() int { return 1 }
func (w duplicateDynamicSignalWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}}, nil
}
func (w duplicateDynamicSignalWorkflow) MaterializeWaits(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
	return []durable.DurableWait{
		durable.SignalWaitWithOptions("approval:a", "approval", false, durable.SignalWaitOptions{Handler: "approval", Meta: "a"}),
		durable.SignalWaitWithOptions("approval:b", "approval", false, durable.SignalWaitOptions{Handler: "approval", Meta: "b"}),
	}, nil
}
func (w duplicateDynamicSignalWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected run")), nil
}
func (w duplicateDynamicSignalWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected event")), nil
}
func (w duplicateDynamicSignalWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w duplicateDynamicSignalWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func forceTerminal(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, ref durable.InstanceRef, next durable.InstanceStatus, now time.Time) {
	t.Helper()
	instance, err := provider.LoadInstance(ctx, ref, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance == nil {
		t.Fatalf("missing instance: %#v", ref)
	}
	shardID := durable.WorkflowPartitionShard(ref.WorkflowID, ref.RunID, 1)
	lease, err := provider.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: shardID, OwnerID: "worker-a", Now: now, Lease: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil {
		t.Fatalf("failed to claim shard %d", shardID)
	}
	session := provider.OpenShard(durable.OpenShardInput{ShardID: shardID, OwnerID: "worker-a", LeaseEpoch: lease.LeaseEpoch})
	claims, err := session.ClaimTasks(ctx, durable.ClaimShardTasksInput{
		Workflows: map[string]int{instance.WorkflowName: instance.WorkflowVersion},
		Now:       now,
		Lease:     time.Second,
		Limit:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	activationID := ""
	for _, claim := range claims.Claims {
		if claim.Activation.WorkflowID == ref.WorkflowID && claim.Activation.RunID == ref.RunID {
			activationID = claim.Activation.ActivationID
			break
		}
	}
	if activationID == "" {
		t.Fatalf("no activation claim for %#v", ref)
	}
	result, err := provider.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
		WorkflowID:       ref.WorkflowID,
		RunID:            ref.RunID,
		ExpectedSequence: 0,
		ActivationID:     activationID,
		WorkerID:         "worker-a",
		WorkflowVersion:  1,
		Next:             next,
		Waits:            []durable.DurableWait{},
		Now:              now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatalf("terminal commit failed: %#v", result)
	}
}

func hasSignalWait(waits []durable.DurableWait, name, typ, handler string) bool {
	for _, wait := range waits {
		if wait.Kind == "signal" && wait.Name == name && wait.Type == typ && durable.SignalWaitHandler(wait) == handler {
			return true
		}
	}
	return false
}

func TestRuntimeSignalFlowAndQuery(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:   "worker-a",
		ShardCount: 1,
		Clock:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	workflow := testWorkflow{}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "runtime-flow"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain boot = %#v, %v", result, err)
	}
	if _, err := runtime.Signal(ctx, workflow, ref, "finish", map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain signal = %#v, %v", result, err)
	}
	instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != "completed" {
		t.Fatalf("status = %s", instance.Status)
	}
	query, err := runtime.Query(ctx, workflow, ref, "progress")
	if err != nil {
		t.Fatal(err)
	}
	if query.(map[string]any)["status"] != "completed" {
		t.Fatalf("query = %#v", query)
	}
}

func TestRuntimeStartSendSignal(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:   "worker-a",
		ShardCount: 1,
		Clock:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct := directSignalWorkflow{}
	started, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "first"}, "finish", map[string]any{"value": "one"}, durable.StartSendSignalOptions{
		WorkflowID: "start-send-runtime",
		RunID:      "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !started.Created || started.Signal.IdempotencyKey != "request-1" {
		t.Fatalf("started = %#v", started)
	}
	existing, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "ignored"}, "finish", map[string]any{"value": "two"}, durable.StartSendSignalOptions{
		WorkflowID:     "start-send-runtime",
		IdempotencyKey: "request-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if existing.Created || existing.RunID != started.RunID || existing.Signal.RunID != started.RunID || existing.Signal.IdempotencyKey != "request-2" {
		t.Fatalf("existing = %#v", existing)
	}
	instance, err := provider.LoadInstance(ctx, started.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Common.(map[string]any)["label"] != "first" {
		t.Fatalf("existing start replaced common: %#v", instance.Common)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain direct = %#v, %v", result, err)
	}
	instance, err = provider.LoadInstance(ctx, started.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != "completed" || instance.Output.(map[string]any)["label"] != "first" {
		t.Fatalf("completed direct = %#v", instance)
	}
	clock.Advance(time.Millisecond)
	retry, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "ignored"}, "finish", map[string]any{"value": "ignored"}, durable.StartSendSignalOptions{
		WorkflowID: "start-send-runtime",
		RunID:      "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Signal.SignalID != started.Signal.SignalID {
		t.Fatalf("retry = %#v, want signal %s", retry, started.Signal.SignalID)
	}
	clock.Advance(time.Millisecond)
	secondRun, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "second-run"}, "finish", map[string]any{"value": "three"}, durable.StartSendSignalOptions{
		WorkflowID:     "start-send-runtime",
		IdempotencyKey: "request-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !secondRun.Created || secondRun.RunID == started.RunID {
		t.Fatalf("second run = %#v", secondRun)
	}
	retryWhileRunning, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "ignored"}, "finish", map[string]any{"value": "ignored"}, durable.StartSendSignalOptions{
		WorkflowID: "start-send-runtime",
		RunID:      "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if retryWhileRunning.Signal.SignalID != started.Signal.SignalID {
		t.Fatalf("retry while running = %#v, want signal %s", retryWhileRunning, started.Signal.SignalID)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain second run = %#v, %v", result, err)
	}
	if _, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "bad"}, "finish", map[string]any{}, durable.StartSendSignalOptions{
		RunID: "missing-workflow-id",
	}); err == nil {
		t.Fatalf("runId without workflowId succeeded")
	}
	generated, err := runtime.StartSendSignal(ctx, direct, map[string]any{"label": "generated"}, "finish", map[string]any{"value": "generated"}, durable.StartSendSignalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if generated.WorkflowID == "" || generated.RunID == "" || generated.Signal.IdempotencyKey != generated.RunID {
		t.Fatalf("generated = %#v", generated)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain generated = %#v, %v", result, err)
	}

	early, err := runtime.StartSendSignal(ctx, testWorkflow{name: "start_send_later"}, map[string]any{}, "finish", map[string]any{"value": "early"}, durable.StartSendSignalOptions{
		WorkflowID: "start-send-later",
		RunID:      "later-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 2}); err != nil || result.Activations != 2 {
		t.Fatalf("drain early = %#v, %v", result, err)
	}
	instance, err = provider.LoadInstance(ctx, early.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != "completed" || instance.Output.(map[string]any)["value"] != "early" {
		t.Fatalf("early completed = %#v", instance)
	}
}

func TestRuntimeDynamicSignalWaits(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	path := filepath.Join(t.TempDir(), "dynamic.sqlite")
	provider, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 1})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "dynamic-a", Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	workflow := dynamicSignalWorkflow{}
	ref, err := runtime.Start(ctx, workflow, map[string]any{
		"jobId":     "job-1",
		"approvers": []string{"ada", "grace"},
	}, durable.StartOptions{WorkflowID: "dynamic-signals"})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasSignalWait(instance.Waits, "provider:job-1", "provider_result:job-1", "provider_result") {
		t.Fatalf("missing provider wait: %#v", instance.Waits)
	}
	if !hasSignalWait(instance.Waits, "approval:ada", "approval:ada", "approval") {
		t.Fatalf("missing approval wait: %#v", instance.Waits)
	}

	restartedProvider, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 1})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := durable.NewRuntime(restartedProvider, durable.RuntimeOptions{
		WorkerID:  "dynamic-b",
		Clock:     clock.Now,
		Workflows: []durable.Workflow{workflow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Signal(ctx, workflow, ref, "provider_result:job-1", map[string]any{"status": "ready"}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Signal(ctx, workflow, ref, "approval:ada", map[string]any{"decision": "yes"}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		t.Fatal(err)
	}
	running, err := restartedProvider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if hasSignalWait(running.Waits, "approval:ada", "approval:ada", "approval") {
		t.Fatalf("approval:ada wait still active: %#v", running.Waits)
	}
	if _, err := restarted.Signal(ctx, workflow, ref, "approval:grace", map[string]any{"decision": "yes"}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		t.Fatal(err)
	}
	completed, err := restartedProvider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != "completed" {
		t.Fatalf("status = %s, instance = %#v", completed.Status, completed)
	}
	output := completed.Output.(map[string]any)
	if output["providerStatus"] != "ready" {
		t.Fatalf("output = %#v", output)
	}
}

func TestRuntimeRejectsDuplicateDynamicSignalTypes(t *testing.T) {
	ctx := context.Background()
	runtime, err := durable.NewRuntime(shardengine.New(), durable.RuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Start(ctx, duplicateDynamicSignalWorkflow{}, map[string]any{}, durable.StartOptions{WorkflowID: "duplicate-dynamic"})
	if err == nil || !strings.Contains(err.Error(), "duplicate active signal type approval") {
		t.Fatalf("err = %v", err)
	}
}

func TestRuntimeSignalWithOptionsDeduplicatesIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:   "worker-a",
		ShardCount: 1,
		Clock:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	workflow := testWorkflow{name: "idempotent_signal_runtime"}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "idempotent-signal-runtime"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain boot = %#v, %v", result, err)
	}
	first, err := runtime.SignalWithOptions(ctx, workflow, ref, "finish", map[string]any{"ok": true}, durable.SignalOptions{IdempotencyKey: "send-1"})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := runtime.SignalWithOptions(ctx, workflow, ref, "finish", map[string]any{"ok": false}, durable.SignalOptions{IdempotencyKey: "send-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.SignalID != duplicate.SignalID || first.IdempotencyKey != duplicate.IdempotencyKey {
		t.Fatalf("duplicate = %#v, want %#v", duplicate, first)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("drain signal = %#v, %v", result, err)
	}
	afterConsumed, err := runtime.SignalWithOptions(ctx, workflow, ref, "finish", map[string]any{"ok": false}, durable.SignalOptions{IdempotencyKey: "send-1"})
	if err != nil {
		t.Fatal(err)
	}
	if afterConsumed.SignalID != first.SignalID || afterConsumed.ConsumedBySequence == nil || *afterConsumed.ConsumedBySequence != 2 {
		t.Fatalf("after consumed duplicate = %#v", afterConsumed)
	}
	signals, err := provider.ListSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 {
		t.Fatalf("signals = %#v", signals)
	}
}

func TestWorkflowIDReusePoliciesAndRunPagination(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "worker-a", ShardCount: 1, Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	workflow := testWorkflow{}
	workflowID := "series-policy"

	first, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.RunID == "run-1" {
		t.Fatalf("first start = %#v", first)
	}

	clock.Advance(time.Millisecond)
	duplicate, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Created || duplicate.RunID != first.RunID {
		t.Fatalf("duplicate start = %#v, want %s", duplicate, first.RunID)
	}

	clock.Advance(time.Millisecond)
	always, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyAlways})
	if err != nil {
		t.Fatal(err)
	}
	if !always.Created || always.RunID == first.RunID {
		t.Fatalf("always start = %#v", always)
	}
	forceTerminal(t, ctx, provider, always.InstanceRef, durable.InstanceStatus{Status: "completed", Output: map[string]any{"ok": true}}, clock.Now())

	clock.Advance(time.Millisecond)
	afterCompleted, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID})
	if err != nil {
		t.Fatal(err)
	}
	if !afterCompleted.Created {
		t.Fatalf("not_running did not create after completed")
	}
	forceTerminal(t, ctx, provider, afterCompleted.InstanceRef, durable.InstanceStatus{Status: "completed", Output: map[string]any{"ok": true}}, clock.Now())

	clock.Advance(time.Millisecond)
	failedOnlyAfterCompleted, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyFailedOnly})
	if err != nil {
		t.Fatal(err)
	}
	if failedOnlyAfterCompleted.Created || failedOnlyAfterCompleted.RunID != afterCompleted.RunID {
		t.Fatalf("failed_only after completed = %#v", failedOnlyAfterCompleted)
	}

	clock.Advance(time.Millisecond)
	failed, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyAlways})
	if err != nil {
		t.Fatal(err)
	}
	forceTerminal(t, ctx, provider, failed.InstanceRef, durable.InstanceStatus{Status: "failed", Error: durable.SerializedError{Message: "failed"}}, clock.Now())

	clock.Advance(time.Millisecond)
	afterFailed, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyFailedOnly})
	if err != nil {
		t.Fatal(err)
	}
	if !afterFailed.Created {
		t.Fatalf("failed_only did not create after failed")
	}

	clock.Advance(time.Millisecond)
	canceled, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyAlways})
	if err != nil {
		t.Fatal(err)
	}
	forceTerminal(t, ctx, provider, canceled.InstanceRef, durable.InstanceStatus{Status: "canceled", Reason: "canceled"}, clock.Now())

	clock.Advance(time.Millisecond)
	afterCanceled, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowID, WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyFailedOnly})
	if err != nil {
		t.Fatal(err)
	}
	if !afterCanceled.Created {
		t.Fatalf("failed_only did not create after canceled")
	}

	ascHead, err := runtime.GetWorkflowRuns(ctx, durable.GetWorkflowRunsInput{ID: workflowID, Direction: durable.WorkflowRunDirectionAsc, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(ascHead.Runs) != 2 || ascHead.Runs[0].RunID != first.RunID || ascHead.Runs[1].RunID != always.RunID || ascHead.Cursor == "" {
		t.Fatalf("asc head = %#v", ascHead)
	}
	ascNext, err := runtime.GetWorkflowRuns(ctx, durable.GetWorkflowRunsInput{ID: workflowID, Direction: durable.WorkflowRunDirectionAsc, Cursor: ascHead.Cursor, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(ascNext.Runs) == 0 || ascNext.Runs[0].RunID != afterCompleted.RunID {
		t.Fatalf("asc next = %#v", ascNext)
	}
	descTail, err := runtime.GetWorkflowRuns(ctx, durable.GetWorkflowRunsInput{ID: workflowID, Direction: durable.WorkflowRunDirectionDesc, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(descTail.Runs) != 1 || descTail.Runs[0].RunID != afterCanceled.RunID {
		t.Fatalf("desc tail = %#v", descTail)
	}
	if _, err := runtime.GetWorkflowRuns(ctx, durable.GetWorkflowRunsInput{ID: workflowID, Direction: durable.WorkflowRunDirectionDesc, Cursor: ascHead.Cursor, Limit: 1}); err == nil {
		t.Fatalf("direction-mismatched cursor succeeded")
	}
}

func TestProviderFencesStaleShardCommitsAndPreservesSignals(t *testing.T) {
	ctx := context.Background()
	provider := shardengine.New()
	ref, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "conformance", WorkflowVersion: 1, WorkflowID: "fence", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}},
		Waits: []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	signal, err := provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"ok": true}, ReceivedAt: t0})
	if err != nil {
		t.Fatal(err)
	}
	leaseA, err := provider.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: "worker-a", Now: t0, Lease: time.Millisecond})
	if err != nil || leaseA == nil {
		t.Fatalf("lease A = %#v, %v", leaseA, err)
	}
	claimA, err := provider.OpenShard(durable.OpenShardInput{ShardID: 0, OwnerID: "worker-a", LeaseEpoch: leaseA.LeaseEpoch}).ClaimTasks(ctx, durable.ClaimShardTasksInput{
		Workflows: map[string]int{"conformance": 1}, Now: t0, Lease: time.Second, Limit: 1,
	})
	if err != nil || len(claimA.Claims) != 1 {
		t.Fatalf("claim A = %#v, %v", claimA, err)
	}
	t1 := t0.Add(2 * time.Millisecond)
	leaseB, err := provider.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: "worker-b", Now: t1, Lease: time.Minute})
	if err != nil || leaseB == nil {
		t.Fatalf("lease B = %#v, %v", leaseB, err)
	}
	stale, err := provider.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: claimA.Claims[0].Activation.ActivationID,
		WorkerID: "worker-a", WorkflowVersion: 1, Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"bad": true}},
		Now: t1, ConsumeSignalID: signal.SignalID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stale.OK {
		t.Fatalf("stale commit unexpectedly succeeded")
	}
	signals, err := provider.ListSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if signals[0].ConsumedBySequence != nil {
		t.Fatalf("stale commit consumed signal")
	}
}

func TestSQLiteProviderRestartsFromJournal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.sqlite")
	provider, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "restart", WorkflowVersion: 1, WorkflowID: "restart", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{"value": "saved"}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	instance, err := restarted.LoadInstance(ctx, durable.InstanceRef{WorkflowID: "restart", RunID: "run-1"}, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance == nil || instance.Common.(map[string]any)["value"] != "saved" {
		t.Fatalf("restarted instance = %#v", instance)
	}
}

func TestSQLiteProviderIdempotentSignalReplaySkipsDuplicateJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.sqlite")
	provider, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "sqlite_signal_replay", WorkflowVersion: 1, WorkflowID: "sqlite-signal-replay", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(1)}, ReceivedAt: t0, IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(99)}, ReceivedAt: t0.Add(time.Millisecond), IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.SignalID != first.SignalID || duplicate.Payload.(map[string]any)["index"] != float64(1) {
		t.Fatalf("duplicate = %#v, want %#v", duplicate, first)
	}
	if _, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(2)}, ReceivedAt: t0.Add(2 * time.Millisecond), IdempotencyKey: "request-2",
	}); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var journalEntries int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM shard_journal`).Scan(&journalEntries); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	_ = db.Close()
	if journalEntries != 3 {
		t.Fatalf("journal entries = %d, want 3", journalEntries)
	}

	restarted, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	signals, err := restarted.ListSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 2 || signals[0].Payload.(map[string]any)["index"] != float64(1) || signals[1].Payload.(map[string]any)["index"] != float64(2) {
		t.Fatalf("replayed signals = %#v", signals)
	}
}

func TestSQLiteProviderIdempotentSignalDeduplicatesAcrossProviderInstances(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.sqlite")
	providerA, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer providerA.Close(ctx)
	ref, err := providerA.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "sqlite_signal_cross_provider", WorkflowVersion: 1, WorkflowID: "sqlite-signal-cross-provider", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerB, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer providerB.Close(ctx)

	first, err := providerA.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"sender": "a"}, ReceivedAt: t0, IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := providerB.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"sender": "b"}, ReceivedAt: t0.Add(time.Second), IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.SignalID != first.SignalID || duplicate.Payload.(map[string]any)["sender"] != "a" {
		t.Fatalf("duplicate = %#v, want original %#v", duplicate, first)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var journalEntries int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM shard_journal`).Scan(&journalEntries); err != nil {
		t.Fatal(err)
	}
	if journalEntries != 2 {
		t.Fatalf("journal entries = %d, want 2", journalEntries)
	}
}

func TestSQLiteShardFileProviderRestartsFromJournals(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	provider, err := sqliteprovider.NewShardFile(sqliteprovider.ShardFileOptions{Directory: dir, ShardCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	var refs []durable.InstanceRef
	for shard := 0; shard < 2; shard++ {
		workflowID := workflowIDForShard(t, shard, 2)
		ref, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
			WorkflowName: "restart_shard", WorkflowVersion: 1, WorkflowID: workflowID, RunID: "run-1", PartitionShard: shard,
			Common: map[string]any{"shard": float64(shard)}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{}, Now: t0,
		})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref.InstanceRef)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := sqliteprovider.NewShardFile(sqliteprovider.ShardFileOptions{Directory: dir, ShardCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	for shard, ref := range refs {
		instance, err := restarted.LoadInstance(ctx, ref, durable.LoadInstanceOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if instance == nil || instance.Common.(map[string]any)["shard"] != float64(shard) {
			t.Fatalf("shard %d instance = %#v", shard, instance)
		}
	}
}

func TestPostgresProviderRestartsFromJournalWhenConfigured(t *testing.T) {
	url := os.Getenv("DURABLE_POSTGRES_URL")
	if url == "" {
		t.Skip("DURABLE_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("durable_go_test_%d", time.Now().UnixNano())
	provider, err := postgresprovider.New(ctx, postgresprovider.Options{ConnectionString: url, Schema: schema, PhysicalPartitions: 2, SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "postgres_restart", WorkflowVersion: 1, WorkflowID: "postgres-restart", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{"value": "saved"}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := postgresprovider.New(ctx, postgresprovider.Options{ConnectionString: url, Schema: schema, PhysicalPartitions: 2, SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	instance, err := restarted.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance == nil || instance.Common.(map[string]any)["value"] != "saved" {
		t.Fatalf("restarted postgres instance = %#v", instance)
	}
}

func TestPostgresProviderIdempotentSignalDeduplicatesAcrossProviderInstances(t *testing.T) {
	url := os.Getenv("DURABLE_POSTGRES_URL")
	if url == "" {
		t.Skip("DURABLE_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("durable_go_signal_idempotency_%d", time.Now().UnixNano())
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	defer pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)

	providerA, err := postgresprovider.New(ctx, postgresprovider.Options{ConnectionString: url, Schema: schema, PhysicalPartitions: 2, SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer providerA.Close(ctx)
	ref, err := providerA.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "postgres_signal_cross_provider", WorkflowVersion: 1, WorkflowID: "postgres-signal-cross-provider", RunID: "run-1", PartitionShard: 0,
		Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Waits: []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerB, err := postgresprovider.New(ctx, postgresprovider.Options{ConnectionString: url, Schema: schema, PhysicalPartitions: 2, SnapshotInterval: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer providerB.Close(ctx)

	first, err := providerA.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"sender": "a"}, ReceivedAt: t0, IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := providerB.AppendSignal(ctx, durable.AppendSignalInput{
		WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"sender": "b"}, ReceivedAt: t0.Add(time.Second), IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.SignalID != first.SignalID || duplicate.Payload.(map[string]any)["sender"] != "a" {
		t.Fatalf("duplicate = %#v, want original %#v", duplicate, first)
	}

	var journalEntries int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*)::int FROM "%s".shard_journal_p00 WHERE shard_id = 0`, schema)).Scan(&journalEntries); err != nil {
		t.Fatal(err)
	}
	if journalEntries != 2 {
		t.Fatalf("journal entries = %d, want 2", journalEntries)
	}
}

type timerWorkflow struct {
	due time.Time
}

func (w timerWorkflow) Name() string { return "timer_workflow" }
func (w timerWorkflow) Version() int { return 1 }
func (w timerWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}}, nil
}
func (w timerWorkflow) MaterializeWaits(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
	return []durable.DurableWait{durable.TimerWait("wake", w.due)}, nil
}
func (w timerWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("timer workflow has no run handler")), nil
}
func (w timerWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Complete(map[string]any{"woke": true}), nil
}
func (w timerWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w timerWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func TestRuntimeTimerWaitFiresAfterClockAdvances(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "timer-worker", Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	workflow := timerWorkflow{due: t0.Add(time.Hour)}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "timer-flow"})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 0 {
		t.Fatalf("early drain = %#v, %v", result, err)
	}
	clock.Advance(time.Hour)
	if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("due drain = %#v, %v", result, err)
	}
	instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != "completed" {
		t.Fatalf("status = %s", instance.Status)
	}
}

func TestRunShardStepProcessesOnlyRequestedShard(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:         "step-worker",
		ShardCount:       3,
		DispatchShardIDs: []int{0},
		Clock:            clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	workflow := testWorkflow{name: "run_shard_step_workflow"}
	refs := map[int]durable.StartWorkflowResult{}
	for shardID := 0; shardID < 3; shardID++ {
		ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowIDForShard(t, shardID, 3)})
		if err != nil {
			t.Fatal(err)
		}
		if actual := runtime.ShardForRef(ref); actual != shardID {
			t.Fatalf("ref shard = %d, want %d", actual, shardID)
		}
		refs[shardID] = ref
	}

	result, err := runtime.RunShardStep(ctx, durable.RunShardStepOptions{
		ShardID:                  1,
		MaxActivations:           1,
		MaxConcurrentActivations: 1,
		ActivationPrefetchLimit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ClaimedShard || result.Activations != 1 || result.ShardID != 1 {
		t.Fatalf("run shard step = %#v", result)
	}

	for shardID, ref := range refs {
		instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if instance == nil {
			t.Fatalf("missing instance for shard %d", shardID)
		}
		wantSequence := int64(0)
		if shardID == 1 {
			wantSequence = 1
		}
		if instance.Sequence != wantSequence {
			t.Fatalf("shard %d sequence = %d, want %d", shardID, instance.Sequence, wantSequence)
		}
	}
}

func TestRunShardStepReportsWhenAnotherWorkerOwnsShard(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	lease, err := provider.ClaimShard(ctx, durable.ClaimDispatchShardInput{
		ShardID: 0,
		OwnerID: "other-worker",
		Now:     clock.Now(),
		Lease:   time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil {
		t.Fatal("expected other worker to claim shard")
	}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:   "step-worker",
		ShardCount: 1,
		Clock:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := runtime.RunShardStep(ctx, durable.RunShardStepOptions{ShardID: 0, MaxActivations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.ClaimedShard || result.Activations != 0 || result.ShardID != 0 {
		t.Fatalf("run shard step = %#v", result)
	}
}

func TestRunShardStepReturnsNextWakeAtForFutureTimer(t *testing.T) {
	ctx := context.Background()
	clock := &manualClock{now: t0}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:         "step-worker",
		ShardCount:       3,
		DispatchShardIDs: []int{0},
		Clock:            clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	due := t0.Add(time.Hour)
	workflow := timerWorkflow{due: due}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: workflowIDForShard(t, 2, 3)})
	if err != nil {
		t.Fatal(err)
	}
	if actual := runtime.ShardForRef(ref); actual != 2 {
		t.Fatalf("ref shard = %d, want 2", actual)
	}

	result, err := runtime.RunShardStep(ctx, durable.RunShardStepOptions{ShardID: 2, MaxActivations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ClaimedShard || result.Activations != 0 || result.ShardID != 2 || !result.NextWakeAt.Equal(due) {
		t.Fatalf("run shard step = %#v, want next wake %s", result, due)
	}
}

func TestRunShardStepValidatesShardIDs(t *testing.T) {
	ctx := context.Background()
	runtime, err := durable.NewRuntime(shardengine.New(), durable.RuntimeOptions{
		WorkerID:   "step-worker",
		ShardCount: 2,
		Clock:      (&manualClock{now: t0}).Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, shardID := range []int{-1, 2} {
		_, err := runtime.RunShardStep(ctx, durable.RunShardStepOptions{ShardID: shardID})
		if err == nil {
			t.Fatalf("shard id %d unexpectedly succeeded", shardID)
		}
		want := fmt.Sprintf("shard id %d outside 0..1", shardID)
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want %q", err.Error(), want)
		}
	}
}

type activityWorkflow struct {
	calls *int
	eager bool
}

func (w activityWorkflow) Name() string { return "activity_workflow" }
func (w activityWorkflow) Version() int { return 1 }
func (w activityWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
}
func (w activityWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "boot" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}
func (w activityWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	options := durable.ActivityOptions{}
	if w.eager {
		options.Durability = durable.ActivityEager
		options.StartToCloseTimeout = time.Minute
		options.HeartbeatTimeout = time.Minute
		options.Retry = durable.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Millisecond}
	}
	value, err := durable.ActivityWithOptions[map[string]any](ctx, dctx, "work", options, func(ctx context.Context, activity durable.ActivityContext) (map[string]any, error) {
		*w.calls++
		if err := activity.Heartbeat(ctx, map[string]any{"step": "running"}); err != nil {
			return nil, err
		}
		return map[string]any{"attempt": float64(activity.Attempt), "idempotency": activity.IdempotencyKey != ""}, nil
	})
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(value), nil
}
func (w activityWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("activity workflow has no event handler")), nil
}
func (w activityWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w activityWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func TestRuntimeActivitiesCheckpointAndEagerMemoizeResults(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(strconv.FormatBool(eager), func(t *testing.T) {
			ctx := context.Background()
			calls := 0
			provider := shardengine.New()
			runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "activity-worker", Clock: (&manualClock{now: t0}).Now})
			if err != nil {
				t.Fatal(err)
			}
			workflow := activityWorkflow{calls: &calls, eager: eager}
			ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "activity-flow-" + strconv.FormatBool(eager)})
			if err != nil {
				t.Fatal(err)
			}
			if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
				t.Fatalf("drain = %#v, %v", result, err)
			}
			instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{IncludeEffects: true})
			if err != nil {
				t.Fatal(err)
			}
			if instance.Status != "completed" || calls != 1 {
				t.Fatalf("instance = %#v calls=%d", instance, calls)
			}
			if len(instance.Effects) != 1 || instance.Effects[0].Status != "completed" {
				t.Fatalf("effects = %#v", instance.Effects)
			}
		})
	}
}

type childWorkflow struct{}

func (w childWorkflow) Name() string { return "child_workflow" }
func (w childWorkflow) Version() int { return 1 }
func (w childWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
}
func (w childWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "boot" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}
func (w childWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Complete(map[string]any{"child": true}), nil
}
func (w childWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("child workflow has no event handler")), nil
}
func (w childWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w childWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type parentWorkflow struct{}

func (w parentWorkflow) Name() string { return "parent_workflow" }
func (w parentWorkflow) Version() int { return 1 }
func (w parentWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
}
func (w parentWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "boot" {
		return []durable.DurableWait{durable.RunWait(t0)}, nil
	}
	if phase.Name != "waiting_child" {
		return nil, nil
	}
	handle, err := durable.DecodeJSON[durable.ChildHandleAny](phase.Data)
	if err != nil {
		return nil, err
	}
	return []durable.DurableWait{durable.ChildWait("child_done", handle)}, nil
}
func (w parentWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	handle, err := dctx.ChildStart(ctx, "child", childWorkflow{}, map[string]any{}, durable.DefaultChildOptions())
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Go(durable.PhaseSnapshot{Name: "waiting_child", Data: handle}), nil
}
func (w parentWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "child_done" || event.Event == nil || !event.Event.OK {
		return durable.Fail(durable.Errf("unexpected child event")), nil
	}
	return durable.Complete(event.Event.Output), nil
}
func (w parentWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w parentWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func TestRuntimeChildWorkflowCompletionIsDeliveredToParent(t *testing.T) {
	ctx := context.Background()
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "child-worker", Clock: (&manualClock{now: t0}).Now})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := runtime.Start(ctx, parentWorkflow{}, map[string]any{}, durable.StartOptions{WorkflowID: "parent-flow"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if result, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
			t.Fatalf("drain %d = %#v, %v", i, result, err)
		}
	}
	instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != "completed" || instance.Output.(map[string]any)["child"] != true {
		t.Fatalf("parent instance = %#v", instance)
	}
}

type migrationWorkflow struct {
	version int
}

func (w migrationWorkflow) Name() string { return "migration_workflow" }
func (w migrationWorkflow) Version() int {
	if w.version == 0 {
		return 1
	}
	return w.version
}
func (w migrationWorkflow) Initial(context.Context, durable.JSON) (durable.Start, error) {
	return durable.Start{Common: map[string]any{"version": float64(w.Version())}, Phase: durable.PhaseSnapshot{Name: "hold", Data: map[string]any{}}}, nil
}
func (w migrationWorkflow) MaterializeWaits(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
	return nil, nil
}
func (w migrationWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("migration workflow has no run handler")), nil
}
func (w migrationWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("migration workflow has no event handler")), nil
}
func (w migrationWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w migrationWorkflow) Migrate(_ context.Context, from int, args durable.MigrationArgs) (*durable.MigrationResult, error) {
	common := map[string]any{"version": float64(from + 1), "migrated": true}
	phase := durable.PhaseSnapshot{Name: args.Phase.Name, Data: map[string]any{"from": float64(from)}}
	return &durable.MigrationResult{Common: common, Phase: &phase}, nil
}

func TestRuntimeMigrationTaskUpgradesSnapshotVersion(t *testing.T) {
	ctx := context.Background()
	provider := shardengine.New()
	runtimeV1, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "migration-v1", Clock: (&manualClock{now: t0}).Now})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := runtimeV1.Start(ctx, migrationWorkflow{version: 1}, map[string]any{}, durable.StartOptions{WorkflowID: "migration-flow"})
	if err != nil {
		t.Fatal(err)
	}
	runtimeV2, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "migration-v2", Clock: (&manualClock{now: t0}).Now, Workflows: []durable.Workflow{migrationWorkflow{version: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runtimeV2.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil || result.Activations != 1 {
		t.Fatalf("migration drain = %#v, %v", result, err)
	}
	instance, err := provider.LoadInstance(ctx, ref.InstanceRef, durable.LoadInstanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if instance.WorkflowVersion != 2 || instance.Common.(map[string]any)["migrated"] != true {
		t.Fatalf("migrated instance = %#v", instance)
	}
}

func TestSQLiteShardFileRejectsCrossShardCheckpointChildren(t *testing.T) {
	ctx := context.Background()
	provider, err := sqliteprovider.NewShardFile(sqliteprovider.ShardFileOptions{Directory: t.TempDir(), ShardCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close(ctx)
	parentID := "parent"
	parentShard := durable.WorkflowPartitionShard(parentID, "run-1", 2)
	_, err = provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: "parent", WorkflowVersion: 1, WorkflowID: parentID, RunID: "run-1", PartitionShard: parentShard,
		Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "run", Data: map[string]any{}}, Waits: []durable.DurableWait{durable.RunWait(t0)}, Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	childID := ""
	for i := range 256 {
		candidate := "child-" + strconv.Itoa(i)
		if durable.WorkflowPartitionShard(candidate, "run-1", 2) != parentShard {
			childID = candidate
			break
		}
	}
	if childID == "" {
		t.Fatalf("could not find child workflow id outside parent shard")
	}
	result, err := provider.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
		WorkflowID: parentID, RunID: "run-1", ExpectedSequence: 0, ActivationID: "activation", WorkerID: "worker", WorkflowVersion: 1,
		Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{}}, Now: t0,
		ChildStarts: []durable.CheckpointChildStart{{
			Key: "child", WorkflowName: "child", WorkflowVersion: 1, WorkflowID: childID, RunID: "run-1", PartitionShard: 1 - parentShard,
			Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "run", Data: map[string]any{}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OK || result.Reason != "cross_shard_child_start" {
		t.Fatalf("result = %#v", result)
	}
}

func workflowIDForShard(t *testing.T, targetShard, shardCount int) string {
	t.Helper()
	for i := 0; i < 1024; i++ {
		candidate := fmt.Sprintf("workflow-%d-%d", targetShard, i)
		if durable.WorkflowPartitionShard(candidate, "run-1", shardCount) == targetShard {
			return candidate
		}
	}
	t.Fatalf("could not find workflow id for shard %d", targetShard)
	return ""
}

func TestNoGoJSONDurabilityProviderSymbol(t *testing.T) {
	root := "."
	var matches []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		if strings.Contains(text, "JsonDurabilityProvider") || strings.Contains(text, "JSONDurabilityProvider") {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) > 0 {
		t.Fatalf("JSON durability provider symbols found: %v", matches)
	}
}
