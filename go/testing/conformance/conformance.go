package conformance

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
)

type ProviderHandle struct {
	Provider durable.DurabilityProvider
	Close    func(context.Context) error
}

type Store struct {
	New     func(*testing.T) ProviderHandle
	Cleanup func(context.Context) error
}

type Factory struct {
	Name     string
	NewStore func(*testing.T) Store
}

var (
	t0        = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1        = t0.Add(time.Second)
	t2        = t0.Add(2 * time.Second)
	t5        = t0.Add(5 * time.Second)
	longLease = time.Minute
	workflows = map[string]int{
		"conformance":       1,
		"conformance_child": 1,
		"conformance_other": 1,
		"runtime_parent":    1,
		"runtime_child":     1,
	}
)

func AssertProviderConformance(t *testing.T, factory Factory) {
	t.Helper()
	if factory.Name == "" || factory.NewStore == nil {
		t.Fatalf("conformance factory is incomplete")
	}
	t.Run(factory.Name+"/lifecycle", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			ref := createInstance(t, ctx, provider, createOptions{
				workflowID: "lifecycle",
				common:     map[string]any{"value": "original"},
				phase:      durable.PhaseSnapshot{Name: "boot", Data: map[string]any{"step": float64(1)}},
			})
			if _, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
				WorkflowName: "conformance", WorkflowVersion: 1, WorkflowID: ref.WorkflowID, RunID: ref.RunID, PartitionShard: 0,
				Common: map[string]any{"value": "fail"}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}, Waits: []durable.DurableWait{runWait(t0)}, Now: t0, ConflictPolicy: durable.ConflictFail,
			}); err == nil {
				t.Fatalf("duplicate create with fail policy succeeded")
			}
			existing, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
				WorkflowName: "conformance", WorkflowVersion: 1, WorkflowID: ref.WorkflowID, RunID: ref.RunID, PartitionShard: 0,
				Common: map[string]any{"value": "ignored"}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}, Waits: []durable.DurableWait{runWait(t0)}, Now: t0, ConflictPolicy: durable.ConflictUseExisting,
			})
			requireNoError(t, err)
			requireEqual(t, ref, existing.InstanceRef)
			loaded := load(t, ctx, provider, ref, durable.LoadInstanceOptions{})
			requireEqual(t, "running", loaded.Status)
			requireEqual(t, int64(0), loaded.Sequence)
			requireEqual(t, "original", loaded.Common.(map[string]any)["value"])

			_, err = provider.CreateInstance(ctx, durable.CreateInstanceInput{
				WorkflowName: "conformance", WorkflowVersion: 1, WorkflowID: ref.WorkflowID, RunID: ref.RunID, PartitionShard: 0,
				Common: map[string]any{"value": "replacement"}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{"step": float64(4)}}, Waits: []durable.DurableWait{runWait(t0)}, Now: t1, ConflictPolicy: durable.ConflictTerminateExisting,
			})
			requireNoError(t, err)
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			claim := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows))
			requireEqual(t, "run", claim.Activation.Kind)
			committed, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: claim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next:  durable.Running(map[string]any{"value": "replacement"}, durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{"ready": false}}),
				Waits: []durable.DurableWait{signalWait("finish")}, Now: t1,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 1)
			signal, err := provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"ok": true}, ReceivedAt: t2})
			requireNoError(t, err)
			event := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t2, longLease, workflows), "signal")
			requireEqual(t, signal.SignalID, event.Activation.Event.ConsumeSignalID)
			committed, err = session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 1, ActivationID: event.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"ok": true}}, Waits: nil, Now: t2, ConsumeSignalID: event.Activation.Event.ConsumeSignalID,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 2)
			loaded = load(t, ctx, provider, ref, durable.LoadInstanceOptions{})
			requireEqual(t, "completed", loaded.Status)
			requireEqual(t, int64(2), loaded.Sequence)
		})
	})

	t.Run(factory.Name+"/ordered-batch-and-lean-load", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			createInstance(t, ctx, provider, createOptions{workflowID: "batch-2", waits: []durable.DurableWait{runWait(t0.Add(2 * time.Millisecond))}})
			createInstance(t, ctx, provider, createOptions{workflowID: "batch-1", waits: []durable.DurableWait{runWait(t0.Add(time.Millisecond))}})
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			if _, err := session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: t0, Lease: longLease, Limit: 0}); err == nil {
				t.Fatalf("claim with limit 0 succeeded")
			}
			batch, err := session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: t0.Add(10 * time.Millisecond), Lease: longLease, Limit: 2})
			requireNoError(t, err)
			requireEqual(t, 2, len(batch.Claims))
			requireEqual(t, "batch-1", batch.Claims[0].Activation.WorkflowID)
			requireEqual(t, "batch-2", batch.Claims[1].Activation.WorkflowID)
			requireEqual(t, 0, len(batch.Claims[0].Instance.Effects))
			lean := load(t, ctx, provider, durable.InstanceRef{WorkflowID: "batch-1", RunID: "run-1"}, durable.LoadInstanceOptions{})
			requireEqual(t, 0, len(lean.Effects))
		})
	})

	t.Run(factory.Name+"/leases-and-reclaim", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			providerA := store.New(t).Provider
			providerB := store.New(t).Provider
			createInstance(t, ctx, providerA, createOptions{workflowID: "lease-run"})
			if _, err := providerA.OpenShard(durable.OpenShardInput{ShardID: 0, OwnerID: "worker-a"}).ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: t0, Lease: longLease, Limit: 1}); err == nil {
				t.Fatalf("claim without shard lease succeeded")
			}
			leaseA, err := providerA.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: "worker-a", Now: t0, Lease: 100 * time.Millisecond})
			requireNoError(t, err)
			if leaseA == nil {
				t.Fatalf("worker-a did not claim shard")
			}
			leaseB, err := providerB.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: "worker-b", Now: t0, Lease: 100 * time.Millisecond})
			requireNoError(t, err)
			if leaseB != nil {
				t.Fatalf("worker-b claimed unexpired shard: %#v", leaseB)
			}
			sessionA := providerA.OpenShard(durable.OpenShardInput{ShardID: 0, OwnerID: "worker-a", LeaseEpoch: leaseA.LeaseEpoch})
			claimA := requireClaim(t, claimOne(t, ctx, sessionA, "worker-a", t0, 100*time.Millisecond, workflows))
			reclaimAt := t0.Add(150 * time.Millisecond)
			leaseB, err = providerB.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: "worker-b", Now: reclaimAt, Lease: longLease})
			requireNoError(t, err)
			if leaseB == nil {
				t.Fatalf("worker-b did not reclaim expired shard")
			}
			sessionB := providerB.OpenShard(durable.OpenShardInput{ShardID: 0, OwnerID: "worker-b", LeaseEpoch: leaseB.LeaseEpoch})
			claimB := requireClaim(t, claimOne(t, ctx, sessionB, "worker-b", reclaimAt, longLease, workflows))
			requireEqual(t, claimA.Activation.ActivationID, claimB.Activation.ActivationID)
			stale, err := sessionA.CommitCheckpoint(ctx, completeInput(claimA, "worker-a", map[string]any{"worker": "a"}, reclaimAt))
			requireNoError(t, err)
			if stale.OK {
				t.Fatalf("stale commit succeeded: %#v", stale)
			}
			fresh, err := sessionB.CommitCheckpoint(ctx, completeInput(claimB, "worker-b", map[string]any{"worker": "b"}, reclaimAt))
			requireNoError(t, err)
			requireCommitOK(t, fresh, 1)
		})
	})

	t.Run(factory.Name+"/ready-indexes-and-ordering", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			signalRef := createInstance(t, ctx, provider, createOptions{workflowID: "signal-ready", waits: []durable.DurableWait{signalWait("finish")}})
			_, err := provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: signalRef.WorkflowID, RunID: signalRef.RunID, Type: "finish", Payload: map[string]any{"value": float64(42)}, ReceivedAt: t1})
			requireNoError(t, err)
			signalClaim := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t1, longLease, workflows), "signal")
			requireEqual(t, "signal-ready", signalClaim.Activation.WorkflowID)
			_, err = session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: signalRef.WorkflowID, RunID: signalRef.RunID, ExpectedSequence: 0, ActivationID: signalClaim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"value": float64(42)}}, Now: t1, ConsumeSignalID: signalClaim.Activation.Event.ConsumeSignalID,
			})
			requireNoError(t, err)

			createInstance(t, ctx, provider, createOptions{workflowID: "timer-ready", waits: []durable.DurableWait{durable.TimerWait("wake", t5)}})
			empty, err := session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: t2, Lease: longLease, Limit: 1})
			requireNoError(t, err)
			requireEqual(t, 0, len(empty.Claims))
			requireEqual(t, t5, empty.NextWakeAt)
			timerClaim := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t5, longLease, workflows), "timer")
			requireEqual(t, "timer-ready", timerClaim.Activation.WorkflowID)
			requireNoError(t, session.ReleaseActivation(ctx, timerClaim.Activation.ActivationID, "worker-a"))

			createInstance(t, ctx, provider, createOptions{workflowName: "conformance_other", workflowID: "migration-ready", workflowVersion: 1, waits: []durable.DurableWait{signalWait("never")}})
			migration := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, map[string]int{"conformance_other": 2}))
			requireEqual(t, "migration", migration.Activation.Kind)
			requireEqual(t, "migration-ready", migration.Activation.WorkflowID)
			requireNoError(t, session.ReleaseActivation(ctx, migration.Activation.ActivationID, "worker-a"))

			parent := createInstance(t, ctx, provider, createOptions{workflowID: "ordered-parent", waits: []durable.DurableWait{runWait(t0)}})
			setup := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows))
			child, err := session.CreateChildInstance(ctx, childCreateInput(parent, setup.Activation.ActivationID, "child", "ordered-child"))
			requireNoError(t, err)
			_, err = session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: parent.WorkflowID, RunID: parent.RunID, ExpectedSequence: 0, ActivationID: setup.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next:  durable.Running(map[string]any{}, durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}),
				Waits: []durable.DurableWait{durable.ChildWait("child_done", child), signalWait("signal_done"), durable.TimerWait("timer_done", t1)}, Now: t0,
			})
			requireNoError(t, err)
			childRun := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows))
			requireEqual(t, "ordered-child", childRun.Activation.WorkflowID)
			_, err = session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: child.WorkflowID, RunID: child.RunID, ExpectedSequence: 0, ActivationID: childRun.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"child": true}}, Now: t1,
			})
			requireNoError(t, err)
			_, err = provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: parent.WorkflowID, RunID: parent.RunID, Type: "signal_done", Payload: map[string]any{}, ReceivedAt: t1})
			requireNoError(t, err)
			ordered := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t1, longLease, workflows), "child")
			requireEqual(t, "child_done", ordered.Activation.WaitName)
		})
	})

	t.Run(factory.Name+"/checkpoint-conflicts-are-non-destructive", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			ref := createInstance(t, ctx, provider, createOptions{workflowID: "checkpoint-authority", waits: []durable.DurableWait{signalWait("finish")}})
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			first, err := provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(1)}, ReceivedAt: t0})
			requireNoError(t, err)
			second, err := provider.AppendSignal(ctx, durable.AppendSignalInput{WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(2)}, ReceivedAt: t1})
			requireNoError(t, err)
			claim := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows), "signal")
			requireEqual(t, first.SignalID, claim.Activation.Event.ConsumeSignalID)
			conflict, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: claim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"wrong": true}}, Now: t1, ConsumeSignalID: second.SignalID,
				Effects: []durable.CheckpointEffectMutation{{Key: "checkpoint-local", Status: "completed", Result: map[string]any{"shouldNotPersist": true}}},
			})
			requireNoError(t, err)
			if conflict.OK {
				t.Fatalf("mismatched signal commit succeeded")
			}
			requireEqual(t, int64(0), load(t, ctx, provider, ref, durable.LoadInstanceOptions{}).Sequence)
			requireEqual(t, 0, len(load(t, ctx, provider, ref, durable.LoadInstanceOptions{IncludeEffects: true}).Effects))
			ok, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: claim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"index": float64(1)}}, Now: t1, ConsumeSignalID: claim.Activation.Event.ConsumeSignalID,
			})
			requireNoError(t, err)
			requireCommitOK(t, ok, 1)
		})
	})

	t.Run(factory.Name+"/signal-idempotency", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			ref := createInstance(t, ctx, provider, createOptions{workflowID: "signal-idempotency", waits: []durable.DurableWait{signalWait("finish")}})
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			first, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(1)}, ReceivedAt: t0, IdempotencyKey: "request-1",
			})
			requireNoError(t, err)
			duplicate, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(99)}, ReceivedAt: t1, IdempotencyKey: "request-1",
			})
			requireNoError(t, err)
			requireEqual(t, first, duplicate)
			signals, err := provider.ListSignals(ctx)
			requireNoError(t, err)
			requireEqual(t, 1, len(signals))
			second, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(2)}, ReceivedAt: t2, IdempotencyKey: "request-2",
			})
			requireNoError(t, err)
			if second.SignalID == first.SignalID {
				t.Fatalf("second idempotency key reused signal id %s", second.SignalID)
			}
			claim := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows), "signal")
			requireEqual(t, first.SignalID, claim.Activation.Event.ConsumeSignalID)
			committed, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: claim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.Running(map[string]any{}, durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}), Waits: []durable.DurableWait{signalWait("finish")}, Now: t2, ConsumeSignalID: claim.Activation.Event.ConsumeSignalID,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 1)
			afterConsumed, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(100)}, ReceivedAt: t5, IdempotencyKey: "request-1",
			})
			requireNoError(t, err)
			requireEqual(t, first.SignalID, afterConsumed.SignalID)
			requireEqual(t, "request-1", afterConsumed.IdempotencyKey)
			if afterConsumed.ConsumedBySequence == nil || *afterConsumed.ConsumedBySequence != 1 {
				t.Fatalf("duplicate after consumption returned sequence %#v", afterConsumed.ConsumedBySequence)
			}
			next := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t5, longLease, workflows), "signal")
			requireEqual(t, second.SignalID, next.Activation.Event.ConsumeSignalID)
		})
	})

	t.Run(factory.Name+"/future-only-signals", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			ref := createInstance(t, ctx, provider, createOptions{workflowID: "future-signal", waits: []durable.DurableWait{runWait(t0)}})
			oldSignal, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(1)}, ReceivedAt: t0, IdempotencyKey: "old",
			})
			requireNoError(t, err)
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			run := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows))
			committed, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: run.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.Running(map[string]any{}, durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}), Waits: []durable.DurableWait{futureSignalWaitWithAfter("finish", 0)}, Now: t1,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 1)
			duplicate, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(99)}, ReceivedAt: t2, IdempotencyKey: "old",
			})
			requireNoError(t, err)
			requireEqual(t, oldSignal.SignalID, duplicate.SignalID)
			requireEqual(t, 0, len(claimOne(t, ctx, session, "worker-a", t2, longLease, workflows).Claims))
			newSignal, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(2)}, ReceivedAt: t2, IdempotencyKey: "new",
			})
			requireNoError(t, err)
			event := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t2, longLease, workflows), "signal")
			requireEqual(t, newSignal.SignalID, event.Activation.Event.ConsumeSignalID)
			committed, err = session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 1, ActivationID: event.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "completed", Output: map[string]any{"ok": true}}, Now: t2, ConsumeSignalID: event.Activation.Event.ConsumeSignalID,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 2)
			signals, err := provider.ListSignals(ctx)
			requireNoError(t, err)
			for _, signal := range signals {
				if signal.SignalID == oldSignal.SignalID && signal.ConsumedBySequence != nil {
					t.Fatalf("old future-ignored signal was consumed: %#v", signal.ConsumedBySequence)
				}
			}
		})
	})

	t.Run(factory.Name+"/future-signal-cursor-preserved", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			ref := createInstance(t, ctx, provider, createOptions{workflowID: "future-signal-cursor", waits: []durable.DurableWait{futureSignalWait("finish"), durable.TimerWait("tick", t0)}})
			session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
			timer := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows), "timer")
			signal, err := provider.AppendSignal(ctx, durable.AppendSignalInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, Type: "finish", Payload: map[string]any{"index": float64(1)}, ReceivedAt: t1,
			})
			requireNoError(t, err)
			committed, err := session.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: ref.WorkflowID, RunID: ref.RunID, ExpectedSequence: 0, ActivationID: timer.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.Running(map[string]any{}, durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}), Waits: []durable.DurableWait{futureSignalWaitWithAfter("finish", 999)}, Now: t1,
			})
			requireNoError(t, err)
			requireCommitOK(t, committed, 1)
			event := requireEventClaim(t, claimOne(t, ctx, session, "worker-a", t1, longLease, workflows), "signal")
			requireEqual(t, signal.SignalID, event.Activation.Event.ConsumeSignalID)
		})
	})

	t.Run(factory.Name+"/effects-retry-timeout-and-memoization", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			active := activeRun(t, ctx, provider, "effects")
			reserved := reserve(t, ctx, provider, active, "work", durable.ActivityOptions{HeartbeatTimeout: time.Second})
			requireEqual(t, 1, reserved.Attempt)
			requireNoError(t, provider.HeartbeatEffect(ctx, durable.HeartbeatEffectInput{
				WorkflowID: active.Ref.WorkflowID, RunID: active.Ref.RunID, ActivationID: active.Claim.Activation.ActivationID, WorkerID: "worker-a", EffectID: reserved.EffectID, AttemptID: reserved.AttemptID, Now: t0, Details: map[string]any{"progress": float64(1)},
			}))
			requireNoError(t, provider.CompleteEffect(ctx, durable.CompleteEffectInput{
				WorkflowID: active.Ref.WorkflowID, RunID: active.Ref.RunID, ActivationID: active.Claim.Activation.ActivationID, WorkerID: "worker-a", EffectID: reserved.EffectID, AttemptID: reserved.AttemptID, Result: map[string]any{"ok": true}, Now: t0,
			}))
			memo, err := provider.GetOrReserveEffect(ctx, durable.ReserveEffectInput{WorkflowID: active.Ref.WorkflowID, RunID: active.Ref.RunID, ActivationID: active.Claim.Activation.ActivationID, WorkerID: "worker-a", Key: "work", Now: t1})
			requireNoError(t, err)
			requireEqual(t, "completed", memo.Status)
			effects := load(t, ctx, provider, active.Ref, durable.LoadInstanceOptions{IncludeEffects: true}).Effects
			requireEqual(t, 1, len(effects))
			requireEqual(t, "completed", effects[0].Status)

			retry := activeRun(t, ctx, provider, "retry-effect")
			first := reserve(t, ctx, provider, retry, "retry", durable.ActivityOptions{Retry: durable.RetryPolicy{MaxAttempts: 2, InitialInterval: 500 * time.Millisecond, MaxInterval: 500 * time.Millisecond, BackoffCoefficient: 1}})
			requireNoError(t, provider.HeartbeatEffect(ctx, durable.HeartbeatEffectInput{WorkflowID: retry.Ref.WorkflowID, RunID: retry.Ref.RunID, ActivationID: retry.Claim.Activation.ActivationID, WorkerID: "worker-a", EffectID: first.EffectID, AttemptID: first.AttemptID, Now: t0, Details: map[string]any{"offset": float64(10)}}))
			failed, err := provider.FailEffect(ctx, durable.FailEffectInput{WorkflowID: retry.Ref.WorkflowID, RunID: retry.Ref.RunID, ActivationID: retry.Claim.Activation.ActivationID, WorkerID: "worker-a", EffectID: first.EffectID, AttemptID: first.AttemptID, Error: durable.SerializedError{Name: "RetryMe", Message: "try again"}, Now: t0})
			requireNoError(t, err)
			requireEqual(t, "retry_scheduled", failed.Status)
			requireNoError(t, retry.Session.ReleaseActivation(ctx, retry.Claim.Activation.ActivationID, "worker-a"))
			early, err := retry.Session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: t0.Add(100 * time.Millisecond), Lease: longLease, Limit: 1})
			requireNoError(t, err)
			requireEqual(t, 0, len(early.Claims))
			reclaimed := requireClaim(t, claimOne(t, ctx, retry.Session, "worker-a", failed.NextAttemptAt, longLease, workflows))
			requireEqual(t, retry.Claim.Activation.ActivationID, reclaimed.Activation.ActivationID)
			second := reserve(t, ctx, provider, activeClaim{Ref: retry.Ref, Claim: reclaimed, Session: retry.Session}, "retry", durable.ActivityOptions{Retry: durable.RetryPolicy{MaxAttempts: 2}})
			requireEqual(t, 2, second.Attempt)
			requireEqual(t, any(map[string]any{"offset": float64(10)}), second.HeartbeatDetails)

			timeout := activeRun(t, ctx, provider, "timeout-effect")
			timed := reserve(t, ctx, provider, timeout, "heartbeat-timeout", durable.ActivityOptions{HeartbeatTimeout: 500 * time.Millisecond, Retry: durable.RetryPolicy{MaxAttempts: 2}})
			requireNoError(t, provider.HeartbeatEffect(ctx, durable.HeartbeatEffectInput{WorkflowID: timeout.Ref.WorkflowID, RunID: timeout.Ref.RunID, ActivationID: timeout.Claim.Activation.ActivationID, WorkerID: "worker-a", EffectID: timed.EffectID, AttemptID: timed.AttemptID, Now: t0.Add(250 * time.Millisecond), Details: map[string]any{"page": float64(3)}}))
			timeoutAt := t0.Add(800 * time.Millisecond)
			if err := provider.HeartbeatActivations(ctx, []string{timeout.Claim.Activation.ActivationID}, "worker-a", timeoutAt, longLease); err == nil {
				t.Fatalf("heartbeat after activity timeout succeeded")
			}
			timeoutPoll, err := timeout.Session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: timeoutAt, Lease: longLease, Limit: 1, ShardCount: 1})
			requireNoError(t, err)
			if len(timeoutPoll.Claims) == 0 && timeoutPoll.NextWakeAt.IsZero() {
				t.Fatalf("timeout retry was not claimable and did not report a wake time")
			}
			if len(timeoutPoll.Claims) == 0 {
				timeoutPoll, err = timeout.Session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: workflows, Now: timeoutPoll.NextWakeAt, Lease: longLease, Limit: 1, ShardCount: 1})
				requireNoError(t, err)
			}
			retryClaim := requireClaim(t, timeoutPoll)
			retryReservation := reserve(t, ctx, provider, activeClaim{Ref: timeout.Ref, Claim: retryClaim, Session: timeout.Session}, "heartbeat-timeout", durable.ActivityOptions{HeartbeatTimeout: 500 * time.Millisecond, Retry: durable.RetryPolicy{MaxAttempts: 2}})
			requireEqual(t, 2, retryReservation.Attempt)
			requireEqual(t, any(map[string]any{"page": float64(3)}), retryReservation.HeartbeatDetails)
		})
	})

	t.Run(factory.Name+"/children-and-parent-close", func(t *testing.T) {
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			active := activeRun(t, ctx, provider, "children")
			if _, err := provider.CreateChildInstance(ctx, childCreateInput(active.Ref, active.Claim.Activation.ActivationID, "late", "late-child-without-lease")); err != nil {
				t.Fatalf("live child start failed: %v", err)
			}
			first, err := provider.CreateChildInstance(ctx, childCreateInput(active.Ref, active.Claim.Activation.ActivationID, "same", "same-child"))
			requireNoError(t, err)
			existing, err := provider.CreateChildInstance(ctx, withConflict(childCreateInput(active.Ref, active.Claim.Activation.ActivationID, "same", "ignored-child"), durable.ConflictUseExisting))
			requireNoError(t, err)
			requireEqual(t, first, existing)
			if _, err := provider.CreateChildInstance(ctx, withConflict(childCreateInput(active.Ref, active.Claim.Activation.ActivationID, "same", "fail-child"), durable.ConflictFail)); err == nil {
				t.Fatalf("child conflict with fail policy succeeded")
			}
			replacement, err := provider.CreateChildInstance(ctx, withConflict(childCreateInput(active.Ref, active.Claim.Activation.ActivationID, "same", "replacement-child"), durable.ConflictTerminateExisting))
			requireNoError(t, err)
			if load(t, ctx, provider, refFromHandle(first), durable.LoadInstanceOptions{}) != nil {
				t.Fatalf("terminated child still exists")
			}
			requireEqual(t, "replacement-child", replacement.WorkflowID)
		})
		withStore(t, factory, func(ctx context.Context, store Store) {
			provider := store.New(t).Provider
			parent := activeRun(t, ctx, provider, "parent-close")
			cancelChild, err := provider.CreateChildInstance(ctx, childCreateInput(parent.Ref, parent.Claim.Activation.ActivationID, "cancel", "close-cancel-child"))
			requireNoError(t, err)
			abandonChild, err := provider.CreateChildInstance(ctx, withParentClose(childCreateInput(parent.Ref, parent.Claim.Activation.ActivationID, "abandon", "close-abandon-child"), durable.ParentCloseAbandon))
			requireNoError(t, err)
			closed, err := provider.CommitCheckpoint(ctx, durable.CommitCheckpointInput{
				WorkflowID: parent.Ref.WorkflowID, RunID: parent.Ref.RunID, ExpectedSequence: 0, ActivationID: parent.Claim.Activation.ActivationID, WorkerID: "worker-a", WorkflowVersion: 1,
				Next: durable.InstanceStatus{Status: "canceled", Reason: "parent canceled"}, Now: t1,
			})
			requireNoError(t, err)
			requireCommitOK(t, closed, 1)
			requireEqual(t, "canceled", load(t, ctx, provider, refFromHandle(cancelChild), durable.LoadInstanceOptions{}).Status)
			requireEqual(t, "running", load(t, ctx, provider, refFromHandle(abandonChild), durable.LoadInstanceOptions{}).Status)
		})
	})
}

type createOptions struct {
	workflowName    string
	workflowVersion int
	workflowID      string
	common          durable.JSON
	phase           durable.PhaseSnapshot
	waits           []durable.DurableWait
}

type activeClaim struct {
	Ref     durable.InstanceRef
	Claim   durable.ClaimedActivationWithInstance
	Session durable.ShardDurabilitySession
}

func withStore(t *testing.T, factory Factory, fn func(context.Context, Store)) {
	t.Helper()
	ctx := context.Background()
	store := factory.NewStore(t)
	var handles []ProviderHandle
	originalNew := store.New
	store.New = func(t *testing.T) ProviderHandle {
		t.Helper()
		handle := originalNew(t)
		if handle.Provider == nil {
			t.Fatalf("provider factory returned nil provider")
		}
		handles = append(handles, handle)
		return handle
	}
	defer func() {
		for i := len(handles) - 1; i >= 0; i-- {
			if handles[i].Close != nil {
				_ = handles[i].Close(ctx)
			}
		}
		if store.Cleanup != nil {
			_ = store.Cleanup(ctx)
		}
	}()
	fn(ctx, store)
}

func createInstance(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, options createOptions) durable.InstanceRef {
	t.Helper()
	if options.workflowName == "" {
		options.workflowName = "conformance"
	}
	if options.workflowVersion == 0 {
		options.workflowVersion = 1
	}
	if options.workflowID == "" {
		options.workflowID = "conformance-instance"
	}
	if options.common == nil {
		options.common = map[string]any{}
	}
	if options.phase.Name == "" {
		options.phase = durable.PhaseSnapshot{Name: "run", Data: map[string]any{}}
	}
	if options.waits == nil {
		options.waits = []durable.DurableWait{runWait(t0)}
	}
	ref, err := provider.CreateInstance(ctx, durable.CreateInstanceInput{
		WorkflowName: options.workflowName, WorkflowVersion: options.workflowVersion, WorkflowID: options.workflowID, RunID: "run-1", PartitionShard: 0,
		Common: options.common, Phase: options.phase, Waits: options.waits, Now: t0, ConflictPolicy: durable.ConflictFail,
	})
	requireNoError(t, err)
	return ref.InstanceRef
}

func runWait(readyAt time.Time) durable.DurableWait {
	return durable.RunWait(readyAt)
}

func signalWait(name string) durable.DurableWait {
	return durable.SignalWait(name, name, false)
}

func futureSignalWait(name string) durable.DurableWait {
	return durable.SignalWaitWithOptions(name, name, false, durable.SignalWaitOptions{Delivery: durable.SignalDeliveryFuture})
}

func futureSignalWaitWithAfter(name string, after int64) durable.DurableWait {
	wait := futureSignalWait(name)
	wait.AfterSignalSequence = &after
	return wait
}

func ownShard(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, worker string, now time.Time, lease time.Duration) durable.ShardDurabilitySession {
	t.Helper()
	shard, err := provider.ClaimShard(ctx, durable.ClaimDispatchShardInput{ShardID: 0, OwnerID: worker, Now: now, Lease: lease})
	requireNoError(t, err)
	if shard == nil {
		t.Fatalf("%s did not claim shard", worker)
	}
	return provider.OpenShard(durable.OpenShardInput{ShardID: 0, OwnerID: worker, LeaseEpoch: shard.LeaseEpoch, LeaseUntil: shard.LeaseUntil})
}

func claimOne(t *testing.T, ctx context.Context, session durable.ShardDurabilitySession, worker string, now time.Time, lease time.Duration, versions map[string]int) durable.ClaimShardTasksResult {
	t.Helper()
	result, err := session.ClaimTasks(ctx, durable.ClaimShardTasksInput{Workflows: versions, Now: now, Lease: lease, Limit: 1, ShardCount: 1})
	requireNoError(t, err)
	return result
}

func requireClaim(t *testing.T, result durable.ClaimShardTasksResult) durable.ClaimedActivationWithInstance {
	t.Helper()
	if len(result.Claims) == 0 {
		t.Fatalf("expected activation claim, got none; nextWakeAt=%s", result.NextWakeAt)
	}
	claim := result.Claims[0]
	if claim.Instance.WorkflowID != claim.Activation.WorkflowID || claim.Instance.Sequence != claim.Activation.Sequence {
		t.Fatalf("claim instance does not match activation: %#v", claim)
	}
	return claim
}

func requireEventClaim(t *testing.T, result durable.ClaimShardTasksResult, kind string) durable.ClaimedActivationWithInstance {
	t.Helper()
	claim := requireClaim(t, result)
	requireEqual(t, "event", claim.Activation.Kind)
	if claim.Activation.Event == nil {
		t.Fatalf("event claim missing event: %#v", claim)
	}
	requireEqual(t, kind, claim.Activation.Event.Kind)
	return claim
}

func completeInput(claim durable.ClaimedActivationWithInstance, workerID string, output durable.JSON, now time.Time) durable.CommitCheckpointInput {
	return durable.CommitCheckpointInput{
		WorkflowID: claim.Activation.WorkflowID, RunID: claim.Activation.RunID, ExpectedSequence: claim.Activation.Sequence, ActivationID: claim.Activation.ActivationID,
		WorkerID: workerID, WorkflowVersion: claim.Instance.WorkflowVersion, Next: durable.InstanceStatus{Status: "completed", Output: output}, Now: now,
	}
}

func activeRun(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, workflowID string) activeClaim {
	t.Helper()
	ref := createInstance(t, ctx, provider, createOptions{workflowID: workflowID, waits: []durable.DurableWait{runWait(t0)}})
	session := ownShard(t, ctx, provider, "worker-a", t0, longLease)
	claim := requireClaim(t, claimOne(t, ctx, session, "worker-a", t0, longLease, workflows))
	requireEqual(t, workflowID, claim.Activation.WorkflowID)
	return activeClaim{Ref: ref, Claim: claim, Session: session}
}

func reserve(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, active activeClaim, key string, options durable.ActivityOptions) durable.EffectReservation {
	t.Helper()
	reservation, err := provider.GetOrReserveEffect(ctx, durable.ReserveEffectInput{
		WorkflowID: active.Ref.WorkflowID, RunID: active.Ref.RunID, ActivationID: active.Claim.Activation.ActivationID, WorkerID: "worker-a", Key: key, Now: t0, Options: options,
	})
	requireNoError(t, err)
	requireEqual(t, "reserved", reservation.Status)
	return reservation
}

func childCreateInput(parent durable.InstanceRef, activationID, key, workflowID string) durable.CreateChildInstanceInput {
	return durable.CreateChildInstanceInput{
		CreateInstanceInput: durable.CreateInstanceInput{
			WorkflowName: "conformance_child", WorkflowVersion: 1, WorkflowID: workflowID, RunID: "run-1", PartitionShard: 0,
			Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "run", Data: map[string]any{}}, Waits: []durable.DurableWait{runWait(t0)}, Now: t0, ConflictPolicy: durable.ConflictUseExisting,
		},
		ParentWorkflowID: parent.WorkflowID, ParentRunID: parent.RunID, ActivationID: activationID, WorkerID: "worker-a", LeaseNow: t0, Key: key, ParentClosePolicy: durable.ParentCloseCancel,
	}
}

func withConflict(input durable.CreateChildInstanceInput, policy durable.ConflictPolicy) durable.CreateChildInstanceInput {
	input.ConflictPolicy = policy
	input.CreateInstanceInput.ConflictPolicy = policy
	return input
}

func withParentClose(input durable.CreateChildInstanceInput, policy durable.ParentClosePolicy) durable.CreateChildInstanceInput {
	input.ParentClosePolicy = policy
	return input
}

func refFromHandle(handle durable.ChildHandleAny) durable.InstanceRef {
	return durable.InstanceRef{WorkflowID: handle.WorkflowID, RunID: handle.RunID}
}

func load(t *testing.T, ctx context.Context, provider durable.DurabilityProvider, ref durable.InstanceRef, options durable.LoadInstanceOptions) *durable.PersistedInstance {
	t.Helper()
	instance, err := provider.LoadInstance(ctx, ref, options)
	requireNoError(t, err)
	return instance
}

func requireCommitOK(t *testing.T, result durable.CommitCheckpointResult, sequence int64) {
	t.Helper()
	if !result.OK || result.Sequence != sequence {
		t.Fatalf("commit = %#v, want ok sequence %d", result, sequence)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireEqual[T any](t *testing.T, want, got T) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
}

func (h ProviderHandle) String() string {
	return fmt.Sprintf("%T", h.Provider)
}
