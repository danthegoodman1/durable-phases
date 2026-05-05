mod examples;

use chrono::{DateTime, Duration, TimeZone, Utc};
use durable::{
    complete, go, start, workflow, ActivityDurability, ActivityOptions, AppendSignalInput,
    CancelChildInput, CheckpointChildStart, ChildOptions, ChildRecord, ClaimShardInput,
    ClaimShardTasksInput, CommitCheckpointInput, ConflictPolicy, CreateChildInstanceInput,
    CreateInstanceInput, DrainOptions, DurabilityProvider, DurableRuntime, DurableWait,
    EffectReservation, FailEffectInput, FailEffectResult, GetWorkflowRunsInput,
    HeartbeatEffectInput, InstanceRef, InstanceStatusValue, LoadInstanceOptions,
    NullDurabilityProvider, OpenShardInput, ParentClosePolicy, PersistedInstance, PersistedStatus,
    PhaseSnapshot, PostgresDurabilityProvider, PostgresDurabilityProviderOptions,
    ReserveEffectInput, RunShardStepOptions, RunWorkerOptions, RuntimeOptions, SerializedError,
    SignalOptions, SignalRecord, SqliteDurabilityOptions, SqliteDurabilityProvider,
    SqliteShardFileDurabilityProvider, StartOptions, WorkerCancellation, WorkflowError,
    WorkflowIdReusePolicy, WorkflowRunDirection,
};
use examples::migration::{FinishEvent, MigratingOrderV1, MigratingOrderV2, MigrationInput};
use examples::parent_child::{
    self, Empty, Progress, TestChildInput, TestChildWorkflow, TestInput, TestWorkflow,
};
use examples::unstable::{self, UnstableInput, UnstableWorkflow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Barrier;

static TEST_LOCK: Mutex<()> = Mutex::new(());
static COMMIT_LOCAL_CHILD_SHOULD_THROW: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitLocalChildInput {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitLocalChildOutput {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitLocalChildPhase {}

workflow! {
    pub workflow CommitLocalChildWorkflow {
        name: "commit_local_child",
        version: 1,
        input: CommitLocalChildInput,
        output: CommitLocalChildOutput,
        common: Empty,

        initial(_input) {
            start! {
                common: Empty {},
                phase: running(CommitLocalChildPhase {}),
            }
        }

        phase running(_data: CommitLocalChildPhase) {
            run async |ctx| {
                ctx.child_start::<TestChildWorkflow>(
                    "child",
                    TestChildInput {
                        value: "buffered".to_string(),
                    },
                    ChildOptions::default(),
                ).await?;

                if COMMIT_LOCAL_CHILD_SHOULD_THROW.swap(false, Ordering::SeqCst) {
                    return Err(WorkflowError::new("boom after child start"));
                }

                complete!(CommitLocalChildOutput {})
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepShardInput {
    shard: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepShardOutput {
    shard: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepShardPhase {
    shard: u32,
}

workflow! {
    pub workflow StepShardWorkflow {
        name: "step_requested_shard",
        version: 1,
        input: StepShardInput,
        output: StepShardOutput,
        common: Empty,

        initial(input) {
            start! {
                common: Empty {},
                phase: finish(StepShardPhase { shard: input.shard }),
            }
        }

        phase finish(data: StepShardPhase) {
            run async |data| {
                complete!(StepShardOutput { shard: data.shard })
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepTimerInput {
    fire_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepTimerOutput {
    fire_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepTimerPhase {
    fire_at: DateTime<Utc>,
}

workflow! {
    pub workflow StepTimerWorkflow {
        name: "step_future_timer",
        version: 1,
        input: StepTimerInput,
        output: StepTimerOutput,
        common: Empty,

        initial(input) {
            start! {
                common: Empty {},
                phase: waiting(StepTimerPhase { fire_at: input.fire_at }),
            }
        }

        phase waiting(data: StepTimerPhase) {
            on {
                wake: timer(data.fire_at.clone()) async |data| {
                    complete!(StepTimerOutput { fire_at: data.fire_at })
                },
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FutureSignalInput {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FutureSignalOutput {
    value: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FutureSignalBoot {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FutureSignalWaiting {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FutureSignalEvent {
    value: i32,
}

workflow! {
    pub workflow FutureSignalMacroWorkflow {
        name: "future_signal_macro",
        version: 1,
        input: FutureSignalInput,
        output: FutureSignalOutput,
        common: Empty,

        initial(_input) {
            start! {
                common: Empty {},
                phase: boot(FutureSignalBoot {}),
            }
        }

        phase boot(_data: FutureSignalBoot) {
            run async |_ctx| {
                go!(waiting(FutureSignalWaiting {}))
            }
        }

        phase waiting(_data: FutureSignalWaiting) {
            on {
                finish: signal<FutureSignalEvent>(delivery = future) async |event| {
                    complete!(FutureSignalOutput { value: event.value })
                },
            }
        }
    }
}

#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )),
        }
    }

    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }

    fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

fn provider(path: &std::path::Path) -> SqliteDurabilityProvider {
    SqliteDurabilityProvider::new(path).unwrap()
}

fn make_runtime(path: &std::path::Path, clock: &ManualClock) -> DurableRuntime {
    DurableRuntime::with_clock(provider(path), {
        let clock = clock.clone();
        move || clock.now()
    })
}

fn workflow_id_for_shard(prefix: &str, shard_id: u32, shard_count: u32) -> String {
    for attempt in 0..10_000 {
        let workflow_id = format!("{prefix}-{attempt}");
        if durable::workflow_partition_shard(&workflow_id, "run-1", shard_count) == shard_id {
            return workflow_id;
        }
    }
    panic!("could not find workflow id for shard {shard_id}");
}

async fn load_runtime_instance(runtime: &DurableRuntime, ref_: &InstanceRef) -> PersistedInstance {
    runtime
        .provider()
        .load_instance(
            ref_,
            LoadInstanceOptions {
                include_effects: true,
            },
        )
        .await
        .unwrap()
        .unwrap()
}

fn mailbox_signal_wait(name: &str, signal_type: &str) -> DurableWait {
    DurableWait::Signal {
        name: name.to_string(),
        r#type: signal_type.to_string(),
        scope: durable::WaitScope::Phase,
        delivery: durable::SignalDelivery::Mailbox,
        after_signal_sequence: None,
    }
}

async fn force_terminal<P>(
    provider: &P,
    ref_: &InstanceRef,
    next: InstanceStatusValue,
    now: DateTime<Utc>,
) where
    P: DurabilityProvider,
{
    let result = provider
        .commit_checkpoint(CommitCheckpointInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            expected_sequence: 0,
            activation_id: format!("force-terminal/{}", ref_.run_id),
            workflow_version: 1,
            next,
            waits: Vec::new(),
            now,
            consume_signal_id: None,
            consume_child_record_id: None,
            effects: Vec::new(),
            child_starts: Vec::new(),
        })
        .await
        .unwrap();
    assert!(result.ok, "terminal commit failed: {result:?}");
}

async fn runtime_signals(runtime: &DurableRuntime) -> Vec<SignalRecord> {
    runtime.provider().list_signals().await.unwrap()
}

async fn runtime_children(runtime: &DurableRuntime) -> Vec<ChildRecord> {
    runtime.provider().list_children().await.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn workflow_id_reuse_policies_and_run_pagination_work() {
    let provider = NullDurabilityProvider::new();
    let clock = ManualClock::new();
    let runtime = DurableRuntime::with_clock(provider.clone(), {
        let clock = clock.clone();
        move || clock.now()
    });
    let workflow_id = "series-policy".to_string();

    let first = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(first.created);
    assert_ne!(first.run_id, "run-1");

    clock.advance(Duration::milliseconds(1));
    let duplicate = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(!duplicate.created);
    assert_eq!(duplicate.run_id, first.run_id);

    clock.advance(Duration::milliseconds(1));
    let always = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::Always,
            },
        )
        .await
        .unwrap();
    assert!(always.created);
    assert_ne!(always.run_id, first.run_id);
    force_terminal(
        &provider,
        &always,
        InstanceStatusValue::Completed {
            output: serde_json::json!({ "ok": true }),
        },
        clock.now(),
    )
    .await;

    clock.advance(Duration::milliseconds(1));
    let after_completed = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(after_completed.created);
    force_terminal(
        &provider,
        &after_completed,
        InstanceStatusValue::Completed {
            output: serde_json::json!({ "ok": true }),
        },
        clock.now(),
    )
    .await;

    clock.advance(Duration::milliseconds(1));
    let failed_only_after_completed = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::FailedOnly,
            },
        )
        .await
        .unwrap();
    assert!(!failed_only_after_completed.created);
    assert_eq!(failed_only_after_completed.run_id, after_completed.run_id);

    clock.advance(Duration::milliseconds(1));
    let failed = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::Always,
            },
        )
        .await
        .unwrap();
    force_terminal(
        &provider,
        &failed,
        InstanceStatusValue::Failed {
            error: SerializedError {
                name: None,
                message: "failed".to_string(),
            },
        },
        clock.now(),
    )
    .await;

    clock.advance(Duration::milliseconds(1));
    let after_failed = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::FailedOnly,
            },
        )
        .await
        .unwrap();
    assert!(after_failed.created);

    clock.advance(Duration::milliseconds(1));
    let canceled = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::Always,
            },
        )
        .await
        .unwrap();
    force_terminal(
        &provider,
        &canceled,
        InstanceStatusValue::Canceled {
            reason: "canceled".to_string(),
        },
        clock.now(),
    )
    .await;

    clock.advance(Duration::milliseconds(1));
    let after_canceled = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some(workflow_id.clone()),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::FailedOnly,
            },
        )
        .await
        .unwrap();
    assert!(after_canceled.created);

    let asc_head = runtime
        .get_workflow_runs(GetWorkflowRunsInput {
            id: workflow_id.clone(),
            cursor: None,
            limit: Some(2),
            direction: Some(WorkflowRunDirection::Asc),
            include_effects: Some(false),
        })
        .await
        .unwrap();
    assert_eq!(asc_head.runs.len(), 2);
    assert_eq!(asc_head.runs[0].run_id, first.run_id);
    assert_eq!(asc_head.runs[1].run_id, always.run_id);
    assert!(asc_head.cursor.is_some());

    let asc_next = runtime
        .get_workflow_runs(GetWorkflowRunsInput {
            id: workflow_id.clone(),
            cursor: asc_head.cursor.clone(),
            limit: Some(10),
            direction: Some(WorkflowRunDirection::Asc),
            include_effects: Some(false),
        })
        .await
        .unwrap();
    assert_eq!(
        asc_next.runs.first().unwrap().run_id,
        after_completed.run_id
    );

    let desc_tail = runtime
        .get_workflow_runs(GetWorkflowRunsInput {
            id: workflow_id.clone(),
            cursor: None,
            limit: Some(1),
            direction: Some(WorkflowRunDirection::Desc),
            include_effects: Some(false),
        })
        .await
        .unwrap();
    assert_eq!(desc_tail.runs.len(), 1);
    assert_eq!(desc_tail.runs[0].run_id, after_canceled.run_id);

    let wrong_direction = runtime
        .get_workflow_runs(GetWorkflowRunsInput {
            id: workflow_id,
            cursor: asc_head.cursor,
            limit: Some(1),
            direction: Some(WorkflowRunDirection::Desc),
            include_effects: Some(false),
        })
        .await;
    assert!(wrong_direction.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn run_shard_step_processes_only_the_requested_shard() {
    let provider = NullDurabilityProvider::new();
    let clock = ManualClock::new();
    let mut options = RuntimeOptions::default();
    options.shard_count = 3;
    options.dispatch_shard_ids = vec![0];
    options.worker_id = "step-worker".to_string();
    let runtime = DurableRuntime::with_options(provider.clone(), options, {
        let clock = clock.clone();
        move || clock.now()
    });

    let mut refs = Vec::new();
    for shard in 0..3 {
        let ref_ = runtime
            .start::<StepShardWorkflow>(
                StepShardInput { shard },
                StartOptions {
                    workflow_id: Some(workflow_id_for_shard(
                        &format!("step-requested-{shard}"),
                        shard,
                        3,
                    )),
                    ..StartOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(runtime.shard_for_ref(&ref_), shard);
        refs.push(ref_);
    }

    let result = runtime
        .run_shard_step(RunShardStepOptions {
            shard_id: 1,
            max_activations: Some(1),
            cancellation: None,
        })
        .await
        .unwrap();
    assert_eq!(result.shard_id, 1);
    assert!(result.claimed_shard);
    assert_eq!(result.activations, 1);
    assert_eq!(result.next_wake_at, None);

    let first = load_runtime_instance(&runtime, &refs[0]).await;
    let second = load_runtime_instance(&runtime, &refs[1]).await;
    let third = load_runtime_instance(&runtime, &refs[2]).await;
    assert_eq!(first.status, PersistedStatus::Running);
    assert_eq!(first.sequence, 0);
    assert_eq!(second.status, PersistedStatus::Completed);
    assert_eq!(second.sequence, 1);
    assert_eq!(second.output.unwrap()["shard"], 1);
    assert_eq!(third.status, PersistedStatus::Running);
    assert_eq!(third.sequence, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn run_shard_step_reports_when_another_worker_owns_the_shard() {
    let provider = NullDurabilityProvider::new();
    let clock = ManualClock::new();
    provider
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "existing-owner".to_string(),
            now: clock.now(),
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let mut options = RuntimeOptions::default();
    options.shard_count = 2;
    options.worker_id = "step-worker".to_string();
    let runtime = DurableRuntime::with_options(provider, options, {
        let clock = clock.clone();
        move || clock.now()
    });

    let result = runtime
        .run_shard_step(RunShardStepOptions {
            shard_id: 0,
            max_activations: Some(1),
            cancellation: None,
        })
        .await
        .unwrap();
    assert_eq!(result.shard_id, 0);
    assert!(!result.claimed_shard);
    assert_eq!(result.activations, 0);
    assert_eq!(result.next_wake_at, None);
}

#[tokio::test(flavor = "current_thread")]
async fn run_shard_step_returns_next_wake_at_for_future_timer() {
    let provider = NullDurabilityProvider::new();
    let clock = ManualClock::new();
    let mut options = RuntimeOptions::default();
    options.shard_count = 3;
    options.worker_id = "timer-step-worker".to_string();
    let runtime = DurableRuntime::with_options(provider, options, {
        let clock = clock.clone();
        move || clock.now()
    });
    let fire_at = clock.now() + Duration::milliseconds(5_000);
    let ref_ = runtime
        .start::<StepTimerWorkflow>(
            StepTimerInput { fire_at },
            StartOptions {
                workflow_id: Some(workflow_id_for_shard("step-future-timer", 2, 3)),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let result = runtime
        .run_shard_step(RunShardStepOptions {
            shard_id: runtime.shard_for_ref(&ref_),
            max_activations: Some(1),
            cancellation: None,
        })
        .await
        .unwrap();
    assert_eq!(result.shard_id, 2);
    assert!(result.claimed_shard);
    assert_eq!(result.activations, 0);
    assert_eq!(result.next_wake_at, Some(fire_at));
}

#[tokio::test(flavor = "current_thread")]
async fn run_shard_step_validates_shard_ids() {
    let provider = NullDurabilityProvider::new();
    let mut options = RuntimeOptions::default();
    options.shard_count = 2;
    let runtime = DurableRuntime::with_options(provider, options, Utc::now);

    let error = runtime
        .run_shard_step(RunShardStepOptions {
            shard_id: 2,
            max_activations: Some(1),
            cancellation: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "shard_id must be between 0 and 1");
}

#[tokio::test(flavor = "current_thread")]
async fn persists_initial_snapshot_and_reloads() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            StartOptions {
                workflow_id: Some("parent-1".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let persisted = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(persisted.sequence, 0);
    assert_eq!(persisted.phase.unwrap().name, "boot");
    assert_eq!(persisted.common.unwrap()["label"], "Ada");
    assert_eq!(
        persisted.waits,
        vec![DurableWait::Run {
            name: "__run".to_string(),
            ready_at: clock.now(),
        }]
    );

    let reloaded = provider(&path).load_instance(&ref_).await.unwrap().unwrap();
    assert_eq!(reloaded.sequence, 0);
    assert_eq!(reloaded.phase.unwrap().name, "boot");
}

#[tokio::test(flavor = "current_thread")]
async fn survives_restart_with_pending_timer_and_stay_checkpoint() {
    let _guard = TEST_LOCK.lock().unwrap();
    parent_child::reset_reminders();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("timer-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(
        load_runtime_instance(&runtime, &ref_)
            .await
            .phase
            .unwrap()
            .name,
        "waiting"
    );

    clock.advance(Duration::milliseconds(1_000));
    let restarted = make_runtime(&path, &clock);
    restarted.register::<TestWorkflow>().unwrap();
    restarted.register::<TestChildWorkflow>().unwrap();
    restarted
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();

    let persisted = load_runtime_instance(&restarted, &ref_).await;
    assert_eq!(persisted.sequence, 2);
    assert_eq!(persisted.phase.as_ref().unwrap().name, "waiting");
    assert_eq!(persisted.phase.unwrap().data["reminders"], 1);
    assert_eq!(parent_child::reminders(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn persists_signals_and_consumes_atomically_with_go_checkpoint() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string(), "b".to_string()],
            },
            StartOptions {
                workflow_id: Some("signal-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    runtime.signal(&ref_, "begin", Empty {}).await.unwrap();
    assert_eq!(
        runtime_signals(&runtime).await[0].consumed_by_sequence,
        None
    );

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let persisted = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(persisted.sequence, 2);
    assert_eq!(persisted.phase.unwrap().name, "processing");
    assert_eq!(
        runtime_signals(&runtime).await[0].consumed_by_sequence,
        Some(2)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_signal_with_options_deduplicates_idempotency_key() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("idempotent-signal-runtime".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let first = runtime
        .signal_with_options(
            &ref_,
            "begin",
            Empty {},
            SignalOptions {
                idempotency_key: Some("send-1".to_string()),
            },
        )
        .await
        .unwrap();
    let duplicate = runtime
        .signal_with_options(
            &ref_,
            "begin",
            Empty {},
            SignalOptions {
                idempotency_key: Some("send-1".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate, first);

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let after_consumed = runtime
        .signal_with_options(
            &ref_,
            "begin",
            Empty {},
            SignalOptions {
                idempotency_key: Some("send-1".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(after_consumed.signal_id, first.signal_id);
    assert_eq!(after_consumed.consumed_by_sequence, Some(2));
    assert_eq!(runtime_signals(&runtime).await.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn workflow_macro_supports_future_only_signal_waits() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);

    let ref_ = runtime
        .start::<FutureSignalMacroWorkflow>(
            FutureSignalInput {},
            StartOptions {
                workflow_id: Some("future-signal-macro".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    let old_signal = runtime
        .signal(&ref_, "finish", FutureSignalEvent { value: 1 })
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let waiting = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(waiting.phase.as_ref().unwrap().name, "waiting");
    assert_eq!(waiting.waits.len(), 1);
    match &waiting.waits[0] {
        DurableWait::Signal {
            delivery,
            after_signal_sequence,
            ..
        } => {
            assert_eq!(*delivery, durable::SignalDelivery::Future);
            assert_eq!(*after_signal_sequence, Some(1));
        }
        other => panic!("expected future signal wait, got {:?}", other),
    }

    let drained = runtime.drain(DrainOptions::default()).await.unwrap();
    assert_eq!(drained.activations, 0);
    let new_signal = runtime
        .signal(&ref_, "finish", FutureSignalEvent { value: 2 })
        .await
        .unwrap();
    runtime.drain(DrainOptions::default()).await.unwrap();
    let completed = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.output.unwrap()["value"], 2);

    let signals = runtime_signals(&runtime).await;
    assert_eq!(
        signals
            .iter()
            .find(|signal| signal.signal_id == old_signal.signal_id)
            .unwrap()
            .consumed_by_sequence,
        None
    );
    assert_eq!(
        signals
            .iter()
            .find(|signal| signal.signal_id == new_signal.signal_id)
            .unwrap()
            .consumed_by_sequence,
        Some(2)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn uses_checkpoint_for_bounded_loop_pattern() {
    let _guard = TEST_LOCK.lock().unwrap();
    parent_child::reset_processed();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            StartOptions {
                workflow_id: Some("loop-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    runtime.signal(&ref_, "begin", Empty {}).await.unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(3),
        })
        .await
        .unwrap();

    let persisted = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(persisted.sequence, 5);
    assert_eq!(persisted.phase.as_ref().unwrap().name, "processing");
    assert_eq!(
        persisted.phase.unwrap().data["processed"],
        serde_json::json!(["a!", "b!", "c!"])
    );
    assert_eq!(parent_child::processed(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn wakes_parent_from_completed_child_after_reconstruction() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            StartOptions {
                workflow_id: Some("child-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    runtime.signal(&ref_, "begin", Empty {}).await.unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(5),
        })
        .await
        .unwrap();
    assert_eq!(
        load_runtime_instance(&runtime, &ref_)
            .await
            .phase
            .unwrap()
            .name,
        "waiting_child"
    );

    let restarted = make_runtime(&path, &clock);
    restarted.register::<TestWorkflow>().unwrap();
    restarted.register::<TestChildWorkflow>().unwrap();
    restarted
        .drain(DrainOptions {
            max_activations: Some(2),
        })
        .await
        .unwrap();

    let parent = load_runtime_instance(&restarted, &ref_).await;
    let children = runtime_children(&restarted).await;
    assert_eq!(children[0].status, durable::ChildStatus::Completed);
    assert_eq!(children[0].delivered_by_sequence, Some(parent.sequence));
    assert_eq!(parent.phase.as_ref().unwrap().name, "cooldown");
    assert_eq!(
        parent.phase.as_ref().unwrap().data["child_value"],
        "child:a!,b!,c!"
    );

    let before_query = parent.sequence;
    let progress: Progress = restarted
        .query::<TestWorkflow, Progress>(&ref_, "progress")
        .await
        .unwrap();
    assert_eq!(progress.sequence, before_query);
    assert_eq!(
        load_runtime_instance(&restarted, &ref_).await.sequence,
        before_query
    );

    clock.advance(Duration::milliseconds(1_000));
    restarted
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let completed = load_runtime_instance(&restarted, &ref_).await;
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.output.unwrap()["child_value"], "child:a!,b!,c!");
}

#[tokio::test(flavor = "current_thread")]
async fn memoizes_completed_activities_across_failed_activation_retry() {
    let _guard = TEST_LOCK.lock().unwrap();
    unstable::reset(true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);

    let ref_ = runtime
        .start::<UnstableWorkflow>(
            UnstableInput {},
            StartOptions {
                workflow_id: Some("unstable-1".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let error = runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "boom after durable effect");
    assert_eq!(unstable::activity_calls(), 1);
    assert_eq!(load_runtime_instance(&runtime, &ref_).await.sequence, 0);

    let restarted = make_runtime(&path, &clock);
    restarted.register::<UnstableWorkflow>().unwrap();
    restarted
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();

    assert_eq!(unstable::activity_calls(), 1);
    let completed = load_runtime_instance(&restarted, &ref_).await;
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.sequence, 1);
    assert_eq!(completed.output.unwrap()["ok"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn migrates_running_instance_and_recomputes_waits() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime_v1 = make_runtime(&path, &clock);

    let ref_ = runtime_v1
        .start::<MigratingOrderV1>(
            MigrationInput {
                customer_id: "Ada".to_string(),
            },
            StartOptions {
                workflow_id: Some("migration-1".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let before = load_runtime_instance(&runtime_v1, &ref_).await;
    assert_eq!(before.workflow_version, 1);
    assert_eq!(before.sequence, 0);
    assert_eq!(before.phase.as_ref().unwrap().name, "waiting");

    let runtime_v2 = make_runtime(&path, &clock);
    runtime_v2.register::<MigratingOrderV2>().unwrap();
    runtime_v2
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();

    let migrated = load_runtime_instance(&runtime_v2, &ref_).await;
    assert_eq!(migrated.workflow_version, 2);
    assert_eq!(migrated.sequence, 1);
    assert_eq!(migrated.common.as_ref().unwrap()["plan"], "starter");
    assert_eq!(migrated.phase.as_ref().unwrap().name, "waiting_for_finish");
    assert_eq!(
        migrated.phase.as_ref().unwrap().data["migrated_from"],
        "waiting"
    );

    runtime_v2
        .signal(
            &ref_,
            "finish",
            FinishEvent {
                punctuation: "!".to_string(),
            },
        )
        .await
        .unwrap();
    runtime_v2.drain(DrainOptions::default()).await.unwrap();
    let completed = load_runtime_instance(&runtime_v2, &ref_).await;
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.sequence, 2);
    assert_eq!(
        completed.output.unwrap()["message"],
        "hello, Ada on starter!"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_provider_persists_state_and_uses_wal_full() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let provider = SqliteDurabilityProvider::new(&path).unwrap();
    assert_eq!(
        provider
            .pragma_string("journal_mode")
            .unwrap()
            .to_lowercase(),
        "wal"
    );
    assert_eq!(provider.pragma_i64("synchronous").unwrap(), 2);

    let runtime = DurableRuntime::with_clock(provider.clone(), {
        let clock = clock.clone();
        move || clock.now()
    });
    runtime.register::<TestChildWorkflow>().unwrap();
    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("sqlite-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();

    let reloaded = SqliteDurabilityProvider::new(&path).unwrap();
    let loaded = reloaded.load_instance(&ref_).await.unwrap().unwrap();
    assert_eq!(loaded.sequence, 1);
    assert_eq!(loaded.phase.unwrap().name, "waiting");

    let connection = rusqlite::Connection::open(&path).unwrap();
    let columns = sqlite_columns(&connection, "shard_journal");
    assert!(columns.iter().any(|column| column == "operation_json"));
    assert!(!columns.iter().any(|column| column == "snapshot_json"));
    let operation_json: String = connection
        .query_row(
            "SELECT operation_json FROM shard_journal ORDER BY entry_id ASC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(operation_json.contains("create_instance"));
    assert!(!operation_json.contains("\"instances\""));
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_provider_recovers_from_snapshot_plus_journal_tail() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let provider = SqliteDurabilityProvider::new_with_options(
        &path,
        SqliteDurabilityOptions {
            snapshot_interval: 2,
        },
    )
    .unwrap();
    let ref_ = provider
        .create_instance(CreateInstanceInput {
            workflow_name: "sqlite-tail".to_string(),
            workflow_version: 1,
            workflow_id: "sqlite-tail".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![DurableWait::Signal {
                name: "continue".to_string(),
                r#type: "continue".to_string(),
                scope: durable::WaitScope::Phase,
                delivery: durable::SignalDelivery::Mailbox,
                after_signal_sequence: None,
            }],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    provider
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "continue".to_string(),
            payload: serde_json::json!({ "index": 1 }),
            received_at: now,
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    let duplicate = provider
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "continue".to_string(),
            payload: serde_json::json!({ "index": 99 }),
            received_at: now + Duration::milliseconds(500),
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(duplicate.payload["index"], 1);
    provider
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "continue".to_string(),
            payload: serde_json::json!({ "index": 2 }),
            received_at: now + Duration::seconds(1),
            idempotency_key: None,
        })
        .await
        .unwrap();

    let connection = rusqlite::Connection::open(&path).unwrap();
    let snapshot_entry: i64 = connection
        .query_row(
            "SELECT last_entry_id FROM shard_snapshots WHERE snapshot_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(snapshot_entry, 2);

    let reloaded = SqliteDurabilityProvider::new_with_options(
        &path,
        SqliteDurabilityOptions {
            snapshot_interval: 2,
        },
    )
    .unwrap();
    let signals = reloaded.list_signals().await.unwrap();
    assert_eq!(signals.len(), 2);
    assert_eq!(signals[0].payload["index"], 1);
    assert_eq!(signals[1].payload["index"], 2);
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_provider_catches_up_stale_instances_before_reads_writes_and_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let provider_a = SqliteDurabilityProvider::new_with_options(
        &path,
        SqliteDurabilityOptions {
            snapshot_interval: 2,
        },
    )
    .unwrap();
    let provider_b = SqliteDurabilityProvider::new_with_options(
        &path,
        SqliteDurabilityOptions {
            snapshot_interval: 2,
        },
    )
    .unwrap();
    let ref_ = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_sqlite_catchup".to_string(),
            workflow_version: 1,
            workflow_id: "rust-sqlite-catchup".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    assert!(provider_b.load_instance(&ref_).await.unwrap().is_some());
    let duplicate_start = provider_b
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_sqlite_catchup".to_string(),
            workflow_version: 1,
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            partition_shard: 0,
            common: serde_json::json!({ "duplicate": true }),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::UseExisting),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    assert!(!duplicate_start.created);

    let first = provider_a
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "a" }),
            received_at: now,
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    let second = provider_a
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "second" }),
            received_at: now + Duration::seconds(1),
            idempotency_key: Some("request-2".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(
        provider_b
            .list_signals()
            .await
            .unwrap()
            .into_iter()
            .map(|signal| signal.signal_id)
            .collect::<Vec<_>>(),
        vec![first.signal_id.clone(), second.signal_id.clone()]
    );
    let duplicate = provider_b
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "b" }),
            received_at: now + Duration::seconds(2),
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(duplicate.signal_id, first.signal_id);
    assert_eq!(duplicate.payload["sender"], "a");

    let connection = rusqlite::Connection::open(&path).unwrap();
    let journal_entries: i64 = connection
        .query_row("SELECT count(*) FROM shard_journal", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_entries, 3);
    drop(connection);

    let session_ref = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_sqlite_session_catchup".to_string(),
            workflow_version: 1,
            workflow_id: "rust-sqlite-session-catchup".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let lease = provider_a
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "worker-a".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let stale_session = provider_a.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some(lease.owner_id.clone()),
        lease_epoch: Some(lease.lease_epoch),
    });
    let before_signal = stale_session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("rust_sqlite_session_catchup".to_string(), 1)]),
            shard_count: 1,
            now,
            lease_ms: 60_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert!(before_signal.claims.is_empty());
    stale_session
        .release_activation("missing-activation", "worker-a")
        .await
        .unwrap();

    let third = provider_b
        .append_signal(AppendSignalInput {
            workflow_id: session_ref.workflow_id.clone(),
            run_id: session_ref.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "third" }),
            received_at: now + Duration::seconds(3),
            idempotency_key: Some("request-3".to_string()),
        })
        .await
        .unwrap();
    let duplicate_from_session = stale_session
        .append_signal_with_status(AppendSignalInput {
            workflow_id: session_ref.workflow_id.clone(),
            run_id: session_ref.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "session" }),
            received_at: now + Duration::seconds(4),
            idempotency_key: Some("request-3".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(duplicate_from_session.0.signal_id, third.signal_id);
    assert!(!duplicate_from_session.1);
    let claim = stale_session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("rust_sqlite_session_catchup".to_string(), 1)]),
            shard_count: 1,
            now: now + Duration::seconds(4),
            lease_ms: 60_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(claim.claims.len(), 1);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let journal_entries: i64 = connection
        .query_row("SELECT count(*) FROM shard_journal", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_entries, 7);

    let parent_ref = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_sqlite_child_parent".to_string(),
            workflow_version: 1,
            workflow_id: "rust-sqlite-child-parent".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: Vec::new(),
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let child_input = CreateChildInstanceInput {
        workflow_name: "rust_sqlite_child".to_string(),
        workflow_version: 1,
        workflow_id: "rust-sqlite-child".to_string(),
        run_id: "run-1".to_string(),
        partition_shard: 0,
        common: serde_json::json!({}),
        phase: PhaseSnapshot {
            name: "waiting".to_string(),
            data: serde_json::json!({}),
        },
        waits: Vec::new(),
        now,
        parent_workflow_id: parent_ref.workflow_id.clone(),
        parent_run_id: parent_ref.run_id.clone(),
        activation_id: "activation-child".to_string(),
        worker_id: "worker-a".to_string(),
        lease_now: now,
        key: "child".to_string(),
        parent_close_policy: ParentClosePolicy::Cancel,
        conflict_policy: ConflictPolicy::Fail,
        workflow_id_reuse_policy: None,
    };
    let child = provider_a
        .create_child_instance(child_input.clone())
        .await
        .unwrap();
    let duplicate_child = provider_b
        .create_child_instance(child_input.clone())
        .await
        .unwrap();
    assert_eq!(duplicate_child, child);
    let cancel_child = CancelChildInput {
        parent_workflow_id: parent_ref.workflow_id.clone(),
        parent_run_id: parent_ref.run_id.clone(),
        activation_id: "activation-child".to_string(),
        worker_id: "worker-a".to_string(),
        workflow_id: child.workflow_id.clone(),
        run_id: child.run_id.clone(),
        now,
    };
    provider_b.cancel_child(cancel_child.clone()).await.unwrap();
    stale_session.cancel_child(cancel_child).await.unwrap();

    let connection = rusqlite::Connection::open(&path).unwrap();
    let journal_entries: i64 = connection
        .query_row("SELECT count(*) FROM shard_journal", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_entries, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_provider_catches_up_stale_instances_before_reads_writes_and_sessions_when_configured(
) {
    let Ok(connection_string) = std::env::var("DURABLE_POSTGRES_URL") else {
        return;
    };
    let schema = format!(
        "durable_rust_catchup_{}_{}",
        std::process::id(),
        Utc::now().timestamp_millis().abs()
    );
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let provider_a = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(2),
    })
    .await
    .unwrap();
    let provider_b = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(2),
    })
    .await
    .unwrap();

    let ref_ = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_catchup".to_string(),
            workflow_version: 1,
            workflow_id: "rust-postgres-catchup".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    assert!(provider_b
        .load_instance(
            &ref_,
            LoadInstanceOptions {
                include_effects: true
            },
        )
        .await
        .unwrap()
        .is_some());
    let duplicate_start = provider_b
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_catchup".to_string(),
            workflow_version: 1,
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            partition_shard: 0,
            common: serde_json::json!({ "duplicate": true }),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::UseExisting),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    assert!(!duplicate_start.created);

    let first = provider_a
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "a" }),
            received_at: now,
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    let second = provider_a
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "second" }),
            received_at: now + Duration::seconds(1),
            idempotency_key: Some("request-2".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(
        provider_b
            .list_signals()
            .await
            .unwrap()
            .into_iter()
            .map(|signal| signal.signal_id)
            .collect::<Vec<_>>(),
        vec![first.signal_id.clone(), second.signal_id.clone()]
    );
    let duplicate = provider_b
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "b" }),
            received_at: now + Duration::seconds(2),
            idempotency_key: Some("request-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(duplicate.signal_id, first.signal_id);
    assert_eq!(duplicate.payload["sender"], "a");

    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let journal_table = format!("\"{schema}\".\"shard_journal_p00\"");
    let journal_entries: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {journal_table} WHERE shard_id = 0"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal_entries, 3);
    let snapshot_table = format!("\"{schema}\".\"shard_snapshots_p00\"");
    let snapshot_json: String = client
        .query_one(
            &format!("SELECT snapshot_json FROM {snapshot_table} WHERE shard_id = 0"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let snapshot_value: serde_json::Value = serde_json::from_str(&snapshot_json).unwrap();
    assert!(snapshot_value.get("store").is_some());
    assert!(snapshot_value.get("applied_entries").is_some());

    let old_snapshot_json = serde_json::json!({
        "instances": {},
        "signals": [],
        "children": [],
        "tasks": {},
        "claimed_sequence_epochs": {},
        "completed_activation_claims": [],
        "shard_leases": {},
        "next_signal_id": 1,
        "next_effect_id": 1,
        "next_child_id": 1
    })
    .to_string();
    client
        .execute(
            &format!("UPDATE {snapshot_table} SET snapshot_json = $1 WHERE shard_id = 0"),
            &[&old_snapshot_json],
        )
        .await
        .unwrap();
    let old_snapshot_reloaded =
        PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
            connection_string: connection_string.clone(),
            schema: Some(schema.clone()),
            physical_partitions: 2,
            snapshot_interval: Some(2),
        })
        .await
        .unwrap();
    assert_eq!(
        old_snapshot_reloaded
            .list_signals()
            .await
            .unwrap()
            .into_iter()
            .map(|signal| signal.signal_id)
            .collect::<Vec<_>>(),
        vec![first.signal_id.clone(), second.signal_id.clone()]
    );

    let session_ref = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_session_catchup".to_string(),
            workflow_version: 1,
            workflow_id: "rust-postgres-session-catchup".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let lease = provider_a
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "worker-a".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let stale_session = provider_a.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some(lease.owner_id.clone()),
        lease_epoch: Some(lease.lease_epoch),
    });
    let before_signal = stale_session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("rust_postgres_session_catchup".to_string(), 1)]),
            shard_count: 1,
            now,
            lease_ms: 60_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert!(before_signal.claims.is_empty());
    stale_session
        .release_activation("missing-activation", "worker-a")
        .await
        .unwrap();

    let third = provider_b
        .append_signal(AppendSignalInput {
            workflow_id: session_ref.workflow_id.clone(),
            run_id: session_ref.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "third" }),
            received_at: now + Duration::seconds(3),
            idempotency_key: Some("request-3".to_string()),
        })
        .await
        .unwrap();
    let duplicate_from_session = stale_session
        .append_signal_with_status(AppendSignalInput {
            workflow_id: session_ref.workflow_id.clone(),
            run_id: session_ref.run_id.clone(),
            r#type: "finish".to_string(),
            payload: serde_json::json!({ "sender": "session" }),
            received_at: now + Duration::seconds(4),
            idempotency_key: Some("request-3".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(duplicate_from_session.0.signal_id, third.signal_id);
    assert!(!duplicate_from_session.1);
    let claim = stale_session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("rust_postgres_session_catchup".to_string(), 1)]),
            shard_count: 1,
            now: now + Duration::seconds(4),
            lease_ms: 60_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(claim.claims.len(), 1);

    let journal_entries: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {journal_table} WHERE shard_id = 0"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal_entries, 7);

    let parent_ref = provider_a
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_child_parent".to_string(),
            workflow_version: 1,
            workflow_id: "rust-postgres-child-parent".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: Vec::new(),
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let child_input = CreateChildInstanceInput {
        workflow_name: "rust_postgres_child".to_string(),
        workflow_version: 1,
        workflow_id: "rust-postgres-child".to_string(),
        run_id: "run-1".to_string(),
        partition_shard: 0,
        common: serde_json::json!({}),
        phase: PhaseSnapshot {
            name: "waiting".to_string(),
            data: serde_json::json!({}),
        },
        waits: Vec::new(),
        now,
        parent_workflow_id: parent_ref.workflow_id.clone(),
        parent_run_id: parent_ref.run_id.clone(),
        activation_id: "activation-child".to_string(),
        worker_id: "worker-a".to_string(),
        lease_now: now,
        key: "child".to_string(),
        parent_close_policy: ParentClosePolicy::Cancel,
        conflict_policy: ConflictPolicy::Fail,
        workflow_id_reuse_policy: None,
    };
    let child = provider_a
        .create_child_instance(child_input.clone())
        .await
        .unwrap();
    let duplicate_child = provider_b
        .create_child_instance(child_input.clone())
        .await
        .unwrap();
    assert_eq!(duplicate_child, child);
    let cancel_child = CancelChildInput {
        parent_workflow_id: parent_ref.workflow_id.clone(),
        parent_run_id: parent_ref.run_id.clone(),
        activation_id: "activation-child".to_string(),
        worker_id: "worker-a".to_string(),
        workflow_id: child.workflow_id.clone(),
        run_id: child.run_id.clone(),
        now,
    };
    provider_b.cancel_child(cancel_child.clone()).await.unwrap();
    stale_session.cancel_child(cancel_child).await.unwrap();

    let journal_entries: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {journal_table} WHERE shard_id = 0"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(journal_entries, 10);

    client
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_provider_serializes_concurrent_cross_shard_signal_ids_when_configured() {
    let Ok(connection_string) = std::env::var("DURABLE_POSTGRES_URL") else {
        return;
    };
    let schema = format!(
        "durable_rust_cross_shard_ids_{}_{}",
        std::process::id(),
        Utc::now().timestamp_millis().abs()
    );
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let bootstrap = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(100),
    })
    .await
    .unwrap();
    let ref_a = bootstrap
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_cross_shard_a".to_string(),
            workflow_version: 1,
            workflow_id: "rust-postgres-cross-shard-a".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let ref_b = bootstrap
        .create_instance(CreateInstanceInput {
            workflow_name: "rust_postgres_cross_shard_b".to_string(),
            workflow_version: 1,
            workflow_id: "rust-postgres-cross-shard-b".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 1,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![mailbox_signal_wait("finish", "finish")],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();

    let provider_a = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(100),
    })
    .await
    .unwrap();
    let provider_b = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(100),
    })
    .await
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let task_a = tokio::spawn({
        let provider = provider_a.clone();
        let ref_ = ref_a.clone();
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            provider
                .append_signal(AppendSignalInput {
                    workflow_id: ref_.workflow_id.clone(),
                    run_id: ref_.run_id.clone(),
                    r#type: "finish".to_string(),
                    payload: serde_json::json!({ "sender": "a" }),
                    received_at: now,
                    idempotency_key: Some("cross-shard-a".to_string()),
                })
                .await
        }
    });
    let task_b = tokio::spawn({
        let provider = provider_b.clone();
        let ref_ = ref_b.clone();
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            provider
                .append_signal(AppendSignalInput {
                    workflow_id: ref_.workflow_id.clone(),
                    run_id: ref_.run_id.clone(),
                    r#type: "finish".to_string(),
                    payload: serde_json::json!({ "sender": "b" }),
                    received_at: now + Duration::milliseconds(1),
                    idempotency_key: Some("cross-shard-b".to_string()),
                })
                .await
        }
    });
    let (signal_a, signal_b) = tokio::join!(task_a, task_b);
    let signal_a = signal_a.unwrap().unwrap();
    let signal_b = signal_b.unwrap().unwrap();
    assert_ne!(signal_a.signal_id, signal_b.signal_id);

    let reloaded = PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
        connection_string: connection_string.clone(),
        schema: Some(schema.clone()),
        physical_partitions: 2,
        snapshot_interval: Some(100),
    })
    .await
    .unwrap();
    let signals = reloaded.list_signals().await.unwrap();
    let reloaded_a = signals
        .iter()
        .find(|signal| signal.idempotency_key.as_deref() == Some("cross-shard-a"))
        .unwrap();
    let reloaded_b = signals
        .iter()
        .find(|signal| signal.idempotency_key.as_deref() == Some("cross-shard-b"))
        .unwrap();
    assert_eq!(reloaded_a.signal_id, signal_a.signal_id);
    assert_eq!(reloaded_b.signal_id, signal_b.signal_id);

    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let journal_p00 = format!("\"{schema}\".\"shard_journal_p00\"");
    let journal_p01 = format!("\"{schema}\".\"shard_journal_p01\"");
    let rows = client
        .query(
            &format!(
                "SELECT global_entry_id FROM {journal_p00}
                 UNION ALL
                 SELECT global_entry_id FROM {journal_p01}
                 ORDER BY global_entry_id"
            ),
            &[],
        )
        .await
        .unwrap();
    let global_entry_ids = rows
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    assert_eq!(global_entry_ids.len(), 4);
    assert!(global_entry_ids
        .windows(2)
        .all(|window| window[0] < window[1]));

    client
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_shard_file_default_child_ids_stay_on_parent_shard() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new();
    let provider = SqliteShardFileDurabilityProvider::new(dir.path(), 4).unwrap();
    let mut options = RuntimeOptions::default();
    options.shard_count = 4;
    options.dispatch_shard_ids = vec![0, 1, 2, 3];
    let runtime = DurableRuntime::with_options(provider.clone(), options, {
        let clock = clock.clone();
        move || clock.now()
    });
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("sqlite-sharded-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    runtime.signal(&ref_, "begin", Empty {}).await.unwrap();
    runtime
        .drain(DrainOptions {
            max_activations: Some(5),
        })
        .await
        .unwrap();
    let children = runtime_children(&runtime).await;
    assert_eq!(children.len(), 1);
    assert_eq!(
        durable::workflow_partition_shard(&children[0].workflow_id, &children[0].run_id, 4),
        durable::workflow_partition_shard(&ref_.workflow_id, &ref_.run_id, 4),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn commit_local_child_start_materializes_only_at_checkpoint() {
    let _guard = TEST_LOCK.lock().unwrap();
    COMMIT_LOCAL_CHILD_SHOULD_THROW.store(true, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let clock = ManualClock::new();
    let runtime = make_runtime(&path, &clock);
    runtime.register::<CommitLocalChildWorkflow>().unwrap();
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<CommitLocalChildWorkflow>(
            CommitLocalChildInput {},
            StartOptions {
                workflow_id: Some("commit-local-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let error = runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "boom after child start");
    assert!(runtime_children(&runtime).await.is_empty());
    assert_eq!(load_runtime_instance(&runtime, &ref_).await.sequence, 0);

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let parent = load_runtime_instance(&runtime, &ref_).await;
    assert_eq!(parent.status, PersistedStatus::Completed);
    let children = runtime_children(&runtime).await;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].key, "child");
    assert_eq!(children[0].status, durable::ChildStatus::Started);
}

#[tokio::test(flavor = "current_thread")]
async fn run_worker_stop_when_idle_uses_same_activation_path_as_drain() {
    let clock = ManualClock::new();
    let runtime = DurableRuntime::with_clock(durable::NullDurabilityProvider::new(), {
        let clock = clock.clone();
        move || clock.now()
    });
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("run-worker-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();

    let result = runtime
        .run_worker(RunWorkerOptions {
            max_activations: Some(1),
            stop_when_idle: true,
            ..RunWorkerOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(result.activations, 1);
    assert_eq!(
        load_runtime_instance(&runtime, &ref_)
            .await
            .phase
            .unwrap()
            .name,
        "waiting"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn run_worker_honors_cancellation_before_claiming_work() {
    let clock = ManualClock::new();
    let runtime = DurableRuntime::with_clock(durable::NullDurabilityProvider::new(), {
        let clock = clock.clone();
        move || clock.now()
    });
    runtime.register::<TestChildWorkflow>().unwrap();

    let ref_ = runtime
        .start::<TestWorkflow>(
            TestInput {
                label: "Ada".to_string(),
                items: vec!["a".to_string()],
            },
            StartOptions {
                workflow_id: Some("cancelled-worker-parent".to_string()),
                ..StartOptions::default()
            },
        )
        .await
        .unwrap();
    let cancellation = WorkerCancellation::new();
    cancellation.cancel();

    let result = runtime
        .run_worker(RunWorkerOptions {
            max_activations: Some(10),
            cancellation: Some(cancellation),
            ..RunWorkerOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(result.activations, 0);
    assert_eq!(load_runtime_instance(&runtime, &ref_).await.sequence, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn provider_parent_close_applies_cancel_and_abandon_child_policies() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

    let cancel_provider = durable::NullDurabilityProvider::new();
    let cancel_claim = reserve_run_activation_for(&cancel_provider, "parent-cancel", now).await;
    let cancel = cancel_provider
        .commit_checkpoint(durable::CommitCheckpointInput {
            workflow_id: "parent-cancel".to_string(),
            run_id: "run-1".to_string(),
            expected_sequence: cancel_claim.activation.sequence(),
            activation_id: cancel_claim.activation.activation_id().to_string(),
            workflow_version: 1,
            next: InstanceStatusValue::Canceled {
                reason: "parent done".to_string(),
            },
            waits: Vec::new(),
            now,
            consume_signal_id: None,
            consume_child_record_id: None,
            effects: Vec::new(),
            child_starts: vec![checkpoint_child_start(
                "parent-cancel",
                "child-cancel",
                ParentClosePolicy::Cancel,
                now,
            )],
        })
        .await
        .unwrap();
    assert!(cancel.ok);
    let cancel_children = cancel_provider.list_children().await.unwrap();
    assert_eq!(cancel_children.len(), 1);
    assert_eq!(cancel_children[0].status, durable::ChildStatus::Failed);
    assert_eq!(cancel_children[0].delivered_by_sequence, Some(1));
    assert_eq!(
        cancel_children[0].error.as_ref().unwrap().name.as_deref(),
        Some("ParentClosed")
    );
    let canceled_child = cancel_provider
        .load_instance(
            &InstanceRef::new("child-cancel".to_string(), "run-1".to_string()),
            LoadInstanceOptions {
                include_effects: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(canceled_child.status, PersistedStatus::Canceled);

    let abandon_provider = durable::NullDurabilityProvider::new();
    let abandon_claim = reserve_run_activation_for(&abandon_provider, "parent-abandon", now).await;
    let abandon = abandon_provider
        .commit_checkpoint(durable::CommitCheckpointInput {
            workflow_id: "parent-abandon".to_string(),
            run_id: "run-1".to_string(),
            expected_sequence: abandon_claim.activation.sequence(),
            activation_id: abandon_claim.activation.activation_id().to_string(),
            workflow_version: 1,
            next: InstanceStatusValue::Canceled {
                reason: "parent done".to_string(),
            },
            waits: Vec::new(),
            now,
            consume_signal_id: None,
            consume_child_record_id: None,
            effects: Vec::new(),
            child_starts: vec![checkpoint_child_start(
                "parent-abandon",
                "child-abandon",
                ParentClosePolicy::Abandon,
                now,
            )],
        })
        .await
        .unwrap();
    assert!(abandon.ok);
    let abandon_children = abandon_provider.list_children().await.unwrap();
    assert_eq!(abandon_children.len(), 1);
    assert_eq!(abandon_children[0].status, durable::ChildStatus::Abandoned);
    assert_eq!(abandon_children[0].delivered_by_sequence, Some(1));
    let abandoned_child = abandon_provider
        .load_instance(
            &InstanceRef::new("child-abandon".to_string(), "run-1".to_string()),
            LoadInstanceOptions {
                include_effects: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(abandoned_child.status, PersistedStatus::Running);
}

#[tokio::test(flavor = "current_thread")]
async fn shard_actors_claim_independent_shards_without_global_execution_store() {
    let provider = NullDurabilityProvider::new();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for (workflow_id, shard_id) in [("actor-shard-0", 0), ("actor-shard-1", 1)] {
        provider
            .create_instance(CreateInstanceInput {
                workflow_name: "actor_claim".to_string(),
                workflow_version: 1,
                workflow_id: workflow_id.to_string(),
                run_id: "run-1".to_string(),
                partition_shard: shard_id,
                common: serde_json::json!({}),
                phase: PhaseSnapshot {
                    name: "running".to_string(),
                    data: serde_json::json!({}),
                },
                waits: vec![DurableWait::Run {
                    name: "__run".to_string(),
                    ready_at: now,
                }],
                now,
                parent: None,
                conflict_policy: Some(ConflictPolicy::Fail),
                workflow_id_reuse_policy: None,
            })
            .await
            .unwrap();
    }

    let lease_0 = provider
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "worker-0".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let lease_1 = provider
        .claim_shard(ClaimShardInput {
            shard_id: 1,
            owner_id: "worker-1".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();

    let session_0 = provider.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some(lease_0.owner_id),
        lease_epoch: Some(lease_0.lease_epoch),
    });
    let session_1 = provider.open_shard(OpenShardInput {
        shard_id: 1,
        owner_id: Some(lease_1.owner_id),
        lease_epoch: Some(lease_1.lease_epoch),
    });
    let workflows = HashMap::from([("actor_claim".to_string(), 1)]);
    let claims_0 = session_0
        .claim_tasks(ClaimShardTasksInput {
            workflows: workflows.clone(),
            shard_count: 2,
            now,
            lease_ms: 60_000,
            limit: 10,
        })
        .await
        .unwrap();
    let claims_1 = session_1
        .claim_tasks(ClaimShardTasksInput {
            workflows,
            shard_count: 2,
            now,
            lease_ms: 60_000,
            limit: 10,
        })
        .await
        .unwrap();

    assert_eq!(claims_0.claims.len(), 1);
    assert_eq!(claims_0.claims[0].instance.workflow_id, "actor-shard-0");
    assert_eq!(claims_1.claims.len(), 1);
    assert_eq!(claims_1.claims[0].instance.workflow_id, "actor-shard-1");
}

#[tokio::test(flavor = "current_thread")]
async fn shard_directory_routes_signals_to_custom_partition_shard() {
    let provider = NullDurabilityProvider::new();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let ref_ = provider
        .create_instance(CreateInstanceInput {
            workflow_name: "actor_signal".to_string(),
            workflow_version: 1,
            workflow_id: "custom-shard-signal".to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 3,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "waiting".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![DurableWait::Signal {
                name: "continue".to_string(),
                r#type: "continue".to_string(),
                scope: durable::WaitScope::Phase,
                delivery: durable::SignalDelivery::Mailbox,
                after_signal_sequence: None,
            }],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    provider
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "continue".to_string(),
            payload: serde_json::json!({ "ok": true }),
            received_at: now,
            idempotency_key: None,
        })
        .await
        .unwrap();

    let wrong_lease = provider
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "wrong-worker".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let right_lease = provider
        .claim_shard(ClaimShardInput {
            shard_id: 3,
            owner_id: "right-worker".to_string(),
            now,
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .unwrap();
    let wrong_session = provider.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some(wrong_lease.owner_id),
        lease_epoch: Some(wrong_lease.lease_epoch),
    });
    let right_session = provider.open_shard(OpenShardInput {
        shard_id: 3,
        owner_id: Some(right_lease.owner_id),
        lease_epoch: Some(right_lease.lease_epoch),
    });
    let workflows = HashMap::from([("actor_signal".to_string(), 1)]);

    let wrong_claims = wrong_session
        .claim_tasks(ClaimShardTasksInput {
            workflows: workflows.clone(),
            shard_count: 4,
            now,
            lease_ms: 60_000,
            limit: 10,
        })
        .await
        .unwrap();
    let right_claims = right_session
        .claim_tasks(ClaimShardTasksInput {
            workflows,
            shard_count: 4,
            now,
            lease_ms: 60_000,
            limit: 10,
        })
        .await
        .unwrap();

    assert!(wrong_claims.claims.is_empty());
    assert_eq!(right_claims.claims.len(), 1);
    assert_eq!(
        right_claims.claims[0].instance.workflow_id,
        ref_.workflow_id
    );
}

#[tokio::test(flavor = "current_thread")]
async fn null_provider_passes_basic_conformance() {
    durable::testing::conformance::assert_basic_provider_conformance(|| async {
        Ok::<_, durable::WorkflowError>(durable::NullDurabilityProvider::new())
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_provider_passes_basic_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("conformance.sqlite");
    durable::testing::conformance::assert_basic_provider_conformance(|| {
        let path = path.clone();
        async move { SqliteDurabilityProvider::new(path) }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_shard_file_provider_passes_basic_conformance() {
    let dir = tempfile::tempdir().unwrap();
    durable::testing::conformance::assert_basic_provider_conformance(|| {
        let path = dir.path().to_path_buf();
        async move { SqliteShardFileDurabilityProvider::new(path, 1) }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_eager_effect_retry_blocks_and_reclaims_activation() {
    let provider = durable::NullDurabilityProvider::new();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let claim = reserve_run_activation(&provider, now).await;
    let EffectReservation::Reserved {
        effect_id,
        attempt_id,
        attempt,
        ..
    } = provider
        .get_or_reserve_effect(ReserveEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: claim.activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            key: "activity".to_string(),
            now,
            options: ActivityOptions {
                durability: ActivityDurability::Eager,
                max_attempts: Some(2),
                initial_interval_ms: Some(250),
                ..ActivityOptions::default()
            },
            max_attempts: None,
        })
        .await
        .unwrap()
    else {
        panic!("expected reservation");
    };
    assert_eq!(attempt, 1);

    let failed = provider
        .fail_effect(FailEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: claim.activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            effect_id: effect_id.clone(),
            attempt_id,
            error: SerializedError {
                name: Some("Transient".to_string()),
                message: "try again".to_string(),
            },
            now,
            retryable: Some(true),
        })
        .await
        .unwrap();
    let FailEffectResult::RetryScheduled {
        next_attempt_at,
        next_attempt,
    } = failed
    else {
        panic!("expected retry");
    };
    assert_eq!(next_attempt, 2);
    assert_eq!(next_attempt_at, now + Duration::milliseconds(250));

    let session = provider.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some("worker-a".to_string()),
        lease_epoch: Some(1),
    });
    let early = session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("effect".to_string(), 1)]),
            shard_count: 1,
            now: now + Duration::milliseconds(100),
            lease_ms: 30_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert!(early.claims.is_empty());

    let ready = session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("effect".to_string(), 1)]),
            shard_count: 1,
            now: next_attempt_at,
            lease_ms: 30_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(ready.claims.len(), 1);
    let EffectReservation::Reserved {
        attempt,
        heartbeat_details,
        ..
    } = session
        .get_or_reserve_effect(ReserveEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: ready.claims[0].activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            key: "activity".to_string(),
            now: next_attempt_at,
            options: ActivityOptions {
                durability: ActivityDurability::Eager,
                max_attempts: Some(2),
                initial_interval_ms: Some(250),
                ..ActivityOptions::default()
            },
            max_attempts: None,
        })
        .await
        .unwrap()
    else {
        panic!("expected second reservation");
    };
    assert_eq!(attempt, 2);
    assert!(heartbeat_details.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn provider_heartbeat_timeout_releases_attempt_and_preserves_details() {
    let provider = durable::NullDurabilityProvider::new();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let claim = reserve_run_activation(&provider, now).await;
    let EffectReservation::Reserved {
        effect_id,
        attempt_id,
        ..
    } = provider
        .get_or_reserve_effect(ReserveEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: claim.activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            key: "heartbeat".to_string(),
            now,
            options: ActivityOptions {
                durability: ActivityDurability::Eager,
                heartbeat_timeout_ms: Some(100),
                max_attempts: Some(2),
                initial_interval_ms: Some(50),
                ..ActivityOptions::default()
            },
            max_attempts: None,
        })
        .await
        .unwrap()
    else {
        panic!("expected reservation");
    };
    provider
        .heartbeat_effect(HeartbeatEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: claim.activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            effect_id,
            attempt_id,
            now: now + Duration::milliseconds(20),
            details: Some(serde_json::json!({ "step": 1 })),
        })
        .await
        .unwrap();
    let session = provider.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some("worker-a".to_string()),
        lease_epoch: Some(1),
    });
    let timed_out = session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("effect".to_string(), 1)]),
            shard_count: 1,
            now: now + Duration::milliseconds(171),
            lease_ms: 30_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert!(timed_out.claims.is_empty());
    let retry_at = now + Duration::milliseconds(221);
    let ready = session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("effect".to_string(), 1)]),
            shard_count: 1,
            now: retry_at,
            lease_ms: 30_000,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(ready.claims.len(), 1);
    let EffectReservation::Reserved {
        attempt,
        heartbeat_details,
        ..
    } = session
        .get_or_reserve_effect(ReserveEffectInput {
            workflow_id: "effect-workflow".to_string(),
            run_id: "run-1".to_string(),
            activation_id: ready.claims[0].activation.activation_id().to_string(),
            worker_id: "worker-a".to_string(),
            key: "heartbeat".to_string(),
            now: retry_at,
            options: ActivityOptions {
                durability: ActivityDurability::Eager,
                heartbeat_timeout_ms: Some(100),
                max_attempts: Some(2),
                initial_interval_ms: Some(50),
                ..ActivityOptions::default()
            },
            max_attempts: None,
        })
        .await
        .unwrap()
    else {
        panic!("expected retry reservation");
    };
    assert_eq!(attempt, 2);
    assert_eq!(heartbeat_details.unwrap()["step"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_provider_passes_basic_conformance_when_configured() {
    let Ok(connection_string) = std::env::var("DURABLE_POSTGRES_URL") else {
        return;
    };
    let schema = format!(
        "durable_rust_test_{}_{}",
        std::process::id(),
        Utc::now().timestamp_millis().abs()
    );
    durable::testing::conformance::assert_basic_provider_conformance(|| {
        let connection_string = connection_string.clone();
        let schema = schema.clone();
        async move {
            PostgresDurabilityProvider::create(PostgresDurabilityProviderOptions {
                connection_string,
                schema: Some(schema),
                physical_partitions: 2,
                snapshot_interval: Some(2),
            })
            .await
        }
    })
    .await;
}

async fn reserve_run_activation<P>(
    provider: &P,
    now: DateTime<Utc>,
) -> durable::ClaimedActivationWithInstance
where
    P: DurabilityProvider,
{
    reserve_run_activation_for(provider, "effect-workflow", now).await
}

async fn reserve_run_activation_for<P>(
    provider: &P,
    workflow_id: &str,
    now: DateTime<Utc>,
) -> durable::ClaimedActivationWithInstance
where
    P: DurabilityProvider,
{
    provider
        .create_instance(CreateInstanceInput {
            workflow_name: "effect".to_string(),
            workflow_version: 1,
            workflow_id: workflow_id.to_string(),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            common: serde_json::json!({}),
            phase: PhaseSnapshot {
                name: "run".to_string(),
                data: serde_json::json!({}),
            },
            waits: vec![DurableWait::Run {
                name: "__run".to_string(),
                ready_at: now,
            }],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
            workflow_id_reuse_policy: None,
        })
        .await
        .unwrap();
    let lease = provider
        .claim_shard(ClaimShardInput {
            shard_id: 0,
            owner_id: "worker-a".to_string(),
            now,
            lease_ms: 30_000,
        })
        .await
        .unwrap()
        .unwrap();
    let session = provider.open_shard(OpenShardInput {
        shard_id: 0,
        owner_id: Some("worker-a".to_string()),
        lease_epoch: Some(lease.lease_epoch),
    });
    session
        .claim_tasks(ClaimShardTasksInput {
            workflows: HashMap::from([("effect".to_string(), 1)]),
            shard_count: 1,
            now,
            lease_ms: 30_000,
            limit: 1,
        })
        .await
        .unwrap()
        .claims
        .into_iter()
        .next()
        .unwrap()
}

fn checkpoint_child_start(
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_close_policy: ParentClosePolicy,
    now: DateTime<Utc>,
) -> CheckpointChildStart {
    CheckpointChildStart {
        key: "child".to_string(),
        workflow_name: "effect_child".to_string(),
        workflow_version: 1,
        workflow_id: child_workflow_id.to_string(),
        run_id: "run-1".to_string(),
        partition_shard: 0,
        common: serde_json::json!({ "parent": parent_workflow_id }),
        phase: PhaseSnapshot {
            name: "run".to_string(),
            data: serde_json::json!({ "created_at": now }),
        },
        waits: vec![DurableWait::Run {
            name: "__run".to_string(),
            ready_at: now,
        }],
        parent_close_policy,
        conflict_policy: ConflictPolicy::Fail,
        workflow_id_reuse_policy: None,
        created: true,
    }
}

fn sqlite_columns(connection: &rusqlite::Connection, table: &str) -> Vec<String> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}
