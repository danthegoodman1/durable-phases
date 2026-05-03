mod examples;

use chrono::{DateTime, Duration, TimeZone, Utc};
use durable::{
    complete, start, workflow, ActivityDurability, ActivityOptions, AppendSignalInput,
    CheckpointChildStart, ChildOptions, ChildRecord, ClaimShardInput, ClaimShardTasksInput,
    ConflictPolicy, CreateInstanceInput, DrainOptions, DurabilityProvider, DurableRuntime,
    DurableWait, EffectReservation, FailEffectInput, FailEffectResult, HeartbeatEffectInput,
    InstanceRef, InstanceStatusValue, LoadInstanceOptions, NullDurabilityProvider, OpenShardInput,
    ParentClosePolicy, PersistedInstance, PersistedStatus, PhaseSnapshot,
    PostgresDurabilityProvider, PostgresDurabilityProviderOptions, ReserveEffectInput,
    RunWorkerOptions, RuntimeOptions, SerializedError, SignalRecord, SqliteDurabilityOptions,
    SqliteDurabilityProvider, SqliteShardFileDurabilityProvider, StartOptions, WorkerCancellation,
    WorkflowError,
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

async fn runtime_signals(runtime: &DurableRuntime) -> Vec<SignalRecord> {
    runtime.provider().list_signals().await.unwrap()
}

async fn runtime_children(runtime: &DurableRuntime) -> Vec<ChildRecord> {
    runtime.provider().list_children().await.unwrap()
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
            }],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
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
        })
        .await
        .unwrap();
    provider
        .append_signal(AppendSignalInput {
            workflow_id: ref_.workflow_id.clone(),
            run_id: ref_.run_id.clone(),
            r#type: "continue".to_string(),
            payload: serde_json::json!({ "index": 2 }),
            received_at: now + Duration::seconds(1),
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
            }],
            now,
            parent: None,
            conflict_policy: Some(ConflictPolicy::Fail),
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
