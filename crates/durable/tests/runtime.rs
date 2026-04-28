mod examples;

use chrono::{DateTime, Duration, TimeZone, Utc};
use durable::{
    DrainOptions, DurableRuntime, DurableWait, JsonFileDurabilityProvider, PersistedStatus,
    StartOptions,
};
use examples::migration::{FinishEvent, MigratingOrderV1, MigratingOrderV2, MigrationInput};
use examples::parent_child::{self, Empty, Progress, TestChildWorkflow, TestInput, TestWorkflow};
use examples::unstable::{self, UnstableInput, UnstableWorkflow};
use std::sync::{Arc, Mutex};

static TEST_LOCK: Mutex<()> = Mutex::new(());

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

fn provider(path: &std::path::Path) -> JsonFileDurabilityProvider {
    JsonFileDurabilityProvider::new(path).unwrap()
}

fn make_runtime(path: &std::path::Path, clock: &ManualClock) -> DurableRuntime {
    DurableRuntime::with_clock(provider(path), {
        let clock = clock.clone();
        move || clock.now()
    })
}

#[tokio::test(flavor = "current_thread")]
async fn persists_initial_snapshot_and_reloads() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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

    let persisted = runtime.provider().load_instance(&ref_).unwrap().unwrap();
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

    let reloaded = provider(&path).load_instance(&ref_).unwrap().unwrap();
    assert_eq!(reloaded.sequence, 0);
    assert_eq!(reloaded.phase.unwrap().name, "boot");
}

#[tokio::test(flavor = "current_thread")]
async fn survives_restart_with_pending_timer_and_stay_checkpoint() {
    let _guard = TEST_LOCK.lock().unwrap();
    parent_child::reset_reminders();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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
        runtime
            .provider()
            .load_instance(&ref_)
            .unwrap()
            .unwrap()
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

    let persisted = restarted.provider().load_instance(&ref_).unwrap().unwrap();
    assert_eq!(persisted.sequence, 2);
    assert_eq!(persisted.phase.as_ref().unwrap().name, "waiting");
    assert_eq!(persisted.phase.unwrap().data["reminders"], 1);
    assert_eq!(parent_child::reminders(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn persists_signals_and_consumes_atomically_with_go_checkpoint() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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
        runtime.provider().list_signals().unwrap()[0].consumed_by_sequence,
        None
    );

    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let persisted = runtime.provider().load_instance(&ref_).unwrap().unwrap();
    assert_eq!(persisted.sequence, 2);
    assert_eq!(persisted.phase.unwrap().name, "processing");
    assert_eq!(
        runtime.provider().list_signals().unwrap()[0].consumed_by_sequence,
        Some(2)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn uses_checkpoint_for_bounded_loop_pattern() {
    let _guard = TEST_LOCK.lock().unwrap();
    parent_child::reset_processed();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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

    let persisted = runtime.provider().load_instance(&ref_).unwrap().unwrap();
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
    let path = dir.path().join("store.json");
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
        runtime
            .provider()
            .load_instance(&ref_)
            .unwrap()
            .unwrap()
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

    let parent = restarted.provider().load_instance(&ref_).unwrap().unwrap();
    let children = restarted.provider().list_children().unwrap();
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
        restarted
            .provider()
            .load_instance(&ref_)
            .unwrap()
            .unwrap()
            .sequence,
        before_query
    );

    clock.advance(Duration::milliseconds(1_000));
    restarted
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();
    let completed = restarted.provider().load_instance(&ref_).unwrap().unwrap();
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.output.unwrap()["child_value"], "child:a!,b!,c!");
}

#[tokio::test(flavor = "current_thread")]
async fn memoizes_completed_activities_across_failed_activation_retry() {
    let _guard = TEST_LOCK.lock().unwrap();
    unstable::reset(true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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
    assert_eq!(
        runtime
            .provider()
            .load_instance(&ref_)
            .unwrap()
            .unwrap()
            .sequence,
        0
    );

    let restarted = make_runtime(&path, &clock);
    restarted.register::<UnstableWorkflow>().unwrap();
    restarted
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await
        .unwrap();

    assert_eq!(unstable::activity_calls(), 1);
    let completed = restarted.provider().load_instance(&ref_).unwrap().unwrap();
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.sequence, 1);
    assert_eq!(completed.output.unwrap()["ok"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn migrates_running_instance_and_recomputes_waits() {
    let _guard = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.json");
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

    let before = runtime_v1.provider().load_instance(&ref_).unwrap().unwrap();
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

    let migrated = runtime_v2.provider().load_instance(&ref_).unwrap().unwrap();
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
    let completed = runtime_v2.provider().load_instance(&ref_).unwrap().unwrap();
    assert_eq!(completed.status, PersistedStatus::Completed);
    assert_eq!(completed.sequence, 2);
    assert_eq!(
        completed.output.unwrap()["message"],
        "hello, Ada on starter!"
    );
}
