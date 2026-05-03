/*
 * Shows checkpoint-boundary migration:
 * a v1 worker creates a running instance, then a v2 worker loads the same
 * durable store, applies migration 1 -> 2, recomputes waits for the migrated
 * phase, and then handles a signal using the v2 workflow code.
 */

use chrono::{DateTime, TimeZone, Utc};
use durable::{
    complete, start, workflow, DrainOptions, DurableRuntime, InstanceRef, MigrationArgs,
    MigrationResult, PersistedInstance, PersistedStatus, SqliteDurabilityProvider, StartOptions,
    WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationInput {
    customer_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationOutput {
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationCommonV1 {
    customer_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationWaitingV1 {
    salutation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FinishEvent {
    punctuation: String,
}

workflow! {
    workflow MigratingOrderV1 {
        name: "demo_migrating_order",
        version: 1,
        input: MigrationInput,
        output: MigrationOutput,
        common: MigrationCommonV1,

        initial(input) {
            start! {
                common: MigrationCommonV1 { customer_id: input.customer_id },
                phase: waiting(MigrationWaitingV1 { salutation: "hello".to_string() }),
            }
        }

        phase waiting(data: MigrationWaitingV1) {
            on {
                finish: signal<FinishEvent> async |common, data, event| {
                    complete!(MigrationOutput {
                        message: format!("{}, {}{}", data.salutation, common.customer_id, event.punctuation),
                    })
                },
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationCommonV2 {
    customer_id: String,
    plan: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationWaitingV2 {
    greeting: String,
    migrated_from: String,
}

fn migrate_order_v1_to_v2(
    args: MigrationArgs,
) -> MigrationResult<MigrationCommonV2, MigratingOrderV2Phase> {
    let common: MigrationCommonV1 = serde_json::from_value(args.common).unwrap();
    let phase = args.phase;
    let data: MigrationWaitingV1 = serde_json::from_value(phase.data).unwrap();

    MigrationResult {
        common: MigrationCommonV2 {
            customer_id: common.customer_id,
            plan: "starter".to_string(),
        },
        phase: MigratingOrderV2Phase::WaitingForFinish(MigrationWaitingV2 {
            greeting: data.salutation,
            migrated_from: phase.name,
        }),
    }
}

workflow! {
    workflow MigratingOrderV2 {
        name: "demo_migrating_order",
        version: 2,
        input: MigrationInput,
        output: MigrationOutput,
        common: MigrationCommonV2,

        initial(input) {
            start! {
                common: MigrationCommonV2 {
                    customer_id: input.customer_id,
                    plan: "pro".to_string(),
                },
                phase: waiting_for_finish(MigrationWaitingV2 {
                    greeting: "hello".to_string(),
                    migrated_from: "initial".to_string(),
                }),
            }
        }

        migrations {
            1: migrate_order_v1_to_v2,
        }

        phase waiting_for_finish(data: MigrationWaitingV2) {
            on {
                finish: signal<FinishEvent> async |common, data, event| {
                    complete!(MigrationOutput {
                        message: format!("{}, {} on {}{}", data.greeting, common.customer_id, common.plan, event.punctuation),
                    })
                },
            }
        }
    }
}

pub async fn run_migration_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("migration")?;
    let clock = ManualClock::new();
    let provider_v1 = SqliteDurabilityProvider::new(&path)?;
    let runtime_v1 = DurableRuntime::with_clock(provider_v1.clone(), clock.closure());

    let ref_ = runtime_v1
        .start::<MigratingOrderV1>(
            MigrationInput {
                customer_id: "Ada".to_string(),
            },
            StartOptions {
                workflow_id: Some("migration-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;
    print_committed("migration: v1 persisted", &provider_v1, &ref_).await?;

    let upgraded_provider = SqliteDurabilityProvider::new(&path)?;
    let upgraded_runtime = DurableRuntime::with_clock(upgraded_provider.clone(), clock.closure());
    upgraded_runtime.register::<MigratingOrderV2>()?;
    upgraded_runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await?;
    print_committed(
        "migration: after v2 migration checkpoint",
        &upgraded_provider,
        &ref_,
    )
    .await?;

    upgraded_runtime
        .signal(
            &ref_,
            "finish",
            FinishEvent {
                punctuation: "!".to_string(),
            },
        )
        .await?;
    upgraded_runtime.drain(DrainOptions::default()).await?;
    print_committed("migration: completed on v2", &upgraded_provider, &ref_).await
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_migration_demo().await
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

    fn closure(&self) -> impl Fn() -> DateTime<Utc> + Send + Sync + 'static {
        let clock = self.clone();
        move || clock.now()
    }
}

fn demo_store_path(name: &str) -> PathBuf {
    PathBuf::from(".durable-demo").join(format!("rust-{name}.sqlite"))
}

fn reset_demo_store(name: &str) -> Result<PathBuf, std::io::Error> {
    let path = demo_store_path(name);
    for candidate in [
        path.clone(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        match std::fs::remove_file(candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(path)
}

async fn print_committed(
    label: &str,
    provider: &SqliteDurabilityProvider,
    ref_: &InstanceRef,
) -> Result<(), WorkflowError> {
    println!(
        "{label} {}",
        serde_json::to_string_pretty(&summarize(provider.load_instance(ref_).await?))?
    );
    Ok(())
}

fn summarize(instance: Option<PersistedInstance>) -> JsonValue {
    let Some(instance) = instance else {
        return JsonValue::Null;
    };

    match instance.status {
        PersistedStatus::Running => json!({
            "workflowVersion": instance.workflow_version,
            "sequence": instance.sequence,
            "status": "running",
            "phase": instance.phase.as_ref().map(|phase| phase.name.clone()),
            "data": instance.phase.as_ref().map(|phase| phase.data.clone()),
            "waits": instance.waits,
        }),
        PersistedStatus::Completed => json!({
            "workflowVersion": instance.workflow_version,
            "sequence": instance.sequence,
            "status": "completed",
            "output": instance.output,
        }),
        PersistedStatus::Canceled => json!({
            "workflowVersion": instance.workflow_version,
            "sequence": instance.sequence,
            "status": "canceled",
            "reason": instance.cancel_reason,
        }),
        PersistedStatus::Failed => json!({
            "workflowVersion": instance.workflow_version,
            "sequence": instance.sequence,
            "status": "failed",
            "error": instance.error,
        }),
    }
}
