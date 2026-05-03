/*
 * Shows two immediate run-phase handoffs around a durable signal:
 * start enters a run phase immediately, the workflow then waits for an
 * `approved` signal, and that signal transitions into another immediate run
 * phase that records an activity and completes.
 */

use chrono::TimeZone;
use durable::{
    complete, go, start, workflow, DrainOptions, DurablePhase, DurableRuntime, InstanceRef,
    InstanceSnapshot, PersistedInstance, PersistedStatus, SqliteDurabilityProvider, StartOptions,
    WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ImmediateInput {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ImmediateOutput {
    message: String,
    approved_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ImmediateCommon {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootImmediately {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaitingForApproval {
    entered_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AcknowledgeImmediately {
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApprovedSignal {
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StatusQuery {
    sequence: u64,
    status: String,
    phase: Option<String>,
}

workflow! {
    workflow ImmediateApprovalWorkflow {
        name: "demo_immediate_approval",
        version: 1,
        input: ImmediateInput,
        output: ImmediateOutput,
        common: ImmediateCommon,

        initial(input) {
            start! {
                common: ImmediateCommon { name: input.name },
                phase: boot_immediately(BootImmediately {}),
            }
        }

        queries {
            status: query<StatusQuery> |snapshot, sequence| {
                StatusQuery {
                    sequence,
                    status: snapshot.status().to_string(),
                    phase: match snapshot {
                        InstanceSnapshot::Running { phase, .. } => {
                            Some(phase.phase_name().to_string())
                        }
                        _ => None,
                    },
                }
            },
        }

        phase boot_immediately(data: BootImmediately) {
            run async |ctx| {
                go!(waiting_for_approval(WaitingForApproval {
                    entered_at: ctx.now(),
                }))
            }
        }

        phase waiting_for_approval(data: WaitingForApproval) {
            on {
                approved: signal<ApprovedSignal> async |event| {
                    go!(acknowledge_immediately(AcknowledgeImmediately {
                        message: event.message,
                    }))
                },
            }
        }

        phase acknowledge_immediately(data: AcknowledgeImmediately) {
            run async |ctx, common, data| {
                let name = common.name.clone();
                let message = data.message.clone();
                ctx.activity("record_approval", move || async move {
                    Ok(serde_json::json!({
                        "name": name,
                        "message": message,
                    }))
                }).await?;

                complete!(ImmediateOutput {
                    message: format!("{}: {}", common.name, data.message),
                    approved_at: ctx.now(),
                })
            }
        }
    }
}

pub async fn run_immediate_and_signal_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("immediate-and-signal")?;
    let clock = ManualClock::new();
    let provider = SqliteDurabilityProvider::new(path)?;
    let runtime = DurableRuntime::with_clock(provider.clone(), clock.closure());

    let ref_ = runtime
        .start::<ImmediateApprovalWorkflow>(
            ImmediateInput {
                name: "Ada".to_string(),
            },
            StartOptions {
                workflow_id: Some("immediate-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;
    runtime.drain(DrainOptions::default()).await?;

    let status: StatusQuery = runtime
        .query::<ImmediateApprovalWorkflow, StatusQuery>(&ref_, "status")
        .await?;
    println!(
        "immediate + signal: after immediate boot {}",
        serde_json::to_string_pretty(&serde_json::to_value(status)?)?
    );

    runtime
        .signal(
            &ref_,
            "approved",
            ApprovedSignal {
                message: "ship it".to_string(),
            },
        )
        .await?;
    runtime.drain(DrainOptions::default()).await?;
    print_committed("immediate + signal: completed", &provider, &ref_).await
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_immediate_and_signal_demo().await
}

fn demo_store_path(name: &str) -> PathBuf {
    PathBuf::from(".durable-demo").join(format!("rust-{name}.sqlite"))
}

#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(
                chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )),
        }
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self.now.lock().unwrap()
    }

    fn closure(&self) -> impl Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync + 'static {
        let clock = self.clone();
        move || clock.now()
    }
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
