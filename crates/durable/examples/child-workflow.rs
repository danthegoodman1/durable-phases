/*
 * Shows a local child workflow wait across runtime restart:
 * the parent starts a child from an immediate phase, persists a child wait,
 * and after reconstruction the child completion wakes and completes the parent.
 */

use chrono::{DateTime, TimeZone, Utc};
use durable::{
    complete, go, start, workflow, ChildEvent, ChildHandle, ChildOptions, DrainOptions,
    DurableRuntime, InstanceRef, PersistedInstance, PersistedStatus, SqliteDurabilityProvider,
    StartOptions, WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GreetingInput {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GreetingOutput {
    greeting: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GreetingCommon {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Compose {}

workflow! {
    workflow GreetingChildWorkflow {
        name: "demo_greeting_child",
        version: 1,
        input: GreetingInput,
        output: GreetingOutput,
        common: GreetingCommon,

        initial(input) {
            start! {
                common: GreetingCommon { name: input.name },
                phase: compose(Compose {}),
            }
        }

        phase compose(data: Compose) {
            run async |ctx, common| {
                let name = common.name.clone();
                let greeting = ctx.activity("compose_greeting", move || async move {
                    Ok(GreetingOutput {
                        greeting: format!("Hello, {name}!"),
                    })
                }).await?;

                complete!(greeting)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ParentOutput {
    greeting: String,
    completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StartChild {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaitingForChild {
    greeting: ChildHandle<GreetingChildWorkflow>,
}

workflow! {
    workflow GreetingParentWorkflow {
        name: "demo_greeting_parent",
        version: 1,
        input: GreetingInput,
        output: ParentOutput,
        common: GreetingCommon,

        initial(input) {
            start! {
                common: GreetingCommon { name: input.name },
                phase: start_child(StartChild {}),
            }
        }

        phase start_child(data: StartChild) {
            run async |ctx, common| {
                let greeting = ctx.child_start::<GreetingChildWorkflow>(
                    "greeting",
                    GreetingInput {
                        name: common.name,
                    },
                    ChildOptions::default(),
                ).await?;

                go!(waiting_for_child(WaitingForChild { greeting }))
            }
        }

        phase waiting_for_child(data: WaitingForChild) {
            on {
                greeting_finished: child(data.greeting.clone()) async |ctx, event| {
                    match event {
                        ChildEvent::Ok { output } => {
                            complete!(ParentOutput {
                                greeting: output.greeting,
                                completed_at: ctx.now(),
                            })
                        }
                        ChildEvent::Err { .. } => {
                            complete!(ParentOutput {
                                greeting: "child failed".to_string(),
                                completed_at: ctx.now(),
                            })
                        }
                    }
                },
            }
        }
    }
}

pub async fn run_child_workflow_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("child-workflow")?;
    let clock = ManualClock::new();
    let provider = SqliteDurabilityProvider::new(&path)?;
    let runtime = DurableRuntime::with_clock(provider.clone(), clock.closure());
    runtime.register::<GreetingChildWorkflow>()?;

    let ref_ = runtime
        .start::<GreetingParentWorkflow>(
            GreetingInput {
                name: "Ada".to_string(),
            },
            StartOptions {
                workflow_id: Some("child-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;
    runtime
        .drain(DrainOptions {
            max_activations: Some(1),
        })
        .await?;
    print_committed("child workflow: parent waiting", &provider, &ref_).await?;

    let restarted_provider = SqliteDurabilityProvider::new(&path)?;
    let restarted = DurableRuntime::with_clock(restarted_provider.clone(), clock.closure());
    restarted.register::<GreetingParentWorkflow>()?;
    restarted.register::<GreetingChildWorkflow>()?;
    restarted.drain(DrainOptions::default()).await?;
    print_committed(
        "child workflow: completed after restart",
        &restarted_provider,
        &ref_,
    )
    .await
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_child_workflow_demo().await
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
