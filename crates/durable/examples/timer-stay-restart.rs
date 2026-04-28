/*
 * Shows a durable timer wait and `stay!()` checkpoint across runtime restart:
 * the workflow persists a pending timer, the runtime/provider are recreated,
 * the clock advances, and the timer handler patches phase data with `stay!()`.
 */

use chrono::{DateTime, Duration, TimeZone, Utc};
use durable::{
    complete, start, stay, workflow, DrainOptions, DurableRuntime, InstanceSnapshot, StartOptions,
    WorkflowError,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReminderInput {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReminderOutput {
    reminders_sent: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReminderCommon {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Waiting {
    reminders_sent: usize,
    next_reminder_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Empty {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgressQuery {
    sequence: u64,
    status: String,
    reminders_sent: Option<usize>,
}

workflow! {
    workflow ReminderWorkflow {
        name: "demo_timer_stay",
        version: 1,
        input: ReminderInput,
        output: ReminderOutput,
        common: ReminderCommon,

        initial(input) {
            start! {
                common: ReminderCommon { name: input.name },
                phase: waiting(Waiting {
                    reminders_sent: 0,
                    next_reminder_at: Utc
                        .with_ymd_and_hms(2026, 1, 1, 0, 0, 1)
                        .unwrap(),
                }),
            }
        }

        queries {
            progress: query<ProgressQuery> |snapshot, sequence| {
                match snapshot {
                    InstanceSnapshot::Running { phase: ReminderWorkflowPhase::Waiting(data), .. } => {
                        ProgressQuery {
                            sequence,
                            status: "running".to_string(),
                            reminders_sent: Some(data.reminders_sent),
                        }
                    }
                    snapshot => ProgressQuery {
                        sequence,
                        status: snapshot.status().to_string(),
                        reminders_sent: None,
                    },
                }
            },
        }

        phase waiting(data: Waiting) {
            on {
                reminder_due: timer(data.next_reminder_at.clone()) async |ctx, common, data| {
                    let key = format!("send_reminder_{}", data.reminders_sent + 1);
                    let name = common.name.clone();
                    ctx.activity(&key, move || async move {
                        Ok(serde_json::json!({ "name": name }))
                    }).await?;

                    stay!(waiting(Waiting {
                        reminders_sent: data.reminders_sent + 1,
                        next_reminder_at: ctx.now() + Duration::milliseconds(1_000),
                    }))
                },

                done: signal<Empty> async |data| {
                    complete!(ReminderOutput {
                        reminders_sent: data.reminders_sent,
                    })
                },
            }
        }
    }
}

pub async fn run_timer_stay_restart_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("timer-stay-restart")?;
    let clock = ManualClock::new();
    let provider = durable::JsonFileDurabilityProvider::new(&path)?;
    let runtime = DurableRuntime::with_clock(provider, clock.closure());

    let ref_ = runtime
        .start::<ReminderWorkflow>(
            ReminderInput {
                name: "Ada".to_string(),
            },
            StartOptions {
                workflow_id: Some("timer-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;

    let progress: ProgressQuery = runtime
        .query::<ReminderWorkflow, ProgressQuery>(&ref_, "progress")
        .await?;
    println!(
        "timer + stay: pending timer {}",
        serde_json::to_string_pretty(&serde_json::to_value(progress)?)?
    );

    clock.advance(1_000);
    let restarted_provider = durable::JsonFileDurabilityProvider::new(&path)?;
    let restarted = DurableRuntime::with_clock(restarted_provider, clock.closure());
    restarted.register::<ReminderWorkflow>()?;
    restarted.drain(DrainOptions::default()).await?;

    let progress: ProgressQuery = restarted
        .query::<ReminderWorkflow, ProgressQuery>(&ref_, "progress")
        .await?;
    println!(
        "timer + stay: after restart and timer {}",
        serde_json::to_string_pretty(&serde_json::to_value(progress)?)?
    );
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_timer_stay_restart_demo().await
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

    fn advance(&self, ms: i64) {
        let mut now = self.now.lock().unwrap();
        *now += Duration::milliseconds(ms);
    }

    fn closure(&self) -> impl Fn() -> DateTime<Utc> + Send + Sync + 'static {
        let clock = self.clone();
        move || clock.now()
    }
}

fn demo_store_path(name: &str) -> PathBuf {
    PathBuf::from(".durable-demo").join(format!("rust-{name}.json"))
}

fn reset_demo_store(name: &str) -> Result<PathBuf, std::io::Error> {
    let path = demo_store_path(name);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(path)
}
