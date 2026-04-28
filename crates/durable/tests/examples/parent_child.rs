/*
 * Shows the core durable workflow patterns from the TypeScript demos in one
 * compact Rust workflow:
 * immediate run-phase handoffs around a durable signal, durable timer waits
 * with `stay!()`, the bounded unbound-loop pattern with `checkpoint!()`, and
 * a local child workflow wait that survives runtime reconstruction.
 */

use chrono::{DateTime, Duration, Utc};
use durable::{
    checkpoint, complete, go, start, stay, workflow, ChildEvent, ChildHandle, ChildOptions,
    InstanceSnapshot,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

static REMINDERS: AtomicUsize = AtomicUsize::new(0);
static PROCESSED: AtomicUsize = AtomicUsize::new(0);

pub fn reset_reminders() {
    REMINDERS.store(0, Ordering::SeqCst);
}

pub fn reminders() -> usize {
    REMINDERS.load(Ordering::SeqCst)
}

pub fn reset_processed() {
    PROCESSED.store(0, Ordering::SeqCst);
}

pub fn processed() -> usize {
    PROCESSED.load(Ordering::SeqCst)
}

fn add_ms(time: DateTime<Utc>, ms: i64) -> DateTime<Utc> {
    time + Duration::milliseconds(ms)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Empty {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestChildInput {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestChildOutput {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestChildCommon {
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestChildDone {}

workflow! {
    pub workflow TestChildWorkflow {
        name: "test_child",
        version: 1,
        input: TestChildInput,
        output: TestChildOutput,
        common: TestChildCommon,

        initial(input) {
            start! {
                common: TestChildCommon { value: input.value },
                phase: child_done(TestChildDone {}),
            }
        }

        phase child_done(data: TestChildDone) {
            run async |common| {
                complete!(TestChildOutput {
                    value: format!("child:{}", common.value),
                })
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestInput {
    pub label: String,
    pub items: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestOutput {
    pub processed: Vec<String>,
    pub child_value: String,
    pub reminders: usize,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestCommon {
    label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Boot {
    items: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Waiting {
    items: Vec<String>,
    reminders: usize,
    wake_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Processing {
    items: Vec<String>,
    cursor: usize,
    processed: Vec<String>,
    reminders: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitingChild {
    child_handle: ChildHandle<TestChildWorkflow>,
    processed: Vec<String>,
    reminders: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cooldown {
    child_value: String,
    processed: Vec<String>,
    reminders: usize,
    finish_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Progress {
    pub sequence: u64,
    pub status: String,
    pub phase: Option<String>,
    pub cursor: Option<usize>,
    pub reminders: Option<usize>,
}

workflow! {
    pub workflow TestWorkflow {
        name: "test_parent",
        version: 1,
        input: TestInput,
        output: TestOutput,
        common: TestCommon,

        initial(input) {
            start! {
                common: TestCommon { label: input.label },
                phase: boot(Boot { items: input.items }),
            }
        }

        queries {
            progress: query<Progress> |snapshot, sequence| {
                match snapshot {
                    InstanceSnapshot::Running { phase, .. } => match phase {
                        TestWorkflowPhase::Boot(_) => Progress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("boot".to_string()),
                            cursor: None,
                            reminders: None,
                        },
                        TestWorkflowPhase::Waiting(data) => Progress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting".to_string()),
                            cursor: None,
                            reminders: Some(data.reminders),
                        },
                        TestWorkflowPhase::Processing(data) => Progress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("processing".to_string()),
                            cursor: Some(data.cursor),
                            reminders: Some(data.reminders),
                        },
                        TestWorkflowPhase::WaitingChild(data) => Progress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting_child".to_string()),
                            cursor: None,
                            reminders: Some(data.reminders),
                        },
                        TestWorkflowPhase::Cooldown(data) => Progress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("cooldown".to_string()),
                            cursor: None,
                            reminders: Some(data.reminders),
                        },
                    },
                    snapshot => Progress {
                        sequence,
                        status: snapshot.status().to_string(),
                        phase: None,
                        cursor: None,
                        reminders: None,
                    },
                }
            },
        }

        phase boot(data: Boot) {
            run async |ctx, data| {
                go!(waiting(Waiting {
                    items: data.items,
                    reminders: 0,
                    wake_at: add_ms(ctx.now(), 1_000),
                }))
            }
        }

        phase waiting(data: Waiting) {
            on {
                reminder_due: timer(data.wake_at.clone()) async |ctx, data| {
                    ctx.activity("send_reminder", || async {
                        REMINDERS.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }).await?;

                    stay!(waiting(Waiting {
                        items: data.items,
                        reminders: data.reminders + 1,
                        wake_at: add_ms(ctx.now(), 1_000),
                    }))
                },

                begin: signal<Empty> async |data| {
                    go!(processing(Processing {
                        items: data.items,
                        cursor: 0,
                        processed: Vec::new(),
                        reminders: data.reminders,
                    }))
                },
            }
        }

        phase processing(data: Processing) {
            run async |ctx, data| {
                if data.cursor < data.items.len() {
                    let key = format!("process_{}", data.cursor);
                    let item = data.items[data.cursor].clone();
                    let value: String = ctx.activity(&key, move || async move {
                        PROCESSED.fetch_add(1, Ordering::SeqCst);
                        Ok(format!("{item}!"))
                    }).await?;

                    return checkpoint!(processing(Processing {
                        items: data.items,
                        cursor: data.cursor + 1,
                        processed: {
                            let mut processed = data.processed;
                            processed.push(value);
                            processed
                        },
                        reminders: data.reminders,
                    }));
                }

                let child_handle = ctx.child_start::<TestChildWorkflow>(
                    "child",
                    TestChildInput {
                        value: data.processed.join(","),
                    },
                    ChildOptions::default(),
                ).await?;

                go!(waiting_child(WaitingChild {
                    child_handle,
                    processed: data.processed,
                    reminders: data.reminders,
                }))
            }
        }

        phase waiting_child(data: WaitingChild) {
            on {
                child_done: child(data.child_handle.clone()) async |ctx, data, event| {
                    match event {
                        ChildEvent::Ok { output } => {
                            go!(cooldown(Cooldown {
                                child_value: output.value,
                                processed: data.processed,
                                reminders: data.reminders,
                                finish_at: add_ms(ctx.now(), 1_000),
                            }))
                        }
                        ChildEvent::Err { .. } => {
                            go!(cooldown(Cooldown {
                                child_value: "child failed".to_string(),
                                processed: data.processed,
                                reminders: data.reminders,
                                finish_at: add_ms(ctx.now(), 1_000),
                            }))
                        }
                    }
                },
            }
        }

        phase cooldown(data: Cooldown) {
            on {
                finish_due: timer(data.finish_at.clone()) async |ctx, data| {
                    complete!(TestOutput {
                        child_value: data.child_value,
                        processed: data.processed,
                        reminders: data.reminders,
                        finished_at: ctx.now(),
                    })
                },
            }
        }
    }
}
