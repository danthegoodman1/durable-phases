/*
 * Shows the bounded unbound-loop pattern with `checkpoint!()`:
 * each immediate activation processes a small chunk, checkpoints progress into
 * phase data, and the runtime re-enters the same run phase until complete.
 */

use durable::{
    checkpoint, complete, start, workflow, DrainOptions, DurableRuntime, InstanceRef,
    JsonFileDurabilityProvider, PersistedInstance, PersistedStatus, StartOptions, WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BatchInput {
    items: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BatchOutput {
    processed: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BatchCommon {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProcessBatch {
    cursor: usize,
    items: Vec<String>,
    processed: Vec<String>,
}

workflow! {
    workflow BatchWorkflow {
        name: "demo_checkpoint_loop",
        version: 1,
        input: BatchInput,
        output: BatchOutput,
        common: BatchCommon,

        initial(input) {
            start! {
                common: BatchCommon {},
                phase: process_batch(ProcessBatch {
                    cursor: 0,
                    items: input.items,
                    processed: Vec::new(),
                }),
            }
        }

        phase process_batch(data: ProcessBatch) {
            run async |ctx, data| {
                let chunk: Vec<String> = data
                    .items
                    .iter()
                    .skip(data.cursor)
                    .take(2)
                    .cloned()
                    .collect();

                if chunk.is_empty() {
                    return complete!(BatchOutput {
                        processed: data.processed,
                    });
                }

                let key = format!("process_{}", data.cursor);
                let processed_chunk: Vec<String> = ctx
                    .activity(&key, move || async move {
                        Ok(chunk
                            .into_iter()
                            .map(|item| item.to_uppercase())
                            .collect())
                    })
                    .await?;

                checkpoint!(process_batch(ProcessBatch {
                    cursor: data.cursor + processed_chunk.len(),
                    items: data.items,
                    processed: {
                        let mut processed = data.processed;
                        processed.extend(processed_chunk);
                        processed
                    },
                }))
            }
        }
    }
}

pub async fn run_checkpoint_loop_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("checkpoint-loop")?;
    let provider = JsonFileDurabilityProvider::new(path)?;
    let runtime = DurableRuntime::new(provider.clone());

    let ref_ = runtime
        .start::<BatchWorkflow>(
            BatchInput {
                items: vec![
                    "alpha".to_string(),
                    "bravo".to_string(),
                    "charlie".to_string(),
                    "delta".to_string(),
                    "echo".to_string(),
                ],
            },
            StartOptions {
                workflow_id: Some("loop-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;

    runtime.drain(DrainOptions::default()).await?;
    print_committed("checkpoint loop: completed", &provider, &ref_)
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_checkpoint_loop_demo().await
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

fn print_committed(
    label: &str,
    provider: &JsonFileDurabilityProvider,
    ref_: &InstanceRef,
) -> Result<(), WorkflowError> {
    println!(
        "{label} {}",
        serde_json::to_string_pretty(&summarize(provider.load_instance(ref_)?))?
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
