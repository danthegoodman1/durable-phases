/*
 * Shows a custom runner driving the public shard-step API. Each loop owns one
 * shard at a time, runs one bounded activation, then yields back to the caller.
 */

use durable::{
    complete, start, stay, workflow, DurableRuntime, InstanceRef, PersistedInstance,
    PersistedStatus, RunShardStepOptions, RuntimeOptions, SqliteDurabilityProvider, StartOptions,
    WorkerCancellation, WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;
use std::time::Duration as StdDuration;

const SHARD_COUNT: u32 = 3;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CustomRunnerInput {
    items: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CustomRunnerOutput {
    processed: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CustomRunnerCommon {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Process {
    cursor: usize,
    items: Vec<String>,
    processed: Vec<String>,
}

workflow! {
    workflow CustomRunnerWorkflow {
        name: "demo_custom_runner",
        version: 1,
        input: CustomRunnerInput,
        output: CustomRunnerOutput,
        common: CustomRunnerCommon,

        initial(input) {
            start! {
                common: CustomRunnerCommon {},
                phase: process(Process {
                    cursor: 0,
                    items: input.items,
                    processed: Vec::new(),
                }),
            }
        }

        phase process(data: Process) {
            run async |data| {
                let Some(item) = data.items.get(data.cursor).cloned() else {
                    return complete!(CustomRunnerOutput {
                        processed: data.processed,
                    });
                };

                stay!(process(Process {
                    cursor: data.cursor + 1,
                    items: data.items,
                    processed: {
                        let mut processed = data.processed;
                        processed.push(item.to_uppercase());
                        processed
                    },
                }))
            }
        }
    }
}

pub async fn run_custom_runner_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("custom-runner")?;
    let provider = SqliteDurabilityProvider::new(&path)?;
    let mut options = RuntimeOptions::default();
    options.worker_id = "custom-runner".to_string();
    options.shard_count = SHARD_COUNT;
    let runtime = DurableRuntime::with_options(provider.clone(), options, chrono::Utc::now);

    // This cancellation handle stands in for whatever lifecycle signal a host
    // gives a custom runtime. A serverless adapter might instead run one bounded
    // step and return.
    let cancellation = WorkerCancellation::new();

    // Start one tiny loop per shard. Each loop repeatedly "kicks" its shard by
    // calling run_shard_step; the durable shard lease decides who may work.
    let mut loops = Vec::new();
    for shard_id in 0..SHARD_COUNT {
        let runtime = runtime.clone();
        let cancellation = cancellation.clone();
        loops.push(tokio::spawn(async move {
            run_shard_loop(runtime, shard_id, cancellation).await
        }));
    }

    let mut refs: Vec<(u32, InstanceRef)> = Vec::new();
    for shard_id in 0..SHARD_COUNT {
        // Demo-only placement hack: normally workflow IDs come from business
        // identity, then the runtime hashes the ref to decide which shard to kick.
        // Here we choose IDs by shard only so the output shows one workflow per
        // runner loop.
        let workflow_id = workflow_id_for_shard(shard_id);
        let ref_ = runtime
            .start::<CustomRunnerWorkflow>(
                CustomRunnerInput {
                    items: vec![
                        format!("item-{shard_id}-a"),
                        format!("item-{shard_id}-b"),
                        format!("item-{shard_id}-c"),
                    ],
                },
                StartOptions {
                    workflow_id: Some(workflow_id),
                    ..StartOptions::default()
                },
            )
            .await?;
        refs.push((shard_id, ref_.into()));
    }

    println!(
        "custom runner: shard mapping {}",
        serde_json::to_string_pretty(
            &refs
                .iter()
                .map(|(shard_id, ref_)| {
                    json!({
                        "shardId": shard_id,
                        "workflowId": ref_.workflow_id,
                        "runId": ref_.run_id,
                    })
                })
                .collect::<Vec<_>>()
        )?
    );

    wait_for_completed(&provider, &refs).await?;

    // Stop the loops after all work is done. This mirrors a hosted runner
    // returning after it has no more immediate work, rather than running forever.
    cancellation.cancel();
    let mut activations_by_shard = Vec::new();
    for handle in loops {
        activations_by_shard.push(
            handle
                .await
                .map_err(|error| WorkflowError::new(error.to_string()))??,
        );
    }

    let mut outputs = Vec::new();
    for (shard_id, ref_) in refs {
        outputs.push(json!({
            "shardId": shard_id,
            "workflowId": ref_.workflow_id,
            "result": summarize(provider.load_instance(&ref_).await?),
        }));
    }

    println!("custom runner: shard activations {activations_by_shard:?}");
    println!(
        "custom runner: completed {}",
        serde_json::to_string_pretty(&outputs)?
    );

    drop(runtime);
    drop(provider);
    reset_demo_store("custom-runner")?;
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_custom_runner_demo().await
}

async fn run_shard_loop(
    runtime: DurableRuntime,
    shard_id: u32,
    cancellation: WorkerCancellation,
) -> Result<usize, WorkflowError> {
    let mut activations = 0;
    while !cancellation.is_cancelled() {
        // This is the public custom-runner primitive. It claims one shard, runs
        // at most one activation, commits through the provider, then releases.
        let result = runtime
            .run_shard_step(RunShardStepOptions {
                shard_id,
                max_activations: Some(1),
                cancellation: Some(cancellation.clone()),
            })
            .await?;
        activations += result.activations;
        if !result.claimed_shard || result.activations == 0 {
            // No lease or no ready work. A serverless adapter could return here
            // and rely on next_wake_at/watchdogs; this local demo idles briefly.
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    }
    Ok(activations)
}

fn workflow_id_for_shard(shard_id: u32) -> String {
    for attempt in 0..10_000 {
        let workflow_id = format!("custom-runner-{shard_id}-{attempt}");
        if durable::workflow_partition_shard(&workflow_id, "run-1", SHARD_COUNT) == shard_id {
            return workflow_id;
        }
    }
    panic!("could not find workflow id for shard {shard_id}");
}

async fn wait_for_completed(
    provider: &SqliteDurabilityProvider,
    refs: &[(u32, InstanceRef)],
) -> Result<Vec<PersistedInstance>, WorkflowError> {
    for _ in 0..1_000 {
        let mut instances = Vec::new();
        for (_, ref_) in refs {
            if let Some(instance) = provider.load_instance(ref_).await? {
                instances.push(instance);
            }
        }
        if instances.len() == refs.len()
            && instances
                .iter()
                .all(|instance| instance.status == PersistedStatus::Completed)
        {
            return Ok(instances);
        }
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
    Err(WorkflowError::new(
        "timed out waiting for custom runner demo workflows",
    ))
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
