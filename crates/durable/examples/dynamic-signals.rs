/*
 * Shows static signals (`configure`, `cancel`), a state-derived signal type
 * (`provider_result:${job_id}`), and dynamic fanout (`approval:${approver}`).
 */

use durable::{
    cancel, complete, go, start, stay, workflow, DrainOptions, DurableRuntime,
    SqliteDurabilityProvider, StartOptions, WorkflowError,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamicInput {
    request_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DynamicOutput {
    job_id: String,
    approvals: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Empty {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaitingConfig {
    request_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConfigureSignal {
    job_id: String,
    approvers: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CancelSignal {
    reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaitingProvider {
    job_id: String,
    approvers: Vec<String>,
    approvals: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProviderResult {
    ok: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaitingApprovals {
    job_id: String,
    pending: Vec<String>,
    approvals: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApprovalSignal {
    decision: String,
}

workflow! {
    workflow DynamicSignalWorkflow {
        name: "demo_dynamic_signals",
        version: 1,
        input: DynamicInput,
        output: DynamicOutput,
        common: Empty,

        initial(input) {
            start! {
                common: Empty {},
                phase: waiting_config(WaitingConfig { request_id: input.request_id }),
            }
        }

        phase waiting_config(_data: WaitingConfig) {
            on {
                configure: signal<ConfigureSignal> async |event| {
                    go!(waiting_provider(WaitingProvider {
                        job_id: event.job_id,
                        approvers: event.approvers,
                        approvals: HashMap::new(),
                    }))
                },
                cancel: signal<CancelSignal> async |event| {
                    cancel!(event.reason)
                },
            }
        }

        phase waiting_provider(data: WaitingProvider) {
            on {
                provider_result: signal<ProviderResult> {
                    type: format!("provider_result:{}", data.job_id),
                    meta: data.job_id.clone(),
                } async |data| {
                    go!(waiting_approvals(WaitingApprovals {
                        job_id: data.job_id,
                        pending: data.approvers,
                        approvals: data.approvals,
                    }))
                },
                cancel: signal<CancelSignal> async |event| {
                    cancel!(event.reason)
                },
            }
        }

        phase waiting_approvals(data: WaitingApprovals) {
            on {
                for approver_id in data.pending.clone() {
                    approval: signal<ApprovalSignal> {
                        name: format!("approval:{approver_id}"),
                        type: format!("approval:{approver_id}"),
                        meta: approver_id.clone(),
                    } async |ctx, data, event| {
                        let approver_id: String = ctx.wait_meta()?;
                        let mut approvals = data.approvals;
                        approvals.insert(approver_id.clone(), event.decision);
                        let pending = data
                            .pending
                            .into_iter()
                            .filter(|id| id != &approver_id)
                            .collect::<Vec<_>>();

                        if pending.is_empty() {
                            complete!(DynamicOutput {
                                job_id: data.job_id,
                                approvals,
                            })
                        } else {
                            stay!(waiting_approvals(WaitingApprovals {
                                job_id: data.job_id,
                                pending,
                                approvals,
                            }))
                        }
                    },
                },
                cancel: signal<CancelSignal> async |event| {
                    cancel!(event.reason)
                },
            }
        }
    }
}

pub async fn run_dynamic_signals_demo() -> Result<(), WorkflowError> {
    let path = reset_demo_store("dynamic-signals")?;
    let provider = SqliteDurabilityProvider::new(&path)?;
    let runtime = DurableRuntime::new(provider);
    let ref_ = runtime
        .start::<DynamicSignalWorkflow>(
            DynamicInput {
                request_id: "request-1".to_string(),
            },
            StartOptions {
                workflow_id: Some("dynamic-signals-demo".to_string()),
                ..StartOptions::default()
            },
        )
        .await?;

    runtime
        .signal(
            &ref_,
            "configure",
            ConfigureSignal {
                job_id: "job-1".to_string(),
                approvers: vec!["ada".to_string(), "grace".to_string()],
            },
        )
        .await?;
    runtime.drain(DrainOptions::default()).await?;

    let restarted_provider = SqliteDurabilityProvider::new(&path)?;
    let restarted = DurableRuntime::new(restarted_provider);
    restarted.register::<DynamicSignalWorkflow>()?;
    restarted
        .signal(&ref_, "provider_result:job-1", ProviderResult { ok: true })
        .await?;
    restarted.drain(DrainOptions::default()).await?;
    restarted
        .signal(
            &ref_,
            "approval:ada",
            ApprovalSignal {
                decision: "yes".to_string(),
            },
        )
        .await?;
    restarted.drain(DrainOptions::default()).await?;
    restarted
        .signal(
            &ref_,
            "approval:grace",
            ApprovalSignal {
                decision: "yes".to_string(),
            },
        )
        .await?;
    restarted.drain(DrainOptions::default()).await?;
    println!("dynamic signals: completed");
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    run_dynamic_signals_demo().await
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
