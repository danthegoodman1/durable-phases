/*
 * Shows activation-scoped activity memoization across a failed handler retry:
 * the first activation records a completed activity and then fails before
 * checkpointing, and the retried activation reuses the completed effect result
 * instead of running the side effect again.
 */

use durable::{complete, start, workflow, WorkflowError};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static UNSTABLE_ACTIVITY_CALLS: AtomicUsize = AtomicUsize::new(0);
static UNSTABLE_SHOULD_THROW: AtomicBool = AtomicBool::new(false);

pub fn reset(should_throw: bool) {
    UNSTABLE_ACTIVITY_CALLS.store(0, Ordering::SeqCst);
    UNSTABLE_SHOULD_THROW.store(should_throw, Ordering::SeqCst);
}

pub fn activity_calls() -> usize {
    UNSTABLE_ACTIVITY_CALLS.load(Ordering::SeqCst)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnstableInput {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnstableOutput {
    pub ok: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnstableCommon {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnstablePhase {}

workflow! {
    pub workflow UnstableWorkflow {
        name: "unstable",
        version: 1,
        input: UnstableInput,
        output: UnstableOutput,
        common: UnstableCommon,

        initial(_input) {
            start! {
                common: UnstableCommon {},
                phase: unstable(UnstablePhase {}),
            }
        }

        phase unstable(data: UnstablePhase) {
            run async |ctx| {
                let result: UnstableOutput = ctx.activity("side_effect_once", || async {
                    UNSTABLE_ACTIVITY_CALLS.fetch_add(1, Ordering::SeqCst);
                    Ok(UnstableOutput { ok: true })
                }).await?;

                if UNSTABLE_SHOULD_THROW.swap(false, Ordering::SeqCst) {
                    return Err(WorkflowError::new("boom after durable effect"));
                }

                complete!(result)
            }
        }
    }
}
