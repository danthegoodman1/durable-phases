#[path = "checkpoint-loop.rs"]
mod checkpoint_loop;
#[path = "child-workflow.rs"]
mod child_workflow;
#[path = "immediate-and-signal.rs"]
mod immediate_and_signal;
#[path = "migration.rs"]
mod migration;
#[path = "timer-stay-restart.rs"]
mod timer_stay_restart;

use durable::WorkflowError;

#[tokio::main]
async fn main() -> Result<(), WorkflowError> {
    immediate_and_signal::run_immediate_and_signal_demo().await?;
    timer_stay_restart::run_timer_stay_restart_demo().await?;
    checkpoint_loop::run_checkpoint_loop_demo().await?;
    child_workflow::run_child_workflow_demo().await?;
    migration::run_migration_demo().await?;
    Ok(())
}
