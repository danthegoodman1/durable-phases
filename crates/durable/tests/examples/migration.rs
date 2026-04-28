/*
 * Shows checkpoint-boundary migration:
 * a v1 worker creates a running instance, then a v2 worker loads the same
 * durable store, applies migration 1 -> 2, recomputes waits for the migrated
 * phase, and then handles a signal using the v2 workflow code.
 */

use durable::{complete, start, workflow, MigrationArgs, MigrationResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationInput {
    pub customer_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationOutput {
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationCommonV1 {
    customer_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationWaitingV1 {
    salutation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinishEvent {
    pub punctuation: String,
}

workflow! {
    pub workflow MigratingOrderV1 {
        name: "migrating_order",
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationCommonV2 {
    customer_id: String,
    plan: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationWaitingV2 {
    greeting: String,
    migrated_from: String,
}

fn migrate_order_v1_to_v2(
    args: MigrationArgs,
) -> MigrationResult<MigrationCommonV2, MigratingOrderV2Phase> {
    let old_common: MigrationCommonV1 = serde_json::from_value(args.common).unwrap();
    let old_phase = args.phase;
    let old_data: MigrationWaitingV1 = serde_json::from_value(old_phase.data).unwrap();
    MigrationResult {
        common: MigrationCommonV2 {
            customer_id: old_common.customer_id,
            plan: "starter".to_string(),
        },
        phase: MigratingOrderV2Phase::WaitingForFinish(MigrationWaitingV2 {
            greeting: old_data.salutation,
            migrated_from: old_phase.name,
        }),
    }
}

workflow! {
    pub workflow MigratingOrderV2 {
        name: "migrating_order",
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
