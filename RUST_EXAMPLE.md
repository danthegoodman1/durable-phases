# Durable Phase Workflow example in Rust

This is the section 14 workflow translated into a Rust-shaped SDK. The intended Rust DX uses a procedural macro for workflow authoring, while the runtime still receives ordinary typed contracts, durable wait specs, and transition commands.

## Supporting runtime-facing types

```rust
use std::any::type_name;
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub trait Workflow {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    type Common: Serialize + DeserializeOwned + Send + 'static;
    type Phase: PhaseName + Serialize + DeserializeOwned + Send + 'static;

    const NAME: &'static str;
    const VERSION: u32;

    fn initial(input: Self::Input) -> Start<Self::Common, Self::Phase>;

    fn global_waits() -> Vec<WaitSpec> {
        vec![]
    }

    fn queries() -> Vec<QuerySpec> {
        vec![]
    }

    fn phase_action(phase: &Self::Phase) -> PhaseAction;
}

pub trait PhaseName {
    fn phase_name(&self) -> &'static str;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Start<Common, Phase> {
    pub common: Common,
    pub phase: Phase,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PhaseAction {
    Run,
    Wait {
        waits: Vec<WaitSpec>,
    },
}

impl PhaseAction {
    pub fn run() -> Self {
        Self::Run
    }

    pub fn wait(waits: Vec<WaitSpec>) -> Self {
        Self::Wait { waits }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WaitSpec {
    Signal {
        name: String,
        signal_type: String,
    },
    Timer {
        name: String,
        fire_at: Option<DateTime<Utc>>,
    },
    Child {
        name: String,
        workflow_name: String,
        workflow_version: u32,
        workflow_id: String,
        run_id: String,
    },
}

impl WaitSpec {
    pub fn signal<T>(name: impl Into<String>) -> Self
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        Self::Signal {
            name: name.into(),
            signal_type: type_name::<T>().to_string(),
        }
    }

    pub fn timer(name: impl Into<String>, fire_at: Option<DateTime<Utc>>) -> Self {
        Self::Timer {
            name: name.into(),
            fire_at,
        }
    }

    pub fn child<W>(name: impl Into<String>, handle: &ChildHandle<W>) -> Self
    where
        W: Workflow,
    {
        Self::Child {
            name: name.into(),
            workflow_name: handle.workflow_name.clone(),
            workflow_version: handle.workflow_version,
            workflow_id: handle.workflow_id.clone(),
            run_id: handle.run_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Transition<Output, Phase> {
    Stay(Phase),
    Go(Phase),
    Complete(Output),
    Cancel(String),
    Fail(SerializedError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuerySpec {
    pub name: String,
    pub output_type: String,
}

impl QuerySpec {
    pub fn new<T>(name: impl Into<String>) -> Self
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        Self {
            name: name.into(),
            output_type: type_name::<T>().to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InstanceSnapshot<Output, Common, Phase> {
    Running { common: Common, phase: Phase },
    Completed { output: Output },
    Canceled { reason: String },
    Failed { error: SerializedError },
}

impl<Output, Common, Phase> InstanceSnapshot<Output, Common, Phase> {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Running { .. } => "running",
            Self::Completed { .. } => "completed",
            Self::Canceled { .. } => "canceled",
            Self::Failed { .. } => "failed",
        }
    }
}

pub struct QueryContext<'a, Output, Common, Phase> {
    pub snapshot: &'a InstanceSnapshot<Output, Common, Phase>,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SerializedError {
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildHandle<W: Workflow> {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    #[serde(skip)]
    pub workflow: PhantomData<fn() -> W>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChildEvent<W: Workflow> {
    Ok {
        output: W::Output,
    },
    Err {
        error: SerializedError,
    },
}

#[async_trait]
pub trait DurableContext {
    fn now(&self) -> DateTime<Utc>;

    async fn activity<T, F, Fut>(&mut self, key: &'static str, f: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, WorkflowError>> + Send;

    async fn activity_with_options<T, F, Fut>(
        &mut self,
        key: &'static str,
        options: ActivityOptions,
        f: F,
    ) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce(ActivityContext) -> Fut + Send,
        Fut: Future<Output = Result<T, WorkflowError>> + Send;

    async fn child_start<W>(
        &mut self,
        key: &'static str,
        input: W::Input,
        options: ChildOptions,
    ) -> Result<ChildHandle<W>, WorkflowError>
    where
        W: Workflow + Send + 'static;

    async fn child_result<W>(
        &mut self,
        handle: ChildHandle<W>,
    ) -> Result<W::Output, WorkflowError>
    where
        W: Workflow + Send + 'static;

    async fn child_cancel<W>(&mut self, handle: ChildHandle<W>) -> Result<(), WorkflowError>
    where
        W: Workflow + Send + 'static;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ActivityOptions {
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub retry: Option<RetryPolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_interval: Duration,
}

pub struct ActivityContext {
    pub idempotency_key: String,
    pub last_heartbeat_details: Option<JsonValue>,
}

impl ActivityContext {
    pub async fn heartbeat<T>(&mut self, details: T) -> Result<(), WorkflowError>
    where
        T: Serialize + Send,
    {
        let _details = serde_json::to_value(details).map_err(|error| WorkflowError {
            message: error.to_string(),
        })?;

        // The runtime persists heartbeat details as effect metadata.
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChildOptions {
    pub workflow_id: Option<String>,
    pub parent_close_policy: ParentClosePolicy,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum ParentClosePolicy {
    #[default]
    Cancel,
    Abandon,
    Wait,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum ConflictPolicy {
    #[default]
    UseExisting,
    Fail,
    TerminateExisting,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowError {
    pub message: String,
}
```

`phase_action` is the Rust equivalent of the TypeScript `phase({ run })` or `phase({ on })` declaration. The runtime calls it after every checkpoint. `Run` means invoke the workflow's run dispatcher for the current phase. `Wait` means materialize the returned durable waits, including timers selected from current phase data.

The strings in `WaitSpec` are durable identifiers, not method references. When a timer fires or a signal/child result is selected, the runtime converts the persisted wait record into a workflow-specific ready-event enum and calls the workflow's dispatch function. The dispatch match is what actually invokes `poll_due`, `document_uploaded`, or any other handler.

Queries are typed snapshot reads generated by the macro. They load one committed sequence and do not create activations, consume signals, run activities, or checkpoint. Activity heartbeats are effect metadata only; they help retry/resume long-running activities but never update phase data.

## Domain types

```rust
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartialDocumentSet {
    pub documents: Vec<Document>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteDocumentSet {
    pub documents: Vec<Document>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycJob {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycPollResult {
    pub done: bool,
    pub value: Option<KycOutput>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycInput {
    pub customer_id: String,
    pub documents: CompleteDocumentSet,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycOutput {
    pub status: KycStatus,
    pub contract_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KycStatus {
    Approved,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingInput {
    pub customer_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingOutput {
    pub customer_id: String,
    pub activated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelEvent {
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmptyEvent {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycSummary {
    pub sequence: u64,
    pub status: String,
    pub phase: Option<String>,
    pub provider_job_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingProgress {
    pub sequence: u64,
    pub status: String,
    pub phase: Option<String>,
    pub documents_received: Option<usize>,
    pub next_reminder_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KycSubmitHeartbeat {
    pub step: String,
}

pub fn empty_document_set() -> PartialDocumentSet {
    PartialDocumentSet { documents: vec![] }
}

pub fn add_document(mut documents: PartialDocumentSet, document: Document) -> PartialDocumentSet {
    documents.documents.push(document);
    documents
}

pub fn has_all_required_documents(documents: &PartialDocumentSet) -> bool {
    !documents.documents.is_empty()
}

pub fn to_complete_document_set(documents: PartialDocumentSet) -> CompleteDocumentSet {
    CompleteDocumentSet {
        documents: documents.documents,
    }
}

pub fn minutes_from_now(minutes: i64) -> DateTime<Utc> {
    Utc::now() + Duration::minutes(minutes)
}

pub fn days_from_now(days: i64) -> DateTime<Utc> {
    Utc::now() + Duration::days(days)
}
```

## Macro-authored workflows

This is the intended developer experience. The `workflow!` procedural macro is hypothetical here, but it is realistic for Rust: it can parse phase declarations, generate durable wait names, generate phase enums, generate ready-event enums, and generate dispatch code under the hood.

The durable strings still exist because the runtime must persist names like `poll_due` and `document_uploaded`. The developer writes them once as Rust identifiers, and the macro generates the string constants and dispatch matches.

### KYC child workflow

```rust
workflow! {
    pub workflow KycWorkflow {
        name: "kyc",
        version: 1,

        input: KycInput,
        output: KycOutput,
        common: KycCommon,

        initial(input) {
            start! {
                common: KycCommon {
                    customer_id: input.customer_id,
                },
                phase: submitting(KycSubmitting {
                    documents: input.documents,
                }),
            }
        }

        queries {
            summary: query<KycSummary> |snapshot, sequence| {
                match snapshot {
                    InstanceSnapshot::Running { phase, .. } => match phase {
                        KycPhase::Submitting(_) => KycSummary {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("submitting".to_string()),
                            provider_job_id: None,
                        },
                        KycPhase::WaitingForProvider(data) => KycSummary {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting_for_provider".to_string()),
                            provider_job_id: Some(data.provider_job_id.clone()),
                        },
                    },
                    snapshot => KycSummary {
                        sequence,
                        status: snapshot.status().to_string(),
                        phase: None,
                        provider_job_id: None,
                    },
                }
            },
        }

        phase submitting(data: KycSubmitting) {
            run async |ctx, common, data| {
                let job = ctx
                    .activity_with_options(
                        "submit_kyc",
                        ActivityOptions {
                            heartbeat_timeout: Some(Duration::minutes(1)),
                            ..ActivityOptions::default()
                        },
                        |activity| async move {
                            submit_kyc_with_heartbeat(
                                common.customer_id.clone(),
                                data.documents.clone(),
                                activity,
                            )
                            .await
                        },
                    )
                    .await?;

                go!(waiting_for_provider(KycWaitingForProvider {
                    provider_job_id: job.id,
                    next_poll_at: minutes_from_now(10),
                }))
            }
        }

        phase waiting_for_provider(data: KycWaitingForProvider) {
            on {
                provider_webhook: signal<KycOutput> async |event| {
                    complete!(event)
                },

                poll_due: timer(data.next_poll_at.clone()) async |ctx, data| {
                    let result = ctx
                        .activity("poll_kyc", || {
                            poll_kyc_provider(data.provider_job_id.clone())
                        })
                        .await?;

                    if result.done {
                        complete!(result.value.expect("done poll has value"))
                    } else {
                        stay!(waiting_for_provider(KycWaitingForProvider {
                            provider_job_id: data.provider_job_id,
                            next_poll_at: minutes_from_now(10),
                        }))
                    }
                },
            }
        }
    }
}
```

### Parent onboarding workflow

```rust
workflow! {
    pub workflow OnboardingWorkflow {
        name: "customer_onboarding",
        version: 1,

        input: OnboardingInput,
        output: OnboardingOutput,
        common: OnboardingCommon,

        initial(input) {
            start! {
                common: OnboardingCommon {
                    customer_id: input.customer_id,
                },
                phase: waiting_for_documents(WaitingForDocuments {
                    documents: empty_document_set(),
                    next_reminder_at: days_from_now(7),
                }),
            }
        }

        global {
            customer_canceled: signal<CancelEvent> async |event| {
                cancel!(event.reason)
            },
        }

        queries {
            progress: query<OnboardingProgress> |snapshot, sequence| {
                match snapshot {
                    InstanceSnapshot::Running { phase, .. } => match phase {
                        OnboardingPhase::WaitingForDocuments(data) => OnboardingProgress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting_for_documents".to_string()),
                            documents_received: Some(data.documents.documents.len()),
                            next_reminder_at: Some(data.next_reminder_at.clone()),
                        },
                        OnboardingPhase::WaitingForKyc(_) => OnboardingProgress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting_for_kyc".to_string()),
                            documents_received: None,
                            next_reminder_at: None,
                        },
                        OnboardingPhase::WaitingForSignature(data) => OnboardingProgress {
                            sequence,
                            status: "running".to_string(),
                            phase: Some("waiting_for_signature".to_string()),
                            documents_received: None,
                            next_reminder_at: Some(data.next_reminder_at.clone()),
                        },
                    },
                    snapshot => OnboardingProgress {
                        sequence,
                        status: snapshot.status().to_string(),
                        phase: None,
                        documents_received: None,
                        next_reminder_at: None,
                    },
                }
            },
        }

        phase waiting_for_documents(data: WaitingForDocuments) {
            on {
                document_uploaded: signal<Document> async |ctx, common, data, event| {
                    let documents = add_document(data.documents, event);

                    if !has_all_required_documents(&documents) {
                        return stay!(waiting_for_documents(WaitingForDocuments {
                            documents,
                            next_reminder_at: data.next_reminder_at,
                        }));
                    }

                    let kyc = ctx
                        .child_start::<KycWorkflow>(
                            "kyc",
                            KycInput {
                                customer_id: common.customer_id,
                                documents: to_complete_document_set(documents),
                            },
                            ChildOptions::default(),
                        )
                        .await?;

                    go!(waiting_for_kyc(WaitingForKyc { kyc }))
                },

                reminder_due: timer(data.next_reminder_at.clone()) async |ctx, common, data| {
                    ctx.activity("send_document_reminder", || {
                        send_document_reminder(common.customer_id.clone())
                    })
                    .await?;

                    stay!(waiting_for_documents(WaitingForDocuments {
                        documents: data.documents,
                        next_reminder_at: days_from_now(7),
                    }))
                },
            }
        }

        phase waiting_for_kyc(data: WaitingForKyc) {
            on {
                kyc_finished: child(data.kyc.clone()) async |event| {
                    let output = match event {
                        ChildEvent::Ok { output } => output,
                        ChildEvent::Err { .. } => {
                            return cancel!("KYC workflow failed");
                        }
                    };

                    if matches!(output.status, KycStatus::Failed) {
                        return cancel!(
                            output.reason.unwrap_or_else(|| "KYC failed".to_string())
                        );
                    }

                    go!(waiting_for_signature(WaitingForSignature {
                        contract_id: output.contract_id.expect("approved KYC has contract id"),
                        next_reminder_at: days_from_now(3),
                    }))
                },
            }
        }

        phase waiting_for_signature(data: WaitingForSignature) {
            on {
                contract_signed: signal<EmptyEvent> async |ctx, common, data| {
                    ctx.activity("activate_account", || {
                        activate_account(common.customer_id.clone(), data.contract_id.clone())
                    })
                    .await?;

                    complete!(OnboardingOutput {
                        customer_id: common.customer_id,
                        activated_at: ctx.now(),
                    })
                },

                reminder_due: timer(data.next_reminder_at.clone()) async |ctx, common, data| {
                    ctx.activity("send_signature_reminder", || {
                        send_signature_reminder(
                            common.customer_id.clone(),
                            data.contract_id.clone(),
                        )
                    })
                    .await?;

                    stay!(waiting_for_signature(WaitingForSignature {
                        contract_id: data.contract_id,
                        next_reminder_at: days_from_now(3),
                    }))
                },
            }
        }
    }
}
```

## Activity stubs

```rust
pub async fn submit_kyc_with_heartbeat(
    _customer_id: String,
    _documents: CompleteDocumentSet,
    mut activity: ActivityContext,
) -> Result<KycJob, WorkflowError> {
    activity
        .heartbeat(KycSubmitHeartbeat {
            step: "submitted_to_provider".to_string(),
        })
        .await?;

    todo!()
}

pub async fn poll_kyc_provider(_provider_job_id: String) -> Result<KycPollResult, WorkflowError> {
    todo!()
}

pub async fn send_document_reminder(_customer_id: String) -> Result<(), WorkflowError> {
    todo!()
}

pub async fn activate_account(
    _customer_id: String,
    _contract_id: String,
) -> Result<(), WorkflowError> {
    todo!()
}

pub async fn send_signature_reminder(
    _customer_id: String,
    _contract_id: String,
) -> Result<(), WorkflowError> {
    todo!()
}
```
