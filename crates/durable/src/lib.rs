pub use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
pub use durable_macros::workflow;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::type_name;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SerializedError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowError {
    pub message: String,
}

impl WorkflowError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorkflowError {}

impl From<serde_json::Error> for WorkflowError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<std::io::Error> for WorkflowError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<rusqlite::Error> for WorkflowError {
    fn from(error: rusqlite::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<tokio_postgres::Error> for WorkflowError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::new(format!("{error:?}"))
    }
}

impl From<&str> for WorkflowError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl From<String> for WorkflowError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<WorkflowError> for SerializedError {
    fn from(error: WorkflowError) -> Self {
        Self {
            name: None,
            message: error.message,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PhaseSnapshot {
    pub name: String,
    pub data: JsonValue,
}

pub trait DurablePhase: Clone + Send + Sync + 'static {
    fn phase_name(&self) -> &'static str;
    fn into_snapshot(self) -> Result<PhaseSnapshot, WorkflowError>;
    fn from_snapshot(snapshot: PhaseSnapshot) -> Result<Self, WorkflowError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "lowercase")]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Start<Common, Phase> {
    pub common: Common,
    pub phase: Phase,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Transition<Output, Phase> {
    Stay(Phase),
    Go(Phase),
    Complete(Output),
    Cancel(String),
    Fail(SerializedError),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PhaseAction {
    Run,
    Wait { waits: Vec<WaitSpec> },
}

impl PhaseAction {
    pub fn run() -> Self {
        Self::Run
    }

    pub fn wait(waits: Vec<WaitSpec>) -> Self {
        Self::Wait { waits }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitSpec {
    Signal {
        name: String,
        signal_type: String,
    },
    Timer {
        name: String,
        fire_at: DateTime<Utc>,
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

    pub fn timer(name: impl Into<String>, fire_at: DateTime<Utc>) -> Self {
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

pub trait IntoTimerFireAt {
    fn into_fire_at(self) -> Option<DateTime<Utc>>;
}

impl IntoTimerFireAt for DateTime<Utc> {
    fn into_fire_at(self) -> Option<DateTime<Utc>> {
        Some(self)
    }
}

impl IntoTimerFireAt for Option<DateTime<Utc>> {
    fn into_fire_at(self) -> Option<DateTime<Utc>> {
        self
    }
}

pub trait IntoChildWait {
    fn into_wait_spec(self, name: impl Into<String>) -> Option<WaitSpec>;
}

impl<W> IntoChildWait for ChildHandle<W>
where
    W: Workflow,
{
    fn into_wait_spec(self, name: impl Into<String>) -> Option<WaitSpec> {
        Some(WaitSpec::child(name, &self))
    }
}

impl<W> IntoChildWait for Option<ChildHandle<W>>
where
    W: Workflow,
{
    fn into_wait_spec(self, name: impl Into<String>) -> Option<WaitSpec> {
        self.map(|handle| WaitSpec::child(name, &handle))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InstanceRef {
    pub workflow_id: String,
    pub run_id: String,
}

impl InstanceRef {
    pub fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ChildHandle<W: Workflow> {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    #[serde(skip)]
    pub workflow: PhantomData<fn() -> W>,
}

impl<W: Workflow> Clone for ChildHandle<W> {
    fn clone(&self) -> Self {
        Self {
            workflow_name: self.workflow_name.clone(),
            workflow_version: self.workflow_version,
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            workflow: PhantomData,
        }
    }
}

impl<W: Workflow> fmt::Debug for ChildHandle<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildHandle")
            .field("workflow_name", &self.workflow_name)
            .field("workflow_version", &self.workflow_version)
            .field("workflow_id", &self.workflow_id)
            .field("run_id", &self.run_id)
            .finish()
    }
}

impl<W: Workflow> PartialEq for ChildHandle<W> {
    fn eq(&self, other: &Self) -> bool {
        self.workflow_name == other.workflow_name
            && self.workflow_version == other.workflow_version
            && self.workflow_id == other.workflow_id
            && self.run_id == other.run_id
    }
}

impl<W: Workflow> Eq for ChildHandle<W> {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ChildEvent<W: Workflow> {
    Ok { output: W::Output },
    Err { error: SerializedError },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivityOptions {
    pub durability: ActivityDurability,
    pub heartbeat_timeout_ms: Option<u64>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub max_attempts: Option<u32>,
    pub max_elapsed_ms: Option<u64>,
    pub initial_interval_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
    pub backoff_coefficient: Option<u32>,
    pub non_retryable_error_names: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityDurability {
    #[default]
    Commit,
    Eager,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildOptions {
    pub workflow_id: Option<String>,
    pub durability: ChildDurability,
    pub parent_close_policy: ParentClosePolicy,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildDurability {
    #[default]
    Commit,
    Eager,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParentClosePolicy {
    #[default]
    Cancel,
    Abandon,
    Wait,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    #[default]
    UseExisting,
    Fail,
    TerminateExisting,
}

#[derive(Clone)]
pub struct ActivityContext {
    provider: Option<Arc<dyn DurabilityProvider>>,
    workflow_id: String,
    run_id: String,
    activation_id: String,
    worker_id: String,
    effect_id: String,
    attempt_id: String,
    pub idempotency_key: String,
    pub last_heartbeat_details: Option<JsonValue>,
}

impl ActivityContext {
    pub async fn heartbeat<T>(&mut self, details: T) -> Result<(), WorkflowError>
    where
        T: Serialize + Send,
    {
        let Some(provider) = &self.provider else {
            self.last_heartbeat_details = Some(serde_json::to_value(details)?);
            return Ok(());
        };
        let details = serde_json::to_value(details)?;
        provider
            .heartbeat_effect(HeartbeatEffectInput {
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                worker_id: self.worker_id.clone(),
                effect_id: self.effect_id.clone(),
                attempt_id: self.attempt_id.clone(),
                now: Utc::now(),
                details: Some(details.clone()),
            })
            .await?;
        self.last_heartbeat_details = Some(details);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TimerEvent {
    pub fired_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MigrationArgs {
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub from_version: u32,
    pub to_version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MigrationResult<Common, Phase> {
    pub common: Common,
    pub phase: Phase,
}

pub trait IntoMigrationOutput<Common, Phase> {
    fn into_output(self) -> Result<MigrationResult<Common, Phase>, WorkflowError>;
}

impl<Common, Phase> IntoMigrationOutput<Common, Phase> for MigrationResult<Common, Phase> {
    fn into_output(self) -> Result<MigrationResult<Common, Phase>, WorkflowError> {
        Ok(self)
    }
}

impl<Common, Phase> IntoMigrationOutput<Common, Phase>
    for Result<MigrationResult<Common, Phase>, WorkflowError>
{
    fn into_output(self) -> Result<MigrationResult<Common, Phase>, WorkflowError> {
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ReadyEvent {
    Signal {
        signal_id: String,
        payload: JsonValue,
        occurred_at: DateTime<Utc>,
    },
    Timer {
        fired_at: DateTime<Utc>,
        occurred_at: DateTime<Utc>,
    },
    Child {
        child_record_id: String,
        occurred_at: DateTime<Utc>,
        event: ChildEventValue,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ChildEventValue {
    Ok { output: JsonValue },
    Err { error: SerializedError },
}

pub fn decode_timer_event(event: ReadyEvent) -> Result<TimerEvent, WorkflowError> {
    match event {
        ReadyEvent::Timer { fired_at, .. } => Ok(TimerEvent { fired_at }),
        _ => Err(WorkflowError::new("timer wait received non-timer event")),
    }
}

pub fn decode_signal_event<T>(event: ReadyEvent) -> Result<T, WorkflowError>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    match event {
        ReadyEvent::Signal { payload, .. } => Ok(serde_json::from_value(payload)?),
        _ => Err(WorkflowError::new("signal wait received non-signal event")),
    }
}

pub fn decode_child_event<W>(
    _handle: &ChildHandle<W>,
    event: ReadyEvent,
) -> Result<ChildEvent<W>, WorkflowError>
where
    W: Workflow,
{
    match event {
        ReadyEvent::Child {
            event: ChildEventValue::Ok { output },
            ..
        } => Ok(ChildEvent::Ok {
            output: serde_json::from_value(output)?,
        }),
        ReadyEvent::Child {
            event: ChildEventValue::Err { error },
            ..
        } => Ok(ChildEvent::Err { error }),
        _ => Err(WorkflowError::new("child wait received non-child event")),
    }
}

#[async_trait]
pub trait Workflow: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    type Common: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type Phase: DurablePhase;

    const NAME: &'static str;
    const VERSION: u32;

    fn initial(input: Self::Input) -> Start<Self::Common, Self::Phase>;

    fn global_waits() -> Vec<WaitSpec> {
        vec![]
    }

    fn phase_action(_phase: &Self::Phase) -> PhaseAction {
        PhaseAction::wait(vec![])
    }

    async fn dispatch_run(
        _ctx: &mut DurableContext,
        _common: Self::Common,
        _phase: Self::Phase,
    ) -> Result<Transition<Self::Output, Self::Phase>, WorkflowError> {
        Err(WorkflowError::new("phase is not runnable"))
    }

    async fn dispatch_event(
        _ctx: &mut DurableContext,
        _common: Self::Common,
        _phase: Self::Phase,
        _wait_name: &str,
        _event: ReadyEvent,
    ) -> Result<Transition<Self::Output, Self::Phase>, WorkflowError> {
        Err(WorkflowError::new("unknown wait"))
    }

    fn query(
        _name: &str,
        _snapshot: InstanceSnapshot<Self::Output, Self::Common, Self::Phase>,
        _sequence: u64,
    ) -> Result<JsonValue, WorkflowError> {
        Err(WorkflowError::new("unknown query"))
    }

    fn migrate(
        _from_version: u32,
        _args: MigrationArgs,
    ) -> Result<Option<MigrationResult<Self::Common, Self::Phase>>, WorkflowError> {
        Ok(None)
    }
}

#[async_trait]
pub trait ErasedWorkflow: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn initial_value(&self, input: JsonValue) -> Result<StartValue, WorkflowError>;
    fn materialize_waits(
        &self,
        common: &JsonValue,
        phase: &PhaseSnapshot,
        ready_at: DateTime<Utc>,
    ) -> Result<Vec<DurableWait>, WorkflowError>;
    fn validate_running(
        &self,
        common: JsonValue,
        phase: PhaseSnapshot,
    ) -> Result<InstanceStatusValue, WorkflowError>;
    fn query_value(
        &self,
        name: &str,
        instance: &PersistedInstance,
    ) -> Result<JsonValue, WorkflowError>;
    fn migrate_value(
        &self,
        from_version: u32,
        args: MigrationArgs,
    ) -> Result<Option<StartValue>, WorkflowError>;

    async fn dispatch_run_value(
        &self,
        ctx: &mut DurableContext,
        common: JsonValue,
        phase: PhaseSnapshot,
    ) -> Result<TransitionValue, WorkflowError>;

    async fn dispatch_event_value(
        &self,
        ctx: &mut DurableContext,
        common: JsonValue,
        phase: PhaseSnapshot,
        wait_name: &str,
        event: ReadyEvent,
    ) -> Result<TransitionValue, WorkflowError>;
}

pub struct WorkflowAdapter<W: Workflow>(PhantomData<fn() -> W>);

impl<W: Workflow> Default for WorkflowAdapter<W> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<W> ErasedWorkflow for WorkflowAdapter<W>
where
    W: Workflow,
{
    fn name(&self) -> &'static str {
        W::NAME
    }

    fn version(&self) -> u32 {
        W::VERSION
    }

    fn initial_value(&self, input: JsonValue) -> Result<StartValue, WorkflowError> {
        let input = serde_json::from_value::<W::Input>(input)?;
        let start = W::initial(input);
        Ok(StartValue {
            common: serde_json::to_value(start.common)?,
            phase: start.phase.into_snapshot()?,
        })
    }

    fn materialize_waits(
        &self,
        common: &JsonValue,
        phase: &PhaseSnapshot,
        ready_at: DateTime<Utc>,
    ) -> Result<Vec<DurableWait>, WorkflowError> {
        let _common = serde_json::from_value::<W::Common>(common.clone())?;
        let phase = W::Phase::from_snapshot(phase.clone())?;
        let mut waits = Vec::new();

        for wait in W::global_waits() {
            waits.push(wait_spec_to_durable(wait, WaitScope::Global));
        }

        match W::phase_action(&phase) {
            PhaseAction::Run => waits.push(DurableWait::Run {
                name: "__run".to_string(),
                ready_at,
            }),
            PhaseAction::Wait { waits: phase_waits } => {
                for wait in phase_waits {
                    waits.push(wait_spec_to_durable(wait, WaitScope::Phase));
                }
            }
        }

        Ok(waits)
    }

    fn validate_running(
        &self,
        common: JsonValue,
        phase: PhaseSnapshot,
    ) -> Result<InstanceStatusValue, WorkflowError> {
        let common = serde_json::to_value(serde_json::from_value::<W::Common>(common)?)?;
        let phase = W::Phase::from_snapshot(phase)?.into_snapshot()?;
        Ok(InstanceStatusValue::Running { common, phase })
    }

    fn query_value(
        &self,
        name: &str,
        instance: &PersistedInstance,
    ) -> Result<JsonValue, WorkflowError> {
        let snapshot = typed_snapshot::<W>(instance)?;
        W::query(name, snapshot, instance.sequence)
    }

    fn migrate_value(
        &self,
        from_version: u32,
        args: MigrationArgs,
    ) -> Result<Option<StartValue>, WorkflowError> {
        let Some(result) = W::migrate(from_version, args)? else {
            return Ok(None);
        };

        Ok(Some(StartValue {
            common: serde_json::to_value(result.common)?,
            phase: result.phase.into_snapshot()?,
        }))
    }

    async fn dispatch_run_value(
        &self,
        ctx: &mut DurableContext,
        common: JsonValue,
        phase: PhaseSnapshot,
    ) -> Result<TransitionValue, WorkflowError> {
        let common = serde_json::from_value::<W::Common>(common)?;
        let phase = W::Phase::from_snapshot(phase)?;
        let transition = W::dispatch_run(ctx, common, phase).await?;
        transition_to_value(transition)
    }

    async fn dispatch_event_value(
        &self,
        ctx: &mut DurableContext,
        common: JsonValue,
        phase: PhaseSnapshot,
        wait_name: &str,
        event: ReadyEvent,
    ) -> Result<TransitionValue, WorkflowError> {
        let common = serde_json::from_value::<W::Common>(common)?;
        let phase = W::Phase::from_snapshot(phase)?;
        let transition = W::dispatch_event(ctx, common, phase, wait_name, event).await?;
        transition_to_value(transition)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StartValue {
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TransitionValue {
    Stay(PhaseSnapshot),
    Go(PhaseSnapshot),
    Complete(JsonValue),
    Cancel(String),
    Fail(SerializedError),
}

fn transition_to_value<Output, Phase>(
    transition: Transition<Output, Phase>,
) -> Result<TransitionValue, WorkflowError>
where
    Output: Serialize,
    Phase: DurablePhase,
{
    match transition {
        Transition::Stay(phase) => Ok(TransitionValue::Stay(phase.into_snapshot()?)),
        Transition::Go(phase) => Ok(TransitionValue::Go(phase.into_snapshot()?)),
        Transition::Complete(output) => {
            Ok(TransitionValue::Complete(serde_json::to_value(output)?))
        }
        Transition::Cancel(reason) => Ok(TransitionValue::Cancel(reason)),
        Transition::Fail(error) => Ok(TransitionValue::Fail(error)),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PersistedStatus {
    Running,
    Completed,
    Canceled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersistedInstance {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub partition_shard: u32,
    pub sequence: u64,
    pub status: PersistedStatus,
    pub common: Option<JsonValue>,
    pub phase: Option<PhaseSnapshot>,
    pub output: Option<JsonValue>,
    pub error: Option<SerializedError>,
    pub cancel_reason: Option<String>,
    pub waits: Vec<DurableWait>,
    pub effects: Vec<EffectRecord>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub parent: Option<ParentLink>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ParentLink {
    pub workflow_id: String,
    pub run_id: String,
    pub child_record_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DurableWait {
    Run {
        name: String,
        ready_at: DateTime<Utc>,
    },
    Signal {
        name: String,
        r#type: String,
        scope: WaitScope,
    },
    Timer {
        name: String,
        fire_at: DateTime<Utc>,
    },
    Child {
        name: String,
        workflow_name: String,
        workflow_version: u32,
        workflow_id: String,
        run_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitScope {
    Phase,
    Global,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignalRecord {
    pub signal_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub r#type: String,
    pub payload: JsonValue,
    pub received_at: DateTime<Utc>,
    pub consumed_by_sequence: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EffectRecord {
    pub effect_id: String,
    pub activation_id: String,
    pub key: String,
    pub idempotency_key: String,
    pub status: EffectStatus,
    pub attempt: Option<u32>,
    pub attempt_id: Option<String>,
    pub attempt_owner_id: Option<String>,
    pub attempt_started_at: Option<DateTime<Utc>>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub start_to_close_deadline: Option<DateTime<Utc>>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub heartbeat_deadline: Option<DateTime<Utc>>,
    pub max_attempts: Option<u32>,
    pub max_elapsed_ms: Option<u64>,
    pub initial_interval_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
    pub backoff_coefficient: Option<u32>,
    pub first_attempt_started_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_failure: Option<SerializedError>,
    pub non_retryable_error_names: Vec<String>,
    pub timed_out_at: Option<DateTime<Utc>>,
    pub timeout_kind: Option<ActivityTimeoutKind>,
    pub result: Option<JsonValue>,
    pub error: Option<SerializedError>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub heartbeat_details: Option<JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityTimeoutKind {
    Heartbeat,
    StartToClose,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChildRecord {
    pub child_record_id: String,
    pub parent_workflow_id: String,
    pub parent_run_id: String,
    pub activation_id: String,
    pub key: String,
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub status: ChildStatus,
    pub parent_close_policy: ParentClosePolicy,
    pub completed_at: Option<DateTime<Utc>>,
    pub output: Option<JsonValue>,
    pub error: Option<SerializedError>,
    pub delivered_by_sequence: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildStatus {
    Started,
    Completed,
    Failed,
    Abandoned,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MemoryTaskKind {
    Migration,
    Run,
    Event,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MemoryTask {
    task_id: String,
    activation_id: String,
    workflow_name: String,
    workflow_version: u32,
    workflow_id: String,
    run_id: String,
    partition_shard: u32,
    sequence: u64,
    kind: MemoryTaskKind,
    wait_name: Option<String>,
    event: Option<ReadyEvent>,
    ready_at: DateTime<Utc>,
    sort_key: String,
    claim_owner_id: Option<String>,
    claim_epoch: Option<u64>,
    lease_until: Option<DateTime<Utc>>,
    blocked_until: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompletedActivationClaim {
    activation_id: String,
    workflow_id: String,
    run_id: String,
    sequence: u64,
    kind: MemoryTaskKind,
    owner_id: Option<String>,
    completed_by_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Store {
    instances: HashMap<String, PersistedInstance>,
    signals: Vec<SignalRecord>,
    children: Vec<ChildRecord>,
    tasks: HashMap<String, MemoryTask>,
    #[serde(default)]
    task_order: BTreeSet<(String, String)>,
    claimed_sequence_epochs: HashMap<String, u64>,
    completed_activation_claims: Vec<CompletedActivationClaim>,
    shard_leases: HashMap<u32, ShardLease>,
    next_signal_id: u64,
    next_effect_id: u64,
    next_child_id: u64,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            instances: HashMap::new(),
            signals: Vec::new(),
            children: Vec::new(),
            tasks: HashMap::new(),
            task_order: BTreeSet::new(),
            claimed_sequence_epochs: HashMap::new(),
            completed_activation_claims: Vec::new(),
            shard_leases: HashMap::new(),
            next_signal_id: 1,
            next_effect_id: 1,
            next_child_id: 1,
        }
    }
}

#[derive(Clone)]
struct ShardEngine {
    state: Store,
}

impl ShardEngine {
    fn from_store(state: Store) -> Self {
        let mut state = state;
        rebuild_task_order(&mut state);
        Self { state }
    }

    fn snapshot_store(&self) -> Store {
        self.state.clone()
    }

    pub fn create_instance(
        &mut self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        let state = &mut self.state;
        let key = instance_key(&input.workflow_id, &input.run_id);
        let conflict_policy = input.conflict_policy.unwrap_or(ConflictPolicy::Fail);

        if let Some(existing) = state.instances.get(&key) {
            match conflict_policy {
                ConflictPolicy::UseExisting => {
                    return Ok(InstanceRef::new(
                        existing.workflow_id.clone(),
                        existing.run_id.clone(),
                    ))
                }
                ConflictPolicy::Fail => {
                    return Err(WorkflowError::new(format!(
                        "workflow instance already exists: {}/{}",
                        input.workflow_id, input.run_id
                    )))
                }
                ConflictPolicy::TerminateExisting => {}
            }
        }

        let instance = PersistedInstance {
            workflow_name: input.workflow_name,
            workflow_version: input.workflow_version,
            workflow_id: input.workflow_id.clone(),
            run_id: input.run_id.clone(),
            partition_shard: input.partition_shard,
            sequence: 0,
            status: PersistedStatus::Running,
            common: Some(input.common),
            phase: Some(input.phase),
            output: None,
            error: None,
            cancel_reason: None,
            waits: input.waits,
            effects: Vec::new(),
            created_at: input.now,
            updated_at: input.now,
            parent: input.parent,
        };
        state.instances.insert(key, instance.clone());
        replace_tasks_for_instance(state, &instance);

        save_engine(state)?;
        Ok(InstanceRef::new(input.workflow_id, input.run_id))
    }

    pub fn create_child_instance(
        &mut self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        let state = &mut self.state;

        if let Some(existing) = state.children.iter().find(|record| {
            record.parent_workflow_id == input.parent_workflow_id
                && record.parent_run_id == input.parent_run_id
                && record.activation_id == input.activation_id
                && record.key == input.key
        }) {
            return Ok(child_handle_value(existing));
        }

        let instance_key = instance_key(&input.workflow_id, &input.run_id);
        if state.instances.contains_key(&instance_key) {
            return Err(WorkflowError::new(format!(
                "child workflow instance already exists: {}/{}",
                input.workflow_id, input.run_id
            )));
        }

        let child_record_id = format!("child-{}", state.next_child_id);
        state.next_child_id += 1;

        let child_instance = PersistedInstance {
            workflow_name: input.workflow_name.clone(),
            workflow_version: input.workflow_version,
            workflow_id: input.workflow_id.clone(),
            run_id: input.run_id.clone(),
            partition_shard: input.partition_shard,
            sequence: 0,
            status: PersistedStatus::Running,
            common: Some(input.common),
            phase: Some(input.phase),
            output: None,
            error: None,
            cancel_reason: None,
            waits: input.waits,
            effects: Vec::new(),
            created_at: input.now,
            updated_at: input.now,
            parent: Some(ParentLink {
                workflow_id: input.parent_workflow_id.clone(),
                run_id: input.parent_run_id.clone(),
                child_record_id: child_record_id.clone(),
            }),
        };
        state.instances.insert(instance_key, child_instance.clone());
        replace_tasks_for_instance(state, &child_instance);

        state.children.push(ChildRecord {
            child_record_id: child_record_id.clone(),
            parent_workflow_id: input.parent_workflow_id,
            parent_run_id: input.parent_run_id,
            activation_id: input.activation_id,
            key: input.key,
            workflow_name: input.workflow_name,
            workflow_version: input.workflow_version,
            workflow_id: input.workflow_id,
            run_id: input.run_id,
            status: ChildStatus::Started,
            parent_close_policy: input.parent_close_policy,
            completed_at: None,
            output: None,
            error: None,
            delivered_by_sequence: None,
        });

        let handle = state
            .children
            .last()
            .map(child_handle_value)
            .ok_or_else(|| WorkflowError::new("failed to create child record"))?;

        save_engine(state)?;
        Ok(handle)
    }

    pub fn cancel_child(&mut self, input: CancelChildInput) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        cancel_child_in_state(state, input);
        save_engine(state)
    }

    pub fn load_instance(
        &self,
        ref_: &InstanceRef,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let state = &self.state;
        Ok(state.instances.get(&instance_key_ref(ref_)).cloned())
    }

    pub fn append_signal(
        &mut self,
        workflow_id: String,
        run_id: String,
        r#type: String,
        payload: JsonValue,
        received_at: DateTime<Utc>,
    ) -> Result<SignalRecord, WorkflowError> {
        let state = &mut self.state;
        let key = instance_key(&workflow_id, &run_id);
        let Some(instance) = state.instances.get(&key) else {
            return Err(WorkflowError::new(format!(
                "cannot signal unknown workflow: {}/{}",
                workflow_id, run_id
            )));
        };

        if instance.status != PersistedStatus::Running {
            return Err(WorkflowError::new(format!(
                "cannot signal non-running workflow: {}/{}",
                workflow_id, run_id
            )));
        }

        let signal = SignalRecord {
            signal_id: format!("signal-{}", state.next_signal_id),
            workflow_id,
            run_id,
            r#type,
            payload,
            received_at,
            consumed_by_sequence: None,
        };
        state.next_signal_id += 1;
        state.signals.push(signal.clone());
        if let Some(instance) = state.instances.get(&key).cloned() {
            refresh_signal_tasks_for_instance(state, &instance);
        }
        save_engine(state)?;
        Ok(signal)
    }

    pub fn get_or_reserve_effect(
        &mut self,
        workflow_id: &str,
        run_id: &str,
        activation_id: &str,
        key: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        options: ActivityOptions,
        max_attempts: Option<u32>,
    ) -> Result<EffectReservation, WorkflowError> {
        let state = &mut self.state;
        expire_activity_timeouts(state, now, None, Some(worker_id), None);
        let key_for_instance = instance_key(workflow_id, run_id);

        let existing = state
            .instances
            .get(&key_for_instance)
            .ok_or_else(|| {
                WorkflowError::new(format!("unknown workflow instance: {workflow_id}/{run_id}"))
            })?
            .effects
            .iter()
            .find(|effect| effect.activation_id == activation_id && effect.key == key)
            .cloned();

        if let Some(existing) = &existing {
            match existing.status {
                EffectStatus::Completed => {
                    return Ok(EffectReservation::Completed {
                        result: existing.result.clone().unwrap_or(JsonValue::Null),
                    });
                }
                EffectStatus::Failed => {
                    return Ok(EffectReservation::Failed {
                        error: existing.error.clone().unwrap_or(SerializedError {
                            name: None,
                            message: "effect failed".to_string(),
                        }),
                    });
                }
                EffectStatus::Pending => {
                    if existing
                        .next_attempt_at
                        .is_some_and(|next_attempt_at| next_attempt_at > now)
                    {
                        return Err(WorkflowError::new(format!(
                            "effect retry is not ready: {}",
                            existing.effect_id
                        )));
                    }
                }
            }
        }

        let idempotency_key = format!("{workflow_id}/{run_id}/{activation_id}/{key}");
        let allocated_effect_id = if existing.is_none() {
            let effect_id = format!("effect-{}", state.next_effect_id);
            state.next_effect_id += 1;
            Some(effect_id)
        } else {
            None
        };
        let instance = state.instances.get_mut(&key_for_instance).ok_or_else(|| {
            WorkflowError::new(format!("unknown workflow instance: {workflow_id}/{run_id}"))
        })?;
        let existing_index = instance
            .effects
            .iter()
            .position(|effect| effect.activation_id == activation_id && effect.key == key);
        let attempt = existing_index
            .and_then(|index| instance.effects[index].attempt)
            .unwrap_or(1);
        let effect_id = if let Some(index) = existing_index {
            instance.effects[index].effect_id.clone()
        } else {
            allocated_effect_id.expect("new effect id allocated")
        };
        let first_attempt_started_at = existing_index
            .and_then(|index| instance.effects[index].first_attempt_started_at)
            .unwrap_or(now);
        let attempt_id = format!("attempt-{}", Uuid::new_v4());
        let heartbeat_details =
            existing_index.and_then(|index| instance.effects[index].heartbeat_details.clone());
        let effect = EffectRecord {
            effect_id: effect_id.clone(),
            activation_id: activation_id.to_string(),
            key: key.to_string(),
            idempotency_key: existing_index
                .map(|index| instance.effects[index].idempotency_key.clone())
                .unwrap_or_else(|| idempotency_key.clone()),
            status: EffectStatus::Pending,
            attempt: Some(attempt),
            attempt_id: Some(attempt_id.clone()),
            attempt_owner_id: Some(worker_id.to_string()),
            attempt_started_at: Some(now),
            start_to_close_timeout_ms: options.start_to_close_timeout_ms,
            start_to_close_deadline: deadline_from(now, options.start_to_close_timeout_ms),
            heartbeat_timeout_ms: options.heartbeat_timeout_ms,
            heartbeat_deadline: deadline_from(now, options.heartbeat_timeout_ms),
            max_attempts: max_attempts.or(options.max_attempts).or(Some(1)),
            max_elapsed_ms: options.max_elapsed_ms,
            initial_interval_ms: options.initial_interval_ms.or(Some(1_000)),
            max_interval_ms: options.max_interval_ms.or(Some(30_000)),
            backoff_coefficient: options.backoff_coefficient.or(Some(2)),
            first_attempt_started_at: Some(first_attempt_started_at),
            next_attempt_at: None,
            last_failure: existing_index
                .and_then(|index| instance.effects[index].last_failure.clone()),
            non_retryable_error_names: options.non_retryable_error_names,
            timed_out_at: None,
            timeout_kind: None,
            result: None,
            error: None,
            heartbeat_at: None,
            heartbeat_details: heartbeat_details.clone(),
        };
        if let Some(index) = existing_index {
            instance.effects[index] = effect;
        } else {
            instance.effects.push(effect);
        }

        save_engine(state)?;
        Ok(EffectReservation::Reserved {
            effect_id,
            idempotency_key,
            attempt,
            attempt_id,
            heartbeat_details,
        })
    }

    pub fn complete_effect(
        &mut self,
        workflow_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        result: JsonValue,
    ) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        expire_activity_timeouts(state, now, None, Some(worker_id), None);
        let instance = require_instance_mut(state, workflow_id, run_id)?;
        let effect = instance
            .effects
            .iter_mut()
            .find(|effect| effect.effect_id == effect_id)
            .ok_or_else(|| WorkflowError::new(format!("unknown effect: {effect_id}")))?;
        assert_mutable_effect(effect, attempt_id, worker_id)?;
        effect.status = EffectStatus::Completed;
        effect.result = Some(result);
        effect.error = None;
        effect.attempt_id = None;
        effect.attempt_owner_id = None;
        effect.attempt_started_at = None;
        effect.start_to_close_deadline = None;
        effect.heartbeat_deadline = None;
        effect.next_attempt_at = None;
        save_engine(state)
    }

    pub fn heartbeat_effect(
        &mut self,
        workflow_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        details: Option<JsonValue>,
    ) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        expire_activity_timeouts(state, now, None, Some(worker_id), None);
        let instance = require_instance_mut(state, workflow_id, run_id)?;
        let effect = instance
            .effects
            .iter_mut()
            .find(|effect| effect.effect_id == effect_id)
            .ok_or_else(|| WorkflowError::new(format!("unknown effect: {effect_id}")))?;
        assert_mutable_effect(effect, attempt_id, worker_id)?;
        effect.heartbeat_at = Some(now);
        effect.heartbeat_details = details;
        effect.heartbeat_deadline = deadline_from(now, effect.heartbeat_timeout_ms);
        save_engine(state)
    }

    pub fn fail_effect(
        &mut self,
        workflow_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt_id: &str,
        worker_id: &str,
        error: SerializedError,
        now: DateTime<Utc>,
        retryable: Option<bool>,
    ) -> Result<FailEffectResult, WorkflowError> {
        let state = &mut self.state;
        expire_activity_timeouts(state, now, None, Some(worker_id), None);
        let release_activation_id;
        let blocked_until;
        let instance = require_instance_mut(state, workflow_id, run_id)?;
        let effect = instance
            .effects
            .iter_mut()
            .find(|effect| effect.effect_id == effect_id)
            .ok_or_else(|| WorkflowError::new(format!("unknown effect: {effect_id}")))?;
        assert_mutable_effect(effect, attempt_id, worker_id)?;
        let decision = retry_decision_for_effect(effect, &error, now, retryable.unwrap_or(true));
        match &decision {
            FailEffectResult::RetryScheduled {
                next_attempt_at,
                next_attempt,
            } => {
                effect.status = EffectStatus::Pending;
                effect.error = Some(error.clone());
                effect.last_failure = Some(error);
                effect.next_attempt_at = Some(*next_attempt_at);
                effect.attempt = Some(*next_attempt);
                effect.attempt_id = None;
                effect.attempt_owner_id = None;
                effect.attempt_started_at = None;
                effect.start_to_close_deadline = None;
                effect.heartbeat_deadline = None;
                release_activation_id = effect.activation_id.clone();
                blocked_until = Some(*next_attempt_at);
            }
            FailEffectResult::Failed => {
                effect.status = EffectStatus::Failed;
                effect.error = Some(error.clone());
                effect.last_failure = Some(error);
                effect.attempt_id = None;
                effect.attempt_owner_id = None;
                effect.attempt_started_at = None;
                effect.start_to_close_deadline = None;
                effect.heartbeat_deadline = None;
                release_activation_id = effect.activation_id.clone();
                blocked_until = None;
            }
        }
        release_tasks_for_activation(state, &release_activation_id, blocked_until);
        save_engine(state)?;
        Ok(decision)
    }

    pub fn commit_checkpoint(
        &mut self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let state = &mut self.state;
        let key = instance_key(&input.workflow_id, &input.run_id);
        let current_sequence = {
            let Some(instance) = state.instances.get(&key) else {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: 0,
                    reason: Some("not_found".to_string()),
                    retryable: Some(true),
                    error: None,
                });
            };

            if instance.status != PersistedStatus::Running {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: instance.sequence,
                    reason: Some("not_running".to_string()),
                    retryable: Some(true),
                    error: None,
                });
            }

            if instance.sequence != input.expected_sequence {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: instance.sequence,
                    reason: Some("stale_sequence".to_string()),
                    retryable: Some(true),
                    error: None,
                });
            }

            instance.sequence
        };

        let signal_index = if let Some(signal_id) = &input.consume_signal_id {
            let index = state.signals.iter().position(|signal| {
                signal.signal_id == *signal_id
                    && signal.workflow_id == input.workflow_id
                    && signal.run_id == input.run_id
                    && signal.consumed_by_sequence.is_none()
            });
            if index.is_none() {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: current_sequence,
                    reason: Some("signal_not_consumable".to_string()),
                    retryable: Some(true),
                    error: None,
                });
            }
            index
        } else {
            None
        };

        let child_index = if let Some(child_record_id) = &input.consume_child_record_id {
            let index = state.children.iter().position(|record| {
                record.child_record_id == *child_record_id
                    && record.parent_workflow_id == input.workflow_id
                    && record.parent_run_id == input.run_id
                    && record.delivered_by_sequence.is_none()
            });
            if index.is_none() {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: current_sequence,
                    reason: Some("child_not_consumable".to_string()),
                    retryable: Some(true),
                    error: None,
                });
            }
            index
        } else {
            None
        };

        for start in &input.child_starts {
            let child_key = instance_key(&start.workflow_id, &start.run_id);
            if state.instances.contains_key(&child_key)
                && start.conflict_policy != ConflictPolicy::TerminateExisting
            {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: current_sequence,
                    reason: Some("existing_child_instance".to_string()),
                    retryable: Some(false),
                    error: Some(SerializedError {
                        name: None,
                        message: format!(
                            "child workflow instance already exists: {}/{}",
                            start.workflow_id, start.run_id
                        ),
                    }),
                });
            }
        }

        let mut checkpoint_effects = Vec::new();
        for effect in &input.effects {
            let effect_id = format!("effect-{}", state.next_effect_id);
            state.next_effect_id += 1;
            let idempotency_key = format!(
                "{}/{}/{}/{}",
                input.workflow_id,
                input.run_id,
                input.activation_id,
                effect.key()
            );
            checkpoint_effects.push(effect.to_record(
                effect_id,
                input.activation_id.clone(),
                idempotency_key,
                input.now,
            ));
        }

        let next_sequence = current_sequence + 1;
        let (parent, instance_status, output, error, cancel_reason, updated_instance) = {
            let instance = state
                .instances
                .get_mut(&key)
                .ok_or_else(|| WorkflowError::new("instance disappeared during commit"))?;

            instance.sequence = next_sequence;
            instance.workflow_version = input.workflow_version;
            instance.waits = input.waits;
            instance.updated_at = input.now;

            match input.next {
                InstanceStatusValue::Running { common, phase } => {
                    instance.status = PersistedStatus::Running;
                    instance.common = Some(common);
                    instance.phase = Some(phase);
                    instance.output = None;
                    instance.error = None;
                    instance.cancel_reason = None;
                }
                InstanceStatusValue::Completed { output } => {
                    instance.status = PersistedStatus::Completed;
                    instance.output = Some(output);
                    instance.common = None;
                    instance.phase = None;
                    instance.error = None;
                    instance.cancel_reason = None;
                }
                InstanceStatusValue::Canceled { reason } => {
                    instance.status = PersistedStatus::Canceled;
                    instance.cancel_reason = Some(reason);
                    instance.common = None;
                    instance.phase = None;
                    instance.output = None;
                    instance.error = None;
                }
                InstanceStatusValue::Failed { error } => {
                    instance.status = PersistedStatus::Failed;
                    instance.error = Some(error);
                    instance.common = None;
                    instance.phase = None;
                    instance.output = None;
                    instance.cancel_reason = None;
                }
            }

            if !input.effects.is_empty() {
                instance
                    .effects
                    .retain(|effect| effect.activation_id != input.activation_id);
                instance.effects.extend(checkpoint_effects);
            }

            (
                instance.parent.clone(),
                instance.status.clone(),
                instance.output.clone(),
                instance.error.clone(),
                instance.cancel_reason.clone(),
                instance.clone(),
            )
        };

        if let Some(index) = signal_index {
            state.signals[index].consumed_by_sequence = Some(next_sequence);
        }

        if let Some(index) = child_index {
            state.children[index].delivered_by_sequence = Some(next_sequence);
        }

        for start in input.child_starts {
            let child_record_id = format!("child-{}", state.next_child_id);
            state.next_child_id += 1;
            let child_key = instance_key(&start.workflow_id, &start.run_id);
            if start.conflict_policy == ConflictPolicy::TerminateExisting {
                delete_instance_records(state, &start.workflow_id, &start.run_id);
            }
            let child_instance = PersistedInstance {
                workflow_name: start.workflow_name.clone(),
                workflow_version: start.workflow_version,
                workflow_id: start.workflow_id.clone(),
                run_id: start.run_id.clone(),
                partition_shard: start.partition_shard,
                sequence: 0,
                status: PersistedStatus::Running,
                common: Some(start.common),
                phase: Some(start.phase),
                output: None,
                error: None,
                cancel_reason: None,
                waits: start.waits,
                effects: Vec::new(),
                created_at: input.now,
                updated_at: input.now,
                parent: Some(ParentLink {
                    workflow_id: input.workflow_id.clone(),
                    run_id: input.run_id.clone(),
                    child_record_id: child_record_id.clone(),
                }),
            };
            state.instances.insert(child_key, child_instance.clone());
            replace_tasks_for_instance(state, &child_instance);
            state.children.push(ChildRecord {
                child_record_id,
                parent_workflow_id: input.workflow_id.clone(),
                parent_run_id: input.run_id.clone(),
                activation_id: input.activation_id.clone(),
                key: start.key,
                workflow_name: start.workflow_name,
                workflow_version: start.workflow_version,
                workflow_id: start.workflow_id,
                run_id: start.run_id,
                status: ChildStatus::Started,
                parent_close_policy: start.parent_close_policy,
                completed_at: None,
                output: None,
                error: None,
                delivered_by_sequence: None,
            });
        }

        if let Some(parent) = parent {
            if instance_status != PersistedStatus::Running {
                if let Some(child_record) = state
                    .children
                    .iter_mut()
                    .find(|record| record.child_record_id == parent.child_record_id)
                {
                    if child_record.status == ChildStatus::Started {
                        child_record.completed_at = Some(input.now);
                        match instance_status {
                            PersistedStatus::Completed => {
                                child_record.status = ChildStatus::Completed;
                                child_record.output = output;
                            }
                            PersistedStatus::Canceled => {
                                child_record.status = ChildStatus::Failed;
                                child_record.error = Some(SerializedError {
                                    name: None,
                                    message: cancel_reason
                                        .unwrap_or_else(|| "child canceled".to_string()),
                                });
                            }
                            PersistedStatus::Failed => {
                                child_record.status = ChildStatus::Failed;
                                child_record.error = error.or(Some(SerializedError {
                                    name: None,
                                    message: "child failed".to_string(),
                                }));
                            }
                            PersistedStatus::Running => {}
                        }
                    }
                }
                let parent_key = instance_key(&parent.workflow_id, &parent.run_id);
                if let Some(parent_instance) = state.instances.get(&parent_key).cloned() {
                    refresh_child_tasks_for_instance(state, &parent_instance);
                }
            }
        }

        if matches!(
            instance_status,
            PersistedStatus::Canceled | PersistedStatus::Failed
        ) {
            apply_parent_close_policy(
                state,
                &input.workflow_id,
                &input.run_id,
                input.now,
                next_sequence,
            );
        }

        replace_tasks_for_instance(state, &updated_instance);
        state
            .completed_activation_claims
            .push(CompletedActivationClaim {
                activation_id: input.activation_id.clone(),
                workflow_id: input.workflow_id.clone(),
                run_id: input.run_id.clone(),
                sequence: current_sequence,
                kind: MemoryTaskKind::Event,
                owner_id: None,
                completed_by_sequence: next_sequence,
            });

        save_engine(state)?;
        Ok(CommitCheckpointResult {
            ok: true,
            sequence: next_sequence,
            reason: None,
            retryable: None,
            error: None,
        })
    }

    fn commit_activations(
        &mut self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let mut results = Vec::with_capacity(inputs.len());
        for input in inputs {
            results.push(self.commit_checkpoint(input)?);
        }
        Ok(CommitActivationsResult { results })
    }

    fn record_activation_failures(
        &mut self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }

        let state = &mut self.state;
        for input in inputs {
            let key = instance_key(&input.workflow_id, &input.run_id);
            if !state.instances.contains_key(&key) {
                return Err(WorkflowError::new(format!(
                    "unknown workflow instance: {}/{}",
                    input.workflow_id, input.run_id
                )));
            }

            let mutation_keys = input
                .effects
                .iter()
                .map(|effect| effect.key().to_string())
                .collect::<std::collections::HashSet<_>>();
            let mut records = Vec::with_capacity(input.effects.len());
            for effect in input.effects {
                let effect_id = format!("effect-{}", state.next_effect_id);
                state.next_effect_id += 1;
                let idempotency_key = format!(
                    "{}/{}/{}/{}",
                    input.workflow_id,
                    input.run_id,
                    input.activation_id,
                    effect.key()
                );
                records.push(effect.to_record(
                    effect_id,
                    input.activation_id.clone(),
                    idempotency_key,
                    input.now,
                ));
            }

            if !records.is_empty() {
                let instance = state.instances.get_mut(&key).ok_or_else(|| {
                    WorkflowError::new("instance disappeared while recording activation failure")
                })?;
                instance.effects.retain(|effect| {
                    effect.activation_id != input.activation_id
                        || !mutation_keys.contains(&effect.key)
                });
                instance.effects.extend(records);
            }

            if input.release_activation {
                release_tasks_for_activation(state, &input.activation_id, None);
            }
        }

        save_engine(state)
    }

    fn claim_shard_sync(
        &mut self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        let state = &mut self.state;
        if let Some(existing) = state.shard_leases.get(&input.shard_id) {
            if existing.owner_id != input.owner_id && existing.lease_until > input.now {
                return Ok(None);
            }
        }
        let epoch = state
            .shard_leases
            .get(&input.shard_id)
            .map(|lease| {
                if lease.owner_id == input.owner_id && lease.lease_until > input.now {
                    lease.lease_epoch
                } else {
                    lease.lease_epoch + 1
                }
            })
            .unwrap_or(1);
        let lease = ShardLease {
            shard_id: input.shard_id,
            owner_id: input.owner_id,
            lease_until: input.now + chrono::Duration::milliseconds(input.lease_ms as i64),
            lease_epoch: epoch,
        };
        state.shard_leases.insert(input.shard_id, lease.clone());
        save_engine(state)?;
        Ok(Some(lease))
    }

    fn heartbeat_shard_sync(
        &mut self,
        shard_id: u32,
        owner_id: &str,
        now: DateTime<Utc>,
        lease_ms: u64,
    ) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        let lease = state
            .shard_leases
            .get_mut(&shard_id)
            .ok_or_else(|| WorkflowError::new(format!("lost shard lease: {shard_id}")))?;
        if lease.owner_id != owner_id || lease.lease_until < now {
            return Err(WorkflowError::new(format!("lost shard lease: {shard_id}")));
        }
        lease.lease_until = now + chrono::Duration::milliseconds(lease_ms as i64);
        save_engine(state)
    }

    fn release_shard_sync(&mut self, shard_id: u32, owner_id: &str) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        if let Some(lease) = state.shard_leases.get_mut(&shard_id) {
            if lease.owner_id == owner_id {
                lease.lease_until = DateTime::<Utc>::from(std::time::UNIX_EPOCH);
            }
        }
        save_engine(state)
    }

    fn release_activation_sync(
        &mut self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        let mut released_sequences = Vec::new();
        for task in state.tasks.values_mut() {
            if task.activation_id == activation_id
                && task.claim_owner_id.as_deref() == Some(worker_id)
            {
                task.claim_owner_id = None;
                task.claim_epoch = None;
                task.lease_until = None;
                released_sequences.push(sequence_key_for_task(task));
            }
        }
        for sequence in released_sequences {
            state.claimed_sequence_epochs.remove(&sequence);
        }
        save_engine(state)
    }

    fn assert_activation_claim_sync(
        &self,
        activation_id: &str,
        owner_id: Option<&str>,
        lease_epoch: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let state = &self.state;
        let Some(task) = state
            .tasks
            .values()
            .find(|task| task.activation_id == activation_id)
        else {
            return Ok(());
        };
        if task.claim_owner_id.as_deref() != owner_id
            || task.claim_epoch != lease_epoch
            || task.lease_until.is_none_or(|lease_until| lease_until < now)
        {
            return Err(WorkflowError::new(format!(
                "lost activation lease: {activation_id}"
            )));
        }
        Ok(())
    }

    fn claim_tasks_for_session(
        &mut self,
        session: &OpenShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        if input.limit == 0 {
            return Err(WorkflowError::new("limit must be positive"));
        }
        let Some(owner_id) = &session.owner_id else {
            return Err(WorkflowError::new(format!(
                "shard {} is not opened with an owner",
                session.shard_id
            )));
        };
        let state = &mut self.state;
        let lease = state
            .shard_leases
            .get(&session.shard_id)
            .ok_or_else(|| WorkflowError::new(format!("lost shard lease: {}", session.shard_id)))?;
        if lease.owner_id != *owner_id || lease.lease_until < input.now {
            return Err(WorkflowError::new(format!(
                "lost shard lease: {}",
                session.shard_id
            )));
        }
        if let Some(epoch) = session.lease_epoch {
            if epoch != lease.lease_epoch {
                return Err(WorkflowError::new(format!(
                    "lost shard lease: {}",
                    session.shard_id
                )));
            }
        }
        let lease_epoch = lease.lease_epoch;

        expire_activity_timeouts(
            state,
            input.now,
            None,
            Some(owner_id),
            Some(session.shard_id),
        );
        refresh_migration_tasks(state, session.shard_id, input.now, &input.workflows);

        let candidates = state
            .task_order
            .iter()
            .filter_map(|(_, task_id)| state.tasks.get(task_id))
            .filter(|task| task.partition_shard == session.shard_id)
            .filter(|task| task.ready_at <= input.now)
            .filter(|task| {
                task.blocked_until
                    .map_or(true, |blocked| blocked <= input.now)
            })
            .filter(|task| {
                input
                    .workflows
                    .get(&task.workflow_name)
                    .is_some_and(|version| task.workflow_version <= *version)
            })
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();

        let mut claims = Vec::new();
        let mut claimed_sequences = std::collections::HashSet::new();
        for candidate_id in candidates {
            if claims.len() >= input.limit {
                break;
            }
            let Some(candidate) = state.tasks.get(&candidate_id).cloned() else {
                continue;
            };
            let sequence_key = sequence_key_for_task(&candidate);
            if claimed_sequences.contains(&sequence_key) {
                continue;
            }
            if let Some(existing_epoch) = state.claimed_sequence_epochs.get(&sequence_key).copied()
            {
                if existing_epoch == lease_epoch {
                    continue;
                }
                state.claimed_sequence_epochs.remove(&sequence_key);
            }
            let has_unexpired_claim = state.tasks.values().any(|task| {
                task.workflow_id == candidate.workflow_id
                    && task.run_id == candidate.run_id
                    && task.sequence == candidate.sequence
                    && task
                        .lease_until
                        .is_some_and(|lease_until| lease_until > input.now)
                    && task.claim_owner_id.is_some()
            });
            if has_unexpired_claim {
                continue;
            }

            let Some(instance) = state
                .instances
                .get(&instance_key(&candidate.workflow_id, &candidate.run_id))
                .cloned()
            else {
                delete_task(state, &candidate.task_id);
                continue;
            };
            if instance.status != PersistedStatus::Running
                || instance.sequence != candidate.sequence
            {
                delete_task(state, &candidate.task_id);
                continue;
            }

            if let Some(task) = state.tasks.get_mut(&candidate.task_id) {
                task.claim_owner_id = Some(owner_id.clone());
                task.claim_epoch = Some(lease_epoch);
                task.lease_until =
                    Some(input.now + chrono::Duration::milliseconds(input.lease_ms as i64));
            }
            state
                .claimed_sequence_epochs
                .insert(sequence_key.clone(), lease_epoch);
            claimed_sequences.insert(sequence_key);

            let lease_until = input.now + chrono::Duration::milliseconds(input.lease_ms as i64);
            let activation = match candidate.kind {
                MemoryTaskKind::Migration => ClaimedActivation::Migration {
                    activation_id: candidate.activation_id.clone(),
                    workflow_name: candidate.workflow_name.clone(),
                    workflow_id: candidate.workflow_id.clone(),
                    run_id: candidate.run_id.clone(),
                    sequence: candidate.sequence,
                    activation_time: input.now,
                    lease_until,
                },
                MemoryTaskKind::Run => ClaimedActivation::Run {
                    activation_id: candidate.activation_id.clone(),
                    workflow_name: candidate.workflow_name.clone(),
                    workflow_id: candidate.workflow_id.clone(),
                    run_id: candidate.run_id.clone(),
                    sequence: candidate.sequence,
                    activation_time: input.now,
                    lease_until,
                },
                MemoryTaskKind::Event => ClaimedActivation::Event {
                    activation_id: candidate.activation_id.clone(),
                    workflow_name: candidate.workflow_name.clone(),
                    workflow_id: candidate.workflow_id.clone(),
                    run_id: candidate.run_id.clone(),
                    sequence: candidate.sequence,
                    activation_time: input.now,
                    wait_name: candidate.wait_name.clone().unwrap_or_default(),
                    event: candidate
                        .event
                        .clone()
                        .ok_or_else(|| WorkflowError::new("event task missing event"))?,
                    lease_until,
                },
            };
            let effects = instance
                .effects
                .iter()
                .filter(|effect| effect.activation_id == candidate.activation_id)
                .cloned()
                .collect();
            claims.push(ClaimedActivationWithInstance {
                activation,
                instance,
                effects,
                lease: ActivationClaimLease::Shard {
                    shard_id: session.shard_id,
                    epoch: lease_epoch,
                },
            });
        }

        let next_wake_at = state
            .tasks
            .values()
            .filter(|task| task.partition_shard == session.shard_id)
            .filter(|task| {
                input
                    .workflows
                    .get(&task.workflow_name)
                    .is_some_and(|version| task.workflow_version <= *version)
            })
            .filter_map(|task| {
                if let Some(blocked) = task.blocked_until {
                    if blocked > input.now {
                        return Some(blocked);
                    }
                }
                (task.ready_at > input.now).then_some(task.ready_at)
            })
            .min();

        save_engine(state)?;
        Ok(ClaimShardTasksResult {
            claims,
            next_wake_at,
        })
    }

    fn put_effect_record(
        &mut self,
        workflow_id: &str,
        run_id: &str,
        effect: EffectRecord,
    ) -> Result<(), WorkflowError> {
        let state = &mut self.state;
        if let Some(number) = effect
            .effect_id
            .strip_prefix("effect-")
            .and_then(|suffix| suffix.parse::<u64>().ok())
        {
            state.next_effect_id = state.next_effect_id.max(number + 1);
        }
        let instance = require_instance_mut(state, workflow_id, run_id)?;
        if let Some(existing) = instance
            .effects
            .iter_mut()
            .find(|existing| existing.effect_id == effect.effect_id)
        {
            *existing = effect;
        } else {
            instance.effects.push(effect);
        }
        save_engine(state)
    }
}

fn save_engine(_state: &Store) -> Result<(), WorkflowError> {
    Ok(())
}

#[derive(Clone)]
struct ShardActorHandle {
    sender: mpsc::Sender<ShardCommand>,
}

enum ShardCommand {
    Execute {
        operation: EngineOperation,
        response: oneshot::Sender<Result<EngineOutput, WorkflowError>>,
    },
    Snapshot {
        response: oneshot::Sender<Store>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

enum EngineOperation {
    ClaimShard(ClaimShardInput),
    ClaimTasks {
        session: OpenShardInput,
        input: ClaimShardTasksInput,
    },
    ClaimShardAndTasks {
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    },
    CreateInstance(CreateInstanceInput),
    CreateChildInstance(CreateChildInstanceInput),
    LoadInstance {
        ref_: InstanceRef,
        options: LoadInstanceOptions,
    },
    AppendSignal(AppendSignalInput),
    CancelChild(CancelChildInput),
    ReserveEffect(ReserveEffectInput),
    HeartbeatEffect(HeartbeatEffectInput),
    CompleteEffect(CompleteEffectInput),
    FailEffect(FailEffectInput),
    CommitCheckpoint {
        session: Option<OpenShardInput>,
        input: CommitCheckpointInput,
    },
    CommitActivations {
        session: Option<OpenShardInput>,
        inputs: Vec<CommitCheckpointInput>,
    },
    RecordActivationFailures(Vec<RecordActivationFailureInput>),
    ReleaseActivation {
        activation_id: String,
        worker_id: String,
    },
    HeartbeatShard {
        shard_id: u32,
        owner_id: String,
        now: DateTime<Utc>,
        lease_ms: u64,
    },
    ReleaseShard {
        shard_id: u32,
        owner_id: String,
    },
}

enum EngineOutput {
    OptionalShardLease(Option<ShardLease>),
    ClaimTasks(ClaimShardTasksResult),
    ClaimShardAndTasks(Option<(ShardLease, ClaimShardTasksResult)>),
    InstanceCreated {
        ref_: InstanceRef,
        instance: PersistedInstance,
    },
    ChildCreated {
        handle: ChildHandleValue,
        instance: PersistedInstance,
    },
    OptionalInstance(Option<PersistedInstance>),
    Signal(SignalRecord),
    EffectReservation(EffectReservation),
    FailEffect(FailEffectResult),
    CommitCheckpoint {
        result: CommitCheckpointResult,
        instances: Vec<PersistedInstance>,
        invalidate_cache: bool,
    },
    CommitActivations {
        result: CommitActivationsResult,
        instances: Vec<PersistedInstance>,
        invalidate_cache: bool,
    },
    Unit,
}

impl EngineOperation {
    fn apply(self, engine: &mut ShardEngine) -> Result<EngineOutput, WorkflowError> {
        match self {
            EngineOperation::ClaimShard(input) => engine
                .claim_shard_sync(input)
                .map(EngineOutput::OptionalShardLease),
            EngineOperation::ClaimTasks { session, input } => engine
                .claim_tasks_for_session(&session, input)
                .map(EngineOutput::ClaimTasks),
            EngineOperation::ClaimShardAndTasks { claim, input } => {
                let Some(lease) = engine.claim_shard_sync(claim)? else {
                    return Ok(EngineOutput::ClaimShardAndTasks(None));
                };
                let tasks = engine.claim_tasks_for_session(
                    &OpenShardInput {
                        shard_id: lease.shard_id,
                        owner_id: Some(lease.owner_id.clone()),
                        lease_epoch: Some(lease.lease_epoch),
                    },
                    input,
                )?;
                Ok(EngineOutput::ClaimShardAndTasks(Some((lease, tasks))))
            }
            EngineOperation::CreateInstance(input) => {
                let ref_ = engine.create_instance(input)?;
                let instance = engine
                    .load_instance(&ref_)?
                    .ok_or_else(|| WorkflowError::new("created instance missing from shard"))?;
                Ok(EngineOutput::InstanceCreated { ref_, instance })
            }
            EngineOperation::CreateChildInstance(input) => {
                let handle = engine.create_child_instance(input)?;
                let ref_ = InstanceRef::new(handle.workflow_id.clone(), handle.run_id.clone());
                let instance = engine.load_instance(&ref_)?.ok_or_else(|| {
                    WorkflowError::new("created child instance missing from shard")
                })?;
                Ok(EngineOutput::ChildCreated { handle, instance })
            }
            EngineOperation::LoadInstance { ref_, options } => {
                let mut instance = engine.load_instance(&ref_)?;
                if !options.include_effects {
                    if let Some(instance) = &mut instance {
                        instance.effects.clear();
                    }
                }
                Ok(EngineOutput::OptionalInstance(instance))
            }
            EngineOperation::AppendSignal(input) => engine
                .append_signal(
                    input.workflow_id,
                    input.run_id,
                    input.r#type,
                    input.payload,
                    input.received_at,
                )
                .map(EngineOutput::Signal),
            EngineOperation::CancelChild(input) => {
                engine.cancel_child(input)?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::ReserveEffect(input) => engine
                .get_or_reserve_effect(
                    &input.workflow_id,
                    &input.run_id,
                    &input.activation_id,
                    &input.key,
                    &input.worker_id,
                    input.now,
                    input.options,
                    input.max_attempts,
                )
                .map(EngineOutput::EffectReservation),
            EngineOperation::HeartbeatEffect(input) => {
                engine.heartbeat_effect(
                    &input.workflow_id,
                    &input.run_id,
                    &input.effect_id,
                    &input.attempt_id,
                    &input.worker_id,
                    input.now,
                    input.details,
                )?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::CompleteEffect(input) => {
                engine.complete_effect(
                    &input.workflow_id,
                    &input.run_id,
                    &input.effect_id,
                    &input.attempt_id,
                    &input.worker_id,
                    input.now,
                    input.result,
                )?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::FailEffect(input) => engine
                .fail_effect(
                    &input.workflow_id,
                    &input.run_id,
                    &input.effect_id,
                    &input.attempt_id,
                    &input.worker_id,
                    input.error,
                    input.now,
                    input.retryable,
                )
                .map(EngineOutput::FailEffect),
            EngineOperation::CommitCheckpoint { session, input } => {
                if let Some(session) = session {
                    if let Err(error) = engine.assert_activation_claim_sync(
                        &input.activation_id,
                        session.owner_id.as_deref(),
                        session.lease_epoch,
                        input.now,
                    ) {
                        return Ok(EngineOutput::CommitCheckpoint {
                            result: CommitCheckpointResult {
                                ok: false,
                                sequence: input.expected_sequence,
                                reason: Some("lost_activation_lease".to_string()),
                                retryable: Some(true),
                                error: Some(SerializedError {
                                    name: Some("LostActivationLease".to_string()),
                                    message: error.message,
                                }),
                            },
                            instances: Vec::new(),
                            invalidate_cache: false,
                        });
                    }
                }
                let refs = commit_result_refs(&input);
                let invalidate_cache = commit_invalidates_cache(&input);
                let result = engine.commit_checkpoint(input)?;
                let instances = if result.ok {
                    load_instances_for_cache(engine, refs)?
                } else {
                    Vec::new()
                };
                Ok(EngineOutput::CommitCheckpoint {
                    result,
                    instances,
                    invalidate_cache,
                })
            }
            EngineOperation::CommitActivations { session, inputs } => {
                if let Some(session) = session {
                    let mut results = Vec::with_capacity(inputs.len());
                    let mut instances = Vec::new();
                    let mut invalidate_cache = false;
                    for input in inputs {
                        let output = EngineOperation::CommitCheckpoint {
                            session: Some(session.clone()),
                            input,
                        }
                        .apply(engine)?;
                        let EngineOutput::CommitCheckpoint {
                            result,
                            instances: committed_instances,
                            invalidate_cache: commit_invalidated_cache,
                        } = output
                        else {
                            unreachable!("commit checkpoint operation returned wrong output");
                        };
                        results.push(result);
                        instances.extend(committed_instances);
                        invalidate_cache |= commit_invalidated_cache;
                    }
                    Ok(EngineOutput::CommitActivations {
                        result: CommitActivationsResult { results },
                        instances,
                        invalidate_cache,
                    })
                } else {
                    let mut results = Vec::with_capacity(inputs.len());
                    let mut instances = Vec::new();
                    let mut invalidate_cache = false;
                    for input in inputs {
                        let output = EngineOperation::CommitCheckpoint {
                            session: None,
                            input,
                        }
                        .apply(engine)?;
                        let EngineOutput::CommitCheckpoint {
                            result,
                            instances: committed_instances,
                            invalidate_cache: commit_invalidated_cache,
                        } = output
                        else {
                            unreachable!("commit checkpoint operation returned wrong output");
                        };
                        results.push(result);
                        instances.extend(committed_instances);
                        invalidate_cache |= commit_invalidated_cache;
                    }
                    Ok(EngineOutput::CommitActivations {
                        result: CommitActivationsResult { results },
                        instances,
                        invalidate_cache,
                    })
                }
            }
            EngineOperation::RecordActivationFailures(inputs) => {
                engine.record_activation_failures(inputs)?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::ReleaseActivation {
                activation_id,
                worker_id,
            } => {
                engine.release_activation_sync(&activation_id, &worker_id)?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::HeartbeatShard {
                shard_id,
                owner_id,
                now,
                lease_ms,
            } => {
                engine.heartbeat_shard_sync(shard_id, &owner_id, now, lease_ms)?;
                Ok(EngineOutput::Unit)
            }
            EngineOperation::ReleaseShard { shard_id, owner_id } => {
                engine.release_shard_sync(shard_id, &owner_id)?;
                Ok(EngineOutput::Unit)
            }
        }
    }
}

fn commit_result_refs(input: &CommitCheckpointInput) -> Vec<InstanceRef> {
    let mut refs = Vec::with_capacity(input.child_starts.len() + 1);
    refs.push(InstanceRef::new(
        input.workflow_id.clone(),
        input.run_id.clone(),
    ));
    refs.extend(
        input
            .child_starts
            .iter()
            .map(|child| InstanceRef::new(child.workflow_id.clone(), child.run_id.clone())),
    );
    refs
}

fn commit_invalidates_cache(input: &CommitCheckpointInput) -> bool {
    matches!(
        &input.next,
        InstanceStatusValue::Canceled { .. } | InstanceStatusValue::Failed { .. }
    )
}

fn load_instances_for_cache(
    engine: &ShardEngine,
    refs: Vec<InstanceRef>,
) -> Result<Vec<PersistedInstance>, WorkflowError> {
    let mut instances = Vec::with_capacity(refs.len());
    for ref_ in refs {
        if let Some(instance) = engine.load_instance(&ref_)? {
            instances.push(instance);
        }
    }
    Ok(instances)
}

async fn run_shard_actor(mut engine: ShardEngine, mut receiver: mpsc::Receiver<ShardCommand>) {
    while let Some(command) = receiver.recv().await {
        match command {
            ShardCommand::Execute {
                operation,
                response,
            } => {
                let _ = response.send(operation.apply(&mut engine));
            }
            ShardCommand::Snapshot { response } => {
                let _ = response.send(engine.snapshot_store());
            }
            ShardCommand::Shutdown { response } => {
                let _ = response.send(());
                break;
            }
        }
    }
}

#[derive(Clone)]
struct ShardRouter {
    inner: Arc<ShardRouterInner>,
}

struct ShardRouterInner {
    actors: Mutex<HashMap<u32, ShardActorHandle>>,
    initial_shards: Mutex<HashMap<u32, Store>>,
    directory: Mutex<HashMap<String, u32>>,
    instance_cache: Mutex<HashMap<String, PersistedInstance>>,
}

impl ShardRouter {
    fn in_memory() -> Self {
        Self::from_store(Store::default())
    }

    fn from_store(store: Store) -> Self {
        let directory = store
            .instances
            .values()
            .map(|instance| {
                (
                    instance_key(&instance.workflow_id, &instance.run_id),
                    instance.partition_shard,
                )
            })
            .collect();
        let instance_cache = store.instances.clone();
        Self {
            inner: Arc::new(ShardRouterInner {
                actors: Mutex::new(HashMap::new()),
                initial_shards: Mutex::new(split_store_by_shard(store)),
                directory: Mutex::new(directory),
                instance_cache: Mutex::new(instance_cache),
            }),
        }
    }

    async fn execute(
        &self,
        shard_id: u32,
        operation: EngineOperation,
    ) -> Result<EngineOutput, WorkflowError> {
        let handle = self.ensure_actor(shard_id)?;
        execute_on_actor(shard_id, &handle, operation).await
    }

    fn ensure_actor(&self, shard_id: u32) -> Result<ShardActorHandle, WorkflowError> {
        if let Some(handle) = self
            .inner
            .actors
            .lock()
            .map_err(|_| WorkflowError::new("shard actor registry lock poisoned"))?
            .get(&shard_id)
            .cloned()
        {
            return Ok(handle);
        }

        let store = self
            .inner
            .initial_shards
            .lock()
            .map_err(|_| WorkflowError::new("shard initial state lock poisoned"))?
            .remove(&shard_id)
            .unwrap_or_else(Store::default);
        let (sender, receiver) = mpsc::channel(256);
        tokio::spawn(run_shard_actor(ShardEngine::from_store(store), receiver));
        let handle = ShardActorHandle { sender };
        self.inner
            .actors
            .lock()
            .map_err(|_| WorkflowError::new("shard actor registry lock poisoned"))?
            .insert(shard_id, handle.clone());
        Ok(handle)
    }
}

async fn execute_on_actor(
    shard_id: u32,
    handle: &ShardActorHandle,
    operation: EngineOperation,
) -> Result<EngineOutput, WorkflowError> {
    let (response, receiver) = oneshot::channel();
    let command = ShardCommand::Execute {
        operation,
        response,
    };
    match handle.sender.try_send(command) {
        Ok(()) => {}
        Err(TrySendError::Full(command)) => handle
            .sender
            .send(command)
            .await
            .map_err(|_| WorkflowError::new(format!("shard actor {shard_id} is closed")))?,
        Err(TrySendError::Closed(_)) => {
            return Err(WorkflowError::new(format!(
                "shard actor {shard_id} is closed"
            )));
        }
    }
    receiver
        .await
        .map_err(|_| WorkflowError::new(format!("shard actor {shard_id} dropped response")))?
}

impl ShardRouter {
    fn directory_get(&self, ref_: &InstanceRef) -> Result<Option<u32>, WorkflowError> {
        Ok(self
            .inner
            .directory
            .lock()
            .map_err(|_| WorkflowError::new("instance directory lock poisoned"))?
            .get(&instance_key_ref(ref_))
            .copied())
    }

    fn directory_insert(&self, ref_: &InstanceRef, shard_id: u32) -> Result<(), WorkflowError> {
        self.inner
            .directory
            .lock()
            .map_err(|_| WorkflowError::new("instance directory lock poisoned"))?
            .insert(instance_key_ref(ref_), shard_id);
        Ok(())
    }

    fn cache_get(
        &self,
        ref_: &InstanceRef,
        options: &LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let mut instance = self
            .inner
            .instance_cache
            .lock()
            .map_err(|_| WorkflowError::new("instance cache lock poisoned"))?
            .get(&instance_key_ref(ref_))
            .cloned();
        if !options.include_effects {
            if let Some(instance) = &mut instance {
                instance.effects.clear();
            }
        }
        Ok(instance)
    }

    fn cache_insert(&self, instance: PersistedInstance) -> Result<(), WorkflowError> {
        self.directory_insert(
            &InstanceRef::new(instance.workflow_id.clone(), instance.run_id.clone()),
            instance.partition_shard,
        )?;
        self.inner
            .instance_cache
            .lock()
            .map_err(|_| WorkflowError::new("instance cache lock poisoned"))?
            .insert(
                instance_key(&instance.workflow_id, &instance.run_id),
                instance,
            );
        Ok(())
    }

    fn cache_insert_many(&self, instances: Vec<PersistedInstance>) -> Result<(), WorkflowError> {
        let mut directory = self
            .inner
            .directory
            .lock()
            .map_err(|_| WorkflowError::new("instance directory lock poisoned"))?;
        let mut cache = self
            .inner
            .instance_cache
            .lock()
            .map_err(|_| WorkflowError::new("instance cache lock poisoned"))?;
        for instance in instances {
            let key = instance_key(&instance.workflow_id, &instance.run_id);
            directory.insert(key.clone(), instance.partition_shard);
            cache.insert(key, instance);
        }
        Ok(())
    }

    fn cache_remove(&self, ref_: &InstanceRef) -> Result<(), WorkflowError> {
        self.inner
            .instance_cache
            .lock()
            .map_err(|_| WorkflowError::new("instance cache lock poisoned"))?
            .remove(&instance_key_ref(ref_));
        Ok(())
    }

    fn cache_clear(&self) -> Result<(), WorkflowError> {
        self.inner
            .instance_cache
            .lock()
            .map_err(|_| WorkflowError::new("instance cache lock poisoned"))?
            .clear();
        Ok(())
    }

    fn known_shard_ids(&self) -> Result<Vec<u32>, WorkflowError> {
        let mut ids = self
            .inner
            .actors
            .lock()
            .map_err(|_| WorkflowError::new("shard actor registry lock poisoned"))?
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        ids.extend(
            self.inner
                .initial_shards
                .lock()
                .map_err(|_| WorkflowError::new("shard initial state lock poisoned"))?
                .keys()
                .copied(),
        );
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }

    async fn shard_for_ref(&self, ref_: &InstanceRef) -> Result<u32, WorkflowError> {
        if let Some(shard_id) = self.directory_get(ref_)? {
            return Ok(shard_id);
        }
        for shard_id in self.known_shard_ids()? {
            let output = self
                .execute(
                    shard_id,
                    EngineOperation::LoadInstance {
                        ref_: ref_.clone(),
                        options: LoadInstanceOptions {
                            include_effects: true,
                        },
                    },
                )
                .await?;
            let EngineOutput::OptionalInstance(instance) = output else {
                unreachable!("load instance operation returned wrong output");
            };
            if let Some(instance) = instance {
                self.directory_insert(ref_, instance.partition_shard)?;
                return Ok(instance.partition_shard);
            }
        }
        Err(WorkflowError::new(format!(
            "unknown workflow instance for shard routing: {}/{}",
            ref_.workflow_id, ref_.run_id
        )))
    }

    async fn load_instance_internal(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let include_effects = options.include_effects;
        if let Some(instance) = self.cache_get(ref_, &options)? {
            return Ok(Some(instance));
        }
        let shard_id = match self.directory_get(ref_)? {
            Some(shard_id) => shard_id,
            None => {
                for shard_id in self.known_shard_ids()? {
                    let output = self
                        .execute(
                            shard_id,
                            EngineOperation::LoadInstance {
                                ref_: ref_.clone(),
                                options: options.clone(),
                            },
                        )
                        .await?;
                    let EngineOutput::OptionalInstance(instance) = output else {
                        unreachable!("load instance operation returned wrong output");
                    };
                    if let Some(instance) = instance {
                        self.directory_insert(ref_, instance.partition_shard)?;
                        if include_effects {
                            self.cache_insert(instance.clone())?;
                        }
                        let mut instance = instance;
                        if !include_effects {
                            instance.effects.clear();
                        }
                        return Ok(Some(instance));
                    }
                }
                return Ok(None);
            }
        };
        let output = self
            .execute(
                shard_id,
                EngineOperation::LoadInstance {
                    ref_: ref_.clone(),
                    options,
                },
            )
            .await?;
        let EngineOutput::OptionalInstance(instance) = output else {
            unreachable!("load instance operation returned wrong output");
        };
        if include_effects {
            if let Some(instance) = &instance {
                self.cache_insert(instance.clone())?;
            }
        }
        Ok(instance)
    }

    async fn snapshot_store(&self) -> Result<Store, WorkflowError> {
        let mut stores = Vec::new();
        for shard_id in self.known_shard_ids()? {
            let handle = self.ensure_actor(shard_id)?;
            let (response, receiver) = oneshot::channel();
            handle
                .sender
                .send(ShardCommand::Snapshot { response })
                .await
                .map_err(|_| WorkflowError::new(format!("shard actor {shard_id} is closed")))?;
            stores.push(
                receiver
                    .await
                    .map_err(|_| WorkflowError::new("shard actor dropped snapshot response"))?,
            );
        }
        Ok(merge_shard_stores(stores))
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        let handles = {
            let mut actors = self
                .inner
                .actors
                .lock()
                .map_err(|_| WorkflowError::new("shard actor registry lock poisoned"))?;
            actors.drain().map(|(_, handle)| handle).collect::<Vec<_>>()
        };
        for handle in handles {
            let (response, receiver) = oneshot::channel();
            let _ = handle
                .sender
                .send(ShardCommand::Shutdown { response })
                .await;
            let _ = receiver.await;
        }
        Ok(())
    }
}

fn split_store_by_shard(store: Store) -> HashMap<u32, Store> {
    let mut by_shard: HashMap<u32, Store> = HashMap::new();
    let mut directory = HashMap::new();
    for instance in store.instances.values() {
        directory.insert(
            instance_key(&instance.workflow_id, &instance.run_id),
            instance.partition_shard,
        );
    }
    for (key, instance) in store.instances {
        by_shard
            .entry(instance.partition_shard)
            .or_insert_with(Store::default)
            .instances
            .insert(key, instance);
    }
    for signal in store.signals {
        if let Some(shard_id) = directory.get(&instance_key(&signal.workflow_id, &signal.run_id)) {
            by_shard
                .entry(*shard_id)
                .or_insert_with(Store::default)
                .signals
                .push(signal);
        }
    }
    for child in store.children {
        if let Some(shard_id) = directory.get(&instance_key(
            &child.parent_workflow_id,
            &child.parent_run_id,
        )) {
            by_shard
                .entry(*shard_id)
                .or_insert_with(Store::default)
                .children
                .push(child);
        }
    }
    for (task_id, task) in store.tasks {
        let shard = by_shard
            .entry(task.partition_shard)
            .or_insert_with(Store::default);
        shard
            .task_order
            .insert((task.sort_key.clone(), task_id.clone()));
        shard.tasks.insert(task_id, task);
    }
    for claim in store.completed_activation_claims {
        if let Some(shard_id) = directory.get(&instance_key(&claim.workflow_id, &claim.run_id)) {
            by_shard
                .entry(*shard_id)
                .or_insert_with(Store::default)
                .completed_activation_claims
                .push(claim);
        }
    }
    for (shard_id, lease) in store.shard_leases {
        by_shard
            .entry(shard_id)
            .or_insert_with(Store::default)
            .shard_leases
            .insert(shard_id, lease);
    }
    for shard in by_shard.values_mut() {
        shard.next_signal_id = store.next_signal_id;
        shard.next_effect_id = store.next_effect_id;
        shard.next_child_id = store.next_child_id;
    }
    by_shard
}

fn merge_shard_stores(stores: Vec<Store>) -> Store {
    let mut merged = Store::default();
    for store in stores {
        merged.instances.extend(store.instances);
        merged.signals.extend(store.signals);
        merged.children.extend(store.children);
        merged.task_order.extend(store.task_order);
        merged.tasks.extend(store.tasks);
        merged
            .claimed_sequence_epochs
            .extend(store.claimed_sequence_epochs);
        merged
            .completed_activation_claims
            .extend(store.completed_activation_claims);
        merged.shard_leases.extend(store.shard_leases);
        merged.next_signal_id = merged.next_signal_id.max(store.next_signal_id);
        merged.next_effect_id = merged.next_effect_id.max(store.next_effect_id);
        merged.next_child_id = merged.next_child_id.max(store.next_child_id);
    }
    merged
}

#[derive(Clone)]
pub struct NullDurabilityProvider {
    inner: ShardRouter,
}

impl NullDurabilityProvider {
    pub fn new() -> Self {
        Self {
            inner: ShardRouter::in_memory(),
        }
    }
}

impl Default for NullDurabilityProvider {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_SQLITE_SNAPSHOT_INTERVAL: u64 = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", rename_all = "snake_case")]
enum JournalOperation {
    ClaimShard(ClaimShardInput),
    HeartbeatShard {
        shard_id: u32,
        owner_id: String,
        now: DateTime<Utc>,
        lease_ms: u64,
    },
    ReleaseShard {
        shard_id: u32,
        owner_id: String,
    },
    ReleaseActivation {
        activation_id: String,
        worker_id: String,
    },
    ClaimShardTasks {
        session: OpenShardInput,
        input: ClaimShardTasksInput,
    },
    ClaimShardAndTasks {
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    },
    CreateInstance(CreateInstanceInput),
    CreateChildInstance(CreateChildInstanceInput),
    AppendSignal(AppendSignalInput),
    CancelChild(CancelChildInput),
    ReserveEffect(ReserveEffectInput),
    PutEffectRecord {
        workflow_id: String,
        run_id: String,
        effect: EffectRecord,
    },
    HeartbeatEffect(HeartbeatEffectInput),
    CompleteEffect(CompleteEffectInput),
    FailEffect(FailEffectInput),
    CommitCheckpoint(CommitCheckpointInput),
    CommitActivations(Vec<CommitCheckpointInput>),
    RecordActivationFailures(Vec<RecordActivationFailureInput>),
}

#[derive(Clone)]
pub struct SqliteDurabilityProvider {
    inner: ShardRouter,
    writer: SqliteWriter,
    snapshot_interval: u64,
}

#[derive(Clone)]
struct SqliteWriter {
    inner: Arc<SqliteWriterInner>,
}

struct SqliteWriterInner {
    sender: std::sync::mpsc::Sender<SqliteWriterCommand>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

enum SqliteWriterCommand {
    AppendJournal {
        operation_json: String,
        response: oneshot::Sender<Result<u64, WorkflowError>>,
    },
    WriteSnapshot {
        entry_id: u64,
        snapshot_json: String,
        response: oneshot::Sender<Result<(), WorkflowError>>,
    },
    PragmaString {
        pragma: String,
        response: std::sync::mpsc::Sender<Result<String, WorkflowError>>,
    },
    PragmaI64 {
        pragma: String,
        response: std::sync::mpsc::Sender<Result<i64, WorkflowError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), WorkflowError>>,
    },
}

#[derive(Clone, Debug)]
pub struct SqliteDurabilityOptions {
    pub snapshot_interval: u64,
}

impl Default for SqliteDurabilityOptions {
    fn default() -> Self {
        Self {
            snapshot_interval: DEFAULT_SQLITE_SNAPSHOT_INTERVAL,
        }
    }
}

impl SqliteWriter {
    fn start(connection: rusqlite::Connection) -> Result<Self, WorkflowError> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("durable-sqlite-writer".to_string())
            .spawn(move || sqlite_writer_loop(connection, receiver))
            .map_err(|error| WorkflowError::new(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(SqliteWriterInner {
                sender,
                handle: Mutex::new(Some(handle)),
            }),
        })
    }

    async fn append_journal(&self, operation_json: String) -> Result<u64, WorkflowError> {
        self.request(|response| SqliteWriterCommand::AppendJournal {
            operation_json,
            response,
        })
        .await
    }

    async fn write_snapshot(
        &self,
        entry_id: u64,
        snapshot_json: String,
    ) -> Result<(), WorkflowError> {
        self.request(|response| SqliteWriterCommand::WriteSnapshot {
            entry_id,
            snapshot_json,
            response,
        })
        .await
    }

    fn pragma_string(&self, pragma: &str) -> Result<String, WorkflowError> {
        self.request_blocking(|response| SqliteWriterCommand::PragmaString {
            pragma: pragma.to_string(),
            response,
        })
    }

    fn pragma_i64(&self, pragma: &str) -> Result<i64, WorkflowError> {
        self.request_blocking(|response| SqliteWriterCommand::PragmaI64 {
            pragma: pragma.to_string(),
            response,
        })
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        self.request(|response| SqliteWriterCommand::Shutdown { response })
            .await?;
        let handle = self
            .inner
            .handle
            .lock()
            .map_err(|_| WorkflowError::new("sqlite writer handle lock poisoned"))?
            .take();
        if let Some(handle) = handle {
            tokio::task::spawn_blocking(move || {
                handle
                    .join()
                    .map_err(|_| WorkflowError::new("sqlite writer thread panicked"))
            })
            .await
            .map_err(|error| WorkflowError::new(error.to_string()))??;
        }
        Ok(())
    }

    async fn request<T, F>(&self, build: F) -> Result<T, WorkflowError>
    where
        T: Send + 'static,
        F: FnOnce(oneshot::Sender<Result<T, WorkflowError>>) -> SqliteWriterCommand,
    {
        let (response, receiver) = oneshot::channel();
        self.inner
            .sender
            .send(build(response))
            .map_err(|_| WorkflowError::new("sqlite writer is closed"))?;
        receiver
            .await
            .map_err(|_| WorkflowError::new("sqlite writer closed before responding"))?
    }

    fn request_blocking<T, F>(&self, build: F) -> Result<T, WorkflowError>
    where
        F: FnOnce(std::sync::mpsc::Sender<Result<T, WorkflowError>>) -> SqliteWriterCommand,
    {
        let (response, receiver) = std::sync::mpsc::channel();
        self.inner
            .sender
            .send(build(response))
            .map_err(|_| WorkflowError::new("sqlite writer is closed"))?;
        receiver
            .recv()
            .map_err(|_| WorkflowError::new("sqlite writer closed before responding"))?
    }
}

fn sqlite_writer_loop(
    connection: rusqlite::Connection,
    receiver: std::sync::mpsc::Receiver<SqliteWriterCommand>,
) {
    for command in receiver {
        match command {
            SqliteWriterCommand::AppendJournal {
                operation_json,
                response,
            } => {
                let result = sqlite_append_journal(&connection, operation_json);
                let _ = response.send(result);
            }
            SqliteWriterCommand::WriteSnapshot {
                entry_id,
                snapshot_json,
                response,
            } => {
                let result = sqlite_write_snapshot(&connection, entry_id, snapshot_json);
                let _ = response.send(result);
            }
            SqliteWriterCommand::PragmaString { pragma, response } => {
                let result = connection
                    .query_row(&format!("PRAGMA {pragma}"), [], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(WorkflowError::from);
                let _ = response.send(result);
            }
            SqliteWriterCommand::PragmaI64 { pragma, response } => {
                let result = connection
                    .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get::<_, i64>(0))
                    .map_err(WorkflowError::from);
                let _ = response.send(result);
            }
            SqliteWriterCommand::Shutdown { response } => {
                let _ = response.send(Ok(()));
                break;
            }
        }
    }
}

fn sqlite_append_journal(
    connection: &rusqlite::Connection,
    operation_json: String,
) -> Result<u64, WorkflowError> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    {
        let mut statement = connection.prepare_cached(
            "INSERT INTO shard_journal (operation_json, created_at) VALUES (?1, ?2)",
        )?;
        statement.execute(rusqlite::params![operation_json, now])?;
    }
    Ok(connection.last_insert_rowid() as u64)
}

fn sqlite_write_snapshot(
    connection: &rusqlite::Connection,
    entry_id: u64,
    snapshot_json: String,
) -> Result<(), WorkflowError> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut statement = connection.prepare_cached(
        "INSERT INTO shard_snapshots (snapshot_id, last_entry_id, snapshot_json, created_at)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(snapshot_id) DO UPDATE SET
           last_entry_id = excluded.last_entry_id,
           snapshot_json = excluded.snapshot_json,
           created_at = excluded.created_at",
    )?;
    statement.execute(rusqlite::params![entry_id, snapshot_json, now])?;
    Ok(())
}

impl SqliteDurabilityProvider {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, WorkflowError> {
        Self::new_with_options(path, SqliteDurabilityOptions::default())
    }

    pub fn new_with_options(
        path: impl AsRef<Path>,
        options: SqliteDurabilityOptions,
    ) -> Result<Self, WorkflowError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = rusqlite::Connection::open(&path)?;
        configure_sqlite(&connection)?;
        ensure_sqlite_schema(&connection)?;
        let state = load_sqlite_store(&connection)?;
        let writer = SqliteWriter::start(connection)?;
        Ok(Self {
            inner: ShardRouter::from_store(state),
            writer,
            snapshot_interval: options.snapshot_interval.max(1),
        })
    }

    pub fn pragma_string(&self, pragma: &str) -> Result<String, WorkflowError> {
        self.writer.pragma_string(pragma)
    }

    pub fn pragma_i64(&self, pragma: &str) -> Result<i64, WorkflowError> {
        self.writer.pragma_i64(pragma)
    }

    async fn append_journal_operation(
        &self,
        operation: JournalOperation,
    ) -> Result<(), WorkflowError> {
        let operation_json = serde_json::to_string(&operation)?;
        let entry_id = self.writer.append_journal(operation_json).await?;
        if entry_id % self.snapshot_interval == 0 {
            let snapshot = serde_json::to_string(&self.inner.snapshot_store().await?)?;
            self.writer.write_snapshot(entry_id, snapshot).await?;
        }
        Ok(())
    }

    pub async fn load_instance(
        &self,
        ref_: &InstanceRef,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::load_instance(
            &self.inner,
            ref_,
            LoadInstanceOptions {
                include_effects: true,
            },
        )
        .await
    }

    pub async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
    }

    pub async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
    }
}

#[async_trait]
impl DurabilityProvider for SqliteDurabilityProvider {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::claim_shard(&self.inner, input.clone()).await?;
        if output.is_some() {
            self.append_journal_operation(JournalOperation::ClaimShard(input))
                .await?;
        }
        Ok(output)
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        let output = <ShardRouter as DurabilityProvider>::claim_shard_tasks(
            &self.inner,
            claim.clone(),
            input.clone(),
        )
        .await?;
        if output.is_some() {
            self.append_journal_operation(JournalOperation::ClaimShardAndTasks { claim, input })
                .await?;
        }
        Ok(output)
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        Arc::new(SqliteShardSession {
            provider: self.clone(),
            inner: <ShardRouter as DurabilityProvider>::open_shard(&self.inner, input),
        })
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::create_instance(&self.inner, input.clone())
                .await?;
        self.append_journal_operation(JournalOperation::CreateInstance(input))
            .await?;
        Ok(output)
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::create_child_instance(&self.inner, input.clone())
                .await?;
        self.append_journal_operation(JournalOperation::CreateChildInstance(input))
            .await?;
        Ok(output)
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::load_instance(&self.inner, ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::append_signal(&self.inner, input.clone()).await?;
        self.append_journal_operation(JournalOperation::AppendSignal(input))
            .await?;
        Ok(output)
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::cancel_child(&self.inner, input.clone()).await?;
        self.append_journal_operation(JournalOperation::CancelChild(input))
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::get_or_reserve_effect(&self.inner, input.clone())
                .await?;
        if let EffectReservation::Reserved { effect_id, .. } = &output {
            let effect = reserved_effect_record_from_router(
                &self.inner,
                &input.workflow_id,
                &input.run_id,
                effect_id,
            )
            .await?;
            self.append_journal_operation(JournalOperation::PutEffectRecord {
                workflow_id: input.workflow_id,
                run_id: input.run_id,
                effect,
            })
            .await?;
        }
        Ok(output)
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.inner, input.clone()).await?;
        self.append_journal_operation(JournalOperation::HeartbeatEffect(input))
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::complete_effect(&self.inner, input.clone()).await?;
        self.append_journal_operation(JournalOperation::CompleteEffect(input))
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::fail_effect(&self.inner, input.clone()).await?;
        self.append_journal_operation(JournalOperation::FailEffect(input))
            .await?;
        Ok(output)
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::commit_checkpoint(&self.inner, input.clone())
                .await?;
        if output.ok {
            self.append_journal_operation(JournalOperation::CommitCheckpoint(input))
                .await?;
        }
        Ok(output)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::commit_activations(&self.inner, inputs.clone())
                .await?;
        if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
            self.append_journal_operation(JournalOperation::CommitActivations(inputs))
                .await?;
        }
        Ok(output)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        <ShardRouter as DurabilityProvider>::record_activation_failures(
            &self.inner,
            inputs.clone(),
        )
        .await?;
        self.append_journal_operation(JournalOperation::RecordActivationFailures(inputs))
            .await
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_instances(&self.inner).await
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        self.inner.shutdown().await?;
        self.writer.shutdown().await
    }
}

struct SqliteShardSession {
    provider: SqliteDurabilityProvider,
    inner: Arc<dyn ShardDurabilitySession>,
}

#[async_trait]
impl ShardDurabilitySession for SqliteShardSession {
    fn shard_id(&self) -> u32 {
        self.inner.shard_id()
    }

    fn owner_id(&self) -> Option<&str> {
        self.inner.owner_id()
    }

    fn lease_epoch(&self) -> Option<u64> {
        self.inner.lease_epoch()
    }

    async fn claim_tasks(
        &self,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        let session = OpenShardInput {
            shard_id: self.shard_id(),
            owner_id: self.owner_id().map(str::to_string),
            lease_epoch: self.lease_epoch(),
        };
        let output = self.inner.claim_tasks(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::ClaimShardTasks { session, input })
            .await?;
        Ok(output)
    }

    async fn read_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.inner.read_instance(ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let output = self.inner.append_signal(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::AppendSignal(input))
            .await?;
        Ok(output)
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        self.inner.cancel_child(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::CancelChild(input))
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let output = self.inner.get_or_reserve_effect(input.clone()).await?;
        if let EffectReservation::Reserved { effect_id, .. } = &output {
            let effect = self
                .inner
                .read_instance(
                    &InstanceRef::new(input.workflow_id.clone(), input.run_id.clone()),
                    LoadInstanceOptions {
                        include_effects: true,
                    },
                )
                .await?
                .and_then(|instance| {
                    instance
                        .effects
                        .into_iter()
                        .find(|effect| effect.effect_id == *effect_id)
                })
                .ok_or_else(|| {
                    WorkflowError::new(format!("reserved effect missing: {effect_id}"))
                })?;
            self.provider
                .append_journal_operation(JournalOperation::PutEffectRecord {
                    workflow_id: input.workflow_id,
                    run_id: input.run_id,
                    effect,
                })
                .await?;
        }
        Ok(output)
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        self.inner.heartbeat_effect(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::HeartbeatEffect(input))
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        self.inner.complete_effect(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::CompleteEffect(input))
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let output = self.inner.fail_effect(input.clone()).await?;
        self.provider
            .append_journal_operation(JournalOperation::FailEffect(input))
            .await?;
        Ok(output)
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let output = self.inner.commit_checkpoint(input.clone()).await?;
        if output.ok {
            self.provider
                .append_journal_operation(JournalOperation::CommitCheckpoint(input))
                .await?;
        }
        Ok(output)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let output = self.inner.commit_activations(inputs.clone()).await?;
        if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
            self.provider
                .append_journal_operation(JournalOperation::CommitActivations(inputs))
                .await?;
        }
        Ok(output)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        self.inner
            .record_activation_failures(inputs.clone())
            .await?;
        self.provider
            .append_journal_operation(JournalOperation::RecordActivationFailures(inputs))
            .await
    }

    async fn release_activation(
        &self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError> {
        self.inner
            .release_activation(activation_id, worker_id)
            .await?;
        self.provider
            .append_journal_operation(JournalOperation::ReleaseActivation {
                activation_id: activation_id.to_string(),
                worker_id: worker_id.to_string(),
            })
            .await
    }

    async fn heartbeat(&self, now: DateTime<Utc>, lease_ms: u64) -> Result<(), WorkflowError> {
        self.inner.heartbeat(now, lease_ms).await?;
        if let Some(owner_id) = self.owner_id() {
            self.provider
                .append_journal_operation(JournalOperation::HeartbeatShard {
                    shard_id: self.shard_id(),
                    owner_id: owner_id.to_string(),
                    now,
                    lease_ms,
                })
                .await?;
        }
        Ok(())
    }

    async fn release(&self) -> Result<(), WorkflowError> {
        self.inner.release().await?;
        if let Some(owner_id) = self.owner_id() {
            self.provider
                .append_journal_operation(JournalOperation::ReleaseShard {
                    shard_id: self.shard_id(),
                    owner_id: owner_id.to_string(),
                })
                .await?;
        }
        Ok(())
    }
}

fn configure_sqlite(connection: &rusqlite::Connection) -> Result<(), WorkflowError> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(std::time::Duration::from_millis(5_000))?;
    Ok(())
}

fn ensure_sqlite_schema(connection: &rusqlite::Connection) -> Result<(), WorkflowError> {
    let journal_columns = sqlite_table_columns(connection, "shard_journal")?;
    if !journal_columns.is_empty()
        && !journal_columns
            .iter()
            .any(|column| column == "operation_json")
    {
        connection.execute_batch(
            "
            DROP TABLE IF EXISTS shard_journal;
            DROP TABLE IF EXISTS shard_snapshots;
            ",
        )?;
    }

    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS shard_journal (
            entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shard_snapshots (
            snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
            last_entry_id INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS shard_journal_entry ON shard_journal(entry_id);
        ",
    )?;
    Ok(())
}

fn load_sqlite_store(connection: &rusqlite::Connection) -> Result<Store, WorkflowError> {
    let snapshot = connection.query_row(
        "SELECT last_entry_id, snapshot_json FROM shard_snapshots WHERE snapshot_id = 1",
        [],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
    );
    let (last_entry_id, store) = match snapshot {
        Ok((last_entry_id, raw)) => (last_entry_id, serde_json::from_str(&raw)?),
        Err(rusqlite::Error::QueryReturnedNoRows) => (0, Store::default()),
        Err(error) => return Err(error.into()),
    };
    let mut provider = ShardEngine::from_store(store);
    let mut statement = connection.prepare(
        "SELECT operation_json FROM shard_journal WHERE entry_id > ?1 ORDER BY entry_id ASC",
    )?;
    let rows = statement.query_map([last_entry_id], |row| row.get::<_, String>(0))?;
    for row in rows {
        let operation = serde_json::from_str::<JournalOperation>(&row?)?;
        apply_journal_operation(&mut provider, operation)?;
    }
    Ok(provider.snapshot_store())
}

fn sqlite_table_columns(
    connection: &rusqlite::Connection,
    table: &str,
) -> Result<Vec<String>, WorkflowError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn apply_journal_operation(
    provider: &mut ShardEngine,
    operation: JournalOperation,
) -> Result<(), WorkflowError> {
    match operation {
        JournalOperation::ClaimShard(input) => {
            provider.claim_shard_sync(input)?;
        }
        JournalOperation::HeartbeatShard {
            shard_id,
            owner_id,
            now,
            lease_ms,
        } => provider.heartbeat_shard_sync(shard_id, &owner_id, now, lease_ms)?,
        JournalOperation::ReleaseShard { shard_id, owner_id } => {
            provider.release_shard_sync(shard_id, &owner_id)?;
        }
        JournalOperation::ReleaseActivation {
            activation_id,
            worker_id,
        } => {
            provider.release_activation_sync(&activation_id, &worker_id)?;
        }
        JournalOperation::ClaimShardTasks { session, input } => {
            provider.claim_tasks_for_session(&session, input)?;
        }
        JournalOperation::ClaimShardAndTasks { claim, input } => {
            if let Some(lease) = provider.claim_shard_sync(claim)? {
                provider.claim_tasks_for_session(
                    &OpenShardInput {
                        shard_id: lease.shard_id,
                        owner_id: Some(lease.owner_id.clone()),
                        lease_epoch: Some(lease.lease_epoch),
                    },
                    input,
                )?;
            }
        }
        JournalOperation::CreateInstance(input) => {
            provider.create_instance(input)?;
        }
        JournalOperation::CreateChildInstance(input) => {
            provider.create_child_instance(input)?;
        }
        JournalOperation::AppendSignal(input) => {
            provider.append_signal(
                input.workflow_id,
                input.run_id,
                input.r#type,
                input.payload,
                input.received_at,
            )?;
        }
        JournalOperation::CancelChild(input) => {
            provider.cancel_child(input)?;
        }
        JournalOperation::ReserveEffect(input) => {
            provider.get_or_reserve_effect(
                &input.workflow_id,
                &input.run_id,
                &input.activation_id,
                &input.key,
                &input.worker_id,
                input.now,
                input.options,
                input.max_attempts,
            )?;
        }
        JournalOperation::PutEffectRecord {
            workflow_id,
            run_id,
            effect,
        } => {
            provider.put_effect_record(&workflow_id, &run_id, effect)?;
        }
        JournalOperation::HeartbeatEffect(input) => {
            provider.heartbeat_effect(
                &input.workflow_id,
                &input.run_id,
                &input.effect_id,
                &input.attempt_id,
                &input.worker_id,
                input.now,
                input.details,
            )?;
        }
        JournalOperation::CompleteEffect(input) => {
            provider.complete_effect(
                &input.workflow_id,
                &input.run_id,
                &input.effect_id,
                &input.attempt_id,
                &input.worker_id,
                input.now,
                input.result,
            )?;
        }
        JournalOperation::FailEffect(input) => {
            provider.fail_effect(
                &input.workflow_id,
                &input.run_id,
                &input.effect_id,
                &input.attempt_id,
                &input.worker_id,
                input.error,
                input.now,
                input.retryable,
            )?;
        }
        JournalOperation::CommitCheckpoint(input) => {
            let result = provider.commit_checkpoint(input)?;
            if !result.ok {
                return Err(WorkflowError::new(format!(
                    "journal replay hit checkpoint conflict: {:?}",
                    result
                )));
            }
        }
        JournalOperation::CommitActivations(inputs) => {
            let results = provider.commit_activations(inputs)?;
            if let Some(conflict) = results.results.iter().find(|result| !result.ok) {
                return Err(WorkflowError::new(format!(
                    "journal replay hit activation commit conflict: {:?}",
                    conflict
                )));
            }
        }
        JournalOperation::RecordActivationFailures(inputs) => {
            provider.record_activation_failures(inputs)?;
        }
    }
    Ok(())
}

async fn reserved_effect_record_from_router(
    provider: &ShardRouter,
    workflow_id: &str,
    run_id: &str,
    effect_id: &str,
) -> Result<EffectRecord, WorkflowError> {
    provider
        .load_instance(
            &InstanceRef::new(workflow_id.to_string(), run_id.to_string()),
            LoadInstanceOptions {
                include_effects: true,
            },
        )
        .await?
        .and_then(|instance| {
            instance
                .effects
                .into_iter()
                .find(|effect| effect.effect_id == effect_id)
        })
        .ok_or_else(|| WorkflowError::new(format!("reserved effect missing: {effect_id}")))
}

#[derive(Clone)]
pub struct SqliteShardFileDurabilityProvider {
    shard_count: u32,
    providers: Arc<Vec<SqliteDurabilityProvider>>,
}

impl SqliteShardFileDurabilityProvider {
    pub fn new(directory: impl AsRef<Path>, shard_count: u32) -> Result<Self, WorkflowError> {
        if shard_count == 0 {
            return Err(WorkflowError::new("shard_count must be positive"));
        }
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let mut providers = Vec::with_capacity(shard_count as usize);
        for shard_id in 0..shard_count {
            providers.push(SqliteDurabilityProvider::new(
                directory.join(format!("shard-{shard_id}.sqlite")),
            )?);
        }
        Ok(Self {
            shard_count,
            providers: Arc::new(providers),
        })
    }

    fn provider_for_shard(&self, shard_id: u32) -> Result<SqliteDurabilityProvider, WorkflowError> {
        if shard_id >= self.shard_count {
            return Err(WorkflowError::new(format!(
                "shard {shard_id} outside configured shard count {}",
                self.shard_count
            )));
        }
        Ok(self.providers[shard_id as usize].clone())
    }

    fn provider_for_ref(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<SqliteDurabilityProvider, WorkflowError> {
        self.provider_for_shard(workflow_partition_shard(
            workflow_id,
            run_id,
            self.shard_count,
        ))
    }
}

#[async_trait]
impl DurabilityProvider for SqliteShardFileDurabilityProvider {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        self.provider_for_shard(input.shard_id)?
            .claim_shard(input)
            .await
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        self.provider_for_shard(claim.shard_id)?
            .claim_shard_tasks(claim, input)
            .await
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        match self.provider_for_shard(input.shard_id) {
            Ok(provider) => provider.open_shard(input),
            Err(error) => Arc::new(FailedShardSession { error }),
        }
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        self.provider_for_shard(input.partition_shard)?
            .create_instance(input)
            .await
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        let parent_shard = workflow_partition_shard(
            &input.parent_workflow_id,
            &input.parent_run_id,
            self.shard_count,
        );
        if input.partition_shard != parent_shard {
            return Err(WorkflowError::new(
                "SQLite shard-file provider requires local child workflow starts to be shard-affine",
            ));
        }
        self.provider_for_shard(input.partition_shard)?
            .create_child_instance(input)
            .await
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let provider = self.provider_for_ref(&ref_.workflow_id, &ref_.run_id)?;
        <SqliteDurabilityProvider as DurabilityProvider>::load_instance(&provider, ref_, options)
            .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .append_signal(input)
            .await
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        self.provider_for_ref(&input.parent_workflow_id, &input.parent_run_id)?
            .cancel_child(input)
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .get_or_reserve_effect(input)
            .await
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .heartbeat_effect(input)
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .complete_effect(input)
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .fail_effect(input)
            .await
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let parent_shard =
            workflow_partition_shard(&input.workflow_id, &input.run_id, self.shard_count);
        if input
            .child_starts
            .iter()
            .any(|start| start.partition_shard != parent_shard)
        {
            return Ok(CommitCheckpointResult {
                ok: false,
                sequence: input.expected_sequence,
                reason: Some("cross_shard_child_start".to_string()),
                retryable: Some(false),
                error: Some(SerializedError {
                            name: None,
                            message: "SQLite shard-file provider requires commit-local children to stay on the parent shard".to_string(),
                }),
            });
        }
        self.provider_for_shard(parent_shard)?
            .commit_checkpoint(input)
            .await
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let input_count = inputs.len();
        let mut results = vec![None; input_count];
        let mut groups: HashMap<u32, Vec<(usize, CommitCheckpointInput)>> = HashMap::new();

        for (index, input) in inputs.into_iter().enumerate() {
            let parent_shard =
                workflow_partition_shard(&input.workflow_id, &input.run_id, self.shard_count);
            if input
                .child_starts
                .iter()
                .any(|start| start.partition_shard != parent_shard)
            {
                results[index] = Some(CommitCheckpointResult {
                    ok: false,
                    sequence: input.expected_sequence,
                    reason: Some("cross_shard_child_start".to_string()),
                    retryable: Some(false),
                    error: Some(SerializedError {
                            name: None,
                            message: "SQLite shard-file provider requires commit-local children to stay on the parent shard".to_string(),
                    }),
                });
                continue;
            }
            groups.entry(parent_shard).or_default().push((index, input));
        }

        for (shard_id, group) in groups {
            let indices = group.iter().map(|(index, _)| *index).collect::<Vec<_>>();
            let group_inputs = group
                .into_iter()
                .map(|(_, input)| input)
                .collect::<Vec<_>>();
            let output = self
                .provider_for_shard(shard_id)?
                .commit_activations(group_inputs)
                .await?;
            for (index, result) in indices.into_iter().zip(output.results) {
                results[index] = Some(result);
            }
        }

        Ok(CommitActivationsResult {
            results: results
                .into_iter()
                .map(|result| result.expect("all activation commit results are populated"))
                .collect(),
        })
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        let mut groups: HashMap<u32, Vec<RecordActivationFailureInput>> = HashMap::new();
        for input in inputs {
            let shard_id =
                workflow_partition_shard(&input.workflow_id, &input.run_id, self.shard_count);
            groups.entry(shard_id).or_default().push(input);
        }
        for (shard_id, group) in groups {
            self.provider_for_shard(shard_id)?
                .record_activation_failures(group)
                .await?;
        }
        Ok(())
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        let mut output = Vec::new();
        for shard_id in 0..self.shard_count {
            output.extend(self.provider_for_shard(shard_id)?.list_instances().await?);
        }
        Ok(output)
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        let mut output = Vec::new();
        for shard_id in 0..self.shard_count {
            let provider = self.provider_for_shard(shard_id)?;
            output.extend(
                <SqliteDurabilityProvider as DurabilityProvider>::list_signals(&provider).await?,
            );
        }
        output.sort_by(compare_signal_records);
        Ok(output)
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        let mut output = Vec::new();
        for shard_id in 0..self.shard_count {
            let provider = self.provider_for_shard(shard_id)?;
            output.extend(
                <SqliteDurabilityProvider as DurabilityProvider>::list_children(&provider).await?,
            );
        }
        output.sort_by(|left, right| left.child_record_id.cmp(&right.child_record_id));
        Ok(output)
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        for shard_id in 0..self.shard_count {
            self.provider_for_shard(shard_id)?.shutdown().await?;
        }
        Ok(())
    }
}

struct FailedShardSession {
    error: WorkflowError,
}

#[async_trait]
impl ShardDurabilitySession for FailedShardSession {
    fn shard_id(&self) -> u32 {
        0
    }

    fn owner_id(&self) -> Option<&str> {
        None
    }

    fn lease_epoch(&self) -> Option<u64> {
        None
    }

    async fn claim_tasks(
        &self,
        _input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        Err(self.error.clone())
    }

    async fn read_instance(
        &self,
        _ref_: &InstanceRef,
        _options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        Err(self.error.clone())
    }

    async fn append_signal(
        &self,
        _input: AppendSignalInput,
    ) -> Result<SignalRecord, WorkflowError> {
        Err(self.error.clone())
    }

    async fn cancel_child(&self, _input: CancelChildInput) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }

    async fn get_or_reserve_effect(
        &self,
        _input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        Err(self.error.clone())
    }

    async fn heartbeat_effect(&self, _input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }

    async fn complete_effect(&self, _input: CompleteEffectInput) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }

    async fn fail_effect(
        &self,
        _input: FailEffectInput,
    ) -> Result<FailEffectResult, WorkflowError> {
        Err(self.error.clone())
    }

    async fn commit_checkpoint(
        &self,
        _input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        Err(self.error.clone())
    }

    async fn release_activation(
        &self,
        _activation_id: &str,
        _worker_id: &str,
    ) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }

    async fn heartbeat(&self, _now: DateTime<Utc>, _lease_ms: u64) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }

    async fn release(&self) -> Result<(), WorkflowError> {
        Err(self.error.clone())
    }
}

#[derive(Clone, Debug)]
pub struct PostgresDurabilityProviderOptions {
    pub connection_string: String,
    pub schema: Option<String>,
    pub physical_partitions: u32,
    pub snapshot_interval: Option<u64>,
}

#[derive(Clone)]
pub struct PostgresDurabilityProvider {
    inner: ShardRouter,
    pool: Arc<PostgresClientPool>,
    schema: String,
    physical_partitions: u32,
    snapshot_interval: u64,
}

struct PostgresClientPool {
    clients: Vec<Arc<tokio_postgres::Client>>,
    next: AtomicUsize,
}

impl PostgresClientPool {
    async fn create(connection_string: &str, size: usize) -> Result<Self, WorkflowError> {
        let mut clients = Vec::with_capacity(size);
        for index in 0..size {
            let (client, connection) =
                tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    eprintln!("durable Postgres connection {index} error: {error}");
                }
            });
            client
                .batch_execute(
                    "
                    SET statement_timeout = 30000;
                    SET lock_timeout = 5000;
                    ",
                )
                .await?;
            clients.push(Arc::new(client));
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn next(&self) -> Arc<tokio_postgres::Client> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[index].clone()
    }
}

impl PostgresDurabilityProvider {
    pub async fn create(options: PostgresDurabilityProviderOptions) -> Result<Self, WorkflowError> {
        if options.physical_partitions == 0 {
            return Err(WorkflowError::new("physical_partitions must be positive"));
        }
        let schema = normalize_postgres_schema(options.schema)?;
        let pool_size = (options.physical_partitions as usize).max(4);
        let pool =
            Arc::new(PostgresClientPool::create(&options.connection_string, pool_size).await?);
        let provider = Self {
            inner: ShardRouter::in_memory(),
            pool,
            schema,
            physical_partitions: options.physical_partitions,
            snapshot_interval: options
                .snapshot_interval
                .unwrap_or(DEFAULT_SQLITE_SNAPSHOT_INTERVAL),
        };
        provider.initialize_postgres_schema().await?;
        let state = provider.load_postgres_store().await?;
        Ok(Self {
            inner: ShardRouter::from_store(state),
            ..provider
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    async fn initialize_postgres_schema(&self) -> Result<(), WorkflowError> {
        let client = self.pool.next();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {};",
                quote_postgres_identifier(&self.schema)
            ))
            .await?;
        client
            .batch_execute(&format!(
                "
                CREATE TABLE IF NOT EXISTS {}.provider_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                ",
                quote_postgres_identifier(&self.schema)
            ))
            .await?;
        self.verify_postgres_metadata(&client, "postgres_storage_shape", "rust_append_store_v1")
            .await?;
        self.verify_postgres_metadata(
            &client,
            "physical_partition_count",
            &self.physical_partitions.to_string(),
        )
        .await?;
        for partition in 0..self.physical_partitions {
            client
                .batch_execute(&format!(
                    "
                    CREATE TABLE IF NOT EXISTS {} (
                        shard_id INTEGER PRIMARY KEY,
                        last_entry_id BIGINT NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS {} (
                        shard_id INTEGER NOT NULL,
                        entry_id BIGINT NOT NULL,
                        operation_json TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (shard_id, entry_id)
                    );

                    CREATE TABLE IF NOT EXISTS {} (
                        shard_id INTEGER PRIMARY KEY,
                        last_entry_id BIGINT NOT NULL,
                        snapshot_json TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    ",
                    self.postgres_table("shard_heads", partition),
                    self.postgres_table("shard_journal", partition),
                    self.postgres_table("shard_snapshots", partition),
                ))
                .await?;
        }
        Ok(())
    }

    async fn verify_postgres_metadata(
        &self,
        client: &tokio_postgres::Client,
        key: &str,
        expected: &str,
    ) -> Result<(), WorkflowError> {
        let table = format!(
            "{}.provider_metadata",
            quote_postgres_identifier(&self.schema)
        );
        let row = client
            .query_opt(
                &format!("SELECT value FROM {table} WHERE key = $1"),
                &[&key],
            )
            .await?;
        if let Some(row) = row {
            let actual: String = row.get(0);
            if actual != expected {
                return Err(WorkflowError::new(format!(
                    "PostgresDurabilityProvider metadata mismatch for {key}: expected {expected}, found {actual}"
                )));
            }
            return Ok(());
        }
        client
            .execute(
                &format!(
                    "INSERT INTO {table} (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING"
                ),
                &[&key, &expected],
            )
            .await?;
        Ok(())
    }

    async fn append_postgres_operation(
        &self,
        operation: JournalOperation,
    ) -> Result<(), WorkflowError> {
        let shard_id = self.postgres_shard_for_operation(&operation).await?;
        self.append_postgres_operation_for_shard(shard_id, operation)
            .await
    }

    async fn append_postgres_operation_for_shard(
        &self,
        shard_id: u32,
        operation: JournalOperation,
    ) -> Result<(), WorkflowError> {
        let operation_json = serde_json::to_string(&operation)?;
        let shard_id_i32 = shard_id as i32;
        let client = self.pool.next();
        let row = client
            .query_one(
                &format!(
                    "
                    WITH next_head AS (
                        INSERT INTO {} AS h (shard_id, last_entry_id, updated_at)
                        VALUES ($1, 1, NOW())
                        ON CONFLICT (shard_id) DO UPDATE
                        SET last_entry_id = h.last_entry_id + 1,
                            updated_at = NOW()
                        RETURNING last_entry_id
                    )
                    INSERT INTO {} (shard_id, entry_id, operation_json, created_at)
                    SELECT $1, last_entry_id, $2, NOW()
                    FROM next_head
                    RETURNING entry_id
                    ",
                    self.postgres_head_table_for_shard(shard_id),
                    self.postgres_journal_table_for_shard(shard_id)
                ),
                &[&shard_id_i32, &operation_json],
            )
            .await?;
        let next_entry_id: i64 = row.get(0);
        if self.snapshot_interval > 0 && next_entry_id as u64 % self.snapshot_interval == 0 {
            let snapshot = serde_json::to_string(&self.inner.snapshot_store().await?)?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {} (shard_id, last_entry_id, snapshot_json, created_at)
                         VALUES ($1, $2, $3, NOW())
                         ON CONFLICT (shard_id) DO UPDATE SET
                           last_entry_id = EXCLUDED.last_entry_id,
                           snapshot_json = EXCLUDED.snapshot_json,
                           created_at = EXCLUDED.created_at",
                        self.postgres_snapshot_table_for_shard(shard_id)
                    ),
                    &[&shard_id_i32, &next_entry_id, &snapshot],
                )
                .await?;
        }
        Ok(())
    }

    async fn load_postgres_store(&self) -> Result<Store, WorkflowError> {
        let client = self.pool.next();
        let mut latest_snapshot: Option<(DateTime<Utc>, String)> = None;
        for partition in 0..self.physical_partitions {
            let snapshot = client
                .query_opt(
                    &format!(
                        "SELECT snapshot_json, created_at FROM {} ORDER BY created_at DESC LIMIT 1",
                        self.postgres_table("shard_snapshots", partition)
                    ),
                    &[],
                )
                .await?;
            if let Some(row) = snapshot {
                let raw: String = row.get(0);
                let created_at: DateTime<Utc> = row.get(1);
                if latest_snapshot
                    .as_ref()
                    .is_none_or(|(current, _)| created_at > *current)
                {
                    latest_snapshot = Some((created_at, raw));
                }
            }
        }
        let (snapshot_created_at, store) = if let Some((created_at, raw)) = latest_snapshot {
            (Some(created_at), serde_json::from_str(&raw)?)
        } else {
            (None, Store::default())
        };
        let mut provider = ShardEngine::from_store(store);
        let mut rows = Vec::new();
        for partition in 0..self.physical_partitions {
            let table = self.postgres_table("shard_journal", partition);
            let partition_rows = if let Some(created_at) = snapshot_created_at {
                client
                    .query(
                        &format!(
                            "SELECT shard_id, entry_id, operation_json, created_at FROM {table} WHERE created_at > $1 ORDER BY created_at ASC, shard_id ASC, entry_id ASC"
                        ),
                        &[&created_at],
                    )
                    .await?
            } else {
                client
                    .query(
                        &format!(
                            "SELECT shard_id, entry_id, operation_json, created_at FROM {table} ORDER BY created_at ASC, shard_id ASC, entry_id ASC"
                        ),
                        &[],
                    )
                    .await?
            };
            for row in partition_rows {
                rows.push((
                    row.get::<_, DateTime<Utc>>(3),
                    row.get::<_, i32>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, String>(2),
                ));
            }
        }
        rows.sort_by(|left, right| (left.0, left.1, left.2).cmp(&(right.0, right.1, right.2)));
        for row in rows {
            let operation = serde_json::from_str::<JournalOperation>(&row.3)?;
            apply_journal_operation(&mut provider, operation)?;
        }
        Ok(provider.snapshot_store())
    }

    async fn postgres_shard_for_operation(
        &self,
        operation: &JournalOperation,
    ) -> Result<u32, WorkflowError> {
        match operation {
            JournalOperation::ClaimShard(input) => Ok(input.shard_id),
            JournalOperation::HeartbeatShard { shard_id, .. }
            | JournalOperation::ReleaseShard { shard_id, .. } => Ok(*shard_id),
            JournalOperation::ReleaseActivation { activation_id, .. } => self
                .shard_for_activation(activation_id)
                .await
                .ok_or_else(|| WorkflowError::new("unknown activation shard for release")),
            JournalOperation::ClaimShardTasks { session, .. } => Ok(session.shard_id),
            JournalOperation::ClaimShardAndTasks { claim, .. } => Ok(claim.shard_id),
            JournalOperation::CreateInstance(input) => Ok(input.partition_shard),
            JournalOperation::CreateChildInstance(input) => Ok(input.partition_shard),
            JournalOperation::AppendSignal(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::CancelChild(input) => {
                self.shard_for_ref(&input.parent_workflow_id, &input.parent_run_id)
                    .await
            }
            JournalOperation::ReserveEffect(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::PutEffectRecord {
                workflow_id,
                run_id,
                ..
            } => self.shard_for_ref(workflow_id, run_id).await,
            JournalOperation::HeartbeatEffect(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::CompleteEffect(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::FailEffect(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::CommitCheckpoint(input) => {
                self.shard_for_ref(&input.workflow_id, &input.run_id).await
            }
            JournalOperation::CommitActivations(inputs) => {
                if let Some(input) = inputs.first() {
                    self.shard_for_ref(&input.workflow_id, &input.run_id).await
                } else {
                    Err(WorkflowError::new("empty activation commit batch"))
                }
            }
            JournalOperation::RecordActivationFailures(inputs) => {
                if let Some(input) = inputs.first() {
                    self.shard_for_ref(&input.workflow_id, &input.run_id).await
                } else {
                    Err(WorkflowError::new("empty activation failure batch"))
                }
            }
        }
    }

    async fn shard_for_ref(&self, workflow_id: &str, run_id: &str) -> Result<u32, WorkflowError> {
        let ref_ = InstanceRef::new(workflow_id.to_string(), run_id.to_string());
        if let Some(shard_id) = self.inner.directory_get(&ref_)? {
            return Ok(shard_id);
        }
        self.inner
            .load_instance(
                &ref_,
                LoadInstanceOptions {
                    include_effects: false,
                },
            )
            .await?
            .map(|instance| instance.partition_shard)
            .ok_or_else(|| {
                WorkflowError::new(format!(
                    "unknown workflow instance for shard routing: {workflow_id}/{run_id}"
                ))
            })
    }

    async fn shard_for_activation(&self, activation_id: &str) -> Option<u32> {
        self.inner.snapshot_store().await.ok().and_then(|store| {
            store
                .tasks
                .values()
                .find(|task| task.activation_id == activation_id)
                .map(|task| task.partition_shard)
        })
    }

    fn postgres_table(&self, base: &str, partition: u32) -> String {
        format!(
            "{}.{}",
            quote_postgres_identifier(&self.schema),
            quote_postgres_identifier(&format!(
                "{}_{}",
                base,
                postgres_partition_suffix(partition)
            ))
        )
    }

    fn postgres_head_table_for_shard(&self, shard_id: u32) -> String {
        self.postgres_table(
            "shard_heads",
            self.postgres_physical_partition_for_shard(shard_id),
        )
    }

    fn postgres_journal_table_for_shard(&self, shard_id: u32) -> String {
        self.postgres_table(
            "shard_journal",
            self.postgres_physical_partition_for_shard(shard_id),
        )
    }

    fn postgres_snapshot_table_for_shard(&self, shard_id: u32) -> String {
        self.postgres_table(
            "shard_snapshots",
            self.postgres_physical_partition_for_shard(shard_id),
        )
    }

    fn postgres_physical_partition_for_shard(&self, shard_id: u32) -> u32 {
        shard_id % self.physical_partitions
    }
}

#[async_trait]
impl DurabilityProvider for PostgresDurabilityProvider {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::claim_shard(&self.inner, input.clone()).await?;
        if output.is_some() {
            self.append_postgres_operation(JournalOperation::ClaimShard(input))
                .await?;
        }
        Ok(output)
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        let output = <ShardRouter as DurabilityProvider>::claim_shard_tasks(
            &self.inner,
            claim.clone(),
            input.clone(),
        )
        .await?;
        if output.is_some() {
            self.append_postgres_operation(JournalOperation::ClaimShardAndTasks { claim, input })
                .await?;
        }
        Ok(output)
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        Arc::new(PostgresShardSession {
            provider: self.clone(),
            inner: <ShardRouter as DurabilityProvider>::open_shard(&self.inner, input),
        })
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::create_instance(&self.inner, input.clone())
                .await?;
        self.append_postgres_operation(JournalOperation::CreateInstance(input))
            .await?;
        Ok(output)
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        if let Some(parent) = <ShardRouter as DurabilityProvider>::load_instance(
            &self.inner,
            &InstanceRef::new(
                input.parent_workflow_id.clone(),
                input.parent_run_id.clone(),
            ),
            LoadInstanceOptions {
                include_effects: false,
            },
        )
        .await?
        {
            if input.partition_shard != parent.partition_shard {
                return Err(WorkflowError::new(
                    "Postgres provider requires local child workflow starts to be shard-affine",
                ));
            }
        }
        let output =
            <ShardRouter as DurabilityProvider>::create_child_instance(&self.inner, input.clone())
                .await?;
        self.append_postgres_operation(JournalOperation::CreateChildInstance(input))
            .await?;
        Ok(output)
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::load_instance(&self.inner, ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::append_signal(&self.inner, input.clone()).await?;
        self.append_postgres_operation(JournalOperation::AppendSignal(input))
            .await?;
        Ok(output)
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::cancel_child(&self.inner, input.clone()).await?;
        self.append_postgres_operation(JournalOperation::CancelChild(input))
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::get_or_reserve_effect(&self.inner, input.clone())
                .await?;
        if let EffectReservation::Reserved { effect_id, .. } = &output {
            let effect = reserved_effect_record_from_router(
                &self.inner,
                &input.workflow_id,
                &input.run_id,
                effect_id,
            )
            .await?;
            self.append_postgres_operation(JournalOperation::PutEffectRecord {
                workflow_id: input.workflow_id,
                run_id: input.run_id,
                effect,
            })
            .await?;
        }
        Ok(output)
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.inner, input.clone()).await?;
        self.append_postgres_operation(JournalOperation::HeartbeatEffect(input))
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::complete_effect(&self.inner, input.clone()).await?;
        self.append_postgres_operation(JournalOperation::CompleteEffect(input))
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::fail_effect(&self.inner, input.clone()).await?;
        self.append_postgres_operation(JournalOperation::FailEffect(input))
            .await?;
        Ok(output)
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let parent = <ShardRouter as DurabilityProvider>::load_instance(
            &self.inner,
            &InstanceRef::new(input.workflow_id.clone(), input.run_id.clone()),
            LoadInstanceOptions {
                include_effects: false,
            },
        )
        .await?;
        if let Some(parent) = parent {
            if input
                .child_starts
                .iter()
                .any(|start| start.partition_shard != parent.partition_shard)
            {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: input.expected_sequence,
                    reason: Some("cross_shard_child_start".to_string()),
                    retryable: Some(false),
                    error: Some(SerializedError {
                            name: None,
                            message: "Postgres provider requires commit-local children to stay on the parent shard".to_string(),
                    }),
                });
            }
        }
        let output =
            <ShardRouter as DurabilityProvider>::commit_checkpoint(&self.inner, input.clone())
                .await?;
        if output.ok {
            self.append_postgres_operation(JournalOperation::CommitCheckpoint(input))
                .await?;
        }
        Ok(output)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let output =
            <ShardRouter as DurabilityProvider>::commit_activations(&self.inner, inputs.clone())
                .await?;
        if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
            self.append_postgres_operation(JournalOperation::CommitActivations(inputs))
                .await?;
        }
        Ok(output)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        <ShardRouter as DurabilityProvider>::record_activation_failures(
            &self.inner,
            inputs.clone(),
        )
        .await?;
        self.append_postgres_operation(JournalOperation::RecordActivationFailures(inputs))
            .await
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_instances(&self.inner).await
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        self.inner.shutdown().await
    }
}

#[derive(Clone)]
struct PostgresShardSession {
    provider: PostgresDurabilityProvider,
    inner: Arc<dyn ShardDurabilitySession>,
}

#[async_trait]
impl ShardDurabilitySession for PostgresShardSession {
    fn shard_id(&self) -> u32 {
        self.inner.shard_id()
    }

    fn owner_id(&self) -> Option<&str> {
        self.inner.owner_id()
    }

    fn lease_epoch(&self) -> Option<u64> {
        self.inner.lease_epoch()
    }

    async fn claim_tasks(
        &self,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        let session = OpenShardInput {
            shard_id: self.shard_id(),
            owner_id: self.owner_id().map(str::to_string),
            lease_epoch: self.lease_epoch(),
        };
        let output = self.inner.claim_tasks(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::ClaimShardTasks { session, input },
            )
            .await?;
        Ok(output)
    }

    async fn read_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.inner.read_instance(ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let output = self.inner.append_signal(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::AppendSignal(input),
            )
            .await?;
        Ok(output)
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        self.inner.cancel_child(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::CancelChild(input),
            )
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let output = self.inner.get_or_reserve_effect(input.clone()).await?;
        if let EffectReservation::Reserved { effect_id, .. } = &output {
            let effect = self
                .inner
                .read_instance(
                    &InstanceRef::new(input.workflow_id.clone(), input.run_id.clone()),
                    LoadInstanceOptions {
                        include_effects: true,
                    },
                )
                .await?
                .and_then(|instance| {
                    instance
                        .effects
                        .into_iter()
                        .find(|effect| effect.effect_id == *effect_id)
                })
                .ok_or_else(|| {
                    WorkflowError::new(format!("reserved effect missing: {effect_id}"))
                })?;
            self.provider
                .append_postgres_operation_for_shard(
                    self.shard_id(),
                    JournalOperation::PutEffectRecord {
                        workflow_id: input.workflow_id,
                        run_id: input.run_id,
                        effect,
                    },
                )
                .await?;
        }
        Ok(output)
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        self.inner.heartbeat_effect(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::HeartbeatEffect(input),
            )
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        self.inner.complete_effect(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::CompleteEffect(input),
            )
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let output = self.inner.fail_effect(input.clone()).await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::FailEffect(input),
            )
            .await?;
        Ok(output)
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let output = self.inner.commit_checkpoint(input.clone()).await?;
        if output.ok {
            self.provider
                .append_postgres_operation_for_shard(
                    self.shard_id(),
                    JournalOperation::CommitCheckpoint(input),
                )
                .await?;
        }
        Ok(output)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let output = self.inner.commit_activations(inputs.clone()).await?;
        if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
            self.provider
                .append_postgres_operation_for_shard(
                    self.shard_id(),
                    JournalOperation::CommitActivations(inputs),
                )
                .await?;
        }
        Ok(output)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        self.inner
            .record_activation_failures(inputs.clone())
            .await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::RecordActivationFailures(inputs),
            )
            .await
    }

    async fn release_activation(
        &self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError> {
        self.inner
            .release_activation(activation_id, worker_id)
            .await?;
        self.provider
            .append_postgres_operation_for_shard(
                self.shard_id(),
                JournalOperation::ReleaseActivation {
                    activation_id: activation_id.to_string(),
                    worker_id: worker_id.to_string(),
                },
            )
            .await
    }

    async fn heartbeat(&self, now: DateTime<Utc>, lease_ms: u64) -> Result<(), WorkflowError> {
        self.inner.heartbeat(now, lease_ms).await?;
        if let Some(owner_id) = self.owner_id() {
            self.provider
                .append_postgres_operation_for_shard(
                    self.shard_id(),
                    JournalOperation::HeartbeatShard {
                        shard_id: self.shard_id(),
                        owner_id: owner_id.to_string(),
                        now,
                        lease_ms,
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn release(&self) -> Result<(), WorkflowError> {
        self.inner.release().await?;
        if let Some(owner_id) = self.owner_id() {
            self.provider
                .append_postgres_operation_for_shard(
                    self.shard_id(),
                    JournalOperation::ReleaseShard {
                        shard_id: self.shard_id(),
                        owner_id: owner_id.to_string(),
                    },
                )
                .await?;
        }
        Ok(())
    }
}

fn normalize_postgres_schema(schema: Option<String>) -> Result<String, WorkflowError> {
    let schema = schema.unwrap_or_else(|| format!("durable_{}", Uuid::new_v4().simple()));
    let mut chars = schema.chars();
    let Some(first) = chars.next() else {
        return Err(WorkflowError::new(
            "PostgresDurabilityProvider schema must be non-empty",
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(WorkflowError::new(
            "PostgresDurabilityProvider schema must be a valid identifier",
        ));
    }
    Ok(schema)
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn postgres_partition_suffix(partition: u32) -> String {
    format!("p{partition:02}")
}

#[derive(Clone)]
struct ShardRouterSession {
    router: ShardRouter,
    handle: ShardActorHandle,
    shard_id: u32,
    owner_id: Option<String>,
    lease_epoch: Option<u64>,
}

impl ShardRouterSession {
    async fn execute(&self, operation: EngineOperation) -> Result<EngineOutput, WorkflowError> {
        execute_on_actor(self.shard_id, &self.handle, operation).await
    }
}

#[async_trait]
impl ShardDurabilitySession for ShardRouterSession {
    fn shard_id(&self) -> u32 {
        self.shard_id
    }

    fn owner_id(&self) -> Option<&str> {
        self.owner_id.as_deref()
    }

    fn lease_epoch(&self) -> Option<u64> {
        self.lease_epoch
    }

    async fn claim_tasks(
        &self,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        let output = self
            .execute(EngineOperation::ClaimTasks {
                session: OpenShardInput {
                    shard_id: self.shard_id,
                    owner_id: self.owner_id.clone(),
                    lease_epoch: self.lease_epoch,
                },
                input,
            })
            .await?;
        let EngineOutput::ClaimTasks(output) = output else {
            unreachable!("claim tasks operation returned wrong output");
        };
        Ok(output)
    }

    async fn read_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.router.load_instance_internal(ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        <ShardRouter as DurabilityProvider>::append_signal(&self.router, input).await
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::cancel_child(&self.router, input).await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        <ShardRouter as DurabilityProvider>::get_or_reserve_effect(&self.router, input).await
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.router, input).await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::complete_effect(&self.router, input).await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        <ShardRouter as DurabilityProvider>::fail_effect(&self.router, input).await
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let output = self
            .execute(EngineOperation::CommitCheckpoint {
                session: Some(OpenShardInput {
                    shard_id: self.shard_id,
                    owner_id: self.owner_id.clone(),
                    lease_epoch: self.lease_epoch,
                }),
                input,
            })
            .await?;
        let EngineOutput::CommitCheckpoint {
            result,
            instances,
            invalidate_cache,
        } = output
        else {
            unreachable!("commit checkpoint operation returned wrong output");
        };
        if invalidate_cache {
            self.router.cache_clear()?;
        }
        self.router.cache_insert_many(instances)?;
        Ok(result)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let output = self
            .execute(EngineOperation::CommitActivations {
                session: Some(OpenShardInput {
                    shard_id: self.shard_id,
                    owner_id: self.owner_id.clone(),
                    lease_epoch: self.lease_epoch,
                }),
                inputs,
            })
            .await?;
        let EngineOutput::CommitActivations {
            result,
            instances,
            invalidate_cache,
        } = output
        else {
            unreachable!("commit activations operation returned wrong output");
        };
        if invalidate_cache {
            self.router.cache_clear()?;
        }
        self.router.cache_insert_many(instances)?;
        Ok(result)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::record_activation_failures(&self.router, inputs).await
    }

    async fn release_activation(
        &self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError> {
        self.execute(EngineOperation::ReleaseActivation {
            activation_id: activation_id.to_string(),
            worker_id: worker_id.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn heartbeat(&self, now: DateTime<Utc>, lease_ms: u64) -> Result<(), WorkflowError> {
        if let Some(owner_id) = &self.owner_id {
            self.execute(EngineOperation::HeartbeatShard {
                shard_id: self.shard_id,
                owner_id: owner_id.clone(),
                now,
                lease_ms,
            })
            .await?;
        }
        Ok(())
    }

    async fn release(&self) -> Result<(), WorkflowError> {
        if let Some(owner_id) = &self.owner_id {
            self.execute(EngineOperation::ReleaseShard {
                shard_id: self.shard_id,
                owner_id: owner_id.clone(),
            })
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DurabilityProvider for ShardRouter {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        let shard_id = input.shard_id;
        let output = self
            .execute(shard_id, EngineOperation::ClaimShard(input))
            .await?;
        let EngineOutput::OptionalShardLease(output) = output else {
            unreachable!("claim shard operation returned wrong output");
        };
        Ok(output)
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        let shard_id = claim.shard_id;
        let output = self
            .execute(
                shard_id,
                EngineOperation::ClaimShardAndTasks { claim, input },
            )
            .await?;
        let EngineOutput::ClaimShardAndTasks(output) = output else {
            unreachable!("claim shard and tasks operation returned wrong output");
        };
        Ok(output)
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        let handle = self
            .ensure_actor(input.shard_id)
            .expect("failed to open shard actor");
        Arc::new(ShardRouterSession {
            router: self.clone(),
            handle,
            shard_id: input.shard_id,
            owner_id: input.owner_id,
            lease_epoch: input.lease_epoch,
        })
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        let shard_id = input.partition_shard;
        let output = self
            .execute(shard_id, EngineOperation::CreateInstance(input))
            .await?;
        let EngineOutput::InstanceCreated {
            ref_: output,
            instance,
        } = output
        else {
            unreachable!("create instance operation returned wrong output");
        };
        self.directory_insert(&output, shard_id)?;
        self.cache_insert(instance)?;
        Ok(output)
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        let parent = InstanceRef::new(
            input.parent_workflow_id.clone(),
            input.parent_run_id.clone(),
        );
        let parent_shard = self.shard_for_ref(&parent).await?;
        if input.partition_shard != parent_shard {
            return Err(WorkflowError::new(
                "local child workflow starts must be shard-affine",
            ));
        }
        let child_ref = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let output = self
            .execute(parent_shard, EngineOperation::CreateChildInstance(input))
            .await?;
        let EngineOutput::ChildCreated {
            handle: output,
            instance,
        } = output
        else {
            unreachable!("create child operation returned wrong output");
        };
        self.directory_insert(&child_ref, parent_shard)?;
        self.cache_insert(instance)?;
        Ok(output)
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.load_instance_internal(ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let shard_id = self
            .shard_for_ref(&InstanceRef::new(
                input.workflow_id.clone(),
                input.run_id.clone(),
            ))
            .await?;
        let output = self
            .execute(shard_id, EngineOperation::AppendSignal(input))
            .await?;
        let EngineOutput::Signal(output) = output else {
            unreachable!("append signal operation returned wrong output");
        };
        Ok(output)
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        let parent_ref = InstanceRef::new(
            input.parent_workflow_id.clone(),
            input.parent_run_id.clone(),
        );
        let child_ref = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let shard_id = self.shard_for_ref(&parent_ref).await?;
        self.execute(shard_id, EngineOperation::CancelChild(input))
            .await?;
        self.cache_remove(&parent_ref)?;
        self.cache_remove(&child_ref)?;
        Ok(())
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let ref_ = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let shard_id = self.shard_for_ref(&ref_).await?;
        let output = self
            .execute(shard_id, EngineOperation::ReserveEffect(input))
            .await?;
        let EngineOutput::EffectReservation(output) = output else {
            unreachable!("reserve effect operation returned wrong output");
        };
        self.cache_remove(&ref_)?;
        Ok(output)
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        let ref_ = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let shard_id = self.shard_for_ref(&ref_).await?;
        self.execute(shard_id, EngineOperation::HeartbeatEffect(input))
            .await?;
        self.cache_remove(&ref_)?;
        Ok(())
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        let ref_ = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let shard_id = self.shard_for_ref(&ref_).await?;
        self.execute(shard_id, EngineOperation::CompleteEffect(input))
            .await?;
        self.cache_remove(&ref_)?;
        Ok(())
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let ref_ = InstanceRef::new(input.workflow_id.clone(), input.run_id.clone());
        let shard_id = self.shard_for_ref(&ref_).await?;
        let output = self
            .execute(shard_id, EngineOperation::FailEffect(input))
            .await?;
        let EngineOutput::FailEffect(output) = output else {
            unreachable!("fail effect operation returned wrong output");
        };
        self.cache_remove(&ref_)?;
        Ok(output)
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let shard_id = self
            .shard_for_ref(&InstanceRef::new(
                input.workflow_id.clone(),
                input.run_id.clone(),
            ))
            .await?;
        if input
            .child_starts
            .iter()
            .any(|start| start.partition_shard != shard_id)
        {
            return Ok(CommitCheckpointResult {
                ok: false,
                sequence: input.expected_sequence,
                reason: Some("cross_shard_child_start".to_string()),
                retryable: Some(false),
                error: Some(SerializedError {
                    name: None,
                    message: "local child workflow starts must stay on the parent shard"
                        .to_string(),
                }),
            });
        }
        let child_refs = input
            .child_starts
            .iter()
            .map(|start| InstanceRef::new(start.workflow_id.clone(), start.run_id.clone()))
            .collect::<Vec<_>>();
        let output = self
            .execute(
                shard_id,
                EngineOperation::CommitCheckpoint {
                    session: None,
                    input,
                },
            )
            .await?;
        let EngineOutput::CommitCheckpoint {
            result: output,
            instances,
            invalidate_cache,
        } = output
        else {
            unreachable!("commit checkpoint operation returned wrong output");
        };
        if invalidate_cache {
            self.cache_clear()?;
        }
        self.cache_insert_many(instances)?;
        if output.ok {
            for ref_ in child_refs {
                self.directory_insert(&ref_, shard_id)?;
            }
        }
        Ok(output)
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        if inputs.is_empty() {
            return Ok(CommitActivationsResult {
                results: Vec::new(),
            });
        }
        let shard_id = self
            .shard_for_ref(&InstanceRef::new(
                inputs[0].workflow_id.clone(),
                inputs[0].run_id.clone(),
            ))
            .await?;
        let output = self
            .execute(
                shard_id,
                EngineOperation::CommitActivations {
                    session: None,
                    inputs,
                },
            )
            .await?;
        let EngineOutput::CommitActivations {
            result: output,
            instances,
            invalidate_cache,
        } = output
        else {
            unreachable!("commit activations operation returned wrong output");
        };
        if invalidate_cache {
            self.cache_clear()?;
        }
        self.cache_insert_many(instances)?;
        Ok(output)
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        let refs = inputs
            .iter()
            .map(|input| InstanceRef::new(input.workflow_id.clone(), input.run_id.clone()))
            .collect::<Vec<_>>();
        let shard_id = self
            .shard_for_ref(&InstanceRef::new(
                inputs[0].workflow_id.clone(),
                inputs[0].run_id.clone(),
            ))
            .await?;
        self.execute(shard_id, EngineOperation::RecordActivationFailures(inputs))
            .await?;
        for ref_ in refs {
            self.cache_remove(&ref_)?;
        }
        Ok(())
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        Ok(self
            .snapshot_store()
            .await?
            .instances
            .into_values()
            .collect())
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        Ok(self.snapshot_store().await?.signals)
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        Ok(self.snapshot_store().await?.children)
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        ShardRouter::shutdown(self).await
    }
}

#[async_trait]
impl DurabilityProvider for NullDurabilityProvider {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::claim_shard(&self.inner, input).await
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::claim_shard_tasks(&self.inner, claim, input).await
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        <ShardRouter as DurabilityProvider>::open_shard(&self.inner, input)
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        <ShardRouter as DurabilityProvider>::create_instance(&self.inner, input).await
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        <ShardRouter as DurabilityProvider>::create_child_instance(&self.inner, input).await
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::load_instance(&self.inner, ref_, options).await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        <ShardRouter as DurabilityProvider>::append_signal(&self.inner, input).await
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::cancel_child(&self.inner, input).await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        <ShardRouter as DurabilityProvider>::get_or_reserve_effect(&self.inner, input).await
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.inner, input).await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::complete_effect(&self.inner, input).await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        <ShardRouter as DurabilityProvider>::fail_effect(&self.inner, input).await
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        <ShardRouter as DurabilityProvider>::commit_checkpoint(&self.inner, input).await
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        <ShardRouter as DurabilityProvider>::commit_activations(&self.inner, inputs).await
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        <ShardRouter as DurabilityProvider>::record_activation_failures(&self.inner, inputs).await
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_instances(&self.inner).await
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        self.inner.shutdown().await
    }
}

fn replace_tasks_for_instance(state: &mut Store, instance: &PersistedInstance) {
    delete_tasks_for_ref(state, &instance.workflow_id, &instance.run_id);
    if instance.status != PersistedStatus::Running {
        return;
    }
    for wait in &instance.waits {
        insert_task_for_wait(state, instance, wait);
    }
}

fn refresh_signal_tasks_for_instance(state: &mut Store, instance: &PersistedInstance) {
    let ids = state
        .tasks
        .values()
        .filter(|task| {
            task.workflow_id == instance.workflow_id
                && task.run_id == instance.run_id
                && task.sequence == instance.sequence
                && matches!(task.event, Some(ReadyEvent::Signal { .. }))
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    for id in ids {
        delete_task(state, &id);
    }
    for wait in &instance.waits {
        if matches!(wait, DurableWait::Signal { .. }) {
            insert_task_for_wait(state, instance, wait);
        }
    }
}

fn refresh_child_tasks_for_instance(state: &mut Store, instance: &PersistedInstance) {
    let ids = state
        .tasks
        .values()
        .filter(|task| {
            task.workflow_id == instance.workflow_id
                && task.run_id == instance.run_id
                && task.sequence == instance.sequence
                && matches!(task.event, Some(ReadyEvent::Child { .. }))
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    for id in ids {
        delete_task(state, &id);
    }
    for wait in &instance.waits {
        if matches!(wait, DurableWait::Child { .. }) {
            insert_task_for_wait(state, instance, wait);
        }
    }
}

fn refresh_migration_tasks(
    state: &mut Store,
    shard_id: u32,
    now: DateTime<Utc>,
    workflows: &HashMap<String, u32>,
) {
    let instances = state.instances.values().cloned().collect::<Vec<_>>();
    for instance in instances {
        if instance.partition_shard != shard_id || instance.status != PersistedStatus::Running {
            continue;
        }
        let Some(worker_version) = workflows.get(&instance.workflow_name).copied() else {
            continue;
        };
        if instance.workflow_version >= worker_version {
            let ids = state
                .tasks
                .values()
                .filter(|task| {
                    task.workflow_id == instance.workflow_id
                        && task.run_id == instance.run_id
                        && task.sequence == instance.sequence
                        && task.kind == MemoryTaskKind::Migration
                })
                .map(|task| task.task_id.clone())
                .collect::<Vec<_>>();
            for id in ids {
                delete_task(state, &id);
            }
            continue;
        }
        let has_current_task = state.tasks.values().any(|task| {
            task.workflow_id == instance.workflow_id
                && task.run_id == instance.run_id
                && task.sequence == instance.sequence
        });
        if has_current_task {
            continue;
        }
        insert_task(
            state,
            &instance,
            InsertTaskInput {
                kind: MemoryTaskKind::Migration,
                event_id: format!("migration-{worker_version}"),
                ready_at: now,
                wait_name: None,
                event: None,
                task_suffix: None,
                sort_key: task_sort_key(&[
                    format_time(now),
                    "migration".to_string(),
                    instance.workflow_name.clone(),
                    instance.workflow_id.clone(),
                    instance.run_id.clone(),
                ]),
            },
        );
    }
}

fn insert_task_for_wait(state: &mut Store, instance: &PersistedInstance, wait: &DurableWait) {
    match wait {
        DurableWait::Run { name, ready_at } => {
            insert_task(
                state,
                instance,
                InsertTaskInput {
                    kind: MemoryTaskKind::Run,
                    event_id: name.clone(),
                    ready_at: *ready_at,
                    wait_name: None,
                    event: None,
                    task_suffix: None,
                    sort_key: task_sort_key(&[
                        format_time(*ready_at),
                        "run".to_string(),
                        name.clone(),
                        instance.workflow_id.clone(),
                        instance.run_id.clone(),
                    ]),
                },
            );
        }
        DurableWait::Timer { name, fire_at } => {
            insert_task(
                state,
                instance,
                InsertTaskInput {
                    kind: MemoryTaskKind::Event,
                    event_id: format!("{}:{}", name, format_time(*fire_at)),
                    ready_at: *fire_at,
                    wait_name: Some(name.clone()),
                    event: Some(ReadyEvent::Timer {
                        fired_at: *fire_at,
                        occurred_at: *fire_at,
                    }),
                    task_suffix: None,
                    sort_key: task_sort_key(&[
                        format_time(*fire_at),
                        "timer".to_string(),
                        name.clone(),
                        format!("{}:{}", name, format_time(*fire_at)),
                    ]),
                },
            );
        }
        DurableWait::Signal { name, r#type, .. } => {
            let Some(signal) = state
                .signals
                .iter()
                .filter(|candidate| {
                    candidate.workflow_id == instance.workflow_id
                        && candidate.run_id == instance.run_id
                        && candidate.r#type == *r#type
                        && candidate.consumed_by_sequence.is_none()
                })
                .min_by(compare_signals)
                .cloned()
            else {
                return;
            };
            insert_task(
                state,
                instance,
                InsertTaskInput {
                    kind: MemoryTaskKind::Event,
                    event_id: signal.signal_id.clone(),
                    ready_at: signal.received_at,
                    wait_name: Some(name.clone()),
                    event: Some(ReadyEvent::Signal {
                        signal_id: signal.signal_id.clone(),
                        payload: signal.payload.clone(),
                        occurred_at: signal.received_at,
                    }),
                    task_suffix: Some(name.clone()),
                    sort_key: task_sort_key(&[
                        format_time(signal.received_at),
                        "signal".to_string(),
                        name.clone(),
                        signal.signal_id,
                    ]),
                },
            );
        }
        DurableWait::Child {
            name,
            workflow_name,
            workflow_version,
            workflow_id,
            run_id,
        } => {
            let Some(child) = state
                .children
                .iter()
                .filter(|record| {
                    record.parent_workflow_id == instance.workflow_id
                        && record.parent_run_id == instance.run_id
                        && record.workflow_name == *workflow_name
                        && record.workflow_version == *workflow_version
                        && record.workflow_id == *workflow_id
                        && record.run_id == *run_id
                        && record.status != ChildStatus::Started
                        && record.delivered_by_sequence.is_none()
                })
                .min_by(|left, right| {
                    (
                        left.completed_at.unwrap_or(instance.updated_at),
                        left.child_record_id.as_str(),
                    )
                        .cmp(&(
                            right.completed_at.unwrap_or(instance.updated_at),
                            right.child_record_id.as_str(),
                        ))
                })
                .cloned()
            else {
                return;
            };
            let occurred_at = child.completed_at.unwrap_or(instance.updated_at);
            insert_task(
                state,
                instance,
                InsertTaskInput {
                    kind: MemoryTaskKind::Event,
                    event_id: child.child_record_id.clone(),
                    ready_at: occurred_at,
                    wait_name: Some(name.clone()),
                    event: Some(ReadyEvent::Child {
                        child_record_id: child.child_record_id.clone(),
                        occurred_at,
                        event: match child.status {
                            ChildStatus::Completed => ChildEventValue::Ok {
                                output: child.output.clone().unwrap_or(JsonValue::Null),
                            },
                            ChildStatus::Failed | ChildStatus::Started | ChildStatus::Abandoned => {
                                ChildEventValue::Err {
                                    error: child.error.clone().unwrap_or(SerializedError {
                                        name: None,
                                        message: "child failed".to_string(),
                                    }),
                                }
                            }
                        },
                    }),
                    task_suffix: None,
                    sort_key: task_sort_key(&[
                        format_time(occurred_at),
                        "child".to_string(),
                        name.clone(),
                        child.child_record_id,
                    ]),
                },
            );
        }
    }
}

struct InsertTaskInput {
    kind: MemoryTaskKind,
    event_id: String,
    ready_at: DateTime<Utc>,
    wait_name: Option<String>,
    event: Option<ReadyEvent>,
    task_suffix: Option<String>,
    sort_key: String,
}

fn insert_task(state: &mut Store, instance: &PersistedInstance, input: InsertTaskInput) {
    let activation_kind = match &input.event {
        Some(ReadyEvent::Signal { .. }) => "signal",
        Some(ReadyEvent::Timer { .. }) => "timer",
        Some(ReadyEvent::Child { .. }) => "child",
        None => match input.kind {
            MemoryTaskKind::Migration => "migration",
            MemoryTaskKind::Run => "run",
            MemoryTaskKind::Event => "event",
        },
    };
    let activation_id = activation_id(instance, activation_kind, &input.event_id);
    let task_id = input
        .task_suffix
        .map(|suffix| format!("{activation_id}/{suffix}"))
        .unwrap_or_else(|| activation_id.clone());
    delete_task(state, &task_id);
    state
        .task_order
        .insert((input.sort_key.clone(), task_id.clone()));
    state.tasks.insert(
        task_id.clone(),
        MemoryTask {
            task_id,
            activation_id,
            workflow_name: instance.workflow_name.clone(),
            workflow_version: instance.workflow_version,
            workflow_id: instance.workflow_id.clone(),
            run_id: instance.run_id.clone(),
            partition_shard: instance.partition_shard,
            sequence: instance.sequence,
            kind: input.kind,
            wait_name: input.wait_name,
            event: input.event,
            ready_at: input.ready_at,
            sort_key: input.sort_key,
            claim_owner_id: None,
            claim_epoch: None,
            lease_until: None,
            blocked_until: None,
        },
    );
}

fn delete_tasks_for_ref(state: &mut Store, workflow_id: &str, run_id: &str) {
    let ids = state
        .tasks
        .values()
        .filter(|task| task.workflow_id == workflow_id && task.run_id == run_id)
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    for id in ids {
        delete_task(state, &id);
    }
}

fn delete_instance_records(state: &mut Store, workflow_id: &str, run_id: &str) {
    state.instances.remove(&instance_key(workflow_id, run_id));
    delete_tasks_for_ref(state, workflow_id, run_id);
    state
        .signals
        .retain(|signal| signal.workflow_id != workflow_id || signal.run_id != run_id);
    state.children.retain(|child| {
        (child.parent_workflow_id != workflow_id || child.parent_run_id != run_id)
            && (child.workflow_id != workflow_id || child.run_id != run_id)
    });
}

fn cancel_child_in_state(state: &mut Store, input: CancelChildInput) {
    let Some(index) = state.children.iter().position(|record| {
        record.parent_workflow_id == input.parent_workflow_id
            && record.parent_run_id == input.parent_run_id
            && record.workflow_id == input.workflow_id
            && record.run_id == input.run_id
            && record.status == ChildStatus::Started
    }) else {
        return;
    };

    let child_workflow_id = state.children[index].workflow_id.clone();
    let child_run_id = state.children[index].run_id.clone();
    let mut delete_child_tasks = false;
    if let Some(child_instance) = state
        .instances
        .get_mut(&instance_key(&child_workflow_id, &child_run_id))
    {
        if child_instance.status == PersistedStatus::Running {
            child_instance.status = PersistedStatus::Canceled;
            child_instance.cancel_reason = Some("Child canceled by parent".to_string());
            child_instance.waits.clear();
            child_instance.updated_at = input.now;
            delete_child_tasks = true;
        }
    }
    if delete_child_tasks {
        delete_tasks_for_ref(state, &child_workflow_id, &child_run_id);
    }

    let child = &mut state.children[index];
    child.status = ChildStatus::Failed;
    child.completed_at = Some(input.now);
    child.error = Some(SerializedError {
        name: Some("ChildCanceled".to_string()),
        message: "Child canceled by parent".to_string(),
    });

    if let Some(parent) = state
        .instances
        .get(&instance_key(
            &input.parent_workflow_id,
            &input.parent_run_id,
        ))
        .cloned()
    {
        if parent.status == PersistedStatus::Running {
            refresh_child_tasks_for_instance(state, &parent);
        }
    }
}

fn apply_parent_close_policy(
    state: &mut Store,
    parent_workflow_id: &str,
    parent_run_id: &str,
    now: DateTime<Utc>,
    delivered_by_sequence: u64,
) {
    let child_record_ids = state
        .children
        .iter()
        .filter(|record| {
            record.parent_workflow_id == parent_workflow_id
                && record.parent_run_id == parent_run_id
                && record.status == ChildStatus::Started
        })
        .map(|record| record.child_record_id.clone())
        .collect::<Vec<_>>();

    for child_record_id in child_record_ids {
        let Some(index) = state
            .children
            .iter()
            .position(|record| record.child_record_id == child_record_id)
        else {
            continue;
        };
        if state.children[index].parent_close_policy == ParentClosePolicy::Abandon {
            state.children[index].status = ChildStatus::Abandoned;
            state.children[index].delivered_by_sequence = Some(delivered_by_sequence);
            continue;
        }
        cancel_child_tree_for_parent_close(state, &child_record_id, now, delivered_by_sequence);
    }
}

fn cancel_child_tree_for_parent_close(
    state: &mut Store,
    child_record_id: &str,
    now: DateTime<Utc>,
    delivered_by_sequence: u64,
) {
    let Some(index) = state
        .children
        .iter()
        .position(|record| record.child_record_id == child_record_id)
    else {
        return;
    };
    let child_workflow_id = state.children[index].workflow_id.clone();
    let child_run_id = state.children[index].run_id.clone();

    let mut delete_child_tasks = false;
    if let Some(child_instance) = state
        .instances
        .get_mut(&instance_key(&child_workflow_id, &child_run_id))
    {
        if child_instance.status == PersistedStatus::Running {
            child_instance.status = PersistedStatus::Canceled;
            child_instance.cancel_reason =
                Some("Child canceled because parent canceled".to_string());
            child_instance.waits.clear();
            child_instance.updated_at = now;
            delete_child_tasks = true;
        }
    }
    if delete_child_tasks {
        delete_tasks_for_ref(state, &child_workflow_id, &child_run_id);
    }

    let child = &mut state.children[index];
    child.status = ChildStatus::Failed;
    child.completed_at = Some(now);
    child.error = Some(SerializedError {
        name: Some("ParentClosed".to_string()),
        message: "Child canceled because parent canceled".to_string(),
    });
    child.delivered_by_sequence = Some(delivered_by_sequence);

    let descendants = state
        .children
        .iter()
        .filter(|record| {
            record.parent_workflow_id == child_workflow_id
                && record.parent_run_id == child_run_id
                && record.status == ChildStatus::Started
        })
        .map(|record| record.child_record_id.clone())
        .collect::<Vec<_>>();
    for descendant_id in descendants {
        let Some(descendant_index) = state
            .children
            .iter()
            .position(|record| record.child_record_id == descendant_id)
        else {
            continue;
        };
        if state.children[descendant_index].parent_close_policy == ParentClosePolicy::Abandon {
            state.children[descendant_index].status = ChildStatus::Abandoned;
            state.children[descendant_index].delivered_by_sequence = Some(delivered_by_sequence);
            continue;
        }
        cancel_child_tree_for_parent_close(state, &descendant_id, now, delivered_by_sequence);
    }
}

fn delete_task(state: &mut Store, task_id: &str) {
    if let Some(task) = state.tasks.remove(task_id) {
        state
            .task_order
            .remove(&(task.sort_key.clone(), task.task_id.clone()));
        state
            .claimed_sequence_epochs
            .remove(&sequence_key_for_task(&task));
    }
}

fn rebuild_task_order(state: &mut Store) {
    state.task_order = state
        .tasks
        .values()
        .map(|task| (task.sort_key.clone(), task.task_id.clone()))
        .collect();
}

fn sequence_key_for_task(task: &MemoryTask) -> String {
    format!("{}\0{}\0{}", task.workflow_id, task.run_id, task.sequence)
}

fn task_sort_key(parts: &[String]) -> String {
    parts.join("\0")
}

fn deadline_from(now: DateTime<Utc>, timeout_ms: Option<u64>) -> Option<DateTime<Utc>> {
    timeout_ms.map(|timeout_ms| now + chrono::Duration::milliseconds(timeout_ms as i64))
}

fn assert_mutable_effect(
    effect: &EffectRecord,
    attempt_id: &str,
    worker_id: &str,
) -> Result<(), WorkflowError> {
    if effect.status != EffectStatus::Pending {
        return Err(WorkflowError::new(format!(
            "effect is already terminal: {}",
            effect.effect_id
        )));
    }
    if effect.attempt_id.as_deref() != Some(attempt_id)
        || effect.attempt_owner_id.as_deref() != Some(worker_id)
    {
        return Err(WorkflowError::new(format!(
            "lost effect attempt: {}/{}",
            effect.effect_id, attempt_id
        )));
    }
    Ok(())
}

fn retry_decision_for_effect(
    effect: &EffectRecord,
    error: &SerializedError,
    now: DateTime<Utc>,
    retryable: bool,
) -> FailEffectResult {
    let attempt = effect.attempt.unwrap_or(1);
    let max_attempts = effect.max_attempts.unwrap_or(1);
    let non_retryable = error.name.as_ref().is_some_and(|name| {
        effect
            .non_retryable_error_names
            .iter()
            .any(|candidate| candidate == name)
    });
    if !retryable || non_retryable || attempt >= max_attempts {
        return FailEffectResult::Failed;
    }
    let initial_interval_ms = effect.initial_interval_ms.unwrap_or(1_000);
    let max_interval_ms = effect.max_interval_ms.unwrap_or(30_000);
    let backoff_coefficient = effect.backoff_coefficient.unwrap_or(2).max(1);
    let multiplier = backoff_coefficient.saturating_pow(attempt.saturating_sub(1));
    let delay_ms = max_interval_ms.min(initial_interval_ms.saturating_mul(multiplier as u64));
    let next_attempt_at = now + chrono::Duration::milliseconds(delay_ms as i64);
    if let (Some(first), Some(max_elapsed_ms)) =
        (effect.first_attempt_started_at, effect.max_elapsed_ms)
    {
        let deadline = first + chrono::Duration::milliseconds(max_elapsed_ms as i64);
        if next_attempt_at > deadline {
            return FailEffectResult::Failed;
        }
    }
    FailEffectResult::RetryScheduled {
        next_attempt_at,
        next_attempt: attempt + 1,
    }
}

fn release_tasks_for_activation(
    state: &mut Store,
    activation_id: &str,
    blocked_until: Option<DateTime<Utc>>,
) {
    let mut sequences = Vec::new();
    for task in state.tasks.values_mut() {
        if task.activation_id == activation_id {
            task.claim_owner_id = None;
            task.claim_epoch = None;
            task.lease_until = None;
            task.blocked_until = blocked_until;
            sequences.push(sequence_key_for_task(task));
        }
    }
    for sequence in sequences {
        state.claimed_sequence_epochs.remove(&sequence);
    }
}

fn expire_activity_timeouts(
    state: &mut Store,
    now: DateTime<Utc>,
    activation_filter: Option<&str>,
    owner_filter: Option<&str>,
    shard_filter: Option<u32>,
) {
    let tasks = state.tasks.values().cloned().collect::<Vec<_>>();
    let mut releases = Vec::new();
    for instance in state.instances.values_mut() {
        for effect in &mut instance.effects {
            if effect.status != EffectStatus::Pending || effect.attempt_id.is_none() {
                continue;
            }
            if activation_filter.is_some_and(|activation_id| activation_id != effect.activation_id)
            {
                continue;
            }
            let task = tasks
                .iter()
                .find(|task| task.activation_id == effect.activation_id);
            if shard_filter
                .is_some_and(|shard_id| task.map(|task| task.partition_shard) != Some(shard_id))
            {
                continue;
            }
            if let Some(owner_id) = owner_filter {
                let owner_matches = task
                    .and_then(|task| task.claim_owner_id.as_deref())
                    .is_some_and(|claim_owner| claim_owner == owner_id)
                    && task
                        .and_then(|task| task.lease_until)
                        .is_some_and(|lease_until| lease_until >= now);
                if !owner_matches {
                    continue;
                }
            }
            let timeout_kind = if effect
                .start_to_close_deadline
                .is_some_and(|deadline| deadline <= now)
            {
                Some(ActivityTimeoutKind::StartToClose)
            } else if effect
                .heartbeat_deadline
                .is_some_and(|deadline| deadline <= now)
            {
                Some(ActivityTimeoutKind::Heartbeat)
            } else {
                None
            };
            let Some(timeout_kind) = timeout_kind else {
                continue;
            };
            let error = SerializedError {
                name: Some("ActivityTimeoutError".to_string()),
                message: match timeout_kind {
                    ActivityTimeoutKind::Heartbeat => {
                        format!("Activity {} failed due to heartbeat timeout", effect.key)
                    }
                    ActivityTimeoutKind::StartToClose => {
                        format!(
                            "Activity {} failed due to start-to-close timeout",
                            effect.key
                        )
                    }
                },
            };
            let decision = retry_decision_for_effect(effect, &error, now, true);
            effect.timed_out_at = Some(now);
            effect.timeout_kind = Some(timeout_kind);
            effect.error = Some(error.clone());
            effect.last_failure = Some(error);
            effect.attempt_id = None;
            effect.attempt_owner_id = None;
            effect.attempt_started_at = None;
            effect.start_to_close_deadline = None;
            effect.heartbeat_deadline = None;
            match decision {
                FailEffectResult::RetryScheduled {
                    next_attempt_at,
                    next_attempt,
                } => {
                    effect.status = EffectStatus::Pending;
                    effect.next_attempt_at = Some(next_attempt_at);
                    effect.attempt = Some(next_attempt);
                    releases.push((effect.activation_id.clone(), Some(next_attempt_at)));
                }
                FailEffectResult::Failed => {
                    effect.status = EffectStatus::Failed;
                    releases.push((effect.activation_id.clone(), None));
                }
            }
        }
    }
    for (activation_id, blocked_until) in releases {
        release_tasks_for_activation(state, &activation_id, blocked_until);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateInstanceInput {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub partition_shard: u32,
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub waits: Vec<DurableWait>,
    pub now: DateTime<Utc>,
    pub parent: Option<ParentLink>,
    pub conflict_policy: Option<ConflictPolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateChildInstanceInput {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub partition_shard: u32,
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub waits: Vec<DurableWait>,
    pub now: DateTime<Utc>,
    pub parent_workflow_id: String,
    pub parent_run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub lease_now: DateTime<Utc>,
    pub key: String,
    pub parent_close_policy: ParentClosePolicy,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChildHandleValue {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EffectReservation {
    Reserved {
        effect_id: String,
        idempotency_key: String,
        attempt: u32,
        attempt_id: String,
        heartbeat_details: Option<JsonValue>,
    },
    Completed {
        result: JsonValue,
    },
    Failed {
        error: SerializedError,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitCheckpointInput {
    pub workflow_id: String,
    pub run_id: String,
    pub expected_sequence: u64,
    pub activation_id: String,
    pub workflow_version: u32,
    pub next: InstanceStatusValue,
    pub waits: Vec<DurableWait>,
    pub now: DateTime<Utc>,
    pub consume_signal_id: Option<String>,
    pub consume_child_record_id: Option<String>,
    pub effects: Vec<CheckpointEffectMutation>,
    pub child_starts: Vec<CheckpointChildStart>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelChildInput {
    pub parent_workflow_id: String,
    pub parent_run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommitCheckpointResult {
    pub ok: bool,
    pub sequence: u64,
    pub reason: Option<String>,
    pub retryable: Option<bool>,
    pub error: Option<SerializedError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommitActivationsResult {
    pub results: Vec<CommitCheckpointResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordActivationFailureInput {
    pub workflow_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub now: DateTime<Utc>,
    pub effects: Vec<CheckpointEffectMutation>,
    pub release_activation: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CheckpointEffectMutation {
    Completed { key: String, result: JsonValue },
    Failed { key: String, error: SerializedError },
}

impl CheckpointEffectMutation {
    fn key(&self) -> &str {
        match self {
            Self::Completed { key, .. } | Self::Failed { key, .. } => key,
        }
    }

    fn to_record(
        &self,
        effect_id: String,
        activation_id: String,
        idempotency_key: String,
        now: DateTime<Utc>,
    ) -> EffectRecord {
        let (status, result, error) = match self {
            Self::Completed { result, .. } => (EffectStatus::Completed, Some(result.clone()), None),
            Self::Failed { error, .. } => (EffectStatus::Failed, None, Some(error.clone())),
        };
        EffectRecord {
            effect_id,
            activation_id,
            key: self.key().to_string(),
            idempotency_key,
            status,
            attempt: Some(1),
            attempt_id: None,
            attempt_owner_id: None,
            attempt_started_at: None,
            start_to_close_timeout_ms: None,
            start_to_close_deadline: None,
            heartbeat_timeout_ms: None,
            heartbeat_deadline: None,
            max_attempts: Some(1),
            max_elapsed_ms: None,
            initial_interval_ms: Some(1_000),
            max_interval_ms: Some(30_000),
            backoff_coefficient: Some(2),
            first_attempt_started_at: Some(now),
            next_attempt_at: None,
            last_failure: error.clone(),
            non_retryable_error_names: Vec::new(),
            timed_out_at: None,
            timeout_kind: None,
            result,
            error,
            heartbeat_at: None,
            heartbeat_details: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointChildStart {
    pub key: String,
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub partition_shard: u32,
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub waits: Vec<DurableWait>,
    pub parent_close_policy: ParentClosePolicy,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLease {
    pub shard_id: u32,
    pub owner_id: String,
    pub lease_until: DateTime<Utc>,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimShardInput {
    pub shard_id: u32,
    pub owner_id: String,
    pub now: DateTime<Utc>,
    pub lease_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenShardInput {
    pub shard_id: u32,
    pub owner_id: Option<String>,
    pub lease_epoch: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimShardTasksInput {
    pub workflows: HashMap<String, u32>,
    pub shard_count: u32,
    pub now: DateTime<Utc>,
    pub lease_ms: u64,
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimShardTasksResult {
    pub claims: Vec<ClaimedActivationWithInstance>,
    pub next_wake_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimedActivationWithInstance {
    pub activation: ClaimedActivation,
    pub instance: PersistedInstance,
    pub effects: Vec<EffectRecord>,
    pub lease: ActivationClaimLease,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ActivationClaimLease {
    Activation,
    Shard { shard_id: u32, epoch: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClaimedActivation {
    Migration {
        activation_id: String,
        workflow_name: String,
        workflow_id: String,
        run_id: String,
        sequence: u64,
        activation_time: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    },
    Run {
        activation_id: String,
        workflow_name: String,
        workflow_id: String,
        run_id: String,
        sequence: u64,
        activation_time: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    },
    Event {
        activation_id: String,
        workflow_name: String,
        workflow_id: String,
        run_id: String,
        sequence: u64,
        activation_time: DateTime<Utc>,
        wait_name: String,
        event: ReadyEvent,
        lease_until: DateTime<Utc>,
    },
}

impl ClaimedActivation {
    pub fn activation_id(&self) -> &str {
        match self {
            Self::Migration { activation_id, .. }
            | Self::Run { activation_id, .. }
            | Self::Event { activation_id, .. } => activation_id,
        }
    }

    pub fn sequence(&self) -> u64 {
        match self {
            Self::Migration { sequence, .. }
            | Self::Run { sequence, .. }
            | Self::Event { sequence, .. } => *sequence,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendSignalInput {
    pub workflow_id: String,
    pub run_id: String,
    pub r#type: String,
    pub payload: JsonValue,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadInstanceOptions {
    pub include_effects: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReserveEffectInput {
    pub workflow_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub key: String,
    pub now: DateTime<Utc>,
    pub options: ActivityOptions,
    pub max_attempts: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeartbeatEffectInput {
    pub workflow_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub effect_id: String,
    pub attempt_id: String,
    pub now: DateTime<Utc>,
    pub details: Option<JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteEffectInput {
    pub workflow_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub effect_id: String,
    pub attempt_id: String,
    pub result: JsonValue,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailEffectInput {
    pub workflow_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub worker_id: String,
    pub effect_id: String,
    pub attempt_id: String,
    pub error: SerializedError,
    pub now: DateTime<Utc>,
    pub retryable: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FailEffectResult {
    Failed,
    RetryScheduled {
        next_attempt_at: DateTime<Utc>,
        next_attempt: u32,
    },
}

#[async_trait]
pub trait ShardDurabilitySession: Send + Sync {
    fn shard_id(&self) -> u32;
    fn owner_id(&self) -> Option<&str>;
    fn lease_epoch(&self) -> Option<u64>;

    async fn claim_tasks(
        &self,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError>;

    async fn read_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError>;

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError>;

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError>;

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError>;

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError>;

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError>;

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError>;

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError>;

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let mut results = Vec::with_capacity(inputs.len());
        for input in inputs {
            results.push(self.commit_checkpoint(input).await?);
        }
        Ok(CommitActivationsResult { results })
    }

    async fn record_activation_failures(
        &self,
        _inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        Ok(())
    }

    async fn release_activation(
        &self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError>;

    async fn heartbeat(&self, now: DateTime<Utc>, lease_ms: u64) -> Result<(), WorkflowError>;
    async fn release(&self) -> Result<(), WorkflowError>;
}

#[async_trait]
pub trait DurabilityProvider: Send + Sync {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError>;

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        let Some(lease) = self.claim_shard(claim).await? else {
            return Ok(None);
        };
        let session = self.open_shard(OpenShardInput {
            shard_id: lease.shard_id,
            owner_id: Some(lease.owner_id.clone()),
            lease_epoch: Some(lease.lease_epoch),
        });
        let tasks = session.claim_tasks(input).await?;
        Ok(Some((lease, tasks)))
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession>;

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError>;

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError>;

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError>;

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError>;

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError>;

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError>;

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError>;

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError>;
    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError>;

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError>;

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let mut results = Vec::with_capacity(inputs.len());
        for input in inputs {
            results.push(self.commit_checkpoint(input).await?);
        }
        Ok(CommitActivationsResult { results })
    }

    async fn record_activation_failures(
        &self,
        _inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        Ok(())
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError>;
    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError>;
    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError>;

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum InstanceStatusValue {
    Running {
        common: JsonValue,
        phase: PhaseSnapshot,
    },
    Completed {
        output: JsonValue,
    },
    Canceled {
        reason: String,
    },
    Failed {
        error: SerializedError,
    },
}

#[derive(Clone)]
pub struct DurableRuntime {
    provider: Arc<dyn DurabilityProvider>,
    workflows: Arc<Mutex<HashMap<String, Arc<dyn ErasedWorkflow>>>>,
    clock: Clock,
    options: RuntimeOptions,
}

impl DurableRuntime {
    pub fn new<P>(provider: P) -> Self
    where
        P: DurabilityProvider + 'static,
    {
        Self::from_provider(Arc::new(provider))
    }

    pub fn from_provider(provider: Arc<dyn DurabilityProvider>) -> Self {
        Self {
            provider,
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(Utc::now),
            options: RuntimeOptions::default(),
        }
    }

    pub fn from_provider_with_options(
        provider: Arc<dyn DurabilityProvider>,
        options: RuntimeOptions,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        Self {
            provider,
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(clock),
            options,
        }
    }

    pub fn with_clock<P>(
        provider: P,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self
    where
        P: DurabilityProvider + 'static,
    {
        Self {
            provider: Arc::new(provider),
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(clock),
            options: RuntimeOptions::default(),
        }
    }

    pub fn with_options<P>(
        provider: P,
        options: RuntimeOptions,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self
    where
        P: DurabilityProvider + 'static,
    {
        Self {
            provider: Arc::new(provider),
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(clock),
            options,
        }
    }

    pub fn provider(&self) -> Arc<dyn DurabilityProvider> {
        self.provider.clone()
    }

    pub fn register<W>(&self) -> Result<(), WorkflowError>
    where
        W: Workflow,
    {
        let mut workflows = self
            .workflows
            .lock()
            .map_err(|_| WorkflowError::new("workflow registry lock poisoned"))?;
        workflows.insert(
            W::NAME.to_string(),
            Arc::new(WorkflowAdapter::<W>::default()),
        );
        Ok(())
    }

    pub async fn start<W>(
        &self,
        input: W::Input,
        options: StartOptions,
    ) -> Result<InstanceRef, WorkflowError>
    where
        W: Workflow,
    {
        self.register::<W>()?;
        let workflow = self.workflow(W::NAME)?;
        let input = serde_json::to_value(input)?;
        let start = workflow.initial_value(input)?;
        let now = self.now();
        let workflow_id = options
            .workflow_id
            .unwrap_or_else(|| format!("{}-{}", W::NAME, Uuid::new_v4()));
        let run_id = options.run_id.unwrap_or_else(|| "run-1".to_string());
        let waits = workflow.materialize_waits(&start.common, &start.phase, now)?;

        self.provider
            .create_instance(CreateInstanceInput {
                workflow_name: W::NAME.to_string(),
                workflow_version: W::VERSION,
                workflow_id: workflow_id.clone(),
                run_id: run_id.clone(),
                partition_shard: workflow_partition_shard(
                    &workflow_id,
                    &run_id,
                    self.options.shard_count,
                ),
                common: start.common,
                phase: start.phase,
                waits,
                now,
                parent: None,
                conflict_policy: options.conflict_policy,
            })
            .await
    }

    pub async fn signal<T>(
        &self,
        ref_: &InstanceRef,
        signal_type: impl Into<String>,
        payload: T,
    ) -> Result<SignalRecord, WorkflowError>
    where
        T: Serialize,
    {
        self.provider
            .append_signal(AppendSignalInput {
                workflow_id: ref_.workflow_id.clone(),
                run_id: ref_.run_id.clone(),
                r#type: signal_type.into(),
                payload: serde_json::to_value(payload)?,
                received_at: self.now(),
            })
            .await
    }

    pub async fn query<W, Q>(&self, ref_: &InstanceRef, name: &str) -> Result<Q, WorkflowError>
    where
        W: Workflow,
        Q: DeserializeOwned,
    {
        self.register::<W>()?;
        let workflow = self.workflow(W::NAME)?;
        let instance = self.require_instance(ref_).await?;
        let value = workflow.query_value(name, &instance)?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn drain(&self, options: DrainOptions) -> Result<DrainResult, WorkflowError> {
        self.run_worker(RunWorkerOptions {
            max_activations: options.max_activations,
            stop_when_idle: true,
            ..RunWorkerOptions::default()
        })
        .await
    }

    pub async fn run_worker(
        &self,
        options: RunWorkerOptions,
    ) -> Result<DrainResult, WorkflowError> {
        let max_activations = options.max_activations.unwrap_or(usize::MAX);
        let mut activations = 0;
        let mut successful_shards = HashMap::new();
        let mut idle_sleep_ms = options
            .idle_sleep_ms
            .unwrap_or(self.options.min_poll_interval_ms)
            .max(1);
        let max_idle_sleep_ms = self.options.max_poll_interval_ms.max(idle_sleep_ms);

        while activations < max_activations {
            if options
                .cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
            {
                break;
            }
            let remaining = max_activations - activations;
            let batch_limit = self.options.activation_prefetch_limit.max(1).min(remaining);
            let batch = self.next_activation_batch(batch_limit).await?;
            if batch.is_empty() {
                if options.stop_when_idle {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(idle_sleep_ms)).await;
                idle_sleep_ms = (idle_sleep_ms.saturating_mul(2)).min(max_idle_sleep_ms);
                continue;
            }
            idle_sleep_ms = options
                .idle_sleep_ms
                .unwrap_or(self.options.min_poll_interval_ms)
                .max(1);
            let mut pending_commits = Vec::new();
            for activation in batch {
                match self.prepare_activation_commit(activation.clone()).await {
                    Ok(ActivationRunOutcome::Prepared(input)) => {
                        pending_commits.push((activation, input));
                    }
                    Ok(ActivationRunOutcome::Skipped) => {
                        let _ = self.release_claimed_activation(&activation).await;
                    }
                    Err(error) => {
                        match self.commit_prepared_activations(&pending_commits).await {
                            Ok(committed_shards) => {
                                successful_shards.extend(committed_shards);
                            }
                            Err(commit_error) => {
                                let _ = self.release_claimed_activation(&activation).await;
                                let _ = self.release_successful_shards(successful_shards).await;
                                return Err(commit_error);
                            }
                        }
                        let _ = self.release_claimed_activation(&activation).await;
                        let _ = self.release_successful_shards(successful_shards).await;
                        return Err(error);
                    }
                }
                activations += 1;
                if activations >= max_activations {
                    break;
                }
            }
            successful_shards.extend(self.commit_prepared_activations(&pending_commits).await?);
        }

        self.release_successful_shards(successful_shards).await?;
        Ok(DrainResult { activations })
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    fn workflow(&self, name: &str) -> Result<Arc<dyn ErasedWorkflow>, WorkflowError> {
        let workflows = self
            .workflows
            .lock()
            .map_err(|_| WorkflowError::new("workflow registry lock poisoned"))?;
        workflows
            .get(name)
            .cloned()
            .ok_or_else(|| WorkflowError::new(format!("unknown workflow: {name}")))
    }

    async fn require_instance(
        &self,
        ref_: &InstanceRef,
    ) -> Result<PersistedInstance, WorkflowError> {
        self.provider
            .load_instance(
                ref_,
                LoadInstanceOptions {
                    include_effects: true,
                },
            )
            .await?
            .ok_or_else(|| {
                WorkflowError::new(format!(
                    "unknown workflow instance: {}/{}",
                    ref_.workflow_id, ref_.run_id
                ))
            })
    }

    async fn next_activation_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<ReadyActivation>, WorkflowError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now();
        let workflows = {
            let workflows = self
                .workflows
                .lock()
                .map_err(|_| WorkflowError::new("workflow registry lock poisoned"))?;
            workflows
                .iter()
                .map(|(name, workflow)| (name.clone(), workflow.version()))
                .collect::<HashMap<_, _>>()
        };
        let shard_ids = if self.options.dispatch_shard_ids.is_empty() {
            (0..self.options.shard_count).collect::<Vec<_>>()
        } else {
            self.options.dispatch_shard_ids.clone()
        };

        let mut ready = Vec::new();
        for shard_id in shard_ids {
            if ready.len() >= limit {
                break;
            }
            let Some((_lease, claims)) = self
                .provider
                .claim_shard_tasks(
                    ClaimShardInput {
                        shard_id,
                        owner_id: self.options.worker_id.clone(),
                        now,
                        lease_ms: self.options.shard_lease_ms,
                    },
                    ClaimShardTasksInput {
                        workflows: workflows.clone(),
                        shard_count: self.options.shard_count,
                        now,
                        lease_ms: self.options.activation_lease_ms,
                        limit: limit - ready.len(),
                    },
                )
                .await?
            else {
                continue;
            };
            for claim in claims.claims {
                if let Some(activation) = self.ready_activation_from_claim(claim)? {
                    ready.push(activation);
                    if ready.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(ready)
    }

    fn ready_activation_from_claim(
        &self,
        claim: ClaimedActivationWithInstance,
    ) -> Result<Option<ReadyActivation>, WorkflowError> {
        let lease = claim.lease;
        match claim.activation {
            ClaimedActivation::Migration {
                activation_id,
                workflow_name,
                ..
            } => Ok(Some(ReadyActivation {
                kind: ReadyActivationKind::Migration,
                workflow: self.workflow(&workflow_name)?,
                instance: claim.instance,
                activation_id,
                wait_name: None,
                event: None,
                lease,
            })),
            ClaimedActivation::Run {
                activation_id,
                workflow_name,
                ..
            } => Ok(Some(ReadyActivation {
                kind: ReadyActivationKind::Run,
                workflow: self.workflow(&workflow_name)?,
                instance: claim.instance,
                activation_id,
                wait_name: None,
                event: None,
                lease,
            })),
            ClaimedActivation::Event {
                activation_id,
                workflow_name,
                wait_name,
                event,
                ..
            } => Ok(Some(ReadyActivation {
                kind: ReadyActivationKind::Event,
                workflow: self.workflow(&workflow_name)?,
                instance: claim.instance,
                activation_id,
                wait_name: Some(wait_name),
                event: Some(event),
                lease,
            })),
        }
    }

    async fn release_claimed_activation(
        &self,
        activation: &ReadyActivation,
    ) -> Result<(), WorkflowError> {
        match activation.lease {
            ActivationClaimLease::Shard { shard_id, epoch } => {
                let session = self.provider.open_shard(OpenShardInput {
                    shard_id,
                    owner_id: Some(self.options.worker_id.clone()),
                    lease_epoch: Some(epoch),
                });
                session
                    .release_activation(&activation.activation_id, &self.options.worker_id)
                    .await?;
                session.release().await
            }
            ActivationClaimLease::Activation => Ok(()),
        }
    }

    async fn release_successful_shards(
        &self,
        shards: HashMap<u32, u64>,
    ) -> Result<(), WorkflowError> {
        for (shard_id, epoch) in shards {
            let session = self.provider.open_shard(OpenShardInput {
                shard_id,
                owner_id: Some(self.options.worker_id.clone()),
                lease_epoch: Some(epoch),
            });
            session.release().await?;
        }
        Ok(())
    }

    async fn commit_prepared_activations(
        &self,
        pending: &[(ReadyActivation, CommitCheckpointInput)],
    ) -> Result<HashMap<u32, u64>, WorkflowError> {
        let mut successful_shards = HashMap::new();
        let mut shard_groups: HashMap<(u32, u64), Vec<(ReadyActivation, CommitCheckpointInput)>> =
            HashMap::new();
        for (activation, input) in pending.iter().cloned() {
            match activation.lease {
                ActivationClaimLease::Shard { shard_id, epoch } => {
                    shard_groups
                        .entry((shard_id, epoch))
                        .or_default()
                        .push((activation, input));
                }
                ActivationClaimLease::Activation => {
                    let result = self.provider.commit_checkpoint(input).await?;
                    if result.ok {
                        continue;
                    }
                    if result.retryable.unwrap_or(false) {
                        continue;
                    }
                    self.handle_commit_result(result)?;
                }
            }
        }

        for ((shard_id, epoch), group) in shard_groups {
            let session = self.provider.open_shard(OpenShardInput {
                shard_id,
                owner_id: Some(self.options.worker_id.clone()),
                lease_epoch: Some(epoch),
            });
            let inputs = group
                .iter()
                .map(|(_, input)| input.clone())
                .collect::<Vec<_>>();
            let output = session.commit_activations(inputs).await?;
            for ((activation, _), result) in group.iter().zip(output.results) {
                if result.ok {
                    successful_shards.insert(shard_id, epoch);
                    continue;
                }
                if result.retryable.unwrap_or(false) {
                    let _ = self.release_claimed_activation(activation).await;
                    continue;
                }
                self.handle_commit_result(result)?;
            }
        }

        Ok(successful_shards)
    }

    async fn prepare_activation_commit(
        &self,
        activation: ReadyActivation,
    ) -> Result<ActivationRunOutcome, WorkflowError> {
        let latest = activation.instance.clone();
        if latest.status != PersistedStatus::Running
            || latest.sequence != activation.instance.sequence
        {
            return Ok(ActivationRunOutcome::Skipped);
        }

        if activation.kind == ReadyActivationKind::Migration {
            let next = self.migrate_snapshot(activation.workflow.clone(), &latest)?;
            let waits = if let InstanceStatusValue::Running { common, phase } = &next {
                activation
                    .workflow
                    .materialize_waits(common, phase, latest.updated_at)?
            } else {
                Vec::new()
            };
            return Ok(ActivationRunOutcome::Prepared(CommitCheckpointInput {
                workflow_id: latest.workflow_id,
                run_id: latest.run_id,
                expected_sequence: latest.sequence,
                activation_id: activation.activation_id.clone(),
                workflow_version: activation.workflow.version(),
                next,
                waits,
                now: self.now(),
                consume_signal_id: None,
                consume_child_record_id: None,
                effects: Vec::new(),
                child_starts: Vec::new(),
            }));
        }

        let common = latest
            .common
            .clone()
            .ok_or_else(|| WorkflowError::new("running workflow has no common state"))?;
        let phase = latest
            .phase
            .clone()
            .ok_or_else(|| WorkflowError::new("running workflow has no phase"))?;

        let mut ctx = DurableContext {
            provider: self.provider.clone(),
            registry: self.workflows.clone(),
            workflow_id: latest.workflow_id.clone(),
            run_id: latest.run_id.clone(),
            partition_shard: latest.partition_shard,
            sequence: latest.sequence,
            activation_id: activation.activation_id.clone(),
            commit_effects: HashMap::new(),
            commit_child_starts: HashMap::new(),
            commit_child_key_by_ref: HashMap::new(),
            clock: self.clock.clone(),
            worker_id: self.options.worker_id.clone(),
            shard_count: self.options.shard_count,
        };

        let (transition, consume_signal_id, consume_child_record_id) = match activation.kind {
            ReadyActivationKind::Run => (
                activation
                    .workflow
                    .dispatch_run_value(&mut ctx, common, phase)
                    .await?,
                None,
                None,
            ),
            ReadyActivationKind::Event => {
                let wait_name = activation
                    .wait_name
                    .as_deref()
                    .ok_or_else(|| WorkflowError::new("event activation missing wait name"))?;
                let event = activation
                    .event
                    .clone()
                    .ok_or_else(|| WorkflowError::new("event activation missing event"))?;
                let consume_signal_id = match &event {
                    ReadyEvent::Signal { signal_id, .. } => Some(signal_id.clone()),
                    _ => None,
                };
                let consume_child_record_id = match &event {
                    ReadyEvent::Child {
                        child_record_id, ..
                    } => Some(child_record_id.clone()),
                    _ => None,
                };
                (
                    activation
                        .workflow
                        .dispatch_event_value(&mut ctx, common, phase, wait_name, event)
                        .await?,
                    consume_signal_id,
                    consume_child_record_id,
                )
            }
            ReadyActivationKind::Migration => unreachable!("handled migration earlier"),
        };

        let next = self.apply_transition(activation.workflow.clone(), &latest, transition)?;
        let waits = if let InstanceStatusValue::Running { common, phase } = &next {
            activation
                .workflow
                .materialize_waits(common, phase, latest.updated_at)?
        } else {
            Vec::new()
        };

        Ok(ActivationRunOutcome::Prepared(CommitCheckpointInput {
            workflow_id: latest.workflow_id,
            run_id: latest.run_id,
            expected_sequence: latest.sequence,
            activation_id: activation.activation_id.clone(),
            workflow_version: activation.workflow.version(),
            next,
            waits,
            now: self.now(),
            consume_signal_id,
            consume_child_record_id,
            effects: ctx
                .commit_effects
                .into_values()
                .filter(|effect| !matches!(effect, CheckpointEffectMutation::Completed { .. }))
                .collect(),
            child_starts: ctx.commit_child_starts.into_values().collect(),
        }))
    }

    fn handle_commit_result(&self, result: CommitCheckpointResult) -> Result<(), WorkflowError> {
        if result.ok || result.retryable.unwrap_or(false) {
            return Ok(());
        }
        Err(WorkflowError::new(
            result
                .error
                .map(|error| error.message)
                .or(result.reason)
                .unwrap_or_else(|| "activation commit failed".to_string()),
        ))
    }

    fn apply_transition(
        &self,
        workflow: Arc<dyn ErasedWorkflow>,
        instance: &PersistedInstance,
        transition: TransitionValue,
    ) -> Result<InstanceStatusValue, WorkflowError> {
        let current_phase = instance
            .phase
            .as_ref()
            .ok_or_else(|| WorkflowError::new("running workflow has no phase"))?;
        let common = instance
            .common
            .clone()
            .ok_or_else(|| WorkflowError::new("running workflow has no common state"))?;

        match transition {
            TransitionValue::Stay(phase) => {
                if phase.name != current_phase.name {
                    return Err(WorkflowError::new(format!(
                        "stay transition cannot change phase from {} to {}",
                        current_phase.name, phase.name
                    )));
                }
                workflow.validate_running(common, phase)
            }
            TransitionValue::Go(phase) => workflow.validate_running(common, phase),
            TransitionValue::Complete(output) => Ok(InstanceStatusValue::Completed { output }),
            TransitionValue::Cancel(reason) => Ok(InstanceStatusValue::Canceled { reason }),
            TransitionValue::Fail(error) => Ok(InstanceStatusValue::Failed { error }),
        }
    }

    fn migrate_snapshot(
        &self,
        workflow: Arc<dyn ErasedWorkflow>,
        instance: &PersistedInstance,
    ) -> Result<InstanceStatusValue, WorkflowError> {
        let mut common = instance
            .common
            .clone()
            .ok_or_else(|| WorkflowError::new("running workflow has no common state"))?;
        let mut phase = instance
            .phase
            .clone()
            .ok_or_else(|| WorkflowError::new("running workflow has no phase"))?;

        for version in instance.workflow_version..workflow.version() {
            let args = MigrationArgs {
                common: common.clone(),
                phase: phase.clone(),
                from_version: version,
                to_version: version + 1,
            };
            if let Some(result) = workflow.migrate_value(version, args)? {
                common = result.common;
                phase = result.phase;
            }
        }

        workflow.validate_running(common, phase)
    }
}

#[derive(Clone, Debug, Default)]
pub struct StartOptions {
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub conflict_policy: Option<ConflictPolicy>,
}

#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    pub worker_id: String,
    pub shard_count: u32,
    pub dispatch_shard_ids: Vec<u32>,
    pub shard_lease_ms: u64,
    pub activation_lease_ms: u64,
    pub max_concurrent_activations: usize,
    pub activation_prefetch_limit: usize,
    pub commit_batch_size: usize,
    pub commit_max_delay_ms: u64,
    pub min_poll_interval_ms: u64,
    pub max_poll_interval_ms: u64,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::new_v4()),
            shard_count: 1,
            dispatch_shard_ids: vec![0],
            shard_lease_ms: 30_000,
            activation_lease_ms: 30_000,
            max_concurrent_activations: 1,
            activation_prefetch_limit: 32,
            commit_batch_size: 32,
            commit_max_delay_ms: 0,
            min_poll_interval_ms: 10,
            max_poll_interval_ms: 1_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DrainOptions {
    pub max_activations: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct WorkerCancellation {
    cancelled: Arc<AtomicBool>,
}

impl WorkerCancellation {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl Default for WorkerCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct RunWorkerOptions {
    pub max_activations: Option<usize>,
    pub stop_when_idle: bool,
    pub idle_sleep_ms: Option<u64>,
    pub cancellation: Option<WorkerCancellation>,
}

impl Default for RunWorkerOptions {
    fn default() -> Self {
        Self {
            max_activations: None,
            stop_when_idle: false,
            idle_sleep_ms: None,
            cancellation: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainResult {
    pub activations: usize,
}

pub struct DurableContext {
    provider: Arc<dyn DurabilityProvider>,
    registry: Arc<Mutex<HashMap<String, Arc<dyn ErasedWorkflow>>>>,
    workflow_id: String,
    run_id: String,
    partition_shard: u32,
    sequence: u64,
    activation_id: String,
    commit_effects: HashMap<String, CheckpointEffectMutation>,
    commit_child_starts: HashMap<String, CheckpointChildStart>,
    commit_child_key_by_ref: HashMap<String, String>,
    clock: Clock,
    worker_id: String,
    shard_count: u32,
}

impl DurableContext {
    pub fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    pub async fn activity<T, F, Fut>(&mut self, key: &str, f: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, WorkflowError>> + Send,
    {
        self.activity_with_options(key, ActivityOptions::default(), |_| f())
            .await
    }

    pub async fn activity_with_options<T, F, Fut>(
        &mut self,
        key: &str,
        _options: ActivityOptions,
        f: F,
    ) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: FnOnce(ActivityContext) -> Fut + Send,
        Fut: Future<Output = Result<T, WorkflowError>> + Send,
    {
        if _options.durability == ActivityDurability::Commit {
            if let Some(effect) = self.commit_effects.get(key) {
                match effect {
                    CheckpointEffectMutation::Completed { result, .. } => {
                        return Ok(serde_json::from_value(result.clone())?);
                    }
                    CheckpointEffectMutation::Failed { error, .. } => {
                        return Err(WorkflowError::new(error.message.clone()));
                    }
                }
            }
            let context = ActivityContext {
                provider: None,
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                worker_id: self.worker_id.clone(),
                effect_id: String::new(),
                attempt_id: String::new(),
                idempotency_key: format!(
                    "{}/{}/{}/{}",
                    self.workflow_id, self.run_id, self.activation_id, key
                ),
                last_heartbeat_details: None,
            };
            let result = match f(context).await {
                Ok(result) => result,
                Err(error) => {
                    self.commit_effects.insert(
                        key.to_string(),
                        CheckpointEffectMutation::Failed {
                            key: key.to_string(),
                            error: SerializedError {
                                name: None,
                                message: error.message.clone(),
                            },
                        },
                    );
                    return Err(error);
                }
            };
            self.commit_effects.insert(
                key.to_string(),
                CheckpointEffectMutation::Completed {
                    key: key.to_string(),
                    result: serde_json::to_value(&result)?,
                },
            );
            return Ok(result);
        }

        match self
            .provider
            .get_or_reserve_effect(ReserveEffectInput {
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                worker_id: self.worker_id.clone(),
                key: key.to_string(),
                now: self.now(),
                options: _options.clone(),
                max_attempts: _options.max_attempts,
            })
            .await?
        {
            EffectReservation::Completed { result } => Ok(serde_json::from_value(result)?),
            EffectReservation::Failed { error } => Err(WorkflowError::new(error.message)),
            EffectReservation::Reserved {
                effect_id,
                idempotency_key,
                attempt: _,
                attempt_id,
                heartbeat_details,
            } => {
                let context = ActivityContext {
                    provider: Some(self.provider.clone()),
                    workflow_id: self.workflow_id.clone(),
                    run_id: self.run_id.clone(),
                    activation_id: self.activation_id.clone(),
                    worker_id: self.worker_id.clone(),
                    effect_id: effect_id.clone(),
                    attempt_id: attempt_id.clone(),
                    idempotency_key,
                    last_heartbeat_details: heartbeat_details,
                };
                match f(context).await {
                    Ok(result) => {
                        self.provider
                            .complete_effect(CompleteEffectInput {
                                workflow_id: self.workflow_id.clone(),
                                run_id: self.run_id.clone(),
                                activation_id: self.activation_id.clone(),
                                worker_id: self.worker_id.clone(),
                                effect_id,
                                attempt_id,
                                result: serde_json::to_value(&result)?,
                                now: self.now(),
                            })
                            .await?;
                        Ok(result)
                    }
                    Err(error) => {
                        self.provider
                            .fail_effect(FailEffectInput {
                                workflow_id: self.workflow_id.clone(),
                                run_id: self.run_id.clone(),
                                activation_id: self.activation_id.clone(),
                                worker_id: self.worker_id.clone(),
                                effect_id,
                                attempt_id,
                                error: SerializedError {
                                    name: None,
                                    message: error.message.clone(),
                                },
                                now: self.now(),
                                retryable: None,
                            })
                            .await?;
                        Err(error)
                    }
                }
            }
        }
    }

    pub async fn child_start<W>(
        &mut self,
        key: &str,
        input: W::Input,
        options: ChildOptions,
    ) -> Result<ChildHandle<W>, WorkflowError>
    where
        W: Workflow,
    {
        {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| WorkflowError::new("workflow registry lock poisoned"))?;
            registry.insert(
                W::NAME.to_string(),
                Arc::new(WorkflowAdapter::<W>::default()),
            );
        }

        let adapter = WorkflowAdapter::<W>::default();
        let start = adapter.initial_value(serde_json::to_value(input)?)?;
        let now = (self.clock)();
        let workflow_id = options.workflow_id.unwrap_or_else(|| {
            default_child_workflow_id(
                &self.workflow_id,
                &self.run_id,
                self.sequence,
                key,
                self.shard_count,
                self.partition_shard,
            )
        });
        let run_id = "run-1".to_string();
        let partition_shard = workflow_partition_shard(&workflow_id, &run_id, self.shard_count);
        let waits = adapter.materialize_waits(&start.common, &start.phase, now)?;

        if options.durability == ChildDurability::Commit {
            if let Some(existing) = self.commit_child_starts.get(key).cloned() {
                if options.conflict_policy == ConflictPolicy::Fail {
                    return Err(WorkflowError::new(format!(
                        "child workflow already exists for activation key: {}/{}/{}/{}",
                        self.workflow_id, self.run_id, self.activation_id, key
                    )));
                }
                if options.conflict_policy != ConflictPolicy::TerminateExisting {
                    return Ok(ChildHandle {
                        workflow_name: existing.workflow_name,
                        workflow_version: existing.workflow_version,
                        workflow_id: existing.workflow_id,
                        run_id: existing.run_id,
                        workflow: PhantomData,
                    });
                }
                self.commit_child_starts.remove(key);
                self.commit_child_key_by_ref
                    .remove(&child_ref_key(&existing.workflow_id, &existing.run_id));
            }

            let ref_key = child_ref_key(&workflow_id, &run_id);
            if let Some(existing_key) = self.commit_child_key_by_ref.get(&ref_key).cloned() {
                if options.conflict_policy == ConflictPolicy::TerminateExisting {
                    self.commit_child_starts.remove(&existing_key);
                    self.commit_child_key_by_ref.remove(&ref_key);
                } else {
                    return Err(WorkflowError::new(format!(
                        "child workflow instance already exists in this activation: {}/{}",
                        workflow_id, run_id
                    )));
                }
            }

            self.commit_child_starts.insert(
                key.to_string(),
                CheckpointChildStart {
                    key: key.to_string(),
                    workflow_name: W::NAME.to_string(),
                    workflow_version: W::VERSION,
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                    partition_shard,
                    common: start.common,
                    phase: start.phase,
                    waits,
                    parent_close_policy: options.parent_close_policy,
                    conflict_policy: options.conflict_policy,
                },
            );
            self.commit_child_key_by_ref
                .insert(ref_key, key.to_string());
            return Ok(ChildHandle {
                workflow_name: W::NAME.to_string(),
                workflow_version: W::VERSION,
                workflow_id,
                run_id,
                workflow: PhantomData,
            });
        }

        let handle = self
            .provider
            .create_child_instance(CreateChildInstanceInput {
                workflow_name: W::NAME.to_string(),
                workflow_version: W::VERSION,
                workflow_id,
                run_id,
                partition_shard,
                common: start.common,
                phase: start.phase,
                waits,
                now,
                parent_workflow_id: self.workflow_id.clone(),
                parent_run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                worker_id: self.worker_id.clone(),
                lease_now: now,
                key: key.to_string(),
                parent_close_policy: options.parent_close_policy,
                conflict_policy: options.conflict_policy,
            })
            .await?;

        Ok(ChildHandle {
            workflow_name: handle.workflow_name,
            workflow_version: handle.workflow_version,
            workflow_id: handle.workflow_id,
            run_id: handle.run_id,
            workflow: PhantomData,
        })
    }

    pub async fn child_result<W>(
        &mut self,
        handle: &ChildHandle<W>,
    ) -> Result<W::Output, WorkflowError>
    where
        W: Workflow,
    {
        let ref_ = InstanceRef::new(handle.workflow_id.clone(), handle.run_id.clone());
        let instance = self
            .provider
            .load_instance(
                &ref_,
                LoadInstanceOptions {
                    include_effects: false,
                },
            )
            .await?
            .ok_or_else(|| {
                WorkflowError::new(format!(
                    "unknown child workflow: {}/{}",
                    handle.workflow_id, handle.run_id
                ))
            })?;
        if instance.status != PersistedStatus::Completed {
            return Err(WorkflowError::new(format!(
                "child workflow is not complete: {}/{}",
                handle.workflow_id, handle.run_id
            )));
        }
        serde_json::from_value(instance.output.unwrap_or(JsonValue::Null))
            .map_err(WorkflowError::from)
    }

    pub async fn child_cancel<W>(&mut self, handle: &ChildHandle<W>) -> Result<(), WorkflowError>
    where
        W: Workflow,
    {
        let ref_key = child_ref_key(&handle.workflow_id, &handle.run_id);
        if let Some(key) = self.commit_child_key_by_ref.remove(&ref_key) {
            self.commit_child_starts.remove(&key);
            return Ok(());
        }
        self.provider
            .cancel_child(CancelChildInput {
                parent_workflow_id: self.workflow_id.clone(),
                parent_run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                worker_id: self.worker_id.clone(),
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                now: self.now(),
            })
            .await
    }
}

#[derive(Clone)]
struct ReadyActivation {
    kind: ReadyActivationKind,
    workflow: Arc<dyn ErasedWorkflow>,
    instance: PersistedInstance,
    activation_id: String,
    wait_name: Option<String>,
    event: Option<ReadyEvent>,
    lease: ActivationClaimLease,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadyActivationKind {
    Migration,
    Run,
    Event,
}

#[derive(Clone, Debug)]
enum ActivationRunOutcome {
    Prepared(CommitCheckpointInput),
    Skipped,
}

fn typed_snapshot<W>(
    instance: &PersistedInstance,
) -> Result<InstanceSnapshot<W::Output, W::Common, W::Phase>, WorkflowError>
where
    W: Workflow,
{
    match instance.status {
        PersistedStatus::Running => {
            let common = serde_json::from_value::<W::Common>(
                instance.common.clone().unwrap_or(JsonValue::Null),
            )?;
            let phase = W::Phase::from_snapshot(
                instance
                    .phase
                    .clone()
                    .ok_or_else(|| WorkflowError::new("running workflow has no phase"))?,
            )?;
            Ok(InstanceSnapshot::Running { common, phase })
        }
        PersistedStatus::Completed => Ok(InstanceSnapshot::Completed {
            output: serde_json::from_value(instance.output.clone().unwrap_or(JsonValue::Null))?,
        }),
        PersistedStatus::Canceled => Ok(InstanceSnapshot::Canceled {
            reason: instance
                .cancel_reason
                .clone()
                .unwrap_or_else(|| "canceled".to_string()),
        }),
        PersistedStatus::Failed => Ok(InstanceSnapshot::Failed {
            error: instance.error.clone().unwrap_or(SerializedError {
                name: None,
                message: "failed".to_string(),
            }),
        }),
    }
}

fn wait_spec_to_durable(wait: WaitSpec, scope: WaitScope) -> DurableWait {
    match wait {
        WaitSpec::Signal { name, .. } => DurableWait::Signal {
            r#type: name.clone(),
            name,
            scope,
        },
        WaitSpec::Timer { name, fire_at } => DurableWait::Timer { name, fire_at },
        WaitSpec::Child {
            name,
            workflow_name,
            workflow_version,
            workflow_id,
            run_id,
        } => DurableWait::Child {
            name,
            workflow_name,
            workflow_version,
            workflow_id,
            run_id,
        },
    }
}

fn compare_signals(left: &&SignalRecord, right: &&SignalRecord) -> std::cmp::Ordering {
    (
        left.received_at,
        left.r#type.as_str(),
        left.signal_id.as_str(),
    )
        .cmp(&(
            right.received_at,
            right.r#type.as_str(),
            right.signal_id.as_str(),
        ))
}

fn compare_signal_records(left: &SignalRecord, right: &SignalRecord) -> std::cmp::Ordering {
    (
        left.received_at,
        left.r#type.as_str(),
        left.signal_id.as_str(),
    )
        .cmp(&(
            right.received_at,
            right.r#type.as_str(),
            right.signal_id.as_str(),
        ))
}

fn activation_id(instance: &PersistedInstance, kind: &str, event_id: &str) -> String {
    format!(
        "{}/{}/{}/{}/{}",
        instance.workflow_id, instance.run_id, instance.sequence, kind, event_id
    )
}

fn instance_key_ref(ref_: &InstanceRef) -> String {
    instance_key(&ref_.workflow_id, &ref_.run_id)
}

fn instance_key(workflow_id: &str, run_id: &str) -> String {
    format!("{workflow_id}:{run_id}")
}

pub fn workflow_partition_shard(workflow_id: &str, run_id: &str, shard_count: u32) -> u32 {
    assert!(shard_count > 0, "shard_count must be positive");
    let mut hash = 0x811c9dc5u32;
    for byte in workflow_id
        .as_bytes()
        .iter()
        .copied()
        .chain(std::iter::once(0))
        .chain(run_id.as_bytes().iter().copied())
    {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash % shard_count
}

fn child_handle_value(record: &ChildRecord) -> ChildHandleValue {
    ChildHandleValue {
        workflow_name: record.workflow_name.clone(),
        workflow_version: record.workflow_version,
        workflow_id: record.workflow_id.clone(),
        run_id: record.run_id.clone(),
    }
}

fn child_ref_key(workflow_id: &str, run_id: &str) -> String {
    format!("{workflow_id}:{run_id}")
}

fn require_instance_mut<'a>(
    state: &'a mut Store,
    workflow_id: &str,
    run_id: &str,
) -> Result<&'a mut PersistedInstance, WorkflowError> {
    state
        .instances
        .get_mut(&instance_key(workflow_id, run_id))
        .ok_or_else(|| {
            WorkflowError::new(format!("unknown workflow instance: {workflow_id}/{run_id}"))
        })
}

fn format_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn default_child_workflow_id(
    parent_workflow_id: &str,
    parent_run_id: &str,
    sequence: u64,
    key: &str,
    shard_count: u32,
    parent_shard: u32,
) -> String {
    let base = format!("{parent_workflow_id}__{sequence}__{}", safe_id(key));
    if workflow_partition_shard(&base, "run-1", shard_count) == parent_shard {
        return base;
    }
    for attempt in 1..=10_000 {
        let candidate = format!("{base}__shard_{attempt}");
        if workflow_partition_shard(&candidate, "run-1", shard_count) == parent_shard {
            return candidate;
        }
    }
    panic!(
        "could not find shard-affine child workflow id for {}/{}/{}",
        parent_workflow_id, parent_run_id, key
    );
}

#[macro_export]
macro_rules! start {
    (common: $common:expr, phase: $phase:expr $(,)?) => {
        $crate::Start {
            common: $common,
            phase: $phase,
        }
    };
    (phase: $phase:expr $(,)?) => {
        $crate::Start {
            common: (),
            phase: $phase,
        }
    };
}

#[macro_export]
macro_rules! stay {
    ($phase:expr $(,)?) => {
        Ok($crate::Transition::Stay($phase))
    };
}

#[macro_export]
macro_rules! go {
    ($phase:expr $(,)?) => {
        Ok($crate::Transition::Go($phase))
    };
}

#[macro_export]
macro_rules! complete {
    ($output:expr $(,)?) => {
        Ok($crate::Transition::Complete($output))
    };
}

#[macro_export]
macro_rules! cancel {
    ($reason:expr $(,)?) => {
        Ok($crate::Transition::Cancel($reason.to_string()))
    };
}

#[macro_export]
macro_rules! fail {
    ($error:expr $(,)?) => {
        Ok($crate::Transition::Fail($crate::SerializedError {
            name: None,
            message: $error.to_string(),
        }))
    };
}

pub mod testing {
    pub mod conformance {
        use super::super::*;
        use std::future::Future;

        pub async fn assert_basic_provider_conformance<P, F, Fut>(factory: F)
        where
            P: DurabilityProvider + 'static,
            F: Fn() -> Fut,
            Fut: Future<Output = Result<P, WorkflowError>>,
        {
            let provider = factory().await.expect("provider factory failed");
            let now = Utc::now();
            let ref_ = create_conformance_instance(
                &provider,
                "conformance-instance",
                "conformance",
                vec![run_wait(now)],
                now,
            )
            .await;
            let lease = provider
                .claim_shard(ClaimShardInput {
                    shard_id: 0,
                    owner_id: "worker-a".to_string(),
                    now,
                    lease_ms: 60_000,
                })
                .await
                .expect("claim shard failed")
                .expect("shard was not claimed");
            let session = provider.open_shard(OpenShardInput {
                shard_id: lease.shard_id,
                owner_id: Some(lease.owner_id.clone()),
                lease_epoch: Some(lease.lease_epoch),
            });
            let mut workflows = HashMap::new();
            workflows.insert("conformance".to_string(), 1);
            let claims = session
                .claim_tasks(ClaimShardTasksInput {
                    workflows,
                    shard_count: 1,
                    now,
                    lease_ms: 60_000,
                    limit: 1,
                })
                .await
                .expect("claim tasks failed");
            assert_eq!(claims.claims.len(), 1);
            let claim = &claims.claims[0];
            let result = session
                .commit_checkpoint(CommitCheckpointInput {
                    workflow_id: ref_.workflow_id.clone(),
                    run_id: ref_.run_id.clone(),
                    expected_sequence: claim.activation.sequence(),
                    activation_id: claim.activation.activation_id().to_string(),
                    workflow_version: 1,
                    next: InstanceStatusValue::Completed {
                        output: serde_json::json!({ "ok": true }),
                    },
                    waits: Vec::new(),
                    now,
                    consume_signal_id: None,
                    consume_child_record_id: None,
                    effects: Vec::new(),
                    child_starts: Vec::new(),
                })
                .await
                .expect("commit failed");
            assert!(result.ok, "commit conflict: {:?}", result);
            assert_eq!(result.sequence, 1);
            let loaded = provider
                .load_instance(
                    &ref_,
                    LoadInstanceOptions {
                        include_effects: false,
                    },
                )
                .await
                .expect("load failed")
                .expect("instance missing after commit");
            assert_eq!(loaded.status, PersistedStatus::Completed);
            assert_eq!(loaded.sequence, 1);
            session.release().await.expect("release basic shard failed");

            assert_ordered_batch_claims_and_lean_reads(&provider, now).await;
            assert_lost_activation_lease_preserves_signal(&provider, now).await;
            assert_eager_effect_retry_reclaims_activation(&provider, now).await;
        }

        async fn assert_ordered_batch_claims_and_lean_reads<P>(provider: &P, now: DateTime<Utc>)
        where
            P: DurabilityProvider,
        {
            let later = create_conformance_instance(
                provider,
                "ordered-later",
                "ordered",
                vec![run_wait(now + chrono::Duration::milliseconds(2))],
                now,
            )
            .await;
            let earlier = create_conformance_instance(
                provider,
                "ordered-earlier",
                "ordered",
                vec![run_wait(now + chrono::Duration::milliseconds(1))],
                now,
            )
            .await;
            let lease = provider
                .claim_shard(ClaimShardInput {
                    shard_id: 0,
                    owner_id: "ordered-worker".to_string(),
                    now,
                    lease_ms: 60_000,
                })
                .await
                .expect("ordered shard claim failed")
                .expect("ordered shard was not claimed");
            let session = provider.open_shard(OpenShardInput {
                shard_id: lease.shard_id,
                owner_id: Some(lease.owner_id.clone()),
                lease_epoch: Some(lease.lease_epoch),
            });
            let claims = session
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("ordered".to_string(), 1)]),
                    shard_count: 1,
                    now: now + chrono::Duration::milliseconds(10),
                    lease_ms: 60_000,
                    limit: 2,
                })
                .await
                .expect("ordered claim failed");
            assert_eq!(claims.claims.len(), 2);
            assert_eq!(claims.claims[0].instance.workflow_id, earlier.workflow_id);
            assert_eq!(claims.claims[1].instance.workflow_id, later.workflow_id);

            let lean = provider
                .load_instance(
                    &earlier,
                    LoadInstanceOptions {
                        include_effects: false,
                    },
                )
                .await
                .expect("lean load failed")
                .expect("ordered instance missing");
            assert!(lean.effects.is_empty());
            session
                .release()
                .await
                .expect("release ordered shard failed");
        }

        async fn assert_lost_activation_lease_preserves_signal<P>(provider: &P, now: DateTime<Utc>)
        where
            P: DurabilityProvider,
        {
            let ref_ = create_conformance_instance(
                provider,
                "lost-lease-signal",
                "lost_lease",
                vec![DurableWait::Signal {
                    name: "finish".to_string(),
                    r#type: "finish".to_string(),
                    scope: WaitScope::Phase,
                }],
                now,
            )
            .await;
            let signal = provider
                .append_signal(AppendSignalInput {
                    workflow_id: ref_.workflow_id.clone(),
                    run_id: ref_.run_id.clone(),
                    r#type: "finish".to_string(),
                    payload: serde_json::json!({ "ok": true }),
                    received_at: now,
                })
                .await
                .expect("append signal failed");

            let lease_a = provider
                .claim_shard(ClaimShardInput {
                    shard_id: 0,
                    owner_id: "lease-worker-a".to_string(),
                    now,
                    lease_ms: 10,
                })
                .await
                .expect("worker a shard claim failed")
                .expect("worker a shard not claimed");
            let session_a = provider.open_shard(OpenShardInput {
                shard_id: 0,
                owner_id: Some("lease-worker-a".to_string()),
                lease_epoch: Some(lease_a.lease_epoch),
            });
            let claim_a = session_a
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("lost_lease".to_string(), 1)]),
                    shard_count: 1,
                    now,
                    lease_ms: 10,
                    limit: 1,
                })
                .await
                .expect("worker a claim failed")
                .claims
                .into_iter()
                .next()
                .expect("worker a did not claim signal");

            let reclaim_at = now + chrono::Duration::milliseconds(20);
            let lease_b = provider
                .claim_shard(ClaimShardInput {
                    shard_id: 0,
                    owner_id: "lease-worker-b".to_string(),
                    now: reclaim_at,
                    lease_ms: 60_000,
                })
                .await
                .expect("worker b shard claim failed")
                .expect("worker b shard not claimed");
            let session_b = provider.open_shard(OpenShardInput {
                shard_id: 0,
                owner_id: Some("lease-worker-b".to_string()),
                lease_epoch: Some(lease_b.lease_epoch),
            });
            let claim_b = session_b
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("lost_lease".to_string(), 1)]),
                    shard_count: 1,
                    now: reclaim_at,
                    lease_ms: 60_000,
                    limit: 1,
                })
                .await
                .expect("worker b claim failed")
                .claims
                .into_iter()
                .next()
                .expect("worker b did not reclaim signal");

            let stale = session_a
                .commit_checkpoint(complete_input(
                    &ref_,
                    claim_a.activation.sequence(),
                    claim_a.activation.activation_id(),
                    1,
                    reclaim_at,
                    Some(signal.signal_id.clone()),
                ))
                .await
                .expect("stale commit errored");
            assert!(!stale.ok);
            assert_eq!(stale.reason.as_deref(), Some("lost_activation_lease"));
            let unconsumed = provider
                .list_signals()
                .await
                .expect("list signals failed")
                .into_iter()
                .find(|record| record.signal_id == signal.signal_id)
                .expect("signal missing");
            assert_eq!(unconsumed.consumed_by_sequence, None);

            let committed = session_b
                .commit_checkpoint(complete_input(
                    &ref_,
                    claim_b.activation.sequence(),
                    claim_b.activation.activation_id(),
                    1,
                    reclaim_at,
                    Some(signal.signal_id.clone()),
                ))
                .await
                .expect("fresh commit errored");
            assert!(committed.ok, "fresh commit conflict: {:?}", committed);
            let consumed = provider
                .list_signals()
                .await
                .expect("list signals failed")
                .into_iter()
                .find(|record| record.signal_id == signal.signal_id)
                .expect("signal missing");
            assert_eq!(consumed.consumed_by_sequence, Some(1));
            session_b
                .release()
                .await
                .expect("release lost-lease shard failed");
        }

        async fn assert_eager_effect_retry_reclaims_activation<P>(provider: &P, now: DateTime<Utc>)
        where
            P: DurabilityProvider,
        {
            create_conformance_instance(
                provider,
                "effect-retry",
                "effect_retry",
                vec![run_wait(now)],
                now,
            )
            .await;
            let lease = provider
                .claim_shard(ClaimShardInput {
                    shard_id: 0,
                    owner_id: "effect-worker".to_string(),
                    now,
                    lease_ms: 60_000,
                })
                .await
                .expect("effect shard claim failed")
                .expect("effect shard not claimed");
            let session = provider.open_shard(OpenShardInput {
                shard_id: 0,
                owner_id: Some("effect-worker".to_string()),
                lease_epoch: Some(lease.lease_epoch),
            });
            let claim = session
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("effect_retry".to_string(), 1)]),
                    shard_count: 1,
                    now,
                    lease_ms: 60_000,
                    limit: 1,
                })
                .await
                .expect("effect claim failed")
                .claims
                .into_iter()
                .next()
                .expect("effect activation missing");
            let EffectReservation::Reserved {
                effect_id,
                attempt_id,
                attempt,
                ..
            } = session
                .get_or_reserve_effect(ReserveEffectInput {
                    workflow_id: "effect-retry".to_string(),
                    run_id: "run-1".to_string(),
                    activation_id: claim.activation.activation_id().to_string(),
                    worker_id: "effect-worker".to_string(),
                    key: "activity".to_string(),
                    now,
                    options: ActivityOptions {
                        durability: ActivityDurability::Eager,
                        max_attempts: Some(2),
                        initial_interval_ms: Some(25),
                        ..ActivityOptions::default()
                    },
                    max_attempts: None,
                })
                .await
                .expect("reserve effect failed")
            else {
                panic!("expected effect reservation");
            };
            assert_eq!(attempt, 1);
            let failed = session
                .fail_effect(FailEffectInput {
                    workflow_id: "effect-retry".to_string(),
                    run_id: "run-1".to_string(),
                    activation_id: claim.activation.activation_id().to_string(),
                    worker_id: "effect-worker".to_string(),
                    effect_id,
                    attempt_id,
                    error: SerializedError {
                        name: Some("Transient".to_string()),
                        message: "try again".to_string(),
                    },
                    now,
                    retryable: Some(true),
                })
                .await
                .expect("fail effect failed");
            let FailEffectResult::RetryScheduled {
                next_attempt_at,
                next_attempt,
            } = failed
            else {
                panic!("expected retry schedule");
            };
            assert_eq!(next_attempt, 2);

            let early = session
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("effect_retry".to_string(), 1)]),
                    shard_count: 1,
                    now: now + chrono::Duration::milliseconds(10),
                    lease_ms: 60_000,
                    limit: 1,
                })
                .await
                .expect("early effect reclaim failed");
            assert!(early.claims.is_empty());

            let retry = session
                .claim_tasks(ClaimShardTasksInput {
                    workflows: HashMap::from([("effect_retry".to_string(), 1)]),
                    shard_count: 1,
                    now: next_attempt_at,
                    lease_ms: 60_000,
                    limit: 1,
                })
                .await
                .expect("retry effect reclaim failed");
            assert_eq!(retry.claims.len(), 1);
        }

        async fn create_conformance_instance<P>(
            provider: &P,
            workflow_id: &str,
            workflow_name: &str,
            waits: Vec<DurableWait>,
            now: DateTime<Utc>,
        ) -> InstanceRef
        where
            P: DurabilityProvider,
        {
            provider
                .create_instance(CreateInstanceInput {
                    workflow_name: workflow_name.to_string(),
                    workflow_version: 1,
                    workflow_id: workflow_id.to_string(),
                    run_id: "run-1".to_string(),
                    partition_shard: 0,
                    common: serde_json::json!({ "value": "original" }),
                    phase: PhaseSnapshot {
                        name: "run".to_string(),
                        data: serde_json::json!({}),
                    },
                    waits,
                    now,
                    parent: None,
                    conflict_policy: Some(ConflictPolicy::Fail),
                })
                .await
                .expect("create instance failed")
        }

        fn run_wait(ready_at: DateTime<Utc>) -> DurableWait {
            DurableWait::Run {
                name: "__run".to_string(),
                ready_at,
            }
        }

        fn complete_input(
            ref_: &InstanceRef,
            expected_sequence: u64,
            activation_id: &str,
            workflow_version: u32,
            now: DateTime<Utc>,
            consume_signal_id: Option<String>,
        ) -> CommitCheckpointInput {
            CommitCheckpointInput {
                workflow_id: ref_.workflow_id.clone(),
                run_id: ref_.run_id.clone(),
                expected_sequence,
                activation_id: activation_id.to_string(),
                workflow_version,
                next: InstanceStatusValue::Completed {
                    output: serde_json::json!({ "ok": true }),
                },
                waits: Vec::new(),
                now,
                consume_signal_id,
                consume_child_record_id: None,
                effects: Vec::new(),
                child_starts: Vec::new(),
            }
        }
    }
}
