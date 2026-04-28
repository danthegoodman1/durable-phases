pub use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
pub use durable_macros::workflow;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::type_name;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::future::Future;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SerializedError {
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
    pub heartbeat_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildOptions {
    pub workflow_id: Option<String>,
    pub parent_close_policy: ParentClosePolicy,
    pub conflict_policy: ConflictPolicy,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ActivityContext {
    pub idempotency_key: String,
    pub last_heartbeat_details: Option<JsonValue>,
}

impl ActivityContext {
    pub async fn heartbeat<T>(&mut self, _details: T) -> Result<(), WorkflowError>
    where
        T: Serialize + Send,
    {
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
    pub result: Option<JsonValue>,
    pub error: Option<SerializedError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Pending,
    Completed,
    Failed,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Store {
    instances: HashMap<String, PersistedInstance>,
    signals: Vec<SignalRecord>,
    children: Vec<ChildRecord>,
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
            next_signal_id: 1,
            next_effect_id: 1,
            next_child_id: 1,
        }
    }
}

#[derive(Clone)]
pub struct JsonFileDurabilityProvider {
    file_path: PathBuf,
    state: Arc<Mutex<Store>>,
}

impl JsonFileDurabilityProvider {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, WorkflowError> {
        let file_path = path.as_ref().to_path_buf();
        let state = if file_path.exists() {
            let raw = fs::read_to_string(&file_path)?;
            serde_json::from_str::<Store>(&raw)?
        } else {
            Store::default()
        };

        Ok(Self {
            file_path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<InstanceRef, WorkflowError> {
        let mut state = self.lock()?;
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

        state.instances.insert(
            key,
            PersistedInstance {
                workflow_name: input.workflow_name,
                workflow_version: input.workflow_version,
                workflow_id: input.workflow_id.clone(),
                run_id: input.run_id.clone(),
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
            },
        );

        self.save_locked(&state)?;
        Ok(InstanceRef::new(input.workflow_id, input.run_id))
    }

    pub fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        let mut state = self.lock()?;

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

        state.instances.insert(
            instance_key,
            PersistedInstance {
                workflow_name: input.workflow_name.clone(),
                workflow_version: input.workflow_version,
                workflow_id: input.workflow_id.clone(),
                run_id: input.run_id.clone(),
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
            },
        );

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

        self.save_locked(&state)?;
        Ok(handle)
    }

    pub fn load_instance(
        &self,
        ref_: &InstanceRef,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let state = self.lock()?;
        Ok(state.instances.get(&instance_key_ref(ref_)).cloned())
    }

    pub fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        let state = self.lock()?;
        Ok(state.instances.values().cloned().collect())
    }

    pub fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        let state = self.lock()?;
        Ok(state.signals.clone())
    }

    pub fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        let state = self.lock()?;
        Ok(state.children.clone())
    }

    pub fn append_signal(
        &self,
        workflow_id: String,
        run_id: String,
        r#type: String,
        payload: JsonValue,
        received_at: DateTime<Utc>,
    ) -> Result<SignalRecord, WorkflowError> {
        let mut state = self.lock()?;
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
        self.save_locked(&state)?;
        Ok(signal)
    }

    pub fn get_or_reserve_effect(
        &self,
        workflow_id: &str,
        run_id: &str,
        activation_id: &str,
        key: &str,
    ) -> Result<EffectReservation, WorkflowError> {
        let mut state = self.lock()?;
        let key_for_instance = instance_key(workflow_id, run_id);

        {
            let instance = state.instances.get(&key_for_instance).ok_or_else(|| {
                WorkflowError::new(format!("unknown workflow instance: {workflow_id}/{run_id}"))
            })?;

            if let Some(existing) = instance
                .effects
                .iter()
                .find(|effect| effect.activation_id == activation_id && effect.key == key)
            {
                return match existing.status {
                    EffectStatus::Completed => Ok(EffectReservation::Completed {
                        result: existing.result.clone().unwrap_or(JsonValue::Null),
                    }),
                    EffectStatus::Failed => Ok(EffectReservation::Failed {
                        error: existing.error.clone().unwrap_or(SerializedError {
                            message: "effect failed".to_string(),
                        }),
                    }),
                    EffectStatus::Pending => Ok(EffectReservation::Reserved {
                        effect_id: existing.effect_id.clone(),
                        idempotency_key: existing.idempotency_key.clone(),
                        heartbeat_details: None,
                    }),
                };
            }
        }

        let effect_id = format!("effect-{}", state.next_effect_id);
        state.next_effect_id += 1;
        let idempotency_key = format!("{workflow_id}/{run_id}/{activation_id}/{key}");
        let instance = state.instances.get_mut(&key_for_instance).ok_or_else(|| {
            WorkflowError::new(format!("unknown workflow instance: {workflow_id}/{run_id}"))
        })?;
        instance.effects.push(EffectRecord {
            effect_id: effect_id.clone(),
            activation_id: activation_id.to_string(),
            key: key.to_string(),
            idempotency_key: idempotency_key.clone(),
            status: EffectStatus::Pending,
            result: None,
            error: None,
        });

        self.save_locked(&state)?;
        Ok(EffectReservation::Reserved {
            effect_id,
            idempotency_key,
            heartbeat_details: None,
        })
    }

    pub fn complete_effect(
        &self,
        workflow_id: &str,
        run_id: &str,
        effect_id: &str,
        result: JsonValue,
    ) -> Result<(), WorkflowError> {
        let mut state = self.lock()?;
        let instance = require_instance_mut(&mut state, workflow_id, run_id)?;
        let effect = instance
            .effects
            .iter_mut()
            .find(|effect| effect.effect_id == effect_id)
            .ok_or_else(|| WorkflowError::new(format!("unknown effect: {effect_id}")))?;
        effect.status = EffectStatus::Completed;
        effect.result = Some(result);
        effect.error = None;
        self.save_locked(&state)
    }

    pub fn fail_effect(
        &self,
        workflow_id: &str,
        run_id: &str,
        effect_id: &str,
        error: SerializedError,
    ) -> Result<(), WorkflowError> {
        let mut state = self.lock()?;
        let instance = require_instance_mut(&mut state, workflow_id, run_id)?;
        let effect = instance
            .effects
            .iter_mut()
            .find(|effect| effect.effect_id == effect_id)
            .ok_or_else(|| WorkflowError::new(format!("unknown effect: {effect_id}")))?;
        effect.status = EffectStatus::Failed;
        effect.error = Some(error);
        self.save_locked(&state)
    }

    pub fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let mut state = self.lock()?;
        let key = instance_key(&input.workflow_id, &input.run_id);
        let current_sequence = {
            let Some(instance) = state.instances.get(&key) else {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: 0,
                });
            };

            if instance.status != PersistedStatus::Running {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: instance.sequence,
                });
            }

            if instance.sequence != input.expected_sequence {
                return Ok(CommitCheckpointResult {
                    ok: false,
                    sequence: instance.sequence,
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
                });
            }
            index
        } else {
            None
        };

        let next_sequence = current_sequence + 1;
        let (parent, instance_status, output, error, cancel_reason) = {
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

            (
                instance.parent.clone(),
                instance.status.clone(),
                instance.output.clone(),
                instance.error.clone(),
                instance.cancel_reason.clone(),
            )
        };

        if let Some(index) = signal_index {
            state.signals[index].consumed_by_sequence = Some(next_sequence);
        }

        if let Some(index) = child_index {
            state.children[index].delivered_by_sequence = Some(next_sequence);
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
                                    message: cancel_reason
                                        .unwrap_or_else(|| "child canceled".to_string()),
                                });
                            }
                            PersistedStatus::Failed => {
                                child_record.status = ChildStatus::Failed;
                                child_record.error = error.or(Some(SerializedError {
                                    message: "child failed".to_string(),
                                }));
                            }
                            PersistedStatus::Running => {}
                        }
                    }
                }
            }
        }

        self.save_locked(&state)?;
        Ok(CommitCheckpointResult {
            ok: true,
            sequence: next_sequence,
        })
    }

    pub fn read_output<W>(&self, handle: &ChildHandle<W>) -> Result<W::Output, WorkflowError>
    where
        W: Workflow,
    {
        let state = self.lock()?;
        let key = instance_key(&handle.workflow_id, &handle.run_id);
        let instance = state.instances.get(&key).ok_or_else(|| {
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

        serde_json::from_value(instance.output.clone().unwrap_or(JsonValue::Null))
            .map_err(WorkflowError::from)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Store>, WorkflowError> {
        self.state
            .lock()
            .map_err(|_| WorkflowError::new("durability provider lock poisoned"))
    }

    fn save_locked(&self, state: &Store) -> Result<(), WorkflowError> {
        if let Some(parent) = self.file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self
            .file_path
            .with_extension(format!("{}.tmp", std::process::id()));
        fs::write(&tmp_path, serde_json::to_string_pretty(state)?)?;
        fs::rename(tmp_path, &self.file_path)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CreateInstanceInput {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub waits: Vec<DurableWait>,
    pub now: DateTime<Utc>,
    pub parent: Option<ParentLink>,
    pub conflict_policy: Option<ConflictPolicy>,
}

#[derive(Clone, Debug)]
pub struct CreateChildInstanceInput {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
    pub common: JsonValue,
    pub phase: PhaseSnapshot,
    pub waits: Vec<DurableWait>,
    pub now: DateTime<Utc>,
    pub parent_workflow_id: String,
    pub parent_run_id: String,
    pub activation_id: String,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChildHandleValue {
    pub workflow_name: String,
    pub workflow_version: u32,
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug)]
pub enum EffectReservation {
    Reserved {
        effect_id: String,
        idempotency_key: String,
        heartbeat_details: Option<JsonValue>,
    },
    Completed {
        result: JsonValue,
    },
    Failed {
        error: SerializedError,
    },
}

#[derive(Clone, Debug)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommitCheckpointResult {
    pub ok: bool,
    pub sequence: u64,
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
    provider: JsonFileDurabilityProvider,
    workflows: Arc<Mutex<HashMap<String, Arc<dyn ErasedWorkflow>>>>,
    clock: Clock,
}

impl DurableRuntime {
    pub fn new(provider: JsonFileDurabilityProvider) -> Self {
        Self {
            provider,
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(Utc::now),
        }
    }

    pub fn with_clock(
        provider: JsonFileDurabilityProvider,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        Self {
            provider,
            workflows: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(clock),
        }
    }

    pub fn provider(&self) -> &JsonFileDurabilityProvider {
        &self.provider
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

        self.provider.create_instance(CreateInstanceInput {
            workflow_name: W::NAME.to_string(),
            workflow_version: W::VERSION,
            workflow_id,
            run_id,
            common: start.common,
            phase: start.phase,
            waits,
            now,
            parent: None,
            conflict_policy: options.conflict_policy,
        })
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
        self.provider.append_signal(
            ref_.workflow_id.clone(),
            ref_.run_id.clone(),
            signal_type.into(),
            serde_json::to_value(payload)?,
            self.now(),
        )
    }

    pub async fn query<W, Q>(&self, ref_: &InstanceRef, name: &str) -> Result<Q, WorkflowError>
    where
        W: Workflow,
        Q: DeserializeOwned,
    {
        self.register::<W>()?;
        let workflow = self.workflow(W::NAME)?;
        let instance = self.require_instance(ref_)?;
        let value = workflow.query_value(name, &instance)?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn drain(&self, options: DrainOptions) -> Result<DrainResult, WorkflowError> {
        let max_activations = options.max_activations.unwrap_or(100);
        let mut activations = 0;

        while activations < max_activations {
            let Some(activation) = self.next_activation()? else {
                break;
            };
            self.run_activation(activation).await?;
            activations += 1;
        }

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

    fn require_instance(&self, ref_: &InstanceRef) -> Result<PersistedInstance, WorkflowError> {
        self.provider.load_instance(ref_)?.ok_or_else(|| {
            WorkflowError::new(format!(
                "unknown workflow instance: {}/{}",
                ref_.workflow_id, ref_.run_id
            ))
        })
    }

    fn next_activation(&self) -> Result<Option<ReadyActivation>, WorkflowError> {
        let instances = self.provider.list_instances()?;
        let signals = self.provider.list_signals()?;
        let children = self.provider.list_children()?;
        let now = self.now();
        let mut ready = Vec::new();

        for instance in instances {
            if instance.status != PersistedStatus::Running {
                continue;
            }

            let Ok(workflow) = self.workflow(&instance.workflow_name) else {
                continue;
            };

            if instance.workflow_version < workflow.version() {
                ready.push(ReadyActivation {
                    kind: ReadyActivationKind::Migration,
                    workflow,
                    instance: instance.clone(),
                    activation_id: activation_id(
                        &instance,
                        "migration",
                        &format!(
                            "{}->{}",
                            instance.workflow_version,
                            instance.workflow_version + 1
                        ),
                    ),
                    wait_name: None,
                    event: None,
                    sort: vec![
                        format_time(instance.updated_at),
                        "migration".to_string(),
                        instance.workflow_id.clone(),
                    ],
                });
                continue;
            }

            if instance.workflow_version > workflow.version() {
                return Err(WorkflowError::new(format!(
                    "workflow {} instance {}/{} is at newer version {}; worker only has {}",
                    workflow.name(),
                    instance.workflow_id,
                    instance.run_id,
                    instance.workflow_version,
                    workflow.version()
                )));
            }

            for wait in &instance.waits {
                match wait {
                    DurableWait::Run { name, ready_at } => {
                        ready.push(ReadyActivation {
                            kind: ReadyActivationKind::Run,
                            workflow: workflow.clone(),
                            instance: instance.clone(),
                            activation_id: activation_id(&instance, "run", name),
                            wait_name: None,
                            event: None,
                            sort: vec![
                                format_time(*ready_at),
                                "run".to_string(),
                                name.clone(),
                                instance.workflow_id.clone(),
                            ],
                        });
                    }
                    DurableWait::Signal { name, r#type, .. } => {
                        let signal = signals
                            .iter()
                            .filter(|candidate| {
                                candidate.workflow_id == instance.workflow_id
                                    && candidate.run_id == instance.run_id
                                    && candidate.r#type == *r#type
                                    && candidate.consumed_by_sequence.is_none()
                            })
                            .min_by(compare_signals);

                        if let Some(signal) = signal {
                            ready.push(ReadyActivation {
                                kind: ReadyActivationKind::Event,
                                workflow: workflow.clone(),
                                instance: instance.clone(),
                                activation_id: activation_id(
                                    &instance,
                                    "signal",
                                    &signal.signal_id,
                                ),
                                wait_name: Some(name.clone()),
                                event: Some(ReadyEvent::Signal {
                                    signal_id: signal.signal_id.clone(),
                                    payload: signal.payload.clone(),
                                    occurred_at: signal.received_at,
                                }),
                                sort: vec![
                                    format_time(signal.received_at),
                                    "signal".to_string(),
                                    name.clone(),
                                    signal.signal_id.clone(),
                                ],
                            });
                        }
                    }
                    DurableWait::Timer { name, fire_at } if *fire_at <= now => {
                        ready.push(ReadyActivation {
                            kind: ReadyActivationKind::Event,
                            workflow: workflow.clone(),
                            instance: instance.clone(),
                            activation_id: activation_id(
                                &instance,
                                "timer",
                                &format!("{}:{}", name, format_time(*fire_at)),
                            ),
                            wait_name: Some(name.clone()),
                            event: Some(ReadyEvent::Timer {
                                fired_at: now,
                                occurred_at: *fire_at,
                            }),
                            sort: vec![
                                format_time(*fire_at),
                                "timer".to_string(),
                                name.clone(),
                                format!("{}:{}", name, format_time(*fire_at)),
                            ],
                        });
                    }
                    DurableWait::Timer { .. } => {}
                    DurableWait::Child {
                        name,
                        workflow_id,
                        run_id,
                        ..
                    } => {
                        let child = children.iter().find(|record| {
                            record.workflow_id == *workflow_id
                                && record.run_id == *run_id
                                && record.status != ChildStatus::Started
                                && record.delivered_by_sequence.is_none()
                        });

                        if let Some(child) = child {
                            let occurred_at = child.completed_at.unwrap_or(now);
                            ready.push(ReadyActivation {
                                kind: ReadyActivationKind::Event,
                                workflow: workflow.clone(),
                                instance: instance.clone(),
                                activation_id: activation_id(
                                    &instance,
                                    "child",
                                    &child.child_record_id,
                                ),
                                wait_name: Some(name.clone()),
                                event: Some(ReadyEvent::Child {
                                    child_record_id: child.child_record_id.clone(),
                                    occurred_at,
                                    event: match child.status {
                                        ChildStatus::Completed => ChildEventValue::Ok {
                                            output: child.output.clone().unwrap_or(JsonValue::Null),
                                        },
                                        ChildStatus::Failed | ChildStatus::Started => {
                                            ChildEventValue::Err {
                                                error: child.error.clone().unwrap_or(
                                                    SerializedError {
                                                        message: "child failed".to_string(),
                                                    },
                                                ),
                                            }
                                        }
                                    },
                                }),
                                sort: vec![
                                    format_time(occurred_at),
                                    "child".to_string(),
                                    name.clone(),
                                    child.child_record_id.clone(),
                                ],
                            });
                        }
                    }
                }
            }
        }

        ready.sort_by(compare_activations);
        Ok(ready.into_iter().next())
    }

    async fn run_activation(&self, activation: ReadyActivation) -> Result<(), WorkflowError> {
        let ref_ = InstanceRef::new(
            activation.instance.workflow_id.clone(),
            activation.instance.run_id.clone(),
        );
        let latest = self.require_instance(&ref_)?;
        if latest.status != PersistedStatus::Running
            || latest.sequence != activation.instance.sequence
        {
            return Ok(());
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
            self.provider.commit_checkpoint(CommitCheckpointInput {
                workflow_id: latest.workflow_id,
                run_id: latest.run_id,
                expected_sequence: latest.sequence,
                activation_id: activation.activation_id,
                workflow_version: activation.workflow.version(),
                next,
                waits,
                now: self.now(),
                consume_signal_id: None,
                consume_child_record_id: None,
            })?;
            return Ok(());
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
            sequence: latest.sequence,
            activation_id: activation.activation_id.clone(),
            clock: self.clock.clone(),
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

        self.provider.commit_checkpoint(CommitCheckpointInput {
            workflow_id: latest.workflow_id,
            run_id: latest.run_id,
            expected_sequence: latest.sequence,
            activation_id: activation.activation_id,
            workflow_version: activation.workflow.version(),
            next,
            waits,
            now: self.now(),
            consume_signal_id,
            consume_child_record_id,
        })?;

        Ok(())
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

#[derive(Clone, Debug, Default)]
pub struct DrainOptions {
    pub max_activations: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainResult {
    pub activations: usize,
}

pub struct DurableContext {
    provider: JsonFileDurabilityProvider,
    registry: Arc<Mutex<HashMap<String, Arc<dyn ErasedWorkflow>>>>,
    workflow_id: String,
    run_id: String,
    sequence: u64,
    activation_id: String,
    clock: Clock,
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
        match self.provider.get_or_reserve_effect(
            &self.workflow_id,
            &self.run_id,
            &self.activation_id,
            key,
        )? {
            EffectReservation::Completed { result } => Ok(serde_json::from_value(result)?),
            EffectReservation::Failed { error } => Err(WorkflowError::new(error.message)),
            EffectReservation::Reserved {
                effect_id,
                idempotency_key,
                heartbeat_details,
            } => {
                let context = ActivityContext {
                    idempotency_key,
                    last_heartbeat_details: heartbeat_details,
                };
                match f(context).await {
                    Ok(result) => {
                        self.provider.complete_effect(
                            &self.workflow_id,
                            &self.run_id,
                            &effect_id,
                            serde_json::to_value(&result)?,
                        )?;
                        Ok(result)
                    }
                    Err(error) => {
                        self.provider.fail_effect(
                            &self.workflow_id,
                            &self.run_id,
                            &effect_id,
                            SerializedError {
                                message: error.message.clone(),
                            },
                        )?;
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
            format!("{}__{}__{}", self.workflow_id, self.sequence, safe_id(key))
        });
        let run_id = "run-1".to_string();
        let waits = adapter.materialize_waits(&start.common, &start.phase, now)?;

        let handle = self
            .provider
            .create_child_instance(CreateChildInstanceInput {
                workflow_name: W::NAME.to_string(),
                workflow_version: W::VERSION,
                workflow_id,
                run_id,
                common: start.common,
                phase: start.phase,
                waits,
                now,
                parent_workflow_id: self.workflow_id.clone(),
                parent_run_id: self.run_id.clone(),
                activation_id: self.activation_id.clone(),
                key: key.to_string(),
            })?;

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
        self.provider.read_output(handle)
    }

    pub async fn child_cancel<W>(&mut self, _handle: &ChildHandle<W>) -> Result<(), WorkflowError>
    where
        W: Workflow,
    {
        Ok(())
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
    sort: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadyActivationKind {
    Migration,
    Run,
    Event,
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

fn compare_activations(left: &ReadyActivation, right: &ReadyActivation) -> std::cmp::Ordering {
    left.sort.cmp(&right.sort)
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

fn child_handle_value(record: &ChildRecord) -> ChildHandleValue {
    ChildHandleValue {
        workflow_name: record.workflow_name.clone(),
        workflow_version: record.workflow_version,
        workflow_id: record.workflow_id.clone(),
        run_id: record.run_id.clone(),
    }
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
macro_rules! checkpoint {
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
            message: $error.to_string(),
        }))
    };
}
