use chrono::{DateTime, TimeZone, Utc};
use durable::{
    complete, go, start, workflow, ChildEvent, ChildHandle, ChildOptions, DrainOptions,
    DurabilityProvider, DurableRuntime, InstanceRef, ParentClosePolicy, PersistedInstance,
    PersistedStatus, RuntimeOptions, SqliteDurabilityProvider, SqliteShardFileDurabilityProvider,
    StartOptions, WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

static WORKFLOW_STARTS: AtomicU64 = AtomicU64::new(0);
static SIGNALS: AtomicU64 = AtomicU64::new(0);
static CHILD_STARTS: AtomicU64 = AtomicU64::new(0);
static CHILD_COMPLETIONS: AtomicU64 = AtomicU64::new(0);
static TIMER_HANDLERS: AtomicU64 = AtomicU64::new(0);
static ACTIVITIES: AtomicU64 = AtomicU64::new(0);

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    if let Err(error) = runtime.block_on(async_main()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn async_main() -> Result<(), WorkflowError> {
    let options = BenchOptions::parse()?;
    reset_counters();
    let started_at = Instant::now();
    let setup_started_at = Instant::now();
    let provider = options.provider().await?;
    let clock = ManualClock::new();
    let runtimes = (0..options.workers)
        .map(|worker| {
            let mut runtime_options = RuntimeOptions::default();
            runtime_options.worker_id = format!("rust-bench-worker-{worker}");
            runtime_options.shard_count = options.shards;
            runtime_options.dispatch_shard_ids =
                dispatch_shard_ids_for_worker(worker, options.workers, options.shards);
            runtime_options.max_concurrent_activations = options.activation_concurrency;
            runtime_options.activation_prefetch_limit = options.activation_prefetch_limit;
            runtime_options.commit_batch_size = options.batch;
            DurableRuntime::from_provider_with_options(provider.clone(), runtime_options, {
                let clock = clock.clone();
                move || clock.now()
            })
        })
        .collect::<Vec<_>>();
    for runtime in &runtimes {
        register_mode(runtime, options.mode)?;
    }

    let mut refs = Vec::new();
    for index in 0..options.workflows {
        let workflow_id = format!("{}-bench-{index}", options.mode.as_str());
        let ref_ = match options.mode {
            BenchMode::Bare => {
                runtimes[0]
                    .start::<BareWorkflow>(
                        BenchInput { index },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?
            }
            BenchMode::Activity => {
                runtimes[0]
                    .start::<ActivityWorkflow>(
                        BenchInput { index },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?
            }
            BenchMode::Signal => {
                let started = runtimes[0]
                    .start::<SignalWorkflow>(
                        BenchInput { index },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?;
                let ref_ = started.instance_ref();
                runtimes[0]
                    .signal(
                        &ref_,
                        "finish",
                        BenchSignal {
                            value: index + 1_000,
                        },
                    )
                    .await?;
                SIGNALS.fetch_add(1, Ordering::Relaxed);
                started
            }
            BenchMode::Timer => {
                runtimes[0]
                    .start::<TimerWorkflow>(
                        TimerInput {
                            index,
                            wake_at: clock.now(),
                        },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?
            }
            BenchMode::Child => {
                runtimes[0]
                    .start::<ChildParentWorkflow>(
                        BenchInput { index },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?
            }
            BenchMode::Mixed => {
                let started = runtimes[0]
                    .start::<MixedParentWorkflow>(
                        BenchInput { index },
                        StartOptions {
                            workflow_id: Some(workflow_id),
                            ..StartOptions::default()
                        },
                    )
                    .await?;
                let ref_ = started.instance_ref();
                runtimes[0]
                    .signal(
                        &ref_,
                        "finish",
                        BenchSignal {
                            value: index + 1_000,
                        },
                    )
                    .await?;
                SIGNALS.fetch_add(1, Ordering::Relaxed);
                started
            }
        };
        WORKFLOW_STARTS.fetch_add(1, Ordering::Relaxed);
        refs.push(ref_.instance_ref());
    }
    let setup_ms = setup_started_at.elapsed().as_secs_f64() * 1_000.0;
    let processing_started_at = Instant::now();
    let mut rounds = 0usize;
    let mut activations = 0usize;
    while rounds < options.max_rounds {
        rounds += 1;
        let mut round_activations = 0usize;
        for runtime in &runtimes {
            let result = runtime
                .drain(DrainOptions {
                    max_activations: Some(options.activation_prefetch_limit.max(1)),
                })
                .await?;
            round_activations += result.activations;
        }
        activations += round_activations;
        if completed_count(provider.as_ref(), &refs).await? == options.workflows {
            break;
        }
        if round_activations == 0 {
            break;
        }
    }
    let processing_ms = processing_started_at.elapsed().as_secs_f64() * 1_000.0;
    let verify_started_at = Instant::now();
    let root_instances = load_root_instances(provider.as_ref(), &refs).await?;
    let completed = root_instances
        .iter()
        .filter(|instance| instance.status == PersistedStatus::Completed)
        .count();
    if completed != options.workflows {
        return Err(WorkflowError::new(format!(
            "benchmark did not complete: {completed}/{} workflows completed",
            options.workflows
        )));
    }
    verify_benchmark(&options, &root_instances, activations)?;
    let verify_ms = verify_started_at.elapsed().as_secs_f64() * 1_000.0;
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1_000.0;
    let mixed_actions = mixed_actions();
    let output = json!({
        "backend": options.provider.as_str(),
        "mode": options.mode.as_str(),
        "correct": true,
        "options": options.to_json(),
        "elapsedMs": elapsed_ms,
        "setupMs": setup_ms,
        "processingMs": processing_ms,
        "verifyMs": verify_ms,
        "rounds": rounds,
        "activations": activations,
        "expectedActivations": options.workflows * options.mode.activations_per_workflow(),
        "completedWorkflows": completed,
        "activeWorkers": options.workers,
        "mixedActions": mixed_actions,
        "activationsPerSecond": per_second(activations as u64, elapsed_ms),
        "mixedActionsPerSecond": per_second(mixed_actions, elapsed_ms),
        "workflowsPerSecond": per_second(completed as u64, elapsed_ms),
        "processingActivationsPerSecond": per_second(activations as u64, processing_ms),
        "processingMixedActionsPerSecond": per_second(mixed_actions, processing_ms),
        "processingWorkflowsPerSecond": per_second(completed as u64, processing_ms),
        "counters": {
            "workflowStarts": WORKFLOW_STARTS.load(Ordering::Relaxed),
            "signals": SIGNALS.load(Ordering::Relaxed),
            "childStarts": CHILD_STARTS.load(Ordering::Relaxed),
            "childCompletions": CHILD_COMPLETIONS.load(Ordering::Relaxed),
            "timerHandlers": TIMER_HANDLERS.load(Ordering::Relaxed),
            "activities": ACTIVITIES.load(Ordering::Relaxed)
        }
    });
    if options.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "rust {} {}: {:.0} workflows/s, {:.0} activations/s",
            options.provider.as_str(),
            options.mode.as_str(),
            per_second(completed as u64, processing_ms),
            per_second(activations as u64, processing_ms)
        );
    }
    Ok(())
}

fn register_mode(runtime: &DurableRuntime, mode: BenchMode) -> Result<(), WorkflowError> {
    match mode {
        BenchMode::Bare => runtime.register::<BareWorkflow>(),
        BenchMode::Activity => runtime.register::<ActivityWorkflow>(),
        BenchMode::Signal => runtime.register::<SignalWorkflow>(),
        BenchMode::Timer => runtime.register::<TimerWorkflow>(),
        BenchMode::Child => {
            runtime.register::<BenchChildOnlyWorkflow>()?;
            runtime.register::<ChildParentWorkflow>()
        }
        BenchMode::Mixed => {
            runtime.register::<BenchChildWorkflow>()?;
            runtime.register::<MixedParentWorkflow>()
        }
    }
}

async fn completed_count(
    provider: &dyn DurabilityProvider,
    refs: &[InstanceRef],
) -> Result<usize, WorkflowError> {
    let mut completed = 0;
    for ref_ in refs {
        if provider
            .load_instance(
                ref_,
                durable::LoadInstanceOptions {
                    include_effects: false,
                },
            )
            .await?
            .is_some_and(|instance| instance.status == durable::PersistedStatus::Completed)
        {
            completed += 1;
        }
    }
    Ok(completed)
}

async fn load_root_instances(
    provider: &dyn DurabilityProvider,
    refs: &[InstanceRef],
) -> Result<Vec<PersistedInstance>, WorkflowError> {
    let mut instances = Vec::with_capacity(refs.len());
    for ref_ in refs {
        let instance = provider
            .load_instance(
                ref_,
                durable::LoadInstanceOptions {
                    include_effects: false,
                },
            )
            .await?
            .ok_or_else(|| {
                WorkflowError::new(format!(
                    "missing workflow instance: {}/{}",
                    ref_.workflow_id, ref_.run_id
                ))
            })?;
        instances.push(instance);
    }
    Ok(instances)
}

fn verify_benchmark(
    options: &BenchOptions,
    instances: &[PersistedInstance],
    activations: usize,
) -> Result<(), WorkflowError> {
    let expected_activations = options.workflows * options.mode.activations_per_workflow();
    if activations != expected_activations {
        return Err(WorkflowError::new(format!(
            "benchmark processed {activations} activations, expected {expected_activations}"
        )));
    }
    for (index, instance) in instances.iter().enumerate() {
        let expected_workflow_id = format!("{}-bench-{index}", options.mode.as_str());
        if instance.workflow_id != expected_workflow_id {
            return Err(WorkflowError::new(format!(
                "unexpected workflow id at index {index}: {}",
                instance.workflow_id
            )));
        }
        if instance.status != PersistedStatus::Completed {
            return Err(WorkflowError::new(format!(
                "workflow {expected_workflow_id} finished with status {:?}",
                instance.status
            )));
        }
        let output = instance.output.as_ref().ok_or_else(|| {
            WorkflowError::new(format!("workflow {expected_workflow_id} missing output"))
        })?;
        let output_index = output_usize(output, "index", &expected_workflow_id)?;
        if output_index != index {
            return Err(WorkflowError::new(format!(
                "workflow {expected_workflow_id} output index {output_index}, expected {index}"
            )));
        }
        let value = output_usize(output, "value", &expected_workflow_id)?;
        let expected_value = match options.mode {
            BenchMode::Mixed => index * 10 + index + 1_000,
            BenchMode::Signal => index + 1_000,
            BenchMode::Child => index * 10,
            BenchMode::Bare | BenchMode::Activity | BenchMode::Timer => index,
        };
        if value != expected_value {
            return Err(WorkflowError::new(format!(
                "workflow {expected_workflow_id} output value {value}, expected {expected_value}"
            )));
        }
    }

    let workflows = options.workflows as u64;
    expect_counter(
        "workflowStarts",
        WORKFLOW_STARTS.load(Ordering::Relaxed),
        workflows,
    )?;
    expect_counter(
        "signals",
        SIGNALS.load(Ordering::Relaxed),
        if matches!(options.mode, BenchMode::Signal | BenchMode::Mixed) {
            workflows
        } else {
            0
        },
    )?;
    expect_counter(
        "childStarts",
        CHILD_STARTS.load(Ordering::Relaxed),
        if matches!(options.mode, BenchMode::Child | BenchMode::Mixed) {
            workflows
        } else {
            0
        },
    )?;
    expect_counter(
        "childCompletions",
        CHILD_COMPLETIONS.load(Ordering::Relaxed),
        if matches!(options.mode, BenchMode::Child | BenchMode::Mixed) {
            workflows
        } else {
            0
        },
    )?;
    expect_counter(
        "timerHandlers",
        TIMER_HANDLERS.load(Ordering::Relaxed),
        if matches!(options.mode, BenchMode::Timer | BenchMode::Mixed) {
            workflows
        } else {
            0
        },
    )?;
    let expected_activities = match options.mode {
        BenchMode::Activity => workflows,
        BenchMode::Mixed => workflows * 3,
        BenchMode::Bare | BenchMode::Signal | BenchMode::Timer | BenchMode::Child => 0,
    };
    expect_counter(
        "activities",
        ACTIVITIES.load(Ordering::Relaxed),
        expected_activities,
    )?;
    Ok(())
}

fn output_usize(
    output: &JsonValue,
    field: &str,
    workflow_id: &str,
) -> Result<usize, WorkflowError> {
    let value = output
        .get(field)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            WorkflowError::new(format!(
                "workflow {workflow_id} missing numeric output field {field}"
            ))
        })?;
    usize::try_from(value).map_err(|error| {
        WorkflowError::new(format!(
            "workflow {workflow_id} output field {field} out of range: {error}"
        ))
    })
}

fn expect_counter(name: &str, actual: u64, expected: u64) -> Result<(), WorkflowError> {
    if actual != expected {
        return Err(WorkflowError::new(format!(
            "counter {name} was {actual}, expected {expected}"
        )));
    }
    Ok(())
}

fn reset_counters() {
    WORKFLOW_STARTS.store(0, Ordering::Relaxed);
    SIGNALS.store(0, Ordering::Relaxed);
    CHILD_STARTS.store(0, Ordering::Relaxed);
    CHILD_COMPLETIONS.store(0, Ordering::Relaxed);
    TIMER_HANDLERS.store(0, Ordering::Relaxed);
    ACTIVITIES.store(0, Ordering::Relaxed);
}

fn mixed_actions() -> u64 {
    WORKFLOW_STARTS.load(Ordering::Relaxed)
        + SIGNALS.load(Ordering::Relaxed)
        + CHILD_STARTS.load(Ordering::Relaxed)
        + CHILD_COMPLETIONS.load(Ordering::Relaxed)
        + TIMER_HANDLERS.load(Ordering::Relaxed)
        + ACTIVITIES.load(Ordering::Relaxed)
}

fn per_second(count: u64, elapsed_ms: f64) -> f64 {
    if elapsed_ms <= 0.0 {
        0.0
    } else {
        count as f64 / (elapsed_ms / 1_000.0)
    }
}

fn dispatch_shard_ids_for_worker(worker: usize, workers: usize, shards: u32) -> Vec<u32> {
    let mut ids = (0..shards)
        .filter(|shard| (*shard as usize) % workers == worker)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        ids = (0..shards).collect();
    }
    ids
}

#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )),
        }
    }

    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

#[derive(Clone, Copy, Debug)]
enum BenchMode {
    Mixed,
    Bare,
    Activity,
    Signal,
    Timer,
    Child,
}

impl BenchMode {
    fn parse(value: &str) -> Result<Self, WorkflowError> {
        match value {
            "mixed" => Ok(Self::Mixed),
            "bare" => Ok(Self::Bare),
            "activity" => Ok(Self::Activity),
            "signal" => Ok(Self::Signal),
            "timer" => Ok(Self::Timer),
            "child" => Ok(Self::Child),
            _ => Err(WorkflowError::new(format!("unknown mode: {value}"))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::Bare => "bare",
            Self::Activity => "activity",
            Self::Signal => "signal",
            Self::Timer => "timer",
            Self::Child => "child",
        }
    }

    fn activations_per_workflow(self) -> usize {
        match self {
            Self::Mixed => 5,
            Self::Bare | Self::Activity | Self::Signal | Self::Timer => 1,
            Self::Child => 3,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum BenchProvider {
    Null,
    Sqlite,
    SqliteShardFile,
    Postgres,
}

impl BenchProvider {
    fn parse(value: &str) -> Result<Self, WorkflowError> {
        match value {
            "null" => Ok(Self::Null),
            "sqlite" => Ok(Self::Sqlite),
            "sqlite-shard-file" | "sqlite_shard_file" => Ok(Self::SqliteShardFile),
            "postgres" => Ok(Self::Postgres),
            _ => Err(WorkflowError::new(format!("unknown provider: {value}"))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Sqlite => "sqlite",
            Self::SqliteShardFile => "sqlite-shard-file",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Debug)]
struct BenchOptions {
    provider: BenchProvider,
    mode: BenchMode,
    workflows: usize,
    workers: usize,
    shards: u32,
    activation_concurrency: usize,
    activation_prefetch_limit: usize,
    batch: usize,
    max_rounds: usize,
    path: Option<PathBuf>,
    postgres_url: Option<String>,
    physical_partitions: u32,
    json: bool,
}

impl BenchOptions {
    fn parse() -> Result<Self, WorkflowError> {
        let mut options = Self {
            provider: BenchProvider::Null,
            mode: BenchMode::Mixed,
            workflows: 250,
            workers: 4,
            shards: 4,
            activation_concurrency: 4,
            activation_prefetch_limit: 32,
            batch: 32,
            max_rounds: 10_000,
            path: None,
            postgres_url: env::var("DURABLE_POSTGRES_URL").ok(),
            physical_partitions: 4,
            json: false,
        };
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--provider" => {
                    options.provider = BenchProvider::parse(&require_arg(&mut args, "--provider")?)?
                }
                "--mode" => options.mode = BenchMode::parse(&require_arg(&mut args, "--mode")?)?,
                "--workflows" => options.workflows = parse_arg(&mut args, "--workflows")?,
                "--workers" => options.workers = parse_arg(&mut args, "--workers")?,
                "--shards" => options.shards = parse_arg(&mut args, "--shards")?,
                "--activation-concurrency" => {
                    options.activation_concurrency =
                        parse_arg(&mut args, "--activation-concurrency")?
                }
                "--activation-prefetch-limit" => {
                    options.activation_prefetch_limit =
                        parse_arg(&mut args, "--activation-prefetch-limit")?
                }
                "--batch" => options.batch = parse_arg(&mut args, "--batch")?,
                "--max-rounds" => options.max_rounds = parse_arg(&mut args, "--max-rounds")?,
                "--path" => options.path = Some(PathBuf::from(require_arg(&mut args, "--path")?)),
                "--postgres-url" => {
                    options.postgres_url = Some(require_arg(&mut args, "--postgres-url")?)
                }
                "--physical-partitions" => {
                    options.physical_partitions = parse_arg(&mut args, "--physical-partitions")?
                }
                "--json" => options.json = true,
                _ => return Err(WorkflowError::new(format!("unknown argument: {arg}"))),
            }
        }
        if options.workers == 0 || options.shards == 0 {
            return Err(WorkflowError::new("workers and shards must be positive"));
        }
        Ok(options)
    }

    async fn provider(&self) -> Result<Arc<dyn DurabilityProvider>, WorkflowError> {
        match self.provider {
            BenchProvider::Null => Ok(Arc::new(durable::NullDurabilityProvider::new())),
            BenchProvider::Sqlite => {
                let path = self.path.clone().unwrap_or_else(|| {
                    env::temp_dir().join(format!("durable-rust-bench-{}.sqlite", Uuid::new_v4()))
                });
                Ok(Arc::new(SqliteDurabilityProvider::new(path)?))
            }
            BenchProvider::SqliteShardFile => {
                let path = self.path.clone().unwrap_or_else(|| {
                    env::temp_dir().join(format!("durable-rust-bench-shards-{}", Uuid::new_v4()))
                });
                Ok(Arc::new(SqliteShardFileDurabilityProvider::new(
                    path,
                    self.shards,
                )?))
            }
            BenchProvider::Postgres => {
                let connection_string = self.postgres_url.clone().ok_or_else(|| {
                    WorkflowError::new(
                        "postgres provider requires --postgres-url or DURABLE_POSTGRES_URL",
                    )
                })?;
                Ok(Arc::new(
                    durable::PostgresDurabilityProvider::create(
                        durable::PostgresDurabilityProviderOptions {
                            connection_string,
                            schema: Some(format!("durable_rust_bench_{}", Uuid::new_v4().simple())),
                            physical_partitions: self.physical_partitions,
                            snapshot_interval: Some(512),
                        },
                    )
                    .await?,
                ))
            }
        }
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "workflows": self.workflows,
            "workers": self.workers,
            "shards": self.shards,
            "activationConcurrency": self.activation_concurrency,
            "activationPrefetchLimit": self.activation_prefetch_limit,
            "batch": self.batch,
            "physicalPartitions": self.physical_partitions
        })
    }
}

fn require_arg(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, WorkflowError> {
    args.next()
        .ok_or_else(|| WorkflowError::new(format!("{name} requires a value")))
}

fn parse_arg<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T, WorkflowError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = require_arg(args, name)?;
    raw.parse::<T>()
        .map_err(|error| WorkflowError::new(format!("invalid {name}: {error}")))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchInput {
    index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimerInput {
    index: usize,
    wake_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchSignal {
    value: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchOutput {
    index: usize,
    value: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Empty {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitingSignal {
    index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitingTimer {
    index: usize,
    wake_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildInput {
    index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitingChild {
    index: usize,
    handle: ChildHandle<BenchChildOnlyWorkflow>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MixedWaitingChild {
    index: usize,
    handle: ChildHandle<BenchChildWorkflow>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MixedWaitingSignal {
    index: usize,
    child_value: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MixedWaitingTimer {
    index: usize,
    child_value: usize,
    signal_value: usize,
    wake_at: DateTime<Utc>,
}

workflow! {
    pub workflow BareWorkflow {
        name: "rust_bench_bare",
        version: 1,
        input: BenchInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! { common: input.clone(), phase: bare(Empty {}) }
        }

        phase bare(_data: Empty) {
            run async |common| {
                complete!(BenchOutput { index: common.index, value: common.index })
            }
        }
    }
}

workflow! {
    pub workflow ActivityWorkflow {
        name: "rust_bench_activity",
        version: 1,
        input: BenchInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! { common: input.clone(), phase: run(Empty {}) }
        }

        phase run(_data: Empty) {
            run async |ctx, common| {
                let value = common.index;
                ctx.activity("activity", move || async move {
                    ACTIVITIES.fetch_add(1, Ordering::Relaxed);
                    Ok(value)
                }).await?;
                complete!(BenchOutput { index: common.index, value })
            }
        }
    }
}

workflow! {
    pub workflow SignalWorkflow {
        name: "rust_bench_signal",
        version: 1,
        input: BenchInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! { common: input.clone(), phase: waiting(WaitingSignal { index: input.index }) }
        }

        phase waiting(_data: WaitingSignal) {
            on {
                finish: signal<BenchSignal> async |common, event| {
                    complete!(BenchOutput { index: common.index, value: event.value })
                },
            }
        }
    }
}

workflow! {
    pub workflow TimerWorkflow {
        name: "rust_bench_timer",
        version: 1,
        input: TimerInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! {
                common: BenchInput { index: input.index },
                phase: waiting(WaitingTimer { index: input.index, wake_at: input.wake_at }),
            }
        }

        phase waiting(data: WaitingTimer) {
            on {
                finish_due: timer(data.wake_at.clone()) async |common| {
                    TIMER_HANDLERS.fetch_add(1, Ordering::Relaxed);
                    complete!(BenchOutput { index: common.index, value: common.index })
                },
            }
        }
    }
}

workflow! {
    pub workflow BenchChildOnlyWorkflow {
        name: "rust_bench_child_only_child",
        version: 1,
        input: ChildInput,
        output: BenchOutput,
        common: ChildInput,

        initial(input) {
            start! { common: input.clone(), phase: run(Empty {}) }
        }

        phase run(_data: Empty) {
            run async |common| {
                complete!(BenchOutput { index: common.index, value: common.index * 10 })
            }
        }
    }
}

workflow! {
    pub workflow BenchChildWorkflow {
        name: "rust_bench_child",
        version: 1,
        input: ChildInput,
        output: BenchOutput,
        common: ChildInput,

        initial(input) {
            start! { common: input.clone(), phase: run(Empty {}) }
        }

        phase run(_data: Empty) {
            run async |ctx, common| {
                let value = common.index * 10;
                ctx.activity("child_activity", move || async move {
                    ACTIVITIES.fetch_add(1, Ordering::Relaxed);
                    Ok(value)
                }).await?;
                complete!(BenchOutput { index: common.index, value })
            }
        }
    }
}

workflow! {
    pub workflow ChildParentWorkflow {
        name: "rust_bench_child_parent",
        version: 1,
        input: BenchInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! { common: input.clone(), phase: boot(Empty {}) }
        }

        phase boot(_data: Empty) {
            run async |ctx, common| {
                let handle = ctx.child_start::<BenchChildOnlyWorkflow>(
                    "child",
                    ChildInput { index: common.index },
                    ChildOptions {
                        parent_close_policy: ParentClosePolicy::Cancel,
                        ..ChildOptions::default()
                    },
                ).await?;
                CHILD_STARTS.fetch_add(1, Ordering::Relaxed);
                go!(waiting_child(WaitingChild { index: common.index, handle }))
            }
        }

        phase waiting_child(data: WaitingChild) {
            on {
                child_done: child(data.handle.clone()) async |common, event| {
                    CHILD_COMPLETIONS.fetch_add(1, Ordering::Relaxed);
                    let value = match event {
                        ChildEvent::Ok { output } => output.value,
                        ChildEvent::Err { .. } => 0,
                    };
                    complete!(BenchOutput { index: common.index, value })
                },
            }
        }
    }
}

workflow! {
    pub workflow MixedParentWorkflow {
        name: "rust_bench_mixed_parent",
        version: 1,
        input: BenchInput,
        output: BenchOutput,
        common: BenchInput,

        initial(input) {
            start! { common: input.clone(), phase: boot(Empty {}) }
        }

        phase boot(_data: Empty) {
            run async |ctx, common| {
                let index = common.index;
                ctx.activity("boot_activity", move || async move {
                    ACTIVITIES.fetch_add(1, Ordering::Relaxed);
                    Ok(index)
                }).await?;
                let handle = ctx.child_start::<BenchChildWorkflow>(
                    "child",
                    ChildInput { index: common.index },
                    ChildOptions::default(),
                ).await?;
                CHILD_STARTS.fetch_add(1, Ordering::Relaxed);
                go!(waiting_child(MixedWaitingChild { index: common.index, handle }))
            }
        }

        phase waiting_child(data: MixedWaitingChild) {
            on {
                child_done: child(data.handle.clone()) async |common, event| {
                    CHILD_COMPLETIONS.fetch_add(1, Ordering::Relaxed);
                    let child_value = match event {
                        ChildEvent::Ok { output } => output.value,
                        ChildEvent::Err { .. } => 0,
                    };
                    go!(waiting_signal(MixedWaitingSignal { index: common.index, child_value }))
                },
            }
        }

        phase waiting_signal(data: MixedWaitingSignal) {
            on {
                finish: signal<BenchSignal> async |ctx, common, data, event| {
                    go!(waiting_timer(MixedWaitingTimer {
                        index: common.index,
                        child_value: data.child_value,
                        signal_value: event.value,
                        wake_at: ctx.now(),
                    }))
                },
            }
        }

        phase waiting_timer(data: MixedWaitingTimer) {
            on {
                finish_due: timer(data.wake_at.clone()) async |ctx, common, data| {
                    TIMER_HANDLERS.fetch_add(1, Ordering::Relaxed);
                    let value = data.child_value + data.signal_value;
                    ctx.activity("finish_activity", move || async move {
                        ACTIVITIES.fetch_add(1, Ordering::Relaxed);
                        Ok(value)
                    }).await?;
                    complete!(BenchOutput { index: common.index, value })
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn benchmark_verification_rejects_skipped_mixed_activity_work() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_counters();
        WORKFLOW_STARTS.store(1, Ordering::Relaxed);
        SIGNALS.store(1, Ordering::Relaxed);
        CHILD_STARTS.store(1, Ordering::Relaxed);
        CHILD_COMPLETIONS.store(1, Ordering::Relaxed);
        TIMER_HANDLERS.store(1, Ordering::Relaxed);
        ACTIVITIES.store(2, Ordering::Relaxed);

        let options = test_options(BenchMode::Mixed, 1);
        let instances = vec![completed_instance(BenchMode::Mixed, 0, 1_000)];
        let error = verify_benchmark(&options, &instances, 5).unwrap_err();

        assert!(
            error
                .message
                .contains("counter activities was 2, expected 3"),
            "{}",
            error.message
        );
    }

    #[test]
    fn benchmark_verification_accepts_child_mode_without_child_activity() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_counters();
        WORKFLOW_STARTS.store(1, Ordering::Relaxed);
        CHILD_STARTS.store(1, Ordering::Relaxed);
        CHILD_COMPLETIONS.store(1, Ordering::Relaxed);

        let options = test_options(BenchMode::Child, 1);
        let instances = vec![completed_instance(BenchMode::Child, 0, 0)];

        verify_benchmark(&options, &instances, 3).unwrap();
    }

    fn test_options(mode: BenchMode, workflows: usize) -> BenchOptions {
        BenchOptions {
            provider: BenchProvider::Null,
            mode,
            workflows,
            workers: 1,
            shards: 1,
            activation_concurrency: 1,
            activation_prefetch_limit: 1,
            batch: 1,
            max_rounds: 10,
            path: None,
            postgres_url: None,
            physical_partitions: 1,
            json: true,
        }
    }

    fn completed_instance(mode: BenchMode, index: usize, value: usize) -> PersistedInstance {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        PersistedInstance {
            workflow_name: "benchmark_test".to_string(),
            workflow_version: 1,
            workflow_id: format!("{}-bench-{index}", mode.as_str()),
            run_id: "run-1".to_string(),
            partition_shard: 0,
            sequence: 1,
            status: PersistedStatus::Completed,
            common: None,
            phase: None,
            output: Some(json!({ "index": index, "value": value })),
            error: None,
            cancel_reason: None,
            waits: Vec::new(),
            effects: Vec::new(),
            created_at: now,
            updated_at: now,
            parent: None,
        }
    }
}
