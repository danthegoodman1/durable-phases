use super::*;

#[derive(Clone)]
pub struct SqliteDurabilityProvider {
    inner: ShardRouter,
    writer: SqliteWriter,
    snapshot_interval: u64,
    operation_lock: Arc<AsyncMutex<()>>,
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
    BeginImmediate {
        response: oneshot::Sender<Result<(), WorkflowError>>,
    },
    Commit {
        response: oneshot::Sender<Result<(), WorkflowError>>,
    },
    Rollback {
        response: oneshot::Sender<Result<(), WorkflowError>>,
    },
    LoadStore {
        response: oneshot::Sender<Result<Store, WorkflowError>>,
    },
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

    async fn begin_immediate(&self) -> Result<(), WorkflowError> {
        self.request(|response| SqliteWriterCommand::BeginImmediate { response })
            .await
    }

    async fn commit(&self) -> Result<(), WorkflowError> {
        self.request(|response| SqliteWriterCommand::Commit { response })
            .await
    }

    async fn rollback(&self) -> Result<(), WorkflowError> {
        self.request(|response| SqliteWriterCommand::Rollback { response })
            .await
    }

    async fn load_store(&self) -> Result<Store, WorkflowError> {
        self.request(|response| SqliteWriterCommand::LoadStore { response })
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
            SqliteWriterCommand::BeginImmediate { response } => {
                let result = connection
                    .execute_batch("BEGIN IMMEDIATE")
                    .map_err(WorkflowError::from);
                let _ = response.send(result);
            }
            SqliteWriterCommand::Commit { response } => {
                let result = connection
                    .execute_batch("COMMIT")
                    .map_err(WorkflowError::from);
                let _ = response.send(result);
            }
            SqliteWriterCommand::Rollback { response } => {
                let result = connection
                    .execute_batch("ROLLBACK")
                    .map_err(WorkflowError::from);
                let _ = response.send(result);
            }
            SqliteWriterCommand::LoadStore { response } => {
                let result = load_sqlite_store(&connection);
                let _ = response.send(result);
            }
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
            operation_lock: Arc::new(AsyncMutex::new(())),
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

    async fn catch_up_unlocked(&self) -> Result<(), WorkflowError> {
        let store = self.writer.load_store().await?;
        self.inner.reset_from_store(store).await
    }

    async fn read_caught_up<T, F, Fut>(&self, read: F) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, WorkflowError>>,
    {
        let _guard = self.operation_lock.lock().await;
        self.catch_up_unlocked().await?;
        read().await
    }

    async fn write_caught_up<T, F, Fut>(&self, apply: F) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, Option<JournalOperation>), WorkflowError>>,
    {
        let _guard = self.operation_lock.lock().await;
        self.writer.begin_immediate().await?;
        let result = async {
            self.catch_up_unlocked().await?;
            let (output, operation) = apply().await?;
            if let Some(operation) = operation {
                self.append_journal_operation(operation).await?;
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                if let Err(error) = self.writer.commit().await {
                    let _ = self.writer.rollback().await;
                    return Err(error);
                }
                Ok(output)
            }
            Err(error) => {
                let _ = self.writer.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn load_instance(
        &self,
        ref_: &InstanceRef,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::load_instance(
                &self.inner,
                ref_,
                LoadInstanceOptions {
                    include_effects: true,
                },
            )
            .await
        })
        .await
    }

    pub async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
        })
        .await
    }

    pub async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
        })
        .await
    }
}

#[async_trait]
impl DurabilityProvider for SqliteDurabilityProvider {
    async fn claim_shard(
        &self,
        input: ClaimShardInput,
    ) -> Result<Option<ShardLease>, WorkflowError> {
        self.write_caught_up(|| async {
            let output =
                <ShardRouter as DurabilityProvider>::claim_shard(&self.inner, input.clone())
                    .await?;
            let operation = if output.is_some() {
                Some(JournalOperation::ClaimShard(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn claim_shard_tasks(
        &self,
        claim: ClaimShardInput,
        input: ClaimShardTasksInput,
    ) -> Result<Option<(ShardLease, ClaimShardTasksResult)>, WorkflowError> {
        self.write_caught_up(|| async {
            let output = <ShardRouter as DurabilityProvider>::claim_shard_tasks(
                &self.inner,
                claim.clone(),
                input.clone(),
            )
            .await?;
            let operation = if output.is_some() {
                Some(JournalOperation::ClaimShardAndTasks { claim, input })
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    fn open_shard(&self, input: OpenShardInput) -> Arc<dyn ShardDurabilitySession> {
        Arc::new(SqliteShardSession {
            provider: self.clone(),
            input,
        })
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<StartWorkflowResult, WorkflowError> {
        self.write_caught_up(|| async {
            let output =
                <ShardRouter as DurabilityProvider>::create_instance(&self.inner, input.clone())
                    .await?;
            let operation = if output.created {
                Some(JournalOperation::CreateInstance(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn create_child_instance(
        &self,
        input: CreateChildInstanceInput,
    ) -> Result<ChildHandleValue, WorkflowError> {
        self.write_caught_up(|| async {
            let should_append = create_child_instance_would_mutate(&self.inner, &input).await?;
            let output = <ShardRouter as DurabilityProvider>::create_child_instance(
                &self.inner,
                input.clone(),
            )
            .await?;
            let operation = if should_append {
                Some(JournalOperation::CreateChildInstance(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::load_instance(&self.inner, ref_, options).await
        })
        .await
    }

    async fn get_workflow_runs(
        &self,
        input: GetWorkflowRunsInput,
    ) -> Result<GetWorkflowRunsResult, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::get_workflow_runs(&self.inner, input).await
        })
        .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        self.write_caught_up(|| async {
            let (output, created) = self.inner.append_signal_with_status(input.clone()).await?;
            let operation = if created {
                Some(JournalOperation::AppendSignal(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn start_send_signal(
        &self,
        input: StartSendSignalInput,
    ) -> Result<StartSendSignalResult, WorkflowError> {
        self.write_caught_up(|| async {
            let (output, mutated) = self
                .inner
                .start_send_signal_with_status(input.clone())
                .await?;
            let operation = if mutated {
                Some(JournalOperation::StartSendSignal(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        self.write_caught_up(|| async {
            let should_append = cancel_child_would_mutate(&self.inner, &input).await?;
            <ShardRouter as DurabilityProvider>::cancel_child(&self.inner, input.clone()).await?;
            let operation = if should_append {
                Some(JournalOperation::CancelChild(input))
            } else {
                None
            };
            Ok(((), operation))
        })
        .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        self.write_caught_up(|| async {
            let output = <ShardRouter as DurabilityProvider>::get_or_reserve_effect(
                &self.inner,
                input.clone(),
            )
            .await?;
            let operation = if let EffectReservation::Reserved { effect_id, .. } = &output {
                let effect = reserved_effect_record_from_router(
                    &self.inner,
                    &input.workflow_id,
                    &input.run_id,
                    effect_id,
                )
                .await?;
                Some(JournalOperation::PutEffectRecord {
                    workflow_id: input.workflow_id,
                    run_id: input.run_id,
                    effect,
                })
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        self.write_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.inner, input.clone())
                .await?;
            Ok(((), Some(JournalOperation::HeartbeatEffect(input))))
        })
        .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        self.write_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::complete_effect(&self.inner, input.clone())
                .await?;
            Ok(((), Some(JournalOperation::CompleteEffect(input))))
        })
        .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        self.write_caught_up(|| async {
            let output =
                <ShardRouter as DurabilityProvider>::fail_effect(&self.inner, input.clone())
                    .await?;
            Ok((output, Some(JournalOperation::FailEffect(input))))
        })
        .await
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        self.write_caught_up(|| async {
            let output =
                <ShardRouter as DurabilityProvider>::commit_checkpoint(&self.inner, input.clone())
                    .await?;
            let operation = if output.ok {
                Some(JournalOperation::CommitCheckpoint(input))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        self.write_caught_up(|| async {
            let output = <ShardRouter as DurabilityProvider>::commit_activations(
                &self.inner,
                inputs.clone(),
            )
            .await?;
            let operation = if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
                Some(JournalOperation::CommitActivations(inputs))
            } else {
                None
            };
            Ok((output, operation))
        })
        .await
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        self.write_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::record_activation_failures(
                &self.inner,
                inputs.clone(),
            )
            .await?;
            Ok(((), Some(JournalOperation::RecordActivationFailures(inputs))))
        })
        .await
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_instances(&self.inner).await
        })
        .await
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
        })
        .await
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        self.read_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
        })
        .await
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        let _guard = self.operation_lock.lock().await;
        self.inner.shutdown().await?;
        self.writer.shutdown().await
    }
}

struct SqliteShardSession {
    provider: SqliteDurabilityProvider,
    input: OpenShardInput,
}

#[async_trait]
impl ShardDurabilitySession for SqliteShardSession {
    fn shard_id(&self) -> u32 {
        self.input.shard_id
    }

    fn owner_id(&self) -> Option<&str> {
        self.input.owner_id.as_deref()
    }

    fn lease_epoch(&self) -> Option<u64> {
        self.input.lease_epoch
    }

    async fn claim_tasks(
        &self,
        input: ClaimShardTasksInput,
    ) -> Result<ClaimShardTasksResult, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner = <ShardRouter as DurabilityProvider>::open_shard(
                    &self.provider.inner,
                    session.clone(),
                );
                let output = inner.claim_tasks(input.clone()).await?;
                let operation = if !output.claims.is_empty() {
                    Some(JournalOperation::ClaimShardTasks { session, input })
                } else {
                    None
                };
                Ok((output, operation))
            })
            .await
    }

    async fn read_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .read_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.read_instance(ref_, options).await
            })
            .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let (output, created) = inner.append_signal_with_status(input.clone()).await?;
                let operation = if created {
                    Some(JournalOperation::AppendSignal(input))
                } else {
                    None
                };
                Ok((output, operation))
            })
            .await
    }

    async fn start_send_signal(
        &self,
        input: StartSendSignalInput,
    ) -> Result<StartSendSignalResult, WorkflowError> {
        self.provider
            .write_caught_up(|| async {
                let (output, mutated) = self
                    .provider
                    .inner
                    .start_send_signal_with_status(input.clone())
                    .await?;
                let operation = if mutated {
                    Some(JournalOperation::StartSendSignal(input))
                } else {
                    None
                };
                Ok((output, operation))
            })
            .await
    }

    async fn append_signal_with_status(
        &self,
        input: AppendSignalInput,
    ) -> Result<(SignalRecord, bool), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let (output, created) = inner.append_signal_with_status(input.clone()).await?;
                let operation = if created {
                    Some(JournalOperation::AppendSignal(input))
                } else {
                    None
                };
                Ok(((output, created), operation))
            })
            .await
    }

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let should_append = cancel_child_would_mutate(&self.provider.inner, &input).await?;
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.cancel_child(input.clone()).await?;
                let operation = if should_append {
                    Some(JournalOperation::CancelChild(input))
                } else {
                    None
                };
                Ok(((), operation))
            })
            .await
    }

    async fn get_or_reserve_effect(
        &self,
        input: ReserveEffectInput,
    ) -> Result<EffectReservation, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let output = inner.get_or_reserve_effect(input.clone()).await?;
                let operation = if let EffectReservation::Reserved { effect_id, .. } = &output {
                    let effect = inner
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
                    Some(JournalOperation::PutEffectRecord {
                        workflow_id: input.workflow_id,
                        run_id: input.run_id,
                        effect,
                    })
                } else {
                    None
                };
                Ok((output, operation))
            })
            .await
    }

    async fn heartbeat_effect(&self, input: HeartbeatEffectInput) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.heartbeat_effect(input.clone()).await?;
                Ok(((), Some(JournalOperation::HeartbeatEffect(input))))
            })
            .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.complete_effect(input.clone()).await?;
                Ok(((), Some(JournalOperation::CompleteEffect(input))))
            })
            .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let output = inner.fail_effect(input.clone()).await?;
                Ok((output, Some(JournalOperation::FailEffect(input))))
            })
            .await
    }

    async fn commit_checkpoint(
        &self,
        input: CommitCheckpointInput,
    ) -> Result<CommitCheckpointResult, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let output = inner.commit_checkpoint(input.clone()).await?;
                let operation = if output.ok {
                    Some(JournalOperation::CommitCheckpoint(input))
                } else {
                    None
                };
                Ok((output, operation))
            })
            .await
    }

    async fn commit_activations(
        &self,
        inputs: Vec<CommitCheckpointInput>,
    ) -> Result<CommitActivationsResult, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                let output = inner.commit_activations(inputs.clone()).await?;
                let operation =
                    if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
                        Some(JournalOperation::CommitActivations(inputs))
                    } else {
                        None
                    };
                Ok((output, operation))
            })
            .await
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.record_activation_failures(inputs.clone()).await?;
                Ok(((), Some(JournalOperation::RecordActivationFailures(inputs))))
            })
            .await
    }

    async fn release_activation(
        &self,
        activation_id: &str,
        worker_id: &str,
    ) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        let activation_id = activation_id.to_string();
        let worker_id = worker_id.to_string();
        self.provider
            .write_caught_up(|| async {
                let should_append = activation_release_would_mutate(
                    &self.provider.inner,
                    &activation_id,
                    &worker_id,
                )
                .await?;
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.release_activation(&activation_id, &worker_id).await?;
                let operation = if should_append {
                    Some(JournalOperation::ReleaseActivation {
                        activation_id,
                        worker_id,
                    })
                } else {
                    None
                };
                Ok(((), operation))
            })
            .await
    }

    async fn heartbeat(&self, now: DateTime<Utc>, lease_ms: u64) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let inner = <ShardRouter as DurabilityProvider>::open_shard(
                    &self.provider.inner,
                    session.clone(),
                );
                inner.heartbeat(now, lease_ms).await?;
                let operation =
                    session
                        .owner_id
                        .clone()
                        .map(|owner_id| JournalOperation::HeartbeatShard {
                            shard_id: session.shard_id,
                            owner_id,
                            now,
                            lease_ms,
                        });
                Ok(((), operation))
            })
            .await
    }

    async fn release(&self) -> Result<(), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_caught_up(|| async {
                let operation = if let Some(owner_id) = session.owner_id.clone() {
                    let should_append = shard_release_would_mutate(
                        &self.provider.inner,
                        session.shard_id,
                        &owner_id,
                    )
                    .await?;
                    let inner = <ShardRouter as DurabilityProvider>::open_shard(
                        &self.provider.inner,
                        session.clone(),
                    );
                    inner.release().await?;
                    if should_append {
                        Some(JournalOperation::ReleaseShard {
                            shard_id: session.shard_id,
                            owner_id,
                        })
                    } else {
                        None
                    }
                } else {
                    let inner = <ShardRouter as DurabilityProvider>::open_shard(
                        &self.provider.inner,
                        session.clone(),
                    );
                    inner.release().await?;
                    None
                };
                Ok(((), operation))
            })
            .await
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
    ) -> Result<StartWorkflowResult, WorkflowError> {
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

    async fn get_workflow_runs(
        &self,
        input: GetWorkflowRunsInput,
    ) -> Result<GetWorkflowRunsResult, WorkflowError> {
        self.provider_for_ref(&input.id, "")?
            .get_workflow_runs(input)
            .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        self.provider_for_ref(&input.workflow_id, &input.run_id)?
            .append_signal(input)
            .await
    }

    async fn start_send_signal(
        &self,
        input: StartSendSignalInput,
    ) -> Result<StartSendSignalResult, WorkflowError> {
        self.provider_for_shard(input.partition_shard)?
            .start_send_signal(input)
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

    async fn start_send_signal(
        &self,
        _input: StartSendSignalInput,
    ) -> Result<StartSendSignalResult, WorkflowError> {
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
