use super::*;

const POSTGRES_GLOBAL_WRITE_LOCK_KEY: &str = "global_write_lock";

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
    operation_lock: Arc<AsyncMutex<()>>,
    applied_entries: Arc<Mutex<HashMap<u32, i64>>>,
}

struct PostgresClientPool {
    clients: Vec<Arc<AsyncMutex<tokio_postgres::Client>>>,
    next: AtomicUsize,
}

struct LoadedPostgresStore {
    store: Store,
    applied_entries: HashMap<u32, i64>,
}

#[derive(Serialize, Deserialize)]
struct PostgresSnapshotEnvelope {
    store: Store,
    applied_entries: HashMap<u32, i64>,
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
            clients.push(Arc::new(AsyncMutex::new(client)));
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn next(&self) -> Arc<AsyncMutex<tokio_postgres::Client>> {
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
            operation_lock: Arc::new(AsyncMutex::new(())),
            applied_entries: Arc::new(Mutex::new(HashMap::new())),
        };
        provider.initialize_postgres_schema().await?;
        let loaded = provider.load_postgres_store().await?;
        *provider
            .applied_entries
            .lock()
            .map_err(|_| WorkflowError::new("postgres applied entry lock poisoned"))? =
            loaded.applied_entries.clone();
        Ok(Self {
            inner: ShardRouter::from_store(loaded.store),
            ..provider
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    async fn initialize_postgres_schema(&self) -> Result<(), WorkflowError> {
        let client_handle = self.pool.next();
        let client = client_handle.lock().await;
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
        client
            .batch_execute(&format!(
                "CREATE SEQUENCE IF NOT EXISTS {};",
                self.postgres_global_journal_sequence()
            ))
            .await?;
        client
            .execute(
                &format!(
                    "INSERT INTO {}.provider_metadata (key, value)
                     VALUES ($1, $2)
                     ON CONFLICT (key) DO NOTHING",
                    quote_postgres_identifier(&self.schema)
                ),
                &[&POSTGRES_GLOBAL_WRITE_LOCK_KEY, &"locked"],
            )
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
                        global_entry_id BIGINT NOT NULL DEFAULT nextval('{}'::regclass),
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
                    self.postgres_global_journal_sequence(),
                    self.postgres_table("shard_snapshots", partition),
                ))
                .await?;
        }
        self.ensure_postgres_journal_global_entry_ids(&client)
            .await?;
        Ok(())
    }

    async fn ensure_postgres_journal_global_entry_ids(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<(), WorkflowError> {
        let sequence = self.postgres_global_journal_sequence();
        let default_expr = format!("nextval('{sequence}'::regclass)");
        let mut missing = Vec::new();
        for partition in 0..self.physical_partitions {
            let table = self.postgres_table("shard_journal", partition);
            client
                .batch_execute(&format!(
                    "
                    ALTER TABLE {table}
                      ADD COLUMN IF NOT EXISTS global_entry_id BIGINT;
                    ALTER TABLE {table}
                      ALTER COLUMN global_entry_id SET DEFAULT {default_expr};
                    "
                ))
                .await?;
            for row in client
                .query(
                    &format!(
                        "SELECT shard_id, entry_id, created_at
                         FROM {table}
                         WHERE global_entry_id IS NULL"
                    ),
                    &[],
                )
                .await?
            {
                missing.push((
                    row.get::<_, DateTime<Utc>>(2),
                    table.clone(),
                    row.get::<_, i32>(0),
                    row.get::<_, i64>(1),
                ));
            }
        }
        missing.sort_by(|left, right| (left.0, left.2, left.3).cmp(&(right.0, right.2, right.3)));
        for (_created_at, table, shard_id, entry_id) in missing {
            client
                .execute(
                    &format!(
                        "UPDATE {table}
                         SET global_entry_id = {default_expr}
                         WHERE shard_id = $1
                           AND entry_id = $2
                           AND global_entry_id IS NULL"
                    ),
                    &[&shard_id, &entry_id],
                )
                .await?;
        }
        for partition in 0..self.physical_partitions {
            let table = self.postgres_table("shard_journal", partition);
            client
                .batch_execute(&format!(
                    "ALTER TABLE {table} ALTER COLUMN global_entry_id SET NOT NULL;"
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

    async fn load_postgres_store(&self) -> Result<LoadedPostgresStore, WorkflowError> {
        let client_handle = self.pool.next();
        let client = client_handle.lock().await;
        self.load_postgres_store_with_client(&client).await
    }

    async fn load_postgres_store_with_client(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<LoadedPostgresStore, WorkflowError> {
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
        let (store, mut applied_entries) = if let Some((_created_at, raw)) = latest_snapshot {
            match serde_json::from_str::<PostgresSnapshotEnvelope>(&raw) {
                Ok(envelope) => (envelope.store, envelope.applied_entries),
                Err(_) => (Store::default(), HashMap::new()),
            }
        } else {
            (Store::default(), HashMap::new())
        };
        let mut provider = ShardEngine::from_store(store);
        let mut rows = Vec::new();
        for partition in 0..self.physical_partitions {
            let table = self.postgres_table("shard_journal", partition);
            let partition_rows = client
                .query(
                    &format!(
                        "SELECT shard_id, entry_id, operation_json, global_entry_id FROM {table}"
                    ),
                    &[],
                )
                .await?;
            for row in partition_rows {
                let shard_id = row.get::<_, i32>(0) as u32;
                let entry_id = row.get::<_, i64>(1);
                if entry_id <= *applied_entries.get(&shard_id).unwrap_or(&0) {
                    continue;
                }
                rows.push((
                    row.get::<_, i64>(3),
                    shard_id,
                    entry_id,
                    row.get::<_, String>(2),
                ));
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        for row in rows {
            let operation = serde_json::from_str::<JournalOperation>(&row.3)?;
            apply_journal_operation(&mut provider, operation)?;
            applied_entries
                .entry(row.1)
                .and_modify(|entry| *entry = (*entry).max(row.2))
                .or_insert(row.2);
        }
        Ok(LoadedPostgresStore {
            store: provider.snapshot_store(),
            applied_entries,
        })
    }

    fn set_applied_entry(&self, shard_id: u32, entry_id: i64) -> Result<(), WorkflowError> {
        self.applied_entries
            .lock()
            .map_err(|_| WorkflowError::new("postgres applied entry lock poisoned"))?
            .entry(shard_id)
            .and_modify(|current| *current = (*current).max(entry_id))
            .or_insert(entry_id);
        Ok(())
    }

    fn replace_applied_entries(
        &self,
        applied_entries: HashMap<u32, i64>,
    ) -> Result<(), WorkflowError> {
        *self
            .applied_entries
            .lock()
            .map_err(|_| WorkflowError::new("postgres applied entry lock poisoned"))? =
            applied_entries;
        Ok(())
    }

    async fn encode_postgres_snapshot(&self) -> Result<String, WorkflowError> {
        let store = self.inner.snapshot_store().await?;
        let applied_entries = self
            .applied_entries
            .lock()
            .map_err(|_| WorkflowError::new("postgres applied entry lock poisoned"))?
            .clone();
        Ok(serde_json::to_string(&PostgresSnapshotEnvelope {
            store,
            applied_entries,
        })?)
    }

    async fn catch_up_postgres_unlocked(&self) -> Result<(), WorkflowError> {
        let loaded = self.load_postgres_store().await?;
        self.inner.reset_from_store(loaded.store).await?;
        self.replace_applied_entries(loaded.applied_entries)
    }

    async fn read_postgres_caught_up<T, F, Fut>(&self, read: F) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, WorkflowError>>,
    {
        let _guard = self.operation_lock.lock().await;
        self.catch_up_postgres_unlocked().await?;
        read().await
    }

    async fn write_postgres_caught_up<T, F, Fut>(
        &self,
        target: JournalOperation,
        apply: F,
    ) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, Option<JournalOperation>), WorkflowError>>,
    {
        let _guard = self.operation_lock.lock().await;
        self.catch_up_postgres_unlocked().await?;
        let shard_id = self.postgres_shard_for_operation(&target).await?;
        self.write_postgres_caught_up_locked(shard_id, apply).await
    }

    async fn write_postgres_caught_up_on_shard<T, F, Fut>(
        &self,
        shard_id: u32,
        apply: F,
    ) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, Option<JournalOperation>), WorkflowError>>,
    {
        let _guard = self.operation_lock.lock().await;
        self.catch_up_postgres_unlocked().await?;
        self.write_postgres_caught_up_locked(shard_id, apply).await
    }

    async fn write_postgres_caught_up_locked<T, F, Fut>(
        &self,
        shard_id: u32,
        apply: F,
    ) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, Option<JournalOperation>), WorkflowError>>,
    {
        let client_handle = self.pool.next();
        let client = client_handle.lock().await;
        client.batch_execute("BEGIN").await?;
        let result = async {
            self.lock_postgres_global_write(&client).await?;
            self.lock_postgres_shard_head(&client, shard_id).await?;
            let loaded = self.load_postgres_store_with_client(&client).await?;
            self.inner.reset_from_store(loaded.store).await?;
            self.replace_applied_entries(loaded.applied_entries)?;
            let (output, operation) = apply().await?;
            if let Some(operation) = operation {
                self.append_postgres_operation_for_shard_in_transaction(
                    &client, shard_id, operation,
                )
                .await?;
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                if let Err(error) = client.batch_execute("COMMIT").await {
                    let _ = client.batch_execute("ROLLBACK").await;
                    return Err(error.into());
                }
                Ok(output)
            }
            Err(error) => {
                let _ = client.batch_execute("ROLLBACK").await;
                Err(error)
            }
        }
    }

    async fn lock_postgres_global_write(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<(), WorkflowError> {
        let table = format!(
            "{}.provider_metadata",
            quote_postgres_identifier(&self.schema)
        );
        client
            .execute(
                &format!(
                    "INSERT INTO {table} (key, value)
                     VALUES ($1, $2)
                     ON CONFLICT (key) DO NOTHING"
                ),
                &[&POSTGRES_GLOBAL_WRITE_LOCK_KEY, &"locked"],
            )
            .await?;
        client
            .query_one(
                &format!("SELECT value FROM {table} WHERE key = $1 FOR UPDATE"),
                &[&POSTGRES_GLOBAL_WRITE_LOCK_KEY],
            )
            .await?;
        Ok(())
    }

    async fn lock_postgres_shard_head(
        &self,
        client: &tokio_postgres::Client,
        shard_id: u32,
    ) -> Result<(), WorkflowError> {
        let shard_id_i32 = shard_id as i32;
        client
            .execute(
                &format!(
                    "INSERT INTO {} (shard_id, last_entry_id, updated_at)
                     VALUES ($1, 0, clock_timestamp())
                     ON CONFLICT (shard_id) DO NOTHING",
                    self.postgres_head_table_for_shard(shard_id)
                ),
                &[&shard_id_i32],
            )
            .await?;
        client
            .query_one(
                &format!(
                    "SELECT last_entry_id FROM {} WHERE shard_id = $1 FOR UPDATE",
                    self.postgres_head_table_for_shard(shard_id)
                ),
                &[&shard_id_i32],
            )
            .await?;
        Ok(())
    }

    async fn append_postgres_operation_for_shard_in_transaction(
        &self,
        client: &tokio_postgres::Client,
        shard_id: u32,
        operation: JournalOperation,
    ) -> Result<(), WorkflowError> {
        let operation_json = serde_json::to_string(&operation)?;
        let shard_id_i32 = shard_id as i32;
        let row = client
            .query_one(
                &format!(
                    "UPDATE {}
                     SET last_entry_id = last_entry_id + 1,
                         updated_at = clock_timestamp()
                     WHERE shard_id = $1
                     RETURNING last_entry_id",
                    self.postgres_head_table_for_shard(shard_id)
                ),
                &[&shard_id_i32],
            )
            .await?;
        let next_entry_id: i64 = row.get(0);
        client
            .execute(
                &format!(
                    "INSERT INTO {} (shard_id, entry_id, operation_json, created_at)
                     VALUES ($1, $2, $3, clock_timestamp())",
                    self.postgres_journal_table_for_shard(shard_id)
                ),
                &[&shard_id_i32, &next_entry_id, &operation_json],
            )
            .await?;
        self.set_applied_entry(shard_id, next_entry_id)?;
        if self.snapshot_interval > 0 && next_entry_id as u64 % self.snapshot_interval == 0 {
            let snapshot = self.encode_postgres_snapshot().await?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {} (shard_id, last_entry_id, snapshot_json, created_at)
                         VALUES ($1, $2, $3, clock_timestamp())
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

    fn postgres_global_journal_sequence(&self) -> String {
        format!(
            "{}.{}",
            quote_postgres_identifier(&self.schema),
            quote_postgres_identifier("journal_global_entry_id_seq")
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
        self.write_postgres_caught_up(JournalOperation::ClaimShard(input.clone()), || async {
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
        let target = JournalOperation::ClaimShardAndTasks {
            claim: claim.clone(),
            input: input.clone(),
        };
        self.write_postgres_caught_up(target, || async {
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
        Arc::new(PostgresShardSession {
            provider: self.clone(),
            input,
        })
    }

    async fn create_instance(
        &self,
        input: CreateInstanceInput,
    ) -> Result<StartWorkflowResult, WorkflowError> {
        self.write_postgres_caught_up(JournalOperation::CreateInstance(input.clone()), || async {
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
        self.write_postgres_caught_up(
            JournalOperation::CreateChildInstance(input.clone()),
            || async {
                let should_append = create_child_instance_would_mutate(&self.inner, &input).await?;
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
            },
        )
        .await
    }

    async fn load_instance(
        &self,
        ref_: &InstanceRef,
        options: LoadInstanceOptions,
    ) -> Result<Option<PersistedInstance>, WorkflowError> {
        self.read_postgres_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::load_instance(&self.inner, ref_, options).await
        })
        .await
    }

    async fn get_workflow_runs(
        &self,
        input: GetWorkflowRunsInput,
    ) -> Result<GetWorkflowRunsResult, WorkflowError> {
        self.read_postgres_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::get_workflow_runs(&self.inner, input).await
        })
        .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        self.write_postgres_caught_up(JournalOperation::AppendSignal(input.clone()), || async {
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

    async fn cancel_child(&self, input: CancelChildInput) -> Result<(), WorkflowError> {
        self.write_postgres_caught_up(JournalOperation::CancelChild(input.clone()), || async {
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
        self.write_postgres_caught_up(JournalOperation::ReserveEffect(input.clone()), || async {
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
        self.write_postgres_caught_up(JournalOperation::HeartbeatEffect(input.clone()), || async {
            <ShardRouter as DurabilityProvider>::heartbeat_effect(&self.inner, input.clone())
                .await?;
            Ok(((), Some(JournalOperation::HeartbeatEffect(input))))
        })
        .await
    }

    async fn complete_effect(&self, input: CompleteEffectInput) -> Result<(), WorkflowError> {
        self.write_postgres_caught_up(JournalOperation::CompleteEffect(input.clone()), || async {
            <ShardRouter as DurabilityProvider>::complete_effect(&self.inner, input.clone())
                .await?;
            Ok(((), Some(JournalOperation::CompleteEffect(input))))
        })
        .await
    }

    async fn fail_effect(&self, input: FailEffectInput) -> Result<FailEffectResult, WorkflowError> {
        self.write_postgres_caught_up(JournalOperation::FailEffect(input.clone()), || async {
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
        self.write_postgres_caught_up(JournalOperation::CommitCheckpoint(input.clone()), || async {
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
                    return Ok((
                        CommitCheckpointResult {
                            ok: false,
                            sequence: input.expected_sequence,
                            reason: Some("cross_shard_child_start".to_string()),
                            retryable: Some(false),
                            error: Some(SerializedError {
                                name: None,
                                message: "Postgres provider requires commit-local children to stay on the parent shard".to_string(),
                            }),
                        },
                        None,
                    ));
                }
            }
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
        if inputs.is_empty() {
            return Ok(CommitActivationsResult {
                results: Vec::new(),
            });
        }
        self.write_postgres_caught_up(
            JournalOperation::CommitActivations(inputs.clone()),
            || async {
                let output = <ShardRouter as DurabilityProvider>::commit_activations(
                    &self.inner,
                    inputs.clone(),
                )
                .await?;
                let operation =
                    if output.results.iter().all(|result| result.ok) && !inputs.is_empty() {
                        Some(JournalOperation::CommitActivations(inputs))
                    } else {
                        None
                    };
                Ok((output, operation))
            },
        )
        .await
    }

    async fn record_activation_failures(
        &self,
        inputs: Vec<RecordActivationFailureInput>,
    ) -> Result<(), WorkflowError> {
        if inputs.is_empty() {
            return Ok(());
        }
        self.write_postgres_caught_up(
            JournalOperation::RecordActivationFailures(inputs.clone()),
            || async {
                <ShardRouter as DurabilityProvider>::record_activation_failures(
                    &self.inner,
                    inputs.clone(),
                )
                .await?;
                Ok(((), Some(JournalOperation::RecordActivationFailures(inputs))))
            },
        )
        .await
    }

    async fn list_instances(&self) -> Result<Vec<PersistedInstance>, WorkflowError> {
        self.read_postgres_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_instances(&self.inner).await
        })
        .await
    }

    async fn list_signals(&self) -> Result<Vec<SignalRecord>, WorkflowError> {
        self.read_postgres_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_signals(&self.inner).await
        })
        .await
    }

    async fn list_children(&self) -> Result<Vec<ChildRecord>, WorkflowError> {
        self.read_postgres_caught_up(|| async {
            <ShardRouter as DurabilityProvider>::list_children(&self.inner).await
        })
        .await
    }

    async fn shutdown(&self) -> Result<(), WorkflowError> {
        let _guard = self.operation_lock.lock().await;
        self.inner.shutdown().await
    }
}

#[derive(Clone)]
struct PostgresShardSession {
    provider: PostgresDurabilityProvider,
    input: OpenShardInput,
}

#[async_trait]
impl ShardDurabilitySession for PostgresShardSession {
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
        let target = JournalOperation::ClaimShardTasks {
            session: session.clone(),
            input: input.clone(),
        };
        self.provider
            .write_postgres_caught_up(target, || async {
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
            .read_postgres_caught_up(|| async {
                let inner =
                    <ShardRouter as DurabilityProvider>::open_shard(&self.provider.inner, session);
                inner.read_instance(ref_, options).await
            })
            .await
    }

    async fn append_signal(&self, input: AppendSignalInput) -> Result<SignalRecord, WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_postgres_caught_up(JournalOperation::AppendSignal(input.clone()), || async {
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

    async fn append_signal_with_status(
        &self,
        input: AppendSignalInput,
    ) -> Result<(SignalRecord, bool), WorkflowError> {
        let session = self.input.clone();
        self.provider
            .write_postgres_caught_up(JournalOperation::AppendSignal(input.clone()), || async {
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
            .write_postgres_caught_up(JournalOperation::CancelChild(input.clone()), || async {
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
            .write_postgres_caught_up(JournalOperation::ReserveEffect(input.clone()), || async {
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
            .write_postgres_caught_up(JournalOperation::HeartbeatEffect(input.clone()), || async {
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
            .write_postgres_caught_up(JournalOperation::CompleteEffect(input.clone()), || async {
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
            .write_postgres_caught_up(JournalOperation::FailEffect(input.clone()), || async {
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
            .write_postgres_caught_up(
                JournalOperation::CommitCheckpoint(input.clone()),
                || async {
                    let inner = <ShardRouter as DurabilityProvider>::open_shard(
                        &self.provider.inner,
                        session,
                    );
                    let output = inner.commit_checkpoint(input.clone()).await?;
                    let operation = if output.ok {
                        Some(JournalOperation::CommitCheckpoint(input))
                    } else {
                        None
                    };
                    Ok((output, operation))
                },
            )
            .await
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
        let session = self.input.clone();
        self.provider
            .write_postgres_caught_up(
                JournalOperation::CommitActivations(inputs.clone()),
                || async {
                    let inner = <ShardRouter as DurabilityProvider>::open_shard(
                        &self.provider.inner,
                        session,
                    );
                    let output = inner.commit_activations(inputs.clone()).await?;
                    let operation = if output.results.iter().all(|result| result.ok) {
                        Some(JournalOperation::CommitActivations(inputs))
                    } else {
                        None
                    };
                    Ok((output, operation))
                },
            )
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
            .write_postgres_caught_up(
                JournalOperation::RecordActivationFailures(inputs.clone()),
                || async {
                    let inner = <ShardRouter as DurabilityProvider>::open_shard(
                        &self.provider.inner,
                        session,
                    );
                    inner.record_activation_failures(inputs.clone()).await?;
                    Ok(((), Some(JournalOperation::RecordActivationFailures(inputs))))
                },
            )
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
            .write_postgres_caught_up_on_shard(session.shard_id, || async {
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
        let target = JournalOperation::HeartbeatShard {
            shard_id: session.shard_id,
            owner_id: session.owner_id.clone().unwrap_or_default(),
            now,
            lease_ms,
        };
        self.provider
            .write_postgres_caught_up(target, || async {
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
        let target = JournalOperation::ReleaseShard {
            shard_id: session.shard_id,
            owner_id: session.owner_id.clone().unwrap_or_default(),
        };
        self.provider
            .write_postgres_caught_up(target, || async {
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
