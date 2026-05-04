import { mkdirSync } from "node:fs"
import { randomUUID } from "node:crypto"
import { createRequire } from "node:module"
import { dirname } from "node:path"
import { performance } from "node:perf_hooks"
import type {
  AppendSignalInput,
  CancelChildInput,
  ClaimDispatchShardInput,
  ClaimReadyActivationInput,
  ClaimReadyActivationResult,
  ClaimReadyActivationsInput,
  ClaimReadyActivationsResult,
  ClaimShardTasksInput,
  ClaimShardTasksResult,
  CommitActivationInput,
  CommitActivationsResult,
  CommitCheckpointInput,
  CommitCheckpointResult,
  CompleteEffectInput,
  CreateChildInstanceInput,
  CreateInstanceInput,
  DispatchShardLease,
  DurabilityProvider,
  EffectReservation,
  FailEffectInput,
  FailEffectResult,
  GetWorkflowRunsInput,
  GetWorkflowRunsResult,
  HeartbeatActivationInput,
  HeartbeatActivationsInput,
  HeartbeatDispatchShardInput,
  HeartbeatEffectInput,
  LoadInstanceOptions,
  OpenShardInput,
  PersistedInstance,
  RecordActivationFailureInput,
  ReleaseActivationInput,
  ReleaseActivationsInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
  ShardDurabilitySession,
  ShardLease,
  SignalRecord,
  ChildRecord,
} from "./interface.js"
import type { ChildHandle, InstanceRef } from "./workflow.js"
import type { StartWorkflowResult } from "./workflow.js"
import type { DurableLogFields, DurableMetricTags, DurableObservability } from "./observability.js"
import { countDurable, logDurable } from "./observability.js"
import {
  applyJournalOperation,
  applyJournalOperationSync,
  operationTime,
  type JournalOperation,
} from "./shard-journal.js"
import {
  ShardMemoryDurabilityProvider,
  type ShardMemorySnapshot,
} from "./shard-engine.js"

const require = createRequire(import.meta.url)

type SqliteValue = string | number | bigint | null | Buffer | Uint8Array
type SqliteRunResult = { changes: number; lastInsertRowid: number | bigint }
type SqliteStatement = {
  run(...params: SqliteValue[]): SqliteRunResult
  get(...params: SqliteValue[]): unknown
  all(...params: SqliteValue[]): unknown[]
}
type SqliteDatabase = {
  exec(sql: string): unknown
  prepare(sql: string): SqliteStatement
  pragma(sql: string, options?: { simple?: boolean }): unknown
  close(): unknown
}
type SqliteConstructor = {
  new (filename?: string, options?: { timeout?: number; fileMustExist?: boolean }): SqliteDatabase
}

const Database = require("better-sqlite3") as SqliteConstructor

export type SqliteDurabilityProviderOptions = DurableObservability & {
  busyTimeoutMs?: number
  snapshotInterval?: number
  sqlProfiler?: (event: {
    sql: string
    method: "run" | "get" | "all"
    durationMs: number
  }) => void
}

type JournalRow = {
  entry_id: number
  operation_json: string
}

type SnapshotRow = {
  last_entry_id: number
  snapshot_json: string
}

type SharedSqliteEngineStore = {
  engine: ShardMemoryDurabilityProvider
  appliedEntryId: number
  refCount: number
}

export class SqliteDurabilityProvider implements DurabilityProvider {
  private static readonly writeLocks = new Map<string, Promise<void>>()
  private static readonly engineStores = new Map<string, SharedSqliteEngineStore>()

  private readonly db: SqliteDatabase
  private readonly storeKey: string
  private readonly store: SharedSqliteEngineStore
  private readonly statements = new Map<string, SqliteStatement>()
  private readonly busyTimeoutMs: number
  private readonly snapshotInterval: number
  private readonly sqlProfiler?: SqliteDurabilityProviderOptions["sqlProfiler"]
  private readonly observability: DurableObservability
  private closed = false

  constructor(
    private readonly filePath: string,
    options: SqliteDurabilityProviderOptions = {},
  ) {
    rejectSqliteSynchronousOption(options)
    this.busyTimeoutMs = options.busyTimeoutMs ?? 5_000
    this.snapshotInterval = options.snapshotInterval ?? 512
    this.sqlProfiler = options.sqlProfiler
    this.observability = options
    if (filePath !== ":memory:") {
      mkdirSync(dirname(filePath), { recursive: true })
    }
    this.storeKey = filePath === ":memory:" ? `:memory:${randomUUID()}` : filePath
    const existingStore = SqliteDurabilityProvider.engineStores.get(this.storeKey)
    this.store = existingStore ?? {
      engine: new ShardMemoryDurabilityProvider(),
      appliedEntryId: 0,
      refCount: 0,
    }
    this.store.refCount += 1
    SqliteDurabilityProvider.engineStores.set(this.storeKey, this.store)
    this.db = new Database(filePath, { timeout: this.busyTimeoutMs })
    this.configure()
    this.migrate()
    if (!existingStore) {
      this.reloadFromDurableStore()
    }
  }

  close(): void {
    if (this.closed) {
      return
    }
    this.db.close()
    this.statements.clear()
    this.store.refCount -= 1
    if (this.store.refCount <= 0) {
      this.store.engine.close()
      SqliteDurabilityProvider.engineStores.delete(this.storeKey)
    }
    this.closed = true
  }

  async claimShard(input: ClaimDispatchShardInput): Promise<ShardLease | null> {
    await this.catchUp()
    await SqliteDurabilityProvider.withWriteLock(this.filePath, async () => {
      await this.beginImmediate()
      try {
        const leaseUntil = addMs(input.now, input.leaseMs)
        const result = this.prepare(
          `
          INSERT INTO dispatch_shards (shard_id, owner_id, lease_until, lease_epoch)
          VALUES (?, ?, ?, 1)
          ON CONFLICT(shard_id) DO UPDATE SET
            owner_id = excluded.owner_id,
            lease_until = excluded.lease_until,
            lease_epoch = CASE
              WHEN dispatch_shards.owner_id = excluded.owner_id
                AND dispatch_shards.lease_until IS NOT NULL
                AND dispatch_shards.lease_until > ?
              THEN dispatch_shards.lease_epoch
              ELSE dispatch_shards.lease_epoch + 1
            END
          WHERE dispatch_shards.owner_id IS NULL
            OR dispatch_shards.owner_id = excluded.owner_id
            OR dispatch_shards.lease_until IS NULL
            OR dispatch_shards.lease_until <= ?
        `,
        ).run(input.shardId, input.ownerId, leaseUntil, input.now, input.now)
        this.db.exec("COMMIT")
        if (result.changes === 0) {
          return
        }
      } catch (error) {
        this.db.exec("ROLLBACK")
        throw error
      }
    })
    const row = oneRow<{ lease_epoch: number; owner_id: string | null; lease_until: string | null }>(
      this.prepare("SELECT lease_epoch, owner_id, lease_until FROM dispatch_shards WHERE shard_id = ?").get(input.shardId),
    )
    if (!row || row.owner_id !== input.ownerId) {
      return null
    }
    const lease = {
      shardId: input.shardId,
      ownerId: input.ownerId,
      leaseUntil: row.lease_until ?? addMs(input.now, input.leaseMs),
      leaseEpoch: row.lease_epoch,
    }
    this.store.engine.setShardLease(lease)
    this.providerLog("debug", "provider.shard.claim", {
      workerId: input.ownerId,
      shardId: input.shardId,
      status: "success",
    })
    this.providerCount("durable.provider.shard", {
      shardId: input.shardId,
      status: "claimed",
    })
    return lease
  }

  openShard(input: OpenShardInput): ShardDurabilitySession {
    return new SqliteAppendShardSession(this, input)
  }

  async createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult> {
    return this.mutate({ op: "createInstance", input }, () => this.store.engine.createInstance(input))
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.mutate({ op: "createChildInstance", input }, () => this.store.engine.createChildInstance(input))
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    await this.mutate({ op: "cancelChild", input }, () => this.store.engine.cancelChild(input))
    this.providerLog("debug", "provider.child.cancel", {
      workerId: input.workerId,
      status: "success",
    })
    this.providerCount("durable.provider.child", {
      workerId: input.workerId,
      status: "canceled",
    })
  }

  async loadInstance(ref: InstanceRef, options: LoadInstanceOptions = {}): Promise<PersistedInstance | null> {
    await this.catchUp()
    return this.store.engine.loadInstance(ref, options)
  }

  async getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult> {
    await this.catchUp()
    return this.store.engine.getWorkflowRuns(input)
  }

  async listInstances(options: LoadInstanceOptions = {}): Promise<PersistedInstance[]> {
    await this.catchUp()
    const instances = this.store.engine.listInstances()
    if (!options.includeEffects) {
      return instances
    }
    return Promise.all(
      instances.map(async (instance) =>
        (await this.store.engine.loadInstance(instance, options)) ?? instance,
      ),
    )
  }

  async listSignals(): Promise<SignalRecord[]> {
    await this.catchUp()
    return this.store.engine.listSignals()
  }

  async listChildren(): Promise<ChildRecord[]> {
    await this.catchUp()
    return this.store.engine.listChildren()
  }

  async listActivationClaims(): Promise<
    Array<{
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      kind: "migration" | "run" | "event"
      ownerId?: string
      completedBySequence?: number
    }>
  > {
    await this.catchUp()
    return this.store.engine.listActivationClaims()
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.mutate({ op: "appendSignal", input }, () => this.store.engine.appendSignal(input))
  }

  async claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    return this.claimShard(input)
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    let leaseEpoch = 0
    await SqliteDurabilityProvider.withWriteLock(this.filePath, async () => {
      await this.beginImmediate()
      try {
        const result = this.prepare(
          `
          UPDATE dispatch_shards
          SET lease_until = ?
          WHERE shard_id = ? AND owner_id = ? AND lease_until >= ?
        `,
        ).run(addMs(input.now, input.leaseMs), input.shardId, input.ownerId, input.now)
        if (result.changes === 0) {
          throw new Error(`Lost dispatch shard lease: ${input.shardId}`)
        }
        const row = oneRow<{ lease_epoch: number }>(
          this.prepare("SELECT lease_epoch FROM dispatch_shards WHERE shard_id = ?").get(input.shardId),
        )
        leaseEpoch = row?.lease_epoch ?? 0
        this.db.exec("COMMIT")
      } catch (error) {
        this.db.exec("ROLLBACK")
        throw error
      }
    })
    this.store.engine.setShardLease({
      shardId: input.shardId,
      ownerId: input.ownerId,
      leaseUntil: addMs(input.now, input.leaseMs),
      leaseEpoch,
    })
  }

  async releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    await SqliteDurabilityProvider.withWriteLock(this.filePath, async () => {
      await this.beginImmediate()
      try {
        this.prepare(
          "UPDATE dispatch_shards SET owner_id = NULL, lease_until = NULL WHERE shard_id = ? AND owner_id = ?",
        ).run(input.shardId, input.ownerId)
        this.db.exec("COMMIT")
      } catch (error) {
        this.db.exec("ROLLBACK")
        throw error
      }
    })
    await this.store.engine.releaseDispatchShard(input)
  }

  async claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult> {
    await this.catchUp()
    const result = await this.store.engine.claimReadyActivations(input)
    if (result.claims.length > 0) {
      this.providerLog("debug", "provider.activation.claim", {
        workerId: input.workerId,
        count: result.claims.length,
        status: "success",
      })
      this.providerCount("durable.provider.activation", {
        workerId: input.workerId,
        status: "claimed",
      })
    }
    return result
  }

  async claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult> {
    const result = await this.claimReadyActivations({ ...input, limit: 1 })
    const first = result.claims[0]
    if (!first) {
      return result.nextWakeAt ? { activation: null, nextWakeAt: result.nextWakeAt } : { activation: null }
    }
    return {
      activation: first.activation,
      instance: first.instance,
      effects: first.effects,
      lease: first.lease,
    }
  }

  async claimShardTasks(
    sessionInput: OpenShardInput,
    input: ClaimShardTasksInput,
  ): Promise<ClaimShardTasksResult> {
    await this.catchUp()
    try {
      const result = await this.store.engine.openShard(sessionInput).claimTasks(input)
      if (result.claims.length > 0) {
        this.providerLog("debug", "provider.activation.claim", {
          workerId: sessionInput.ownerId,
          count: result.claims.length,
          status: "success",
        })
        this.providerCount("durable.provider.activation", {
          workerId: sessionInput.ownerId,
          status: "claimed",
        })
      }
      return result
    } catch (error) {
      if (error instanceof Error && /Lost shard lease/.test(error.message)) {
        return { claims: [] }
      }
      throw error
    }
  }

  async heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    await this.catchUp()
    try {
      await this.store.engine.heartbeatActivations(input)
    } catch (error) {
      if (error instanceof Error && /Lost activation lease/.test(error.message)) {
        this.providerLog("debug", "provider.effect.timeout_retry", {
          workerId: input.workerId,
          status: "expired",
        })
        this.providerCount("durable.provider.effect", {
          workerId: input.workerId,
          status: "timeout_retry",
        })
      }
      throw error
    }
  }

  async heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    await this.heartbeatActivations({
      activationIds: [input.activationId],
      workerId: input.workerId,
      now: input.now,
      leaseMs: input.leaseMs,
    })
  }

  async releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    await this.catchUp()
    await this.store.engine.releaseActivations(input)
  }

  async releaseActivation(input: ReleaseActivationInput): Promise<void> {
    await this.releaseActivations({ activationIds: [input.activationId], workerId: input.workerId })
  }

  async getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    const result = await this.mutate(
      { op: "getOrReserveEffect", input },
      () => this.store.engine.getOrReserveEffect(input),
    )
    this.providerLog("debug", "provider.effect.reserve", {
      workerId: input.workerId,
      status: result.status,
    })
    this.providerCount("durable.provider.effect", {
      workerId: input.workerId,
      status: result.status === "reserved" ? "reserved" : "memoized",
    })
    return result
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    await this.mutate({ op: "heartbeatEffect", input }, () => this.store.engine.heartbeatEffect(input))
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    await this.mutate({ op: "completeEffect", input }, () => this.store.engine.completeEffect(input))
    this.providerLog("debug", "provider.effect.complete", {
      workerId: input.workerId,
      status: "success",
    })
    this.providerCount("durable.provider.effect", {
      workerId: input.workerId,
      status: "completed",
    })
  }

  async failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.mutate({ op: "failEffect", input }, () => this.store.engine.failEffect(input))
  }

  async commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult> {
    if (input.length === 0) {
      return { results: [] }
    }
    const result = await this.mutate({ op: "commitActivations", input }, () => this.store.engine.commitActivations(input), {
      shouldAppend: (result) => result.results.some((entry) => entry.ok),
    })
    for (const entry of result.results) {
      const commitInput = input.find((item) => item.activationId === entry.activationId)
      this.providerLog(entry.ok ? "debug" : "warn", entry.ok ? "provider.checkpoint.commit" : "provider.checkpoint.conflict", {
        workerId: commitInput?.workerId,
        status: entry.ok ? "success" : "conflict",
        reason: entry.reason,
      })
      this.providerCount("durable.provider.checkpoint", {
        status: entry.ok ? "committed" : "conflict",
        reason: entry.reason,
      })
      if (
        entry.ok &&
        commitInput &&
        (commitInput.next.status === "failed" || commitInput.next.status === "canceled")
      ) {
        this.providerLog("debug", "provider.child.parent_close_abandon", {
          workerId: commitInput.workerId,
          status: "checked",
        })
      }
    }
    return result
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const result = await this.commitActivations([input])
    const first = result.results[0] ?? { ok: false, sequence: -1, reason: "missing_commit_result" }
    const { activationId: _activationId, ...checkpoint } = first
    return checkpoint
  }

  async recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void> {
    if (input.length === 0) {
      return
    }
    await this.mutate(
      { op: "recordActivationFailures", input },
      () => this.store.engine.recordActivationFailures(input),
    )
  }

  private async mutate<T>(
    operation: JournalOperation,
    apply: () => Promise<T>,
    options: { shouldAppend?: (result: T) => boolean } = {},
  ): Promise<T> {
    return SqliteDurabilityProvider.withWriteLock(
      this.filePath,
      () => this.mutateLocked(operation, apply, options),
    )
  }

  private async mutateLocked<T>(
    operation: JournalOperation,
    apply: () => Promise<T>,
    options: { shouldAppend?: (result: T) => boolean } = {},
  ): Promise<T> {
    this.assertOpen()
    await this.beginImmediate()
    try {
      await this.catchUpInTransaction()
      const result = await apply()
      if (options.shouldAppend?.(result) ?? true) {
        this.appendJournal(operation)
      }
      this.db.exec("COMMIT")
      return result
    } catch (error) {
      this.db.exec("ROLLBACK")
      throw error
    }
  }

  private async catchUp(): Promise<void> {
    this.assertOpen()
    await this.catchUpInTransaction()
  }

  private async catchUpInTransaction(): Promise<void> {
    const rows = this.prepare(
      "SELECT entry_id, operation_json FROM shard_journal WHERE entry_id > ? ORDER BY entry_id",
    ).all(this.store.appliedEntryId)
    for (const row of rows) {
      const journal = requireRow<JournalRow>(row)
      await this.applyJournalOperation(decodeJson<JournalOperation>(journal.operation_json))
      this.store.appliedEntryId = journal.entry_id
    }
  }

  private appendJournal(operation: JournalOperation): void {
    const result = this.prepare(
      "INSERT INTO shard_journal (operation_json, created_at) VALUES (?, ?)",
    ).run(encodeJson(operation), operationTime(operation))
    this.store.appliedEntryId = Number(result.lastInsertRowid)
    if (
      this.snapshotInterval > 0 &&
      this.store.appliedEntryId > 0 &&
      this.store.appliedEntryId % this.snapshotInterval === 0
    ) {
      this.writeSnapshot()
    }
  }

  private writeSnapshot(): void {
    this.prepare(
      `
      INSERT INTO shard_snapshots (snapshot_id, last_entry_id, snapshot_json, created_at)
      VALUES (1, ?, ?, ?)
      ON CONFLICT(snapshot_id) DO UPDATE SET
        last_entry_id = excluded.last_entry_id,
        snapshot_json = excluded.snapshot_json,
        created_at = excluded.created_at
    `,
    ).run(this.store.appliedEntryId, encodeJson(this.store.engine.snapshot()), new Date().toISOString())
  }

  private reloadFromDurableStore(): void {
    this.store.engine.close()
    this.store.appliedEntryId = 0
    const snapshot = oneRow<SnapshotRow>(
      this.prepare("SELECT last_entry_id, snapshot_json FROM shard_snapshots WHERE snapshot_id = 1").get(),
    )
    if (snapshot) {
      this.store.engine.restore(decodeJson<ShardMemorySnapshot>(snapshot.snapshot_json))
      this.store.appliedEntryId = snapshot.last_entry_id
    }
    const rows = this.prepare(
      "SELECT entry_id, operation_json FROM shard_journal WHERE entry_id > ? ORDER BY entry_id",
    ).all(this.store.appliedEntryId)
    for (const row of rows) {
      const journal = requireRow<JournalRow>(row)
      this.applyJournalOperationSync(decodeJson<JournalOperation>(journal.operation_json))
      this.store.appliedEntryId = journal.entry_id
    }
  }

  private async applyJournalOperation(operation: JournalOperation): Promise<void> {
    await applyJournalOperation(this.store.engine, operation)
  }

  private applyJournalOperationSync(operation: JournalOperation): void {
    applyJournalOperationSync(this.store.engine, operation)
  }

  private providerLog(
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: DurableLogFields,
  ): void {
    logDurable(this.observability, level, event, fields)
  }

  private providerCount(name: string, tags?: DurableMetricTags): void {
    countDurable(this.observability, name, 1, tags)
  }

  private configure(): void {
    this.db.pragma(`busy_timeout = ${this.busyTimeoutMs}`)
    this.db.pragma("journal_mode = WAL")
    this.db.pragma("synchronous = FULL")
    this.db.pragma("foreign_keys = ON")
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS provider_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

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

      CREATE TABLE IF NOT EXISTS dispatch_shards (
        shard_id INTEGER PRIMARY KEY,
        owner_id TEXT,
        lease_until TEXT,
        lease_epoch INTEGER NOT NULL DEFAULT 0
      );

      CREATE INDEX IF NOT EXISTS shard_journal_entry
        ON shard_journal(entry_id);
    `)
    this.prepare(
      `
      INSERT INTO provider_metadata (key, value)
      VALUES ('sqlite_storage_shape', 'append_journal_v1')
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `,
    ).run()
  }

  private prepare(sql: string): SqliteStatement {
    const cached = this.statements.get(sql)
    if (cached) {
      return cached
    }
    const raw = this.db.prepare(sql)
    const profiler = this.sqlProfiler
    if (!profiler) {
      this.statements.set(sql, raw)
      return raw
    }
    const profiled: SqliteStatement = {
      run: (...params) => profileSql(profiler, sql, "run", () => raw.run(...params)),
      get: (...params) => profileSql(profiler, sql, "get", () => raw.get(...params)),
      all: (...params) => profileSql(profiler, sql, "all", () => raw.all(...params)),
    }
    this.statements.set(sql, profiled)
    return profiled
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error("SQLite durability provider is closed")
    }
  }

  private async beginImmediate(): Promise<void> {
    const deadline = performance.now() + this.busyTimeoutMs
    for (;;) {
      try {
        this.db.exec("BEGIN IMMEDIATE")
        return
      } catch (error) {
        if (!isDatabaseLocked(error) || performance.now() >= deadline) {
          throw error
        }
        await sleep(1)
      }
    }
  }

  private static async withWriteLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const previous = SqliteDurabilityProvider.writeLocks.get(key) ?? Promise.resolve()
    let release!: () => void
    const current = new Promise<void>((resolve) => {
      release = resolve
    })
    const chained = previous.then(() => current, () => current)
    SqliteDurabilityProvider.writeLocks.set(key, chained)
    await previous.catch(() => undefined)
    try {
      return await fn()
    } finally {
      release()
      if (SqliteDurabilityProvider.writeLocks.get(key) === chained) {
        SqliteDurabilityProvider.writeLocks.delete(key)
      }
    }
  }
}

class SqliteAppendShardSession implements ShardDurabilitySession {
  readonly shardId: number
  readonly ownerId?: string
  readonly leaseEpoch?: number

  constructor(
    private readonly provider: SqliteDurabilityProvider,
    private readonly input: OpenShardInput,
  ) {
    this.shardId = input.shardId
    this.ownerId = input.ownerId
    this.leaseEpoch = input.leaseEpoch
  }

  createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult> {
    if (input.partitionShard !== this.shardId) {
      throw new Error(`Shard session ${this.shardId} cannot write shard ${input.partitionShard}`)
    }
    return this.provider.createInstance(input)
  }

  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.provider.createChildInstance(input)
  }

  cancelChild(input: CancelChildInput): Promise<void> {
    return this.provider.cancelChild(input)
  }

  readInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null> {
    return this.provider.loadInstance(ref, options)
  }

  getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult> {
    return this.provider.getWorkflowRuns(input)
  }

  appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.provider.appendSignal(input)
  }

  claimTasks(input: ClaimShardTasksInput): Promise<ClaimShardTasksResult> {
    if (!this.ownerId) {
      throw new Error(`Shard ${this.shardId} is not opened with an owner`)
    }
    return this.provider.claimShardTasks(this.input, input)
  }

  heartbeat(input: { now: string; leaseMs: number }): Promise<void> {
    if (!this.ownerId) {
      return Promise.resolve()
    }
    return this.provider.heartbeatDispatchShard({
      shardId: this.shardId,
      ownerId: this.ownerId,
      now: input.now,
      leaseMs: input.leaseMs,
    })
  }

  release(): Promise<void> {
    if (!this.ownerId) {
      return Promise.resolve()
    }
    return this.provider.releaseDispatchShard({
      shardId: this.shardId,
      ownerId: this.ownerId,
    })
  }

  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    return this.provider.heartbeatActivations(input)
  }

  heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    return this.provider.heartbeatActivation(input)
  }

  releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    return this.provider.releaseActivations(input)
  }

  releaseActivation(input: ReleaseActivationInput): Promise<void> {
    return this.provider.releaseActivation(input)
  }

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.provider.getOrReserveEffect(input)
  }

  heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    return this.provider.heartbeatEffect(input)
  }

  completeEffect(input: CompleteEffectInput): Promise<void> {
    return this.provider.completeEffect(input)
  }

  failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.provider.failEffect(input)
  }

  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult> {
    return this.provider.commitActivations(input)
  }

  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    return this.provider.commitCheckpoint(input)
  }

  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void> {
    return this.provider.recordActivationFailures(input)
  }
}

function rejectSqliteSynchronousOption(options: SqliteDurabilityProviderOptions & { synchronous?: unknown }): void {
  if (Object.prototype.hasOwnProperty.call(options, "synchronous")) {
    throw new Error("SqliteDurabilityProvider uses fixed SQLite synchronous=FULL")
  }
}

function profileSql<T>(
  profiler: NonNullable<SqliteDurabilityProviderOptions["sqlProfiler"]>,
  sql: string,
  method: "run" | "get" | "all",
  fn: () => T,
): T {
  const startedAt = performance.now()
  try {
    return fn()
  } finally {
    profiler({ sql, method, durationMs: performance.now() - startedAt })
  }
}

function encodeJson(value: unknown): string {
  return JSON.stringify(value)
}

function decodeJson<T>(value: string): T {
  return JSON.parse(value) as T
}

function oneRow<T>(value: unknown): T | null {
  return value === undefined ? null : requireRow<T>(value)
}

function requireRow<T>(value: unknown): T {
  if (!value || typeof value !== "object") {
    throw new Error("Expected SQLite row")
  }
  return value as T
}

function addMs(isoValue: string, ms: number): string {
  return new Date(new Date(isoValue).getTime() + ms).toISOString()
}

function isDatabaseLocked(error: unknown): boolean {
  return error instanceof Error && /database is locked/i.test(error.message)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
