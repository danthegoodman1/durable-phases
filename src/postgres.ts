import { randomUUID } from "node:crypto"
import pg from "pg"
import type {
  AppendSignalInput,
  CancelChildInput,
  ChildRecord,
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
} from "./interface.js"
import type { ChildHandle, InstanceRef } from "./workflow.js"
import type { DurableLogFields, DurableMetricTags, DurableObservability } from "./observability.js"
import { countDurable, logDurable } from "./observability.js"
import {
  applyJournalOperation,
  operationTime,
  type JournalOperation,
} from "./shard-journal.js"
import {
  ShardMemoryDurabilityProvider,
  type ShardMemorySnapshot,
} from "./shard-engine.js"

const { Pool } = pg
type Pool = pg.Pool
type PoolClient = pg.PoolClient
type Queryable = Pool | PoolClient

export type PostgresDurabilityProviderOptions = DurableObservability & {
  connectionString?: string
  pool?: Pool
  schema?: string
  physicalPartitions?: number
  messageBatchSize?: number
  poolSize?: number
  statementTimeoutMs?: number
  lockTimeoutMs?: number
  snapshotInterval?: number
}

type ShardProjection = {
  engine: ShardMemoryDurabilityProvider
  appliedEntryId: number
  loaded: boolean
}

type JournalRow = {
  entry_id: string | number
  operation_json: unknown
}

type SnapshotRow = {
  last_entry_id: string | number
  snapshot_json: unknown
}

type DispatchShardRow = {
  owner_id: string | null
  lease_until: Date | string | null
  lease_epoch: string | number
}

export class PostgresDurabilityProvider implements DurabilityProvider {
  private readonly pool: Pool
  private readonly ownsPool: boolean
  private readonly schema: string
  private readonly physicalPartitions: number
  private readonly statementTimeoutMs?: number
  private readonly lockTimeoutMs?: number
  private readonly snapshotInterval: number
  private readonly observability: DurableObservability
  private readonly projections = new Map<number, ShardProjection>()
  private readonly shardLocks = new Map<number, Promise<void>>()
  private closed = false

  private constructor(options: PostgresDurabilityProviderOptions = {}) {
    this.schema = normalizeSchemaName(options.schema ?? `durable_${randomUUID().replaceAll("-", "_")}`)
    this.physicalPartitions = positiveInteger(options.physicalPartitions ?? 1, "physicalPartitions")
    this.statementTimeoutMs = options.statementTimeoutMs
    this.lockTimeoutMs = options.lockTimeoutMs
    this.snapshotInterval = options.snapshotInterval ?? 512
    this.observability = options
    this.ownsPool = !options.pool
    this.pool = options.pool ?? new Pool({
      connectionString:
        options.connectionString ??
        process.env.DURABLE_POSTGRES_URL ??
        "postgresql://durable:durable@127.0.0.1:55432/durable",
      max: options.poolSize ?? 10,
    })
  }

  static async create(
    options: PostgresDurabilityProviderOptions = {},
  ): Promise<PostgresDurabilityProvider> {
    const provider = new PostgresDurabilityProvider(options)
    await provider.initialize()
    return provider
  }

  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    for (const projection of this.projections.values()) {
      projection.engine.close()
    }
    this.projections.clear()
    if (this.ownsPool) {
      await this.pool.end()
    }
    this.closed = true
  }

  async dropSchema(): Promise<void> {
    await this.pool.query(`DROP SCHEMA IF EXISTS ${this.qSchema()} CASCADE`)
  }

  async claimShard(input: ClaimDispatchShardInput): Promise<ShardLease | null> {
    this.assertOpen()
    const lease = await this.withTransaction(async (client) => {
      await this.ensureShardHead(client, input.shardId)
      const leaseUntil = addMs(input.now, input.leaseMs)
      const result = await client.query<{
        owner_id: string
        lease_until: Date | string
        lease_epoch: string | number
      }>(
        `
        INSERT INTO ${this.qSchema()}.dispatch_shards (shard_id, owner_id, lease_until, lease_epoch)
        VALUES ($1, $2, $3::timestamptz, 1)
        ON CONFLICT (shard_id) DO UPDATE SET
          owner_id = EXCLUDED.owner_id,
          lease_until = EXCLUDED.lease_until,
          lease_epoch = CASE
            WHEN ${this.qSchema()}.dispatch_shards.owner_id = EXCLUDED.owner_id
              AND ${this.qSchema()}.dispatch_shards.lease_until IS NOT NULL
              AND ${this.qSchema()}.dispatch_shards.lease_until > $4::timestamptz
            THEN ${this.qSchema()}.dispatch_shards.lease_epoch
            ELSE ${this.qSchema()}.dispatch_shards.lease_epoch + 1
          END
        WHERE ${this.qSchema()}.dispatch_shards.owner_id IS NULL
          OR ${this.qSchema()}.dispatch_shards.owner_id = EXCLUDED.owner_id
          OR ${this.qSchema()}.dispatch_shards.lease_until IS NULL
          OR ${this.qSchema()}.dispatch_shards.lease_until <= $5::timestamptz
        RETURNING owner_id, lease_until, lease_epoch
        `,
        [input.shardId, input.ownerId, leaseUntil, input.now, input.now],
      )
      const row = result.rows[0]
      if (!row || row.owner_id !== input.ownerId) {
        return null
      }
      return {
        shardId: input.shardId,
        ownerId: input.ownerId,
        leaseUntil: iso(row.lease_until),
        leaseEpoch: Number(row.lease_epoch),
      } satisfies ShardLease
    })
    if (lease) {
      this.projectionForShard(input.shardId).engine.setShardLease(lease)
      this.providerLog("debug", "provider.shard.claim", {
        workerId: input.ownerId,
        shardId: input.shardId,
        status: "success",
      })
      this.providerCount("durable.provider.shard", {
        shardId: input.shardId,
        status: "claimed",
      })
    }
    return lease
  }

  openShard(input: OpenShardInput): ShardDurabilitySession {
    return new PostgresAppendShardSession(this, input)
  }

  async createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    return this.mutateShard(
      input.partitionShard,
      { op: "createInstance", input },
      () => this.projectionForShard(input.partitionShard).engine.createInstance(input),
    )
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.createChildInstanceForShard(input.partitionShard, input)
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    const shardId = await this.findShardForRef({
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
    })
    await this.cancelChildForShard(shardId, input)
  }

  async loadInstance(
    ref: InstanceRef,
    options: LoadInstanceOptions = {},
  ): Promise<PersistedInstance | null> {
    const shardId = await this.findShardForRef(ref, { allowMissing: true })
    return shardId === undefined ? null : this.readInstanceForShard(shardId, ref, options)
  }

  async listInstances(options: LoadInstanceOptions = {}): Promise<PersistedInstance[]> {
    await this.catchUpAllShards()
    const instances: PersistedInstance[] = []
    for (const projection of this.projections.values()) {
      instances.push(...projection.engine.listInstances())
    }
    if (!options.includeEffects) {
      return instances
    }
    return Promise.all(instances.map(async (instance) =>
      (await this.projectionForShard(instance.partitionShard).engine.loadInstance(instance, options)) ?? instance,
    ))
  }

  async listSignals(): Promise<SignalRecord[]> {
    await this.catchUpAllShards()
    return [...this.projections.values()].flatMap((projection) => projection.engine.listSignals())
  }

  async listChildren(): Promise<ChildRecord[]> {
    await this.catchUpAllShards()
    return [...this.projections.values()].flatMap((projection) => projection.engine.listChildren())
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
    await this.catchUpAllShards()
    return [...this.projections.values()].flatMap((projection) => projection.engine.listActivationClaims())
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    const shardId = await this.findShardForRef(input)
    return this.appendSignalForShard(shardId, input)
  }

  claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    return this.claimShard(input)
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    await this.heartbeatShard(input)
  }

  async releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    await this.releaseShard(input)
  }

  async claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult> {
    if (!Number.isInteger(input.limit) || input.limit <= 0) {
      throw new Error("claimReadyActivations limit must be a positive integer")
    }
    const claims: ClaimReadyActivationsResult["claims"] = []
    let nextWakeAt: string | undefined
    for (const shardId of input.shardIds) {
      if (claims.length >= input.limit) {
        break
      }
      const result = await this.claimShardTasks({
        shardId,
        workerId: input.workerId,
        input: {
          workflows: input.workflows,
          shardCount: input.shardCount,
          now: input.now,
          leaseMs: input.leaseMs,
          limit: input.limit - claims.length,
        },
      })
      claims.push(...result.claims)
      nextWakeAt = earliestIso(nextWakeAt, result.nextWakeAt)
    }
    return nextWakeAt ? { claims, nextWakeAt } : { claims }
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

  async heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    await this.forActivationShards(input.activationIds, (shardId, activationIds) =>
      this.heartbeatActivationsForShard(shardId, { ...input, activationIds }),
    )
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
    await this.forActivationShards(input.activationIds, (shardId, activationIds) =>
      this.releaseActivationsForShard(shardId, { ...input, activationIds }),
    )
  }

  async releaseActivation(input: ReleaseActivationInput): Promise<void> {
    await this.releaseActivations({ activationIds: [input.activationId], workerId: input.workerId })
  }

  async getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    const shardId = await this.findShardForRef(input)
    return this.getOrReserveEffectForShard(shardId, input)
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    const shardId = await this.findShardForRef(input)
    await this.heartbeatEffectForShard(shardId, input)
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    const shardId = await this.findShardForRef(input)
    await this.completeEffectForShard(shardId, input)
  }

  async failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    const shardId = await this.findShardForRef(input)
    return this.failEffectForShard(shardId, input)
  }

  async commitActivations(inputs: CommitActivationInput[]): Promise<CommitActivationsResult> {
    if (inputs.length === 0) {
      return { results: [] }
    }
    const grouped = new Map<number, CommitActivationInput[]>()
    for (const input of inputs) {
      const shardId = await this.findShardForRef(input)
      const list = grouped.get(shardId) ?? []
      list.push(input)
      grouped.set(shardId, list)
    }
    const resultsByActivation = new Map<string, CommitActivationsResult["results"][number]>()
    for (const [shardId, group] of grouped) {
      const result = await this.commitActivationsForShard(shardId, group)
      for (const entry of result.results) {
        resultsByActivation.set(entry.activationId, entry)
      }
    }
    return {
      results: inputs.map((input) =>
        resultsByActivation.get(input.activationId) ?? {
          activationId: input.activationId,
          ok: false,
          sequence: input.expectedSequence,
          reason: "missing_commit_result",
        },
      ),
    }
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
    const grouped = new Map<number, RecordActivationFailureInput[]>()
    for (const entry of input) {
      const shardId = await this.findShardForRef(entry)
      const list = grouped.get(shardId) ?? []
      list.push(entry)
      grouped.set(shardId, list)
    }
    for (const [shardId, group] of grouped) {
      await this.recordActivationFailuresForShard(shardId, group)
    }
  }

  async readInstanceForShard(
    shardId: number,
    ref: InstanceRef,
    options: LoadInstanceOptions = {},
  ): Promise<PersistedInstance | null> {
    await this.catchUpShard(shardId)
    return this.projectionForShard(shardId).engine.loadInstance(ref, options)
  }

  async appendSignalForShard(shardId: number, input: AppendSignalInput): Promise<SignalRecord> {
    return this.mutateShard(
      shardId,
      { op: "appendSignal", input },
      () => this.projectionForShard(shardId).engine.appendSignal(input),
    )
  }

  async createChildInstanceForShard(
    shardId: number,
    input: CreateChildInstanceInput,
  ): Promise<ChildHandle> {
    this.assertShardAffineChild(shardId, input.partitionShard)
    return this.mutateShard(
      shardId,
      { op: "createChildInstance", input },
      () => this.projectionForShard(shardId).engine.createChildInstance(input),
    )
  }

  async cancelChildForShard(shardId: number, input: CancelChildInput): Promise<void> {
    await this.mutateShard(
      shardId,
      { op: "cancelChild", input },
      () => this.projectionForShard(shardId).engine.cancelChild(input),
    )
  }

  async claimShardTasks(input: {
    shardId: number
    workerId?: string
    leaseEpoch?: number
    input: ClaimShardTasksInput
  }): Promise<ClaimShardTasksResult> {
    return this.withShardLock(input.shardId, async () => {
      await this.withTransaction(async (client) => {
        await this.ensureShardHead(client, input.shardId)
        await this.catchUpShardInClient(client, input.shardId, this.projectionForShard(input.shardId))
      })
      try {
        const result = await this.projectionForShard(input.shardId).engine.openShard({
          shardId: input.shardId,
          ownerId: input.workerId,
          leaseEpoch: input.leaseEpoch,
        }).claimTasks(input.input)
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
      } catch (error) {
        if (error instanceof Error && /Lost shard lease/.test(error.message)) {
          return { claims: [] }
        }
        throw error
      }
    })
  }

  async heartbeatShard(input: HeartbeatDispatchShardInput): Promise<void> {
    const row = await this.withTransaction(async (client) => {
      const result = await client.query<DispatchShardRow>(
        `
        UPDATE ${this.qSchema()}.dispatch_shards
        SET lease_until = $1::timestamptz
        WHERE shard_id = $2
          AND owner_id = $3
          AND lease_until >= $4::timestamptz
        RETURNING owner_id, lease_until, lease_epoch
        `,
        [addMs(input.now, input.leaseMs), input.shardId, input.ownerId, input.now],
      )
      return result.rows[0]
    })
    if (!row) {
      this.projectionForShard(input.shardId).engine.clearShardLease(input.shardId)
      throw new Error(`Lost dispatch shard lease: ${input.shardId}`)
    }
    this.projectionForShard(input.shardId).engine.setShardLease({
      shardId: input.shardId,
      ownerId: input.ownerId,
      leaseUntil: iso(row.lease_until!),
      leaseEpoch: Number(row.lease_epoch),
    })
  }

  async releaseShard(input: ReleaseDispatchShardInput): Promise<void> {
    await this.withTransaction(async (client) => {
      await client.query(
        `
        UPDATE ${this.qSchema()}.dispatch_shards
        SET owner_id = NULL, lease_until = NULL
        WHERE shard_id = $1 AND owner_id = $2
        `,
        [input.shardId, input.ownerId],
      )
    })
    this.projectionForShard(input.shardId).engine.clearShardLease(input.shardId)
  }

  async heartbeatActivationsForShard(
    shardId: number,
    input: HeartbeatActivationsInput,
  ): Promise<void> {
    await this.withShardLock(shardId, async () => {
      await this.withTransaction(async (client) => {
        await this.ensureShardHead(client, shardId)
        await this.catchUpShardInClient(client, shardId, this.projectionForShard(shardId))
      })
      await this.projectionForShard(shardId).engine.heartbeatActivations(input)
    })
  }

  async releaseActivationsForShard(shardId: number, input: ReleaseActivationsInput): Promise<void> {
    await this.withShardLock(shardId, async () => {
      await this.withTransaction(async (client) => {
        await this.ensureShardHead(client, shardId)
        await this.catchUpShardInClient(client, shardId, this.projectionForShard(shardId))
      })
      await this.projectionForShard(shardId).engine.releaseActivations(input)
    })
  }

  async getOrReserveEffectForShard(
    shardId: number,
    input: ReserveEffectInput,
  ): Promise<EffectReservation> {
    const result = await this.mutateShard(
      shardId,
      { op: "getOrReserveEffect", input },
      () => this.projectionForShard(shardId).engine.getOrReserveEffect(input),
    )
    this.providerLog("debug", "provider.effect.reserve", {
      workerId: input.workerId,
      status: result.status,
    })
    return result
  }

  async heartbeatEffectForShard(shardId: number, input: HeartbeatEffectInput): Promise<void> {
    await this.mutateShard(
      shardId,
      { op: "heartbeatEffect", input },
      () => this.projectionForShard(shardId).engine.heartbeatEffect(input),
    )
  }

  async completeEffectForShard(shardId: number, input: CompleteEffectInput): Promise<void> {
    await this.mutateShard(
      shardId,
      { op: "completeEffect", input },
      () => this.projectionForShard(shardId).engine.completeEffect(input),
    )
  }

  async failEffectForShard(shardId: number, input: FailEffectInput): Promise<FailEffectResult> {
    return this.mutateShard(
      shardId,
      { op: "failEffect", input },
      () => this.projectionForShard(shardId).engine.failEffect(input),
    )
  }

  async commitActivationsForShard(
    shardId: number,
    input: CommitActivationInput[],
  ): Promise<CommitActivationsResult> {
    if (input.length === 0) {
      return { results: [] }
    }
    const result = await this.mutateShard(
      shardId,
      { op: "commitActivations", input },
      () => {
        this.assertCheckpointChildStartsAreShardAffine(shardId, input)
        return this.projectionForShard(shardId).engine.commitActivations(input)
      },
      { shouldAppend: (value) => value.results.some((entry) => entry.ok) },
    )
    for (const entry of result.results) {
      this.providerLog(entry.ok ? "debug" : "warn", entry.ok ? "provider.checkpoint.commit" : "provider.checkpoint.conflict", {
        status: entry.ok ? "success" : "conflict",
        reason: entry.reason,
      })
    }
    return result
  }

  async recordActivationFailuresForShard(
    shardId: number,
    input: RecordActivationFailureInput[],
  ): Promise<void> {
    if (input.length === 0) {
      return
    }
    await this.mutateShard(
      shardId,
      { op: "recordActivationFailures", input },
      () => this.projectionForShard(shardId).engine.recordActivationFailures(input),
    )
  }

  private async initialize(): Promise<void> {
    this.assertOpen()
    const client = await this.pool.connect()
    try {
      await client.query("SELECT pg_advisory_lock(hashtext($1))", [this.schema])
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${this.qSchema()}`)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${this.qSchema()}.provider_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
      `)
      await this.verifyMetadata(client, "postgres_storage_shape", "append_store_v1")
      await this.verifyMetadata(client, "physical_partition_count", String(this.physicalPartitions))
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${this.qSchema()}.dispatch_shards (
          shard_id INTEGER PRIMARY KEY,
          owner_id TEXT,
          lease_until TIMESTAMPTZ,
          lease_epoch BIGINT NOT NULL DEFAULT 0
        )
      `)
      for (let partition = 0; partition < this.physicalPartitions; partition += 1) {
        await this.createPartitionTables(client, partition)
      }
    } finally {
      await client.query("SELECT pg_advisory_unlock(hashtext($1))", [this.schema]).catch(() => undefined)
      client.release()
    }
  }

  private async verifyMetadata(client: Queryable, key: string, expected: string): Promise<void> {
    const result = await client.query<{ value: string }>(
      `SELECT value FROM ${this.qSchema()}.provider_metadata WHERE key = $1`,
      [key],
    )
    const actual = result.rows[0]?.value
    if (actual !== undefined && actual !== expected) {
      throw new Error(`PostgresDurabilityProvider ${key} mismatch: expected ${expected}, found ${actual}`)
    }
    if (actual === undefined) {
      await client.query(
        `
        INSERT INTO ${this.qSchema()}.provider_metadata (key, value)
        VALUES ($1, $2)
        ON CONFLICT (key) DO NOTHING
        `,
        [key, expected],
      )
      const updated = await client.query<{ value: string }>(
        `SELECT value FROM ${this.qSchema()}.provider_metadata WHERE key = $1`,
        [key],
      )
      if (updated.rows[0]?.value !== expected) {
        throw new Error(
          `PostgresDurabilityProvider ${key} mismatch: expected ${expected}, found ${updated.rows[0]?.value}`,
        )
      }
    }
  }

  private async createPartitionTables(client: Queryable, partition: number): Promise<void> {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table("shard_heads", partition)} (
        shard_id INTEGER PRIMARY KEY,
        last_entry_id BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL
      );

      CREATE TABLE IF NOT EXISTS ${this.table("shard_journal", partition)} (
        shard_id INTEGER NOT NULL,
        entry_id BIGINT NOT NULL,
        operation_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (shard_id, entry_id)
      );

      CREATE TABLE IF NOT EXISTS ${this.table("shard_snapshots", partition)} (
        shard_id INTEGER PRIMARY KEY,
        last_entry_id BIGINT NOT NULL,
        snapshot_json TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      );

      CREATE INDEX IF NOT EXISTS ${this.index(`shard_journal_${partitionSuffix(partition)}_created`)}
        ON ${this.table("shard_journal", partition)} (created_at);
    `)
  }

  private async mutateShard<T>(
    shardId: number,
    operation: JournalOperation,
    apply: () => Promise<T>,
    options: { shouldAppend?: (result: T) => boolean } = {},
  ): Promise<T> {
    return this.withShardLock(shardId, async () => {
      try {
        return await this.withTransaction(async (client) => {
          await this.ensureShardHead(client, shardId)
          const head = await this.lockShardHead(client, shardId)
          const projection = this.projectionForShard(shardId)
          await this.catchUpShardInClient(client, shardId, projection)
          if (projection.appliedEntryId !== head) {
            throw new Error(
              `Postgres shard ${shardId} projection is inconsistent: head=${head} applied=${projection.appliedEntryId}`,
            )
          }
          const result = await apply()
          if (options.shouldAppend?.(result) ?? true) {
            const nextEntryId = head + 1
            await client.query(
              `
              INSERT INTO ${this.journalTableForShard(shardId)}
                (shard_id, entry_id, operation_json, created_at)
              VALUES ($1, $2, $3::jsonb, $4::timestamptz)
              `,
              [shardId, nextEntryId, encodeJson(operation), operationTime(operation)],
            )
            await client.query(
              `
              UPDATE ${this.headTableForShard(shardId)}
              SET last_entry_id = $2, updated_at = $3::timestamptz
              WHERE shard_id = $1
              `,
              [shardId, nextEntryId, operationTime(operation)],
            )
            projection.appliedEntryId = nextEntryId
            if (
              this.snapshotInterval > 0 &&
              nextEntryId > 0 &&
              nextEntryId % this.snapshotInterval === 0
            ) {
              await this.writeSnapshot(client, shardId, projection)
            }
          }
          return result
        })
      } catch (error) {
        throw error
      }
    })
  }

  private async catchUpShard(shardId: number): Promise<void> {
    await this.withShardLock(shardId, async () => {
      await this.withTransaction(async (client) => {
        await this.ensureShardHead(client, shardId)
        await this.catchUpShardInClient(client, shardId, this.projectionForShard(shardId))
      })
    })
  }

  private async catchUpShardInClient(
    client: Queryable,
    shardId: number,
    projection: ShardProjection,
  ): Promise<void> {
    if (!projection.loaded) {
      projection.engine.close()
      projection.appliedEntryId = 0
      const snapshot = await client.query<SnapshotRow>(
        `
        SELECT last_entry_id, snapshot_json
        FROM ${this.snapshotTableForShard(shardId)}
        WHERE shard_id = $1
        `,
        [shardId],
      )
      const row = snapshot.rows[0]
      if (row) {
        projection.engine.restore(decodeJson<ShardMemorySnapshot>(row.snapshot_json))
        projection.appliedEntryId = Number(row.last_entry_id)
      }
      projection.loaded = true
    }

    const journal = await client.query<JournalRow>(
      `
      SELECT entry_id, operation_json
      FROM ${this.journalTableForShard(shardId)}
      WHERE shard_id = $1 AND entry_id > $2
      ORDER BY entry_id
      `,
      [shardId, projection.appliedEntryId],
    )
    for (const row of journal.rows) {
      await applyJournalOperation(projection.engine, decodeJson<JournalOperation>(row.operation_json))
      projection.appliedEntryId = Number(row.entry_id)
    }
    await this.syncShardLease(client, shardId, projection)
  }

  private async catchUpAllShards(): Promise<void> {
    const shardIds = await this.knownShardIds()
    for (const shardId of shardIds) {
      await this.catchUpShard(shardId)
    }
  }

  private async knownShardIds(): Promise<number[]> {
    const values = new Set<number>()
    const dispatch = await this.pool.query<{ shard_id: number }>(
      `SELECT shard_id FROM ${this.qSchema()}.dispatch_shards`,
    )
    for (const row of dispatch.rows) {
      values.add(row.shard_id)
    }
    for (let partition = 0; partition < this.physicalPartitions; partition += 1) {
      const result = await this.pool.query<{ shard_id: number }>(
        `SELECT shard_id FROM ${this.table("shard_heads", partition)}`,
      )
      for (const row of result.rows) {
        values.add(row.shard_id)
      }
    }
    return [...values].sort((left, right) => left - right)
  }

  private async findShardForRef(
    ref: InstanceRef,
    options: { allowMissing: true },
  ): Promise<number | undefined>
  private async findShardForRef(ref: InstanceRef, options?: { allowMissing?: false }): Promise<number>
  private async findShardForRef(
    ref: InstanceRef,
    options: { allowMissing?: boolean } = {},
  ): Promise<number | undefined> {
    for (const [shardId, projection] of this.projections) {
      const instance = await projection.engine.loadInstance(ref)
      if (instance) {
        return shardId
      }
    }
    for (const shardId of await this.knownShardIds()) {
      await this.catchUpShard(shardId)
      const instance = await this.projectionForShard(shardId).engine.loadInstance(ref)
      if (instance) {
        return shardId
      }
    }
    if (options.allowMissing) {
      return undefined
    }
    throw new Error(`Unknown workflow instance: ${ref.workflowId}/${ref.runId}`)
  }

  private async findShardForActivation(activationId: string): Promise<number> {
    for (const [shardId, projection] of this.projections) {
      if (projection.engine.listActivationClaims().some((claim) => claim.activationId === activationId)) {
        return shardId
      }
    }
    for (const shardId of await this.knownShardIds()) {
      await this.catchUpShard(shardId)
      const projection = this.projectionForShard(shardId)
      if (projection.engine.listActivationClaims().some((claim) => claim.activationId === activationId)) {
        return shardId
      }
    }
    throw new Error(`Unknown activation: ${activationId}`)
  }

  private async forActivationShards(
    activationIds: string[],
    fn: (shardId: number, activationIds: string[]) => Promise<void>,
  ): Promise<void> {
    const grouped = new Map<number, string[]>()
    for (const activationId of activationIds) {
      const shardId = await this.findShardForActivation(activationId)
      const list = grouped.get(shardId) ?? []
      list.push(activationId)
      grouped.set(shardId, list)
    }
    for (const [shardId, ids] of grouped) {
      await fn(shardId, ids)
    }
  }

  private projectionForShard(shardId: number): ShardProjection {
    const existing = this.projections.get(shardId)
    if (existing) {
      return existing
    }
    const projection: ShardProjection = {
      engine: new ShardMemoryDurabilityProvider(),
      appliedEntryId: 0,
      loaded: false,
    }
    this.projections.set(shardId, projection)
    return projection
  }

  private resetProjection(shardId: number): void {
    const projection = this.projections.get(shardId)
    if (!projection) {
      return
    }
    projection.engine.close()
    projection.appliedEntryId = 0
    projection.loaded = false
  }

  private async ensureShardHead(client: Queryable, shardId: number): Promise<void> {
    await client.query(
      `
      INSERT INTO ${this.headTableForShard(shardId)} (shard_id, last_entry_id, updated_at)
      VALUES ($1, 0, now())
      ON CONFLICT (shard_id) DO NOTHING
      `,
      [shardId],
    )
  }

  private async lockShardHead(client: PoolClient, shardId: number): Promise<number> {
    const result = await client.query<{ last_entry_id: string | number }>(
      `
      SELECT last_entry_id
      FROM ${this.headTableForShard(shardId)}
      WHERE shard_id = $1
      FOR UPDATE
      `,
      [shardId],
    )
    const row = result.rows[0]
    if (!row) {
      throw new Error(`Missing shard head: ${shardId}`)
    }
    return Number(row.last_entry_id)
  }

  private async writeSnapshot(
    client: Queryable,
    shardId: number,
    projection: ShardProjection,
  ): Promise<void> {
    await client.query(
      `
      INSERT INTO ${this.snapshotTableForShard(shardId)}
        (shard_id, last_entry_id, snapshot_json, created_at)
      VALUES ($1, $2, $3, now())
      ON CONFLICT (shard_id) DO UPDATE SET
        last_entry_id = EXCLUDED.last_entry_id,
        snapshot_json = EXCLUDED.snapshot_json,
        created_at = EXCLUDED.created_at
      `,
      [shardId, projection.appliedEntryId, encodeJson(projection.engine.snapshot())],
    )
  }

  private async syncShardLease(
    client: Queryable,
    shardId: number,
    projection: ShardProjection,
  ): Promise<void> {
    const result = await client.query<DispatchShardRow>(
      `
      SELECT owner_id, lease_until, lease_epoch
      FROM ${this.qSchema()}.dispatch_shards
      WHERE shard_id = $1
      `,
      [shardId],
    )
    const row = result.rows[0]
    if (!row?.owner_id || !row.lease_until) {
      projection.engine.clearShardLease(shardId)
      return
    }
    projection.engine.setShardLease({
      shardId,
      ownerId: row.owner_id,
      leaseUntil: iso(row.lease_until),
      leaseEpoch: Number(row.lease_epoch),
    })
  }

  private assertShardAffineChild(parentShardId: number, childShardId: number): void {
    if (parentShardId !== childShardId) {
      throw new Error(
        "Postgres append-store local child workflows must be shard-affine; explicit cross-shard child workflow IDs are not supported yet",
      )
    }
  }

  private assertCheckpointChildStartsAreShardAffine(
    shardId: number,
    inputs: CommitActivationInput[],
  ): void {
    for (const input of inputs) {
      for (const start of input.childStarts ?? []) {
        this.assertShardAffineChild(shardId, start.partitionShard)
      }
    }
  }

  private async withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    this.assertOpen()
    const client = await this.pool.connect()
    try {
      await client.query("BEGIN")
      if (this.statementTimeoutMs !== undefined) {
        await client.query(`SET LOCAL statement_timeout = ${positiveInteger(this.statementTimeoutMs, "statementTimeoutMs")}`)
      }
      if (this.lockTimeoutMs !== undefined) {
        await client.query(`SET LOCAL lock_timeout = ${positiveInteger(this.lockTimeoutMs, "lockTimeoutMs")}`)
      }
      const result = await fn(client)
      await client.query("COMMIT")
      return result
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async withShardLock<T>(shardId: number, fn: () => Promise<T>): Promise<T> {
    const previous = this.shardLocks.get(shardId) ?? Promise.resolve()
    let release!: () => void
    const current = new Promise<void>((resolve) => {
      release = resolve
    })
    const chained = previous.then(() => current, () => current)
    this.shardLocks.set(shardId, chained)
    await previous.catch(() => undefined)
    try {
      return await fn()
    } finally {
      release()
      if (this.shardLocks.get(shardId) === chained) {
        this.shardLocks.delete(shardId)
      }
    }
  }

  private table(base: "shard_heads" | "shard_journal" | "shard_snapshots", partition: number): string {
    return `${this.qSchema()}.${quoteIdentifier(`${base}_${partitionSuffix(partition)}`)}`
  }

  private headTableForShard(shardId: number): string {
    return this.table("shard_heads", this.physicalPartitionForShard(shardId))
  }

  private journalTableForShard(shardId: number): string {
    return this.table("shard_journal", this.physicalPartitionForShard(shardId))
  }

  private snapshotTableForShard(shardId: number): string {
    return this.table("shard_snapshots", this.physicalPartitionForShard(shardId))
  }

  private physicalPartitionForShard(shardId: number): number {
    if (!Number.isInteger(shardId) || shardId < 0) {
      throw new Error("shardId must be a non-negative integer")
    }
    return shardId % this.physicalPartitions
  }

  private qSchema(): string {
    return quoteIdentifier(this.schema)
  }

  private index(name: string): string {
    return quoteIdentifier(name)
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

  private assertOpen(): void {
    if (this.closed) {
      throw new Error("PostgresDurabilityProvider is closed")
    }
  }
}

class PostgresAppendShardSession implements ShardDurabilitySession {
  readonly shardId: number
  readonly ownerId?: string
  readonly leaseEpoch?: number

  constructor(
    private readonly provider: PostgresDurabilityProvider,
    input: OpenShardInput,
  ) {
    this.shardId = input.shardId
    this.ownerId = input.ownerId
    this.leaseEpoch = input.leaseEpoch
  }

  createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    if (input.partitionShard !== this.shardId) {
      throw new Error(`Instance ${input.workflowId}/${input.runId} does not belong to shard ${this.shardId}`)
    }
    return this.provider.createInstance(input)
  }

  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.provider.createChildInstanceForShard(this.shardId, input)
  }

  cancelChild(input: CancelChildInput): Promise<void> {
    return this.provider.cancelChildForShard(this.shardId, input)
  }

  readInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null> {
    return this.provider.readInstanceForShard(this.shardId, ref, options)
  }

  appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.provider.appendSignalForShard(this.shardId, input)
  }

  claimTasks(input: ClaimShardTasksInput): Promise<ClaimShardTasksResult> {
    return this.provider.claimShardTasks({
      shardId: this.shardId,
      workerId: this.ownerId,
      leaseEpoch: this.leaseEpoch,
      input,
    })
  }

  heartbeat(input: { now: string; leaseMs: number }): Promise<void> {
    if (!this.ownerId) {
      return Promise.resolve()
    }
    return this.provider.heartbeatShard({
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
    return this.provider.releaseShard({ shardId: this.shardId, ownerId: this.ownerId })
  }

  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    return this.provider.heartbeatActivationsForShard(this.shardId, input)
  }

  heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    return this.heartbeatActivations({
      activationIds: [input.activationId],
      workerId: input.workerId,
      now: input.now,
      leaseMs: input.leaseMs,
    })
  }

  releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    return this.provider.releaseActivationsForShard(this.shardId, input)
  }

  releaseActivation(input: ReleaseActivationInput): Promise<void> {
    return this.releaseActivations({ activationIds: [input.activationId], workerId: input.workerId })
  }

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.provider.getOrReserveEffectForShard(this.shardId, input)
  }

  heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    return this.provider.heartbeatEffectForShard(this.shardId, input)
  }

  completeEffect(input: CompleteEffectInput): Promise<void> {
    return this.provider.completeEffectForShard(this.shardId, input)
  }

  failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.provider.failEffectForShard(this.shardId, input)
  }

  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult> {
    return this.provider.commitActivationsForShard(this.shardId, input)
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const result = await this.commitActivations([input])
    const first = result.results[0] ?? { ok: false, sequence: -1, reason: "missing_commit_result" }
    const { activationId: _activationId, ...checkpoint } = first
    return checkpoint
  }

  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void> {
    return this.provider.recordActivationFailuresForShard(this.shardId, input)
  }
}

function normalizeSchemaName(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error("PostgresDurabilityProvider schema must be a valid identifier")
  }
  return value
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
  return value
}

function partitionSuffix(partition: number): string {
  return `p${String(partition).padStart(2, "0")}`
}

function encodeJson(value: unknown): string {
  return JSON.stringify(value)
}

function decodeJson<T>(raw: unknown): T {
  if (typeof raw === "string") {
    return JSON.parse(raw) as T
  }
  return structuredClone(raw) as T
}

function iso(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString()
}

function addMs(isoValue: string, ms: number): string {
  return new Date(new Date(isoValue).getTime() + ms).toISOString()
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values.filter(Boolean).sort()[0]
}
