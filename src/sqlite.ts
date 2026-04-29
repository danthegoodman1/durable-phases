import { randomUUID } from "node:crypto"
import { mkdirSync } from "node:fs"
import { createRequire } from "node:module"
import { dirname } from "node:path"
import type {
  AppendSignalInput,
  ChildRecord,
  ClaimDispatchShardInput,
  ClaimedActivation,
  ClaimReadyActivationInput,
  CommitCheckpointInput,
  CommitCheckpointResult,
  CompleteEffectInput,
  CreateChildInstanceInput,
  CreateInstanceInput,
  DispatchShardLease,
  DurableWait,
  DurabilityProvider,
  EffectRecord,
  EffectReservation,
  FailEffectInput,
  HeartbeatActivationInput,
  HeartbeatDispatchShardInput,
  HeartbeatEffectInput,
  PersistedInstance,
  ReadyEvent,
  ReleaseActivationInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
  SignalRecord,
} from "./interface.js"
import type {
  AnyWorkflow,
  ChildHandle,
  InstanceRef,
  JsonObject,
  JsonValue,
  OutputOf,
  SerializedError,
} from "./workflow.js"
import { clone, isPlainObject, toJson } from "./workflow.js"

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
  transaction<T extends (...args: unknown[]) => unknown>(fn: T): T & { immediate: T }
  close(): unknown
}
type SqliteConstructor = {
  new (filename?: string, options?: { timeout?: number; fileMustExist?: boolean }): SqliteDatabase
}

const Database = require("better-sqlite3") as SqliteConstructor

type ReadyCandidate = ClaimedActivation & {
  sort: string[]
}

type InstanceRow = {
  workflow_name: string
  workflow_version: number
  workflow_id: string
  run_id: string
  sequence: number
  status: "running" | "completed" | "canceled" | "failed"
  common_json: string | null
  phase_name: string | null
  phase_data_json: string | null
  output_json: string | null
  error_json: string | null
  cancel_reason: string | null
  waits_json: string
  parent_workflow_id: string | null
  parent_run_id: string | null
  parent_child_record_id: string | null
  created_at: string
  updated_at: string
}

type SignalRow = {
  signal_id: string
  workflow_id: string
  run_id: string
  type: string
  payload_json: string
  received_at: string
  consumed_by_sequence: number | null
}

type ChildRow = {
  child_record_id: string
  parent_workflow_id: string
  parent_run_id: string
  activation_id: string
  key: string
  workflow_name: string
  workflow_version: number
  workflow_id: string
  run_id: string
  status: "started" | "completed" | "failed"
  completed_at: string | null
  output_json: string | null
  error_json: string | null
  delivered_by_sequence: number | null
}

type EffectRow = {
  effect_id: string
  activation_id: string
  key: string
  idempotency_key: string
  status: "pending" | "completed" | "failed"
  result_json: string | null
  error_json: string | null
  heartbeat_at: string | null
  heartbeat_details_json: string | null
}

type ActivationClaimRow = {
  activation_id: string
  owner_id: string | null
  lease_until: string | null
  completed_by_sequence: number | null
}

type DispatchShardRow = {
  shard_id: number
  owner_id: string | null
  lease_until: string | null
}

export class SqliteDurabilityProvider implements DurabilityProvider {
  private readonly db: SqliteDatabase
  private readonly busyTimeoutMs: number
  private closed = false

  constructor(
    private readonly filePath: string,
    options: { busyTimeoutMs?: number } = {},
  ) {
    this.busyTimeoutMs = options.busyTimeoutMs ?? 5_000
    if (filePath !== ":memory:") {
      mkdirSync(dirname(filePath), { recursive: true })
    }
    this.db = new Database(filePath, { timeout: this.busyTimeoutMs })
    this.configure()
    this.migrate()
  }

  close(): void {
    if (this.closed) {
      return
    }
    this.db.close()
    this.closed = true
  }

  async createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    const create = this.db.transaction(() => {
      const existing = this.instanceRow(input)
      const conflictPolicy = input.conflictPolicy ?? "fail"

      if (existing && conflictPolicy === "use_existing") {
        return { workflowId: existing.workflow_id, runId: existing.run_id }
      }

      if (existing && conflictPolicy === "fail") {
        throw new Error(`Workflow instance already exists: ${input.workflowId}/${input.runId}`)
      }

      if (existing && conflictPolicy === "terminate_existing") {
        this.deleteInstanceRecords(input.workflowId, input.runId)
      }

      this.db
        .prepare(
          `
          INSERT INTO instances (
            workflow_name, workflow_version, workflow_id, run_id, partition_shard,
            sequence, status, common_json, phase_name, phase_data_json, output_json,
            error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
            parent_child_record_id, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, 0, 'running', ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?)
        `,
        )
        .run(
          input.workflowName,
          input.workflowVersion,
          input.workflowId,
          input.runId,
          input.partitionShard,
          encodeJson(input.common),
          input.phase.name,
          encodeJson(input.phase.data),
          encodeJson(input.waits),
          input.parent?.workflowId ?? null,
          input.parent?.runId ?? null,
          input.parent?.childRecordId ?? null,
          input.now,
          input.now,
        )

      return { workflowId: input.workflowId, runId: input.runId }
    })

    return create.immediate() as InstanceRef
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    const create = this.db.transaction(() => {
      const existing = oneRow<ChildRow>(
        this.db
          .prepare(
            `
            SELECT * FROM children
            WHERE parent_workflow_id = ? AND parent_run_id = ? AND activation_id = ? AND key = ?
            LIMIT 1
          `,
          )
          .get(input.parentWorkflowId, input.parentRunId, input.activationId, input.key),
      )

      if (existing) {
        return childHandle(rowToChildRecord(existing))
      }

      if (this.instanceRow(input)) {
        throw new Error(`Child workflow instance already exists: ${input.workflowId}/${input.runId}`)
      }

      const childRecordId = `child-${randomUUID()}`
      this.db
        .prepare(
          `
          INSERT INTO instances (
            workflow_name, workflow_version, workflow_id, run_id, partition_shard,
            sequence, status, common_json, phase_name, phase_data_json, output_json,
            error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
            parent_child_record_id, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, 0, 'running', ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?)
        `,
        )
        .run(
          input.workflowName,
          input.workflowVersion,
          input.workflowId,
          input.runId,
          input.partitionShard,
          encodeJson(input.common),
          input.phase.name,
          encodeJson(input.phase.data),
          encodeJson(input.waits),
          input.parentWorkflowId,
          input.parentRunId,
          childRecordId,
          input.now,
          input.now,
        )

      this.db
        .prepare(
          `
          INSERT INTO children (
            child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
            workflow_name, workflow_version, workflow_id, run_id, status
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'started')
        `,
        )
        .run(
          childRecordId,
          input.parentWorkflowId,
          input.parentRunId,
          input.activationId,
          input.key,
          input.workflowName,
          input.workflowVersion,
          input.workflowId,
          input.runId,
        )

      return {
        workflowName: input.workflowName,
        workflowVersion: input.workflowVersion,
        workflowId: input.workflowId,
        runId: input.runId,
      }
    })

    return create.immediate() as ChildHandle
  }

  async loadInstance(ref: InstanceRef): Promise<PersistedInstance | null> {
    const row = this.instanceRow(ref)
    return row ? this.persistedInstance(row) : null
  }

  async listInstances(): Promise<PersistedInstance[]> {
    return this.db
      .prepare("SELECT * FROM instances ORDER BY workflow_id, run_id")
      .all()
      .map((row) => this.persistedInstance(requireRow<InstanceRow>(row)))
  }

  async listSignals(): Promise<SignalRecord[]> {
    return this.db
      .prepare("SELECT * FROM signals ORDER BY received_at, signal_id")
      .all()
      .map((row) => rowToSignalRecord(requireRow<SignalRow>(row)))
  }

  async listChildren(): Promise<ChildRecord[]> {
    return this.db
      .prepare("SELECT * FROM children ORDER BY child_record_id")
      .all()
      .map((row) => rowToChildRecord(requireRow<ChildRow>(row)))
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    const append = this.db.transaction(() => {
      const instance = this.instanceRow(input)
      if (!instance || instance.status !== "running") {
        throw new Error(`Cannot signal non-running workflow: ${input.workflowId}/${input.runId}`)
      }

      const signal: SignalRecord = {
        signalId: `signal-${randomUUID()}`,
        workflowId: input.workflowId,
        runId: input.runId,
        type: input.type,
        payload: clone(input.payload),
        receivedAt: input.receivedAt,
      }

      this.db
        .prepare(
          `
          INSERT INTO signals (signal_id, workflow_id, run_id, type, payload_json, received_at)
          VALUES (?, ?, ?, ?, ?, ?)
        `,
        )
        .run(
          signal.signalId,
          signal.workflowId,
          signal.runId,
          signal.type,
          encodeJson(signal.payload),
          signal.receivedAt,
        )

      return signal
    })

    return append.immediate() as SignalRecord
  }

  async claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    const claim = this.db.transaction(() => {
      const leaseUntil = addMs(input.now, input.leaseMs)

      const result = this.db
        .prepare(
          `
          INSERT INTO dispatch_shards (shard_id, owner_id, lease_until)
          VALUES (?, ?, ?)
          ON CONFLICT(shard_id) DO UPDATE SET
            owner_id = excluded.owner_id,
            lease_until = excluded.lease_until
          WHERE dispatch_shards.owner_id IS NULL
            OR dispatch_shards.owner_id = excluded.owner_id
            OR dispatch_shards.lease_until IS NULL
            OR dispatch_shards.lease_until <= ?
        `,
        )
        .run(input.shardId, input.ownerId, leaseUntil, input.now)

      if (result.changes === 0) {
        return null
      }

      return { shardId: input.shardId, ownerId: input.ownerId, leaseUntil }
    })

    return claim.immediate() as DispatchShardLease | null
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE dispatch_shards
        SET lease_until = ?
        WHERE shard_id = ? AND owner_id = ?
      `,
      )
      .run(addMs(input.now, input.leaseMs), input.shardId, input.ownerId)
  }

  async releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE dispatch_shards
        SET owner_id = NULL, lease_until = NULL
        WHERE shard_id = ? AND owner_id = ?
      `,
      )
      .run(input.shardId, input.ownerId)
  }

  async claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimedActivation | null> {
    const claim = this.db.transaction(() => {
      const ownedShards = input.shardIds.filter((shardId) => {
        const shard = oneRow<DispatchShardRow>(
          this.db.prepare("SELECT * FROM dispatch_shards WHERE shard_id = ?").get(shardId),
        )
        return shard?.owner_id === input.workerId && (shard.lease_until ?? "") >= input.now
      })

      if (ownedShards.length === 0) {
        return null
      }

      const candidates = this.readyCandidates({
        ...input,
        shardIds: ownedShards,
      }).sort(compareCandidates)

      for (const candidate of candidates) {
        if (this.tryWriteActivationClaim(candidate, input.workerId, input.now, input.leaseMs)) {
          return stripSort(candidate)
        }
      }

      return null
    })

    return claim.immediate() as ClaimedActivation | null
  }

  async heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE activation_claims
        SET lease_until = ?
        WHERE activation_id = ? AND owner_id = ? AND completed_by_sequence IS NULL
      `,
      )
      .run(addMs(input.now, input.leaseMs), input.activationId, input.workerId)
  }

  async releaseActivation(input: ReleaseActivationInput): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE activation_claims
        SET owner_id = NULL, lease_until = NULL
        WHERE activation_id = ? AND owner_id = ? AND completed_by_sequence IS NULL
      `,
      )
      .run(input.activationId, input.workerId)
  }

  async getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    const reserve = this.db.transaction(() => {
      const existing = oneRow<EffectRow>(
        this.db
          .prepare(
            `
            SELECT * FROM effects
            WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND key = ?
            LIMIT 1
          `,
          )
          .get(input.workflowId, input.runId, input.activationId, input.key),
      )

      if (existing?.status === "completed") {
        return {
          status: "completed",
          result: decodeJson<JsonValue>(existing.result_json, null),
        } satisfies EffectReservation
      }

      if (existing?.status === "failed") {
        return {
          status: "failed",
          error: decodeJson<SerializedError>(existing.error_json, { message: "Effect failed" }),
        } satisfies EffectReservation
      }

      if (existing) {
        return {
          status: "reserved",
          effectId: existing.effect_id,
          idempotencyKey: existing.idempotency_key,
          heartbeatDetails: decodeJson<JsonValue | undefined>(existing.heartbeat_details_json, undefined),
        } satisfies EffectReservation
      }

      const effectId = `effect-${randomUUID()}`
      const idempotencyKey = `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`
      this.db
        .prepare(
          `
          INSERT INTO effects (
            effect_id, workflow_id, run_id, activation_id, key, idempotency_key, status
          ) VALUES (?, ?, ?, ?, ?, ?, 'pending')
        `,
        )
        .run(effectId, input.workflowId, input.runId, input.activationId, input.key, idempotencyKey)

      return {
        status: "reserved",
        effectId,
        idempotencyKey,
      } satisfies EffectReservation
    })

    return reserve.immediate() as EffectReservation
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    const result = this.db
      .prepare(
        `
        UPDATE effects
        SET heartbeat_at = ?, heartbeat_details_json = ?
        WHERE effect_id = ?
      `,
      )
      .run(input.now, encodeJson(input.details ?? null), input.effectId)
    if (result.changes === 0) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    const result = this.db
      .prepare(
        `
        UPDATE effects
        SET status = 'completed', result_json = ?, error_json = NULL
        WHERE workflow_id = ? AND run_id = ? AND effect_id = ?
      `,
      )
      .run(encodeJson(input.result), input.workflowId, input.runId, input.effectId)
    if (result.changes === 0) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
  }

  async failEffect(input: FailEffectInput): Promise<void> {
    const result = this.db
      .prepare(
        `
        UPDATE effects
        SET status = 'failed', error_json = ?
        WHERE workflow_id = ? AND run_id = ? AND effect_id = ?
      `,
      )
      .run(encodeJson(input.error), input.workflowId, input.runId, input.effectId)
    if (result.changes === 0) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const commit = this.db.transaction(() => {
      const instance = this.instanceRow(input)
      if (!instance || instance.status !== "running") {
        return { ok: false, sequence: instance?.sequence ?? -1 }
      }

      if (instance.sequence !== input.expectedSequence) {
        return { ok: false, sequence: instance.sequence }
      }

      const claim = oneRow<ActivationClaimRow>(
        this.db.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(input.activationId),
      )

      if (
        !claim ||
        claim.owner_id !== input.workerId ||
        !claim.lease_until ||
        claim.lease_until < input.now ||
        claim.completed_by_sequence !== null
      ) {
        return { ok: false, sequence: instance.sequence }
      }

      const signalToConsume = input.consumeSignalId
        ? oneRow<SignalRow>(
            this.db
              .prepare(
                `
                SELECT * FROM signals
                WHERE signal_id = ? AND workflow_id = ? AND run_id = ? AND consumed_by_sequence IS NULL
                LIMIT 1
              `,
              )
              .get(input.consumeSignalId, input.workflowId, input.runId),
          )
        : undefined

      if (input.consumeSignalId && !signalToConsume) {
        return { ok: false, sequence: instance.sequence }
      }

      const childToConsume = input.consumeChildRecordId
        ? oneRow<ChildRow>(
            this.db
              .prepare(
                `
                SELECT * FROM children
                WHERE child_record_id = ? AND parent_workflow_id = ? AND parent_run_id = ?
                  AND delivered_by_sequence IS NULL
                LIMIT 1
              `,
              )
              .get(input.consumeChildRecordId, input.workflowId, input.runId),
          )
        : undefined

      if (input.consumeChildRecordId && !childToConsume) {
        return { ok: false, sequence: instance.sequence }
      }

      const nextSequence = instance.sequence + 1
      this.writeNextInstance(input, nextSequence)

      if (signalToConsume) {
        this.db
          .prepare("UPDATE signals SET consumed_by_sequence = ? WHERE signal_id = ?")
          .run(nextSequence, signalToConsume.signal_id)
      }

      if (childToConsume) {
        this.db
          .prepare("UPDATE children SET delivered_by_sequence = ? WHERE child_record_id = ?")
          .run(nextSequence, childToConsume.child_record_id)
      }

      this.updateParentChildRecord(instance, input, nextSequence)

      this.db
        .prepare(
          `
          UPDATE activation_claims
          SET completed_by_sequence = ?, completed_at = ?, lease_until = ?, owner_id = ?
          WHERE activation_id = ?
        `,
        )
        .run(nextSequence, input.now, input.now, input.workerId, input.activationId)

      return { ok: true, sequence: nextSequence }
    })

    return commit.immediate() as CommitCheckpointResult
  }

  async readOutput<W extends AnyWorkflow>(handle: ChildHandle<W>): Promise<OutputOf<W>> {
    const instance = this.instanceRow(handle)
    if (!instance) {
      throw new Error(`Unknown child workflow: ${handle.workflowId}/${handle.runId}`)
    }

    if (instance.status !== "completed") {
      throw new Error(`Child workflow is not complete: ${handle.workflowId}/${handle.runId}`)
    }

    return decodeJson(instance.output_json, null) as OutputOf<W>
  }

  private configure(): void {
    this.db.pragma("journal_mode = WAL")
    this.db.pragma("foreign_keys = ON")
    this.db.pragma(`busy_timeout = ${this.busyTimeoutMs}`)
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS instances (
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        partition_shard INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'canceled', 'failed')),
        common_json TEXT,
        phase_name TEXT,
        phase_data_json TEXT,
        output_json TEXT,
        error_json TEXT,
        cancel_reason TEXT,
        waits_json TEXT NOT NULL,
        parent_workflow_id TEXT,
        parent_run_id TEXT,
        parent_child_record_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (workflow_id, run_id)
      );

      CREATE TABLE IF NOT EXISTS signals (
        signal_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        received_at TEXT NOT NULL,
        consumed_by_sequence INTEGER,
        FOREIGN KEY (workflow_id, run_id) REFERENCES instances(workflow_id, run_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS children (
        child_record_id TEXT PRIMARY KEY,
        parent_workflow_id TEXT NOT NULL,
        parent_run_id TEXT NOT NULL,
        activation_id TEXT NOT NULL,
        key TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
        completed_at TEXT,
        output_json TEXT,
        error_json TEXT,
        delivered_by_sequence INTEGER,
        UNIQUE(parent_workflow_id, parent_run_id, activation_id, key)
      );

      CREATE TABLE IF NOT EXISTS activation_claims (
        activation_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        kind TEXT NOT NULL,
        wait_name TEXT,
        event_json TEXT,
        wait_json TEXT,
        owner_id TEXT,
        lease_until TEXT,
        activation_time TEXT,
        completed_by_sequence INTEGER,
        completed_at TEXT,
        FOREIGN KEY (workflow_id, run_id) REFERENCES instances(workflow_id, run_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS effects (
        effect_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        activation_id TEXT NOT NULL,
        key TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
        result_json TEXT,
        error_json TEXT,
        heartbeat_at TEXT,
        heartbeat_details_json TEXT,
        UNIQUE(workflow_id, run_id, activation_id, key),
        FOREIGN KEY (workflow_id, run_id) REFERENCES instances(workflow_id, run_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS dispatch_shards (
        shard_id INTEGER PRIMARY KEY,
        owner_id TEXT,
        lease_until TEXT
      );

      CREATE INDEX IF NOT EXISTS instances_by_status_shard
        ON instances(status, partition_shard, updated_at);
      CREATE INDEX IF NOT EXISTS signals_hot_lookup
        ON signals(workflow_id, run_id, consumed_by_sequence, type, received_at, signal_id);
      CREATE INDEX IF NOT EXISTS children_parent_delivery
        ON children(parent_workflow_id, parent_run_id, delivered_by_sequence, completed_at, child_record_id);
      CREATE INDEX IF NOT EXISTS children_child_instance
        ON children(workflow_id, run_id, status, delivered_by_sequence);
      CREATE INDEX IF NOT EXISTS effects_activation_key
        ON effects(workflow_id, run_id, activation_id, key);
      CREATE INDEX IF NOT EXISTS activation_claims_owner_lease
        ON activation_claims(owner_id, lease_until, completed_by_sequence);
    `)
    this.addColumnIfMissing("activation_claims", "activation_time", "TEXT")
  }

  private addColumnIfMissing(table: string, column: string, definition: string): void {
    const columns = this.db.prepare(`PRAGMA table_info(${table})`).all()
    const exists = columns.some((row) => isPlainObject(row) && row.name === column)
    if (!exists) {
      this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
    }
  }

  private instanceRow(ref: InstanceRef): InstanceRow | null {
    return oneRow<InstanceRow>(
      this.db
        .prepare("SELECT * FROM instances WHERE workflow_id = ? AND run_id = ? LIMIT 1")
        .get(ref.workflowId, ref.runId),
    )
  }

  private persistedInstance(row: InstanceRow): PersistedInstance {
    const effects = this.db
      .prepare("SELECT * FROM effects WHERE workflow_id = ? AND run_id = ? ORDER BY effect_id")
      .all(row.workflow_id, row.run_id)
      .map((effectRow) => rowToEffectRecord(requireRow<EffectRow>(effectRow)))

    return {
      workflowName: row.workflow_name,
      workflowVersion: row.workflow_version,
      workflowId: row.workflow_id,
      runId: row.run_id,
      sequence: row.sequence,
      status: row.status,
      common: decodeJson<JsonObject | undefined>(row.common_json, undefined),
      phase:
        row.phase_name && row.phase_data_json
          ? {
              name: row.phase_name,
              data: decodeJson<JsonObject>(row.phase_data_json, {}),
            }
          : undefined,
      output: decodeJson<JsonValue | undefined>(row.output_json, undefined),
      error: decodeJson<SerializedError | undefined>(row.error_json, undefined),
      cancelReason: row.cancel_reason ?? undefined,
      waits: decodeJson<DurableWait[]>(row.waits_json, []),
      effects,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      parent:
        row.parent_workflow_id && row.parent_run_id && row.parent_child_record_id
          ? {
              workflowId: row.parent_workflow_id,
              runId: row.parent_run_id,
              childRecordId: row.parent_child_record_id,
            }
          : undefined,
    }
  }

  private readyCandidates(input: ClaimReadyActivationInput): ReadyCandidate[] {
    const placeholders = input.shardIds.map(() => "?").join(", ")
    const rows = this.db
      .prepare(
        `
        SELECT * FROM instances
        WHERE status = 'running' AND partition_shard IN (${placeholders})
        ORDER BY updated_at, workflow_id, run_id
      `,
      )
      .all(...input.shardIds)
      .map((row) => requireRow<InstanceRow>(row))

    const candidates: ReadyCandidate[] = []

    for (const instance of rows) {
      const workflow = input.workflows[instance.workflow_name]
      if (!workflow) {
        continue
      }

      if (instance.workflow_version < workflow.version) {
        candidates.push({
          kind: "migration",
          activationId: activationIdFromParts(
            instance.workflow_id,
            instance.run_id,
            instance.sequence,
            "migration",
            `${instance.workflow_version}->${workflow.version}`,
          ),
        workflowName: instance.workflow_name,
        workflowId: instance.workflow_id,
        runId: instance.run_id,
        sequence: instance.sequence,
        activationTime: instance.updated_at,
        leaseUntil: "",
        sort: [instance.updated_at, "migration", instance.workflow_id, instance.run_id],
      })
        continue
      }

      if (instance.workflow_version > workflow.version) {
        continue
      }

      for (const wait of decodeJson<DurableWait[]>(instance.waits_json, [])) {
        if (wait.kind === "run") {
          if (wait.readyAt > input.now) {
            continue
          }
          candidates.push({
            kind: "run",
            activationId: activationIdFromParts(
              instance.workflow_id,
              instance.run_id,
              instance.sequence,
              "run",
              wait.name,
            ),
            workflowName: instance.workflow_name,
            workflowId: instance.workflow_id,
            runId: instance.run_id,
            sequence: instance.sequence,
            activationTime: wait.readyAt,
            leaseUntil: "",
            sort: [wait.readyAt, "run", wait.name, instance.workflow_id, instance.run_id],
          })
        } else if (wait.kind === "signal") {
          const signalRow = oneRow<SignalRow>(
            this.db
              .prepare(
                `
                SELECT * FROM signals
                WHERE workflow_id = ? AND run_id = ? AND type = ? AND consumed_by_sequence IS NULL
                ORDER BY received_at, type, signal_id
                LIMIT 1
              `,
              )
              .get(instance.workflow_id, instance.run_id, wait.type),
          )

          if (!signalRow) {
            continue
          }

          candidates.push({
            kind: "event",
            activationId: activationIdFromParts(
              instance.workflow_id,
              instance.run_id,
              instance.sequence,
              "signal",
              signalRow.signal_id,
            ),
            workflowName: instance.workflow_name,
            workflowId: instance.workflow_id,
            runId: instance.run_id,
            sequence: instance.sequence,
            activationTime: signalRow.received_at,
            waitName: wait.name,
            wait,
            event: {
              kind: "signal",
              signalId: signalRow.signal_id,
              payload: decodeJson<JsonValue>(signalRow.payload_json, null),
              occurredAt: signalRow.received_at,
              consumeSignalId: signalRow.signal_id,
            },
            leaseUntil: "",
            sort: [signalRow.received_at, "signal", wait.name, signalRow.signal_id],
          })
        } else if (wait.kind === "timer") {
          if (wait.fireAt > input.now) {
            continue
          }

          candidates.push({
            kind: "event",
            activationId: activationIdFromParts(
              instance.workflow_id,
              instance.run_id,
              instance.sequence,
              "timer",
              `${wait.name}:${wait.fireAt}`,
            ),
            workflowName: instance.workflow_name,
            workflowId: instance.workflow_id,
            runId: instance.run_id,
            sequence: instance.sequence,
            activationTime: wait.fireAt,
            waitName: wait.name,
            wait,
            event: {
              kind: "timer",
              firedAt: wait.fireAt,
              occurredAt: wait.fireAt,
            },
            leaseUntil: "",
            sort: [wait.fireAt, "timer", wait.name, `${wait.name}:${wait.fireAt}`],
          })
        } else {
          const childRow = oneRow<ChildRow>(
            this.db
              .prepare(
                `
                SELECT * FROM children
                WHERE workflow_id = ? AND run_id = ? AND status <> 'started' AND delivered_by_sequence IS NULL
                ORDER BY completed_at, child_record_id
                LIMIT 1
              `,
              )
              .get(wait.workflowId, wait.runId),
          )

          if (!childRow) {
            continue
          }

          const occurredAt = childRow.completed_at ?? input.now
          candidates.push({
            kind: "event",
            activationId: activationIdFromParts(
              instance.workflow_id,
              instance.run_id,
              instance.sequence,
              "child",
              childRow.child_record_id,
            ),
            workflowName: instance.workflow_name,
            workflowId: instance.workflow_id,
            runId: instance.run_id,
            sequence: instance.sequence,
            activationTime: occurredAt,
            waitName: wait.name,
            wait,
            event: {
              kind: "child",
              childRecordId: childRow.child_record_id,
              occurredAt,
              event:
                childRow.status === "completed"
                  ? { ok: true, output: decodeJson<JsonValue>(childRow.output_json, null) }
                  : {
                      ok: false,
                      error: decodeJson<SerializedError>(childRow.error_json, {
                        message: "Child failed",
                      }),
                    },
            },
            leaseUntil: "",
            sort: [occurredAt, "child", wait.name, childRow.child_record_id],
          })
        }
      }
    }

    return candidates
  }

  private tryWriteActivationClaim(
    candidate: ReadyCandidate,
    workerId: string,
    now: string,
    leaseMs: number,
  ): boolean {
    const leaseUntil = addMs(now, leaseMs)
    const result = this.db
      .prepare(
        `
        INSERT INTO activation_claims (
          activation_id, workflow_id, run_id, sequence, kind, wait_name,
          event_json, wait_json, owner_id, lease_until, activation_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(activation_id) DO UPDATE SET
          owner_id = excluded.owner_id,
          lease_until = excluded.lease_until,
          event_json = excluded.event_json,
          wait_json = excluded.wait_json,
          activation_time = COALESCE(activation_claims.activation_time, excluded.activation_time)
        WHERE activation_claims.completed_by_sequence IS NULL
          AND (
            activation_claims.owner_id IS NULL
            OR activation_claims.owner_id = excluded.owner_id
            OR activation_claims.lease_until IS NULL
            OR activation_claims.lease_until <= ?
          )
      `,
      )
      .run(
        candidate.activationId,
        candidate.workflowId,
        candidate.runId,
        candidate.sequence,
        candidate.kind,
        candidate.kind === "event" ? candidate.waitName : null,
        candidate.kind === "event" ? encodeJson(candidate.event) : null,
        candidate.kind === "event" ? encodeJson(candidate.wait) : null,
        workerId,
        leaseUntil,
        candidate.activationTime,
        now,
      )
    if (result.changes === 0) {
      return false
    }
    candidate.leaseUntil = leaseUntil
    return true
  }

  private writeNextInstance(input: CommitCheckpointInput, nextSequence: number): void {
    if (input.next.status === "running") {
      this.db
        .prepare(
          `
          UPDATE instances
          SET workflow_version = ?, sequence = ?, status = 'running',
            common_json = ?, phase_name = ?, phase_data_json = ?, output_json = NULL,
            error_json = NULL, cancel_reason = NULL, waits_json = ?, updated_at = ?
          WHERE workflow_id = ? AND run_id = ?
        `,
        )
        .run(
          input.workflowVersion,
          nextSequence,
          encodeJson(input.next.common),
          input.next.phase.name,
          encodeJson(input.next.phase.data),
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        )
      return
    }

    if (input.next.status === "completed") {
      this.db
        .prepare(
          `
          UPDATE instances
          SET workflow_version = ?, sequence = ?, status = 'completed',
            common_json = NULL, phase_name = NULL, phase_data_json = NULL, output_json = ?,
            error_json = NULL, cancel_reason = NULL, waits_json = ?, updated_at = ?
          WHERE workflow_id = ? AND run_id = ?
        `,
        )
        .run(
          input.workflowVersion,
          nextSequence,
          encodeJson(toJson(input.next.output)),
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        )
      return
    }

    if (input.next.status === "canceled") {
      this.db
        .prepare(
          `
          UPDATE instances
          SET workflow_version = ?, sequence = ?, status = 'canceled',
            common_json = NULL, phase_name = NULL, phase_data_json = NULL, output_json = NULL,
            error_json = NULL, cancel_reason = ?, waits_json = ?, updated_at = ?
          WHERE workflow_id = ? AND run_id = ?
        `,
        )
        .run(
          input.workflowVersion,
          nextSequence,
          input.next.reason,
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        )
      return
    }

    this.db
      .prepare(
        `
        UPDATE instances
        SET workflow_version = ?, sequence = ?, status = 'failed',
          common_json = NULL, phase_name = NULL, phase_data_json = NULL, output_json = NULL,
          error_json = ?, cancel_reason = NULL, waits_json = ?, updated_at = ?
        WHERE workflow_id = ? AND run_id = ?
      `,
      )
      .run(
        input.workflowVersion,
        nextSequence,
        encodeJson(input.next.error),
        encodeJson(input.waits),
        input.now,
        input.workflowId,
        input.runId,
      )
  }

  private updateParentChildRecord(
    previous: InstanceRow,
    input: CommitCheckpointInput,
    nextSequence: number,
  ): void {
    if (!previous.parent_child_record_id || input.next.status === "running") {
      return
    }

    const existing = oneRow<ChildRow>(
      this.db
        .prepare("SELECT * FROM children WHERE child_record_id = ? AND status = 'started'")
        .get(previous.parent_child_record_id),
    )
    if (!existing) {
      return
    }

    if (input.next.status === "completed") {
      this.db
        .prepare(
          `
          UPDATE children
          SET status = 'completed', completed_at = ?, output_json = ?, error_json = NULL
          WHERE child_record_id = ?
        `,
        )
        .run(input.now, encodeJson(toJson(input.next.output)), existing.child_record_id)
    } else {
      const error =
        input.next.status === "failed"
          ? input.next.error
          : { message: input.next.reason || "Child canceled" }
      this.db
        .prepare(
          `
          UPDATE children
          SET status = 'failed', completed_at = ?, output_json = NULL, error_json = ?
          WHERE child_record_id = ?
        `,
        )
        .run(input.now, encodeJson(error), existing.child_record_id)
    }

    void nextSequence
  }

  private deleteInstanceRecords(workflowId: string, runId: string): void {
    this.db.prepare("DELETE FROM instances WHERE workflow_id = ? AND run_id = ?").run(workflowId, runId)
  }
}

function childHandle(record: ChildRecord): ChildHandle {
  return {
    workflowName: record.workflowName,
    workflowVersion: record.workflowVersion,
    workflowId: record.workflowId,
    runId: record.runId,
  }
}

function activationIdFromParts(
  workflowId: string,
  runId: string,
  sequence: number,
  kind: string,
  eventId: string,
): string {
  return `${workflowId}/${runId}/${sequence}/${kind}/${eventId}`
}

function compareCandidates(left: ReadyCandidate, right: ReadyCandidate): number {
  return compareStringArrays(left.sort, right.sort)
}

function compareStringArrays(left: string[], right: string[]): number {
  const length = Math.max(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    if (left[index] === undefined) {
      return -1
    }
    if (right[index] === undefined) {
      return 1
    }
    const compared = left[index].localeCompare(right[index])
    if (compared !== 0) {
      return compared
    }
  }
  return 0
}

function stripSort(candidate: ReadyCandidate): ClaimedActivation {
  const { sort, ...activation } = candidate
  return activation
}

function addMs(iso: string, ms: number): string {
  return new Date(new Date(iso).getTime() + ms).toISOString()
}

function encodeJson(value: unknown): string {
  return JSON.stringify(toJson(value))
}

function decodeJson<T>(raw: unknown, fallback: T): T {
  if (typeof raw !== "string") {
    return fallback
  }
  return JSON.parse(raw) as T
}

function oneRow<T>(value: unknown): T | null {
  if (!isPlainObject(value)) {
    return null
  }
  return value as T
}

function requireRow<T>(value: unknown): T {
  const row = oneRow<T>(value)
  if (!row) {
    throw new Error("Expected SQLite row")
  }
  return row
}

function rowToSignalRecord(row: SignalRow): SignalRecord {
  return {
    signalId: row.signal_id,
    workflowId: row.workflow_id,
    runId: row.run_id,
    type: row.type,
    payload: decodeJson<JsonValue>(row.payload_json, null),
    receivedAt: row.received_at,
    consumedBySequence: row.consumed_by_sequence ?? undefined,
  }
}

function rowToChildRecord(row: ChildRow): ChildRecord {
  return {
    childRecordId: row.child_record_id,
    parentWorkflowId: row.parent_workflow_id,
    parentRunId: row.parent_run_id,
    activationId: row.activation_id,
    key: row.key,
    workflowName: row.workflow_name,
    workflowVersion: row.workflow_version,
    workflowId: row.workflow_id,
    runId: row.run_id,
    status: row.status,
    completedAt: row.completed_at ?? undefined,
    output: decodeJson<JsonValue | undefined>(row.output_json, undefined),
    error: decodeJson<SerializedError | undefined>(row.error_json, undefined),
    deliveredBySequence: row.delivered_by_sequence ?? undefined,
  }
}

function rowToEffectRecord(row: EffectRow): EffectRecord {
  return {
    effectId: row.effect_id,
    activationId: row.activation_id,
    key: row.key,
    idempotencyKey: row.idempotency_key,
    status: row.status,
    result: decodeJson<JsonValue | undefined>(row.result_json, undefined),
    error: decodeJson<SerializedError | undefined>(row.error_json, undefined),
    heartbeatAt: row.heartbeat_at ?? undefined,
    heartbeatDetails: decodeJson<JsonValue | undefined>(row.heartbeat_details_json, undefined),
  }
}

// Make it easy to create a store path before the first provider write.
export function ensureStoreDir(filePath: string): void {
  if (filePath !== ":memory:") {
    mkdirSync(dirname(filePath), { recursive: true })
  }
}
