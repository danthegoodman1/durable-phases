import { randomUUID } from "node:crypto"
import pg from "pg"
import type {
  ActivationInstanceSnapshot,
  AppendSignalInput,
  CancelChildInput,
  ChildRecord,
  ClaimDispatchShardInput,
  ClaimedActivation,
  ClaimedActivationWithInstance,
  ClaimReadyActivationsInput,
  ClaimReadyActivationsResult,
  ClaimReadyActivationInput,
  ClaimReadyActivationResult,
  CheckpointChildStart,
  CheckpointEffectMutation,
  CommitActivationInput,
  CommitActivationsResult,
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
  FailEffectResult,
  HeartbeatActivationsInput,
  HeartbeatActivationInput,
  HeartbeatDispatchShardInput,
  HeartbeatEffectInput,
  LoadInstanceOptions,
  PersistedInstance,
  ReadyEvent,
  RecordActivationFailureInput,
  ReleaseActivationsInput,
  ReleaseActivationInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
  SignalRecord,
} from "./interface.js"
import type {
  ChildHandle,
  InstanceRef,
  JsonObject,
  JsonValue,
  SerializedError,
} from "./workflow.js"
import type { DurableMetricTags, DurableObservability } from "./observability.js"
import { countDurable, gaugeDurable, logDurable } from "./observability.js"
import { clone, toJson } from "./workflow.js"

const { Pool } = pg
type Pool = pg.Pool
type PoolClient = pg.PoolClient
type Queryable = Pool | PoolClient

export type PostgresDurabilityProviderOptions = DurableObservability & {
  connectionString?: string
  pool?: Pool
  schema?: string
  poolSize?: number
  statementTimeoutMs?: number
  lockTimeoutMs?: number
}

type InstanceRow = {
  workflow_name: string
  workflow_version: number
  workflow_id: string
  run_id: string
  partition_shard: number
  sequence: number
  status: "running" | "completed" | "canceled" | "failed"
  common_json: unknown
  phase_name: string | null
  phase_data_json: unknown
  output_json: unknown
  error_json: unknown
  cancel_reason: string | null
  waits_json: unknown
  parent_workflow_id: string | null
  parent_run_id: string | null
  parent_child_record_id: string | null
  created_at: Date | string
  updated_at: Date | string
}

type SignalRow = {
  signal_id: string
  workflow_id: string
  run_id: string
  type: string
  payload_json: unknown
  received_at: Date | string
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
  status: "started" | "completed" | "failed" | "abandoned"
  parent_close_policy: "cancel" | "abandon"
  completed_at: Date | string | null
  output_json: unknown
  error_json: unknown
  delivered_by_sequence: number | null
}

type EffectRow = {
  effect_id: string
  workflow_id: string
  run_id: string
  activation_id: string
  key: string
  idempotency_key: string
  status: "pending" | "completed" | "failed"
  attempt: number | null
  attempt_id: string | null
  attempt_owner_id: string | null
  attempt_started_at: Date | string | null
  start_to_close_timeout_ms: number | null
  start_to_close_deadline: Date | string | null
  heartbeat_timeout_ms: number | null
  heartbeat_deadline: Date | string | null
  max_attempts: number | null
  max_elapsed_ms: number | null
  initial_interval_ms: number | null
  max_interval_ms: number | null
  backoff_coefficient: number | string | null
  first_attempt_started_at: Date | string | null
  next_attempt_at: Date | string | null
  last_failure_json: unknown
  non_retryable_error_names_json: unknown
  timed_out_at: Date | string | null
  timeout_kind: "heartbeat" | "start_to_close" | null
  result_json: unknown
  error_json: unknown
  heartbeat_at: Date | string | null
  heartbeat_details_json: unknown
}

type ActivationClaimRow = {
  activation_id: string
  workflow_id: string
  run_id: string
  sequence: number
  kind: "migration" | "run" | "event" | "signal" | "timer" | "child"
  wait_name: string | null
  event_json: unknown
  wait_json: unknown
  owner_id: string | null
  lease_until: Date | string | null
  activation_time: Date | string | null
  completed_by_sequence: number | null
}

type ActivationClaimUpsertRow = ActivationClaimRow & {
  inserted: boolean
}

type CommitClaimRow = InstanceRow & {
  claim_activation_id: string | null
  claim_workflow_id: string | null
  claim_run_id: string | null
  claim_sequence: number | null
  claim_kind: ActivationClaimRow["kind"] | null
  claim_wait_name: string | null
  claim_event_json: unknown
  claim_wait_json: unknown
  claim_owner_id: string | null
  claim_lease_until: Date | string | null
  claim_activation_time: Date | string | null
  claim_completed_by_sequence: number | null
}

type IndexedInstanceRow = InstanceRow & {
  input_index: number
}

type IndexedActivationClaimRow = ActivationClaimRow & {
  input_index: number
}

type IndexedSignalRow = SignalRow & {
  input_index: number
}

type IndexedChildRow = ChildRow & {
  input_index: number
}

type IndexedChildStartExistingRow<T> = T & {
  input_index: number
  start_index: number
}

type DispatchShardRow = {
  shard_id: number
  owner_id: string | null
  lease_until: Date | string | null
}

type ActivityDeadlineRow = {
  effect_id: string
  activation_id: string
}

type ReadyEventRow = {
  ready_event_id: string
  workflow_id: string
  run_id: string
  workflow_name: string
  workflow_version: number
  partition_shard: number
  sequence: number
  kind: "migration" | "run" | "signal" | "timer" | "child"
  wait_name: string | null
  activation_id: string | null
  ready_at: Date | string
  sort_key: string
  wait_json: unknown
  event_json: unknown
}

type ReadyEventJoinedRow = ReadyEventRow & {
  instance_workflow_name: string
  instance_workflow_version: number
  instance_workflow_id: string
  instance_run_id: string
  instance_partition_shard: number
  instance_sequence: number
  instance_status: "running" | "completed" | "canceled" | "failed"
  instance_common_json: unknown
  instance_phase_name: string | null
  instance_phase_data_json: unknown
  instance_output_json: unknown
  instance_error_json: unknown
  instance_cancel_reason: string | null
  instance_waits_json: unknown
  instance_parent_workflow_id: string | null
  instance_parent_run_id: string | null
  instance_parent_child_record_id: string | null
  instance_created_at: Date | string
  instance_updated_at: Date | string
}

type ReadyCandidate = {
  kind: "migration" | "run" | "event"
  activationId: string
  workflowName: string
  workflowId: string
  runId: string
  sequence: number
  activationTime: string
  leaseUntil: string
  sort: string[]
  instance: InstanceRow
  waitName?: string
  eventKind?: ReadyEvent["kind"]
  waitJson?: unknown
  eventJson?: unknown
}

type ReadyEventInstanceState = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  partitionShard: number
  sequence: number
  status: InstanceRow["status"]
  waits: DurableWait[]
  updatedAt: string
}

type ReadyEventInsert = {
  readyEventId: string
  workflowId: string
  runId: string
  workflowName: string
  workflowVersion: number
  partitionShard: number
  sequence: number
  kind: ReadyEventRow["kind"]
  waitName: string | null
  activationId: string | null
  readyAt: string
  sortKey: string
  wait: DurableWait | null
  event: ReadyEvent | null
}

export class PostgresDurabilityProvider implements DurabilityProvider {
  private readonly pool: Pool
  private readonly ownsPool: boolean
  private readonly schema: string
  private readonly statementTimeoutMs: number
  private readonly lockTimeoutMs: number
  private readonly observability: DurableObservability
  private closed = false

  private constructor(options: PostgresDurabilityProviderOptions = {}) {
    this.schema = normalizeSchemaName(options.schema ?? "durable")
    this.statementTimeoutMs = positiveInteger(
      options.statementTimeoutMs ?? 30_000,
      "statementTimeoutMs",
    )
    this.lockTimeoutMs = positiveInteger(options.lockTimeoutMs ?? 5_000, "lockTimeoutMs")
    this.observability = { logger: options.logger, metrics: options.metrics }
    if (options.pool) {
      this.pool = options.pool
      this.ownsPool = false
    } else {
      this.pool = new Pool({
        connectionString:
          options.connectionString ??
          "postgresql://durable:durable@127.0.0.1:55432/durable",
        max: options.poolSize ?? 16,
      })
      this.ownsPool = true
    }
  }

  static async create(
    options: PostgresDurabilityProviderOptions = {},
  ): Promise<PostgresDurabilityProvider> {
    const provider = new PostgresDurabilityProvider(options)
    try {
      await provider.migrate()
      return provider
    } catch (error) {
      await provider.close()
      throw error
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true
    if (this.ownsPool) {
      await this.pool.end()
    }
  }

  async createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    return this.transaction(async (client) => {
      const existing = await this.instanceRow(client, input)
      const conflictPolicy = input.conflictPolicy ?? "fail"

      if (existing && conflictPolicy === "use_existing") {
        return { workflowId: existing.workflow_id, runId: existing.run_id }
      }
      if (existing && conflictPolicy === "fail") {
        throw new Error(`Workflow instance already exists: ${input.workflowId}/${input.runId}`)
      }
      if (existing && conflictPolicy === "terminate_existing") {
        await this.deleteInstanceRecords(client, input.workflowId, input.runId)
      }

      await client.query(
        `
        INSERT INTO ${this.table("instances")} (
          workflow_name, workflow_version, workflow_id, run_id, partition_shard,
          sequence, status, common_json, phase_name, phase_data_json, output_json,
          error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
          parent_child_record_id, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, 0, 'running', $6::jsonb, $7, $8::jsonb,
          NULL, NULL, NULL, $9::jsonb, $10, $11, $12, $13::timestamptz, $13::timestamptz
        )
        `,
        [
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
        ],
      )

      await this.insertReadyEventsForState(client, {
        workflowName: input.workflowName,
        workflowVersion: input.workflowVersion,
        workflowId: input.workflowId,
        runId: input.runId,
        partitionShard: input.partitionShard,
        sequence: 0,
        status: "running",
        waits: input.waits,
        updatedAt: input.now,
      })

      this.log("info", "provider.instance.create", {
        workflowName: input.workflowName,
        workflowId: input.workflowId,
        runId: input.runId,
        partitionShard: input.partitionShard,
      })
      this.count("durable.provider.instance", {
        workflowName: input.workflowName,
        shardId: input.partitionShard,
        status: "created",
      })
      return { workflowId: input.workflowId, runId: input.runId }
    })
  }

  async loadInstance(ref: InstanceRef, options: LoadInstanceOptions = {}): Promise<PersistedInstance | null> {
    const row = await this.instanceRow(this.pool, ref)
    return row ? await this.persistedInstance(this.pool, row, options) : null
  }

  async listInstances(options: LoadInstanceOptions = {}): Promise<PersistedInstance[]> {
    const rows = await this.rows<InstanceRow>(
      this.pool,
      `SELECT * FROM ${this.table("instances")} ORDER BY workflow_id, run_id`,
    )
    return Promise.all(rows.map((row) => this.persistedInstance(this.pool, row, options)))
  }

  async listSignals(): Promise<SignalRecord[]> {
    const rows = await this.rows<SignalRow>(
      this.pool,
      `SELECT * FROM ${this.table("signals")} ORDER BY received_at, signal_id`,
    )
    return rows.map(rowToSignalRecord)
  }

  async listChildren(): Promise<ChildRecord[]> {
    const rows = await this.rows<ChildRow>(
      this.pool,
      `SELECT * FROM ${this.table("children")} ORDER BY child_record_id`,
    )
    return rows.map(rowToChildRecord)
  }

  async listActivationClaims(): Promise<
    Array<{
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      kind: ActivationClaimRow["kind"]
      ownerId?: string
      completedBySequence?: number
    }>
  > {
    const rows = await this.rows<ActivationClaimRow>(
      this.pool,
      `SELECT * FROM ${this.table("activation_tasks")} ORDER BY workflow_id, run_id, sequence, activation_id`,
    )
    return rows.map((claim) => ({
      activationId: claim.activation_id,
      workflowId: claim.workflow_id,
      runId: claim.run_id,
      sequence: claim.sequence,
      kind: claim.kind,
      ownerId: claim.owner_id ?? undefined,
      completedBySequence: claim.completed_by_sequence ?? undefined,
    }))
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.transaction(async (client) => {
      const instance = await this.instanceRow(client, input)
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
      await client.query(
        `
        INSERT INTO ${this.table("signals")}
          (signal_id, workflow_id, run_id, type, payload_json, received_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::timestamptz)
        `,
        [
          signal.signalId,
          signal.workflowId,
          signal.runId,
          signal.type,
          encodeJson(signal.payload),
          signal.receivedAt,
        ],
      )

      await this.replaceSignalReadyEventsForState(client, readyEventStateFromInstanceRow(instance))
      this.log("info", "provider.signal.append", {
        workflowId: input.workflowId,
        runId: input.runId,
        signalId: signal.signalId,
        type: input.type,
      })
      this.count("durable.provider.signal", { status: "appended" })
      return signal
    })
  }

  async claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    return this.transaction(async (client) => {
      const leaseUntil = addMs(input.now, input.leaseMs)
      const row = await this.one<DispatchShardRow>(
        client,
        `
        INSERT INTO ${this.table("dispatch_shards")} (shard_id, owner_id, lease_until)
        VALUES ($1, $2, $3::timestamptz)
        ON CONFLICT (shard_id) DO UPDATE SET
          owner_id = EXCLUDED.owner_id,
          lease_until = EXCLUDED.lease_until
        WHERE ${this.table("dispatch_shards")}.owner_id IS NULL
          OR ${this.table("dispatch_shards")}.owner_id = EXCLUDED.owner_id
          OR ${this.table("dispatch_shards")}.lease_until IS NULL
          OR ${this.table("dispatch_shards")}.lease_until <= $4::timestamptz
        RETURNING *
        `,
        [input.shardId, input.ownerId, leaseUntil, input.now],
      )
      if (!row) {
        this.log("debug", "provider.shard.claim_miss", {
          workerId: input.ownerId,
          shardId: input.shardId,
        })
        this.count("durable.provider.shard.claim", {
          workerId: input.ownerId,
          shardId: input.shardId,
          status: "miss",
        })
        return null
      }
      this.log("debug", "provider.shard.claim", {
        workerId: input.ownerId,
        shardId: input.shardId,
        leaseUntil,
      })
      this.count("durable.provider.shard.claim", {
        workerId: input.ownerId,
        shardId: input.shardId,
        status: "success",
      })
      return { shardId: input.shardId, ownerId: input.ownerId, leaseUntil }
    })
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    const result = await this.pool.query(
      `
      UPDATE ${this.table("dispatch_shards")}
      SET lease_until = $1::timestamptz
      WHERE shard_id = $2 AND owner_id = $3 AND lease_until >= $4::timestamptz
      `,
      [addMs(input.now, input.leaseMs), input.shardId, input.ownerId, input.now],
    )
    if (result.rowCount === 0) {
      this.log("warn", "provider.shard.heartbeat_failed", {
        workerId: input.ownerId,
        shardId: input.shardId,
      })
      this.count("durable.provider.shard.heartbeat", {
        workerId: input.ownerId,
        shardId: input.shardId,
        status: "failed",
      })
      throw new Error(`Lost dispatch shard lease: ${input.shardId}`)
    }
    this.log("debug", "provider.shard.heartbeat", {
      workerId: input.ownerId,
      shardId: input.shardId,
    })
    this.count("durable.provider.shard.heartbeat", {
      workerId: input.ownerId,
      shardId: input.shardId,
      status: "success",
    })
  }

  async releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    await this.pool.query(
      `
      UPDATE ${this.table("dispatch_shards")}
      SET owner_id = NULL, lease_until = NULL
      WHERE shard_id = $1 AND owner_id = $2
      `,
      [input.shardId, input.ownerId],
    )
    this.log("debug", "provider.shard.release", {
      workerId: input.ownerId,
      shardId: input.shardId,
    })
    this.count("durable.provider.shard.release", {
      workerId: input.ownerId,
      shardId: input.shardId,
      status: "released",
    })
  }

  async claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult> {
    const limit = positiveInteger(input.limit, "claimReadyActivations limit")
    return this.transaction(async (client) => {
      const ownedShards = (
        await this.rows<Pick<DispatchShardRow, "shard_id">>(
          client,
          `
          SELECT shard_id
          FROM ${this.table("dispatch_shards")}
          WHERE shard_id = ANY($1::int[])
            AND owner_id = $2
            AND lease_until >= $3::timestamptz
          ORDER BY shard_id
          FOR UPDATE
          `,
          [input.shardIds, input.workerId, input.now],
        )
      ).map((row) => row.shard_id)

      if (ownedShards.length === 0) {
        this.log("debug", "provider.activation.claim_miss", {
          workerId: input.workerId,
          reason: "no_owned_shards",
        })
        this.count("durable.provider.activation.claim", {
          workerId: input.workerId,
          status: "miss",
          reason: "no_owned_shards",
        })
        return { claims: [] }
      }

      await this.expireActivityTimeouts(client, input.now, { shardIds: ownedShards })
      const candidates = await this.readyCandidates(
        client,
        { ...input, shardIds: ownedShards },
        readyCandidateLimit(limit),
      )
      const claims: ClaimedActivationWithInstance[] = []
      const claimedSequences = new Set<string>()
      const claimableCandidates: ReadyCandidate[] = []
      for (const candidate of candidates) {
        if (claimableCandidates.length >= limit) {
          break
        }
        const sequenceKey = `${candidate.workflowId}\0${candidate.runId}\0${candidate.sequence}`
        if (claimedSequences.has(sequenceKey)) {
          continue
        }
        claimedSequences.add(sequenceKey)
        claimableCandidates.push(candidate)
      }

      const claimedActivationIds = await this.writeActivationClaims(
        client,
        claimableCandidates,
        input.workerId,
        input.now,
        input.leaseMs,
      )
      for (const candidate of claimableCandidates) {
        if (!claimedActivationIds.has(candidate.activationId)) {
          continue
        }
        if (candidate.instance.status !== "running" || candidate.instance.sequence !== candidate.sequence) {
          continue
        }
        claims.push({
          activation: stripCandidateMetadata(candidate),
          instance: this.activationInstanceSnapshot(candidate.instance),
          effects: [],
        })
      }

      const effectsByActivation = await this.effectsForActivations(
        client,
        claims.map((claim) => claim.activation.activationId),
      )
      for (const claim of claims) {
        claim.effects = effectsByActivation.get(claim.activation.activationId) ?? []
      }

      if (claims.length >= limit) {
        return { claims }
      }

      const nextWakeAt = await this.nextWakeAt(client, ownedShards, input.now, input.workflows)
      if (claims.length === 0) {
        this.log("debug", "provider.activation.claim_miss", {
          workerId: input.workerId,
          nextWakeAt,
        })
        this.count("durable.provider.activation.claim", {
          workerId: input.workerId,
          status: "miss",
        })
      }
      if (nextWakeAt) {
        this.gauge("durable.provider.next_wake", new Date(nextWakeAt).getTime(), {
          workerId: input.workerId,
          status: "scheduled",
        })
      }
      return nextWakeAt ? { claims, nextWakeAt } : { claims }
    })
  }

  async claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult> {
    const result = await this.claimReadyActivations({ ...input, limit: 1 })
    const first = result.claims[0]
    if (!first) {
      return result.nextWakeAt
        ? { activation: null, nextWakeAt: result.nextWakeAt }
        : { activation: null }
    }
    return { activation: first.activation, instance: first.instance, effects: first.effects }
  }

  async heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    const activationIds = [...new Set(input.activationIds)]
    if (activationIds.length === 0) {
      return
    }
    const result = await this.transaction(async (client) => {
      await this.expireActivityTimeouts(client, input.now, { activationIds })
      return client.query(
        `
        UPDATE ${this.table("activation_tasks")}
        SET lease_until = $1::timestamptz
        WHERE activation_id = ANY($2::text[]) AND owner_id = $3 AND completed_by_sequence IS NULL
          AND lease_until >= $4::timestamptz
        `,
        [addMs(input.now, input.leaseMs), activationIds, input.workerId, input.now],
      )
    })
    if (result.rowCount !== activationIds.length) {
      this.log("warn", "provider.activation.heartbeat_failed", {
        workerId: input.workerId,
        activationIds,
      })
      this.count("durable.provider.activation.heartbeat", {
        workerId: input.workerId,
        status: "failed",
      })
      throw new Error(`Lost activation lease: ${activationIds.join(", ")}`)
    }
    this.log("debug", "provider.activation.heartbeat", {
      workerId: input.workerId,
      activationIds,
    })
    this.count("durable.provider.activation.heartbeat", {
      workerId: input.workerId,
      status: "success",
    })
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
    const activationIds = [...new Set(input.activationIds)]
    if (activationIds.length === 0) {
      return
    }
    await this.pool.query(
      `
      UPDATE ${this.table("activation_tasks")}
      SET owner_id = NULL, lease_until = NULL
      WHERE activation_id = ANY($1::text[]) AND owner_id = $2 AND completed_by_sequence IS NULL
      `,
      [activationIds, input.workerId],
    )
    this.log("debug", "provider.activation.release", {
      workerId: input.workerId,
      activationIds,
    })
    this.count("durable.provider.activation.release", {
      workerId: input.workerId,
      status: "released",
    })
  }

  async releaseActivation(input: ReleaseActivationInput): Promise<void> {
    await this.releaseActivations({
      activationIds: [input.activationId],
      workerId: input.workerId,
    })
  }

  async getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.transaction(async (client) => {
      const options = normalizeEffectOptions(input)
      const effectId = `effect-${randomUUID()}`
      const attemptId = `attempt-${randomUUID()}`
      const idempotencyKey = `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`
      const inserted = await this.one<EffectRow>(
        client,
        `
        INSERT INTO ${this.table("effects")} (
          effect_id, workflow_id, run_id, activation_id, key, idempotency_key, status,
          attempt, attempt_id, attempt_owner_id, attempt_started_at,
          start_to_close_timeout_ms, start_to_close_deadline,
          heartbeat_timeout_ms, heartbeat_deadline, max_attempts,
          max_elapsed_ms, initial_interval_ms, max_interval_ms, backoff_coefficient,
          first_attempt_started_at, next_attempt_at, non_retryable_error_names_json
        )
        SELECT
          $1, $2, $3, $4, $5, $6, 'pending',
          1, $7, $8, $9::timestamptz,
          $10, $11::timestamptz, $12, $13::timestamptz, $14, $15, $16, $17, $18,
          $9::timestamptz, NULL, $19::jsonb
        WHERE EXISTS (
          SELECT 1 FROM ${this.table("activation_tasks")} a
          WHERE a.activation_id = $4
            AND a.workflow_id = $2
            AND a.run_id = $3
            AND a.owner_id = $8
            AND a.completed_by_sequence IS NULL
            AND a.lease_until >= $9::timestamptz
            AND NOT EXISTS (
              SELECT 1 FROM ${this.table("activity_deadlines")} d
              WHERE d.activation_id = $4 AND d.deadline_at <= $9::timestamptz
            )
        )
        ON CONFLICT (workflow_id, run_id, activation_id, key) DO NOTHING
        RETURNING *
        `,
        [
          effectId,
          input.workflowId,
          input.runId,
          input.activationId,
          input.key,
          idempotencyKey,
          attemptId,
          input.workerId,
          input.now,
          options.startToCloseTimeoutMs,
          deadlineFrom(input.now, options.startToCloseTimeoutMs),
          options.heartbeatTimeoutMs,
          deadlineFrom(input.now, options.heartbeatTimeoutMs),
          options.maxAttempts,
          options.maxElapsedMs,
          options.initialIntervalMs,
          options.maxIntervalMs,
          options.backoffCoefficient,
          encodeJson(options.nonRetryableErrorNames),
        ],
      )
      if (inserted) {
        await this.syncActivityDeadline(client, inserted)
        this.log("debug", "provider.effect.reserve", {
          workerId: input.workerId,
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          effectId: inserted.effect_id,
          attemptId: inserted.attempt_id,
          key: input.key,
          attempt: inserted.attempt ?? 1,
        })
        this.count("durable.provider.effect", { workerId: input.workerId, status: "reserved" })
        return {
          status: "reserved",
          effectId: inserted.effect_id,
          idempotencyKey: inserted.idempotency_key,
          attempt: inserted.attempt ?? 1,
          attemptId: requireAttemptId(inserted),
        } satisfies EffectReservation
      }

      await this.assertLiveActivationLease(client, input)
      const existing = await this.effectRow(client, input)

      if (existing?.status === "completed") {
        this.log("debug", "provider.effect.memoized", {
          workerId: input.workerId,
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          effectId: existing.effect_id,
          key: input.key,
        })
        this.count("durable.provider.effect", { workerId: input.workerId, status: "memoized" })
        return {
          status: "completed",
          result: decodeJson<JsonValue>(existing.result_json, null),
        } satisfies EffectReservation
      }

      if (existing?.status === "failed") {
        this.log("debug", "provider.effect.failed_memoized", {
          workerId: input.workerId,
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          effectId: existing.effect_id,
          key: input.key,
        })
        this.count("durable.provider.effect", { workerId: input.workerId, status: "failed" })
        return {
          status: "failed",
          error: decodeJson<SerializedError>(existing.error_json, { message: "Effect failed" }),
        } satisfies EffectReservation
      }

      if (existing) {
        const started = await this.ensureEffectAttemptStarted(client, existing, input)
        this.log("debug", "provider.effect.reserve", {
          workerId: input.workerId,
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          effectId: started.effect_id,
          attemptId: started.attempt_id,
          key: input.key,
          attempt: started.attempt ?? 1,
        })
        this.count("durable.provider.effect", { workerId: input.workerId, status: "reserved" })
        return {
          status: "reserved",
          effectId: started.effect_id,
          idempotencyKey: started.idempotency_key,
          attempt: started.attempt ?? 1,
          attemptId: requireAttemptId(started),
          heartbeatDetails: decodeJson<JsonValue | undefined>(
            started.heartbeat_details_json,
            undefined,
          ),
        } satisfies EffectReservation
      }

      throw new Error(`Unable to reserve effect: ${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`)
    })
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    const result = await this.transaction(async (client) => {
      await this.assertLiveActivationLease(client, input)
      const effect = await this.effectRow(client, input)
      const heartbeatDeadline = effect?.heartbeat_timeout_ms
        ? addMs(input.now, effect.heartbeat_timeout_ms)
        : null
      const row = await this.one<EffectRow>(
        client,
        `
        UPDATE ${this.table("effects")}
        SET heartbeat_at = $1::timestamptz, heartbeat_details_json = $2::jsonb,
          heartbeat_deadline = $3::timestamptz
        WHERE workflow_id = $4 AND run_id = $5 AND activation_id = $6 AND effect_id = $7
          AND attempt_id = $8 AND status = 'pending'
          AND NOT EXISTS (
            SELECT 1 FROM ${this.table("activity_deadlines")} d
            WHERE d.effect_id = $7 AND d.deadline_at <= $1::timestamptz
          )
        RETURNING *
        `,
        [
          input.now,
          encodeJson(input.details ?? null),
          heartbeatDeadline,
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
        ],
      )
      if (row) {
        await this.syncActivityDeadline(client, row)
      }
      return row
    })
    if (!result) {
      await this.throwEffectMutationError(input)
    }
    this.log("debug", "provider.effect.heartbeat", {
      workerId: input.workerId,
      workflowId: input.workflowId,
      runId: input.runId,
      activationId: input.activationId,
      effectId: input.effectId,
      attemptId: input.attemptId,
    })
    this.count("durable.provider.effect", { workerId: input.workerId, status: "heartbeat" })
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    const result = await this.transaction(async (client) => {
      const row = await this.one<Pick<EffectRow, "start_to_close_timeout_ms" | "heartbeat_timeout_ms">>(
        client,
        `
        UPDATE ${this.table("effects")}
        SET status = 'completed', result_json = $1::jsonb, error_json = NULL,
          start_to_close_deadline = NULL, heartbeat_deadline = NULL
        WHERE workflow_id = $2 AND run_id = $3 AND activation_id = $4 AND effect_id = $5
          AND attempt_id = $6 AND status = 'pending'
          AND EXISTS (
            SELECT 1 FROM ${this.table("activation_tasks")} a
            WHERE a.activation_id = $4
              AND a.workflow_id = $2
              AND a.run_id = $3
              AND a.owner_id = $7
              AND a.completed_by_sequence IS NULL
              AND a.lease_until >= $8::timestamptz
              AND NOT EXISTS (
                SELECT 1 FROM ${this.table("activity_deadlines")} d
                WHERE d.effect_id = $5 AND d.deadline_at <= $8::timestamptz
              )
          )
        RETURNING start_to_close_timeout_ms, heartbeat_timeout_ms
        `,
        [
          encodeJson(input.result),
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
          input.workerId,
          input.now,
        ],
      )
      if (row && (row.start_to_close_timeout_ms !== null || row.heartbeat_timeout_ms !== null)) {
        await this.deleteActivityDeadline(client, input.effectId)
      }
      return row
    })
    if (!result) {
      await this.throwEffectMutationError(input)
    }
    this.log("debug", "provider.effect.complete", {
      workerId: input.workerId,
      workflowId: input.workflowId,
      runId: input.runId,
      activationId: input.activationId,
      effectId: input.effectId,
      attemptId: input.attemptId,
    })
    this.count("durable.provider.effect", { workerId: input.workerId, status: "completed" })
  }

  async failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    const output = await this.transaction(async (client) => {
      await this.assertLiveActivationLease(client, input)
      const effect = await this.effectRow(client, input)
      if (!effect || effect.status !== "pending" || effect.attempt_id !== input.attemptId) {
        return { result: null, changes: 0 }
      }

      const retry = retryDecision(effect, input.error, input.now, input.retryable !== false)
      if (retry.status === "retry_scheduled") {
        const result = await client.query(
          `
          UPDATE ${this.table("effects")}
          SET attempt = $1, attempt_id = $2, attempt_owner_id = NULL, attempt_started_at = NULL,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL,
            next_attempt_at = $3::timestamptz, last_failure_json = $4::jsonb
          WHERE effect_id = $5 AND attempt_id = $6 AND status = 'pending'
          `,
          [
            retry.nextAttempt,
            `attempt-${randomUUID()}`,
            retry.nextAttemptAt,
            encodeJson(input.error),
            input.effectId,
            input.attemptId,
          ],
        )
        if (result.rowCount && result.rowCount > 0) {
          await this.deleteActivityDeadline(client, input.effectId)
        }
        this.log("info", "provider.effect.retry", {
          workerId: input.workerId,
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          effectId: input.effectId,
          attemptId: input.attemptId,
          nextAttemptAt: retry.nextAttemptAt,
          nextAttempt: retry.nextAttempt,
        })
        this.count("durable.provider.effect", {
          workerId: input.workerId,
          status: "retry_scheduled",
        })
        return { result: retry, changes: result.rowCount ?? 0 }
      }

      const result = await client.query(
        `
        UPDATE ${this.table("effects")}
        SET status = 'failed', error_json = $1::jsonb, last_failure_json = $1::jsonb,
          start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
        WHERE workflow_id = $2 AND run_id = $3 AND activation_id = $4 AND effect_id = $5
          AND attempt_id = $6 AND status = 'pending'
        `,
        [
          encodeJson(input.error),
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
        ],
      )
      if (result.rowCount && result.rowCount > 0) {
        await this.deleteActivityDeadline(client, input.effectId)
      }
      this.log("warn", "provider.effect.fail", {
        workerId: input.workerId,
        workflowId: input.workflowId,
        runId: input.runId,
        activationId: input.activationId,
        effectId: input.effectId,
        attemptId: input.attemptId,
        error: input.error,
      })
      this.count("durable.provider.effect", { workerId: input.workerId, status: "failed" })
      return { result: { status: "failed" } satisfies FailEffectResult, changes: result.rowCount ?? 0 }
    })
    if (output.changes === 0 || !output.result) {
      await this.throwEffectMutationError(input)
    }
    return output.result as FailEffectResult
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.transaction(async (client) => {
      await this.assertLiveActivationLease(client, {
        workflowId: input.parentWorkflowId,
        runId: input.parentRunId,
        activationId: input.activationId,
        workerId: input.workerId,
        now: input.leaseNow,
      })
      const conflictPolicy = input.conflictPolicy ?? "use_existing"
      const parentClosePolicy = input.parentClosePolicy ?? "cancel"
      const existing = await this.one<ChildRow>(
        client,
        `
        SELECT * FROM ${this.table("children")}
        WHERE parent_workflow_id = $1 AND parent_run_id = $2 AND activation_id = $3 AND key = $4
        LIMIT 1
        FOR UPDATE
        `,
        [input.parentWorkflowId, input.parentRunId, input.activationId, input.key],
      )

      if (existing) {
        if (conflictPolicy === "fail") {
          this.count("durable.provider.child", {
            workflowName: input.workflowName,
            status: "conflict",
            reason: "existing_parent_activation_key",
          })
          throw new Error(
            `Child workflow already exists for activation key: ${input.parentWorkflowId}/${input.parentRunId}/${input.activationId}/${input.key}`,
          )
        }
        if (conflictPolicy === "terminate_existing") {
          await this.deleteInstanceRecords(client, existing.workflow_id, existing.run_id)
        } else {
          this.count("durable.provider.child", {
            workflowName: existing.workflow_name,
            status: "use_existing",
          })
          return childHandle(rowToChildRecord(existing))
        }
      }

      if (await this.instanceRow(client, input)) {
        if (conflictPolicy === "terminate_existing") {
          await this.deleteInstanceRecords(client, input.workflowId, input.runId)
        } else {
          this.count("durable.provider.child", {
            workflowName: input.workflowName,
            status: "conflict",
            reason: "existing_child_instance",
          })
          throw new Error(`Child workflow instance already exists: ${input.workflowId}/${input.runId}`)
        }
      }

      const childRecordId = `child-${randomUUID()}`
      await client.query(
        `
        INSERT INTO ${this.table("instances")} (
          workflow_name, workflow_version, workflow_id, run_id, partition_shard,
          sequence, status, common_json, phase_name, phase_data_json, output_json,
          error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
          parent_child_record_id, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, 0, 'running', $6::jsonb, $7, $8::jsonb,
          NULL, NULL, NULL, $9::jsonb, $10, $11, $12, $13::timestamptz, $13::timestamptz
        )
        `,
        [
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
        ],
      )
      await client.query(
        `
        INSERT INTO ${this.table("children")} (
          child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
          workflow_name, workflow_version, workflow_id, run_id, status, parent_close_policy
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'started', $10)
        `,
        [
          childRecordId,
          input.parentWorkflowId,
          input.parentRunId,
          input.activationId,
          input.key,
          input.workflowName,
          input.workflowVersion,
          input.workflowId,
          input.runId,
          parentClosePolicy,
        ],
      )
      await this.insertReadyEventsForState(client, {
        workflowName: input.workflowName,
        workflowVersion: input.workflowVersion,
        workflowId: input.workflowId,
        runId: input.runId,
        partitionShard: input.partitionShard,
        sequence: 0,
        status: "running",
        waits: input.waits,
        updatedAt: input.now,
      })
      this.log("info", "provider.child.create", {
        workflowName: input.workflowName,
        workflowId: input.workflowId,
        runId: input.runId,
        childRecordId,
        parentWorkflowId: input.parentWorkflowId,
        parentRunId: input.parentRunId,
        activationId: input.activationId,
        key: input.key,
        parentClosePolicy,
      })
      this.count("durable.provider.child", {
        workflowName: input.workflowName,
        status: "created",
      })
      return {
        workflowName: input.workflowName,
        workflowVersion: input.workflowVersion,
        workflowId: input.workflowId,
        runId: input.runId,
      }
    })
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    await this.transaction(async (client) => {
      await this.assertLiveActivationLease(client, {
        workflowId: input.parentWorkflowId,
        runId: input.parentRunId,
        activationId: input.activationId,
        workerId: input.workerId,
        now: input.now,
      })
      const childInstance = await this.instanceRow(client, input)
      if (!childInstance) {
        throw new Error(`Unknown child workflow: ${input.workflowId}/${input.runId}`)
      }
      if (
        childInstance.parent_workflow_id !== input.parentWorkflowId ||
        childInstance.parent_run_id !== input.parentRunId ||
        !childInstance.parent_child_record_id
      ) {
        throw new Error(`Child workflow is not owned by this parent: ${input.workflowId}/${input.runId}`)
      }
      const childRecord = await this.one<ChildRow>(
        client,
        `SELECT * FROM ${this.table("children")} WHERE child_record_id = $1 LIMIT 1 FOR UPDATE`,
        [childInstance.parent_child_record_id],
      )
      if (!childRecord) {
        throw new Error(`Unknown child record: ${childInstance.parent_child_record_id}`)
      }
      await this.cancelStartedChild(client, childRecord, input.now, {
        deliverToParent: true,
        reason: childCanceledError(),
      })
      this.log("info", "provider.child.cancel", {
        workflowName: childRecord.workflow_name,
        workflowId: childRecord.workflow_id,
        runId: childRecord.run_id,
        childRecordId: childRecord.child_record_id,
        parentWorkflowId: childRecord.parent_workflow_id,
        parentRunId: childRecord.parent_run_id,
        activationId: input.activationId,
      })
      this.count("durable.provider.child", {
        workflowName: childRecord.workflow_name,
        status: "canceled",
      })
    })
  }

  async commitActivations(inputs: CommitActivationInput[]): Promise<CommitActivationsResult> {
    if (inputs.length === 0) {
      return { results: [] }
    }
    if (inputs.length === 1) {
      const result = await this.commitCheckpoint(inputs[0])
      return { results: [{ ...result, activationId: inputs[0].activationId }] }
    }
    return this.transaction(async (client) => {
      if (shouldCommitSequentially(inputs)) {
        const results: Array<CommitCheckpointResult & { activationId: string }> = []
        for (const input of inputs) {
          const result = await this.commitCheckpointInTransaction(client, input)
          results.push({ ...result, activationId: input.activationId })
        }
        return { results }
      }
      return this.commitActivationBatchInTransaction(client, inputs)
    })
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    return this.transaction((client) => this.commitCheckpointInTransaction(client, input))
  }

  private checkpointConflict(
    input: CommitCheckpointInput,
    reason: string,
    sequence: number,
    options: { retryable?: boolean; error?: SerializedError } = {},
  ): CommitCheckpointResult {
    this.log("warn", "provider.checkpoint.conflict", {
      workflowId: input.workflowId,
      runId: input.runId,
      activationId: input.activationId,
      workerId: input.workerId,
      reason,
      sequence,
      expectedSequence: input.expectedSequence,
    })
    this.count("durable.provider.checkpoint", {
      workerId: input.workerId,
      status: "conflict",
      reason,
    })
    return {
      ok: false,
      sequence,
      reason,
      ...(options.retryable === undefined ? {} : { retryable: options.retryable }),
      ...(options.error === undefined ? {} : { error: options.error }),
    }
  }

  private async commitActivationBatchInTransaction(
    client: PoolClient,
    inputs: CommitActivationInput[],
  ): Promise<CommitActivationsResult> {
    const inputRows = inputs.map((input, index) => ({
      input_index: index,
      workflow_id: input.workflowId,
      run_id: input.runId,
      activation_id: input.activationId,
      worker_id: input.workerId,
      expected_sequence: input.expectedSequence,
      now: input.now,
      consume_signal_id: input.consumeSignalId ?? null,
      consume_child_record_id: input.consumeChildRecordId ?? null,
    }))
    const instances = await this.rows<IndexedInstanceRow>(
      client,
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          input_index integer,
          workflow_id text,
          run_id text
        )
      )
      SELECT input.input_index, i.*
      FROM input
      JOIN ${this.table("instances")} i
        ON i.workflow_id = input.workflow_id
       AND i.run_id = input.run_id
      FOR UPDATE OF i
      `,
      [encodeJson(inputRows)],
    )
    const claims = await this.rows<IndexedActivationClaimRow>(
      client,
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          input_index integer,
          workflow_id text,
          run_id text,
          activation_id text,
          now timestamptz
        )
      )
      SELECT input.input_index, a.*
      FROM input
      JOIN ${this.table("activation_tasks")} a
        ON a.activation_id = input.activation_id
       AND NOT EXISTS (
         SELECT 1 FROM ${this.table("activity_deadlines")} d
         WHERE d.activation_id = input.activation_id
           AND d.deadline_at <= input.now
       )
      FOR UPDATE OF a
      `,
      [encodeJson(inputRows)],
    )
    const signalRows = inputRows.some((row) => row.consume_signal_id)
      ? await this.rows<IndexedSignalRow>(
          client,
          `
          WITH input AS (
            SELECT *
            FROM jsonb_to_recordset($1::jsonb) AS i(
              input_index integer,
              workflow_id text,
              run_id text,
              consume_signal_id text
            )
          )
          SELECT input.input_index, s.*
          FROM input
          JOIN ${this.table("signals")} s
            ON s.signal_id = input.consume_signal_id
           AND s.workflow_id = input.workflow_id
           AND s.run_id = input.run_id
           AND s.consumed_by_sequence IS NULL
          FOR UPDATE OF s
          `,
          [encodeJson(inputRows)],
        )
      : []
    const childConsumeRows = inputRows.some((row) => row.consume_child_record_id)
      ? await this.rows<IndexedChildRow>(
          client,
          `
          WITH input AS (
            SELECT *
            FROM jsonb_to_recordset($1::jsonb) AS i(
              input_index integer,
              workflow_id text,
              run_id text,
              consume_child_record_id text
            )
          )
          SELECT input.input_index, c.*
          FROM input
          JOIN ${this.table("children")} c
            ON c.child_record_id = input.consume_child_record_id
           AND c.parent_workflow_id = input.workflow_id
           AND c.parent_run_id = input.run_id
           AND c.delivered_by_sequence IS NULL
          FOR UPDATE OF c
          `,
          [encodeJson(inputRows)],
        )
      : []

    const childStartConflicts = await this.validateCheckpointChildStartsForBatch(client, inputs)
    const instanceByIndex = new Map(instances.map((row) => [row.input_index, row]))
    const claimByIndex = new Map(claims.map((row) => [row.input_index, row]))
    const signalByIndex = new Map(signalRows.map((row) => [row.input_index, row]))
    const childConsumeByIndex = new Map(childConsumeRows.map((row) => [row.input_index, row]))
    const results: Array<CommitCheckpointResult & { activationId: string }> = []
    const successes: Array<{
      input: CommitActivationInput
      instance: InstanceRow
      claim: ActivationClaimRow
      nextSequence: number
    }> = []

    for (const [index, input] of inputs.entries()) {
      const instance = instanceByIndex.get(index)
      if (!instance || instance.status !== "running") {
        results.push({
          ...this.checkpointConflict(input, "not_running", instance?.sequence ?? -1),
          activationId: input.activationId,
        })
        continue
      }
      if (instance.sequence !== input.expectedSequence) {
        results.push({
          ...this.checkpointConflict(input, "stale_sequence", instance.sequence),
          activationId: input.activationId,
        })
        continue
      }

      const claim = claimByIndex.get(index)
      if (
        !claim ||
        claim.workflow_id !== input.workflowId ||
        claim.run_id !== input.runId ||
        claim.sequence !== input.expectedSequence ||
        claim.owner_id !== input.workerId ||
        !claim.lease_until ||
        iso(claim.lease_until) < input.now ||
        claim.completed_by_sequence !== null
      ) {
        results.push({
          ...this.checkpointConflict(input, "lost_activation_lease", instance.sequence),
          activationId: input.activationId,
        })
        continue
      }
      if (!claimMatchesCommit(claim, input)) {
        results.push({
          ...this.checkpointConflict(input, "activation_event_mismatch", instance.sequence),
          activationId: input.activationId,
        })
        continue
      }

      if (input.consumeSignalId && !signalByIndex.has(index)) {
        results.push({
          ...this.checkpointConflict(input, "signal_not_consumable", instance.sequence),
          activationId: input.activationId,
        })
        continue
      }
      if (input.consumeChildRecordId && !childConsumeByIndex.has(index)) {
        results.push({
          ...this.checkpointConflict(input, "child_not_consumable", instance.sequence),
          activationId: input.activationId,
        })
        continue
      }

      const childStartConflict = childStartConflicts.get(index)
      if (childStartConflict) {
        results.push({
          ...this.checkpointConflict(input, childStartConflict.reason, instance.sequence, {
            retryable: false,
            error: childStartConflict.error,
          }),
          activationId: input.activationId,
        })
        continue
      }

      const nextSequence = instance.sequence + 1
      successes.push({ input, instance, claim, nextSequence })
      results.push({ ok: true, sequence: nextSequence, activationId: input.activationId })
    }

    if (successes.length === 0) {
      return { results }
    }

    await this.writeCheckpointEffectMutationsForBatch(client, successes.map((success) => success.input))
    await this.writeCheckpointChildStartsForBatch(client, successes.map((success) => success.input))
    await this.writeNextInstancesForBatch(client, successes)
    await this.consumeSignalsForBatch(client, successes)
    await this.consumeChildrenForBatch(client, successes)

    for (const success of successes) {
      await this.updateParentChildRecord(client, success.instance, success.input, success.nextSequence)
      await this.applyParentClosePolicy(client, success.instance, success.input, success.nextSequence)
      if (success.input.next.status === "running") {
        await this.replaceReadyEventsForState(client, {
          workflowName: success.instance.workflow_name,
          workflowVersion: success.input.workflowVersion,
          workflowId: success.input.workflowId,
          runId: success.input.runId,
          partitionShard: success.instance.partition_shard,
          sequence: success.nextSequence,
          status: success.input.next.status,
          waits: success.input.waits,
          updatedAt: success.input.now,
        })
      } else if (success.claim.kind !== "run") {
        await this.deletePendingReadyEventsForInstance(client, success.input.workflowId, success.input.runId)
      }
    }

    await this.completeActivationTasksForBatch(client, successes)
    for (const success of successes) {
      this.log("info", "provider.checkpoint.commit", {
        workflowName: success.instance.workflow_name,
        workflowId: success.input.workflowId,
        runId: success.input.runId,
        activationId: success.input.activationId,
        workerId: success.input.workerId,
        sequence: success.nextSequence,
        status: success.input.next.status,
      })
      this.count("durable.provider.checkpoint", {
        workerId: success.input.workerId,
        workflowName: success.instance.workflow_name,
        status: "success",
      })
    }
    return { results }
  }

  private async commitCheckpointInTransaction(
    client: PoolClient,
    input: CommitCheckpointInput,
  ): Promise<CommitCheckpointResult> {
      const conflict = (
        reason: string,
        sequence: number,
        options: { retryable?: boolean; error?: SerializedError } = {},
      ): CommitCheckpointResult => {
        this.log("warn", "provider.checkpoint.conflict", {
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          workerId: input.workerId,
          reason,
          sequence,
          expectedSequence: input.expectedSequence,
        })
        this.count("durable.provider.checkpoint", {
          workerId: input.workerId,
          status: "conflict",
          reason,
        })
        return {
          ok: false,
          sequence,
          reason,
          ...(options.retryable === undefined ? {} : { retryable: options.retryable }),
          ...(options.error === undefined ? {} : { error: options.error }),
        }
      }

      const joined = await this.one<CommitClaimRow>(
        client,
        `
        SELECT
          i.*,
          a.activation_id AS claim_activation_id,
          a.workflow_id AS claim_workflow_id,
          a.run_id AS claim_run_id,
          a.sequence AS claim_sequence,
          a.kind AS claim_kind,
          a.wait_name AS claim_wait_name,
          a.event_json AS claim_event_json,
          a.wait_json AS claim_wait_json,
          a.owner_id AS claim_owner_id,
          a.lease_until AS claim_lease_until,
          a.activation_time AS claim_activation_time,
          a.completed_by_sequence AS claim_completed_by_sequence
        FROM ${this.table("instances")} i
        JOIN ${this.table("activation_tasks")} a
          ON a.workflow_id = i.workflow_id
         AND a.run_id = i.run_id
         AND a.activation_id = $3
         AND NOT EXISTS (
           SELECT 1 FROM ${this.table("activity_deadlines")} d
           WHERE d.activation_id = $3 AND d.deadline_at <= $4::timestamptz
         )
        WHERE i.workflow_id = $1 AND i.run_id = $2
        LIMIT 1
        FOR UPDATE OF i, a
        `,
        [input.workflowId, input.runId, input.activationId, input.now],
      )
      const instance = joined ?? (await this.instanceRow(client, input, true))
      if (!instance || instance.status !== "running") {
        return conflict("not_running", instance?.sequence ?? -1)
      }
      if (instance.sequence !== input.expectedSequence) {
        return conflict("stale_sequence", instance.sequence)
      }

      const claim = joined ? activationClaimFromCommitRow(joined) : null
      if (
        !claim ||
        claim.workflow_id !== input.workflowId ||
        claim.run_id !== input.runId ||
        claim.sequence !== input.expectedSequence ||
        claim.owner_id !== input.workerId ||
        !claim.lease_until ||
        iso(claim.lease_until) < input.now ||
        claim.completed_by_sequence !== null
      ) {
        return conflict("lost_activation_lease", instance.sequence)
      }
      if (!claimMatchesCommit(claim, input)) {
        return conflict("activation_event_mismatch", instance.sequence)
      }

      const signalToConsume = input.consumeSignalId
        ? await this.one<SignalRow>(
            client,
            `
            SELECT * FROM ${this.table("signals")}
            WHERE signal_id = $1 AND workflow_id = $2 AND run_id = $3
              AND consumed_by_sequence IS NULL
            LIMIT 1
            FOR UPDATE
            `,
            [input.consumeSignalId, input.workflowId, input.runId],
          )
        : undefined
      if (input.consumeSignalId && !signalToConsume) {
        return conflict("signal_not_consumable", instance.sequence)
      }

      const childToConsume = input.consumeChildRecordId
        ? await this.one<ChildRow>(
            client,
            `
            SELECT * FROM ${this.table("children")}
            WHERE child_record_id = $1 AND parent_workflow_id = $2 AND parent_run_id = $3
              AND delivered_by_sequence IS NULL
            LIMIT 1
            FOR UPDATE
            `,
            [input.consumeChildRecordId, input.workflowId, input.runId],
          )
        : undefined
      if (input.consumeChildRecordId && !childToConsume) {
        return conflict("child_not_consumable", instance.sequence)
      }

      const childStartConflict = await this.validateCheckpointChildStarts(client, input)
      if (childStartConflict) {
        return conflict(childStartConflict.reason, instance.sequence, {
          retryable: false,
          error: childStartConflict.error,
        })
      }

      const nextSequence = instance.sequence + 1
      await this.writeCheckpointEffectMutations(client, input)
      await this.writeCheckpointChildStarts(client, input)
      await this.writeNextInstance(client, input, nextSequence)
      if (signalToConsume) {
        await client.query(
          `UPDATE ${this.table("signals")} SET consumed_by_sequence = $1 WHERE signal_id = $2`,
          [nextSequence, signalToConsume.signal_id],
        )
      }
      if (childToConsume) {
        await client.query(
          `UPDATE ${this.table("children")} SET delivered_by_sequence = $1 WHERE child_record_id = $2`,
          [nextSequence, childToConsume.child_record_id],
        )
      }

      await this.updateParentChildRecord(client, instance, input, nextSequence)
      await this.applyParentClosePolicy(client, instance, input, nextSequence)
      if (input.next.status === "running") {
        await this.replaceReadyEventsForState(client, {
          workflowName: instance.workflow_name,
          workflowVersion: input.workflowVersion,
          workflowId: input.workflowId,
          runId: input.runId,
          partitionShard: instance.partition_shard,
          sequence: nextSequence,
          status: input.next.status,
          waits: input.waits,
          updatedAt: input.now,
        })
      } else if (claim.kind !== "run") {
        await this.deletePendingReadyEventsForInstance(client, input.workflowId, input.runId)
      }

      await client.query(
        `
        UPDATE ${this.table("activation_tasks")}
        SET completed_by_sequence = $1, completed_at = $2::timestamptz,
          lease_until = $2::timestamptz, owner_id = $3
        WHERE activation_id = $4
        `,
        [nextSequence, input.now, input.workerId, input.activationId],
      )

      this.log("info", "provider.checkpoint.commit", {
        workflowName: instance.workflow_name,
        workflowId: input.workflowId,
        runId: input.runId,
        activationId: input.activationId,
        workerId: input.workerId,
        sequence: nextSequence,
        status: input.next.status,
      })
      this.count("durable.provider.checkpoint", {
        workerId: input.workerId,
        workflowName: instance.workflow_name,
        status: "success",
      })
      return { ok: true, sequence: nextSequence }
  }

  async recordActivationFailures(inputs: RecordActivationFailureInput[]): Promise<void> {
    if (inputs.length === 0) {
      return
    }
    await this.transaction(async (client) => {
      for (const input of inputs) {
        await this.assertLiveActivationLease(client, input)
        await this.writeCheckpointEffectMutations(client, {
          workflowId: input.workflowId,
          runId: input.runId,
          activationId: input.activationId,
          now: input.now,
          effects: input.effects,
        })
        if (input.releaseActivation) {
          await client.query(
            `
            UPDATE ${this.table("activation_tasks")}
            SET owner_id = NULL, lease_until = NULL
            WHERE activation_id = $1 AND owner_id = $2 AND completed_by_sequence IS NULL
            `,
            [input.activationId, input.workerId],
          )
        }
      }
    })
  }

  async dropSchema(): Promise<void> {
    await this.pool.query(`DROP SCHEMA IF EXISTS ${this.quotedSchema()} CASCADE`)
  }

  private async migrate(): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query("BEGIN")
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
        `durable-phases:${this.schema}:schema`,
      ])
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${this.quotedSchema()}`)
      await client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table("instances")} (
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        partition_shard INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'canceled', 'failed')),
        common_json JSONB,
        phase_name TEXT,
        phase_data_json JSONB,
        output_json JSONB,
        error_json JSONB,
        cancel_reason TEXT,
        waits_json JSONB NOT NULL,
        parent_workflow_id TEXT,
        parent_run_id TEXT,
        parent_child_record_id TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (workflow_id, run_id)
      );

      CREATE TABLE IF NOT EXISTS ${this.table("signals")} (
        signal_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        type TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        received_at TIMESTAMPTZ NOT NULL,
        consumed_by_sequence INTEGER,
        FOREIGN KEY (workflow_id, run_id) REFERENCES ${this.table("instances")}(workflow_id, run_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS ${this.table("children")} (
        child_record_id TEXT PRIMARY KEY,
        parent_workflow_id TEXT NOT NULL,
        parent_run_id TEXT NOT NULL,
        activation_id TEXT NOT NULL,
        key TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'abandoned')),
        parent_close_policy TEXT NOT NULL DEFAULT 'cancel' CHECK (parent_close_policy IN ('cancel', 'abandon')),
        completed_at TIMESTAMPTZ,
        output_json JSONB,
        error_json JSONB,
        delivered_by_sequence INTEGER,
        UNIQUE(parent_workflow_id, parent_run_id, activation_id, key)
      );

      CREATE TABLE IF NOT EXISTS ${this.table("effects")} (
        effect_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        activation_id TEXT NOT NULL,
        key TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
        attempt INTEGER NOT NULL DEFAULT 1,
        attempt_id TEXT,
        attempt_owner_id TEXT,
        attempt_started_at TIMESTAMPTZ,
        start_to_close_timeout_ms INTEGER,
        start_to_close_deadline TIMESTAMPTZ,
        heartbeat_timeout_ms INTEGER,
        heartbeat_deadline TIMESTAMPTZ,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        max_elapsed_ms INTEGER,
        initial_interval_ms INTEGER NOT NULL DEFAULT 1000,
        max_interval_ms INTEGER NOT NULL DEFAULT 30000,
        backoff_coefficient DOUBLE PRECISION NOT NULL DEFAULT 2,
        first_attempt_started_at TIMESTAMPTZ,
        next_attempt_at TIMESTAMPTZ,
        last_failure_json JSONB,
        non_retryable_error_names_json JSONB,
        timed_out_at TIMESTAMPTZ,
        timeout_kind TEXT CHECK (timeout_kind IS NULL OR timeout_kind IN ('heartbeat', 'start_to_close')),
        result_json JSONB,
        error_json JSONB,
        heartbeat_at TIMESTAMPTZ,
        heartbeat_details_json JSONB,
        UNIQUE(workflow_id, run_id, activation_id, key),
        FOREIGN KEY (workflow_id, run_id) REFERENCES ${this.table("instances")}(workflow_id, run_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS ${this.table("activity_deadlines")} (
        effect_id TEXT PRIMARY KEY REFERENCES ${this.table("effects")}(effect_id) ON DELETE CASCADE,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        activation_id TEXT NOT NULL,
        partition_shard INTEGER NOT NULL,
        deadline_at TIMESTAMPTZ NOT NULL,
        timeout_kind TEXT NOT NULL CHECK (timeout_kind IN ('heartbeat', 'start_to_close'))
      );

      CREATE TABLE IF NOT EXISTS ${this.table("dispatch_shards")} (
        shard_id INTEGER PRIMARY KEY,
        owner_id TEXT,
        lease_until TIMESTAMPTZ
      );

      CREATE TABLE IF NOT EXISTS ${this.table("activation_tasks")} (
        activation_id TEXT PRIMARY KEY,
        ready_event_id TEXT UNIQUE,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        partition_shard INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('migration', 'run', 'event', 'signal', 'timer', 'child')),
        wait_name TEXT,
        ready_at TIMESTAMPTZ NOT NULL,
        sort_key TEXT NOT NULL,
        wait_json JSONB,
        event_json JSONB,
        owner_id TEXT,
        lease_until TIMESTAMPTZ,
        activation_time TIMESTAMPTZ,
        completed_by_sequence INTEGER,
        completed_at TIMESTAMPTZ,
        FOREIGN KEY (workflow_id, run_id) REFERENCES ${this.table("instances")}(workflow_id, run_id) ON DELETE CASCADE
      );
    `)
      await client.query(`
      CREATE INDEX IF NOT EXISTS ${this.index("instances_by_status_shard")}
        ON ${this.table("instances")}(status, partition_shard, updated_at);
      CREATE INDEX IF NOT EXISTS ${this.index("signals_hot_lookup")}
        ON ${this.table("signals")}(workflow_id, run_id, type, received_at, signal_id)
        WHERE consumed_by_sequence IS NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("children_parent_delivery")}
        ON ${this.table("children")}(parent_workflow_id, parent_run_id, completed_at, child_record_id)
        WHERE delivered_by_sequence IS NULL AND status IN ('completed', 'failed');
      CREATE INDEX IF NOT EXISTS ${this.index("children_child_instance")}
        ON ${this.table("children")}(workflow_id, run_id, status, delivered_by_sequence);
      CREATE INDEX IF NOT EXISTS ${this.index("effects_activation_key")}
        ON ${this.table("effects")}(workflow_id, run_id, activation_id, key);
      CREATE INDEX IF NOT EXISTS ${this.index("effects_activation_pending")}
        ON ${this.table("effects")}(activation_id)
        WHERE status = 'pending';
      CREATE INDEX IF NOT EXISTS ${this.index("effects_start_deadline")}
        ON ${this.table("effects")}(start_to_close_deadline)
        WHERE status = 'pending' AND start_to_close_deadline IS NOT NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("effects_heartbeat_deadline")}
        ON ${this.table("effects")}(heartbeat_deadline)
        WHERE status = 'pending' AND heartbeat_deadline IS NOT NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("effects_next_attempt")}
        ON ${this.table("effects")}(next_attempt_at)
        WHERE status = 'pending' AND next_attempt_at IS NOT NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("activity_deadlines_shard_due")}
        ON ${this.table("activity_deadlines")}(partition_shard, deadline_at);
      CREATE INDEX IF NOT EXISTS ${this.index("activity_deadlines_activation_due")}
        ON ${this.table("activity_deadlines")}(activation_id, deadline_at);
      CREATE INDEX IF NOT EXISTS ${this.index("activation_tasks_owner_lease")}
        ON ${this.table("activation_tasks")}(owner_id, lease_until)
        WHERE completed_by_sequence IS NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("activation_tasks_instance_sequence")}
        ON ${this.table("activation_tasks")}(workflow_id, run_id, sequence, activation_id)
        WHERE completed_by_sequence IS NULL;
      CREATE INDEX IF NOT EXISTS ${this.index("activation_tasks_claim")}
        ON ${this.table("activation_tasks")}(partition_shard, ready_at, sort_key)
        WHERE kind <> 'migration';
      CREATE INDEX IF NOT EXISTS ${this.index("activation_tasks_instance")}
        ON ${this.table("activation_tasks")}(workflow_id, run_id, sequence);
    `)
      await client.query("COMMIT")
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect()
    try {
      await client.query(
        `BEGIN; SET LOCAL statement_timeout = '${this.statementTimeoutMs}ms'; SET LOCAL lock_timeout = '${this.lockTimeoutMs}ms'`,
      )
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

  private table(name: string): string {
    return `${this.quotedSchema()}.${quoteIdentifier(name)}`
  }

  private index(name: string): string {
    return quoteIdentifier(name)
  }

  private quotedSchema(): string {
    return quoteIdentifier(this.schema)
  }

  private async rows<T>(client: Queryable, sql: string, params: unknown[] = []): Promise<T[]> {
    return (await client.query(sql, params)).rows as T[]
  }

  private async one<T>(client: Queryable, sql: string, params: unknown[] = []): Promise<T | null> {
    return ((await client.query(sql, params)).rows[0] as T | undefined) ?? null
  }

  private async instanceRow(
    client: Queryable,
    ref: InstanceRef,
    forUpdate = false,
  ): Promise<InstanceRow | null> {
    return this.one<InstanceRow>(
      client,
      `
      SELECT * FROM ${this.table("instances")}
      WHERE workflow_id = $1 AND run_id = $2
      LIMIT 1
      ${forUpdate ? "FOR UPDATE" : ""}
      `,
      [ref.workflowId, ref.runId],
    )
  }

  private activationInstanceSnapshot(row: InstanceRow): ActivationInstanceSnapshot {
    return {
      workflowName: row.workflow_name,
      workflowVersion: row.workflow_version,
      workflowId: row.workflow_id,
      runId: row.run_id,
      sequence: row.sequence,
      status: row.status,
      common: decodeJson<JsonObject | undefined>(row.common_json, undefined),
      phase:
        row.phase_name && row.phase_data_json !== null && row.phase_data_json !== undefined
          ? {
              name: row.phase_name,
              data: decodeJson<JsonObject>(row.phase_data_json, {}),
            }
          : undefined,
      output: decodeJson<JsonValue | undefined>(row.output_json, undefined),
      error: decodeJson<SerializedError | undefined>(row.error_json, undefined),
      cancelReason: row.cancel_reason ?? undefined,
      waits: decodeJson<DurableWait[]>(row.waits_json, []),
      createdAt: iso(row.created_at),
      updatedAt: iso(row.updated_at),
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

  private async persistedInstance(
    client: Queryable,
    row: InstanceRow,
    options: LoadInstanceOptions = {},
  ): Promise<PersistedInstance> {
    const instance: PersistedInstance = this.activationInstanceSnapshot(row)
    if (options.includeEffects) {
      const effects = await this.rows<EffectRow>(
        client,
        `
        SELECT * FROM ${this.table("effects")}
        WHERE workflow_id = $1 AND run_id = $2
        ORDER BY effect_id
        `,
        [row.workflow_id, row.run_id],
      )
      instance.effects = effects.map(rowToEffectRecord)
    }
    return instance
  }

  private async effectsForActivations(
    client: Queryable,
    activationIds: string[],
  ): Promise<Map<string, EffectRecord[]>> {
    const uniqueIds = [...new Set(activationIds)]
    const byActivation = new Map<string, EffectRecord[]>()
    if (uniqueIds.length === 0) {
      return byActivation
    }
    const rows = await this.rows<EffectRow>(
      client,
      `
      SELECT * FROM ${this.table("effects")}
      WHERE activation_id = ANY($1::text[])
      ORDER BY activation_id, key
      `,
      [uniqueIds],
    )
    for (const row of rows) {
      const list = byActivation.get(row.activation_id) ?? []
      list.push(rowToEffectRecord(row))
      byActivation.set(row.activation_id, list)
    }
    return byActivation
  }

  private async effectRow(
    client: Queryable,
    input: { workflowId: string; runId: string; activationId: string; effectId?: string; key?: string },
  ): Promise<EffectRow | null> {
    if (input.effectId) {
      return this.one<EffectRow>(
        client,
        `
        SELECT * FROM ${this.table("effects")}
        WHERE workflow_id = $1 AND run_id = $2 AND activation_id = $3 AND effect_id = $4
        LIMIT 1
        FOR UPDATE
        `,
        [input.workflowId, input.runId, input.activationId, input.effectId],
      )
    }
    return this.one<EffectRow>(
      client,
      `
      SELECT * FROM ${this.table("effects")}
      WHERE workflow_id = $1 AND run_id = $2 AND activation_id = $3 AND key = $4
      LIMIT 1
      FOR UPDATE
      `,
      [input.workflowId, input.runId, input.activationId, input.key],
    )
  }

  private async replaceReadyEventsForInstance(
    client: PoolClient,
    workflowId: string,
    runId: string,
  ): Promise<void> {
    const instance = await this.instanceRow(client, { workflowId, runId })
    if (!instance) {
      return
    }
    await this.replaceReadyEventsForState(client, readyEventStateFromInstanceRow(instance))
  }

  private async replaceReadyEventsForState(
    client: PoolClient,
    state: ReadyEventInstanceState,
  ): Promise<void> {
    await this.deletePendingReadyEventsForInstance(client, state.workflowId, state.runId)
    if (state.status !== "running") {
      return
    }
    await this.insertReadyEventsForState(client, state)
  }

  private async insertReadyEventsForState(
    client: PoolClient,
    state: ReadyEventInstanceState,
  ): Promise<void> {
    if (state.status !== "running") {
      return
    }
    await this.insertReadyEvents(client, await this.buildReadyEventsForState(client, state, state.waits))
  }

  private async deletePendingReadyEventsForInstance(
    client: PoolClient,
    workflowId: string,
    runId: string,
  ): Promise<void> {
    await client.query(
      `
      DELETE FROM ${this.table("activation_tasks")}
      WHERE workflow_id = $1 AND run_id = $2
        AND owner_id IS NULL
        AND completed_by_sequence IS NULL
      `,
      [workflowId, runId],
    )
  }

  private async replaceSignalReadyEventsForState(
    client: PoolClient,
    state: ReadyEventInstanceState,
  ): Promise<void> {
    await client.query(
      `
      DELETE FROM ${this.table("activation_tasks")}
      WHERE workflow_id = $1 AND run_id = $2 AND sequence = $3 AND kind = 'signal'
        AND owner_id IS NULL
        AND completed_by_sequence IS NULL
      `,
      [state.workflowId, state.runId, state.sequence],
    )
    if (state.status !== "running") {
      return
    }
    const signalWaits = state.waits.filter((wait): wait is Extract<DurableWait, { kind: "signal" }> =>
      wait.kind === "signal",
    )
    await this.insertReadyEvents(client, await this.buildReadyEventsForState(client, state, signalWaits))
  }

  private async buildReadyEventsForState(
    client: PoolClient,
    state: ReadyEventInstanceState,
    waits: DurableWait[],
  ): Promise<ReadyEventInsert[]> {
    const inserts: ReadyEventInsert[] = []
    const signalTypes = [...new Set(waits.flatMap((wait) => wait.kind === "signal" ? [wait.type] : []))]
    const signalRows = signalTypes.length === 0
      ? []
      : await this.rows<SignalRow>(
          client,
          `
          SELECT DISTINCT ON (type) *
          FROM ${this.table("signals")}
          WHERE workflow_id = $1 AND run_id = $2 AND type = ANY($3::text[])
            AND consumed_by_sequence IS NULL
          ORDER BY type, received_at, signal_id
          `,
          [state.workflowId, state.runId, signalTypes],
        )
    const signalByType = new Map(signalRows.map((row) => [row.type, row]))
    const childRefs = waits
      .filter((wait): wait is Extract<DurableWait, { kind: "child" }> => wait.kind === "child")
      .map((wait) => ({ workflow_id: wait.workflowId, run_id: wait.runId }))
    const childRows = childRefs.length === 0
      ? []
      : await this.rows<ChildRow>(
          client,
          `
          WITH wanted AS (
            SELECT * FROM jsonb_to_recordset($1::jsonb) AS w(workflow_id text, run_id text)
          )
          SELECT DISTINCT ON (c.workflow_id, c.run_id) c.*
          FROM ${this.table("children")} c
          JOIN wanted w ON w.workflow_id = c.workflow_id AND w.run_id = c.run_id
          WHERE c.status IN ('completed', 'failed')
            AND c.delivered_by_sequence IS NULL
          ORDER BY c.workflow_id, c.run_id, c.completed_at, c.child_record_id
          `,
          [encodeJson(childRefs)],
        )
    const childByRef = new Map(childRows.map((row) => [`${row.workflow_id}\0${row.run_id}`, row]))

    for (const wait of waits) {
      if (wait.kind === "run") {
        const activationId = activationIdFromParts(
          state.workflowId,
          state.runId,
          state.sequence,
          "run",
          wait.name,
        )
        inserts.push({
          readyEventId: activationId,
          workflowId: state.workflowId,
          runId: state.runId,
          workflowName: state.workflowName,
          workflowVersion: state.workflowVersion,
          partitionShard: state.partitionShard,
          sequence: state.sequence,
          kind: "run",
          waitName: wait.name,
          activationId,
          readyAt: wait.readyAt,
          sortKey: sortKey(wait.readyAt, "run", wait.name, state.workflowId, state.runId),
          wait,
          event: null,
        })
        continue
      }

      if (wait.kind === "signal") {
        const signalRow = signalByType.get(wait.type)
        if (!signalRow) {
          continue
        }
        const activationId = activationIdFromParts(
          state.workflowId,
          state.runId,
          state.sequence,
          "signal",
          signalRow.signal_id,
        )
        const event: ReadyEvent = {
          kind: "signal",
          signalId: signalRow.signal_id,
          payload: decodeJson<JsonValue>(signalRow.payload_json, null),
          occurredAt: iso(signalRow.received_at),
          consumeSignalId: signalRow.signal_id,
        }
        inserts.push({
          readyEventId: `${activationId}/${wait.name}`,
          workflowId: state.workflowId,
          runId: state.runId,
          workflowName: state.workflowName,
          workflowVersion: state.workflowVersion,
          partitionShard: state.partitionShard,
          sequence: state.sequence,
          kind: "signal",
          waitName: wait.name,
          activationId,
          readyAt: iso(signalRow.received_at),
          sortKey: sortKey(iso(signalRow.received_at), "signal", wait.name, signalRow.signal_id),
          wait,
          event,
        })
        continue
      }

      if (wait.kind === "timer") {
        const activationId = activationIdFromParts(
          state.workflowId,
          state.runId,
          state.sequence,
          "timer",
          `${wait.name}:${wait.fireAt}`,
        )
        const event: ReadyEvent = {
          kind: "timer",
          firedAt: wait.fireAt,
          occurredAt: wait.fireAt,
        }
        inserts.push({
          readyEventId: activationId,
          workflowId: state.workflowId,
          runId: state.runId,
          workflowName: state.workflowName,
          workflowVersion: state.workflowVersion,
          partitionShard: state.partitionShard,
          sequence: state.sequence,
          kind: "timer",
          waitName: wait.name,
          activationId,
          readyAt: wait.fireAt,
          sortKey: sortKey(wait.fireAt, "timer", wait.name, `${wait.name}:${wait.fireAt}`),
          wait,
          event,
        })
        continue
      }

      const childRow = childByRef.get(`${wait.workflowId}\0${wait.runId}`)
      if (!childRow) {
        continue
      }
      const occurredAt = childRow.completed_at ? iso(childRow.completed_at) : state.updatedAt
      const activationId = activationIdFromParts(
        state.workflowId,
        state.runId,
        state.sequence,
        "child",
        childRow.child_record_id,
      )
      const event: ReadyEvent = {
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
      }
      inserts.push({
        readyEventId: activationId,
        workflowId: state.workflowId,
        runId: state.runId,
        workflowName: state.workflowName,
        workflowVersion: state.workflowVersion,
        partitionShard: state.partitionShard,
        sequence: state.sequence,
        kind: "child",
        waitName: wait.name,
        activationId,
        readyAt: occurredAt,
        sortKey: sortKey(occurredAt, "child", wait.name, childRow.child_record_id),
        wait,
        event,
      })
    }
    return inserts
  }

  private async insertReadyEvents(
    client: PoolClient,
    inputs: ReadyEventInsert[],
  ): Promise<void> {
    if (inputs.length === 0) {
      return
    }
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS r(
          ready_event_id text,
          workflow_id text,
          run_id text,
          workflow_name text,
          workflow_version integer,
          partition_shard integer,
          sequence integer,
          kind text,
          wait_name text,
          activation_id text,
          ready_at timestamptz,
          sort_key text,
          wait_json jsonb,
          event_json jsonb
        )
      )
      INSERT INTO ${this.table("activation_tasks")} (
        activation_id, ready_event_id, workflow_id, run_id, workflow_name, workflow_version,
        partition_shard, sequence, kind, wait_name, ready_at,
        sort_key, wait_json, event_json
      )
      SELECT
        activation_id, ready_event_id, workflow_id, run_id, workflow_name, workflow_version,
        partition_shard, sequence, kind, wait_name, ready_at,
        sort_key, wait_json, event_json
      FROM input
      ON CONFLICT (ready_event_id) DO UPDATE SET
        activation_id = EXCLUDED.activation_id,
        workflow_name = EXCLUDED.workflow_name,
        workflow_version = EXCLUDED.workflow_version,
        partition_shard = EXCLUDED.partition_shard,
        sequence = EXCLUDED.sequence,
        kind = EXCLUDED.kind,
        wait_name = EXCLUDED.wait_name,
        ready_at = EXCLUDED.ready_at,
        sort_key = EXCLUDED.sort_key,
        wait_json = EXCLUDED.wait_json,
        event_json = EXCLUDED.event_json,
        owner_id = NULL,
        lease_until = NULL,
        activation_time = NULL
      WHERE ${this.table("activation_tasks")}.completed_by_sequence IS NULL
      `,
      [
        encodeJson(
          inputs.map((input) => ({
            ready_event_id: input.readyEventId,
            workflow_id: input.workflowId,
            run_id: input.runId,
            workflow_name: input.workflowName,
            workflow_version: input.workflowVersion,
            partition_shard: input.partitionShard,
            sequence: input.sequence,
            kind: input.kind,
            wait_name: input.waitName,
            activation_id: input.activationId,
            ready_at: input.readyAt,
            sort_key: input.sortKey,
            wait_json: input.wait,
            event_json: input.event,
          })),
        ),
      ],
    )
  }

  private async readyCandidates(
    client: PoolClient,
    input: ClaimReadyActivationInput,
    limit: number,
  ): Promise<ReadyCandidate[]> {
    const candidates = [
      ...(await this.migrationReadyCandidates(client, input, limit)),
      ...(await this.indexedReadyCandidates(client, input, limit)),
    ]
    return candidates.sort(compareReadyCandidates).slice(0, limit)
  }

  private async migrationReadyCandidates(
    client: PoolClient,
    input: ClaimReadyActivationInput,
    limit: number,
  ): Promise<ReadyCandidate[]> {
    const workflowEntries = Object.entries(input.workflows)
    if (input.shardIds.length === 0 || workflowEntries.length === 0) {
      return []
    }
    const version = workflowVersionClause("i", workflowEntries, 3, "<")
    const rows = await this.rows<InstanceRow>(
      client,
      `
      SELECT i.*
      FROM ${this.table("instances")} i
      WHERE i.status = 'running'
        AND i.partition_shard = ANY($1::int[])
        AND (${version.sql})
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("activation_tasks")} competing
          WHERE competing.workflow_id = i.workflow_id
            AND competing.run_id = i.run_id
            AND competing.sequence = i.sequence
            AND competing.kind <> 'migration'
            AND competing.completed_by_sequence IS NULL
            AND competing.owner_id IS NOT NULL
            AND competing.lease_until > $2::timestamptz
          LIMIT 1
        )
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("activation_tasks")} same_migration
          WHERE same_migration.workflow_id = i.workflow_id
            AND same_migration.run_id = i.run_id
            AND same_migration.sequence = i.sequence
            AND same_migration.kind = 'migration'
            AND same_migration.completed_by_sequence IS NULL
            AND same_migration.owner_id IS NOT NULL
            AND same_migration.lease_until > $2::timestamptz
          LIMIT 1
        )
      ORDER BY i.updated_at, i.workflow_id, i.run_id
      LIMIT $${version.nextIndex}
      FOR UPDATE SKIP LOCKED
      `,
      [input.shardIds, input.now, ...version.params, limit],
    )
    return rows.map((instance) => {
      const target = input.workflows[instance.workflow_name]
      return {
        kind: "migration",
        activationId: activationIdFromParts(
          instance.workflow_id,
          instance.run_id,
          instance.sequence,
          "migration",
          `${instance.workflow_version}->${target.version}`,
        ),
        workflowName: instance.workflow_name,
        workflowId: instance.workflow_id,
        runId: instance.run_id,
        sequence: instance.sequence,
        activationTime: iso(instance.updated_at),
        leaseUntil: "",
        sort: [sortKey(iso(instance.updated_at), "migration", instance.workflow_id, instance.run_id)],
        instance,
      }
    })
  }

  private async indexedReadyCandidates(
    client: PoolClient,
    input: ClaimReadyActivationInput,
    limit: number,
  ): Promise<ReadyCandidate[]> {
    const workflowEntries = Object.entries(input.workflows)
    if (input.shardIds.length === 0 || workflowEntries.length === 0) {
      return []
    }
    const version = workflowVersionClause("re", workflowEntries, 4)
    const rows = await this.rows<ReadyEventJoinedRow>(
      client,
      `
      SELECT
        re.*,
        i.workflow_name AS instance_workflow_name,
        i.workflow_version AS instance_workflow_version,
        i.workflow_id AS instance_workflow_id,
        i.run_id AS instance_run_id,
        i.partition_shard AS instance_partition_shard,
        i.sequence AS instance_sequence,
        i.status AS instance_status,
        i.common_json AS instance_common_json,
        i.phase_name AS instance_phase_name,
        i.phase_data_json AS instance_phase_data_json,
        i.output_json AS instance_output_json,
        i.error_json AS instance_error_json,
        i.cancel_reason AS instance_cancel_reason,
        i.waits_json AS instance_waits_json,
        i.parent_workflow_id AS instance_parent_workflow_id,
        i.parent_run_id AS instance_parent_run_id,
        i.parent_child_record_id AS instance_parent_child_record_id,
        i.created_at AS instance_created_at,
        i.updated_at AS instance_updated_at
      FROM ${this.table("activation_tasks")} re
      JOIN ${this.table("instances")} i ON i.workflow_id = re.workflow_id AND i.run_id = re.run_id
      WHERE re.partition_shard = ANY($1::int[])
        AND re.ready_at <= $2::timestamptz
        AND re.kind IN ('run', 'signal', 'timer', 'child')
        AND (${version.sql})
        AND i.status = 'running'
        AND i.sequence = re.sequence
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("activation_tasks")} same_activation
          WHERE same_activation.activation_id = re.activation_id
            AND same_activation.completed_by_sequence IS NULL
            AND same_activation.owner_id IS NOT NULL
            AND same_activation.lease_until > $3::timestamptz
          LIMIT 1
        )
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("activation_tasks")} competing
          WHERE competing.workflow_id = re.workflow_id
            AND competing.run_id = re.run_id
            AND competing.sequence = re.sequence
            AND competing.activation_id <> re.activation_id
            AND competing.completed_by_sequence IS NULL
            AND competing.owner_id IS NOT NULL
            AND competing.lease_until > $3::timestamptz
          LIMIT 1
        )
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("effects")} blocked
          WHERE blocked.activation_id = re.activation_id
            AND blocked.status = 'pending'
            AND blocked.next_attempt_at IS NOT NULL
            AND blocked.next_attempt_at > $3::timestamptz
          LIMIT 1
        )
      ORDER BY re.sort_key
      LIMIT $${version.nextIndex}
      FOR UPDATE OF re SKIP LOCKED
      `,
      [input.shardIds, input.now, input.now, ...version.params, limit],
    )
    return rows.flatMap((row) => {
      const candidate = readyCandidateFromRow(row)
      return candidate ? [candidate] : []
    })
  }

  private async nextWakeAt(
    client: Queryable,
    shardIds: number[],
    now: string,
    workflows: Record<string, { version: number }>,
  ): Promise<string | undefined> {
    const workflowEntries = Object.entries(workflows)
    if (shardIds.length === 0 || workflowEntries.length === 0) {
      return undefined
    }
    const current = workflowVersionClause("", workflowEntries, 3)
    const row = await this.one<{ ready_at: Date | string | null }>(
      client,
      `
      SELECT ready_at FROM ${this.table("activation_tasks")}
      WHERE partition_shard = ANY($1::int[])
        AND kind IN ('run', 'timer')
        AND ready_at > $2::timestamptz
        AND (${current.sql})
      ORDER BY ready_at
      LIMIT 1
      `,
      [shardIds, now, ...current.params],
    )
    const retryVersion = workflowVersionClause("i", workflowEntries, 3)
    const deadlineVersion = workflowVersionClause("i", workflowEntries, 3)
    const effectDeadline = await this.one<{ deadline: Date | string | null }>(
      client,
      `
      SELECT MIN(d.deadline_at) AS deadline
      FROM ${this.table("activity_deadlines")} d
      JOIN ${this.table("instances")} i ON i.workflow_id = d.workflow_id AND i.run_id = d.run_id
      JOIN ${this.table("activation_tasks")} a ON a.activation_id = d.activation_id
      WHERE d.partition_shard = ANY($1::int[])
        AND d.deadline_at > $2::timestamptz
        AND i.status = 'running'
        AND a.completed_by_sequence IS NULL
        AND (${deadlineVersion.sql})
      `,
      [shardIds, now, ...deadlineVersion.params],
    )
    const retryWake = await this.one<{ ready_at: Date | string | null }>(
      client,
      `
      SELECT MIN(blocked_until) AS ready_at
      FROM (
        SELECT e.activation_id, MAX(e.next_attempt_at) AS blocked_until
        FROM ${this.table("effects")} e
        JOIN ${this.table("instances")} i ON i.workflow_id = e.workflow_id AND i.run_id = e.run_id
        WHERE i.partition_shard = ANY($1::int[])
          AND i.status = 'running'
          AND e.status = 'pending'
          AND e.next_attempt_at IS NOT NULL
          AND e.next_attempt_at > $2::timestamptz
          AND (${retryVersion.sql})
        GROUP BY e.activation_id
      ) retries
      `,
      [shardIds, now, ...retryVersion.params],
    )
    return earliestIso(
      row?.ready_at ? iso(row.ready_at) : undefined,
      effectDeadline?.deadline ? iso(effectDeadline.deadline) : undefined,
      retryWake?.ready_at ? iso(retryWake.ready_at) : undefined,
    )
  }

  private async writeActivationClaims(
    client: PoolClient,
    candidates: ReadyCandidate[],
    workerId: string,
    now: string,
    leaseMs: number,
  ): Promise<Set<string>> {
    if (candidates.length === 0) {
      return new Set()
    }
    const leaseUntil = addMs(now, leaseMs)
    const existingClaimRows = await this.rows<Pick<ActivationClaimRow, "activation_id" | "owner_id" | "lease_until">>(
      client,
      `
      SELECT activation_id, owner_id, lease_until
      FROM ${this.table("activation_tasks")}
      WHERE activation_id = ANY($1::text[])
      `,
      [candidates.map((candidate) => candidate.activationId)],
    )
    const reclaimedActivationIds = new Set(
      existingClaimRows
        .filter((row) => row.owner_id !== null && (!row.lease_until || iso(row.lease_until) <= now))
        .map((row) => row.activation_id),
    )
    const rows = await this.rows<ActivationClaimUpsertRow>(
      client,
      `
      WITH candidate AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS c(
          activation_id text,
          ready_event_id text,
          workflow_id text,
          run_id text,
          workflow_name text,
          workflow_version integer,
          partition_shard integer,
          sequence integer,
          kind text,
          wait_name text,
          event_json jsonb,
          wait_json jsonb,
          ready_at timestamptz,
          sort_key text
        )
      )
      INSERT INTO ${this.table("activation_tasks")} (
        activation_id, ready_event_id, workflow_id, run_id, workflow_name,
        workflow_version, partition_shard, sequence, kind, wait_name,
        event_json, wait_json, ready_at, sort_key, owner_id, lease_until, activation_time
      )
      SELECT
        activation_id, ready_event_id, workflow_id, run_id, workflow_name,
        workflow_version, partition_shard, sequence, kind, wait_name,
        event_json, wait_json, ready_at, sort_key, $2, $3::timestamptz, ready_at
      FROM candidate
      ON CONFLICT (activation_id) DO UPDATE SET
        owner_id = EXCLUDED.owner_id,
        lease_until = EXCLUDED.lease_until,
        event_json = EXCLUDED.event_json,
        wait_json = EXCLUDED.wait_json,
        kind = EXCLUDED.kind,
        activation_time = COALESCE(${this.table("activation_tasks")}.activation_time, EXCLUDED.activation_time)
      WHERE ${this.table("activation_tasks")}.completed_by_sequence IS NULL
        AND (
          ${this.table("activation_tasks")}.owner_id IS NULL
          OR ${this.table("activation_tasks")}.lease_until IS NULL
          OR ${this.table("activation_tasks")}.lease_until <= $4::timestamptz
        )
      RETURNING *, (xmax = 0) AS inserted
      `,
      [
        encodeJson(
          candidates.map((candidate) => ({
            activation_id: candidate.activationId,
            ready_event_id: candidate.activationId,
            workflow_id: candidate.workflowId,
            run_id: candidate.runId,
            workflow_name: candidate.workflowName,
            workflow_version: candidate.instance.workflow_version,
            partition_shard: candidate.instance.partition_shard,
            sequence: candidate.sequence,
            kind: candidate.kind === "event" ? candidate.eventKind : candidate.kind,
            wait_name: candidate.kind === "event" ? (candidate.waitName ?? null) : null,
            event_json: candidate.kind === "event" ? (candidate.eventJson ?? null) : null,
            wait_json: candidate.kind === "event" ? (candidate.waitJson ?? null) : null,
            ready_at: candidate.activationTime,
            sort_key: candidate.sort.join("\u0000"),
          })),
        ),
        workerId,
        leaseUntil,
        now,
      ],
    )
    const claimed = new Set<string>()
    const byActivation = new Map(candidates.map((candidate) => [candidate.activationId, candidate]))
    for (const row of rows) {
      const candidate = byActivation.get(row.activation_id)
      if (!candidate) {
        continue
      }
      candidate.leaseUntil = leaseUntil
      claimed.add(row.activation_id)
      const reclaimed = reclaimedActivationIds.has(row.activation_id)
      if (reclaimed) {
        await this.resetPendingEffectsForActivationReclaim(client, candidate.activationId)
      }
      this.log("debug", reclaimed ? "provider.activation.reclaim" : "provider.activation.claim", {
        workerId,
        workflowName: candidate.workflowName,
        workflowId: candidate.workflowId,
        runId: candidate.runId,
        activationId: candidate.activationId,
        activationKind: candidate.kind,
        eventKind: candidate.kind === "event" ? candidate.eventKind : undefined,
        leaseUntil,
      })
      this.count("durable.provider.activation.claim", {
        workerId,
        workflowName: candidate.workflowName,
        activationKind: candidate.kind,
        eventKind: candidate.kind === "event" ? candidate.eventKind : undefined,
        status: reclaimed ? "reclaimed" : "success",
      })
    }
    return claimed
  }

  private async tryWriteActivationClaim(
    client: PoolClient,
    candidate: ReadyCandidate,
    workerId: string,
    now: string,
    leaseMs: number,
  ): Promise<boolean> {
    const leaseUntil = addMs(now, leaseMs)
    const row = await this.one<ActivationClaimUpsertRow>(
      client,
      `
      INSERT INTO ${this.table("activation_tasks")} (
        activation_id, workflow_id, run_id, sequence, kind, wait_name,
        event_json, wait_json, owner_id, lease_until, activation_time
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10::timestamptz, $11::timestamptz
      )
      ON CONFLICT (activation_id) DO UPDATE SET
        owner_id = EXCLUDED.owner_id,
        lease_until = EXCLUDED.lease_until,
        event_json = EXCLUDED.event_json,
        wait_json = EXCLUDED.wait_json,
        activation_time = COALESCE(${this.table("activation_tasks")}.activation_time, EXCLUDED.activation_time)
      WHERE ${this.table("activation_tasks")}.completed_by_sequence IS NULL
        AND (
          ${this.table("activation_tasks")}.owner_id IS NULL
          OR ${this.table("activation_tasks")}.lease_until IS NULL
          OR ${this.table("activation_tasks")}.lease_until <= $12::timestamptz
        )
      RETURNING *, (xmax = 0) AS inserted
      `,
      [
        candidate.activationId,
        candidate.workflowId,
        candidate.runId,
        candidate.sequence,
        candidate.kind,
        candidate.kind === "event" ? (candidate.waitName ?? null) : null,
        candidate.kind === "event" ? encodeJson(candidate.eventJson ?? null) : null,
        candidate.kind === "event" ? encodeJson(candidate.waitJson ?? null) : null,
        workerId,
        leaseUntil,
        candidate.activationTime,
        now,
      ],
    )
    if (!row) {
      return false
    }
    const reclaimed = !row.inserted
    if (reclaimed) {
      await this.resetPendingEffectsForActivationReclaim(client, candidate.activationId)
    }
    candidate.leaseUntil = leaseUntil
    this.log("debug", reclaimed ? "provider.activation.reclaim" : "provider.activation.claim", {
      workerId,
      workflowName: candidate.workflowName,
      workflowId: candidate.workflowId,
      runId: candidate.runId,
      activationId: candidate.activationId,
      activationKind: candidate.kind,
      eventKind: candidate.kind === "event" ? candidate.eventKind : undefined,
      leaseUntil,
    })
    this.count("durable.provider.activation.claim", {
      workerId,
      workflowName: candidate.workflowName,
      activationKind: candidate.kind,
      eventKind: candidate.kind === "event" ? candidate.eventKind : undefined,
      status: reclaimed ? "reclaimed" : "success",
    })
    return true
  }

  private async assertLiveActivationLease(
    client: Queryable,
    input: {
      workflowId: string
      runId: string
      activationId: string
      workerId: string
      now: string
    },
  ): Promise<ActivationClaimRow> {
    const claim = await this.one<ActivationClaimRow>(
      client,
      `
      SELECT * FROM ${this.table("activation_tasks")}
      WHERE activation_id = $1
        AND NOT EXISTS (
          SELECT 1 FROM ${this.table("activity_deadlines")} d
          WHERE d.activation_id = $1 AND d.deadline_at <= $2::timestamptz
        )
      FOR UPDATE
      `,
      [input.activationId, input.now],
    )
    if (
      !claim ||
      claim.workflow_id !== input.workflowId ||
      claim.run_id !== input.runId ||
      claim.owner_id !== input.workerId ||
      !claim.lease_until ||
      iso(claim.lease_until) < input.now ||
      claim.completed_by_sequence !== null
    ) {
      throw new Error(`Lost activation lease: ${input.activationId}`)
    }
    return claim
  }

  private async ensureEffectAttemptStarted(
    client: PoolClient,
    effect: EffectRow,
    input: ReserveEffectInput,
  ): Promise<EffectRow> {
    if (effect.next_attempt_at && iso(effect.next_attempt_at) > input.now) {
      throw new Error(`Effect retry is not ready until ${iso(effect.next_attempt_at)}: ${effect.effect_id}`)
    }
    if (
      effect.attempt_started_at &&
      effect.attempt_owner_id === input.workerId &&
      effect.attempt_id
    ) {
      return effect
    }

    const attemptId = effect.attempt_started_at
      ? `attempt-${randomUUID()}`
      : effect.attempt_id ?? `attempt-${randomUUID()}`
    const attempt = effect.attempt ?? 1
    const startToCloseTimeoutMs = effect.start_to_close_timeout_ms
    const heartbeatTimeoutMs = effect.heartbeat_timeout_ms
    const maxAttempts = effect.max_attempts ?? 3
    const firstAttemptStartedAt = effect.first_attempt_started_at
      ? iso(effect.first_attempt_started_at)
      : input.now

    const row = await this.one<EffectRow>(
      client,
      `
      UPDATE ${this.table("effects")}
      SET attempt = $1, attempt_id = $2, attempt_owner_id = $3, attempt_started_at = $4::timestamptz,
        start_to_close_timeout_ms = $5, start_to_close_deadline = $6::timestamptz,
        heartbeat_timeout_ms = $7, heartbeat_deadline = $8::timestamptz, max_attempts = $9,
        first_attempt_started_at = $10::timestamptz, next_attempt_at = NULL
      WHERE effect_id = $11 AND status = 'pending'
      RETURNING *
      `,
      [
        attempt,
        attemptId,
        input.workerId,
        input.now,
        startToCloseTimeoutMs,
        deadlineFrom(input.now, startToCloseTimeoutMs),
        heartbeatTimeoutMs,
        deadlineFrom(input.now, heartbeatTimeoutMs),
        maxAttempts,
        firstAttemptStartedAt,
        effect.effect_id,
      ],
    )
    const started = row ?? {
      ...effect,
      attempt,
      attempt_id: attemptId,
      attempt_owner_id: input.workerId,
      attempt_started_at: input.now,
      start_to_close_deadline: deadlineFrom(input.now, startToCloseTimeoutMs),
      heartbeat_deadline: deadlineFrom(input.now, heartbeatTimeoutMs),
      max_attempts: maxAttempts,
      first_attempt_started_at: firstAttemptStartedAt,
      next_attempt_at: null,
    }
    await this.syncActivityDeadline(client, started)
    return started
  }

  private async syncActivityDeadline(client: PoolClient, effect: EffectRow): Promise<void> {
    const deadline = nextActivityDeadline(effect)
    if (!deadline) {
      return
    }
    await client.query(
      `
      INSERT INTO ${this.table("activity_deadlines")} (
        effect_id, workflow_id, run_id, activation_id, partition_shard, deadline_at, timeout_kind
      )
      SELECT $1, $2, $3, $4, i.partition_shard, $5::timestamptz, $6
      FROM ${this.table("instances")} i
      WHERE i.workflow_id = $2 AND i.run_id = $3
      ON CONFLICT (effect_id) DO UPDATE SET
        workflow_id = EXCLUDED.workflow_id,
        run_id = EXCLUDED.run_id,
        activation_id = EXCLUDED.activation_id,
        partition_shard = EXCLUDED.partition_shard,
        deadline_at = EXCLUDED.deadline_at,
        timeout_kind = EXCLUDED.timeout_kind
      `,
      [
        effect.effect_id,
        effect.workflow_id,
        effect.run_id,
        effect.activation_id,
        deadline.deadlineAt,
        deadline.timeoutKind,
      ],
    )
  }

  private async deleteActivityDeadline(client: PoolClient, effectId: string): Promise<void> {
    await client.query(`DELETE FROM ${this.table("activity_deadlines")} WHERE effect_id = $1`, [
      effectId,
    ])
  }

  private async expireActivityTimeouts(
    client: PoolClient,
    now: string,
    scope: { activationId?: string; activationIds?: string[]; shardIds?: number[] },
  ): Promise<void> {
    const activationIds = [
      ...(scope.activationId ? [scope.activationId] : []),
      ...(scope.activationIds ?? []),
    ]
    if (activationIds.length === 0 && (!scope.shardIds || scope.shardIds.length === 0)) {
      return
    }

    const filters: string[] = []
    const params: unknown[] = [now]
    if (activationIds.length > 0) {
      params.push([...new Set(activationIds)])
      filters.push(`d.activation_id = ANY($${params.length}::text[])`)
    }
    if (scope.shardIds && scope.shardIds.length > 0) {
      params.push(scope.shardIds)
      filters.push(`d.partition_shard = ANY($${params.length}::int[])`)
    }

    const expired = await this.rows<ActivityDeadlineRow>(
      client,
      `
      SELECT d.effect_id, d.activation_id
      FROM ${this.table("activity_deadlines")} d
      JOIN ${this.table("activation_tasks")} a ON a.activation_id = d.activation_id
      WHERE d.deadline_at <= $1::timestamptz
        AND a.completed_by_sequence IS NULL
        AND (${filters.join(" OR ")})
      ORDER BY d.deadline_at, d.effect_id
      FOR UPDATE OF d SKIP LOCKED
      `,
      params,
    )
    for (const row of new Map(expired.map((row) => [row.activation_id, row])).values()) {
      await this.expirePendingEffectsForActivation(client, row.activation_id, now)
    }
  }

  private async expirePendingEffectsForActivation(
    client: PoolClient,
    activationId: string,
    now: string,
  ): Promise<void> {
    const effects = await this.rows<EffectRow>(
      client,
      `
      SELECT * FROM ${this.table("effects")}
      WHERE activation_id = $1 AND status = 'pending' AND attempt_started_at IS NOT NULL
      FOR UPDATE
      `,
      [activationId],
    )
    for (const effect of effects) {
      const timeoutKind = effectTimeoutKind(effect, now)
      if (!timeoutKind) {
        await this.fencePendingEffectAttempt(client, effect)
        continue
      }
      const timeoutError = activityTimeoutError(effect, timeoutKind)
      const retry = retryDecision(effect, timeoutError, now, true)
      if (retry.status === "failed") {
        await client.query(
          `
          UPDATE ${this.table("effects")}
          SET status = 'failed', error_json = $1::jsonb, last_failure_json = $1::jsonb,
            timed_out_at = $2::timestamptz, timeout_kind = $3,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
          WHERE effect_id = $4 AND status = 'pending'
          `,
          [encodeJson(timeoutError), now, timeoutKind, effect.effect_id],
        )
        await this.deleteActivityDeadline(client, effect.effect_id)
        this.count("durable.provider.effect", {
          status: "timeout_failed",
          reason: timeoutKind,
        })
        continue
      }
      await client.query(
        `
        UPDATE ${this.table("effects")}
        SET attempt = $1, attempt_id = $2, attempt_owner_id = NULL, attempt_started_at = NULL,
          start_to_close_deadline = NULL, heartbeat_deadline = NULL,
          timed_out_at = $3::timestamptz, timeout_kind = $4,
          next_attempt_at = $5::timestamptz, last_failure_json = $6::jsonb
        WHERE effect_id = $7 AND status = 'pending'
        `,
        [
          retry.nextAttempt,
          `attempt-${randomUUID()}`,
          now,
          timeoutKind,
          retry.nextAttemptAt,
          encodeJson(timeoutError),
          effect.effect_id,
        ],
      )
      await this.deleteActivityDeadline(client, effect.effect_id)
      this.count("durable.provider.effect", {
        status: "timeout_retry",
        reason: timeoutKind,
      })
    }
    await client.query(
      `
      UPDATE ${this.table("activation_tasks")}
      SET owner_id = NULL, lease_until = NULL
      WHERE activation_id = $1 AND completed_by_sequence IS NULL
      `,
      [activationId],
    )
  }

  private async fencePendingEffectAttempt(client: PoolClient, effect: EffectRow): Promise<void> {
    await client.query(
      `
      UPDATE ${this.table("effects")}
      SET attempt_id = $1, attempt_owner_id = NULL, attempt_started_at = NULL,
        start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
      WHERE effect_id = $2 AND status = 'pending'
      `,
      [`attempt-${randomUUID()}`, effect.effect_id],
    )
    await this.deleteActivityDeadline(client, effect.effect_id)
  }

  private async resetPendingEffectsForActivationReclaim(
    client: PoolClient,
    activationId: string,
  ): Promise<void> {
    const effects = await this.rows<EffectRow>(
      client,
      `
      SELECT * FROM ${this.table("effects")}
      WHERE activation_id = $1 AND status = 'pending' AND attempt_started_at IS NOT NULL
      FOR UPDATE
      `,
      [activationId],
    )
    for (const effect of effects) {
      await this.fencePendingEffectAttempt(client, effect)
    }
  }

  private async validateCheckpointChildStarts(
    client: PoolClient,
    input: CommitCheckpointInput,
  ): Promise<{ reason: string; error: SerializedError } | undefined> {
    const seenKeys = new Set<string>()
    const seenRefs = new Map<string, CheckpointChildStart>()
    for (const start of input.childStarts ?? []) {
      if (seenKeys.has(start.key)) {
        return childStartCommitConflict("duplicate_child_start_key", start)
      }
      seenKeys.add(start.key)

      const refKey = `${start.workflowId}\0${start.runId}`
      if (seenRefs.has(refKey) && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("duplicate_child_start_instance", start)
      }
      seenRefs.set(refKey, start)

      const existingForKey = await this.one<ChildRow>(
        client,
        `
        SELECT * FROM ${this.table("children")}
        WHERE parent_workflow_id = $1 AND parent_run_id = $2 AND activation_id = $3 AND key = $4
        LIMIT 1
        FOR UPDATE
        `,
        [input.workflowId, input.runId, input.activationId, start.key],
      )
      if (existingForKey && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("existing_child_activation_key", start)
      }

      const existingInstance = await this.instanceRow(client, start)
      if (existingInstance && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("existing_child_instance", start)
      }
    }
    return undefined
  }

  private async validateCheckpointChildStartsForBatch(
    client: PoolClient,
    inputs: CommitCheckpointInput[],
  ): Promise<Map<number, { reason: string; error: SerializedError }>> {
    const conflicts = new Map<number, { reason: string; error: SerializedError }>()
    const flattened = inputs.flatMap((input, inputIndex) =>
      (input.childStarts ?? []).map((start, startIndex) => ({
        input_index: inputIndex,
        start_index: startIndex,
        parent_workflow_id: input.workflowId,
        parent_run_id: input.runId,
        activation_id: input.activationId,
        key: start.key,
        workflow_id: start.workflowId,
        run_id: start.runId,
        conflict_policy: start.conflictPolicy ?? "use_existing",
      })),
    )
    if (flattened.length === 0) {
      return conflicts
    }

    for (const [inputIndex, input] of inputs.entries()) {
      const seenKeys = new Set<string>()
      const seenRefs = new Set<string>()
      for (const start of input.childStarts ?? []) {
        if (seenKeys.has(start.key)) {
          conflicts.set(inputIndex, childStartCommitConflict("duplicate_child_start_key", start))
          break
        }
        seenKeys.add(start.key)

        const refKey = `${start.workflowId}\0${start.runId}`
        if (seenRefs.has(refKey) && start.conflictPolicy !== "terminate_existing") {
          conflicts.set(inputIndex, childStartCommitConflict("duplicate_child_start_instance", start))
          break
        }
        seenRefs.add(refKey)
      }
    }

    const existingChildRows = await this.rows<IndexedChildStartExistingRow<ChildRow>>(
      client,
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          input_index integer,
          start_index integer,
          parent_workflow_id text,
          parent_run_id text,
          activation_id text,
          key text
        )
      )
      SELECT input.input_index, input.start_index, c.*
      FROM input
      JOIN ${this.table("children")} c
        ON c.parent_workflow_id = input.parent_workflow_id
       AND c.parent_run_id = input.parent_run_id
       AND c.activation_id = input.activation_id
       AND c.key = input.key
      FOR UPDATE OF c
      `,
      [encodeJson(flattened)],
    )
    for (const row of existingChildRows) {
      if (conflicts.has(row.input_index)) {
        continue
      }
      const start = inputs[row.input_index].childStarts?.[row.start_index]
      if (start && start.conflictPolicy !== "terminate_existing") {
        conflicts.set(row.input_index, childStartCommitConflict("existing_child_activation_key", start))
      }
    }

    const existingInstanceRows = await this.rows<IndexedChildStartExistingRow<InstanceRow>>(
      client,
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          input_index integer,
          start_index integer,
          workflow_id text,
          run_id text
        )
      )
      SELECT input.input_index, input.start_index, i.*
      FROM input
      JOIN ${this.table("instances")} i
        ON i.workflow_id = input.workflow_id
       AND i.run_id = input.run_id
      FOR UPDATE OF i
      `,
      [encodeJson(flattened)],
    )
    for (const row of existingInstanceRows) {
      if (conflicts.has(row.input_index)) {
        continue
      }
      const start = inputs[row.input_index].childStarts?.[row.start_index]
      if (start && start.conflictPolicy !== "terminate_existing") {
        conflicts.set(row.input_index, childStartCommitConflict("existing_child_instance", start))
      }
    }

    return conflicts
  }

  private async writeCheckpointChildStarts(
    client: PoolClient,
    input: CommitCheckpointInput,
  ): Promise<void> {
    for (const start of input.childStarts ?? []) {
      const existingForKey = await this.one<ChildRow>(
        client,
        `
        SELECT * FROM ${this.table("children")}
        WHERE parent_workflow_id = $1 AND parent_run_id = $2 AND activation_id = $3 AND key = $4
        LIMIT 1
        FOR UPDATE
        `,
        [input.workflowId, input.runId, input.activationId, start.key],
      )
      if (existingForKey && start.conflictPolicy === "terminate_existing") {
        await this.deleteInstanceRecords(client, existingForKey.workflow_id, existingForKey.run_id)
      }
      if ((await this.instanceRow(client, start)) && start.conflictPolicy === "terminate_existing") {
        await this.deleteInstanceRecords(client, start.workflowId, start.runId)
      }

      const childRecordId = `child-${randomUUID()}`
      await client.query(
        `
        INSERT INTO ${this.table("instances")} (
          workflow_name, workflow_version, workflow_id, run_id, partition_shard,
          sequence, status, common_json, phase_name, phase_data_json, output_json,
          error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
          parent_child_record_id, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, 0, 'running', $6::jsonb, $7, $8::jsonb,
          NULL, NULL, NULL, $9::jsonb, $10, $11, $12, $13::timestamptz, $13::timestamptz
        )
        `,
        [
          start.workflowName,
          start.workflowVersion,
          start.workflowId,
          start.runId,
          start.partitionShard,
          encodeJson(start.common),
          start.phase.name,
          encodeJson(start.phase.data),
          encodeJson(start.waits),
          input.workflowId,
          input.runId,
          childRecordId,
          input.now,
        ],
      )
      await client.query(
        `
        INSERT INTO ${this.table("children")} (
          child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
          workflow_name, workflow_version, workflow_id, run_id, status, parent_close_policy
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'started', $10)
        `,
        [
          childRecordId,
          input.workflowId,
          input.runId,
          input.activationId,
          start.key,
          start.workflowName,
          start.workflowVersion,
          start.workflowId,
          start.runId,
          start.parentClosePolicy ?? "cancel",
        ],
      )
      await this.insertReadyEventsForState(client, {
        workflowName: start.workflowName,
        workflowVersion: start.workflowVersion,
        workflowId: start.workflowId,
        runId: start.runId,
        partitionShard: start.partitionShard,
        sequence: 0,
        status: "running",
        waits: start.waits,
        updatedAt: input.now,
      })
      this.log("info", "provider.child.create", {
        workflowName: start.workflowName,
        workflowId: start.workflowId,
        runId: start.runId,
        childRecordId,
        parentWorkflowId: input.workflowId,
        parentRunId: input.runId,
        activationId: input.activationId,
        key: start.key,
        parentClosePolicy: start.parentClosePolicy ?? "cancel",
        durability: "checkpoint",
      })
      this.count("durable.provider.child", {
        workflowName: start.workflowName,
        status: "created",
      })
    }
  }

  private async writeCheckpointChildStartsForBatch(
    client: PoolClient,
    inputs: CommitCheckpointInput[],
  ): Promise<void> {
    const flattened = inputs.flatMap((input) =>
      (input.childStarts ?? []).map((start) => ({
        child_record_id: `child-${randomUUID()}`,
        parent_workflow_id: input.workflowId,
        parent_run_id: input.runId,
        activation_id: input.activationId,
        key: start.key,
        workflow_name: start.workflowName,
        workflow_version: start.workflowVersion,
        workflow_id: start.workflowId,
        run_id: start.runId,
        partition_shard: start.partitionShard,
        common_json: start.common,
        phase_name: start.phase.name,
        phase_data_json: start.phase.data,
        waits_json: start.waits,
        parent_close_policy: start.parentClosePolicy ?? "cancel",
        conflict_policy: start.conflictPolicy ?? "use_existing",
        now: input.now,
        start,
      })),
    )
    if (flattened.length === 0) {
      return
    }
    if (flattened.some((row) => row.conflict_policy === "terminate_existing")) {
      for (const input of inputs) {
        await this.writeCheckpointChildStarts(client, input)
      }
      return
    }

    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS c(
          child_record_id text,
          parent_workflow_id text,
          parent_run_id text,
          workflow_name text,
          workflow_version integer,
          workflow_id text,
          run_id text,
          partition_shard integer,
          common_json jsonb,
          phase_name text,
          phase_data_json jsonb,
          waits_json jsonb,
          now timestamptz
        )
      )
      INSERT INTO ${this.table("instances")} (
        workflow_name, workflow_version, workflow_id, run_id, partition_shard,
        sequence, status, common_json, phase_name, phase_data_json, output_json,
        error_json, cancel_reason, waits_json, parent_workflow_id, parent_run_id,
        parent_child_record_id, created_at, updated_at
      )
      SELECT
        workflow_name, workflow_version, workflow_id, run_id, partition_shard,
        0, 'running', common_json, phase_name, phase_data_json, NULL,
        NULL, NULL, waits_json, parent_workflow_id, parent_run_id,
        child_record_id, now, now
      FROM input
      `,
      [encodeJson(flattened)],
    )
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS c(
          child_record_id text,
          parent_workflow_id text,
          parent_run_id text,
          activation_id text,
          key text,
          workflow_name text,
          workflow_version integer,
          workflow_id text,
          run_id text,
          parent_close_policy text
        )
      )
      INSERT INTO ${this.table("children")} (
        child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
        workflow_name, workflow_version, workflow_id, run_id, status, parent_close_policy
      )
      SELECT
        child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
        workflow_name, workflow_version, workflow_id, run_id, 'started', parent_close_policy
      FROM input
      `,
      [encodeJson(flattened)],
    )

    for (const row of flattened) {
      await this.insertReadyEventsForState(client, {
        workflowName: row.workflow_name,
        workflowVersion: row.workflow_version,
        workflowId: row.workflow_id,
        runId: row.run_id,
        partitionShard: row.partition_shard,
        sequence: 0,
        status: "running",
        waits: row.waits_json,
        updatedAt: row.now,
      })
      this.log("info", "provider.child.create", {
        workflowName: row.workflow_name,
        workflowId: row.workflow_id,
        runId: row.run_id,
        childRecordId: row.child_record_id,
        parentWorkflowId: row.parent_workflow_id,
        parentRunId: row.parent_run_id,
        activationId: row.activation_id,
        key: row.key,
        parentClosePolicy: row.parent_close_policy,
        durability: "checkpoint",
      })
      this.count("durable.provider.child", {
        workflowName: row.workflow_name,
        status: "created",
      })
    }
  }

  private async writeCheckpointEffectMutations(
    client: PoolClient,
    input: {
      workflowId: string
      runId: string
      activationId: string
      now: string
      effects?: CheckpointEffectMutation[]
    },
  ): Promise<void> {
    await this.writeCheckpointEffectMutationsForBatch(client, [input])
  }

  private async writeCheckpointEffectMutationsForBatch(
    client: PoolClient,
    inputs: Array<{
      workflowId: string
      runId: string
      activationId: string
      now: string
      effects?: CheckpointEffectMutation[]
    }>,
  ): Promise<void> {
    const effects = inputs.flatMap((input) =>
      (input.effects ?? []).map((effect) => {
        const status = effect.status === "retry_scheduled" ? "pending" : effect.status
        return {
          effect_id: `effect-${randomUUID()}`,
          workflow_id: input.workflowId,
          run_id: input.runId,
          activation_id: input.activationId,
          key: effect.key,
          idempotency_key:
            effect.idempotencyKey ??
            `${input.workflowId}/${input.runId}/${input.activationId}/${effect.key}`,
          status,
          attempt:
            effect.status === "retry_scheduled"
              ? effect.nextAttempt
              : effect.attempt ?? 1,
          max_attempts: effect.maxAttempts ?? 3,
          max_elapsed_ms: effect.maxElapsedMs ?? null,
          initial_interval_ms: effect.initialIntervalMs ?? 1_000,
          max_interval_ms: effect.maxIntervalMs ?? 30_000,
          backoff_coefficient: effect.backoffCoefficient ?? 2,
          first_attempt_started_at: effect.firstAttemptStartedAt ?? input.now,
          next_attempt_at:
            effect.status === "retry_scheduled" ? effect.nextAttemptAt : null,
          last_failure_json: effect.status === "completed" ? null : effect.error,
          non_retryable_error_names_json: effect.nonRetryableErrorNames ?? [],
          result_json: effect.status === "completed" ? effect.result : null,
          error_json: effect.status === "completed" ? null : effect.error,
          heartbeat_details_json: effect.heartbeatDetails ?? null,
        }
      }),
    )
    if (effects.length === 0) {
      return
    }
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS e(
          effect_id text,
          workflow_id text,
          run_id text,
          activation_id text,
          key text,
          idempotency_key text,
          status text,
          attempt integer,
          max_attempts integer,
          max_elapsed_ms integer,
          initial_interval_ms integer,
          max_interval_ms integer,
          backoff_coefficient double precision,
          first_attempt_started_at timestamptz,
          next_attempt_at timestamptz,
          last_failure_json jsonb,
          non_retryable_error_names_json jsonb,
          result_json jsonb,
          error_json jsonb,
          heartbeat_details_json jsonb
        )
      )
      INSERT INTO ${this.table("effects")} (
        effect_id, workflow_id, run_id, activation_id, key, idempotency_key, status,
        attempt, attempt_id, attempt_owner_id, attempt_started_at,
        start_to_close_timeout_ms, start_to_close_deadline,
        heartbeat_timeout_ms, heartbeat_deadline, max_attempts,
        max_elapsed_ms, initial_interval_ms, max_interval_ms, backoff_coefficient,
        first_attempt_started_at, next_attempt_at, last_failure_json,
        non_retryable_error_names_json, result_json, error_json, heartbeat_details_json
      )
      SELECT
        effect_id, workflow_id, run_id, activation_id, key, idempotency_key, status,
        attempt, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, max_attempts,
        max_elapsed_ms, initial_interval_ms, max_interval_ms, backoff_coefficient,
        first_attempt_started_at, next_attempt_at, last_failure_json,
        non_retryable_error_names_json, result_json, error_json, heartbeat_details_json
      FROM input
      ON CONFLICT (workflow_id, run_id, activation_id, key) DO UPDATE SET
        status = EXCLUDED.status,
        attempt = EXCLUDED.attempt,
        attempt_id = NULL,
        attempt_owner_id = NULL,
        attempt_started_at = NULL,
        start_to_close_deadline = NULL,
        heartbeat_deadline = NULL,
        max_attempts = EXCLUDED.max_attempts,
        max_elapsed_ms = EXCLUDED.max_elapsed_ms,
        initial_interval_ms = EXCLUDED.initial_interval_ms,
        max_interval_ms = EXCLUDED.max_interval_ms,
        backoff_coefficient = EXCLUDED.backoff_coefficient,
        first_attempt_started_at = EXCLUDED.first_attempt_started_at,
        next_attempt_at = EXCLUDED.next_attempt_at,
        last_failure_json = EXCLUDED.last_failure_json,
        non_retryable_error_names_json = EXCLUDED.non_retryable_error_names_json,
        result_json = EXCLUDED.result_json,
        error_json = EXCLUDED.error_json,
        heartbeat_details_json = EXCLUDED.heartbeat_details_json
      WHERE ${this.table("effects")}.status <> 'completed'
      `,
      [encodeJson(effects)],
    )
  }

  private async writeNextInstance(
    client: PoolClient,
    input: CommitCheckpointInput,
    nextSequence: number,
  ): Promise<void> {
    if (input.next.status === "running") {
      await client.query(
        `
        UPDATE ${this.table("instances")}
        SET workflow_version = $1, sequence = $2, status = 'running',
          common_json = $3::jsonb, phase_name = $4, phase_data_json = $5::jsonb,
          output_json = NULL, error_json = NULL, cancel_reason = NULL,
          waits_json = $6::jsonb, updated_at = $7::timestamptz
        WHERE workflow_id = $8 AND run_id = $9
        `,
        [
          input.workflowVersion,
          nextSequence,
          encodeJson(input.next.common),
          input.next.phase.name,
          encodeJson(input.next.phase.data),
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        ],
      )
      return
    }

    if (input.next.status === "completed") {
      await client.query(
        `
        UPDATE ${this.table("instances")}
        SET workflow_version = $1, sequence = $2, status = 'completed',
          common_json = NULL, phase_name = NULL, phase_data_json = NULL,
          output_json = $3::jsonb, error_json = NULL, cancel_reason = NULL,
          waits_json = $4::jsonb, updated_at = $5::timestamptz
        WHERE workflow_id = $6 AND run_id = $7
        `,
        [
          input.workflowVersion,
          nextSequence,
          encodeJson(toJson(input.next.output)),
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        ],
      )
      return
    }

    if (input.next.status === "canceled") {
      await client.query(
        `
        UPDATE ${this.table("instances")}
        SET workflow_version = $1, sequence = $2, status = 'canceled',
          common_json = NULL, phase_name = NULL, phase_data_json = NULL,
          output_json = NULL, error_json = NULL, cancel_reason = $3,
          waits_json = $4::jsonb, updated_at = $5::timestamptz
        WHERE workflow_id = $6 AND run_id = $7
        `,
        [
          input.workflowVersion,
          nextSequence,
          input.next.reason,
          encodeJson(input.waits),
          input.now,
          input.workflowId,
          input.runId,
        ],
      )
      return
    }

    await client.query(
      `
      UPDATE ${this.table("instances")}
      SET workflow_version = $1, sequence = $2, status = 'failed',
        common_json = NULL, phase_name = NULL, phase_data_json = NULL,
        output_json = NULL, error_json = $3::jsonb, cancel_reason = NULL,
        waits_json = $4::jsonb, updated_at = $5::timestamptz
      WHERE workflow_id = $6 AND run_id = $7
      `,
      [
        input.workflowVersion,
        nextSequence,
        encodeJson(input.next.error),
        encodeJson(input.waits),
        input.now,
        input.workflowId,
      input.runId,
    ],
  )
  }

  private async writeNextInstancesForBatch(
    client: PoolClient,
    successes: Array<{ input: CommitActivationInput; nextSequence: number }>,
  ): Promise<void> {
    const running = successes
      .filter(({ input }) => input.next.status === "running")
      .map(({ input, nextSequence }) => {
        if (input.next.status !== "running") {
          throw new Error("unreachable")
        }
        return {
          workflow_id: input.workflowId,
          run_id: input.runId,
          workflow_version: input.workflowVersion,
          sequence: nextSequence,
          common_json: input.next.common,
          phase_name: input.next.phase.name,
          phase_data_json: input.next.phase.data,
          waits_json: input.waits,
          updated_at: input.now,
        }
      })
    if (running.length > 0) {
      await client.query(
        `
        WITH input AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS i(
            workflow_id text,
            run_id text,
            workflow_version integer,
            sequence integer,
            common_json jsonb,
            phase_name text,
            phase_data_json jsonb,
            waits_json jsonb,
            updated_at timestamptz
          )
        )
        UPDATE ${this.table("instances")} target
        SET workflow_version = input.workflow_version,
          sequence = input.sequence,
          status = 'running',
          common_json = input.common_json,
          phase_name = input.phase_name,
          phase_data_json = input.phase_data_json,
          output_json = NULL,
          error_json = NULL,
          cancel_reason = NULL,
          waits_json = input.waits_json,
          updated_at = input.updated_at
        FROM input
        WHERE target.workflow_id = input.workflow_id
          AND target.run_id = input.run_id
        `,
        [encodeJson(running)],
      )
    }

    const completed = successes
      .filter(({ input }) => input.next.status === "completed")
      .map(({ input, nextSequence }) => {
        if (input.next.status !== "completed") {
          throw new Error("unreachable")
        }
        return {
          workflow_id: input.workflowId,
          run_id: input.runId,
          workflow_version: input.workflowVersion,
          sequence: nextSequence,
          output_json: toJson(input.next.output),
          waits_json: input.waits,
          updated_at: input.now,
        }
      })
    if (completed.length > 0) {
      await client.query(
        `
        WITH input AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS i(
            workflow_id text,
            run_id text,
            workflow_version integer,
            sequence integer,
            output_json jsonb,
            waits_json jsonb,
            updated_at timestamptz
          )
        )
        UPDATE ${this.table("instances")} target
        SET workflow_version = input.workflow_version,
          sequence = input.sequence,
          status = 'completed',
          common_json = NULL,
          phase_name = NULL,
          phase_data_json = NULL,
          output_json = input.output_json,
          error_json = NULL,
          cancel_reason = NULL,
          waits_json = input.waits_json,
          updated_at = input.updated_at
        FROM input
        WHERE target.workflow_id = input.workflow_id
          AND target.run_id = input.run_id
        `,
        [encodeJson(completed)],
      )
    }

    const canceled = successes
      .filter(({ input }) => input.next.status === "canceled")
      .map(({ input, nextSequence }) => {
        if (input.next.status !== "canceled") {
          throw new Error("unreachable")
        }
        return {
          workflow_id: input.workflowId,
          run_id: input.runId,
          workflow_version: input.workflowVersion,
          sequence: nextSequence,
          cancel_reason: input.next.reason,
          waits_json: input.waits,
          updated_at: input.now,
        }
      })
    if (canceled.length > 0) {
      await client.query(
        `
        WITH input AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS i(
            workflow_id text,
            run_id text,
            workflow_version integer,
            sequence integer,
            cancel_reason text,
            waits_json jsonb,
            updated_at timestamptz
          )
        )
        UPDATE ${this.table("instances")} target
        SET workflow_version = input.workflow_version,
          sequence = input.sequence,
          status = 'canceled',
          common_json = NULL,
          phase_name = NULL,
          phase_data_json = NULL,
          output_json = NULL,
          error_json = NULL,
          cancel_reason = input.cancel_reason,
          waits_json = input.waits_json,
          updated_at = input.updated_at
        FROM input
        WHERE target.workflow_id = input.workflow_id
          AND target.run_id = input.run_id
        `,
        [encodeJson(canceled)],
      )
    }

    const failed = successes
      .filter(({ input }) => input.next.status === "failed")
      .map(({ input, nextSequence }) => {
        if (input.next.status !== "failed") {
          throw new Error("unreachable")
        }
        return {
          workflow_id: input.workflowId,
          run_id: input.runId,
          workflow_version: input.workflowVersion,
          sequence: nextSequence,
          error_json: input.next.error,
          waits_json: input.waits,
          updated_at: input.now,
        }
      })
    if (failed.length > 0) {
      await client.query(
        `
        WITH input AS (
          SELECT *
          FROM jsonb_to_recordset($1::jsonb) AS i(
            workflow_id text,
            run_id text,
            workflow_version integer,
            sequence integer,
            error_json jsonb,
            waits_json jsonb,
            updated_at timestamptz
          )
        )
        UPDATE ${this.table("instances")} target
        SET workflow_version = input.workflow_version,
          sequence = input.sequence,
          status = 'failed',
          common_json = NULL,
          phase_name = NULL,
          phase_data_json = NULL,
          output_json = NULL,
          error_json = input.error_json,
          cancel_reason = NULL,
          waits_json = input.waits_json,
          updated_at = input.updated_at
        FROM input
        WHERE target.workflow_id = input.workflow_id
          AND target.run_id = input.run_id
        `,
        [encodeJson(failed)],
      )
    }
  }

  private async consumeSignalsForBatch(
    client: PoolClient,
    successes: Array<{ input: CommitActivationInput; nextSequence: number }>,
  ): Promise<void> {
    const rows = successes
      .filter(({ input }) => input.consumeSignalId)
      .map(({ input, nextSequence }) => ({
        signal_id: input.consumeSignalId,
        consumed_by_sequence: nextSequence,
      }))
    if (rows.length === 0) {
      return
    }
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          signal_id text,
          consumed_by_sequence integer
        )
      )
      UPDATE ${this.table("signals")} target
      SET consumed_by_sequence = input.consumed_by_sequence
      FROM input
      WHERE target.signal_id = input.signal_id
      `,
      [encodeJson(rows)],
    )
  }

  private async consumeChildrenForBatch(
    client: PoolClient,
    successes: Array<{ input: CommitActivationInput; nextSequence: number }>,
  ): Promise<void> {
    const rows = successes
      .filter(({ input }) => input.consumeChildRecordId)
      .map(({ input, nextSequence }) => ({
        child_record_id: input.consumeChildRecordId,
        delivered_by_sequence: nextSequence,
      }))
    if (rows.length === 0) {
      return
    }
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          child_record_id text,
          delivered_by_sequence integer
        )
      )
      UPDATE ${this.table("children")} target
      SET delivered_by_sequence = input.delivered_by_sequence
      FROM input
      WHERE target.child_record_id = input.child_record_id
      `,
      [encodeJson(rows)],
    )
  }

  private async completeActivationTasksForBatch(
    client: PoolClient,
    successes: Array<{ input: CommitActivationInput; nextSequence: number }>,
  ): Promise<void> {
    await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($1::jsonb) AS i(
          activation_id text,
          completed_by_sequence integer,
          completed_at timestamptz,
          worker_id text
        )
      )
      UPDATE ${this.table("activation_tasks")} target
      SET completed_by_sequence = input.completed_by_sequence,
        completed_at = input.completed_at,
        lease_until = input.completed_at,
        owner_id = input.worker_id
      FROM input
      WHERE target.activation_id = input.activation_id
      `,
      [
        encodeJson(
          successes.map(({ input, nextSequence }) => ({
            activation_id: input.activationId,
            completed_by_sequence: nextSequence,
            completed_at: input.now,
            worker_id: input.workerId,
          })),
        ),
      ],
    )
  }

  private async updateParentChildRecord(
    client: PoolClient,
    previous: InstanceRow,
    input: CommitCheckpointInput,
    nextSequence: number,
  ): Promise<void> {
    if (!previous.parent_child_record_id || input.next.status === "running") {
      return
    }
    const existing = await this.one<ChildRow>(
      client,
      `
      SELECT * FROM ${this.table("children")}
      WHERE child_record_id = $1 AND status = 'started'
      LIMIT 1
      FOR UPDATE
      `,
      [previous.parent_child_record_id],
    )
    if (!existing) {
      return
    }
    if (input.next.status === "completed") {
      await client.query(
        `
        UPDATE ${this.table("children")}
        SET status = 'completed', completed_at = $1::timestamptz,
          output_json = $2::jsonb, error_json = NULL
        WHERE child_record_id = $3
        `,
        [input.now, encodeJson(toJson(input.next.output)), existing.child_record_id],
      )
    } else {
      const error =
        input.next.status === "failed"
          ? input.next.error
          : { message: input.next.reason || "Child canceled" }
      await client.query(
        `
        UPDATE ${this.table("children")}
        SET status = 'failed', completed_at = $1::timestamptz,
          output_json = NULL, error_json = $2::jsonb
        WHERE child_record_id = $3
        `,
        [input.now, encodeJson(error), existing.child_record_id],
      )
    }
    await this.replaceReadyEventsForInstance(client, existing.parent_workflow_id, existing.parent_run_id)
    void nextSequence
  }

  private async applyParentClosePolicy(
    client: PoolClient,
    previous: InstanceRow,
    input: CommitCheckpointInput,
    nextSequence: number,
  ): Promise<void> {
    if (input.next.status !== "canceled" && input.next.status !== "failed") {
      return
    }
    await this.closeStartedChildren(
      client,
      previous.workflow_id,
      previous.run_id,
      input.now,
      nextSequence,
      input.next.status,
    )
  }

  private async closeStartedChildren(
    client: PoolClient,
    parentWorkflowId: string,
    parentRunId: string,
    now: string,
    deliveredBySequence: number,
    status: "canceled" | "failed",
  ): Promise<void> {
    const children = await this.rows<ChildRow>(
      client,
      `
      SELECT * FROM ${this.table("children")}
      WHERE parent_workflow_id = $1 AND parent_run_id = $2 AND status = 'started'
      ORDER BY child_record_id
      FOR UPDATE
      `,
      [parentWorkflowId, parentRunId],
    )
    for (const child of children) {
      if (child.parent_close_policy === "abandon") {
        await this.abandonStartedChild(client, child, now, deliveredBySequence)
      } else {
        await this.cancelStartedChild(client, child, now, {
          deliverToParent: false,
          deliveredBySequence,
          reason: parentClosedChildError(status),
        })
      }
    }
  }

  private async cancelStartedChild(
    client: PoolClient,
    child: ChildRow,
    now: string,
    options: {
      deliverToParent: boolean
      deliveredBySequence?: number
      reason: SerializedError
    },
  ): Promise<void> {
    if (child.status !== "started") {
      return
    }
    const event = options.deliverToParent
      ? "provider.child.cancel_started"
      : "provider.child.parent_close_cancel"
    const status = options.deliverToParent ? "cancel_started" : "parent_close_cancel"
    this.log("info", event, {
      workflowName: child.workflow_name,
      workflowId: child.workflow_id,
      runId: child.run_id,
      childRecordId: child.child_record_id,
      parentWorkflowId: child.parent_workflow_id,
      parentRunId: child.parent_run_id,
      deliverToParent: options.deliverToParent,
    })
    this.count("durable.provider.child", {
      workflowName: child.workflow_name,
      status,
    })

    const instance = await this.instanceRow(
      client,
      { workflowId: child.workflow_id, runId: child.run_id },
      true,
    )
    if (instance?.status === "running") {
      const childCloseSequence = instance.sequence + 1
      await client.query(
        `
        UPDATE ${this.table("instances")}
        SET sequence = sequence + 1, status = 'canceled',
          common_json = NULL, phase_name = NULL, phase_data_json = NULL,
          output_json = NULL, error_json = NULL, cancel_reason = $1,
          waits_json = $2::jsonb, updated_at = $3::timestamptz
        WHERE workflow_id = $4 AND run_id = $5 AND status = 'running'
        `,
        [options.reason.message, encodeJson([]), now, child.workflow_id, child.run_id],
      )
      await client.query(
        `
        UPDATE ${this.table("activation_tasks")}
        SET owner_id = NULL, lease_until = NULL
        WHERE workflow_id = $1 AND run_id = $2 AND completed_by_sequence IS NULL
        `,
        [child.workflow_id, child.run_id],
      )
      await this.replaceReadyEventsForInstance(client, child.workflow_id, child.run_id)
      await this.closeStartedChildren(
        client,
        child.workflow_id,
        child.run_id,
        now,
        childCloseSequence,
        "canceled",
      )
    }

    await client.query(
      `
      UPDATE ${this.table("children")}
      SET status = 'failed', completed_at = $1::timestamptz,
        output_json = NULL, error_json = $2::jsonb, delivered_by_sequence = $3
      WHERE child_record_id = $4 AND status = 'started'
      `,
      [
        now,
        encodeJson(options.reason),
        options.deliverToParent ? null : (options.deliveredBySequence ?? null),
        child.child_record_id,
      ],
    )
    if (options.deliverToParent) {
      await this.replaceReadyEventsForInstance(client, child.parent_workflow_id, child.parent_run_id)
    }
  }

  private async abandonStartedChild(
    client: PoolClient,
    child: ChildRow,
    now: string,
    deliveredBySequence: number,
  ): Promise<void> {
    await client.query(
      `
      UPDATE ${this.table("children")}
      SET status = 'abandoned', completed_at = $1::timestamptz,
        output_json = NULL, error_json = NULL, delivered_by_sequence = $2
      WHERE child_record_id = $3 AND status = 'started'
      `,
      [now, deliveredBySequence, child.child_record_id],
    )
    this.log("info", "provider.child.parent_close_abandon", {
      workflowName: child.workflow_name,
      workflowId: child.workflow_id,
      runId: child.run_id,
      childRecordId: child.child_record_id,
      parentWorkflowId: child.parent_workflow_id,
      parentRunId: child.parent_run_id,
    })
    this.count("durable.provider.child", {
      workflowName: child.workflow_name,
      status: "parent_close_abandon",
    })
  }

  private async deleteInstanceRecords(
    client: PoolClient,
    workflowId: string,
    runId: string,
  ): Promise<void> {
    await client.query(
      `
      DELETE FROM ${this.table("children")}
      WHERE (parent_workflow_id = $1 AND parent_run_id = $2)
        OR (workflow_id = $1 AND run_id = $2)
      `,
      [workflowId, runId],
    )
    await client.query(
      `DELETE FROM ${this.table("instances")} WHERE workflow_id = $1 AND run_id = $2`,
      [workflowId, runId],
    )
  }

  private async throwEffectMutationError(input: {
    workflowId: string
    runId: string
    activationId: string
    effectId: string
  }): Promise<never> {
    const effect = await this.effectRow(this.pool, input)
    if (!effect) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
    if (effect.status === "pending") {
      throw new Error(`Lost effect attempt: ${input.effectId}`)
    }
    throw new Error(`Effect is already terminal: ${input.effectId}`)
  }

  private log(
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: Record<string, unknown>,
  ): void {
    logDurable(this.observability, level, event, fields)
  }

  private count(name: string, tags?: DurableMetricTags): void {
    countDurable(this.observability, name, 1, tags)
  }

  private gauge(name: string, value: number, tags?: DurableMetricTags): void {
    gaugeDurable(this.observability, name, value, tags)
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

function encodeJson(value: unknown): string {
  return JSON.stringify(value)
}

function decodeJson<T>(raw: unknown, fallback: T): T {
  if (raw === null || raw === undefined) {
    return fallback
  }
  if (typeof raw === "string") {
    return raw as T
  }
  return clone(raw) as T
}

function iso(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString()
}

function addMs(isoValue: string, ms: number): string {
  return new Date(new Date(isoValue).getTime() + ms).toISOString()
}

function deadlineFrom(now: string, timeoutMs: number | null): string | null {
  return timeoutMs === null ? null : addMs(now, timeoutMs)
}

function sortKey(...parts: string[]): string {
  return parts.join("\u001f")
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

function readyCandidateLimit(limit: number): number {
  return Math.min(256, Math.max(limit * 4, limit + 16))
}

function compareReadyCandidates(left: ReadyCandidate, right: ReadyCandidate): number {
  const leftKey = left.sort.join("\u0000")
  const rightKey = right.sort.join("\u0000")
  if (leftKey < rightKey) {
    return -1
  }
  if (leftKey > rightKey) {
    return 1
  }
  return 0
}

function readyCandidateFromRow(row: ReadyEventJoinedRow): ReadyCandidate | null {
  const instance = instanceRowFromReadyEventJoinedRow(row)
  const readyAt = iso(row.ready_at)
  if (row.kind === "run") {
    if (!row.activation_id) {
      return null
    }
    return {
      kind: "run",
      activationId: row.activation_id,
      workflowName: row.workflow_name,
      workflowId: row.workflow_id,
      runId: row.run_id,
      sequence: row.sequence,
      activationTime: readyAt,
      leaseUntil: "",
      sort: [row.sort_key],
      instance,
    }
  }
  if (row.kind === "signal" || row.kind === "timer" || row.kind === "child") {
    if (!row.activation_id || !row.wait_name) {
      return null
    }
    return {
      kind: "event",
      activationId: row.activation_id,
      workflowName: row.workflow_name,
      workflowId: row.workflow_id,
      runId: row.run_id,
      sequence: row.sequence,
      activationTime: readyAt,
      waitName: row.wait_name,
      eventKind: row.kind,
      waitJson: row.wait_json,
      eventJson: row.event_json,
      leaseUntil: "",
      sort: [row.sort_key],
      instance,
    }
  }
  return null
}

function stripCandidateMetadata(candidate: ReadyCandidate): ClaimedActivation {
  if (candidate.kind === "migration") {
    return {
      kind: "migration",
      activationId: candidate.activationId,
      workflowName: candidate.workflowName,
      workflowId: candidate.workflowId,
      runId: candidate.runId,
      sequence: candidate.sequence,
      activationTime: candidate.activationTime,
      leaseUntil: candidate.leaseUntil,
    }
  }
  if (candidate.kind === "run") {
    return {
      kind: "run",
      activationId: candidate.activationId,
      workflowName: candidate.workflowName,
      workflowId: candidate.workflowId,
      runId: candidate.runId,
      sequence: candidate.sequence,
      activationTime: candidate.activationTime,
      leaseUntil: candidate.leaseUntil,
    }
  }
  const waitName = candidate.waitName
  if (!waitName) {
    throw new Error(`Claimed event activation is missing wait name: ${candidate.activationId}`)
  }
  return {
    kind: "event",
    activationId: candidate.activationId,
    workflowName: candidate.workflowName,
    workflowId: candidate.workflowId,
    runId: candidate.runId,
    sequence: candidate.sequence,
    activationTime: candidate.activationTime,
    waitName,
    wait: decodeJson<Exclude<DurableWait, { kind: "run" }>>(candidate.waitJson, {
      kind: "timer",
      name: waitName,
      fireAt: candidate.activationTime,
    }),
    event: decodeJson<ReadyEvent>(candidate.eventJson, {
      kind: "timer",
      firedAt: candidate.activationTime,
      occurredAt: candidate.activationTime,
    }),
    leaseUntil: candidate.leaseUntil,
  }
}

function instanceRowFromReadyEventJoinedRow(row: ReadyEventJoinedRow): InstanceRow {
  return {
    workflow_name: row.instance_workflow_name,
    workflow_version: row.instance_workflow_version,
    workflow_id: row.instance_workflow_id,
    run_id: row.instance_run_id,
    partition_shard: row.instance_partition_shard,
    sequence: row.instance_sequence,
    status: row.instance_status,
    common_json: row.instance_common_json,
    phase_name: row.instance_phase_name,
    phase_data_json: row.instance_phase_data_json,
    output_json: row.instance_output_json,
    error_json: row.instance_error_json,
    cancel_reason: row.instance_cancel_reason,
    waits_json: row.instance_waits_json,
    parent_workflow_id: row.instance_parent_workflow_id,
    parent_run_id: row.instance_parent_run_id,
    parent_child_record_id: row.instance_parent_child_record_id,
    created_at: row.instance_created_at,
    updated_at: row.instance_updated_at,
  }
}

function activationClaimFromCommitRow(row: CommitClaimRow): ActivationClaimRow | null {
  if (
    !row.claim_activation_id ||
    !row.claim_workflow_id ||
    !row.claim_run_id ||
    row.claim_sequence === null ||
    !row.claim_kind
  ) {
    return null
  }
  return {
    activation_id: row.claim_activation_id,
    workflow_id: row.claim_workflow_id,
    run_id: row.claim_run_id,
    sequence: row.claim_sequence,
    kind: row.claim_kind,
    wait_name: row.claim_wait_name,
    event_json: row.claim_event_json,
    wait_json: row.claim_wait_json,
    owner_id: row.claim_owner_id,
    lease_until: row.claim_lease_until,
    activation_time: row.claim_activation_time,
    completed_by_sequence: row.claim_completed_by_sequence,
  }
}

function readyEventStateFromInstanceRow(row: InstanceRow): ReadyEventInstanceState {
  return {
    workflowName: row.workflow_name,
    workflowVersion: row.workflow_version,
    workflowId: row.workflow_id,
    runId: row.run_id,
    partitionShard: row.partition_shard,
    sequence: row.sequence,
    status: row.status,
    waits: decodeJson<DurableWait[]>(row.waits_json, []),
    updatedAt: iso(row.updated_at),
  }
}

function workflowVersionClause(
  alias: string,
  workflows: Array<[string, { version: number }]>,
  startIndex: number,
  operator = "=",
): { sql: string; params: unknown[]; nextIndex: number } {
  const prefix = alias ? `${alias}.` : ""
  const clauses: string[] = []
  const params: unknown[] = []
  let index = startIndex
  for (const [name, workflow] of workflows) {
    clauses.push(
      `(${prefix}workflow_name = $${index} AND ${prefix}workflow_version ${operator} $${index + 1})`,
    )
    params.push(name, workflow.version)
    index += 2
  }
  return {
    sql: clauses.length > 0 ? clauses.join(" OR ") : "FALSE",
    params,
    nextIndex: index,
  }
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values
    .filter((value): value is string => Boolean(value))
    .sort()[0]
}

function claimMatchesCommit(claim: ActivationClaimRow, input: CommitCheckpointInput): boolean {
  if (claim.kind === "run" || claim.kind === "migration") {
    return !input.consumeSignalId && !input.consumeChildRecordId
  }
  const event = decodeJson<ReadyEvent | null>(claim.event_json, null)
  if (!event) {
    return false
  }
  if (event.kind === "signal") {
    return input.consumeSignalId === event.consumeSignalId && !input.consumeChildRecordId
  }
  if (event.kind === "child") {
    return input.consumeChildRecordId === event.childRecordId && !input.consumeSignalId
  }
  return !input.consumeSignalId && !input.consumeChildRecordId
}

function rowToSignalRecord(row: SignalRow): SignalRecord {
  return {
    signalId: row.signal_id,
    workflowId: row.workflow_id,
    runId: row.run_id,
    type: row.type,
    payload: decodeJson<JsonValue>(row.payload_json, null),
    receivedAt: iso(row.received_at),
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
    parentClosePolicy: row.parent_close_policy,
    completedAt: row.completed_at ? iso(row.completed_at) : undefined,
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
    attempt: row.attempt ?? undefined,
    attemptId: row.attempt_id ?? undefined,
    attemptOwnerId: row.attempt_owner_id ?? undefined,
    attemptStartedAt: row.attempt_started_at ? iso(row.attempt_started_at) : undefined,
    startToCloseTimeoutMs: row.start_to_close_timeout_ms ?? undefined,
    startToCloseDeadline: row.start_to_close_deadline ? iso(row.start_to_close_deadline) : undefined,
    heartbeatTimeoutMs: row.heartbeat_timeout_ms ?? undefined,
    heartbeatDeadline: row.heartbeat_deadline ? iso(row.heartbeat_deadline) : undefined,
    maxAttempts: row.max_attempts ?? undefined,
    maxElapsedMs: row.max_elapsed_ms ?? undefined,
    initialIntervalMs: row.initial_interval_ms ?? undefined,
    maxIntervalMs: row.max_interval_ms ?? undefined,
    backoffCoefficient: row.backoff_coefficient === null ? undefined : Number(row.backoff_coefficient),
    firstAttemptStartedAt: row.first_attempt_started_at ? iso(row.first_attempt_started_at) : undefined,
    nextAttemptAt: row.next_attempt_at ? iso(row.next_attempt_at) : undefined,
    lastFailure: decodeJson<SerializedError | undefined>(row.last_failure_json, undefined),
    nonRetryableErrorNames: decodeJson<string[] | undefined>(
      row.non_retryable_error_names_json,
      undefined,
    ),
    timedOutAt: row.timed_out_at ? iso(row.timed_out_at) : undefined,
    timeoutKind: row.timeout_kind ?? undefined,
    result: decodeJson<JsonValue | undefined>(row.result_json, undefined),
    error: decodeJson<SerializedError | undefined>(row.error_json, undefined),
    heartbeatAt: row.heartbeat_at ? iso(row.heartbeat_at) : undefined,
    heartbeatDetails: decodeJson<JsonValue | undefined>(row.heartbeat_details_json, undefined),
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

function childCanceledError(): SerializedError {
  return { name: "ChildCanceled", message: "Child canceled by parent" }
}

function parentClosedChildError(status: "canceled" | "failed"): SerializedError {
  return {
    name: "ParentClosed",
    message: `Child canceled because parent ${status}`,
  }
}

function childStartCommitConflict(
  reason: string,
  start: CheckpointChildStart,
): { reason: string; error: SerializedError } {
  return {
    reason,
    error: {
      name: "ChildStartConflict",
      message: `Child start ${start.key} failed: ${reason} (${start.workflowId}/${start.runId})`,
    },
  }
}

function shouldCommitSequentially(inputs: CommitActivationInput[]): boolean {
  const activationIds = new Set<string>()
  const instanceSequences = new Set<string>()
  const childRefs = new Set<string>()
  for (const input of inputs) {
    if (activationIds.has(input.activationId)) {
      return true
    }
    activationIds.add(input.activationId)

    const instanceSequenceKey = `${input.workflowId}\0${input.runId}\0${input.expectedSequence}`
    if (instanceSequences.has(instanceSequenceKey)) {
      return true
    }
    instanceSequences.add(instanceSequenceKey)

    for (const start of input.childStarts ?? []) {
      if (start.conflictPolicy === "terminate_existing") {
        return true
      }
      const refKey = `${start.workflowId}\0${start.runId}`
      if (childRefs.has(refKey)) {
        return true
      }
      childRefs.add(refKey)
    }
  }
  return false
}

type NormalizedEffectOptions = {
  startToCloseTimeoutMs: number | null
  heartbeatTimeoutMs: number | null
  maxAttempts: number
  maxElapsedMs: number | null
  initialIntervalMs: number
  maxIntervalMs: number
  backoffCoefficient: number
  nonRetryableErrorNames: string[]
}

function normalizeEffectOptions(input: ReserveEffectInput): NormalizedEffectOptions {
  const retry = input.options?.retry
  const maxAttempts = retry?.maxAttempts ?? input.maxAttempts ?? 3
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error("activity retry.maxAttempts must be a positive integer")
  }
  const initialIntervalMs = retry?.initialIntervalMs ?? 1_000
  const maxIntervalMs = retry?.maxIntervalMs ?? 30_000
  const backoffCoefficient = retry?.backoffCoefficient ?? 2
  const startToCloseTimeoutMs = optionalPositiveInteger(
    input.options?.startToCloseTimeoutMs,
    "startToCloseTimeoutMs",
  )
  const heartbeatTimeoutMs = optionalPositiveInteger(
    input.options?.heartbeatTimeoutMs,
    "heartbeatTimeoutMs",
  )
  const maxElapsedMs = optionalPositiveInteger(retry?.maxElapsedMs, "retry.maxElapsedMs")
  return {
    startToCloseTimeoutMs,
    heartbeatTimeoutMs,
    maxAttempts,
    maxElapsedMs,
    initialIntervalMs,
    maxIntervalMs,
    backoffCoefficient,
    nonRetryableErrorNames: retry?.nonRetryableErrorNames ?? [],
  }
}

function optionalPositiveInteger(value: number | null | undefined, name: string): number | null {
  if (value === undefined || value === null) {
    return null
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`activity ${name} must be a positive integer when provided`)
  }
  return value
}

function retryDecision(
  effect: EffectRow,
  error: SerializedError,
  now: string,
  retryable: boolean,
): FailEffectResult | { status: "retry_scheduled"; nextAttemptAt: string; nextAttempt: number } {
  if (!retryable || isNonRetryableEffectError(effect, error)) {
    return { status: "failed" }
  }
  const attempt = effect.attempt ?? 1
  const maxAttempts = effect.max_attempts ?? 1
  if (attempt >= maxAttempts) {
    return { status: "failed" }
  }
  const firstAttemptStartedAt = effect.first_attempt_started_at
    ? iso(effect.first_attempt_started_at)
    : now
  const maxElapsedMs = effect.max_elapsed_ms
  const initialIntervalMs = effect.initial_interval_ms ?? 1_000
  const maxIntervalMs = effect.max_interval_ms ?? 30_000
  const backoffCoefficient =
    effect.backoff_coefficient === null ? 2 : Number(effect.backoff_coefficient)
  const rawDelay = initialIntervalMs * Math.pow(backoffCoefficient, Math.max(0, attempt - 1))
  const delayMs = Math.min(maxIntervalMs, Math.max(0, Math.round(rawDelay)))
  const nextAttemptAt = addMs(now, delayMs)
  if (
    maxElapsedMs !== null &&
    new Date(nextAttemptAt).getTime() - new Date(firstAttemptStartedAt).getTime() > maxElapsedMs
  ) {
    return { status: "failed" }
  }
  return {
    status: "retry_scheduled",
    nextAttemptAt,
    nextAttempt: attempt + 1,
  }
}

function isNonRetryableEffectError(effect: EffectRow, error: SerializedError): boolean {
  const name = error.name
  if (!name) {
    return false
  }
  return decodeJson<string[]>(effect.non_retryable_error_names_json, []).includes(name)
}

function effectTimeoutKind(
  effect: EffectRow,
  now: string,
): "heartbeat" | "start_to_close" | undefined {
  const start = effect.start_to_close_deadline ? iso(effect.start_to_close_deadline) : undefined
  const heartbeat = effect.heartbeat_deadline ? iso(effect.heartbeat_deadline) : undefined
  if (heartbeat && heartbeat <= now && (!start || heartbeat <= start)) {
    return "heartbeat"
  }
  if (start && start <= now) {
    return "start_to_close"
  }
  return undefined
}

function nextActivityDeadline(
  effect: EffectRow,
): { deadlineAt: string; timeoutKind: "heartbeat" | "start_to_close" } | undefined {
  if (effect.status !== "pending" || !effect.attempt_started_at) {
    return undefined
  }
  const start = effect.start_to_close_deadline ? iso(effect.start_to_close_deadline) : undefined
  const heartbeat = effect.heartbeat_deadline ? iso(effect.heartbeat_deadline) : undefined
  if (heartbeat && (!start || heartbeat <= start)) {
    return { deadlineAt: heartbeat, timeoutKind: "heartbeat" }
  }
  if (start) {
    return { deadlineAt: start, timeoutKind: "start_to_close" }
  }
  return undefined
}

function activityTimeoutError(
  effect: EffectRow,
  timeoutKind: "heartbeat" | "start_to_close",
): SerializedError {
  const label =
    timeoutKind === "start_to_close" ? "start-to-close timeout" : "heartbeat timeout"
  return {
    name: "ActivityTimeoutError",
    message: `Activity ${effect.key} failed due to ${label}`,
  }
}

function requireAttemptId(effect: EffectRow): string {
  if (!effect.attempt_id) {
    throw new Error(`Effect attempt is missing attempt id: ${effect.effect_id}`)
  }
  return effect.attempt_id
}
