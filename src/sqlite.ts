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
  ClaimReadyActivationResult,
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
  partition_shard: number
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
  workflow_id: string
  run_id: string
  activation_id: string
  key: string
  idempotency_key: string
  status: "pending" | "completed" | "failed"
  attempt: number | null
  attempt_id: string | null
  attempt_owner_id: string | null
  attempt_started_at: string | null
  start_to_close_timeout_ms: number | null
  start_to_close_deadline: string | null
  heartbeat_timeout_ms: number | null
  heartbeat_deadline: string | null
  max_attempts: number | null
  max_elapsed_ms: number | null
  initial_interval_ms: number | null
  max_interval_ms: number | null
  backoff_coefficient: number | null
  first_attempt_started_at: string | null
  next_attempt_at: string | null
  last_failure_json: string | null
  non_retryable_error_names_json: string | null
  timed_out_at: string | null
  timeout_kind: "heartbeat" | "start_to_close" | null
  result_json: string | null
  error_json: string | null
  heartbeat_at: string | null
  heartbeat_details_json: string | null
}

type ActivationClaimRow = {
  activation_id: string
  workflow_id: string
  run_id: string
  sequence: number
  kind: "migration" | "run" | "event"
  wait_name: string | null
  event_json: string | null
  wait_json: string | null
  owner_id: string | null
  lease_until: string | null
  activation_time: string | null
  completed_by_sequence: number | null
}

type DispatchShardRow = {
  shard_id: number
  owner_id: string | null
  lease_until: string | null
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
  ready_at: string
  sort_key: string
  wait_json: string | null
  event_json: string | null
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

      this.replaceReadyEventsForInstance(input.workflowId, input.runId)

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

      this.replaceReadyEventsForInstance(input.workflowId, input.runId)

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

      this.replaceReadyEventsForInstance(input.workflowId, input.runId)

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
    const result = this.db
      .prepare(
        `
        UPDATE dispatch_shards
        SET lease_until = ?
        WHERE shard_id = ? AND owner_id = ? AND lease_until >= ?
      `,
      )
      .run(addMs(input.now, input.leaseMs), input.shardId, input.ownerId, input.now)
    if (result.changes === 0) {
      throw new Error(`Lost dispatch shard lease: ${input.shardId}`)
    }
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

  async claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult> {
    const claim = this.db.transaction(() => {
      const ownedShards = input.shardIds.filter((shardId) => {
        const shard = oneRow<DispatchShardRow>(
          this.db.prepare("SELECT * FROM dispatch_shards WHERE shard_id = ?").get(shardId),
        )
        return shard?.owner_id === input.workerId && (shard.lease_until ?? "") >= input.now
      })

      if (ownedShards.length === 0) {
        return { activation: null }
      }

      this.expireActivityTimeouts(input.now, { shardIds: ownedShards })

      const candidates = this.indexedReadyCandidates({
        ...input,
        shardIds: ownedShards,
      })

      for (const candidate of candidates) {
        if (this.tryWriteActivationClaim(candidate, input.workerId, input.now, input.leaseMs)) {
          return { activation: stripSort(candidate) }
        }
      }

      return {
        activation: null,
        nextWakeAt: this.nextWakeAt(ownedShards, input.now, input.workflows),
      }
    })

    return claim.immediate() as ClaimReadyActivationResult
  }

  async heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    const heartbeat = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      return this.db
        .prepare(
          `
          UPDATE activation_claims
          SET lease_until = ?
          WHERE activation_id = ? AND owner_id = ? AND completed_by_sequence IS NULL
            AND lease_until >= ?
        `,
        )
        .run(addMs(input.now, input.leaseMs), input.activationId, input.workerId, input.now)
    })
    const result = heartbeat.immediate() as SqliteRunResult
    if (result.changes === 0) {
      throw new Error(`Lost activation lease: ${input.activationId}`)
    }
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
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const options = normalizeEffectOptions(input)

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
        const started = this.ensureEffectAttemptStarted(existing, input)
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

      const effectId = `effect-${randomUUID()}`
      const attemptId = `attempt-${randomUUID()}`
      const idempotencyKey = `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`
      this.db
        .prepare(
          `
          INSERT INTO effects (
            effect_id, workflow_id, run_id, activation_id, key, idempotency_key, status,
            attempt, attempt_id, attempt_owner_id, attempt_started_at,
            start_to_close_timeout_ms, start_to_close_deadline,
            heartbeat_timeout_ms, heartbeat_deadline, max_attempts,
            max_elapsed_ms, initial_interval_ms, max_interval_ms, backoff_coefficient,
            first_attempt_started_at, next_attempt_at, non_retryable_error_names_json
          ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        `,
        )
        .run(
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
          input.now,
          encodeJson(options.nonRetryableErrorNames),
        )

      return {
        status: "reserved",
        effectId,
        idempotencyKey,
        attempt: 1,
        attemptId,
      } satisfies EffectReservation
    })

    return reserve.immediate() as EffectReservation
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    const heartbeat = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const effect = this.effectRow(input)
      const heartbeatDeadline = effect?.heartbeat_timeout_ms
        ? addMs(input.now, effect.heartbeat_timeout_ms)
        : null
      return this.db
        .prepare(
          `
          UPDATE effects
          SET heartbeat_at = ?, heartbeat_details_json = ?, heartbeat_deadline = ?
          WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND effect_id = ?
            AND attempt_id = ? AND status = 'pending'
        `,
        )
        .run(
          input.now,
          encodeJson(input.details ?? null),
          heartbeatDeadline,
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
        )
    })
    const result = heartbeat.immediate() as SqliteRunResult
    if (result.changes === 0) {
      this.throwEffectMutationError(input)
    }
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    const complete = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      return this.db
        .prepare(
          `
          UPDATE effects
          SET status = 'completed', result_json = ?, error_json = NULL,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL
          WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND effect_id = ?
            AND attempt_id = ? AND status = 'pending'
        `,
        )
        .run(
          encodeJson(input.result),
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
        )
    })
    const result = complete.immediate() as SqliteRunResult
    if (result.changes === 0) {
      this.throwEffectMutationError(input)
    }
  }

  async failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    const fail = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const effect = this.effectRow(input)
      if (!effect || effect.status !== "pending" || effect.attempt_id !== input.attemptId) {
        return { result: null, changes: 0 }
      }

      const retry = retryDecision(effect, input.error, input.now, input.retryable !== false)
      if (retry.status === "retry_scheduled") {
        const result = this.db
          .prepare(
            `
            UPDATE effects
            SET attempt = ?, attempt_id = ?, attempt_owner_id = NULL, attempt_started_at = NULL,
              start_to_close_deadline = NULL, heartbeat_deadline = NULL,
              next_attempt_at = ?, last_failure_json = ?
            WHERE effect_id = ? AND attempt_id = ? AND status = 'pending'
          `,
          )
          .run(
            retry.nextAttempt,
            `attempt-${randomUUID()}`,
            retry.nextAttemptAt,
            encodeJson(input.error),
            input.effectId,
            input.attemptId,
        )
        return { result: retry, changes: result.changes }
      }

      const result = this.db
        .prepare(
          `
          UPDATE effects
          SET status = 'failed', error_json = ?, last_failure_json = ?,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
          WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND effect_id = ?
            AND attempt_id = ? AND status = 'pending'
        `,
        )
        .run(
          encodeJson(input.error),
          encodeJson(input.error),
          input.workflowId,
          input.runId,
          input.activationId,
          input.effectId,
          input.attemptId,
        )
      return { result: { status: "failed" } satisfies FailEffectResult, changes: result.changes }
    })

    const output = fail.immediate() as { result: FailEffectResult | null; changes: number }
    if (output.changes === 0 || !output.result) {
      this.throwEffectMutationError(input)
    }
    return output.result
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const commit = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })

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
        claim.workflow_id !== input.workflowId ||
        claim.run_id !== input.runId ||
        claim.sequence !== input.expectedSequence ||
        claim.owner_id !== input.workerId ||
        !claim.lease_until ||
        claim.lease_until < input.now ||
        claim.completed_by_sequence !== null
      ) {
        return { ok: false, sequence: instance.sequence }
      }

      if (!claimMatchesCommit(claim, input)) {
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
      this.replaceReadyEventsForInstance(input.workflowId, input.runId)

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
        attempt INTEGER NOT NULL DEFAULT 1,
        attempt_id TEXT,
        attempt_owner_id TEXT,
        attempt_started_at TEXT,
        start_to_close_timeout_ms INTEGER,
        start_to_close_deadline TEXT,
        heartbeat_timeout_ms INTEGER,
        heartbeat_deadline TEXT,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        max_elapsed_ms INTEGER,
        initial_interval_ms INTEGER NOT NULL DEFAULT 1000,
        max_interval_ms INTEGER NOT NULL DEFAULT 30000,
        backoff_coefficient REAL NOT NULL DEFAULT 2,
        first_attempt_started_at TEXT,
        next_attempt_at TEXT,
        last_failure_json TEXT,
        non_retryable_error_names_json TEXT,
        timed_out_at TEXT,
        timeout_kind TEXT CHECK (timeout_kind IS NULL OR timeout_kind IN ('heartbeat', 'start_to_close')),
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

      CREATE TABLE IF NOT EXISTS ready_events (
        ready_event_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        partition_shard INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('migration', 'run', 'signal', 'timer', 'child')),
        wait_name TEXT,
        activation_id TEXT,
        ready_at TEXT NOT NULL,
        sort_key TEXT NOT NULL,
        wait_json TEXT,
        event_json TEXT,
        FOREIGN KEY (workflow_id, run_id) REFERENCES instances(workflow_id, run_id) ON DELETE CASCADE
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
      CREATE INDEX IF NOT EXISTS effects_activation_pending
        ON effects(activation_id, status);
      CREATE INDEX IF NOT EXISTS effects_start_deadline
        ON effects(status, start_to_close_deadline);
      CREATE INDEX IF NOT EXISTS effects_heartbeat_deadline
        ON effects(status, heartbeat_deadline);
      CREATE INDEX IF NOT EXISTS effects_next_attempt
        ON effects(status, next_attempt_at);
      CREATE INDEX IF NOT EXISTS activation_claims_owner_lease
        ON activation_claims(owner_id, lease_until, completed_by_sequence);
      CREATE INDEX IF NOT EXISTS ready_events_claim
        ON ready_events(partition_shard, ready_at, sort_key);
      CREATE INDEX IF NOT EXISTS ready_events_instance
        ON ready_events(workflow_id, run_id, sequence);
    `)
    this.addColumnIfMissing("activation_claims", "activation_time", "TEXT")
    this.addColumnIfMissing("effects", "attempt", "INTEGER DEFAULT 1")
    this.addColumnIfMissing("effects", "attempt_id", "TEXT")
    this.addColumnIfMissing("effects", "attempt_owner_id", "TEXT")
    this.addColumnIfMissing("effects", "attempt_started_at", "TEXT")
    this.addColumnIfMissing("effects", "start_to_close_timeout_ms", "INTEGER")
    this.addColumnIfMissing("effects", "start_to_close_deadline", "TEXT")
    this.addColumnIfMissing("effects", "heartbeat_timeout_ms", "INTEGER")
    this.addColumnIfMissing("effects", "heartbeat_deadline", "TEXT")
    this.addColumnIfMissing("effects", "max_attempts", "INTEGER DEFAULT 3")
    this.addColumnIfMissing("effects", "max_elapsed_ms", "INTEGER")
    this.addColumnIfMissing("effects", "initial_interval_ms", "INTEGER DEFAULT 1000")
    this.addColumnIfMissing("effects", "max_interval_ms", "INTEGER DEFAULT 30000")
    this.addColumnIfMissing("effects", "backoff_coefficient", "REAL DEFAULT 2")
    this.addColumnIfMissing("effects", "first_attempt_started_at", "TEXT")
    this.addColumnIfMissing("effects", "next_attempt_at", "TEXT")
    this.addColumnIfMissing("effects", "last_failure_json", "TEXT")
    this.addColumnIfMissing("effects", "non_retryable_error_names_json", "TEXT")
    this.addColumnIfMissing("effects", "timed_out_at", "TEXT")
    this.addColumnIfMissing("effects", "timeout_kind", "TEXT")
    this.backfillEffectAttemptColumns()
    this.rebuildAllReadyEvents()
  }

  private addColumnIfMissing(table: string, column: string, definition: string): void {
    const columns = this.db.prepare(`PRAGMA table_info(${table})`).all()
    const exists = columns.some((row) => isPlainObject(row) && row.name === column)
    if (!exists) {
      this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
    }
  }

  private backfillEffectAttemptColumns(): void {
    const rows = this.db
      .prepare(
        `
        SELECT * FROM effects
        WHERE attempt IS NULL OR attempt_id IS NULL OR max_attempts IS NULL
          OR initial_interval_ms IS NULL OR max_interval_ms IS NULL OR backoff_coefficient IS NULL
      `,
      )
      .all()
      .map((row) => requireRow<EffectRow>(row))

    for (const row of rows) {
      this.db
        .prepare(
          `
          UPDATE effects
          SET attempt = ?, attempt_id = ?, max_attempts = ?,
            initial_interval_ms = ?, max_interval_ms = ?, backoff_coefficient = ?,
            first_attempt_started_at = COALESCE(first_attempt_started_at, attempt_started_at)
          WHERE effect_id = ?
        `,
      )
        .run(
          row.attempt ?? 1,
          row.attempt_id ?? `attempt-${randomUUID()}`,
          row.max_attempts ?? 3,
          row.initial_interval_ms ?? 1_000,
          row.max_interval_ms ?? 30_000,
          row.backoff_coefficient ?? 2,
          row.effect_id,
        )
    }
  }

  private effectRow(input: {
    workflowId: string
    runId: string
    activationId: string
    effectId: string
  }): EffectRow | null {
    return oneRow<EffectRow>(
      this.db
        .prepare(
          `
          SELECT * FROM effects
          WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND effect_id = ?
          LIMIT 1
        `,
        )
        .get(input.workflowId, input.runId, input.activationId, input.effectId),
    )
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

  private rebuildAllReadyEvents(): void {
    const rebuild = this.db.transaction(() => {
      this.db.prepare("DELETE FROM ready_events").run()
      const rows = this.db
        .prepare("SELECT workflow_id, run_id FROM instances WHERE status = 'running'")
        .all()
      for (const row of rows) {
        const instance = requireRow<{ workflow_id: string; run_id: string }>(row)
        this.replaceReadyEventsForInstance(instance.workflow_id, instance.run_id)
      }
    })
    rebuild.immediate()
  }

  private replaceReadyEventsForInstance(workflowId: string, runId: string): void {
    this.db.prepare("DELETE FROM ready_events WHERE workflow_id = ? AND run_id = ?").run(workflowId, runId)

    const instance = this.instanceRow({ workflowId, runId })
    if (!instance || instance.status !== "running") {
      return
    }

    this.insertReadyEvent({
      readyEventId: `${instance.workflow_id}/${instance.run_id}/${instance.sequence}/migration`,
      workflowId: instance.workflow_id,
      runId: instance.run_id,
      workflowName: instance.workflow_name,
      workflowVersion: instance.workflow_version,
      partitionShard: instance.partition_shard,
      sequence: instance.sequence,
      kind: "migration",
      waitName: null,
      activationId: null,
      readyAt: instance.updated_at,
      sortKey: sortKey(instance.updated_at, "migration", instance.workflow_id, instance.run_id),
      wait: null,
      event: null,
    })

    for (const wait of decodeJson<DurableWait[]>(instance.waits_json, [])) {
      if (wait.kind === "run") {
        const activationId = activationIdFromParts(
          instance.workflow_id,
          instance.run_id,
          instance.sequence,
          "run",
          wait.name,
        )
        this.insertReadyEvent({
          readyEventId: activationId,
          workflowId: instance.workflow_id,
          runId: instance.run_id,
          workflowName: instance.workflow_name,
          workflowVersion: instance.workflow_version,
          partitionShard: instance.partition_shard,
          sequence: instance.sequence,
          kind: "run",
          waitName: wait.name,
          activationId,
          readyAt: wait.readyAt,
          sortKey: sortKey(wait.readyAt, "run", wait.name, instance.workflow_id, instance.run_id),
          wait,
          event: null,
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

        const activationId = activationIdFromParts(
          instance.workflow_id,
          instance.run_id,
          instance.sequence,
          "signal",
          signalRow.signal_id,
        )
        const event: ReadyEvent = {
          kind: "signal",
          signalId: signalRow.signal_id,
          payload: decodeJson<JsonValue>(signalRow.payload_json, null),
          occurredAt: signalRow.received_at,
          consumeSignalId: signalRow.signal_id,
        }
        this.insertReadyEvent({
          readyEventId: `${activationId}/${wait.name}`,
          workflowId: instance.workflow_id,
          runId: instance.run_id,
          workflowName: instance.workflow_name,
          workflowVersion: instance.workflow_version,
          partitionShard: instance.partition_shard,
          sequence: instance.sequence,
          kind: "signal",
          waitName: wait.name,
          activationId,
          readyAt: signalRow.received_at,
          sortKey: sortKey(signalRow.received_at, "signal", wait.name, signalRow.signal_id),
          wait,
          event,
        })
      } else if (wait.kind === "timer") {
        const activationId = activationIdFromParts(
          instance.workflow_id,
          instance.run_id,
          instance.sequence,
          "timer",
          `${wait.name}:${wait.fireAt}`,
        )
        const event: ReadyEvent = {
          kind: "timer",
          firedAt: wait.fireAt,
          occurredAt: wait.fireAt,
        }
        this.insertReadyEvent({
          readyEventId: activationId,
          workflowId: instance.workflow_id,
          runId: instance.run_id,
          workflowName: instance.workflow_name,
          workflowVersion: instance.workflow_version,
          partitionShard: instance.partition_shard,
          sequence: instance.sequence,
          kind: "timer",
          waitName: wait.name,
          activationId,
          readyAt: wait.fireAt,
          sortKey: sortKey(wait.fireAt, "timer", wait.name, `${wait.name}:${wait.fireAt}`),
          wait,
          event,
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

        const occurredAt = childRow.completed_at ?? instance.updated_at
        const activationId = activationIdFromParts(
          instance.workflow_id,
          instance.run_id,
          instance.sequence,
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
        this.insertReadyEvent({
          readyEventId: activationId,
          workflowId: instance.workflow_id,
          runId: instance.run_id,
          workflowName: instance.workflow_name,
          workflowVersion: instance.workflow_version,
          partitionShard: instance.partition_shard,
          sequence: instance.sequence,
          kind: "child",
          waitName: wait.name,
          activationId,
          readyAt: occurredAt,
          sortKey: sortKey(occurredAt, "child", wait.name, childRow.child_record_id),
          wait,
          event,
        })
      }
    }
  }

  private insertReadyEvent(input: {
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
  }): void {
    this.db
      .prepare(
        `
        INSERT INTO ready_events (
          ready_event_id, workflow_id, run_id, workflow_name, workflow_version,
          partition_shard, sequence, kind, wait_name, activation_id, ready_at,
          sort_key, wait_json, event_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      )
      .run(
        input.readyEventId,
        input.workflowId,
        input.runId,
        input.workflowName,
        input.workflowVersion,
        input.partitionShard,
        input.sequence,
        input.kind,
        input.waitName,
        input.activationId,
        input.readyAt,
        input.sortKey,
        input.wait ? encodeJson(input.wait) : null,
        input.event ? encodeJson(input.event) : null,
      )
  }

  private indexedReadyCandidates(input: ClaimReadyActivationInput): ReadyCandidate[] {
    const workflowEntries = Object.entries(input.workflows)
    if (input.shardIds.length === 0 || workflowEntries.length === 0) {
      return []
    }

    const placeholders = input.shardIds.map(() => "?").join(", ")
    const migrationClauses = workflowEntries
      .map(() => "(kind = 'migration' AND workflow_name = ? AND workflow_version < ?)")
      .join(" OR ")
    const currentClauses = workflowEntries
      .map(() => "(kind <> 'migration' AND workflow_name = ? AND workflow_version = ?)")
      .join(" OR ")
    const versionParams = [
      ...workflowEntries.flatMap(([name, workflow]) => [name, workflow.version]),
      ...workflowEntries.flatMap(([name, workflow]) => [name, workflow.version]),
    ]
    const rows = this.db
      .prepare(
        `
        SELECT * FROM ready_events
        WHERE partition_shard IN (${placeholders}) AND ready_at <= ?
          AND (${migrationClauses} OR ${currentClauses})
        ORDER BY sort_key
      `,
      )
      .all(...input.shardIds, input.now, ...versionParams)
      .map((row) => requireRow<ReadyEventRow>(row))

    const candidates: ReadyCandidate[] = []
    for (const row of rows) {
      const instance = this.instanceRow({ workflowId: row.workflow_id, runId: row.run_id })
      if (!instance || instance.status !== "running" || instance.sequence !== row.sequence) {
        this.db.prepare("DELETE FROM ready_events WHERE ready_event_id = ?").run(row.ready_event_id)
        continue
      }

      const workflow = input.workflows[instance.workflow_name]
      if (!workflow) {
        continue
      }

      if (row.kind === "migration") {
        if (instance.workflow_version >= workflow.version) {
          continue
        }
        if (this.hasUncompletedNonMigrationActivation(instance)) {
          continue
        }
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
          activationTime: row.ready_at,
          leaseUntil: "",
          sort: [row.sort_key],
        })
        continue
      }

      if (instance.workflow_version !== workflow.version) {
        continue
      }

      const candidate = this.readyCandidateFromRow(row, instance)
      if (candidate) {
        const blockedUntil = this.activationRetryBlockedUntil(candidate.activationId, input.now)
        if (blockedUntil) {
          continue
        }
        candidates.push(candidate)
      }
    }

    return candidates
  }

  private readyCandidateFromRow(row: ReadyEventRow, instance: InstanceRow): ReadyCandidate | null {
    if (row.kind === "run") {
      if (!row.activation_id) {
        return null
      }
      return {
        kind: "run",
        activationId: row.activation_id,
        workflowName: instance.workflow_name,
        workflowId: instance.workflow_id,
        runId: instance.run_id,
        sequence: instance.sequence,
        activationTime: row.ready_at,
        leaseUntil: "",
        sort: [row.sort_key],
      }
    }

    if (row.kind === "signal" || row.kind === "timer" || row.kind === "child") {
      if (!row.activation_id || !row.wait_name) {
        return null
      }
      return {
        kind: "event",
        activationId: row.activation_id,
        workflowName: instance.workflow_name,
        workflowId: instance.workflow_id,
        runId: instance.run_id,
        sequence: instance.sequence,
        activationTime: row.ready_at,
        waitName: row.wait_name,
        wait: decodeJson<Exclude<DurableWait, { kind: "run" }>>(row.wait_json, {
          kind: "timer",
          name: row.wait_name,
          fireAt: row.ready_at,
        }),
        event: decodeJson<ReadyEvent>(row.event_json, {
          kind: "timer",
          firedAt: row.ready_at,
          occurredAt: row.ready_at,
        }),
        leaseUntil: "",
        sort: [row.sort_key],
      }
    }

    return null
  }

  private hasUncompletedNonMigrationActivation(instance: InstanceRow): boolean {
    return Boolean(
      oneRow(
        this.db
          .prepare(
            `
            SELECT activation_id FROM activation_claims
            WHERE workflow_id = ? AND run_id = ? AND sequence = ?
              AND kind <> 'migration' AND completed_by_sequence IS NULL
            LIMIT 1
          `,
          )
          .get(instance.workflow_id, instance.run_id, instance.sequence),
      ),
    )
  }

  private activationRetryBlockedUntil(activationId: string, now: string): string | undefined {
    const row = oneRow<{ blocked_until: string | null }>(
      this.db
        .prepare(
          `
          SELECT MAX(next_attempt_at) AS blocked_until
          FROM effects
          WHERE activation_id = ? AND status = 'pending'
            AND next_attempt_at IS NOT NULL AND next_attempt_at > ?
        `,
        )
        .get(activationId, now),
    )
    return row?.blocked_until ?? undefined
  }

  private ensureEffectAttemptStarted(effect: EffectRow, input: ReserveEffectInput): EffectRow {
    if (effect.next_attempt_at && effect.next_attempt_at > input.now) {
      throw new Error(`Effect retry is not ready until ${effect.next_attempt_at}: ${effect.effect_id}`)
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
    const firstAttemptStartedAt = effect.first_attempt_started_at ?? input.now

    this.db
      .prepare(
        `
        UPDATE effects
        SET attempt = ?, attempt_id = ?, attempt_owner_id = ?, attempt_started_at = ?,
          start_to_close_timeout_ms = ?, start_to_close_deadline = ?,
          heartbeat_timeout_ms = ?, heartbeat_deadline = ?, max_attempts = ?,
          first_attempt_started_at = ?, next_attempt_at = NULL
        WHERE effect_id = ? AND status = 'pending'
      `,
      )
      .run(
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
      )

    return {
      ...effect,
      attempt,
      attempt_id: attemptId,
      attempt_owner_id: input.workerId,
      attempt_started_at: input.now,
      start_to_close_timeout_ms: startToCloseTimeoutMs,
      start_to_close_deadline: deadlineFrom(input.now, startToCloseTimeoutMs),
      heartbeat_timeout_ms: heartbeatTimeoutMs,
      heartbeat_deadline: deadlineFrom(input.now, heartbeatTimeoutMs),
      max_attempts: maxAttempts,
      first_attempt_started_at: firstAttemptStartedAt,
      next_attempt_at: null,
    }
  }

  private expireActivityTimeouts(
    now: string,
    scope: { activationId?: string; shardIds?: number[] },
  ): void {
    if (!scope.activationId && (!scope.shardIds || scope.shardIds.length === 0)) {
      return
    }

    const filters: string[] = []
    const params: SqliteValue[] = []
    if (scope.activationId) {
      filters.push("e.activation_id = ?")
      params.push(scope.activationId)
    }
    if (scope.shardIds && scope.shardIds.length > 0) {
      filters.push(`i.partition_shard IN (${scope.shardIds.map(() => "?").join(", ")})`)
      params.push(...scope.shardIds)
    }

    const expired = this.db
      .prepare(
        `
        SELECT DISTINCT e.activation_id
        FROM effects e
        JOIN activation_claims a ON a.activation_id = e.activation_id
        JOIN instances i ON i.workflow_id = e.workflow_id AND i.run_id = e.run_id
        WHERE e.status = 'pending'
          AND e.attempt_started_at IS NOT NULL
          AND a.completed_by_sequence IS NULL
          AND (${filters.join(" OR ")})
          AND (
            (e.start_to_close_deadline IS NOT NULL AND e.start_to_close_deadline <= ?)
            OR (e.heartbeat_deadline IS NOT NULL AND e.heartbeat_deadline <= ?)
          )
      `,
      )
      .all(...params, now, now)
      .map((row) => requireRow<{ activation_id: string }>(row).activation_id)

    for (const activationId of expired) {
      this.expirePendingEffectsForActivation(activationId, now)
    }
  }

  private expirePendingEffectsForActivation(activationId: string, now: string): void {
    const effects = this.db
      .prepare(
        `
        SELECT * FROM effects
        WHERE activation_id = ? AND status = 'pending' AND attempt_started_at IS NOT NULL
      `,
      )
      .all(activationId)
      .map((row) => requireRow<EffectRow>(row))

    for (const effect of effects) {
      const timeoutKind = effectTimeoutKind(effect, now)
      if (!timeoutKind) {
        this.fencePendingEffectAttempt(effect)
        continue
      }

      const timeoutError = activityTimeoutError(effect, timeoutKind)
      const retry = retryDecision(effect, timeoutError, now, true)
      if (retry.status === "failed") {
        this.db
          .prepare(
            `
            UPDATE effects
            SET status = 'failed', error_json = ?, last_failure_json = ?,
              timed_out_at = ?, timeout_kind = ?,
              start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
            WHERE effect_id = ? AND status = 'pending'
          `,
          )
          .run(
            encodeJson(timeoutError),
            encodeJson(timeoutError),
            now,
            timeoutKind,
            effect.effect_id,
          )
        continue
      }

      this.db
        .prepare(
          `
          UPDATE effects
          SET attempt = ?, attempt_id = ?, attempt_owner_id = NULL, attempt_started_at = NULL,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL,
            timed_out_at = ?, timeout_kind = ?, next_attempt_at = ?, last_failure_json = ?
          WHERE effect_id = ? AND status = 'pending'
        `,
        )
        .run(
          retry.nextAttempt,
          `attempt-${randomUUID()}`,
          now,
          timeoutKind,
          retry.nextAttemptAt,
          encodeJson(timeoutError),
          effect.effect_id,
        )
    }

    this.db
      .prepare(
        `
        UPDATE activation_claims
        SET owner_id = NULL, lease_until = NULL
        WHERE activation_id = ? AND completed_by_sequence IS NULL
      `,
      )
      .run(activationId)
  }

  private fencePendingEffectAttempt(effect: EffectRow): void {
    this.db
      .prepare(
        `
        UPDATE effects
        SET attempt_id = ?, attempt_owner_id = NULL, attempt_started_at = NULL,
          start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
        WHERE effect_id = ? AND status = 'pending'
      `,
      )
      .run(`attempt-${randomUUID()}`, effect.effect_id)
  }

  private nextWakeAt(
    shardIds: number[],
    now: string,
    workflows: Record<string, { version: number }>,
  ): string | undefined {
    const workflowEntries = Object.entries(workflows)
    if (shardIds.length === 0 || workflowEntries.length === 0) {
      return undefined
    }

    const placeholders = shardIds.map(() => "?").join(", ")
    const versionClauses = workflowEntries.map(() => "(workflow_name = ? AND workflow_version = ?)").join(" OR ")
    const versionParams = workflowEntries.flatMap(([name, workflow]) => [name, workflow.version])
    const row = oneRow<{ ready_at: string }>(
      this.db
        .prepare(
          `
          SELECT ready_at FROM ready_events
          WHERE partition_shard IN (${placeholders})
            AND kind IN ('run', 'timer')
            AND ready_at > ?
            AND (${versionClauses})
          ORDER BY ready_at
          LIMIT 1
        `,
        )
        .get(...shardIds, now, ...versionParams),
    )
    const effectDeadline = oneRow<{ deadline: string }>(
      this.db
        .prepare(
          `
          SELECT MIN(deadline) AS deadline FROM (
            SELECT e.start_to_close_deadline AS deadline
            FROM effects e
            JOIN instances i ON i.workflow_id = e.workflow_id AND i.run_id = e.run_id
            JOIN activation_claims a ON a.activation_id = e.activation_id
            WHERE i.partition_shard IN (${placeholders})
              AND i.status = 'running'
              AND e.status = 'pending'
              AND e.start_to_close_deadline IS NOT NULL
              AND e.start_to_close_deadline > ?
              AND a.completed_by_sequence IS NULL
              AND (${versionClauses})
            UNION ALL
            SELECT e.heartbeat_deadline AS deadline
            FROM effects e
            JOIN instances i ON i.workflow_id = e.workflow_id AND i.run_id = e.run_id
            JOIN activation_claims a ON a.activation_id = e.activation_id
            WHERE i.partition_shard IN (${placeholders})
              AND i.status = 'running'
              AND e.status = 'pending'
              AND e.heartbeat_deadline IS NOT NULL
              AND e.heartbeat_deadline > ?
              AND a.completed_by_sequence IS NULL
              AND (${versionClauses})
          )
        `,
        )
        .get(
          ...shardIds,
          now,
          ...versionParams,
          ...shardIds,
          now,
          ...versionParams,
        ),
    )

    const retryWake = oneRow<{ ready_at: string | null }>(
      this.db
        .prepare(
          `
          SELECT MIN(blocked_until) AS ready_at
          FROM (
            SELECT e.activation_id, MAX(e.next_attempt_at) AS blocked_until
            FROM effects e
            JOIN instances i ON i.workflow_id = e.workflow_id AND i.run_id = e.run_id
            WHERE i.partition_shard IN (${placeholders})
              AND i.status = 'running'
              AND e.status = 'pending'
              AND e.next_attempt_at IS NOT NULL
              AND e.next_attempt_at > ?
              AND (${versionClauses})
            GROUP BY e.activation_id
          )
        `,
        )
        .get(...shardIds, now, ...versionParams),
    )

    return earliestIso(row?.ready_at, effectDeadline?.deadline, retryWake?.ready_at ?? undefined)
  }

  private tryWriteActivationClaim(
    candidate: ReadyCandidate,
    workerId: string,
    now: string,
    leaseMs: number,
  ): boolean {
    const leaseUntil = addMs(now, leaseMs)
    const existing = oneRow<ActivationClaimRow>(
      this.db.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(candidate.activationId),
    )
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
    if (shouldResetPendingEffectAttemptsOnClaim(existing, workerId, now)) {
      this.resetPendingEffectsForActivationReclaim(candidate.activationId)
    }
    candidate.leaseUntil = leaseUntil
    return true
  }

  private resetPendingEffectsForActivationReclaim(activationId: string): void {
    const effects = this.db
      .prepare(
        `
        SELECT * FROM effects
        WHERE activation_id = ? AND status = 'pending' AND attempt_started_at IS NOT NULL
      `,
      )
      .all(activationId)
      .map((row) => requireRow<EffectRow>(row))

    for (const effect of effects) {
      this.db
        .prepare(
          `
          UPDATE effects
          SET attempt_id = ?, attempt_owner_id = NULL, attempt_started_at = NULL,
            start_to_close_deadline = NULL, heartbeat_deadline = NULL, next_attempt_at = NULL
          WHERE effect_id = ? AND status = 'pending'
        `,
        )
        .run(`attempt-${randomUUID()}`, effect.effect_id)
    }
  }

  private assertLiveActivationLease(input: {
    workflowId: string
    runId: string
    activationId: string
    workerId: string
    now: string
  }): ActivationClaimRow {
    const claim = oneRow<ActivationClaimRow>(
      this.db.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(input.activationId),
    )
    if (
      !claim ||
      claim.workflow_id !== input.workflowId ||
      claim.run_id !== input.runId ||
      claim.owner_id !== input.workerId ||
      !claim.lease_until ||
      claim.lease_until < input.now ||
      claim.completed_by_sequence !== null
    ) {
      throw new Error(`Lost activation lease: ${input.activationId}`)
    }
    return claim
  }

  private throwEffectMutationError(input: {
    workflowId: string
    runId: string
    activationId: string
    effectId: string
  }): never {
    const effect = this.effectRow(input)
    if (!effect) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
    if (effect.status === "pending") {
      throw new Error(`Lost effect attempt: ${input.effectId}`)
    }
    throw new Error(`Effect is already terminal: ${input.effectId}`)
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

    this.replaceReadyEventsForInstance(existing.parent_workflow_id, existing.parent_run_id)
    void nextSequence
  }

  private deleteInstanceRecords(workflowId: string, runId: string): void {
    this.db
      .prepare(
        `
        DELETE FROM children
        WHERE (parent_workflow_id = ? AND parent_run_id = ?)
          OR (workflow_id = ? AND run_id = ?)
      `,
      )
      .run(workflowId, runId, workflowId, runId)
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

function claimMatchesCommit(claim: ActivationClaimRow, input: CommitCheckpointInput): boolean {
  if (claim.kind !== "event") {
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

function shouldResetPendingEffectAttemptsOnClaim(
  existing: ActivationClaimRow | null,
  workerId: string,
  now: string,
): boolean {
  if (!existing || existing.completed_by_sequence !== null) {
    return false
  }
  if (existing.owner_id === workerId && existing.lease_until && existing.lease_until > now) {
    return false
  }
  return true
}

function sortKey(...parts: string[]): string {
  return parts.join("\u0000")
}

function stripSort(candidate: ReadyCandidate): ClaimedActivation {
  const { sort, ...activation } = candidate
  return activation
}

function addMs(iso: string, ms: number): string {
  return new Date(new Date(iso).getTime() + ms).toISOString()
}

function deadlineFrom(now: string, timeoutMs: number | null): string | null {
  return timeoutMs === null ? null : addMs(now, timeoutMs)
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
  const startToCloseTimeoutMs = normalizeOptionalTimeout(
    input.options?.startToCloseTimeoutMs,
    "startToCloseTimeoutMs",
  )
  const heartbeatTimeoutMs = normalizeOptionalTimeout(
    input.options?.heartbeatTimeoutMs,
    "heartbeatTimeoutMs",
  )
  const requestedMaxAttempts = input.maxAttempts ?? input.options?.retry?.maxAttempts
  const maxAttempts = requestedMaxAttempts ?? 3
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error("activity retry.maxAttempts must be a positive integer")
  }
  const maxElapsedMs = normalizeOptionalTimeout(input.options?.retry?.maxElapsedMs, "retry.maxElapsedMs")
  const initialIntervalMs = normalizeRetryInterval(
    input.options?.retry?.initialIntervalMs ?? 1_000,
    "retry.initialIntervalMs",
  )
  const maxIntervalMs = normalizeRetryInterval(
    input.options?.retry?.maxIntervalMs ?? 30_000,
    "retry.maxIntervalMs",
  )
  const backoffCoefficient = input.options?.retry?.backoffCoefficient ?? 2
  if (!Number.isFinite(backoffCoefficient) || backoffCoefficient < 1) {
    throw new Error("activity retry.backoffCoefficient must be greater than or equal to 1")
  }
  const nonRetryableErrorNames = input.options?.retry?.nonRetryableErrorNames ?? []
  if (
    !Array.isArray(nonRetryableErrorNames) ||
    nonRetryableErrorNames.some((name) => typeof name !== "string" || name.length === 0)
  ) {
    throw new Error("activity retry.nonRetryableErrorNames must be non-empty strings")
  }
  return {
    startToCloseTimeoutMs,
    heartbeatTimeoutMs,
    maxAttempts,
    maxElapsedMs,
    initialIntervalMs,
    maxIntervalMs,
    backoffCoefficient,
    nonRetryableErrorNames,
  }
}

function normalizeOptionalTimeout(value: number | null | undefined, name: string): number | null {
  if (value == null) {
    return null
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`activity ${name} must be a positive integer when provided`)
  }
  return value
}

function normalizeRetryInterval(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`activity ${name} must be a non-negative integer`)
  }
  return value
}

function requireAttemptId(effect: EffectRow): string {
  if (!effect.attempt_id) {
    throw new Error(`Effect has no active attempt: ${effect.effect_id}`)
  }
  return effect.attempt_id
}

function effectTimeoutKind(
  effect: EffectRow,
  now: string,
): "heartbeat" | "start_to_close" | null {
  const startExpired =
    effect.start_to_close_deadline !== null && effect.start_to_close_deadline <= now
  const heartbeatExpired = effect.heartbeat_deadline !== null && effect.heartbeat_deadline <= now

  if (startExpired && heartbeatExpired) {
    return effect.start_to_close_deadline! <= effect.heartbeat_deadline!
      ? "start_to_close"
      : "heartbeat"
  }
  if (startExpired) {
    return "start_to_close"
  }
  return heartbeatExpired ? "heartbeat" : null
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

function retryDecision(
  effect: EffectRow,
  error: SerializedError,
  now: string,
  retryable: boolean,
): FailEffectResult {
  if (!retryable || isStoredNonRetryable(effect, error)) {
    return { status: "failed" }
  }

  const attempt = effect.attempt ?? 1
  const maxAttempts = effect.max_attempts ?? 3
  if (attempt >= maxAttempts) {
    return { status: "failed" }
  }

  const firstAttemptStartedAt = effect.first_attempt_started_at ?? effect.attempt_started_at ?? now
  const nextAttempt = attempt + 1
  const delayMs = retryDelayMs(effect, attempt)
  const nextAttemptAt = addMs(now, delayMs)
  if (effect.max_elapsed_ms !== null) {
    const maxElapsedAt = addMs(firstAttemptStartedAt, effect.max_elapsed_ms)
    if (nextAttemptAt > maxElapsedAt) {
      return { status: "failed" }
    }
  }

  return { status: "retry_scheduled", nextAttemptAt, nextAttempt }
}

function retryDelayMs(effect: EffectRow, failedAttempt: number): number {
  const initial = effect.initial_interval_ms ?? 1_000
  const max = effect.max_interval_ms ?? 30_000
  const coefficient = effect.backoff_coefficient ?? 2
  const exponential = initial * coefficient ** Math.max(0, failedAttempt - 1)
  return Math.min(max, Math.round(exponential))
}

function isStoredNonRetryable(effect: EffectRow, error: SerializedError): boolean {
  if (!error.name) {
    return false
  }
  return decodeJson<string[]>(effect.non_retryable_error_names_json, []).includes(error.name)
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values.filter((value): value is string => typeof value === "string").sort()[0]
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
    attempt: row.attempt ?? undefined,
    attemptId: row.attempt_id ?? undefined,
    attemptOwnerId: row.attempt_owner_id ?? undefined,
    attemptStartedAt: row.attempt_started_at ?? undefined,
    startToCloseTimeoutMs: row.start_to_close_timeout_ms ?? undefined,
    startToCloseDeadline: row.start_to_close_deadline ?? undefined,
    heartbeatTimeoutMs: row.heartbeat_timeout_ms ?? undefined,
    heartbeatDeadline: row.heartbeat_deadline ?? undefined,
    maxAttempts: row.max_attempts ?? undefined,
    maxElapsedMs: row.max_elapsed_ms ?? undefined,
    initialIntervalMs: row.initial_interval_ms ?? undefined,
    maxIntervalMs: row.max_interval_ms ?? undefined,
    backoffCoefficient: row.backoff_coefficient ?? undefined,
    firstAttemptStartedAt: row.first_attempt_started_at ?? undefined,
    nextAttemptAt: row.next_attempt_at ?? undefined,
    lastFailure: decodeJson<SerializedError | undefined>(row.last_failure_json, undefined),
    nonRetryableErrorNames: decodeJson<string[] | undefined>(
      row.non_retryable_error_names_json,
      undefined,
    ),
    timedOutAt: row.timed_out_at ?? undefined,
    timeoutKind: row.timeout_kind ?? undefined,
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
