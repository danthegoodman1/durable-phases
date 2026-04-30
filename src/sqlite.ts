import { randomUUID } from "node:crypto"
import { mkdirSync } from "node:fs"
import { createRequire } from "node:module"
import { dirname } from "node:path"
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
import {
  countDurable,
  gaugeDurable,
  logDurable,
} from "./observability.js"
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

export type SqliteDurabilityProviderOptions = DurableObservability & {
  busyTimeoutMs?: number
  synchronous?: "full" | "normal"
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
  waitJson?: string | null
  eventJson?: string | null
}

type BufferedObservation =
  | {
      kind: "log"
      level: "debug" | "info" | "warn" | "error"
      event: string
      fields?: Record<string, unknown>
    }
  | {
      kind: "count"
      name: string
      tags?: DurableMetricTags
    }
  | {
      kind: "gauge"
      name: string
      value: number
      tags?: DurableMetricTags
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
  status: "started" | "completed" | "failed" | "abandoned"
  parent_close_policy: "cancel" | "abandon"
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

type ReadyEventJoinedRow = ReadyEventRow & {
  instance_workflow_name: string
  instance_workflow_version: number
  instance_workflow_id: string
  instance_run_id: string
  instance_partition_shard: number
  instance_sequence: number
  instance_status: "running" | "completed" | "canceled" | "failed"
  instance_common_json: string | null
  instance_phase_name: string | null
  instance_phase_data_json: string | null
  instance_output_json: string | null
  instance_error_json: string | null
  instance_cancel_reason: string | null
  instance_waits_json: string
  instance_parent_workflow_id: string | null
  instance_parent_run_id: string | null
  instance_parent_child_record_id: string | null
  instance_created_at: string
  instance_updated_at: string
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

export class SqliteDurabilityProvider implements DurabilityProvider {
  private readonly db: SqliteDatabase
  private readonly busyTimeoutMs: number
  private readonly synchronous: "full" | "normal"
  private readonly observability: DurableObservability
  private readonly statements = new Map<string, SqliteStatement>()
  private observabilityBuffer?: BufferedObservation[]
  private closed = false

  constructor(
    private readonly filePath: string,
    options: SqliteDurabilityProviderOptions = {},
  ) {
    this.busyTimeoutMs = options.busyTimeoutMs ?? 5_000
    this.synchronous = normalizeSqliteSynchronous(options.synchronous)
    this.observability = { logger: options.logger, metrics: options.metrics }
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
    this.statements.clear()
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

      this.prepare(
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

      this.replaceReadyEventsForState({
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

      const ref = { workflowId: input.workflowId, runId: input.runId }
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
      return ref
    })

    return this.withBufferedObservability(() => create.immediate() as InstanceRef)
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    const create = this.db.transaction(() => {
      this.assertLiveActivationLease({
        workflowId: input.parentWorkflowId,
        runId: input.parentRunId,
        activationId: input.activationId,
        workerId: input.workerId,
        now: input.leaseNow,
      })
      const conflictPolicy = input.conflictPolicy ?? "use_existing"
      const parentClosePolicy = input.parentClosePolicy ?? "cancel"
      const existing = oneRow<ChildRow>(
        this.prepare(
            `
            SELECT * FROM children
            WHERE parent_workflow_id = ? AND parent_run_id = ? AND activation_id = ? AND key = ?
            LIMIT 1
          `,
          )
          .get(input.parentWorkflowId, input.parentRunId, input.activationId, input.key),
      )

      if (existing) {
        if (conflictPolicy === "fail") {
          this.log("warn", "provider.child.create_conflict", {
            workflowName: input.workflowName,
            workflowId: input.workflowId,
            runId: input.runId,
            parentWorkflowId: input.parentWorkflowId,
            parentRunId: input.parentRunId,
            activationId: input.activationId,
            key: input.key,
            reason: "existing_parent_activation_key",
          })
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
          this.deleteInstanceRecords(existing.workflow_id, existing.run_id)
          this.log("info", "provider.child.terminate_existing", {
            workflowName: input.workflowName,
            workflowId: existing.workflow_id,
            runId: existing.run_id,
            parentWorkflowId: input.parentWorkflowId,
            parentRunId: input.parentRunId,
            activationId: input.activationId,
            key: input.key,
          })
        } else {
          this.log("debug", "provider.child.use_existing", {
            workflowName: existing.workflow_name,
            workflowId: existing.workflow_id,
            runId: existing.run_id,
            parentWorkflowId: input.parentWorkflowId,
            parentRunId: input.parentRunId,
            activationId: input.activationId,
            key: input.key,
          })
          this.count("durable.provider.child", {
            workflowName: existing.workflow_name,
            status: "use_existing",
          })
          return childHandle(rowToChildRecord(existing))
        }
      }

      if (this.instanceRow(input)) {
        if (conflictPolicy === "terminate_existing") {
          this.deleteInstanceRecords(input.workflowId, input.runId)
          this.log("info", "provider.child.terminate_existing", {
            workflowName: input.workflowName,
            workflowId: input.workflowId,
            runId: input.runId,
            parentWorkflowId: input.parentWorkflowId,
            parentRunId: input.parentRunId,
            activationId: input.activationId,
            key: input.key,
          })
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
      this.prepare(
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

      this.prepare(
          `
          INSERT INTO children (
            child_record_id, parent_workflow_id, parent_run_id, activation_id, key,
            workflow_name, workflow_version, workflow_id, run_id, status, parent_close_policy
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?)
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
          parentClosePolicy,
        )

      this.replaceReadyEventsForState({
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

    return this.withBufferedObservability(() => create.immediate() as ChildHandle)
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    const cancel = this.db.transaction(() => {
      this.assertLiveActivationLease({
        workflowId: input.parentWorkflowId,
        runId: input.parentRunId,
        activationId: input.activationId,
        workerId: input.workerId,
        now: input.now,
      })

      const childInstance = this.instanceRow(input)
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

      const childRecord = oneRow<ChildRow>(
        this.prepare("SELECT * FROM children WHERE child_record_id = ? LIMIT 1")
          .get(childInstance.parent_child_record_id),
      )
      if (!childRecord) {
        throw new Error(`Unknown child record: ${childInstance.parent_child_record_id}`)
      }

      this.cancelStartedChild(childRecord, input.now, {
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

    this.withBufferedObservability(() => {
      cancel.immediate()
    })
  }

  async loadInstance(ref: InstanceRef, options: LoadInstanceOptions = {}): Promise<PersistedInstance | null> {
    const row = this.instanceRow(ref)
    return row ? this.persistedInstance(row, options) : null
  }

  async listInstances(options: LoadInstanceOptions = {}): Promise<PersistedInstance[]> {
    return this.prepare("SELECT * FROM instances ORDER BY workflow_id, run_id")
      .all()
      .map((row) => this.persistedInstance(requireRow<InstanceRow>(row), options))
  }

  async listSignals(): Promise<SignalRecord[]> {
    return this.prepare("SELECT * FROM signals ORDER BY received_at, signal_id")
      .all()
      .map((row) => rowToSignalRecord(requireRow<SignalRow>(row)))
  }

  async listChildren(): Promise<ChildRecord[]> {
    return this.prepare("SELECT * FROM children ORDER BY child_record_id")
      .all()
      .map((row) => rowToChildRecord(requireRow<ChildRow>(row)))
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
    return this.prepare("SELECT * FROM activation_claims ORDER BY workflow_id, run_id, sequence, activation_id")
      .all()
      .map((row) => {
        const claim = requireRow<ActivationClaimRow>(row)
        return {
          activationId: claim.activation_id,
          workflowId: claim.workflow_id,
          runId: claim.run_id,
          sequence: claim.sequence,
          kind: claim.kind,
          ownerId: claim.owner_id ?? undefined,
          completedBySequence: claim.completed_by_sequence ?? undefined,
        }
      })
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

      this.prepare(
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

      this.replaceSignalReadyEventsForState(readyEventStateFromInstanceRow(instance))

      this.log("info", "provider.signal.append", {
        workflowId: input.workflowId,
        runId: input.runId,
        signalId: signal.signalId,
        type: input.type,
      })
      this.count("durable.provider.signal", { status: "appended" })
      return signal
    })

    return this.withBufferedObservability(() => append.immediate() as SignalRecord)
  }

  async claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    const claim = this.db.transaction(() => {
      const leaseUntil = addMs(input.now, input.leaseMs)

      const result = this.prepare(
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

    return this.withBufferedObservability(() => claim.immediate() as DispatchShardLease | null)
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    const result = this.prepare(
        `
        UPDATE dispatch_shards
        SET lease_until = ?
        WHERE shard_id = ? AND owner_id = ? AND lease_until >= ?
      `,
      )
      .run(addMs(input.now, input.leaseMs), input.shardId, input.ownerId, input.now)
    if (result.changes === 0) {
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
    this.prepare(
        `
        UPDATE dispatch_shards
        SET owner_id = NULL, lease_until = NULL
        WHERE shard_id = ? AND owner_id = ?
      `,
      )
      .run(input.shardId, input.ownerId)
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
    const claim = this.db.transaction(() => {
      const ownedShards = input.shardIds.filter((shardId) => {
        const shard = oneRow<DispatchShardRow>(
          this.prepare("SELECT * FROM dispatch_shards WHERE shard_id = ?").get(shardId),
        )
        return shard?.owner_id === input.workerId && (shard.lease_until ?? "") >= input.now
      })

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

      this.expireActivityTimeouts(input.now, { shardIds: ownedShards })

      const candidates = this.readyCandidates(
        {
          ...input,
          shardIds: ownedShards,
        },
        readyCandidateLimit(limit),
      )
      const claims: ClaimedActivationWithInstance[] = []
      const claimedSequences = new Set<string>()

      for (const candidate of candidates) {
        if (claims.length >= limit) {
          break
        }
        const sequenceKey = `${candidate.workflowId}\0${candidate.runId}\0${candidate.sequence}`
        if (claimedSequences.has(sequenceKey)) {
          continue
        }
        if (!this.tryWriteActivationClaim(candidate, input.workerId, input.now, input.leaseMs)) {
          continue
        }
        if (candidate.instance.status !== "running" || candidate.instance.sequence !== candidate.sequence) {
          continue
        }
        claimedSequences.add(sequenceKey)
        claims.push({
          activation: stripCandidateMetadata(candidate),
          instance: this.activationInstanceSnapshot(candidate.instance),
        })
      }

      if (claims.length >= limit) {
        return { claims }
      }

      const nextWakeAt = this.nextWakeAt(ownedShards, input.now, input.workflows)
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

    return this.withBufferedObservability(() => claim.immediate() as ClaimReadyActivationsResult)
  }

  async claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult> {
    const result = await this.claimReadyActivations({
      ...input,
      limit: 1,
    })
    const first = result.claims[0]
    if (!first) {
      return result.nextWakeAt
        ? { activation: null, nextWakeAt: result.nextWakeAt }
        : { activation: null }
    }
    return {
      activation: first.activation,
      instance: first.instance,
    }
  }

  async heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    const activationIds = [...new Set(input.activationIds)]
    if (activationIds.length === 0) {
      return
    }
    const heartbeat = this.db.transaction(() => {
      for (const activationId of activationIds) {
        this.expireActivityTimeouts(input.now, { activationId })
      }
      return this.prepare(
          `
          UPDATE activation_claims
          SET lease_until = ?
          WHERE activation_id IN (${activationIds.map(() => "?").join(", ")}) AND owner_id = ? AND completed_by_sequence IS NULL
            AND lease_until >= ?
        `,
        )
        .run(addMs(input.now, input.leaseMs), ...activationIds, input.workerId, input.now)
    })
    const result = this.withBufferedObservability(() => heartbeat.immediate() as SqliteRunResult)
    if (result.changes !== activationIds.length) {
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
    this.prepare(
        `
        UPDATE activation_claims
        SET owner_id = NULL, lease_until = NULL
        WHERE activation_id IN (${activationIds.map(() => "?").join(", ")}) AND owner_id = ? AND completed_by_sequence IS NULL
      `,
      )
      .run(...activationIds, input.workerId)
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
    const reserve = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const options = normalizeEffectOptions(input)

      const existing = oneRow<EffectRow>(
        this.prepare(
            `
            SELECT * FROM effects
            WHERE workflow_id = ? AND run_id = ? AND activation_id = ? AND key = ?
            LIMIT 1
          `,
          )
          .get(input.workflowId, input.runId, input.activationId, input.key),
      )

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
        const started = this.ensureEffectAttemptStarted(existing, input)
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

      const effectId = `effect-${randomUUID()}`
      const attemptId = `attempt-${randomUUID()}`
      const idempotencyKey = `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`
      this.prepare(
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

      this.log("debug", "provider.effect.reserve", {
        workerId: input.workerId,
        workflowId: input.workflowId,
        runId: input.runId,
        activationId: input.activationId,
        effectId,
        attemptId,
        key: input.key,
        attempt: 1,
      })
      this.count("durable.provider.effect", { workerId: input.workerId, status: "reserved" })
      return {
        status: "reserved",
        effectId,
        idempotencyKey,
        attempt: 1,
        attemptId,
      } satisfies EffectReservation
    })

    return this.withBufferedObservability(() => reserve.immediate() as EffectReservation)
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    const heartbeat = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const effect = this.effectRow(input)
      const heartbeatDeadline = effect?.heartbeat_timeout_ms
        ? addMs(input.now, effect.heartbeat_timeout_ms)
        : null
      return this.prepare(
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
    const result = this.withBufferedObservability(() => heartbeat.immediate() as SqliteRunResult)
    if (result.changes === 0) {
      this.throwEffectMutationError(input)
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
    const complete = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      return this.prepare(
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
    const result = this.withBufferedObservability(() => complete.immediate() as SqliteRunResult)
    if (result.changes === 0) {
      this.throwEffectMutationError(input)
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
    const fail = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      this.assertLiveActivationLease(input)
      const effect = this.effectRow(input)
      if (!effect || effect.status !== "pending" || effect.attempt_id !== input.attemptId) {
        return { result: null, changes: 0 }
      }

      const retry = retryDecision(effect, input.error, input.now, input.retryable !== false)
      if (retry.status === "retry_scheduled") {
        const result = this.prepare(
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
        return { result: retry, changes: result.changes }
      }

      const result = this.prepare(
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
      return { result: { status: "failed" } satisfies FailEffectResult, changes: result.changes }
    })

    const output = this.withBufferedObservability(
      () => fail.immediate() as { result: FailEffectResult | null; changes: number },
    )
    if (output.changes === 0 || !output.result) {
      this.throwEffectMutationError(input)
    }
    return output.result
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const commit = this.db.transaction(() => {
      this.expireActivityTimeouts(input.now, { activationId: input.activationId })
      const conflict = (reason: string, sequence: number): CommitCheckpointResult => {
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
        return { ok: false, sequence }
      }

      const instance = this.instanceRow(input)
      if (!instance || instance.status !== "running") {
        return conflict("not_running", instance?.sequence ?? -1)
      }

      if (instance.sequence !== input.expectedSequence) {
        return conflict("stale_sequence", instance.sequence)
      }

      const claim = oneRow<ActivationClaimRow>(
        this.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(input.activationId),
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
        return conflict("lost_activation_lease", instance.sequence)
      }

      if (!claimMatchesCommit(claim, input)) {
        return conflict("activation_event_mismatch", instance.sequence)
      }

      const signalToConsume = input.consumeSignalId
        ? oneRow<SignalRow>(
            this.prepare(
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
        return conflict("signal_not_consumable", instance.sequence)
      }

      const childToConsume = input.consumeChildRecordId
        ? oneRow<ChildRow>(
            this.prepare(
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
        return conflict("child_not_consumable", instance.sequence)
      }

      const nextSequence = instance.sequence + 1
      this.writeNextInstance(input, nextSequence)

      if (signalToConsume) {
        this.prepare("UPDATE signals SET consumed_by_sequence = ? WHERE signal_id = ?")
          .run(nextSequence, signalToConsume.signal_id)
      }

      if (childToConsume) {
        this.prepare("UPDATE children SET delivered_by_sequence = ? WHERE child_record_id = ?")
          .run(nextSequence, childToConsume.child_record_id)
      }

      this.updateParentChildRecord(instance, input, nextSequence)
      this.applyParentClosePolicy(instance, input, nextSequence)
      this.replaceReadyEventsForState({
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

      this.prepare(
          `
          UPDATE activation_claims
          SET completed_by_sequence = ?, completed_at = ?, lease_until = ?, owner_id = ?
          WHERE activation_id = ?
        `,
        )
        .run(nextSequence, input.now, input.now, input.workerId, input.activationId)

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
    })

    return this.withBufferedObservability(() => commit.immediate() as CommitCheckpointResult)
  }

  private configure(): void {
    this.db.pragma("journal_mode = WAL")
    this.db.pragma(`synchronous = ${this.synchronous.toUpperCase()}`)
    this.db.pragma("foreign_keys = ON")
    this.db.pragma(`busy_timeout = ${this.busyTimeoutMs}`)
  }

  private prepare(sql: string): SqliteStatement {
    const existing = this.statements.get(sql)
    if (existing) {
      return existing
    }
    const statement = this.db.prepare(sql)
    this.statements.set(sql, statement)
    return statement
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
        status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'abandoned')),
        parent_close_policy TEXT NOT NULL DEFAULT 'cancel' CHECK (parent_close_policy IN ('cancel', 'abandon')),
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
      CREATE INDEX IF NOT EXISTS activation_claims_instance_sequence
        ON activation_claims(workflow_id, run_id, sequence, completed_by_sequence, activation_id);
      CREATE INDEX IF NOT EXISTS ready_events_claim
        ON ready_events(partition_shard, ready_at, sort_key);
      CREATE INDEX IF NOT EXISTS ready_events_instance
        ON ready_events(workflow_id, run_id, sequence);
    `)
    this.rebuildAllReadyEvents()
  }

  private effectRow(input: {
    workflowId: string
    runId: string
    activationId: string
    effectId: string
  }): EffectRow | null {
    return oneRow<EffectRow>(
      this.prepare(
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
      this.prepare("SELECT * FROM instances WHERE workflow_id = ? AND run_id = ? LIMIT 1")
        .get(ref.workflowId, ref.runId),
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

  private persistedInstance(row: InstanceRow, options: LoadInstanceOptions = {}): PersistedInstance {
    const instance: PersistedInstance = this.activationInstanceSnapshot(row)
    if (options.includeEffects) {
      instance.effects = this.prepare("SELECT * FROM effects WHERE workflow_id = ? AND run_id = ? ORDER BY effect_id")
        .all(row.workflow_id, row.run_id)
        .map((effectRow) => rowToEffectRecord(requireRow<EffectRow>(effectRow)))
    }
    return instance
  }

  private rebuildAllReadyEvents(): void {
    const rebuild = this.db.transaction(() => {
      this.prepare("DELETE FROM ready_events").run()
      const rows = this.prepare("SELECT workflow_id, run_id FROM instances WHERE status = 'running'")
        .all()
      for (const row of rows) {
        const instance = requireRow<{ workflow_id: string; run_id: string }>(row)
        this.replaceReadyEventsForInstance(instance.workflow_id, instance.run_id)
      }
    })
    rebuild.immediate()
  }

  private replaceReadyEventsForInstance(workflowId: string, runId: string): void {
    const instance = this.instanceRow({ workflowId, runId })
    if (!instance) {
      return
    }
    this.replaceReadyEventsForState(readyEventStateFromInstanceRow(instance))
  }

  private replaceReadyEventsForState(state: ReadyEventInstanceState): void {
    this.prepare("DELETE FROM ready_events WHERE workflow_id = ? AND run_id = ?")
      .run(state.workflowId, state.runId)
    if (state.status !== "running") {
      return
    }

    for (const wait of state.waits) {
      this.insertReadyEventForWait(state, wait)
    }
  }

  private replaceSignalReadyEventsForState(state: ReadyEventInstanceState): void {
    this.prepare(
        `
        DELETE FROM ready_events
        WHERE workflow_id = ? AND run_id = ? AND sequence = ? AND kind = 'signal'
      `,
      )
      .run(state.workflowId, state.runId, state.sequence)
    if (state.status !== "running") {
      return
    }

    for (const wait of state.waits) {
      if (wait.kind === "signal") {
        this.insertReadyEventForWait(state, wait)
      }
    }
  }

  private insertReadyEventForWait(state: ReadyEventInstanceState, wait: DurableWait): void {
    if (wait.kind === "run") {
      const activationId = activationIdFromParts(
        state.workflowId,
        state.runId,
        state.sequence,
        "run",
        wait.name,
      )
      this.insertReadyEvent({
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
      return
    }

    if (wait.kind === "signal") {
      const signalRow = oneRow<SignalRow>(
        this.prepare(
            `
            SELECT * FROM signals
            WHERE workflow_id = ? AND run_id = ? AND type = ? AND consumed_by_sequence IS NULL
            ORDER BY received_at, type, signal_id
            LIMIT 1
          `,
          )
          .get(state.workflowId, state.runId, wait.type),
      )
      if (!signalRow) {
        return
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
        occurredAt: signalRow.received_at,
        consumeSignalId: signalRow.signal_id,
      }
      this.insertReadyEvent({
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
        readyAt: signalRow.received_at,
        sortKey: sortKey(signalRow.received_at, "signal", wait.name, signalRow.signal_id),
        wait,
        event,
      })
      return
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
      this.insertReadyEvent({
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
      return
    }

    const childRow = oneRow<ChildRow>(
      this.prepare(
          `
          SELECT * FROM children
          WHERE workflow_id = ? AND run_id = ? AND status IN ('completed', 'failed') AND delivered_by_sequence IS NULL
          ORDER BY completed_at, child_record_id
          LIMIT 1
        `,
        )
        .get(wait.workflowId, wait.runId),
    )
    if (!childRow) {
      return
    }

    const occurredAt = childRow.completed_at ?? state.updatedAt
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
    this.insertReadyEvent({
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
    this.prepare(
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

  private readyCandidates(input: ClaimReadyActivationInput, limit: number): ReadyCandidate[] {
    return [
      ...this.migrationReadyCandidates(input, limit),
      ...this.indexedReadyCandidates(input, limit),
    ]
      .sort(compareReadyCandidates)
      .slice(0, limit)
  }

  private migrationReadyCandidates(
    input: ClaimReadyActivationInput,
    limit: number,
  ): ReadyCandidate[] {
    const workflowEntries = Object.entries(input.workflows)
    if (input.shardIds.length === 0 || workflowEntries.length === 0) {
      return []
    }

    const shardPlaceholders = input.shardIds.map(() => "?").join(", ")
    const versionClauses = workflowEntries
      .map(() => "(i.workflow_name = ? AND i.workflow_version < ?)")
      .join(" OR ")
    const rows = this.prepare(
      `
      SELECT i.*
      FROM instances i
      WHERE i.status = 'running'
        AND i.partition_shard IN (${shardPlaceholders})
        AND (${versionClauses})
        AND NOT EXISTS (
          SELECT 1 FROM activation_claims competing
          WHERE competing.workflow_id = i.workflow_id
            AND competing.run_id = i.run_id
            AND competing.sequence = i.sequence
            AND competing.kind <> 'migration'
            AND competing.completed_by_sequence IS NULL
          LIMIT 1
        )
        AND NOT EXISTS (
          SELECT 1 FROM activation_claims same_migration
          WHERE same_migration.workflow_id = i.workflow_id
            AND same_migration.run_id = i.run_id
            AND same_migration.sequence = i.sequence
            AND same_migration.kind = 'migration'
            AND same_migration.completed_by_sequence IS NULL
            AND same_migration.owner_id IS NOT NULL
            AND same_migration.lease_until > ?
          LIMIT 1
        )
      ORDER BY i.updated_at, i.workflow_id, i.run_id
      LIMIT ?
    `,
    )
      .all(
        ...input.shardIds,
        ...workflowEntries.flatMap(([name, workflow]) => [name, workflow.version]),
        input.now,
        limit,
      )
      .map((row) => requireRow<InstanceRow>(row))

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
        activationTime: instance.updated_at,
        leaseUntil: "",
        sort: [sortKey(instance.updated_at, "migration", instance.workflow_id, instance.run_id)],
        instance,
      }
    })
  }

  private indexedReadyCandidates(input: ClaimReadyActivationInput, limit: number): ReadyCandidate[] {
    const workflowEntries = Object.entries(input.workflows)
    if (input.shardIds.length === 0 || workflowEntries.length === 0) {
      return []
    }

    const placeholders = input.shardIds.map(() => "?").join(", ")
    const currentClauses = workflowEntries
      .map(() => "(re.workflow_name = ? AND re.workflow_version = ?)")
      .join(" OR ")
    const rows = this.prepare(
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
        FROM ready_events re
        JOIN instances i ON i.workflow_id = re.workflow_id AND i.run_id = re.run_id
        WHERE re.partition_shard IN (${placeholders})
          AND re.ready_at <= ?
          AND re.kind <> 'migration'
          AND (${currentClauses})
          AND i.status = 'running'
          AND i.sequence = re.sequence
          AND NOT EXISTS (
            SELECT 1 FROM activation_claims same_activation
            WHERE same_activation.activation_id = re.activation_id
              AND same_activation.completed_by_sequence IS NULL
              AND same_activation.owner_id IS NOT NULL
              AND same_activation.lease_until > ?
            LIMIT 1
          )
          AND NOT EXISTS (
            SELECT 1 FROM activation_claims competing
            WHERE competing.workflow_id = re.workflow_id
              AND competing.run_id = re.run_id
              AND competing.sequence = re.sequence
              AND competing.activation_id <> re.activation_id
              AND competing.completed_by_sequence IS NULL
            LIMIT 1
          )
          AND NOT EXISTS (
            SELECT 1 FROM effects blocked
            WHERE blocked.activation_id = re.activation_id
              AND blocked.status = 'pending'
              AND blocked.next_attempt_at IS NOT NULL
              AND blocked.next_attempt_at > ?
            LIMIT 1
          )
        ORDER BY re.sort_key
        LIMIT ?
      `,
    )
      .all(
        ...input.shardIds,
        input.now,
        ...workflowEntries.flatMap(([name, workflow]) => [name, workflow.version]),
        input.now,
        input.now,
        limit,
      )
      .map((row) => requireRow<ReadyEventJoinedRow>(row))

    const candidates: ReadyCandidate[] = []
    for (const row of rows) {
      const candidate = this.readyCandidateFromRow(row)
      if (candidate) {
        candidates.push(candidate)
      }
    }

    return candidates
  }

  private readyCandidateFromRow(row: ReadyEventJoinedRow): ReadyCandidate | null {
    const instance = instanceRowFromReadyEventJoinedRow(row)
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
        activationTime: row.ready_at,
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
        activationTime: row.ready_at,
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

  private hasUncompletedNonMigrationActivation(instance: InstanceRow): boolean {
    return Boolean(
      oneRow(
        this.prepare(
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

  private hasCompetingUncompletedActivation(
    instance: InstanceRow,
    activationId: string,
  ): boolean {
    return Boolean(
      oneRow(
        this.prepare(
            `
            SELECT activation_id FROM activation_claims
            WHERE workflow_id = ? AND run_id = ? AND sequence = ?
              AND activation_id <> ? AND completed_by_sequence IS NULL
            LIMIT 1
          `,
          )
          .get(instance.workflow_id, instance.run_id, instance.sequence, activationId),
      ),
    )
  }

  private activationRetryBlockedUntil(activationId: string, now: string): string | undefined {
    const row = oneRow<{ blocked_until: string | null }>(
      this.prepare(
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

    this.prepare(
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

    const expired = this.prepare(
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
    const effects = this.prepare(
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
        this.prepare(
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
        this.log("warn", "provider.effect.timeout", {
          workflowId: effect.workflow_id,
          runId: effect.run_id,
          activationId,
          effectId: effect.effect_id,
          attemptId: effect.attempt_id,
          timeoutKind,
          status: "failed",
        })
        this.count("durable.provider.effect", {
          status: "timeout_failed",
          reason: timeoutKind,
        })
        continue
      }

      this.prepare(
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
      this.log("info", "provider.effect.timeout_retry", {
        workflowId: effect.workflow_id,
        runId: effect.run_id,
        activationId,
        effectId: effect.effect_id,
        attemptId: effect.attempt_id,
        timeoutKind,
        nextAttempt: retry.nextAttempt,
        nextAttemptAt: retry.nextAttemptAt,
      })
      this.count("durable.provider.effect", {
        status: "timeout_retry",
        reason: timeoutKind,
      })
    }

    this.prepare(
        `
        UPDATE activation_claims
        SET owner_id = NULL, lease_until = NULL
        WHERE activation_id = ? AND completed_by_sequence IS NULL
      `,
      )
      .run(activationId)
  }

  private fencePendingEffectAttempt(effect: EffectRow): void {
    this.prepare(
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
      this.prepare(
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
      this.prepare(
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
      this.prepare(
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
      this.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(candidate.activationId),
    )
    const result = this.prepare(
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
        candidate.kind === "event" ? (candidate.waitName ?? null) : null,
        candidate.kind === "event" ? (candidate.eventJson ?? null) : null,
        candidate.kind === "event" ? (candidate.waitJson ?? null) : null,
        workerId,
        leaseUntil,
        candidate.activationTime,
        now,
      )
    if (result.changes === 0) {
      return false
    }
    const reclaimed = shouldResetPendingEffectAttemptsOnClaim(existing, workerId, now)
    if (reclaimed) {
      this.resetPendingEffectsForActivationReclaim(candidate.activationId)
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

  private resetPendingEffectsForActivationReclaim(activationId: string): void {
    const effects = this.prepare(
        `
        SELECT * FROM effects
        WHERE activation_id = ? AND status = 'pending' AND attempt_started_at IS NOT NULL
      `,
      )
      .all(activationId)
      .map((row) => requireRow<EffectRow>(row))

    for (const effect of effects) {
      this.prepare(
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
      this.prepare("SELECT * FROM activation_claims WHERE activation_id = ?").get(input.activationId),
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
      this.prepare(
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
      this.prepare(
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
      this.prepare(
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

    this.prepare(
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
      this.prepare("SELECT * FROM children WHERE child_record_id = ? AND status = 'started'")
        .get(previous.parent_child_record_id),
    )
    if (!existing) {
      return
    }

    if (input.next.status === "completed") {
      this.prepare(
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
      this.prepare(
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

  private applyParentClosePolicy(
    previous: InstanceRow,
    input: CommitCheckpointInput,
    nextSequence: number,
  ): void {
    if (input.next.status !== "canceled" && input.next.status !== "failed") {
      return
    }

    this.closeStartedChildren(
      previous.workflow_id,
      previous.run_id,
      input.now,
      nextSequence,
      input.next.status,
    )
  }

  private closeStartedChildren(
    parentWorkflowId: string,
    parentRunId: string,
    now: string,
    deliveredBySequence: number,
    status: "canceled" | "failed",
  ): void {
    const children = this.prepare(
        `
        SELECT * FROM children
        WHERE parent_workflow_id = ? AND parent_run_id = ? AND status = 'started'
        ORDER BY child_record_id
      `,
      )
      .all(parentWorkflowId, parentRunId)
      .map((row) => requireRow<ChildRow>(row))

    for (const child of children) {
      if (child.parent_close_policy === "abandon") {
        this.abandonStartedChild(child, now, deliveredBySequence)
      } else {
        this.cancelStartedChild(child, now, {
          deliverToParent: false,
          deliveredBySequence,
          reason: parentClosedChildError(status),
        })
      }
    }
  }

  private cancelStartedChild(
    child: ChildRow,
    now: string,
    options: {
      deliverToParent: boolean
      deliveredBySequence?: number
      reason: SerializedError
    },
  ): void {
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

    const instance = this.instanceRow({ workflowId: child.workflow_id, runId: child.run_id })
    if (instance?.status === "running") {
      const childCloseSequence = instance.sequence + 1
      this.prepare(
          `
          UPDATE instances
          SET sequence = sequence + 1, status = 'canceled',
            common_json = NULL, phase_name = NULL, phase_data_json = NULL,
            output_json = NULL, error_json = NULL, cancel_reason = ?,
            waits_json = ?, updated_at = ?
          WHERE workflow_id = ? AND run_id = ? AND status = 'running'
        `,
        )
        .run(
          options.reason.message,
          encodeJson([]),
          now,
          child.workflow_id,
          child.run_id,
        )
      this.prepare(
          `
          UPDATE activation_claims
          SET owner_id = NULL, lease_until = NULL
          WHERE workflow_id = ? AND run_id = ? AND completed_by_sequence IS NULL
        `,
        )
        .run(child.workflow_id, child.run_id)
      this.replaceReadyEventsForInstance(child.workflow_id, child.run_id)
      this.closeStartedChildren(
        child.workflow_id,
        child.run_id,
        now,
        childCloseSequence,
        "canceled",
      )
    }

    this.prepare(
        `
        UPDATE children
        SET status = 'failed', completed_at = ?, output_json = NULL, error_json = ?,
          delivered_by_sequence = ?
        WHERE child_record_id = ? AND status = 'started'
      `,
      )
      .run(
        now,
        encodeJson(options.reason),
        options.deliverToParent ? null : (options.deliveredBySequence ?? null),
        child.child_record_id,
      )

    if (options.deliverToParent) {
      this.replaceReadyEventsForInstance(child.parent_workflow_id, child.parent_run_id)
    }
  }

  private abandonStartedChild(child: ChildRow, now: string, deliveredBySequence: number): void {
    this.prepare(
        `
        UPDATE children
        SET status = 'abandoned', completed_at = ?, output_json = NULL, error_json = NULL,
          delivered_by_sequence = ?
        WHERE child_record_id = ? AND status = 'started'
      `,
      )
      .run(now, deliveredBySequence, child.child_record_id)
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

  private deleteInstanceRecords(workflowId: string, runId: string): void {
    this.prepare(
        `
        DELETE FROM children
        WHERE (parent_workflow_id = ? AND parent_run_id = ?)
          OR (workflow_id = ? AND run_id = ?)
      `,
      )
      .run(workflowId, runId, workflowId, runId)
    this.prepare("DELETE FROM instances WHERE workflow_id = ? AND run_id = ?").run(workflowId, runId)
  }

  private log(
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: Record<string, unknown>,
  ): void {
    if (this.observabilityBuffer) {
      this.observabilityBuffer.push({ kind: "log", level, event, fields })
      return
    }
    logDurable(this.observability, level, event, fields)
  }

  private count(name: string, tags?: DurableMetricTags): void {
    if (this.observabilityBuffer) {
      this.observabilityBuffer.push({ kind: "count", name, tags })
      return
    }
    countDurable(this.observability, name, 1, tags)
  }

  private gauge(name: string, value: number, tags?: DurableMetricTags): void {
    if (this.observabilityBuffer) {
      this.observabilityBuffer.push({ kind: "gauge", name, value, tags })
      return
    }
    gaugeDurable(this.observability, name, value, tags)
  }

  private withBufferedObservability<T>(fn: () => T): T {
    if (this.observabilityBuffer) {
      return fn()
    }

    const buffer: BufferedObservation[] = []
    this.observabilityBuffer = buffer
    let result: T
    let thrown: unknown
    let didThrow = false
    try {
      result = fn()
    } catch (error) {
      thrown = error
      didThrow = true
    } finally {
      this.observabilityBuffer = undefined
    }

    this.flushObservabilityBuffer(buffer)
    if (didThrow) {
      throw thrown
    }
    return result!
  }

  private flushObservabilityBuffer(buffer: BufferedObservation[]): void {
    for (const observation of buffer) {
      if (observation.kind === "log") {
        logDurable(
          this.observability,
          observation.level,
          observation.event,
          observation.fields,
        )
      } else if (observation.kind === "count") {
        countDurable(this.observability, observation.name, 1, observation.tags)
      } else {
        gaugeDurable(this.observability, observation.name, observation.value, observation.tags)
      }
    }
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
    updatedAt: row.updated_at,
  }
}

function normalizeSqliteSynchronous(value: SqliteDurabilityProviderOptions["synchronous"]): "full" | "normal" {
  if (value === undefined) {
    return "full"
  }
  if (value !== "full" && value !== "normal") {
    throw new Error('SqliteDurabilityProvider synchronous must be "full" or "normal"')
  }
  return value
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

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
  return value
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
    parentClosePolicy: row.parent_close_policy,
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
