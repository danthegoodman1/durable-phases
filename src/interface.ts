import type {
  ActivityOptions,
  ChildEvent,
  ChildHandle,
  InstanceRef,
  InstanceStatus,
  JsonObject,
  JsonValue,
  PhaseSnapshot,
  SerializedError,
} from "./workflow.js"

export type DurableWait =
  | { kind: "run"; name: "__run"; readyAt: string }
  | { kind: "signal"; name: string; type: string; scope: "phase" | "global" }
  | { kind: "timer"; name: string; fireAt: string }
  | {
      kind: "child"
      name: string
      workflowName: string
      workflowVersion: number
      workflowId: string
      runId: string
    }

export type SignalRecord = {
  signalId: string
  workflowId: string
  runId: string
  type: string
  payload: JsonValue
  receivedAt: string
  consumedBySequence?: number
}

export type EffectRecord = {
  effectId: string
  activationId: string
  key: string
  idempotencyKey: string
  status: "pending" | "completed" | "failed"
  attempt?: number
  attemptId?: string
  attemptOwnerId?: string
  attemptStartedAt?: string
  startToCloseTimeoutMs?: number
  startToCloseDeadline?: string
  heartbeatTimeoutMs?: number
  heartbeatDeadline?: string
  maxAttempts?: number
  maxElapsedMs?: number
  initialIntervalMs?: number
  maxIntervalMs?: number
  backoffCoefficient?: number
  firstAttemptStartedAt?: string
  nextAttemptAt?: string
  lastFailure?: SerializedError
  nonRetryableErrorNames?: string[]
  timedOutAt?: string
  timeoutKind?: "heartbeat" | "start_to_close"
  result?: JsonValue
  error?: SerializedError
  heartbeatAt?: string
  heartbeatDetails?: JsonValue
}

export type PersistedInstance = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  sequence: number
  status: "running" | "completed" | "canceled" | "failed"
  common?: JsonObject
  phase?: PhaseSnapshot
  output?: JsonValue
  error?: SerializedError
  cancelReason?: string
  waits: DurableWait[]
  effects: EffectRecord[]
  createdAt: string
  updatedAt: string
  parent?: {
    workflowId: string
    runId: string
    childRecordId: string
  }
}

export type ChildRecord = {
  childRecordId: string
  parentWorkflowId: string
  parentRunId: string
  activationId: string
  key: string
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  status: "started" | "completed" | "failed" | "abandoned"
  parentClosePolicy: "cancel" | "abandon"
  completedAt?: string
  output?: JsonValue
  error?: SerializedError
  deliveredBySequence?: number
}

export type ConflictPolicy = "fail" | "use_existing" | "terminate_existing"

export type CreateInstanceInput = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  partitionShard: number
  common: JsonObject
  phase: PhaseSnapshot
  waits: DurableWait[]
  now: string
  parent?: PersistedInstance["parent"]
  conflictPolicy?: ConflictPolicy
}

export type CreateChildInstanceInput = CreateInstanceInput & {
  parentWorkflowId: string
  parentRunId: string
  activationId: string
  workerId: string
  leaseNow: string
  key: string
  parentClosePolicy?: "cancel" | "abandon"
  conflictPolicy?: ConflictPolicy
}

export type CancelChildInput = {
  parentWorkflowId: string
  parentRunId: string
  activationId: string
  workerId: string
  workflowId: string
  runId: string
  now: string
}

export type AppendSignalInput = {
  workflowId: string
  runId: string
  type: string
  payload: JsonValue
  receivedAt: string
}

export type DispatchShardLease = {
  shardId: number
  ownerId: string
  leaseUntil: string
}

export type ClaimDispatchShardInput = {
  shardId: number
  ownerId: string
  now: string
  leaseMs: number
}

export type HeartbeatDispatchShardInput = ClaimDispatchShardInput

export type ReleaseDispatchShardInput = {
  shardId: number
  ownerId: string
}

export type ReadyEvent =
  | {
      kind: "signal"
      signalId: string
      payload: JsonValue
      occurredAt: string
      consumeSignalId: string
    }
  | {
      kind: "timer"
      firedAt: string
      occurredAt: string
    }
  | {
      kind: "child"
      childRecordId: string
      occurredAt: string
      event: ChildEvent
    }

export type ClaimedActivation =
  | {
      kind: "migration"
      activationId: string
      workflowName: string
      workflowId: string
      runId: string
      sequence: number
      activationTime: string
      leaseUntil: string
    }
  | {
      kind: "run"
      activationId: string
      workflowName: string
      workflowId: string
      runId: string
      sequence: number
      activationTime: string
      leaseUntil: string
    }
  | {
      kind: "event"
      activationId: string
      workflowName: string
      workflowId: string
      runId: string
      sequence: number
      activationTime: string
      waitName: string
      wait: Exclude<DurableWait, { kind: "run" }>
      event: ReadyEvent
      leaseUntil: string
    }

export type ClaimReadyActivationInput = {
  workerId: string
  shardIds: number[]
  workflows: Record<string, { version: number }>
  now: string
  leaseMs: number
}

export type ClaimReadyActivationResult = {
  activation: ClaimedActivation | null
  nextWakeAt?: string
}

export type HeartbeatActivationInput = {
  activationId: string
  workerId: string
  now: string
  leaseMs: number
}

export type ReleaseActivationInput = {
  activationId: string
  workerId: string
}

export type EffectReservation<T = JsonValue> =
  | {
      status: "reserved"
      effectId: string
      idempotencyKey: string
      attempt: number
      attemptId: string
      heartbeatDetails?: JsonValue
    }
  | {
      status: "completed"
      result: T
    }
  | {
      status: "failed"
      error: SerializedError
    }

export type ReserveEffectInput = {
  workflowId: string
  runId: string
  activationId: string
  workerId: string
  key: string
  now: string
  options?: ActivityOptions
  maxAttempts?: number
}

export type HeartbeatEffectInput = {
  workflowId: string
  runId: string
  activationId: string
  workerId: string
  effectId: string
  attemptId: string
  now: string
  details?: JsonValue
}

export type CompleteEffectInput = {
  workflowId: string
  runId: string
  activationId: string
  workerId: string
  effectId: string
  attemptId: string
  result: JsonValue
  now: string
}

export type FailEffectInput = {
  workflowId: string
  runId: string
  activationId: string
  workerId: string
  effectId: string
  attemptId: string
  error: SerializedError
  now: string
  retryable?: boolean
}

export type FailEffectResult =
  | { status: "failed" }
  | { status: "retry_scheduled"; nextAttemptAt: string; nextAttempt: number }

export type CommitCheckpointInput = {
  workflowId: string
  runId: string
  expectedSequence: number
  activationId: string
  workerId: string
  workflowVersion: number
  next: InstanceStatus<any>
  waits: DurableWait[]
  now: string
  consumeSignalId?: string
  consumeChildRecordId?: string
}

export type CommitCheckpointResult = {
  ok: boolean
  sequence: number
}

export type DurabilityProvider = {
  createInstance(input: CreateInstanceInput): Promise<InstanceRef>
  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle>
  cancelChild(input: CancelChildInput): Promise<void>
  loadInstance(ref: InstanceRef): Promise<PersistedInstance | null>
  appendSignal(input: AppendSignalInput): Promise<SignalRecord>
  claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null>
  heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void>
  releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void>
  claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult>
  heartbeatActivation(input: HeartbeatActivationInput): Promise<void>
  releaseActivation(input: ReleaseActivationInput): Promise<void>
  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation>
  heartbeatEffect(input: HeartbeatEffectInput): Promise<void>
  completeEffect(input: CompleteEffectInput): Promise<void>
  failEffect(input: FailEffectInput): Promise<FailEffectResult>
  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult>
}

export function workflowPartitionShard(workflowId: string, runId: string, shardCount: number): number {
  if (!Number.isInteger(shardCount) || shardCount <= 0) {
    throw new Error("shardCount must be a positive integer")
  }

  let hash = 0x811c9dc5
  const key = `${workflowId}\0${runId}`
  for (let index = 0; index < key.length; index += 1) {
    hash ^= key.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0) % shardCount
}
