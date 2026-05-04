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
  StartWorkflowResult,
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

export type ActivationInstanceSnapshot = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  partitionShard: number
  sequence: number
  status: "running" | "completed" | "canceled" | "failed"
  common?: JsonObject
  phase?: PhaseSnapshot
  output?: JsonValue
  error?: SerializedError
  cancelReason?: string
  waits: DurableWait[]
  createdAt: string
  updatedAt: string
  parent?: {
    workflowId: string
    runId: string
    childRecordId: string
  }
}

export type PersistedInstance = ActivationInstanceSnapshot & {
  effects?: EffectRecord[]
}

export type LoadInstanceOptions = {
  includeEffects?: boolean
}

export type WorkflowRunDirection = "asc" | "desc"

export type GetWorkflowRunsInput = LoadInstanceOptions & {
  id: string
  cursor?: string
  limit?: number
  direction?: WorkflowRunDirection
}

export type GetWorkflowRunsResult = {
  runs: PersistedInstance[]
  cursor?: string
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
export type WorkflowIdReusePolicy = "failed_only" | "not_running" | "always"

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
  workflowIdReusePolicy?: WorkflowIdReusePolicy
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
  leaseEpoch?: number
}

export type ShardLease = DispatchShardLease & {
  leaseEpoch: number
}

export type OpenShardInput =
  | ShardLease
  | {
      shardId: number
      ownerId?: string
      leaseUntil?: string
      leaseEpoch?: number
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
  shardCount?: number
  workflows: Record<string, { version: number }>
  now: string
  leaseMs: number
}

export type ClaimReadyActivationsInput = ClaimReadyActivationInput & {
  limit: number
}

export type ActivationClaimLease =
  | { scope: "activation" }
  | { scope: "shard"; shardId: number; epoch: number }

export type ClaimedActivationWithInstance = {
  activation: ClaimedActivation
  instance: ActivationInstanceSnapshot
  effects: EffectRecord[]
  lease: ActivationClaimLease
}

export type ClaimReadyActivationsResult = {
  claims: ClaimedActivationWithInstance[]
  nextWakeAt?: string
}

export type ClaimReadyActivationResult =
  | {
      activation: ClaimedActivation
      instance: ActivationInstanceSnapshot
      effects: EffectRecord[]
      lease: ActivationClaimLease
      nextWakeAt?: undefined
    }
  | {
      activation: null
      instance?: undefined
      nextWakeAt?: string
    }

export type HeartbeatActivationInput = {
  activationId: string
  workerId: string
  now: string
  leaseMs: number
}

export type HeartbeatActivationsInput = {
  activationIds: string[]
  workerId: string
  now: string
  leaseMs: number
}

export type ReleaseActivationInput = {
  activationId: string
  workerId: string
}

export type ReleaseActivationsInput = {
  activationIds: string[]
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
  effects?: CheckpointEffectMutation[]
  childStarts?: CheckpointChildStart[]
}

export type CommitCheckpointResult = {
  ok: boolean
  sequence: number
  reason?: string
  retryable?: boolean
  error?: SerializedError
}

export type CheckpointEffectMutation =
  | {
      key: string
      status: "completed"
      result: JsonValue
      heartbeatDetails?: JsonValue
      attempt?: number
      idempotencyKey?: string
      firstAttemptStartedAt?: string
      maxAttempts?: number
      maxElapsedMs?: number | null
      initialIntervalMs?: number
      maxIntervalMs?: number
      backoffCoefficient?: number
      nonRetryableErrorNames?: string[]
    }
  | {
      key: string
      status: "failed"
      error: SerializedError
      retryable?: boolean
      heartbeatDetails?: JsonValue
      attempt?: number
      idempotencyKey?: string
      firstAttemptStartedAt?: string
      maxAttempts?: number
      maxElapsedMs?: number | null
      initialIntervalMs?: number
      maxIntervalMs?: number
      backoffCoefficient?: number
      nonRetryableErrorNames?: string[]
    }
  | {
      key: string
      status: "retry_scheduled"
      error: SerializedError
      nextAttemptAt: string
      nextAttempt: number
      heartbeatDetails?: JsonValue
      attempt?: number
      idempotencyKey?: string
      firstAttemptStartedAt?: string
      maxAttempts?: number
      maxElapsedMs?: number | null
      initialIntervalMs?: number
      maxIntervalMs?: number
      backoffCoefficient?: number
      nonRetryableErrorNames?: string[]
    }

export type CheckpointChildStart = {
  key: string
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  partitionShard: number
  common: JsonObject
  phase: PhaseSnapshot
  waits: DurableWait[]
  parentClosePolicy?: "cancel" | "abandon"
  conflictPolicy?: ConflictPolicy
  workflowIdReusePolicy?: WorkflowIdReusePolicy
}

export type CommitActivationInput = CommitCheckpointInput

export type CommitActivationsResult = {
  results: Array<CommitCheckpointResult & { activationId: string }>
}

export type RecordActivationFailureInput = {
  workflowId: string
  runId: string
  activationId: string
  workerId: string
  now: string
  effects: CheckpointEffectMutation[]
  releaseActivation?: boolean
}

export type ClaimShardTasksInput = {
  workflows: Record<string, { version: number }>
  shardCount?: number
  now: string
  leaseMs: number
  limit: number
}

export type ClaimShardTasksResult = ClaimReadyActivationsResult

export type ShardDurabilitySession = {
  readonly shardId: number
  readonly ownerId?: string
  readonly leaseEpoch?: number
  createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult>
  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle>
  cancelChild(input: CancelChildInput): Promise<void>
  readInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null>
  getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult>
  appendSignal(input: AppendSignalInput): Promise<SignalRecord>
  claimTasks(input: ClaimShardTasksInput): Promise<ClaimShardTasksResult>
  heartbeat(input: { now: string; leaseMs: number }): Promise<void>
  release(): Promise<void>
  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void>
  heartbeatActivation(input: HeartbeatActivationInput): Promise<void>
  releaseActivations(input: ReleaseActivationsInput): Promise<void>
  releaseActivation(input: ReleaseActivationInput): Promise<void>
  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation>
  heartbeatEffect(input: HeartbeatEffectInput): Promise<void>
  completeEffect(input: CompleteEffectInput): Promise<void>
  failEffect(input: FailEffectInput): Promise<FailEffectResult>
  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult>
  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult>
  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void>
}

export type DurabilityProvider = {
  claimShard(input: ClaimDispatchShardInput): Promise<ShardLease | null>
  openShard(input: OpenShardInput): ShardDurabilitySession
  createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult>
  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle>
  cancelChild(input: CancelChildInput): Promise<void>
  loadInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null>
  getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult>
  appendSignal(input: AppendSignalInput): Promise<SignalRecord>
  claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null>
  heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void>
  releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void>
  claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult>
  claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult>
  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void>
  heartbeatActivation(input: HeartbeatActivationInput): Promise<void>
  releaseActivations(input: ReleaseActivationsInput): Promise<void>
  releaseActivation(input: ReleaseActivationInput): Promise<void>
  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation>
  heartbeatEffect(input: HeartbeatEffectInput): Promise<void>
  completeEffect(input: CompleteEffectInput): Promise<void>
  failEffect(input: FailEffectInput): Promise<FailEffectResult>
  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult>
  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult>
  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void>
}

export function workflowPartitionShard(workflowId: string, runId: string, shardCount: number): number {
  if (!Number.isInteger(shardCount) || shardCount <= 0) {
    throw new Error("shardCount must be a positive integer")
  }

  let hash = 0x811c9dc5
  void runId
  const key = workflowId
  for (let index = 0; index < key.length; index += 1) {
    hash ^= key.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0) % shardCount
}
