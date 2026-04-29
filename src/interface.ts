import type {
  AnyWorkflow,
  ChildEvent,
  ChildHandle,
  InstanceRef,
  InstanceStatus,
  JsonObject,
  JsonValue,
  OutputOf,
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
  status: "started" | "completed" | "failed"
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
  key: string
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
  key: string
}

export type HeartbeatEffectInput = {
  effectId: string
  now: string
  details?: JsonValue
}

export type CompleteEffectInput = {
  workflowId: string
  runId: string
  effectId: string
  result: JsonValue
}

export type FailEffectInput = {
  workflowId: string
  runId: string
  effectId: string
  error: SerializedError
}

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
  loadInstance(ref: InstanceRef): Promise<PersistedInstance | null>
  appendSignal(input: AppendSignalInput): Promise<SignalRecord>
  claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null>
  heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void>
  releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void>
  claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimedActivation | null>
  heartbeatActivation(input: HeartbeatActivationInput): Promise<void>
  releaseActivation(input: ReleaseActivationInput): Promise<void>
  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation>
  heartbeatEffect(input: HeartbeatEffectInput): Promise<void>
  completeEffect(input: CompleteEffectInput): Promise<void>
  failEffect(input: FailEffectInput): Promise<void>
  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult>
  readOutput<W extends AnyWorkflow>(handle: ChildHandle<W>): Promise<OutputOf<W>>
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
