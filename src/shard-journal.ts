import type {
  AppendSignalInput,
  CancelChildInput,
  ClaimDispatchShardInput,
  ClaimReadyActivationsInput,
  ClaimShardTasksInput,
  CommitActivationInput,
  CompleteEffectInput,
  CreateChildInstanceInput,
  CreateInstanceInput,
  FailEffectInput,
  HeartbeatActivationsInput,
  HeartbeatDispatchShardInput,
  HeartbeatEffectInput,
  OpenShardInput,
  RecordActivationFailureInput,
  ReleaseActivationsInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
} from "./interface.js"
import type { ShardMemoryDurabilityProvider } from "./shard-engine.js"

export type JournalOperation =
  | { op: "createInstance"; input: CreateInstanceInput }
  | { op: "createChildInstance"; input: CreateChildInstanceInput }
  | { op: "cancelChild"; input: CancelChildInput }
  | { op: "appendSignal"; input: AppendSignalInput }
  | { op: "claimReadyActivations"; input: ClaimReadyActivationsInput }
  | { op: "claimShardTasks"; session: OpenShardInput; input: ClaimShardTasksInput }
  | { op: "heartbeatActivations"; input: HeartbeatActivationsInput }
  | { op: "releaseActivations"; input: ReleaseActivationsInput }
  | { op: "getOrReserveEffect"; input: ReserveEffectInput }
  | { op: "heartbeatEffect"; input: HeartbeatEffectInput }
  | { op: "completeEffect"; input: CompleteEffectInput }
  | { op: "failEffect"; input: FailEffectInput }
  | { op: "commitActivations"; input: CommitActivationInput[] }
  | { op: "recordActivationFailures"; input: RecordActivationFailureInput[] }
  | { op: "claimShard"; input: ClaimDispatchShardInput }
  | { op: "heartbeatDispatchShard"; input: HeartbeatDispatchShardInput }
  | { op: "releaseDispatchShard"; input: ReleaseDispatchShardInput }

export async function applyJournalOperation(
  engine: ShardMemoryDurabilityProvider,
  operation: JournalOperation,
): Promise<void> {
  await engine.replay(() => applyJournalOperationInner(engine, operation))
}

export function applyJournalOperationSync(
  engine: ShardMemoryDurabilityProvider,
  operation: JournalOperation,
): void {
  engine.replaySync(() => {
    void applyJournalOperationInner(engine, operation)
  })
}

export function operationTime(operation: JournalOperation): string {
  if ("input" in operation) {
    const input = operation.input
    if (Array.isArray(input)) {
      return input[0]?.now ?? new Date().toISOString()
    }
    if ("now" in input && typeof input.now === "string") {
      return input.now
    }
    if ("receivedAt" in input && typeof input.receivedAt === "string") {
      return input.receivedAt
    }
    if ("leaseNow" in input && typeof input.leaseNow === "string") {
      return input.leaseNow
    }
  }
  return new Date().toISOString()
}

async function applyJournalOperationInner(
  engine: ShardMemoryDurabilityProvider,
  operation: JournalOperation,
): Promise<void> {
  switch (operation.op) {
    case "claimShard":
      await engine.claimShard(operation.input)
      return
    case "heartbeatDispatchShard":
      await engine.heartbeatDispatchShard(operation.input)
      return
    case "releaseDispatchShard":
      await engine.releaseDispatchShard(operation.input)
      return
    case "createInstance":
      await engine.createInstance(operation.input)
      return
    case "createChildInstance":
      await engine.createChildInstance(operation.input)
      return
    case "cancelChild":
      await engine.cancelChild(operation.input)
      return
    case "appendSignal":
      await engine.appendSignal(operation.input)
      return
    case "claimReadyActivations":
      await engine.claimReadyActivations(operation.input)
      return
    case "claimShardTasks":
      await engine.openShard(operation.session).claimTasks(operation.input)
      return
    case "heartbeatActivations":
      await engine.heartbeatActivations(operation.input)
      return
    case "releaseActivations":
      await engine.releaseActivations(operation.input)
      return
    case "getOrReserveEffect":
      await engine.getOrReserveEffect(operation.input)
      return
    case "heartbeatEffect":
      await engine.heartbeatEffect(operation.input)
      return
    case "completeEffect":
      await engine.completeEffect(operation.input)
      return
    case "failEffect":
      await engine.failEffect(operation.input)
      return
    case "commitActivations":
      await engine.commitActivations(operation.input)
      return
    case "recordActivationFailures":
      await engine.recordActivationFailures(operation.input)
      return
  }
}
