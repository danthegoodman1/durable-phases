import { mkdirSync } from "node:fs"
import { join } from "node:path"
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
  HeartbeatActivationsInput,
  HeartbeatActivationInput,
  HeartbeatDispatchShardInput,
  HeartbeatEffectInput,
  LoadInstanceOptions,
  OpenShardInput,
  PersistedInstance,
  RecordActivationFailureInput,
  ReleaseActivationsInput,
  ReleaseActivationInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
  ShardDurabilitySession,
  ShardLease,
  SignalRecord,
} from "./interface.js"
import { workflowPartitionShard } from "./interface.js"
import type { ChildHandle, InstanceRef } from "./workflow.js"
import type { DurableObservability } from "./observability.js"
import {
  SqliteDurabilityProvider,
  type SqliteDurabilityProviderOptions,
} from "./sqlite.js"

export type SqliteShardFileDurabilityProviderOptions = DurableObservability & {
  directory: string
  shardCount: number
  busyTimeoutMs?: number
  sqlProfiler?: SqliteDurabilityProviderOptions["sqlProfiler"]
  filenameForShard?: (shardId: number) => string
}

export class SqliteShardFileDurabilityProvider implements DurabilityProvider {
  readonly shardCount: number

  private readonly providers: Array<SqliteDurabilityProvider | undefined>

  constructor(private readonly options: SqliteShardFileDurabilityProviderOptions) {
    this.shardCount = positiveInteger(options.shardCount, "shardCount")
    mkdirSync(options.directory, { recursive: true })
    this.providers = Array.from({ length: this.shardCount })
  }

  shardPath(shardId: number): string {
    return this.pathForShard(this.assertShardId(shardId))
  }

  close(): void {
    for (const provider of this.providers) {
      provider?.close()
    }
  }

  async listInstances(options?: LoadInstanceOptions): Promise<PersistedInstance[]> {
    const groups = await Promise.all(this.providersForAllShards().map((provider) => provider.listInstances(options)))
    return groups.flat().sort(compareInstances)
  }

  async listSignals(): Promise<SignalRecord[]> {
    const groups = await Promise.all(this.providersForAllShards().map((provider) => provider.listSignals()))
    return groups.flat().sort((left, right) =>
      `${left.receivedAt}\0${left.signalId}`.localeCompare(`${right.receivedAt}\0${right.signalId}`),
    )
  }

  async listChildren(): Promise<ChildRecord[]> {
    const groups = await Promise.all(this.providersForAllShards().map((provider) => provider.listChildren()))
    return groups.flat().sort((left, right) => left.childRecordId.localeCompare(right.childRecordId))
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
    const groups = await Promise.all(this.providersForAllShards().map((provider) => provider.listActivationClaims()))
    return groups.flat().sort((left, right) =>
      `${left.workflowId}\0${left.runId}\0${left.sequence}\0${left.activationId}`.localeCompare(
        `${right.workflowId}\0${right.runId}\0${right.sequence}\0${right.activationId}`,
      ),
    )
  }

  claimShard(input: ClaimDispatchShardInput): Promise<ShardLease | null> {
    return this.providerForShard(input.shardId).claimShard(input)
  }

  openShard(input: OpenShardInput): ShardDurabilitySession {
    const shardId = this.assertShardId(input.shardId)
    return new SqliteShardFileSession(this, this.providerForShard(shardId).openShard(input))
  }

  createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    this.assertInputShard(input)
    return this.providerForRef(input.workflowId, input.runId).createInstance(input)
  }

  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    this.assertInputShard(input)
    this.assertSameShard(input.parentWorkflowId, input.parentRunId, input.workflowId, input.runId)
    return this.providerForRef(input.workflowId, input.runId).createChildInstance(input)
  }

  cancelChild(input: CancelChildInput): Promise<void> {
    return this.providerForRef(input.parentWorkflowId, input.parentRunId).cancelChild(input)
  }

  loadInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null> {
    return this.providerForRef(ref.workflowId, ref.runId).loadInstance(ref, options)
  }

  appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.providerForRef(input.workflowId, input.runId).appendSignal(input)
  }

  claimDispatchShard(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null> {
    return this.providerForShard(input.shardId).claimDispatchShard(input)
  }

  heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    return this.providerForShard(input.shardId).heartbeatDispatchShard(input)
  }

  releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    return this.providerForShard(input.shardId).releaseDispatchShard(input)
  }

  async claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult> {
    const claims: ClaimReadyActivationsResult["claims"] = []
    let nextWakeAt: string | undefined
    for (const shardId of input.shardIds) {
      if (claims.length >= input.limit) {
        break
      }
      const result = await this.providerForShard(shardId).claimReadyActivations({
        ...input,
        shardIds: [shardId],
        limit: input.limit - claims.length,
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
    await Promise.all(
      groupByActivationShard(input.activationIds, this.shardCount).map(([shardId, activationIds]) =>
        this.providerForShard(shardId).heartbeatActivations({ ...input, activationIds }),
      ),
    )
  }

  heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    return this.providerForActivationId(input.activationId).heartbeatActivation(input)
  }

  async releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    await Promise.all(
      groupByActivationShard(input.activationIds, this.shardCount).map(([shardId, activationIds]) =>
        this.providerForShard(shardId).releaseActivations({ ...input, activationIds }),
      ),
    )
  }

  releaseActivation(input: ReleaseActivationInput): Promise<void> {
    return this.providerForActivationId(input.activationId).releaseActivation(input)
  }

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.providerForRef(input.workflowId, input.runId).getOrReserveEffect(input)
  }

  heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    return this.providerForRef(input.workflowId, input.runId).heartbeatEffect(input)
  }

  completeEffect(input: CompleteEffectInput): Promise<void> {
    return this.providerForRef(input.workflowId, input.runId).completeEffect(input)
  }

  failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.providerForRef(input.workflowId, input.runId).failEffect(input)
  }

  async commitActivations(inputs: CommitActivationInput[]): Promise<CommitActivationsResult> {
    const indexedResults = new Map<number, CommitActivationsResult["results"][number]>()
    await Promise.all(
      groupByInputShard(inputs, this.shardCount).map(async ([shardId, entries]) => {
        this.assertCommitChildStartsStayOnParentShard(entries.map((entry) => entry.input))
        const result = await this.providerForShard(shardId).commitActivations(entries.map((entry) => entry.input))
        for (const [offset, item] of result.results.entries()) {
          const original = entries[offset]
          if (original) {
            indexedResults.set(original.index, item)
          }
        }
      }),
    )
    return {
      results: inputs.map((input, index) =>
        indexedResults.get(index) ?? {
          ok: false,
          sequence: input.expectedSequence,
          activationId: input.activationId,
          reason: "missing_commit_result",
        },
      ),
    }
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const result = await this.commitActivations([input])
    return commitCheckpointResult(result, input)
  }

  async recordActivationFailures(inputs: RecordActivationFailureInput[]): Promise<void> {
    await Promise.all(
      groupByInputShard(inputs, this.shardCount).map(([shardId, entries]) =>
        this.providerForShard(shardId).recordActivationFailures(entries.map((entry) => entry.input)),
      ),
    )
  }

  private pathForShard(shardId: number): string {
    return join(
      this.options.directory,
      this.options.filenameForShard?.(shardId) ?? `shard-${shardId}.sqlite`,
    )
  }

  private providerForShard(shardId: number): SqliteDurabilityProvider {
    const checkedShardId = this.assertShardId(shardId)
    const existing = this.providers[checkedShardId]
    if (existing) {
      return existing
    }
    const provider = new SqliteDurabilityProvider(this.pathForShard(checkedShardId), {
      busyTimeoutMs: this.options.busyTimeoutMs,
      logger: this.options.logger,
      metrics: this.options.metrics,
      sqlProfiler: this.options.sqlProfiler,
    })
    this.providers[checkedShardId] = provider
    return provider
  }

  private providersForAllShards(): SqliteDurabilityProvider[] {
    return Array.from({ length: this.shardCount }, (_value, shardId) => this.providerForShard(shardId))
  }

  private providerForRef(workflowId: string, runId: string): SqliteDurabilityProvider {
    return this.providerForShard(workflowPartitionShard(workflowId, runId, this.shardCount))
  }

  private providerForActivationId(activationId: string): SqliteDurabilityProvider {
    const ref = refFromActivationId(activationId)
    return this.providerForRef(ref.workflowId, ref.runId)
  }

  private assertShardId(shardId: number): number {
    if (!Number.isInteger(shardId) || shardId < 0 || shardId >= this.shardCount) {
      throw new Error(`Shard ${shardId} is outside configured shard count ${this.shardCount}`)
    }
    return shardId
  }

  private assertSameShard(
    parentWorkflowId: string,
    parentRunId: string,
    childWorkflowId: string,
    childRunId: string,
  ): void {
    const parentShard = workflowPartitionShard(parentWorkflowId, parentRunId, this.shardCount)
    const childShard = workflowPartitionShard(childWorkflowId, childRunId, this.shardCount)
    if (parentShard !== childShard) {
      throw new Error(
        `SQLite shard-file provider requires local child workflows to stay on parent shard (${parentShard} !== ${childShard})`,
      )
    }
  }

  private assertInputShard(input: CreateInstanceInput): void {
    const expected = workflowPartitionShard(input.workflowId, input.runId, this.shardCount)
    if (input.partitionShard !== expected) {
      throw new Error(
        `SQLite shard-file provider expected partition shard ${expected} for ${input.workflowId}/${input.runId}, got ${input.partitionShard}`,
      )
    }
  }

  assertCommitChildStartsStayOnParentShard(inputs: CommitActivationInput[]): void {
    for (const input of inputs) {
      for (const child of input.childStarts ?? []) {
        this.assertSameShard(input.workflowId, input.runId, child.workflowId, child.runId)
      }
    }
  }
}

class SqliteShardFileSession implements ShardDurabilitySession {
  constructor(
    private readonly provider: SqliteShardFileDurabilityProvider,
    private readonly inner: ShardDurabilitySession,
  ) {}

  get shardId(): number {
    return this.inner.shardId
  }

  get ownerId(): string | undefined {
    return this.inner.ownerId
  }

  get leaseEpoch(): number | undefined {
    return this.inner.leaseEpoch
  }

  createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    return this.provider.createInstance(input)
  }

  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.provider.createChildInstance(input)
  }

  cancelChild(input: CancelChildInput): Promise<void> {
    return this.inner.cancelChild(input)
  }

  readInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null> {
    return this.inner.readInstance(ref, options)
  }

  appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.inner.appendSignal(input)
  }

  claimTasks(input: ClaimShardTasksInput): Promise<ClaimShardTasksResult> {
    return this.inner.claimTasks(input)
  }

  heartbeat(input: { now: string; leaseMs: number }): Promise<void> {
    return this.inner.heartbeat(input)
  }

  release(): Promise<void> {
    return this.inner.release()
  }

  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    return this.inner.heartbeatActivations(input)
  }

  heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    return this.inner.heartbeatActivation(input)
  }

  releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    return this.inner.releaseActivations(input)
  }

  releaseActivation(input: ReleaseActivationInput): Promise<void> {
    return this.inner.releaseActivation(input)
  }

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.inner.getOrReserveEffect(input)
  }

  heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    return this.inner.heartbeatEffect(input)
  }

  completeEffect(input: CompleteEffectInput): Promise<void> {
    return this.inner.completeEffect(input)
  }

  failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.inner.failEffect(input)
  }

  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult> {
    this.provider.assertCommitChildStartsStayOnParentShard(input)
    return this.inner.commitActivations(input)
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    const result = await this.commitActivations([input])
    return commitCheckpointResult(result, input)
  }

  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void> {
    return this.inner.recordActivationFailures(input)
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
  return value
}

function groupByInputShard<T extends { workflowId: string; runId: string }>(
  inputs: T[],
  shardCount: number,
): Array<[number, Array<{ index: number; input: T }>]> {
  const groups = new Map<number, Array<{ index: number; input: T }>>()
  for (const [index, input] of inputs.entries()) {
    const shardId = workflowPartitionShard(input.workflowId, input.runId, shardCount)
    const group = groups.get(shardId) ?? []
    group.push({ index, input })
    groups.set(shardId, group)
  }
  return [...groups.entries()]
}

function groupByActivationShard(activationIds: string[], shardCount: number): Array<[number, string[]]> {
  const groups = new Map<number, string[]>()
  for (const activationId of activationIds) {
    const { workflowId, runId } = refFromActivationId(activationId)
    const shardId = workflowPartitionShard(workflowId, runId, shardCount)
    const group = groups.get(shardId) ?? []
    group.push(activationId)
    groups.set(shardId, group)
  }
  return [...groups.entries()]
}

function refFromActivationId(activationId: string): InstanceRef {
  const [workflowId, runId] = activationId.split("/")
  if (!workflowId || !runId) {
    throw new Error(`Cannot route activation id: ${activationId}`)
  }
  return { workflowId, runId }
}

function compareInstances(left: PersistedInstance, right: PersistedInstance): number {
  return `${left.workflowId}\0${left.runId}`.localeCompare(`${right.workflowId}\0${right.runId}`)
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values.filter((value): value is string => typeof value === "string").sort()[0]
}

function commitCheckpointResult(
  result: CommitActivationsResult,
  input: CommitCheckpointInput,
): CommitCheckpointResult {
  const first = result.results[0]
  if (!first) {
    return { ok: false, sequence: input.expectedSequence, reason: "missing_commit_result" }
  }
  const { activationId: _activationId, ...checkpoint } = first
  return checkpoint
}
