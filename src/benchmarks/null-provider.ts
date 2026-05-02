import { randomUUID } from "node:crypto"
import type {
  AppendSignalInput,
  CancelChildInput,
  CheckpointChildStart,
  CheckpointEffectMutation,
  ChildRecord,
  ClaimDispatchShardInput,
  ClaimedActivation,
  ClaimedActivationWithInstance,
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
  DurabilityProvider,
  DurableWait,
  EffectRecord,
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
  ReadyEvent,
  RecordActivationFailureInput,
  ReleaseActivationInput,
  ReleaseActivationsInput,
  ReleaseDispatchShardInput,
  ReserveEffectInput,
  ShardDurabilitySession,
  ShardLease,
  SignalRecord,
} from "../interface.js"
import { workflowPartitionShard } from "../interface.js"
import type { ChildHandle, InstanceRef, InstanceStatus, JsonValue, SerializedError } from "../workflow.js"
import { toJson } from "../workflow.js"

type RefKey = string

type MemoryTask = {
  taskId: string
  activationId: string
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  partitionShard: number
  sequence: number
  kind: "run" | "event" | "migration"
  waitName?: string
  wait?: Exclude<DurableWait, { kind: "run" }>
  event?: ReadyEvent
  readyAt: string
  sortKey: string
  claimOwnerId?: string
  claimEpoch?: number
  leaseUntil?: string
  blockedUntil?: string
}

type MemoryShardLease = {
  shardId: number
  ownerId: string
  leaseUntil: string
  leaseEpoch: number
}

type ChildStartConflict = {
  reason: string
  error: SerializedError
}

export class NullDurabilityProvider implements DurabilityProvider {
  private readonly instances = new Map<RefKey, PersistedInstance>()
  private readonly signals = new Map<string, SignalRecord>()
  private readonly children = new Map<string, ChildRecord>()
  private readonly tasks = new Map<string, MemoryTask>()
  private readonly taskIdsByRef = new Map<RefKey, Set<string>>()
  private readonly taskIdsByShard = new Map<number, Set<string>>()
  private readonly taskIdsByActivation = new Map<string, Set<string>>()
  private readonly effectsByActivation = new Map<string, EffectRecord[]>()
  private readonly claimedSequenceEpochs = new Map<string, number>()
  private readonly shardLeases = new Map<number, MemoryShardLease>()
  private signalCounter = 0
  private childCounter = 0

  close(): void {
    this.instances.clear()
    this.signals.clear()
    this.children.clear()
    this.tasks.clear()
    this.taskIdsByRef.clear()
    this.taskIdsByShard.clear()
    this.taskIdsByActivation.clear()
    this.effectsByActivation.clear()
    this.claimedSequenceEpochs.clear()
    this.shardLeases.clear()
  }

  async claimShard(input: ClaimDispatchShardInput): Promise<ShardLease | null> {
    const existing = this.shardLeases.get(input.shardId)
    if (existing && existing.ownerId !== input.ownerId && existing.leaseUntil > input.now) {
      return null
    }
    const leaseEpoch = existing?.ownerId === input.ownerId && existing.leaseUntil > input.now
      ? existing.leaseEpoch
      : (existing?.leaseEpoch ?? 0) + 1
    const lease = {
      shardId: input.shardId,
      ownerId: input.ownerId,
      leaseUntil: addMs(input.now, input.leaseMs),
      leaseEpoch,
    }
    this.shardLeases.set(input.shardId, lease)
    return clone(lease)
  }

  openShard(input: OpenShardInput): ShardDurabilitySession {
    return new NullShardSession(this, input)
  }

  async createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    const key = refKey(input)
    const existing = this.instances.get(key)
    if (existing) {
      if (input.conflictPolicy === "fail") {
        throw new Error(`Workflow instance already exists: ${input.workflowId}/${input.runId}`)
      }
      if (input.conflictPolicy !== "terminate_existing") {
        return { workflowId: input.workflowId, runId: input.runId }
      }
      this.deleteInstanceRecords(input.workflowId, input.runId)
    }

    const instance: PersistedInstance = {
      workflowName: input.workflowName,
      workflowVersion: input.workflowVersion,
      workflowId: input.workflowId,
      runId: input.runId,
      partitionShard: input.partitionShard,
      sequence: 0,
      status: "running",
      common: clone(input.common),
      phase: clone(input.phase),
      waits: clone(input.waits),
      createdAt: input.now,
      updatedAt: input.now,
      ...(input.parent ? { parent: clone(input.parent) } : {}),
    }
    this.instances.set(key, instance)
    this.replaceTasksForInstance(instance)
    return { workflowId: input.workflowId, runId: input.runId }
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    this.assertShardOwnerForRef(input.workflowId, input.runId, input.workerId, input.leaseNow)
    const parent = this.instances.get(refKey({
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
    }))
    if (!parent || parent.status !== "running") {
      throw new Error(`Unknown running parent: ${input.parentWorkflowId}/${input.parentRunId}`)
    }
    const conflict = this.validateChildStarts({
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
      activationId: input.activationId,
      childStarts: [input],
    })
    if (conflict) {
      throw new Error(conflict.error.message)
    }
    this.writeChildStart(input, input.now, {
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
      activationId: input.activationId,
    })
    return {
      workflowName: input.workflowName,
      workflowVersion: input.workflowVersion,
      workflowId: input.workflowId,
      runId: input.runId,
    }
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    this.assertShardOwnerForRef(input.parentWorkflowId, input.parentRunId, input.workerId, input.now)
    const child = [...this.children.values()].find(
      (record) =>
        record.parentWorkflowId === input.parentWorkflowId &&
        record.parentRunId === input.parentRunId &&
        record.workflowId === input.workflowId &&
        record.runId === input.runId,
    )
    if (!child || child.status !== "started") {
      return
    }
    const childInstance = this.instances.get(refKey(input))
    if (childInstance?.status === "running") {
      this.instances.set(refKey(input), {
        ...childInstance,
        status: "canceled",
        cancelReason: "Child canceled by parent",
        waits: [],
        updatedAt: input.now,
      })
      this.deleteTasksForRef(input.workflowId, input.runId)
    }
    child.status = "failed"
    child.completedAt = input.now
    child.error = { name: "ChildCanceled", message: "Child canceled by parent" }
    this.replaceTasksForRef(input.parentWorkflowId, input.parentRunId)
  }

  async loadInstance(ref: InstanceRef, options: LoadInstanceOptions = {}): Promise<PersistedInstance | null> {
    const instance = this.instances.get(refKey(ref))
    if (!instance) {
      return null
    }
    const copy = clone(instance)
    if (options.includeEffects) {
      copy.effects = [...this.effectsByActivation.values()].flat().filter((effect) =>
        effect.activationId.startsWith(`${ref.workflowId}/${ref.runId}/`),
      )
    }
    return copy
  }

  listInstances(): PersistedInstance[] {
    return [...this.instances.values()].map((instance) => clone(instance))
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    const instance = this.instances.get(refKey(input))
    const signal: SignalRecord = {
      signalId: `signal-${++this.signalCounter}`,
      workflowId: input.workflowId,
      runId: input.runId,
      type: input.type,
      payload: clone(input.payload),
      receivedAt: input.receivedAt,
    }
    this.signals.set(signal.signalId, signal)
    if (instance?.status === "running") {
      this.refreshSignalTasksForInstance(instance)
    }
    return clone(signal)
  }

  claimDispatchShard(input: ClaimDispatchShardInput): Promise<ShardLease | null> {
    return this.claimShard(input)
  }

  async heartbeatDispatchShard(input: HeartbeatDispatchShardInput): Promise<void> {
    const lease = this.shardLeases.get(input.shardId)
    if (!lease || lease.ownerId !== input.ownerId || lease.leaseUntil < input.now) {
      throw new Error(`Lost shard lease: ${input.shardId}`)
    }
    lease.leaseUntil = addMs(input.now, input.leaseMs)
  }

  async releaseDispatchShard(input: ReleaseDispatchShardInput): Promise<void> {
    const lease = this.shardLeases.get(input.shardId)
    if (lease?.ownerId === input.ownerId) {
      lease.leaseUntil = new Date(0).toISOString()
    }
  }

  async claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult> {
    if (!Number.isInteger(input.limit) || input.limit <= 0) {
      throw new Error("limit must be a positive integer")
    }
    const claims: ClaimedActivationWithInstance[] = []
    let nextWakeAt: string | undefined
    for (const shardId of input.shardIds) {
      if (claims.length >= input.limit) {
        break
      }
      const session = this.openShard({
        shardId,
        ownerId: input.workerId,
        leaseEpoch: this.shardLeases.get(shardId)?.leaseEpoch,
      })
      const batch = await session.claimTasks({
        workflows: input.workflows,
        shardCount: input.shardCount,
        now: input.now,
        leaseMs: input.leaseMs,
        limit: input.limit - claims.length,
      })
      claims.push(...batch.claims)
      nextWakeAt = earliestIso(nextWakeAt, batch.nextWakeAt)
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

  async claimShardTasks(
    session: NullShardSession,
    input: ClaimShardTasksInput,
  ): Promise<ClaimShardTasksResult> {
    if (!Number.isInteger(input.limit) || input.limit <= 0) {
      throw new Error("limit must be a positive integer")
    }
    if (!session.ownerId) {
      throw new Error(`Shard ${session.shardId} is not opened with an owner`)
    }
    const lease = this.requireShardLease(session.shardId, session.ownerId, input.now)
    if (session.leaseEpoch !== undefined && session.leaseEpoch !== lease.leaseEpoch) {
      throw new Error(`Lost shard lease: ${session.shardId}`)
    }

    const candidates = [...this.tasksForShard(session.shardId)]
      .filter((task) => task.readyAt <= input.now)
      .filter((task) => !task.blockedUntil || task.blockedUntil <= input.now)
      .filter((task) => {
        const workflow = input.workflows[task.workflowName]
        return workflow !== undefined && task.workflowVersion <= workflow.version
      })
      .sort(compareTasks)

    const claims: ClaimedActivationWithInstance[] = []
    const claimedSequences = new Set<string>()
    for (const task of candidates) {
      if (claims.length >= input.limit) {
        break
      }
      const sequenceKey = `${task.workflowId}\0${task.runId}\0${task.sequence}`
      if (claimedSequences.has(sequenceKey)) {
        continue
      }
      if (this.hasCompetingCurrentClaim(task, lease.leaseEpoch)) {
        continue
      }
      task.claimOwnerId = session.ownerId
      task.claimEpoch = lease.leaseEpoch
      task.leaseUntil = addMs(input.now, input.leaseMs)
      claimedSequences.add(sequenceKey)
      this.claimedSequenceEpochs.set(sequenceKey, lease.leaseEpoch)
      const instance = this.instances.get(refKey(task))
      if (!instance || instance.status !== "running" || instance.sequence !== task.sequence) {
        this.deleteTask(task.taskId)
        continue
      }
      claims.push({
        activation: activationFromTask(task),
        instance: clone(instance),
        effects: clone(this.effectsByActivation.get(task.activationId) ?? []),
        lease: { scope: "shard", shardId: session.shardId, epoch: lease.leaseEpoch },
      })
    }

    const nextWakeAt = this.nextWakeAt(session.shardId, input.now, input.workflows)
    return nextWakeAt ? { claims, nextWakeAt } : { claims }
  }

  async heartbeatActivations(_input: HeartbeatActivationsInput): Promise<void> {
    return
  }

  async heartbeatActivation(_input: HeartbeatActivationInput): Promise<void> {
    return
  }

  async releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    for (const activationId of input.activationIds) {
      await this.releaseActivation({ activationId, workerId: input.workerId })
    }
  }

  async releaseActivation(input: ReleaseActivationInput): Promise<void> {
    for (const task of this.tasks.values()) {
      if (task.activationId === input.activationId && task.claimOwnerId === input.workerId) {
        this.claimedSequenceEpochs.delete(sequenceKeyForTask(task))
        task.claimOwnerId = undefined
        task.claimEpoch = undefined
        task.leaseUntil = undefined
      }
    }
  }

  async getOrReserveEffect(_input: ReserveEffectInput): Promise<EffectReservation> {
    throw new Error("NullDurabilityProvider does not support eager activity durability")
  }

  async heartbeatEffect(_input: HeartbeatEffectInput): Promise<void> {
    throw new Error("NullDurabilityProvider does not support eager activity durability")
  }

  async completeEffect(_input: CompleteEffectInput): Promise<void> {
    throw new Error("NullDurabilityProvider does not support eager activity durability")
  }

  async failEffect(_input: FailEffectInput): Promise<FailEffectResult> {
    throw new Error("NullDurabilityProvider does not support eager activity durability")
  }

  async commitActivations(inputs: CommitActivationInput[]): Promise<CommitActivationsResult> {
    return {
      results: inputs.map((input) => ({
        ...this.commitOne(input),
        activationId: input.activationId,
      })),
    }
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    return (await this.commitActivations([input])).results[0] ?? {
      ok: false,
      sequence: -1,
      reason: "missing_commit_result",
    }
  }

  async recordActivationFailures(inputs: RecordActivationFailureInput[]): Promise<void> {
    for (const input of inputs) {
      const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
      if (!task) {
        throw new Error(`Lost activation lease: ${input.activationId}`)
      }
      const effects = checkpointEffectsToRecords(input, task)
      this.effectsByActivation.set(input.activationId, effects)
      const nextAttemptAt = effects
        .map((effect) => effect.nextAttemptAt)
        .filter((value): value is string => value !== undefined)
        .sort()[0]
      task.blockedUntil = nextAttemptAt
      if (input.releaseActivation) {
        this.claimedSequenceEpochs.delete(sequenceKeyForTask(task))
        task.claimOwnerId = undefined
        task.claimEpoch = undefined
        task.leaseUntil = undefined
      }
    }
  }

  private commitOne(input: CommitActivationInput): CommitCheckpointResult {
    const instance = this.instances.get(refKey(input))
    const conflict = (
      reason: string,
      sequence = instance?.sequence ?? -1,
      options: { retryable?: boolean; error?: SerializedError } = {},
    ): CommitCheckpointResult => ({
      ok: false,
      sequence,
      reason,
      ...(options.retryable === undefined ? {} : { retryable: options.retryable }),
      ...(options.error === undefined ? {} : { error: options.error }),
    })

    if (!instance || instance.status !== "running") {
      return conflict("not_running")
    }
    if (instance.sequence !== input.expectedSequence) {
      return conflict("stale_sequence", instance.sequence)
    }
    const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
    if (!task || task.workflowId !== input.workflowId || task.runId !== input.runId || task.sequence !== input.expectedSequence) {
      return conflict("lost_activation_lease", instance.sequence)
    }
    const currentLease = this.shardLeases.get(task.partitionShard)
    if (
      !currentLease ||
      currentLease.ownerId !== input.workerId ||
      currentLease.leaseEpoch !== task.claimEpoch ||
      currentLease.leaseUntil < input.now
    ) {
      return conflict("lost_activation_lease", instance.sequence)
    }
    if (!taskMatchesCommit(task, input)) {
      return conflict("activation_event_mismatch", instance.sequence)
    }
    if (input.consumeSignalId) {
      const signal = this.signals.get(input.consumeSignalId)
      if (!signal || signal.workflowId !== input.workflowId || signal.runId !== input.runId || signal.consumedBySequence !== undefined) {
        return conflict("signal_not_consumable", instance.sequence)
      }
    }
    if (input.consumeChildRecordId) {
      const childRecord = this.children.get(input.consumeChildRecordId)
      if (
        !childRecord ||
        childRecord.parentWorkflowId !== input.workflowId ||
        childRecord.parentRunId !== input.runId ||
        childRecord.deliveredBySequence !== undefined
      ) {
        return conflict("child_not_consumable", instance.sequence)
      }
    }
    const childConflict = this.validateChildStarts(input)
    if (childConflict) {
      return conflict(childConflict.reason, instance.sequence, {
        retryable: false,
        error: childConflict.error,
      })
    }

    const previous = clone(instance)
    const nextSequence = instance.sequence + 1
    const updated = this.nextInstance(instance, input, nextSequence)
    this.instances.set(refKey(input), updated)

    if (input.consumeSignalId) {
      const signal = this.signals.get(input.consumeSignalId)
      if (signal) {
        signal.consumedBySequence = nextSequence
      }
    }
    if (input.consumeChildRecordId) {
      const childRecord = this.children.get(input.consumeChildRecordId)
      if (childRecord) {
        childRecord.deliveredBySequence = nextSequence
      }
    }

    this.effectsByActivation.delete(input.activationId)
    for (const effect of checkpointEffectsToRecords(input, task)) {
      if (effect.status === "pending") {
        const effects = this.effectsByActivation.get(input.activationId) ?? []
        effects.push(effect)
        this.effectsByActivation.set(input.activationId, effects)
      }
    }
    for (const start of input.childStarts ?? []) {
      this.writeChildStart(start, input.now, input)
    }
    this.updateParentChildRecord(previous, input)
    this.applyParentClosePolicy(previous, input, nextSequence)
    this.deleteTasksForRef(input.workflowId, input.runId)
    this.replaceTasksForInstance(updated)
    return { ok: true, sequence: nextSequence }
  }

  private nextInstance(
    current: PersistedInstance,
    input: CommitActivationInput,
    nextSequence: number,
  ): PersistedInstance {
    const base = {
      workflowName: current.workflowName,
      workflowVersion: input.workflowVersion,
      workflowId: current.workflowId,
      runId: current.runId,
      partitionShard: current.partitionShard,
      sequence: nextSequence,
      createdAt: current.createdAt,
      updatedAt: input.now,
      ...(current.parent ? { parent: clone(current.parent) } : {}),
    }
    if (input.next.status === "running") {
      return {
        ...base,
        status: "running",
        common: clone(input.next.common),
        phase: clone(input.next.phase),
        waits: clone(input.waits),
      }
    }
    if (input.next.status === "completed") {
      return {
        ...base,
        status: "completed",
        output: clone(toJson(input.next.output)),
        waits: [],
      }
    }
    if (input.next.status === "canceled") {
      return {
        ...base,
        status: "canceled",
        cancelReason: input.next.reason,
        waits: [],
      }
    }
    return {
      ...base,
      status: "failed",
      error: clone(input.next.error),
      waits: [],
    }
  }

  private replaceTasksForRef(workflowId: string, runId: string): void {
    const instance = this.instances.get(refKey({ workflowId, runId }))
    if (instance) {
      this.replaceTasksForInstance(instance)
    }
  }

  private replaceTasksForInstance(instance: PersistedInstance): void {
    this.deleteTasksForRef(instance.workflowId, instance.runId)
    if (instance.status !== "running") {
      return
    }
    for (const wait of instance.waits) {
      this.insertTaskForWait(instance, wait)
    }
  }

  private refreshSignalTasksForInstance(instance: PersistedInstance): void {
    for (const task of this.tasksForRef(instance.workflowId, instance.runId)) {
      if (
        task.sequence === instance.sequence &&
        task.kind === "event" &&
        task.event?.kind === "signal"
      ) {
        this.deleteTask(task.taskId)
      }
    }
    for (const wait of instance.waits) {
      if (wait.kind === "signal") {
        this.insertTaskForWait(instance, wait)
      }
    }
  }

  private insertTaskForWait(instance: PersistedInstance, wait: DurableWait): void {
    if (wait.kind === "run") {
      this.insertTask(instance, {
        kind: "run",
        eventId: wait.name,
        readyAt: wait.readyAt,
        sortKey: sortKey(wait.readyAt, "run", wait.name, instance.workflowId, instance.runId),
      })
      return
    }
    if (wait.kind === "timer") {
      this.insertTask(instance, {
        kind: "event",
        eventId: `${wait.name}:${wait.fireAt}`,
        readyAt: wait.fireAt,
        wait,
        event: { kind: "timer", firedAt: wait.fireAt, occurredAt: wait.fireAt },
        sortKey: sortKey(wait.fireAt, "timer", wait.name, `${wait.name}:${wait.fireAt}`),
      })
      return
    }
    if (wait.kind === "signal") {
      const signal = [...this.signals.values()]
        .filter((record) =>
          record.workflowId === instance.workflowId &&
          record.runId === instance.runId &&
          record.type === wait.type &&
          record.consumedBySequence === undefined,
        )
        .sort((left, right) => sortKey(left.receivedAt, left.type, left.signalId)
          .localeCompare(sortKey(right.receivedAt, right.type, right.signalId)))[0]
      if (!signal) {
        return
      }
      this.insertTask(instance, {
        kind: "event",
        eventId: signal.signalId,
        readyAt: signal.receivedAt,
        wait,
        event: {
          kind: "signal",
          signalId: signal.signalId,
          payload: clone(signal.payload),
          occurredAt: signal.receivedAt,
          consumeSignalId: signal.signalId,
        },
        taskSuffix: wait.name,
        sortKey: sortKey(signal.receivedAt, "signal", wait.name, signal.signalId),
      })
      return
    }

    const child = [...this.children.values()]
      .filter((record) =>
        record.parentWorkflowId === instance.workflowId &&
        record.parentRunId === instance.runId &&
        record.workflowName === wait.workflowName &&
        record.workflowVersion === wait.workflowVersion &&
        record.workflowId === wait.workflowId &&
        record.runId === wait.runId &&
        (record.status === "completed" || record.status === "failed") &&
        record.deliveredBySequence === undefined,
      )
      .sort((left, right) => sortKey(left.completedAt ?? instance.updatedAt, left.childRecordId)
        .localeCompare(sortKey(right.completedAt ?? instance.updatedAt, right.childRecordId)))[0]
    if (!child) {
      return
    }
    const occurredAt = child.completedAt ?? instance.updatedAt
    this.insertTask(instance, {
      kind: "event",
      eventId: child.childRecordId,
      readyAt: occurredAt,
      wait,
      event: {
        kind: "child",
        childRecordId: child.childRecordId,
        occurredAt,
        event: child.status === "completed"
          ? { ok: true, output: clone(child.output) }
          : {
              ok: false,
              error: clone(child.error ?? { message: "Child failed" }),
            },
      },
      sortKey: sortKey(occurredAt, "child", wait.name, child.childRecordId),
    })
  }

  private insertTask(
    instance: PersistedInstance,
    input: {
      kind: "run" | "event" | "migration"
      eventId: string
      readyAt: string
      sortKey: string
      wait?: Exclude<DurableWait, { kind: "run" }>
      event?: ReadyEvent
      taskSuffix?: string
    },
  ): void {
    const activationId = activationIdFromParts(
      instance.workflowId,
      instance.runId,
      instance.sequence,
      input.event?.kind ?? input.kind,
      input.eventId,
    )
    const taskId = input.taskSuffix ? `${activationId}/${input.taskSuffix}` : activationId
    this.deleteTask(taskId)
    const task = {
      taskId,
      activationId,
      workflowName: instance.workflowName,
      workflowVersion: instance.workflowVersion,
      workflowId: instance.workflowId,
      runId: instance.runId,
      partitionShard: instance.partitionShard,
      sequence: instance.sequence,
      kind: input.kind,
      waitName: input.wait?.name,
      wait: clone(input.wait),
      event: clone(input.event),
      readyAt: input.readyAt,
      sortKey: input.sortKey,
    }
    this.tasks.set(taskId, task)
    addSetValue(this.taskIdsByRef, refKey(task), taskId)
    addSetValue(this.taskIdsByShard, task.partitionShard, taskId)
    addSetValue(this.taskIdsByActivation, activationId, taskId)
  }

  private deleteTasksForRef(workflowId: string, runId: string): void {
    const taskIds = [...(this.taskIdsByRef.get(refKey({ workflowId, runId })) ?? [])]
    for (const taskId of taskIds) {
      this.deleteTask(taskId)
    }
  }

  private deleteTask(taskId: string): void {
    const task = this.tasks.get(taskId)
    if (!task) {
      return
    }
    this.claimedSequenceEpochs.delete(sequenceKeyForTask(task))
    this.tasks.delete(taskId)
    deleteSetValue(this.taskIdsByRef, refKey(task), taskId)
    deleteSetValue(this.taskIdsByShard, task.partitionShard, taskId)
    deleteSetValue(this.taskIdsByActivation, task.activationId, taskId)
  }

  private findClaimedTask(
    activationId: string,
    workerId: string,
    now: string,
  ): MemoryTask | undefined {
    return this.tasksForActivation(activationId).find((task) =>
      task.activationId === activationId &&
      task.claimOwnerId === workerId &&
      task.leaseUntil !== undefined &&
      task.leaseUntil >= now,
    )
  }

  private hasCompetingCurrentClaim(task: MemoryTask, epoch: number): boolean {
    const sequenceKey = sequenceKeyForTask(task)
    const existingEpoch = this.claimedSequenceEpochs.get(sequenceKey)
    if (existingEpoch === undefined) {
      return false
    }
    if (existingEpoch !== epoch) {
      this.claimedSequenceEpochs.delete(sequenceKey)
      return false
    }
    return true
  }

  private nextWakeAt(
    shardId: number,
    now: string,
    workflows: Record<string, { version: number }>,
  ): string | undefined {
    const wakeTimes: string[] = []
    for (const task of this.tasksForShard(shardId)) {
      const workflow = workflows[task.workflowName]
      if (!workflow || task.workflowVersion > workflow.version) {
        continue
      }
      if (task.blockedUntil && task.blockedUntil > now) {
        wakeTimes.push(task.blockedUntil)
      } else if (task.readyAt > now) {
        wakeTimes.push(task.readyAt)
      }
    }
    return wakeTimes.sort()[0]
  }

  private tasksForShard(shardId: number): MemoryTask[] {
    return [...(this.taskIdsByShard.get(shardId) ?? [])]
      .map((taskId) => this.tasks.get(taskId))
      .filter((task): task is MemoryTask => task !== undefined)
  }

  private tasksForRef(workflowId: string, runId: string): MemoryTask[] {
    return [...(this.taskIdsByRef.get(refKey({ workflowId, runId })) ?? [])]
      .map((taskId) => this.tasks.get(taskId))
      .filter((task): task is MemoryTask => task !== undefined)
  }

  private tasksForActivation(activationId: string): MemoryTask[] {
    return [...(this.taskIdsByActivation.get(activationId) ?? [])]
      .map((taskId) => this.tasks.get(taskId))
      .filter((task): task is MemoryTask => task !== undefined)
  }

  private validateChildStarts(input: {
    workflowId: string
    runId: string
    activationId: string
    childStarts?: CheckpointChildStart[]
  }): ChildStartConflict | undefined {
    const seenKeys = new Set<string>()
    const seenRefs = new Map<string, CheckpointChildStart>()
    for (const start of input.childStarts ?? []) {
      if (seenKeys.has(start.key)) {
        return childStartCommitConflict("duplicate_child_start_key", start)
      }
      seenKeys.add(start.key)
      const childRefKey = refKey(start)
      if (seenRefs.has(childRefKey) && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("duplicate_child_start_instance", start)
      }
      seenRefs.set(childRefKey, start)
      const existingForKey = [...this.children.values()].find((record) =>
        record.parentWorkflowId === input.workflowId &&
        record.parentRunId === input.runId &&
        record.activationId === input.activationId &&
        record.key === start.key,
      )
      if (existingForKey && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("existing_child_activation_key", start)
      }
      const existingInstance = this.instances.get(childRefKey)
      if (existingInstance && start.conflictPolicy !== "terminate_existing") {
        return childStartCommitConflict("existing_child_instance", start)
      }
    }
    return undefined
  }

  private writeChildStart(
    start: CheckpointChildStart,
    now: string,
    parent: { workflowId: string; runId: string; activationId: string },
  ): void {
    for (const child of [...this.children.values()]) {
      if (
        child.parentWorkflowId === parent.workflowId &&
        child.parentRunId === parent.runId &&
        child.activationId === parent.activationId &&
        child.key === start.key &&
        start.conflictPolicy === "terminate_existing"
      ) {
        this.deleteInstanceRecords(child.workflowId, child.runId)
      }
    }
    if (this.instances.has(refKey(start)) && start.conflictPolicy === "terminate_existing") {
      this.deleteInstanceRecords(start.workflowId, start.runId)
    }

    const childRecordId = `child-${++this.childCounter}`
    const childInstance: PersistedInstance = {
      workflowName: start.workflowName,
      workflowVersion: start.workflowVersion,
      workflowId: start.workflowId,
      runId: start.runId,
      partitionShard: start.partitionShard,
      sequence: 0,
      status: "running",
      common: clone(start.common),
      phase: clone(start.phase),
      waits: clone(start.waits),
      createdAt: now,
      updatedAt: now,
      parent: {
        workflowId: parent.workflowId,
        runId: parent.runId,
        childRecordId,
      },
    }
    this.instances.set(refKey(start), childInstance)
    this.children.set(childRecordId, {
      childRecordId,
      parentWorkflowId: parent.workflowId,
      parentRunId: parent.runId,
      activationId: parent.activationId,
      key: start.key,
      workflowName: start.workflowName,
      workflowVersion: start.workflowVersion,
      workflowId: start.workflowId,
      runId: start.runId,
      status: "started",
      parentClosePolicy: start.parentClosePolicy ?? "cancel",
    })
    this.replaceTasksForInstance(childInstance)
  }

  private updateParentChildRecord(previous: PersistedInstance, input: CommitActivationInput): void {
    if (!previous.parent?.childRecordId || input.next.status === "running") {
      return
    }
    const child = this.children.get(previous.parent.childRecordId)
    if (!child || child.status !== "started") {
      return
    }
    child.completedAt = input.now
    if (input.next.status === "completed") {
      child.status = "completed"
      child.output = clone(toJson(input.next.output))
      child.error = undefined
    } else {
      child.status = "failed"
      child.output = undefined
      child.error = input.next.status === "failed"
        ? clone(input.next.error)
        : { message: input.next.reason || "Child canceled" }
    }
    this.replaceTasksForRef(child.parentWorkflowId, child.parentRunId)
  }

  private applyParentClosePolicy(
    previous: PersistedInstance,
    input: CommitActivationInput,
    nextSequence: number,
  ): void {
    if (input.next.status !== "failed" && input.next.status !== "canceled") {
      return
    }
    for (const child of [...this.children.values()]) {
      if (
        child.parentWorkflowId !== previous.workflowId ||
        child.parentRunId !== previous.runId ||
        child.status !== "started"
      ) {
        continue
      }
      if (child.parentClosePolicy === "abandon") {
        child.status = "abandoned"
        child.deliveredBySequence = nextSequence
        continue
      }
      const childInstance = this.instances.get(refKey(child))
      if (childInstance?.status === "running") {
        this.instances.set(refKey(child), {
          ...childInstance,
          status: "canceled",
          cancelReason: "Parent closed",
          waits: [],
          updatedAt: input.now,
        })
        this.deleteTasksForRef(child.workflowId, child.runId)
      }
      child.status = "failed"
      child.completedAt = input.now
      child.error = { name: "ParentClosed", message: "Parent closed" }
      child.deliveredBySequence = nextSequence
    }
  }

  private deleteInstanceRecords(workflowId: string, runId: string): void {
    this.instances.delete(refKey({ workflowId, runId }))
    this.deleteTasksForRef(workflowId, runId)
    for (const [signalId, signal] of this.signals) {
      if (signal.workflowId === workflowId && signal.runId === runId) {
        this.signals.delete(signalId)
      }
    }
    for (const [childRecordId, child] of this.children) {
      if (
        (child.parentWorkflowId === workflowId && child.parentRunId === runId) ||
        (child.workflowId === workflowId && child.runId === runId)
      ) {
        this.children.delete(childRecordId)
      }
    }
  }

  private requireShardLease(shardId: number, ownerId: string, now: string): MemoryShardLease {
    const lease = this.shardLeases.get(shardId)
    if (!lease || lease.ownerId !== ownerId || lease.leaseUntil < now) {
      throw new Error(`Lost shard lease: ${shardId}`)
    }
    return lease
  }

  private assertShardOwnerForRef(
    workflowId: string,
    runId: string,
    workerId: string,
    now: string,
  ): void {
    const instance = this.instances.get(refKey({ workflowId, runId }))
    const shardId = instance?.partitionShard ?? workflowPartitionShard(workflowId, runId, 1)
    this.requireShardLease(shardId, workerId, now)
  }
}

class NullShardSession implements ShardDurabilitySession {
  readonly shardId: number
  readonly ownerId?: string
  readonly leaseEpoch?: number

  constructor(
    private readonly provider: NullDurabilityProvider,
    input: OpenShardInput,
  ) {
    this.shardId = input.shardId
    this.ownerId = input.ownerId
    this.leaseEpoch = input.leaseEpoch
  }

  createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    if (input.partitionShard !== this.shardId) {
      throw new Error(`Shard session ${this.shardId} cannot write shard ${input.partitionShard}`)
    }
    return this.provider.createInstance(input)
  }

  createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    return this.provider.createChildInstance(input)
  }

  cancelChild(input: CancelChildInput): Promise<void> {
    return this.provider.cancelChild(input)
  }

  readInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null> {
    return this.provider.loadInstance(ref, options)
  }

  appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    return this.provider.appendSignal(input)
  }

  claimTasks(input: ClaimShardTasksInput): Promise<ClaimShardTasksResult> {
    return this.provider.claimShardTasks(this, input)
  }

  heartbeat(input: { now: string; leaseMs: number }): Promise<void> {
    if (!this.ownerId) {
      return Promise.resolve()
    }
    return this.provider.heartbeatDispatchShard({
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
    return this.provider.releaseDispatchShard({
      shardId: this.shardId,
      ownerId: this.ownerId,
    })
  }

  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    return this.provider.heartbeatActivations(input)
  }

  heartbeatActivation(input: HeartbeatActivationInput): Promise<void> {
    return this.provider.heartbeatActivation(input)
  }

  releaseActivations(input: ReleaseActivationsInput): Promise<void> {
    return this.provider.releaseActivations(input)
  }

  releaseActivation(input: ReleaseActivationInput): Promise<void> {
    return this.provider.releaseActivation(input)
  }

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    return this.provider.getOrReserveEffect(input)
  }

  heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    return this.provider.heartbeatEffect(input)
  }

  completeEffect(input: CompleteEffectInput): Promise<void> {
    return this.provider.completeEffect(input)
  }

  failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    return this.provider.failEffect(input)
  }

  commitActivations(input: CommitActivationInput[]): Promise<CommitActivationsResult> {
    return this.provider.commitActivations(input)
  }

  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult> {
    return this.provider.commitCheckpoint(input)
  }

  recordActivationFailures(input: RecordActivationFailureInput[]): Promise<void> {
    return this.provider.recordActivationFailures(input)
  }
}

function activationFromTask(task: MemoryTask): ClaimedActivation {
  if (task.kind === "run") {
    return {
      kind: "run",
      activationId: task.activationId,
      workflowName: task.workflowName,
      workflowId: task.workflowId,
      runId: task.runId,
      sequence: task.sequence,
      activationTime: task.readyAt,
      leaseUntil: task.leaseUntil ?? task.readyAt,
    }
  }
  if (task.kind === "migration") {
    return {
      kind: "migration",
      activationId: task.activationId,
      workflowName: task.workflowName,
      workflowId: task.workflowId,
      runId: task.runId,
      sequence: task.sequence,
      activationTime: task.readyAt,
      leaseUntil: task.leaseUntil ?? task.readyAt,
    }
  }
  return {
    kind: "event",
    activationId: task.activationId,
    workflowName: task.workflowName,
    workflowId: task.workflowId,
    runId: task.runId,
    sequence: task.sequence,
    activationTime: task.readyAt,
    waitName: task.waitName!,
    wait: clone(task.wait!),
    event: clone(task.event!),
    leaseUntil: task.leaseUntil ?? task.readyAt,
  }
}

function taskMatchesCommit(task: MemoryTask, input: CommitActivationInput): boolean {
  if (task.kind !== "event") {
    return !input.consumeSignalId && !input.consumeChildRecordId
  }
  if (!task.event) {
    return false
  }
  if (task.event.kind === "signal") {
    return input.consumeSignalId === task.event.consumeSignalId && !input.consumeChildRecordId
  }
  if (task.event.kind === "child") {
    return input.consumeChildRecordId === task.event.childRecordId && !input.consumeSignalId
  }
  return !input.consumeSignalId && !input.consumeChildRecordId
}

function checkpointEffectsToRecords(
  input: {
    workflowId: string
    runId: string
    activationId: string
    now: string
    effects?: CheckpointEffectMutation[]
  },
  task: Pick<MemoryTask, "activationId">,
): EffectRecord[] {
  return (input.effects ?? []).map((effect) => ({
    effectId: `effect-${randomUUID()}`,
    activationId: task.activationId,
    key: effect.key,
    idempotencyKey: effect.idempotencyKey ?? `${input.workflowId}/${input.runId}/${input.activationId}/${effect.key}`,
    status: effect.status === "retry_scheduled" ? "pending" : effect.status,
    attempt: effect.status === "retry_scheduled" ? effect.nextAttempt : effect.attempt ?? 1,
    attemptId: `attempt-${randomUUID()}`,
    firstAttemptStartedAt: effect.firstAttemptStartedAt ?? input.now,
    maxAttempts: effect.maxAttempts ?? 3,
    maxElapsedMs: effect.maxElapsedMs ?? undefined,
    initialIntervalMs: effect.initialIntervalMs ?? 1_000,
    maxIntervalMs: effect.maxIntervalMs ?? 30_000,
    backoffCoefficient: effect.backoffCoefficient ?? 2,
    nonRetryableErrorNames: effect.nonRetryableErrorNames,
    nextAttemptAt: effect.status === "retry_scheduled" ? effect.nextAttemptAt : undefined,
    result: effect.status === "completed" ? clone(effect.result) : undefined,
    error: effect.status === "failed" || effect.status === "retry_scheduled"
      ? clone(effect.error)
      : undefined,
    heartbeatDetails: clone(effect.heartbeatDetails),
  }))
}

function childStartCommitConflict(
  reason: string,
  start: CheckpointChildStart,
): ChildStartConflict {
  return {
    reason,
    error: {
      name: "ChildStartConflict",
      message: `Child start ${start.key} failed: ${reason} (${start.workflowId}/${start.runId})`,
    },
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

function compareTasks(left: MemoryTask, right: MemoryTask): number {
  return left.sortKey.localeCompare(right.sortKey)
}

function refKey(ref: { workflowId: string; runId: string }): RefKey {
  return `${ref.workflowId}\0${ref.runId}`
}

function addSetValue<K>(map: Map<K, Set<string>>, key: K, value: string): void {
  const existing = map.get(key)
  if (existing) {
    existing.add(value)
  } else {
    map.set(key, new Set([value]))
  }
}

function deleteSetValue<K>(map: Map<K, Set<string>>, key: K, value: string): void {
  const existing = map.get(key)
  if (!existing) {
    return
  }
  existing.delete(value)
  if (existing.size === 0) {
    map.delete(key)
  }
}

function sequenceKeyForTask(task: Pick<MemoryTask, "workflowId" | "runId" | "sequence">): string {
  return `${task.workflowId}\0${task.runId}\0${task.sequence}`
}

function sortKey(...parts: string[]): string {
  return parts.join("\0")
}

function addMs(isoValue: string, ms: number): string {
  return new Date(new Date(isoValue).getTime() + ms).toISOString()
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values.filter(Boolean).sort()[0]
}

function clone<T>(value: T): T {
  if (value === undefined || value === null) {
    return value
  }
  return structuredClone(value)
}
