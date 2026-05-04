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
  GetWorkflowRunsInput,
  GetWorkflowRunsResult,
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
  WorkflowIdReusePolicy,
} from "./interface.js"
import { workflowPartitionShard } from "./interface.js"
import type { ChildHandle, InstanceRef, InstanceStatus, JsonValue, SerializedError, StartWorkflowResult } from "./workflow.js"
import { toJson } from "./workflow.js"

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

export type ShardMemorySnapshot = {
  instances: PersistedInstance[]
  signals: SignalRecord[]
  children: ChildRecord[]
  tasks: MemoryTask[]
  effectsByActivation: Array<{ activationId: string; effects: EffectRecord[] }>
  claimedSequenceEpochs: Array<{ key: string; epoch: number }>
  completedActivationClaims: Array<{
    activationId: string
    workflowId: string
    runId: string
    sequence: number
    kind: "migration" | "run" | "event"
    ownerId?: string
    completedBySequence: number
  }>
  shardLeases: MemoryShardLease[]
  signalCounter: number
  childCounter: number
}

type ChildStartConflict = {
  reason: string
  error: SerializedError
}

export type ShardMemoryDurabilityProviderOptions = {
  unsafeNoClone?: boolean
}

export class ShardMemoryDurabilityProvider implements DurabilityProvider {
  private readonly instances = new Map<RefKey, PersistedInstance>()
  private readonly signals = new Map<string, SignalRecord>()
  private readonly children = new Map<string, ChildRecord>()
  private readonly tasks = new Map<string, MemoryTask>()
  private readonly taskIdsByRef = new Map<RefKey, Set<string>>()
  private readonly taskIdsByShard = new Map<number, Set<string>>()
  private readonly taskIdsByActivation = new Map<string, Set<string>>()
  private readonly effectsByActivation = new Map<string, EffectRecord[]>()
  private readonly claimedSequenceEpochs = new Map<string, number>()
  private readonly completedActivationClaims: ShardMemorySnapshot["completedActivationClaims"] = []
  private readonly shardLeases = new Map<number, MemoryShardLease>()
  private readonly unsafeNoClone: boolean
  private replaying = false
  private signalCounter = 0
  private childCounter = 0

  constructor(options: ShardMemoryDurabilityProviderOptions = {}) {
    this.unsafeNoClone = options.unsafeNoClone ?? false
  }

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
    this.completedActivationClaims.splice(0)
    this.shardLeases.clear()
    this.signalCounter = 0
    this.childCounter = 0
  }

  snapshot(): ShardMemorySnapshot {
    return this.clone({
      instances: [...this.instances.values()],
      signals: [...this.signals.values()],
      children: [...this.children.values()],
      tasks: [...this.tasks.values()],
      effectsByActivation: [...this.effectsByActivation.entries()].map(([activationId, effects]) => ({
        activationId,
        effects,
      })),
      claimedSequenceEpochs: [...this.claimedSequenceEpochs.entries()].map(([key, epoch]) => ({
        key,
        epoch,
      })),
      completedActivationClaims: this.completedActivationClaims,
      shardLeases: [...this.shardLeases.values()],
      signalCounter: this.signalCounter,
      childCounter: this.childCounter,
    })
  }

  restore(snapshot: ShardMemorySnapshot): void {
    this.instances.clear()
    this.signals.clear()
    this.children.clear()
    this.tasks.clear()
    this.taskIdsByRef.clear()
    this.taskIdsByShard.clear()
    this.taskIdsByActivation.clear()
    this.effectsByActivation.clear()
    this.claimedSequenceEpochs.clear()
    this.completedActivationClaims.splice(0)
    this.shardLeases.clear()

    for (const instance of snapshot.instances) {
      this.instances.set(refKey(instance), this.clone(instance))
    }
    for (const signal of snapshot.signals) {
      this.signals.set(signal.signalId, this.clone(signal))
    }
    for (const child of snapshot.children) {
      this.children.set(child.childRecordId, this.clone(child))
    }
    for (const task of snapshot.tasks) {
      const copy = this.clone(task)
      this.tasks.set(copy.taskId, copy)
      addSetValue(this.taskIdsByRef, refKey(copy), copy.taskId)
      addSetValue(this.taskIdsByShard, copy.partitionShard, copy.taskId)
      addSetValue(this.taskIdsByActivation, copy.activationId, copy.taskId)
    }
    for (const entry of snapshot.effectsByActivation) {
      this.effectsByActivation.set(entry.activationId, this.clone(entry.effects))
    }
    for (const entry of snapshot.claimedSequenceEpochs) {
      this.claimedSequenceEpochs.set(entry.key, entry.epoch)
    }
    this.completedActivationClaims.push(...this.clone(snapshot.completedActivationClaims ?? []))
    for (const lease of snapshot.shardLeases) {
      this.shardLeases.set(lease.shardId, this.clone(lease))
    }
    this.signalCounter = snapshot.signalCounter
    this.childCounter = snapshot.childCounter
  }

  async replay<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.replaying
    this.replaying = true
    try {
      return await fn()
    } finally {
      this.replaying = previous
    }
  }

  replaySync<T>(fn: () => T): T {
    const previous = this.replaying
    this.replaying = true
    try {
      return fn()
    } finally {
      this.replaying = previous
    }
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
    return this.clone(lease)
  }

  setShardLease(lease: ShardLease): void {
    this.shardLeases.set(lease.shardId, this.clone({
      shardId: lease.shardId,
      ownerId: lease.ownerId,
      leaseUntil: lease.leaseUntil,
      leaseEpoch: lease.leaseEpoch,
    }))
  }

  clearShardLease(shardId: number): void {
    this.shardLeases.delete(shardId)
  }

  openShard(input: OpenShardInput): ShardDurabilitySession {
    return new ShardMemorySession(this, input)
  }

  async createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult> {
    if (input.workflowIdReusePolicy) {
      const latest = this.latestWorkflowRun(input.workflowId)
      if (latest && !shouldCreateWorkflowRun(input.workflowIdReusePolicy, latest.status)) {
        return { workflowId: latest.workflowId, runId: latest.runId, created: false }
      }
    }

    const key = refKey(input)
    const existing = this.instances.get(key)
    if (existing) {
      if (input.conflictPolicy === "fail") {
        throw new Error(`Workflow instance already exists: ${input.workflowId}/${input.runId}`)
      }
      if (input.conflictPolicy !== "terminate_existing") {
        return { workflowId: input.workflowId, runId: input.runId, created: false }
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
      common: this.clone(input.common),
      phase: this.clone(input.phase),
      waits: this.stampSignalWaits(input.waits),
      createdAt: input.now,
      updatedAt: input.now,
      ...(input.parent ? { parent: this.clone(input.parent) } : {}),
    }
    this.instances.set(key, instance)
    this.replaceTasksForInstance(instance)
    return { workflowId: input.workflowId, runId: input.runId, created: true }
  }

  async createChildInstance(input: CreateChildInstanceInput): Promise<ChildHandle> {
    if (!this.replaying) {
      this.assertShardOwnerForRef(input.workflowId, input.runId, input.workerId, input.leaseNow)
    }
    const parent = this.instances.get(refKey({
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
    }))
    if (!parent || parent.status !== "running") {
      throw new Error(`Unknown running parent: ${input.parentWorkflowId}/${input.parentRunId}`)
    }
    const conflictPolicy = input.conflictPolicy ?? "use_existing"
    const existingForKey = [...this.children.values()].find((record) =>
      record.parentWorkflowId === input.parentWorkflowId &&
      record.parentRunId === input.parentRunId &&
      record.activationId === input.activationId &&
      record.key === input.key,
    )
    if (existingForKey && conflictPolicy === "use_existing") {
      return childHandle(existingForKey)
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
    return this.writeChildStart(input, input.now, {
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
      activationId: input.activationId,
    })
  }

  async cancelChild(input: CancelChildInput): Promise<void> {
    if (!this.replaying) {
      this.assertShardOwnerForRef(input.parentWorkflowId, input.parentRunId, input.workerId, input.now)
    }
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
    const parent = this.instances.get(refKey({
      workflowId: input.parentWorkflowId,
      runId: input.parentRunId,
    }))
    if (parent?.status === "running") {
      this.refreshChildTasksForInstance(parent)
    }
  }

  async loadInstance(ref: InstanceRef, options: LoadInstanceOptions = {}): Promise<PersistedInstance | null> {
    const instance = this.instances.get(refKey(ref))
    if (!instance) {
      return null
    }
    const copy = this.clone(instance)
    if (options.includeEffects) {
      copy.effects = [...this.effectsByActivation.values()].flat().filter((effect) =>
        effect.activationId.startsWith(`${ref.workflowId}/${ref.runId}/`),
      )
    }
    return copy
  }

  async getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult> {
    const limit = input.limit ?? 100
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new Error("limit must be a positive integer")
    }
    const direction = input.direction ?? "asc"
    const cursor = decodeWorkflowRunsCursor(input.cursor, direction)
    const sorted = this.workflowRuns(input.id)
    const ordered = direction === "asc" ? sorted : [...sorted].reverse()
    const afterCursor = cursor
      ? ordered.filter((run) => compareWorkflowRunPosition(run, cursor) * (direction === "asc" ? 1 : -1) > 0)
      : ordered
    const page = afterCursor.slice(0, limit)
    const runs = page.map((run) => this.cloneInstanceForRead(run, input))
    const next = afterCursor.length > limit ? page.at(-1) : undefined
    return {
      runs,
      ...(next ? { cursor: encodeWorkflowRunsCursor(next, direction) } : {}),
    }
  }

  listInstances(): PersistedInstance[] {
    return [...this.instances.values()].map((instance) => this.clone(instance))
  }

  listSignals(): SignalRecord[] {
    return [...this.signals.values()]
      .sort((left, right) => sortKey(left.receivedAt, left.signalId)
        .localeCompare(sortKey(right.receivedAt, right.signalId)))
      .map((signal) => this.clone(signal))
  }

  findSignalByIdempotencyKey(input: AppendSignalInput): SignalRecord | undefined {
    const idempotencyKey = input.idempotencyKey || undefined
    if (!idempotencyKey) {
      return undefined
    }
    const existing = [...this.signals.values()].find((signal) =>
      signal.workflowId === input.workflowId &&
      signal.runId === input.runId &&
      signal.type === input.type &&
      signal.idempotencyKey === idempotencyKey
    )
    return existing ? this.clone(existing) : undefined
  }

  listChildren(): ChildRecord[] {
    return [...this.children.values()]
      .sort((left, right) => left.childRecordId.localeCompare(right.childRecordId))
      .map((child) => this.clone(child))
  }

  listActivationClaims(): Array<{
    activationId: string
    workflowId: string
    runId: string
    sequence: number
    kind: "migration" | "run" | "event"
    ownerId?: string
    completedBySequence?: number
  }> {
    const byActivationId = new Map<string, {
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      kind: "migration" | "run" | "event"
      ownerId?: string
      completedBySequence?: number
    }>()
    for (const claim of this.completedActivationClaims) {
      byActivationId.set(claim.activationId, this.clone(claim))
    }
    for (const task of this.tasks.values()) {
      if (byActivationId.has(task.activationId)) {
        continue
      }
      byActivationId.set(task.activationId, {
        activationId: task.activationId,
        workflowId: task.workflowId,
        runId: task.runId,
        sequence: task.sequence,
        kind: task.kind === "event" ? "event" as const : task.kind,
        ownerId: task.claimOwnerId,
      })
    }
    return [...byActivationId.values()]
      .sort((left, right) =>
        `${left.workflowId}\0${left.runId}\0${left.sequence}\0${left.activationId}`.localeCompare(
          `${right.workflowId}\0${right.runId}\0${right.sequence}\0${right.activationId}`,
        ),
      )
  }

  async appendSignal(input: AppendSignalInput): Promise<SignalRecord> {
    const instance = this.instances.get(refKey(input))
    const existing = this.findSignalByIdempotencyKey(input)
    if (existing) {
      return existing
    }
    const idempotencyKey = input.idempotencyKey || undefined
    const signal: SignalRecord = {
      signalId: `signal-${++this.signalCounter}`,
      workflowId: input.workflowId,
      runId: input.runId,
      type: input.type,
      payload: this.clone(input.payload),
      receivedAt: input.receivedAt,
      ...(idempotencyKey ? { idempotencyKey } : {}),
    }
    this.signals.set(signal.signalId, signal)
    if (instance?.status === "running") {
      this.refreshSignalTasksForInstance(instance)
    }
    return this.clone(signal)
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
      throw new Error("claimReadyActivations limit must be a positive integer")
    }
    const claims: ClaimedActivationWithInstance[] = []
    let nextWakeAt: string | undefined
    for (const shardId of input.shardIds) {
      if (claims.length >= input.limit) {
        break
      }
      const lease = this.shardLeases.get(shardId)
      if (
        !lease ||
        lease.ownerId !== input.workerId ||
        lease.leaseUntil < input.now
      ) {
        continue
      }
      const session = this.openShard({
        shardId,
        ownerId: input.workerId,
        leaseEpoch: lease.leaseEpoch,
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
    session: ShardMemorySession,
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
    this.expireActivityTimeouts(input.now, {
      shardId: session.shardId,
      ownerId: session.ownerId,
    })
    this.refreshMigrationTasks(session.shardId, input.now, input.workflows)

    const candidates = [...this.tasksForShard(session.shardId)]
      .filter((task) => task.readyAt <= input.now)
      .filter((task) => !task.blockedUntil || task.blockedUntil <= input.now)
      .filter((task) => !this.taskHasUnexpiredPendingAttempt(task, input.now))
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
        activation: activationFromTask(task, this.clone),
        instance: this.clone(instance),
        effects: this.clone(this.effectsByActivation.get(task.activationId) ?? []),
        lease: { scope: "shard", shardId: session.shardId, epoch: lease.leaseEpoch },
      })
    }

    const nextWakeAt = this.nextWakeAt(session.shardId, input.now, input.workflows)
    return nextWakeAt ? { claims, nextWakeAt } : { claims }
  }

  async heartbeatActivations(input: HeartbeatActivationsInput): Promise<void> {
    this.expireActivityTimeouts(input.now, { ownerId: input.workerId })
    for (const activationId of input.activationIds) {
      const task = this.findClaimedTask(activationId, input.workerId, input.now)
      if (!task) {
        throw new Error(`Lost activation lease: ${activationId}`)
      }
      task.leaseUntil = addMs(input.now, input.leaseMs)
    }
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

  async getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation> {
    this.expireActivityTimeouts(input.now, {
      activationId: input.activationId,
      ownerId: input.workerId,
    })
    const existing = this.effectFor(input.activationId, input.key)
    if (existing?.status === "completed") {
      return { status: "completed", result: this.clone(existing.result ?? null) }
    }
    if (existing?.status === "failed") {
      return { status: "failed", error: this.clone(existing.error ?? { message: "Activity failed" }) }
    }
    const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
    if (!task) {
      throw new Error(`Lost activation lease: ${input.activationId}`)
    }
    const options = normalizeEffectOptions(input)
    const now = input.now
    const attempt = existing?.nextAttemptAt && existing.nextAttemptAt <= now
      ? (existing.attempt ?? 1)
      : existing?.attempt ?? 1
    const startToCloseTimeoutMs = existing
      ? existing.startToCloseTimeoutMs ?? null
      : options.startToCloseTimeoutMs
    const heartbeatTimeoutMs = existing
      ? existing.heartbeatTimeoutMs ?? null
      : options.heartbeatTimeoutMs
    const effect: EffectRecord = {
      effectId: existing?.effectId ?? effectIdFor(input.activationId, input.key),
      activationId: input.activationId,
      key: input.key,
      idempotencyKey: existing?.idempotencyKey ?? `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`,
      status: "pending",
      attempt,
      attemptId: attemptIdFor(input.activationId, input.key, attempt, input.workerId, now),
      attemptOwnerId: input.workerId,
      attemptStartedAt: now,
      startToCloseTimeoutMs: startToCloseTimeoutMs ?? undefined,
      startToCloseDeadline: deadlineFrom(now, startToCloseTimeoutMs) ?? undefined,
      heartbeatTimeoutMs: heartbeatTimeoutMs ?? undefined,
      heartbeatDeadline: deadlineFrom(now, heartbeatTimeoutMs) ?? undefined,
      maxAttempts: existing?.maxAttempts ?? options.maxAttempts,
      maxElapsedMs: existing?.maxElapsedMs ?? options.maxElapsedMs ?? undefined,
      initialIntervalMs: existing?.initialIntervalMs ?? options.initialIntervalMs,
      maxIntervalMs: existing?.maxIntervalMs ?? options.maxIntervalMs,
      backoffCoefficient: existing?.backoffCoefficient ?? options.backoffCoefficient,
      firstAttemptStartedAt: existing?.firstAttemptStartedAt ?? now,
      nonRetryableErrorNames: existing?.nonRetryableErrorNames ?? options.nonRetryableErrorNames,
      heartbeatDetails: this.clone(existing?.heartbeatDetails),
      lastFailure: this.clone(existing?.lastFailure),
    }
    this.upsertEffect(input.activationId, effect)
    task.blockedUntil = undefined
    return {
      status: "reserved",
      effectId: effect.effectId,
      idempotencyKey: effect.idempotencyKey,
      attempt,
      attemptId: effect.attemptId!,
      heartbeatDetails: this.clone(effect.heartbeatDetails),
    }
  }

  async heartbeatEffect(input: HeartbeatEffectInput): Promise<void> {
    this.expireActivityTimeouts(input.now, {
      activationId: input.activationId,
      ownerId: input.workerId,
    })
    const effect = this.pendingEffectForAttempt(input)
    this.assertMutableEffect(input, effect)
    effect.heartbeatAt = input.now
    effect.heartbeatDetails = this.clone(input.details)
    effect.heartbeatDeadline = deadlineFrom(input.now, effect.heartbeatTimeoutMs ?? null) ?? undefined
  }

  async completeEffect(input: CompleteEffectInput): Promise<void> {
    this.expireActivityTimeouts(input.now, {
      activationId: input.activationId,
      ownerId: input.workerId,
    })
    const effect = this.pendingEffectForAttempt(input)
    this.assertMutableEffect(input, effect)
    effect.status = "completed"
    effect.result = this.clone(input.result)
    effect.error = undefined
    effect.attemptOwnerId = undefined
    effect.attemptId = undefined
    effect.attemptStartedAt = undefined
    effect.startToCloseDeadline = undefined
    effect.heartbeatDeadline = undefined
    this.clearTaskEffectsIfTerminal(input.activationId)
  }

  async failEffect(input: FailEffectInput): Promise<FailEffectResult> {
    this.expireActivityTimeouts(input.now, {
      activationId: input.activationId,
      ownerId: input.workerId,
    })
    const effect = this.pendingEffectForAttempt(input)
    this.assertMutableEffect(input, effect)
    const decision = retryDecisionForEffect(effect, input.error, input.now, input.retryable !== false)
    if (decision.status === "retry_scheduled") {
      effect.status = "pending"
      effect.error = this.clone(input.error)
      effect.lastFailure = this.clone(input.error)
      effect.nextAttemptAt = decision.nextAttemptAt
      effect.attempt = decision.nextAttempt
      effect.attemptId = undefined
      effect.attemptOwnerId = undefined
      effect.attemptStartedAt = undefined
      effect.startToCloseDeadline = undefined
      effect.heartbeatDeadline = undefined
      const task = this.tasksForActivation(input.activationId)[0]
      if (task) {
        task.blockedUntil = decision.nextAttemptAt
        task.claimOwnerId = undefined
        task.claimEpoch = undefined
        task.leaseUntil = undefined
        this.claimedSequenceEpochs.delete(sequenceKeyForTask(task))
      }
      return decision
    }
    effect.status = "failed"
    effect.error = this.clone(input.error)
    effect.lastFailure = this.clone(input.error)
    effect.attemptId = undefined
    effect.attemptOwnerId = undefined
    effect.attemptStartedAt = undefined
    effect.startToCloseDeadline = undefined
    effect.heartbeatDeadline = undefined
    this.clearTaskEffectsIfTerminal(input.activationId)
    return { status: "failed" }
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
    const result = (await this.commitActivations([input])).results[0] ?? {
      ok: false,
      sequence: -1,
      reason: "missing_commit_result",
    }
    const { activationId: _activationId, ...checkpoint } = result
    return checkpoint
  }

  async recordActivationFailures(inputs: RecordActivationFailureInput[]): Promise<void> {
    for (const input of inputs) {
      const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
      if (!task) {
        throw new Error(`Lost activation lease: ${input.activationId}`)
      }
      const effects = checkpointEffectsToRecords(input, task, this.clone)
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
    const task = this.findTaskClaimedByShardEpoch(input.activationId, input.workerId)
    if (!task || task.workflowId !== input.workflowId || task.runId !== input.runId || task.sequence !== input.expectedSequence) {
      return conflict("lost_shard_task_lease", instance.sequence)
    }
    const currentLease = this.shardLeases.get(task.partitionShard)
    if (
      !this.replaying &&
      (!currentLease ||
      currentLease.ownerId !== input.workerId ||
      currentLease.leaseEpoch !== task.claimEpoch ||
      currentLease.leaseUntil < input.now)
    ) {
      return conflict("lost_shard_task_lease", instance.sequence)
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

    const previous = this.clone(instance)
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
    for (const effect of checkpointEffectsToRecords(input, task, this.clone)) {
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
    this.completedActivationClaims.push({
      activationId: input.activationId,
      workflowId: input.workflowId,
      runId: input.runId,
      sequence: input.expectedSequence,
      kind: task.kind === "event" ? "event" : task.kind,
      ownerId: input.workerId,
      completedBySequence: nextSequence,
    })
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
      ...(current.parent ? { parent: this.clone(current.parent) } : {}),
    }
    if (input.next.status === "running") {
      return {
        ...base,
        status: "running",
        common: this.clone(input.next.common),
        phase: this.clone(input.next.phase),
        waits: this.stampSignalWaits(input.waits, current.waits),
      }
    }
    if (input.next.status === "completed") {
      return {
        ...base,
        status: "completed",
        output: this.clone(toJson(input.next.output)),
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
      error: this.clone(input.next.error),
      waits: [],
    }
  }

  private stampSignalWaits(waits: DurableWait[], previousWaits: DurableWait[] = []): DurableWait[] {
    return waits.map((wait) => {
      if (wait.kind !== "signal") {
        return this.clone(wait)
      }

      const delivery = signalDelivery(wait)
      if (delivery !== "future") {
        const stamped = this.clone(wait)
        delete stamped.afterSignalSequence
        return stamped
      }

      const previous = previousWaits.find((
        candidate,
      ): candidate is Extract<DurableWait, { kind: "signal" }> =>
        candidate.kind === "signal" &&
        signalDelivery(candidate) === "future" &&
        candidate.name === wait.name &&
        candidate.type === wait.type &&
        candidate.scope === wait.scope &&
        candidate.afterSignalSequence !== undefined
      )

      return {
        ...this.clone(wait),
        delivery,
        afterSignalSequence: previous?.afterSignalSequence ?? this.signalCounter,
      }
    })
  }

  private signalIsAfterWaitCursor(
    signal: SignalRecord,
    wait: Extract<DurableWait, { kind: "signal" }>,
  ): boolean {
    if (signalDelivery(wait) !== "future") {
      return true
    }
    return signalSequence(signal.signalId) > (wait.afterSignalSequence ?? 0)
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

  private refreshChildTasksForInstance(instance: PersistedInstance): void {
    for (const task of this.tasksForRef(instance.workflowId, instance.runId)) {
      if (
        task.sequence === instance.sequence &&
        task.kind === "event" &&
        task.event?.kind === "child"
      ) {
        this.deleteTask(task.taskId)
      }
    }
    for (const wait of instance.waits) {
      if (wait.kind === "child") {
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
          record.consumedBySequence === undefined &&
          this.signalIsAfterWaitCursor(record, wait),
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
          payload: this.clone(signal.payload),
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
          ? { ok: true, output: this.clone(child.output) }
          : {
              ok: false,
              error: this.clone(child.error ?? { message: "Child failed" }),
            },
      },
      sortKey: sortKey(occurredAt, "child", wait.name, child.childRecordId),
    })
  }

  private refreshMigrationTasks(
    shardId: number,
    now: string,
    workflows: Record<string, { version: number }>,
  ): void {
    for (const instance of this.instances.values()) {
      if (instance.partitionShard !== shardId || instance.status !== "running") {
        continue
      }
      const workflow = workflows[instance.workflowName]
      if (!workflow || instance.workflowVersion >= workflow.version) {
        for (const task of this.tasksForRef(instance.workflowId, instance.runId)) {
          if (task.kind === "migration" && task.sequence === instance.sequence) {
            this.deleteTask(task.taskId)
          }
        }
        continue
      }
      const hasCurrentTasks = this.tasksForRef(instance.workflowId, instance.runId)
        .some((task) => task.sequence === instance.sequence)
      if (hasCurrentTasks) {
        continue
      }
      this.insertTask(instance, {
        kind: "migration",
        eventId: `migration-${workflow.version}`,
        readyAt: now,
        sortKey: sortKey(now, "migration", instance.workflowName, instance.workflowId, instance.runId),
      })
    }
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
      wait: this.clone(input.wait),
      event: this.clone(input.event),
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
    ) ?? (this.replaying ? this.tasksForActivation(activationId)[0] : undefined)
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
      for (const effect of this.effectsByActivation.get(task.activationId) ?? []) {
        if (effect.status !== "pending") {
          continue
        }
        if (effect.nextAttemptAt && effect.nextAttemptAt > now) {
          wakeTimes.push(effect.nextAttemptAt)
        }
        if (effect.startToCloseDeadline && effect.startToCloseDeadline > now) {
          wakeTimes.push(effect.startToCloseDeadline)
        }
        if (effect.heartbeatDeadline && effect.heartbeatDeadline > now) {
          wakeTimes.push(effect.heartbeatDeadline)
        }
      }
    }
    return wakeTimes.sort()[0]
  }

  private taskHasUnexpiredPendingAttempt(task: MemoryTask, now: string): boolean {
    return (this.effectsByActivation.get(task.activationId) ?? []).some((effect) =>
      effect.status === "pending" &&
      effect.attemptId !== undefined &&
      (effect.startToCloseDeadline !== undefined || effect.heartbeatDeadline !== undefined) &&
      earliestIso(effect.startToCloseDeadline, effect.heartbeatDeadline)! > now,
    )
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

  private clone = <T>(value: T): T => {
    return this.unsafeNoClone ? value : clone(value)
  }

  private cloneInstanceForRead(instance: PersistedInstance, options: LoadInstanceOptions = {}): PersistedInstance {
    const copy = this.clone(instance)
    if (options.includeEffects) {
      copy.effects = [...this.effectsByActivation.values()].flat().filter((effect) =>
        effect.activationId.startsWith(`${instance.workflowId}/${instance.runId}/`),
      )
    }
    return copy
  }

  private workflowRuns(workflowId: string): PersistedInstance[] {
    return [...this.instances.values()]
      .filter((instance) => instance.workflowId === workflowId)
      .sort(compareWorkflowRunsAsc)
  }

  private latestWorkflowRun(workflowId: string): PersistedInstance | undefined {
    return this.workflowRuns(workflowId).at(-1)
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
      if (
        seenRefs.has(childRefKey) &&
        start.conflictPolicy !== "terminate_existing" &&
        (!start.workflowIdReusePolicy || start.workflowIdReusePolicy === "always")
      ) {
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
      if (
        existingInstance &&
        start.conflictPolicy !== "terminate_existing" &&
        (!start.workflowIdReusePolicy || shouldCreateWorkflowRun(start.workflowIdReusePolicy, existingInstance.status))
      ) {
        return childStartCommitConflict("existing_child_instance", start)
      }
    }
    return undefined
  }

  private writeChildStart(
    start: CheckpointChildStart,
    now: string,
    parent: { workflowId: string; runId: string; activationId: string },
  ): ChildHandle {
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
    const workflowIdReusePolicy = start.workflowIdReusePolicy
    const latest = workflowIdReusePolicy ? this.latestWorkflowRun(start.workflowId) : undefined
    if (latest && workflowIdReusePolicy && !shouldCreateWorkflowRun(workflowIdReusePolicy, latest.status)) {
      return this.writeChildRecordForInstance(start, latest, parent, false)
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
      common: this.clone(start.common),
      phase: this.clone(start.phase),
      waits: this.stampSignalWaits(start.waits),
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
    return {
      workflowName: start.workflowName,
      workflowVersion: start.workflowVersion,
      workflowId: start.workflowId,
      runId: start.runId,
      created: true,
    }
  }

  private writeChildRecordForInstance(
    start: Pick<CheckpointChildStart, "key" | "workflowName" | "workflowVersion" | "parentClosePolicy">,
    instance: PersistedInstance,
    parent: { workflowId: string; runId: string; activationId: string },
    created: boolean,
  ): ChildHandle {
    const childRecordId = `child-${++this.childCounter}`
    this.children.set(childRecordId, {
      childRecordId,
      parentWorkflowId: parent.workflowId,
      parentRunId: parent.runId,
      activationId: parent.activationId,
      key: start.key,
      workflowName: instance.workflowName,
      workflowVersion: instance.workflowVersion,
      workflowId: instance.workflowId,
      runId: instance.runId,
      status: "started",
      parentClosePolicy: start.parentClosePolicy ?? "cancel",
    })
    return {
      workflowName: instance.workflowName,
      workflowVersion: instance.workflowVersion,
      workflowId: instance.workflowId,
      runId: instance.runId,
      created,
    }
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
      child.output = this.clone(toJson(input.next.output))
      child.error = undefined
    } else {
      child.status = "failed"
      child.output = undefined
      child.error = input.next.status === "failed"
        ? this.clone(input.next.error)
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
      this.cancelChildTreeForParentClose(child, input.now, nextSequence)
    }
  }

  private cancelChildTreeForParentClose(
    child: ChildRecord,
    now: string,
    deliveredBySequence: number,
  ): void {
    const childInstance = this.instances.get(refKey(child))
    if (childInstance?.status === "running") {
      this.instances.set(refKey(child), {
        ...childInstance,
        status: "canceled",
        cancelReason: "Child canceled because parent canceled",
        waits: [],
        updatedAt: now,
      })
      this.deleteTasksForRef(child.workflowId, child.runId)
    }
    child.status = "failed"
    child.completedAt = now
    child.error = { name: "ParentClosed", message: "Child canceled because parent canceled" }
    child.deliveredBySequence = deliveredBySequence

    for (const descendant of [...this.children.values()]) {
      if (
        descendant.parentWorkflowId !== child.workflowId ||
        descendant.parentRunId !== child.runId ||
        descendant.status !== "started"
      ) {
        continue
      }
      if (descendant.parentClosePolicy === "abandon") {
        descendant.status = "abandoned"
        descendant.deliveredBySequence = deliveredBySequence
        continue
      }
      this.cancelChildTreeForParentClose(descendant, now, deliveredBySequence)
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

  private effectFor(activationId: string, key: string): EffectRecord | undefined {
    return (this.effectsByActivation.get(activationId) ?? []).find((effect) => effect.key === key)
  }

  private upsertEffect(activationId: string, next: EffectRecord): void {
    const effects = this.effectsByActivation.get(activationId) ?? []
    const index = effects.findIndex((effect) => effect.key === next.key)
    if (index === -1) {
      effects.push(next)
    } else {
      effects[index] = next
    }
    this.effectsByActivation.set(activationId, effects)
  }

  private pendingEffectForAttempt(input: {
    workflowId: string
    runId: string
    activationId: string
    workerId: string
    effectId: string
    attemptId: string
    now: string
  }): EffectRecord | undefined {
    const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
    if (!task) {
      return undefined
    }
    return (this.effectsByActivation.get(input.activationId) ?? []).find((effect) =>
      effect.effectId === input.effectId &&
      effect.attemptId === input.attemptId &&
      effect.attemptOwnerId === input.workerId &&
      effect.status === "pending",
    )
  }

  private assertMutableEffect(
    input: {
      activationId: string
      workerId: string
      effectId: string
      attemptId: string
      now: string
    },
    effect: EffectRecord | undefined,
  ): asserts effect is EffectRecord {
    if (effect) {
      return
    }
    const task = this.findClaimedTask(input.activationId, input.workerId, input.now)
    if (!task) {
      throw new Error(`Lost activation lease: ${input.activationId}`)
    }
    const existing = (this.effectsByActivation.get(input.activationId) ?? [])
      .find((candidate) => candidate.effectId === input.effectId)
    if (!existing) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }
    if (existing.status === "completed" || existing.status === "failed") {
      throw new Error(`Effect is already terminal: ${input.effectId}`)
    }
    throw new Error(`Lost effect attempt: ${input.effectId}/${input.attemptId}`)
  }

  private clearTaskEffectsIfTerminal(activationId: string): void {
    const effects = this.effectsByActivation.get(activationId) ?? []
    if (effects.some((effect) => effect.status === "pending" && effect.nextAttemptAt)) {
      return
    }
    const task = this.tasksForActivation(activationId)[0]
    if (task) {
      task.blockedUntil = undefined
    }
  }

  private expireActivityTimeouts(
    now: string,
    filter: { activationId?: string; ownerId?: string; shardId?: number } = {},
  ): void {
    for (const [activationId, effects] of this.effectsByActivation) {
      if (filter.activationId && activationId !== filter.activationId) {
        continue
      }
      const task = this.tasksForActivation(activationId)[0]
      if (filter.shardId !== undefined && task?.partitionShard !== filter.shardId) {
        continue
      }
      if (
        filter.ownerId &&
        (!task ||
          task.claimOwnerId !== filter.ownerId ||
          !this.shardLeaseMatchesTask(task, filter.ownerId, now))
      ) {
        continue
      }
      let activationTimedOut = false
      for (const effect of effects) {
        if (effect.status !== "pending" || !effect.attemptId) {
          continue
        }
        const startDeadline = effect.startToCloseDeadline
        const heartbeatDeadline = effect.heartbeatDeadline
        const timeoutKind =
          startDeadline && startDeadline <= now
            ? "start_to_close" as const
            : heartbeatDeadline && heartbeatDeadline <= now
              ? "heartbeat" as const
              : undefined
        if (!timeoutKind) {
          continue
        }
        const error = {
          name: "ActivityTimeoutError",
          message: timeoutKind === "heartbeat"
            ? `Activity ${effect.key} failed due to heartbeat timeout`
            : `Activity ${effect.key} failed due to start-to-close timeout`,
        }
        const decision = retryDecisionForEffect(effect, error, now, true)
        activationTimedOut = true
        effect.timedOutAt = now
        effect.timeoutKind = timeoutKind
        effect.lastFailure = error
        effect.error = error
        effect.attemptId = undefined
        effect.attemptOwnerId = undefined
        effect.attemptStartedAt = undefined
        effect.startToCloseDeadline = undefined
        effect.heartbeatDeadline = undefined
        const task = this.tasksForActivation(activationId)[0]
        if (task) {
          task.claimOwnerId = undefined
          task.claimEpoch = undefined
          task.leaseUntil = undefined
          this.claimedSequenceEpochs.delete(sequenceKeyForTask(task))
        }
        if (decision.status === "retry_scheduled") {
          effect.status = "pending"
          effect.nextAttemptAt = decision.nextAttemptAt
          effect.attempt = decision.nextAttempt
          if (task) {
            task.blockedUntil = decision.nextAttemptAt
          }
        } else {
          effect.status = "failed"
          if (task) {
            task.blockedUntil = undefined
          }
        }
      }
      if (activationTimedOut) {
        this.releasePendingAttemptsForActivation(activationId)
      }
    }
  }

  private releasePendingAttemptsForActivation(activationId: string): void {
    for (const effect of this.effectsByActivation.get(activationId) ?? []) {
      if (effect.status !== "pending") {
        continue
      }
      effect.attemptId = undefined
      effect.attemptOwnerId = undefined
      effect.attemptStartedAt = undefined
      effect.startToCloseDeadline = undefined
      effect.heartbeatDeadline = undefined
    }
  }

  private findTaskClaimedByShardEpoch(
    activationId: string,
    workerId: string,
  ): MemoryTask | undefined {
    return this.tasksForActivation(activationId).find((task) =>
      task.activationId === activationId &&
      task.claimOwnerId === workerId &&
      task.claimEpoch !== undefined,
    ) ?? (this.replaying ? this.tasksForActivation(activationId)[0] : undefined)
  }

  private shardLeaseMatchesTask(task: MemoryTask, ownerId: string, now: string): boolean {
    if (this.replaying) {
      return true
    }
    const lease = this.shardLeases.get(task.partitionShard)
    return Boolean(
      lease &&
      lease.ownerId === ownerId &&
      lease.leaseEpoch === task.claimEpoch &&
      lease.leaseUntil >= now,
    )
  }

  private requireShardLease(shardId: number, ownerId: string, now: string): MemoryShardLease {
    if (this.replaying) {
      return {
        shardId,
        ownerId,
        leaseUntil: now,
        leaseEpoch: this.shardLeases.get(shardId)?.leaseEpoch ?? 0,
      }
    }
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

class ShardMemorySession implements ShardDurabilitySession {
  readonly shardId: number
  readonly ownerId?: string
  readonly leaseEpoch?: number

  constructor(
    private readonly provider: ShardMemoryDurabilityProvider,
    input: OpenShardInput,
  ) {
    this.shardId = input.shardId
    this.ownerId = input.ownerId
    this.leaseEpoch = input.leaseEpoch
  }

  createInstance(input: CreateInstanceInput): Promise<StartWorkflowResult> {
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

  getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult> {
    return this.provider.getWorkflowRuns(input)
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

function activationFromTask(task: MemoryTask, cloneValue: <T>(value: T) => T): ClaimedActivation {
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
    wait: cloneValue(task.wait!),
    event: cloneValue(task.event!),
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
  cloneValue: <T>(value: T) => T,
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
    result: effect.status === "completed" ? cloneValue(effect.result) : undefined,
    error: effect.status === "failed" || effect.status === "retry_scheduled"
      ? cloneValue(effect.error)
      : undefined,
    lastFailure: effect.status === "failed" || effect.status === "retry_scheduled"
      ? cloneValue(effect.error)
      : undefined,
    heartbeatDetails: cloneValue(effect.heartbeatDetails),
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

function childHandle(record: ChildRecord): ChildHandle {
  return {
    workflowName: record.workflowName,
    workflowVersion: record.workflowVersion,
    workflowId: record.workflowId,
    runId: record.runId,
    created: false,
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

function signalDelivery(wait: Extract<DurableWait, { kind: "signal" }>): "mailbox" | "future" {
  return wait.delivery ?? "mailbox"
}

function signalSequence(signalId: string): number {
  const match = /^signal-(\d+)$/.exec(signalId)
  return match ? Number(match[1]) : Number.POSITIVE_INFINITY
}

function refKey(ref: { workflowId: string; runId: string }): RefKey {
  return `${ref.workflowId}\0${ref.runId}`
}

function shouldCreateWorkflowRun(
  policy: WorkflowIdReusePolicy,
  latestStatus: PersistedInstance["status"],
): boolean {
  if (policy === "always") {
    return true
  }
  if (policy === "not_running") {
    return latestStatus !== "running"
  }
  return latestStatus === "failed" || latestStatus === "canceled"
}

function compareWorkflowRunsAsc(left: Pick<PersistedInstance, "createdAt" | "runId">, right: Pick<PersistedInstance, "createdAt" | "runId">): number {
  return sortKey(left.createdAt, left.runId).localeCompare(sortKey(right.createdAt, right.runId))
}

function compareWorkflowRunPosition(
  run: Pick<PersistedInstance, "createdAt" | "runId">,
  cursor: Pick<PersistedInstance, "createdAt" | "runId">,
): number {
  return compareWorkflowRunsAsc(run, cursor)
}

function encodeWorkflowRunsCursor(
  run: Pick<PersistedInstance, "createdAt" | "runId">,
  direction: "asc" | "desc",
): string {
  return Buffer.from(JSON.stringify({ direction, createdAt: run.createdAt, runId: run.runId }), "utf8")
    .toString("base64url")
}

function decodeWorkflowRunsCursor(
  cursor: string | undefined,
  direction: "asc" | "desc",
): Pick<PersistedInstance, "createdAt" | "runId"> | undefined {
  if (!cursor) {
    return undefined
  }
  let value: unknown
  try {
    value = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"))
  } catch {
    throw new Error("Invalid workflow runs cursor")
  }
  if (
    !value ||
    typeof value !== "object" ||
    !("direction" in value) ||
    !("createdAt" in value) ||
    !("runId" in value) ||
    value.direction !== direction ||
    typeof value.createdAt !== "string" ||
    typeof value.runId !== "string"
  ) {
    throw new Error("Invalid workflow runs cursor")
  }
  return { createdAt: value.createdAt, runId: value.runId }
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

function effectIdFor(activationId: string, key: string): string {
  return `effect-${safeToken(activationId)}-${safeToken(key)}`
}

function attemptIdFor(
  activationId: string,
  key: string,
  attempt: number,
  workerId: string,
  now: string,
): string {
  return `attempt-${safeToken(activationId)}-${safeToken(key)}-${attempt}-${safeToken(workerId)}-${safeToken(now)}`
}

function safeToken(value: string): string {
  return encodeURIComponent(value).replace(/%/g, "_")
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
  return {
    startToCloseTimeoutMs: normalizeOptionalTimeout(input.options?.startToCloseTimeoutMs),
    heartbeatTimeoutMs: normalizeOptionalTimeout(input.options?.heartbeatTimeoutMs),
    maxAttempts: retry?.maxAttempts ?? input.maxAttempts ?? 3,
    maxElapsedMs: retry?.maxElapsedMs ?? null,
    initialIntervalMs: retry?.initialIntervalMs ?? 1_000,
    maxIntervalMs: retry?.maxIntervalMs ?? 30_000,
    backoffCoefficient: retry?.backoffCoefficient ?? 2,
    nonRetryableErrorNames: retry?.nonRetryableErrorNames ?? [],
  }
}

function normalizeOptionalTimeout(value: number | null | undefined): number | null {
  if (value === undefined || value === null) {
    return null
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("Activity timeout options must be non-negative integers")
  }
  return value
}

function deadlineFrom(now: string, timeoutMs: number | null | undefined): string | undefined {
  return timeoutMs === undefined || timeoutMs === null
    ? undefined
    : addMs(now, timeoutMs)
}

function retryDecisionForEffect(
  effect: EffectRecord,
  error: SerializedError,
  now: string,
  retryable: boolean,
): FailEffectResult {
  const attempt = effect.attempt ?? 1
  const maxAttempts = effect.maxAttempts ?? 1
  const nonRetryableNames = effect.nonRetryableErrorNames ?? []
  if (!retryable || (error.name && nonRetryableNames.includes(error.name)) || attempt >= maxAttempts) {
    return { status: "failed" }
  }
  const initialIntervalMs = effect.initialIntervalMs ?? 1_000
  const maxIntervalMs = effect.maxIntervalMs ?? 30_000
  const backoffCoefficient = effect.backoffCoefficient ?? 2
  const delay = Math.min(
    maxIntervalMs,
    Math.round(initialIntervalMs * Math.max(1, backoffCoefficient ** Math.max(0, attempt - 1))),
  )
  const nextAttemptAt = addMs(now, delay)
  if (effect.maxElapsedMs !== undefined && effect.maxElapsedMs !== null && effect.firstAttemptStartedAt) {
    const deadline = addMs(effect.firstAttemptStartedAt, effect.maxElapsedMs)
    if (nextAttemptAt > deadline) {
      return { status: "failed" }
    }
  }
  return {
    status: "retry_scheduled",
    nextAttemptAt,
    nextAttempt: attempt + 1,
  }
}

function clone<T>(value: T): T {
  if (value === undefined || value === null) {
    return value
  }
  return structuredClone(value)
}
