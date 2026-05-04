import { randomUUID } from "node:crypto"
import { z } from "zod"
import type {
  ActivationInstanceSnapshot,
  ClaimedActivation,
  ClaimedActivationWithInstance,
  ClaimShardTasksResult,
  CheckpointChildStart,
  CheckpointEffectMutation,
  CommitCheckpointResult,
  CommitActivationInput,
  DurabilityProvider,
  DurableWait,
  EffectRecord,
  GetWorkflowRunsInput,
  GetWorkflowRunsResult,
  PersistedInstance,
  SignalRecord,
  ShardDurabilitySession,
  ShardLease,
} from "./interface.js"
import type { DurableLogFields, DurableMetricTags, DurableObservability } from "./observability.js"
import {
  countDurable,
  errorFields,
  gaugeDurable,
  histogramDurable,
  logDurable,
} from "./observability.js"
import { workflowPartitionShard } from "./interface.js"
import type {
  AnyWorkflow,
  ActivityContext,
  ActivityOptions,
  ChildHandle,
  ChildOptions,
  DurableContext,
  HandlerArgs,
  InputOf,
  InstanceRef,
  InstanceStatus,
  JsonObject,
  JsonValue,
  PhaseSnapshot,
  Schema,
  SignalWait,
  StartCommand,
  StartWorkflowResult,
  TransitionCommand,
  WaitDefinition,
} from "./workflow.js"
import {
  clone,
  isNonRetryableError,
  isPlainObject,
  safeId,
  serializeError,
  toJson,
  toJsonObject,
} from "./workflow.js"

export type DurableRuntimeOptions = DurableObservability & {
  workerId?: string
  shardCount?: number
  dispatchShardIds?: number[]
  maxConcurrentActivations?: number
  activationPrefetchLimit?: number
  activationCommitBatchSize?: number
  activationCommitMaxDelayMs?: number
  dispatchLeaseMs?: number
  activationLeaseMs?: number
  leaseHeartbeatIntervalMs?: number
  clock?: () => Date
  workflows?: AnyWorkflow[]
}

export type SignalOptions = {
  idempotencyKey?: string
}

export type DrainResult = {
  activations: number
  nextWakeAt?: string
}

export type RunWorkerOptions = {
  maxActivationsPerDrain?: number
  maxConcurrentActivations?: number
  activationPrefetchLimit?: number
  activationCommitBatchSize?: number
  activationCommitMaxDelayMs?: number
  minPollIntervalMs?: number
  maxPollIntervalMs?: number
  jitterRatio?: number
  signal?: AbortSignal
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>
}

export type DrainOptions = {
  maxActivations?: number
  maxConcurrentActivations?: number
  activationPrefetchLimit?: number
  activationCommitBatchSize?: number
  activationCommitMaxDelayMs?: number
  signal?: AbortSignal
}

export type RunShardStepOptions = DrainOptions & {
  shardId: number
}

export type RunShardStepResult = DrainResult & {
  shardId: number
  claimedShard: boolean
}

type ActivationTaskOutcome =
  | { kind: "handled"; activation: ClaimedActivation }
  | { kind: "retry_scheduled"; activation: ClaimedActivation }
  | { kind: "failed"; activation: ClaimedActivation; error: unknown }

type ActivationTask = {
  promise: Promise<ActivationTaskOutcome>
}

type RuntimeClaim = ClaimedActivationWithInstance & {
  session: ShardDurabilitySession
}

type ClaimBatchWithSession = {
  session: ShardDurabilitySession
  batch: ClaimShardTasksResult
}

type ActivationTaskSettled = {
  kind: "activation_task_settled"
  task: ActivationTask
  outcome: ActivationTaskOutcome
}

type DispatchHeartbeat = {
  failure: Promise<never>
  stop(): void
}

type DispatchHeartbeatFailure = {
  kind: "dispatch_heartbeat_failed"
  error: unknown
}

type QueuedActivationHeartbeat = {
  failure: Promise<never>
  stop(): void
}

type QueuedActivationHeartbeatFailure = {
  kind: "queued_activation_heartbeat_failed"
  error: unknown
}

type DrainTaskEvent = ActivationTaskSettled | DispatchHeartbeatFailure | QueuedActivationHeartbeatFailure

type ActivationEffectLedger = {
  initial: Map<string, EffectRecord>
  mutations: Map<string, CheckpointEffectMutation>
}

type ActivationChildLedger = {
  byKey: Map<string, CheckpointChildStart>
  keyByRef: Map<string, string>
  keyByWorkflowId: Map<string, string>
}

type ActivationHeartbeatControl = {
  required: boolean
}

type PendingCommit = {
  input: CommitActivationInput
  resolve(result: CommitCheckpointResult): void
  reject(error: unknown): void
}

class ActivationCommitBatcher {
  private pending: PendingCommit[] = []
  private scheduled = false
  private flushing = false

  constructor(
    private readonly session: ShardDurabilitySession,
    private readonly batchSize: number,
    private readonly maxDelayMs: number,
  ) {}

  commit(input: CommitActivationInput): Promise<CommitCheckpointResult> {
    return new Promise((resolve, reject) => {
      this.pending.push({ input, resolve, reject })
      if (this.pending.length >= this.batchSize) {
        void this.flush()
        return
      }
      this.schedule()
    })
  }

  private schedule(): void {
    if (this.scheduled) {
      return
    }
    this.scheduled = true
    const flush = () => {
      this.scheduled = false
      void this.flush()
    }
    if (this.maxDelayMs === 0) {
      queueMicrotask(flush)
      return
    }
    setTimeout(flush, this.maxDelayMs)
  }

  private async flush(): Promise<void> {
    if (this.flushing || this.pending.length === 0) {
      return
    }
    this.scheduled = false
    this.flushing = true
    const batch = this.pending.splice(0, this.batchSize)
    try {
      const output = await this.session.commitActivations(batch.map((item) => item.input))
      const byActivation = new Map(output.results.map((result) => [result.activationId, result]))
      batch.forEach((item, index) => {
        const result = byActivation.get(item.input.activationId) ?? output.results[index]
        if (!result) {
          item.reject(new Error(`Missing commit result for activation ${item.input.activationId}`))
          return
        }
        item.resolve(result)
      })
    } catch (error) {
      for (const item of batch) {
        item.reject(error)
      }
    } finally {
      this.flushing = false
      if (this.pending.length > 0) {
        this.schedule()
      }
    }
  }
}

export class DurableRuntime {
  private readonly workflows = new Map<string, AnyWorkflow>()
  private readonly clock: () => Date
  private readonly workerId: string
  private readonly shardCount: number
  private readonly dispatchShardIds: number[]
  private readonly maxConcurrentActivations: number
  private readonly activationPrefetchLimit: number
  private readonly activationCommitBatchSize: number
  private readonly activationCommitMaxDelayMs: number
  private readonly dispatchLeaseMs: number
  private readonly activationLeaseMs: number
  private readonly leaseHeartbeatIntervalMs: number
  private readonly observability: DurableObservability
  private readonly hasLogger: boolean
  private readonly hasMetrics: boolean
  private workflowVersionsCache?: Record<string, { version: number }>

  constructor(
    private readonly provider: DurabilityProvider,
    options: DurableRuntimeOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date())
    this.workerId = options.workerId ?? `worker-${randomUUID()}`
    this.shardCount = options.shardCount ?? 1
    this.dispatchShardIds = normalizeDispatchShardIds(options.dispatchShardIds, this.shardCount)
    this.maxConcurrentActivations = positiveInteger(
      options.maxConcurrentActivations ?? 4,
      "maxConcurrentActivations",
    )
    this.activationPrefetchLimit = positiveInteger(
      options.activationPrefetchLimit ?? 32,
      "activationPrefetchLimit",
    )
    this.activationCommitBatchSize = positiveInteger(
      options.activationCommitBatchSize ?? 64,
      "activationCommitBatchSize",
    )
    this.activationCommitMaxDelayMs = nonNegativeInteger(
      options.activationCommitMaxDelayMs ?? 5,
      "activationCommitMaxDelayMs",
    )
    this.dispatchLeaseMs = options.dispatchLeaseMs ?? 30_000
    this.activationLeaseMs = options.activationLeaseMs ?? 30_000
    this.leaseHeartbeatIntervalMs =
      options.leaseHeartbeatIntervalMs ??
      Math.max(1, Math.floor(Math.min(this.dispatchLeaseMs, this.activationLeaseMs) / 3))
    this.observability = { logger: options.logger, metrics: options.metrics }
    this.hasLogger = Boolean(options.logger)
    this.hasMetrics = Boolean(options.metrics)
    this.registerWorkflows(options.workflows ?? [])
  }

  registerWorkflows(workflows: AnyWorkflow[]): void {
    let changed = false
    for (const workflow of workflows) {
      const existing = this.workflows.get(workflow.name)
      if (existing?.version !== workflow.version || existing !== workflow) {
        changed = true
      }
      this.workflows.set(workflow.name, workflow)
    }
    if (changed) {
      this.workflowVersionsCache = undefined
    }
  }

  shardForRef(ref: InstanceRef | string): number {
    const normalizedRef = normalizeRef(ref)
    return workflowPartitionShard(normalizedRef.workflowId, normalizedRef.runId, this.shardCount)
  }

  async start<W extends AnyWorkflow>(
    workflow: W,
    input: InputOf<W>,
    options: { workflowId?: string; workflowIdReusePolicy?: "failed_only" | "not_running" | "always" } = {},
  ): Promise<StartWorkflowResult> {
    this.registerWorkflows([workflow])
    const parsedInput = workflow.input.parse(input)
    const startCommand = workflow.initial(parsedInput)
    const now = this.now()
    const workflowId = options.workflowId ?? `${workflow.name}-${randomUUID()}`
    const runId = randomUUID()
    const partitionShard = workflowPartitionShard(workflowId, runId, this.shardCount)
    const instance = this.initialInstance(workflow, workflowId, runId, partitionShard, startCommand, now)
    const session = this.shardSessionForShard(partitionShard)

    const ref = await session.createInstance({
      workflowName: workflow.name,
      workflowVersion: workflow.version,
      workflowId: instance.workflowId,
      runId: instance.runId,
      partitionShard,
      common: instance.common!,
      phase: instance.phase!,
      waits: instance.waits,
      now,
      workflowIdReusePolicy: options.workflowIdReusePolicy ?? "not_running",
    })
    this.log("info", "workflow.start", {
      workflowName: workflow.name,
      workflowId: ref.workflowId,
      runId: ref.runId,
      workerId: this.workerId,
    })
    this.count("durable.workflow.start", { workflowName: workflow.name, workerId: this.workerId })
    return ref
  }

  async getWorkflowRuns(input: GetWorkflowRunsInput): Promise<GetWorkflowRunsResult> {
    return this.shardSessionForWorkflowId(input.id).getWorkflowRuns(input)
  }

  async signal<W extends AnyWorkflow>(
    workflow: W,
    ref: InstanceRef | string,
    type: string,
    payload: unknown,
    options: SignalOptions = {},
  ): Promise<SignalRecord> {
    this.registerWorkflows([workflow])
    const normalizedRef = normalizeRef(ref)
    const session = this.shardSessionForRef(normalizedRef)
    const instance = await session.readInstance(normalizedRef)
    const parsedPayload = this.parseSignalPayloadForInstance(workflow, instance, type, payload)
    const signalRecord = await session.appendSignal({
      ...normalizedRef,
      type,
      payload: toJson(parsedPayload),
      receivedAt: this.now(),
      idempotencyKey: options.idempotencyKey,
    })
    this.log("info", "workflow.signal", {
      workflowName: workflow.name,
      workflowId: normalizedRef.workflowId,
      runId: normalizedRef.runId,
      signalId: signalRecord.signalId,
      type,
      workerId: this.workerId,
    })
    this.count("durable.workflow.signal", { workflowName: workflow.name, workerId: this.workerId })
    return signalRecord
  }

  async query<W extends AnyWorkflow, QueryName extends keyof NonNullable<W["queries"]> & string>(
    workflow: W,
    ref: InstanceRef | string,
    name: QueryName,
  ): Promise<z.output<NonNullable<W["queries"]>[QueryName]["schema"]>> {
    this.registerWorkflows([workflow])
    const definition = workflow.queries?.[name]
    if (!definition) {
      throw new Error(`Unknown query ${name} on workflow ${workflow.name}`)
    }

    const instance = await this.requireInstance(ref)
    const snapshot = snapshotFromInstance(instance)
    const output = definition.handler({ sequence: instance.sequence, snapshot })
    this.log("debug", "workflow.query", {
      workflowName: workflow.name,
      workflowId: instance.workflowId,
      runId: instance.runId,
      name,
      workerId: this.workerId,
    })
    this.count("durable.workflow.query", { workflowName: workflow.name, workerId: this.workerId })
    return definition.schema.parse(output)
  }

  async drain(options: DrainOptions = {}): Promise<DrainResult> {
    const shardSessions = await this.claimShardSessions()
    let dispatchHeartbeat: DispatchHeartbeat | undefined
    let dispatchFailure: Promise<DispatchHeartbeatFailure> | undefined
    try {
      if (shardSessions.length > 0) {
        dispatchHeartbeat = this.startDispatchShardHeartbeat(shardSessions)
        dispatchFailure = dispatchHeartbeat.failure.catch((error: unknown) => ({
          kind: "dispatch_heartbeat_failed",
          error,
        }))
      }
      return await this.drainOwnedShards(shardSessions, options, dispatchFailure)
    } finally {
      dispatchHeartbeat?.stop()
      await this.releaseShardSessions(shardSessions)
    }
  }

  async runShardStep(options: RunShardStepOptions): Promise<RunShardStepResult> {
    const shardId = validateShardId(options.shardId, this.shardCount)
    const { shardId: _shardId, ...drainOptions } = options
    const session = await this.claimShardSession(shardId)
    if (!session) {
      return { shardId, claimedShard: false, activations: 0 }
    }

    let dispatchHeartbeat: DispatchHeartbeat | undefined
    let dispatchFailure: Promise<DispatchHeartbeatFailure> | undefined
    try {
      dispatchHeartbeat = this.startDispatchShardHeartbeat([session])
      dispatchFailure = dispatchHeartbeat.failure.catch((error: unknown) => ({
        kind: "dispatch_heartbeat_failed",
        error,
      }))
      const result = await this.drainOwnedShards([session], drainOptions, dispatchFailure)
      return { ...result, shardId, claimedShard: true }
    } finally {
      dispatchHeartbeat?.stop()
      await this.releaseShardSessions([session])
    }
  }

  private async drainOwnedShards(
    shardSessions: ShardDurabilitySession[],
    options: DrainOptions,
    dispatchFailure?: Promise<DispatchHeartbeatFailure>,
  ): Promise<DrainResult> {
    const startedAt = Date.now()
    const maxActivations = positiveInteger(options.maxActivations ?? 100, "maxActivations")
    const maxConcurrentActivations = positiveInteger(
      options.maxConcurrentActivations ?? this.maxConcurrentActivations,
      "maxConcurrentActivations",
    )
    const activationPrefetchLimit = positiveInteger(
      options.activationPrefetchLimit ?? this.activationPrefetchLimit,
      "activationPrefetchLimit",
    )
    const activationCommitBatchSize = positiveInteger(
      options.activationCommitBatchSize ?? this.activationCommitBatchSize,
      "activationCommitBatchSize",
    )
    const activationCommitMaxDelayMs = nonNegativeInteger(
      options.activationCommitMaxDelayMs ?? this.activationCommitMaxDelayMs,
      "activationCommitMaxDelayMs",
    )
    let activations = 0
    let totalClaimed = 0
    let nextWakeAt: string | undefined
    this.log("debug", "runtime.drain.start", {
      workerId: this.workerId,
      maxActivations,
      maxConcurrentActivations,
      activationPrefetchLimit,
      activationCommitBatchSize,
      activationCommitMaxDelayMs,
    })

    const shardIds = shardSessions.map((session) => session.shardId)

    if (shardSessions.length === 0) {
      this.log("debug", "runtime.drain.no_shards", {
        workerId: this.workerId,
        maxConcurrentActivations,
        activationPrefetchLimit,
        activationCommitBatchSize,
        activationCommitMaxDelayMs,
      })
      this.count("durable.runtime.drain", { workerId: this.workerId, status: "no_shards" })
      this.histogram("durable.runtime.drain.duration_ms", Date.now() - startedAt, {
        workerId: this.workerId,
        status: "no_shards",
      })
      return { activations }
    }

    const drainController = new AbortController()
    const onExternalAbort = () => {
      if (!drainController.signal.aborted) {
        drainController.abort(abortError())
      }
    }
    options.signal?.addEventListener("abort", onExternalAbort, { once: true })
    if (options.signal?.aborted) {
      drainController.abort(abortError())
    }

    const tasks = new Set<ActivationTask>()
    const queuedClaims: RuntimeClaim[] = []
    const commitBatchers = new Map<number, ActivationCommitBatcher>()
    const commitBatcherFor = (session: ShardDurabilitySession) => {
      let batcher = commitBatchers.get(session.shardId)
      if (!batcher) {
        batcher = new ActivationCommitBatcher(
          session,
          activationCommitBatchSize,
          activationCommitMaxDelayMs,
        )
        commitBatchers.set(session.shardId, batcher)
      }
      return batcher
    }
    const queuedHeartbeat = this.startQueuedActivationHeartbeat(() =>
      queuedClaims
        .filter((claim) => requiresActivationHeartbeat(claim))
    )
    let queuedHeartbeatFailure: Promise<QueuedActivationHeartbeatFailure> | undefined =
      queuedHeartbeat.failure.catch((error: unknown) => ({
        kind: "queued_activation_heartbeat_failed",
        error,
      }))

    const releaseQueuedClaims = async () => {
      if (queuedClaims.length === 0) {
        return
      }
      const claims = queuedClaims.splice(0)
      await Promise.allSettled(
        groupRuntimeClaimsBySession(claims).map(({ session, claims }) =>
          session.releaseActivations({
            activationIds: claims.map((claim) => claim.activation.activationId),
            workerId: this.workerId,
          }),
        ),
      )
      this.gauge("durable.runtime.activation.prefetched", queuedClaims.length, {
        workerId: this.workerId,
      })
    }

    const startActivation = (claim: RuntimeClaim) => {
      const { activation } = claim
      const task: ActivationTask = {
        promise: this.runClaimedActivation(
          claim,
          commitBatcherFor(claim.session),
          drainController.signal,
        ).catch((error: unknown): ActivationTaskOutcome => ({ kind: "failed", activation, error })),
      }
      tasks.add(task)
      this.gauge("durable.runtime.activation.in_flight", tasks.size, {
        workerId: this.workerId,
      })
      this.histogram("durable.runtime.activation.concurrent_slots", tasks.size, {
        workerId: this.workerId,
      })
    }

    const startQueuedActivations = () => {
      while (!firstError && tasks.size < maxConcurrentActivations && queuedClaims.length > 0) {
        startActivation(queuedClaims.shift()!)
      }
      this.gauge("durable.runtime.activation.prefetched", queuedClaims.length, {
        workerId: this.workerId,
      })
    }

    let firstError: unknown
    let claimMissed = false

    const abortDrain = (error: unknown) => {
      firstError ??= error
      if (!drainController.signal.aborted) {
        drainController.abort(error)
      }
    }

    const waitForNextTask = async (): Promise<DrainTaskEvent> => {
      const waitables: Array<Promise<DrainTaskEvent>> = [...tasks].map((task) =>
        task.promise.then((outcome) => ({
          kind: "activation_task_settled" as const,
          task,
          outcome,
        })),
      )
      if (!firstError && dispatchFailure) {
        waitables.push(dispatchFailure)
      }
      if (!firstError && queuedHeartbeatFailure) {
        waitables.push(queuedHeartbeatFailure)
      }
      return Promise.race(waitables)
    }

    const handleOutcome = (event: DrainTaskEvent) => {
      if (event.kind === "dispatch_heartbeat_failed") {
        dispatchFailure = undefined
        this.log("warn", "runtime.dispatch_heartbeat.failure", {
          workerId: this.workerId,
          shardIds,
          ...errorFields(event.error),
        })
        this.count("durable.runtime.dispatch_heartbeat", {
          workerId: this.workerId,
          status: "failed",
        })
        abortDrain(event.error)
        return
      }
      if (event.kind === "queued_activation_heartbeat_failed") {
        queuedHeartbeatFailure = undefined
        this.log("warn", "runtime.queued_activation_heartbeat.failure", {
          workerId: this.workerId,
          queuedActivations: queuedClaims.length,
          ...errorFields(event.error),
        })
        this.count("durable.runtime.activation_heartbeat", {
          workerId: this.workerId,
          status: "failed",
        })
        abortDrain(event.error)
        return
      }

      tasks.delete(event.task)
      this.gauge("durable.runtime.activation.in_flight", tasks.size, {
        workerId: this.workerId,
      })
      const outcome = event.outcome
      if (outcome.kind === "failed") {
        abortDrain(outcome.error)
        return
      }

      activations += 1
    }

    try {
      while (totalClaimed < maxActivations || queuedClaims.length > 0 || tasks.size > 0) {
        if (drainController.signal.aborted) {
          abortDrain(drainController.signal.reason ?? abortError())
        }

        startQueuedActivations()

        while (
          !firstError &&
          !claimMissed &&
          totalClaimed < maxActivations &&
          queuedClaims.length + tasks.size < activationPrefetchLimit
        ) {
          if (drainController.signal.aborted) {
            abortDrain(drainController.signal.reason ?? abortError())
            break
          }

          const openPrefetchSlots = Math.min(
            activationPrefetchLimit - queuedClaims.length - tasks.size,
            maxActivations - totalClaimed,
          )
          const batches: ClaimBatchWithSession[] = []
          let remainingPrefetchSlots = openPrefetchSlots
          for (const session of shardSessions) {
            if (remainingPrefetchSlots <= 0) {
              break
            }
            const batch = await session.claimTasks({
              shardCount: this.shardCount,
              workflows: this.workflowVersions(),
              now: this.now(),
              leaseMs: this.activationLeaseMs,
              limit: remainingPrefetchSlots,
            })
            batches.push({ session, batch })
            remainingPrefetchSlots -= batch.claims.length
          }
          const claimedCount = batches.reduce((total, item) => total + item.batch.claims.length, 0)

          if (claimedCount === 0) {
            nextWakeAt = earliestIso(
              ...batches.map((item) => item.batch.nextWakeAt),
            )
            claimMissed = true
            this.log("debug", "runtime.activation.claim_miss", () => ({
              workerId: this.workerId,
              nextWakeAt,
              inFlight: tasks.size,
              prefetched: queuedClaims.length,
              maxConcurrentActivations,
              activationPrefetchLimit,
            }))
            this.count("durable.runtime.activation.claim", () => ({
              workerId: this.workerId,
              status: "miss",
            }))
            break
          }

          for (const { session, batch } of batches) {
            for (const claim of batch.claims) {
              totalClaimed += 1
              this.log("debug", "runtime.activation.claimed", () => ({
                workerId: this.workerId,
                workflowName: claim.activation.workflowName,
                workflowId: claim.activation.workflowId,
                runId: claim.activation.runId,
                activationId: claim.activation.activationId,
                activationKind: claim.activation.kind,
                eventKind: activationEventKind(claim.activation),
                sequence: claim.activation.sequence,
                shardId: session.shardId,
                activeSlots: tasks.size,
                prefetched: queuedClaims.length + 1,
                maxConcurrentActivations,
                activationPrefetchLimit,
                activationCommitMaxDelayMs,
              }))
              this.count(
                "durable.runtime.activation.claim",
                () => this.activationTags(claim.activation, "claimed"),
              )
              queuedClaims.push({ ...claim, session })
            }
          }
          this.gauge("durable.runtime.activation.prefetched", queuedClaims.length, {
            workerId: this.workerId,
          })
          startQueuedActivations()

          if (claimedCount < openPrefetchSlots) {
            nextWakeAt = earliestIso(
              ...batches.map((item) => item.batch.nextWakeAt),
            )
            claimMissed = true
            break
          }
        }

        if (queuedClaims.length === 0 && tasks.size === 0) {
          break
        }

        if (
          firstError ||
          claimMissed ||
          totalClaimed >= maxActivations ||
          tasks.size >= maxConcurrentActivations ||
          queuedClaims.length + tasks.size >= activationPrefetchLimit
        ) {
          if (tasks.size === 0) {
            break
          }
          handleOutcome(await waitForNextTask())
          claimMissed = false
          if (totalClaimed >= maxActivations) {
            nextWakeAt = undefined
          }
        }
      }

      if (firstError) {
        throw firstError
      }
    } catch (error) {
      abortDrain(error)
      await releaseQueuedClaims()
      while (tasks.size > 0) {
        handleOutcome(await waitForNextTask())
      }
      throw firstError
    } finally {
      queuedHeartbeat.stop()
      options.signal?.removeEventListener("abort", onExternalAbort)
      await releaseQueuedClaims()
      const resultTags = { workerId: this.workerId, status: firstError ? "failed" : "complete" }
      this.log("debug", "runtime.drain.end", {
        workerId: this.workerId,
        activations,
        totalClaimed,
        prefetched: queuedClaims.length,
        nextWakeAt,
        maxConcurrentActivations,
        activationPrefetchLimit,
        activationCommitMaxDelayMs,
        durationMs: Date.now() - startedAt,
      })
      this.count("durable.runtime.drain", resultTags)
      this.histogram("durable.runtime.drain.activations", activations, resultTags)
      this.histogram("durable.runtime.drain.duration_ms", Date.now() - startedAt, resultTags)
    }

    return nextWakeAt ? { activations, nextWakeAt } : { activations }
  }

  async runWorker(options: RunWorkerOptions = {}): Promise<{ activations: number }> {
    const maxActivationsPerDrain =
      options.maxActivationsPerDrain === undefined
        ? undefined
        : positiveInteger(options.maxActivationsPerDrain, "maxActivationsPerDrain")
    const maxConcurrentActivations =
      options.maxConcurrentActivations === undefined
        ? undefined
        : positiveInteger(options.maxConcurrentActivations, "maxConcurrentActivations")
    const activationPrefetchLimit =
      options.activationPrefetchLimit === undefined
        ? undefined
        : positiveInteger(options.activationPrefetchLimit, "activationPrefetchLimit")
    const activationCommitBatchSize =
      options.activationCommitBatchSize === undefined
        ? undefined
        : positiveInteger(options.activationCommitBatchSize, "activationCommitBatchSize")
    const activationCommitMaxDelayMs =
      options.activationCommitMaxDelayMs === undefined
        ? undefined
        : nonNegativeInteger(options.activationCommitMaxDelayMs, "activationCommitMaxDelayMs")
    const minPollIntervalMs = options.minPollIntervalMs ?? 10
    const maxPollIntervalMs = options.maxPollIntervalMs ?? 1_000
    const jitterRatio = options.jitterRatio ?? 0.1
    const sleep = options.sleep ?? sleepMs
    let activations = 0
    let shardSessions: ShardDurabilitySession[] = []
    let dispatchHeartbeat: DispatchHeartbeat | undefined
    let dispatchFailure: Promise<DispatchHeartbeatFailure> | undefined
    this.log("info", "runtime.worker.start", {
      workerId: this.workerId,
      maxConcurrentActivations: maxConcurrentActivations ?? this.maxConcurrentActivations,
      activationPrefetchLimit: activationPrefetchLimit ?? this.activationPrefetchLimit,
      activationCommitBatchSize: activationCommitBatchSize ?? this.activationCommitBatchSize,
      activationCommitMaxDelayMs: activationCommitMaxDelayMs ?? this.activationCommitMaxDelayMs,
    })
    this.count("durable.runtime.worker", { workerId: this.workerId, status: "start" })

    const releaseWorkerShards = async () => {
      dispatchHeartbeat?.stop()
      dispatchHeartbeat = undefined
      dispatchFailure = undefined
      if (shardSessions.length > 0) {
        await this.releaseShardSessions(shardSessions)
        shardSessions = []
      }
    }

    try {
      while (!options.signal?.aborted) {
        let result: DrainResult
        try {
          if (shardSessions.length === 0) {
            shardSessions = await this.claimShardSessions()
            if (shardSessions.length > 0) {
              dispatchHeartbeat = this.startDispatchShardHeartbeat(shardSessions)
              dispatchFailure = dispatchHeartbeat.failure.catch((error: unknown) => ({
                kind: "dispatch_heartbeat_failed",
                error,
              }))
            }
          }

          result = await this.drainOwnedShards(shardSessions, {
            maxActivations: maxActivationsPerDrain,
            maxConcurrentActivations,
            activationPrefetchLimit,
            activationCommitBatchSize,
            activationCommitMaxDelayMs,
            signal: options.signal,
          }, dispatchFailure)
        } catch (error) {
          if (isAbortError(error) || options.signal?.aborted) {
            break
          }
          this.log("error", "runtime.worker.error", {
            workerId: this.workerId,
            ...errorFields(error),
          })
          this.count("durable.runtime.worker", { workerId: this.workerId, status: "error" })
          throw error
        }
        activations += result.activations

        if (options.signal?.aborted) {
          break
        }

        const delayMs = pollDelayMs({
          now: this.now(),
          nextWakeAt: result.nextWakeAt,
          activations: result.activations,
          minPollIntervalMs,
          maxPollIntervalMs,
          jitterRatio,
        })
        this.log("debug", "runtime.worker.sleep", {
          workerId: this.workerId,
          delayMs,
          nextWakeAt: result.nextWakeAt,
          activations: result.activations,
        })
        this.histogram("durable.runtime.worker.sleep_ms", delayMs, { workerId: this.workerId })

        try {
          const sleeper = sleep(delayMs, options.signal)
          const wake = await (dispatchFailure
            ? Promise.race([sleeper.then(() => undefined), dispatchFailure])
            : sleeper.then(() => undefined))
          if (wake?.kind === "dispatch_heartbeat_failed") {
            throw wake.error
          }
        } catch (error) {
          if (isAbortError(error) || options.signal?.aborted) {
            break
          }
          this.log("error", "runtime.worker.error", {
            workerId: this.workerId,
            ...errorFields(error),
          })
          this.count("durable.runtime.worker", { workerId: this.workerId, status: "error" })
          throw error
        }
      }
    } finally {
      await releaseWorkerShards()
      this.log("info", "runtime.worker.stop", { workerId: this.workerId, activations })
      this.count("durable.runtime.worker", { workerId: this.workerId, status: "stop" })
    }

    return { activations }
  }

  private async claimShardSessions(): Promise<ShardDurabilitySession[]> {
    const sessions: ShardDurabilitySession[] = []
    for (const shardId of this.dispatchShardIds) {
      const session = await this.claimShardSession(shardId)
      if (session) {
        sessions.push(session)
      }
    }
    const shardIds = sessions.map((session) => session.shardId)
    this.log("debug", "runtime.dispatch_shards.claimed", {
      workerId: this.workerId,
      shardIds,
      shardCount: this.shardCount,
      configuredShardIds: this.dispatchShardIds,
    })
    this.gauge("durable.runtime.dispatch_shards.owned", shardIds.length, {
      workerId: this.workerId,
    })
    return sessions
  }

  private async claimShardSession(shardId: number): Promise<ShardDurabilitySession | null> {
    const lease = await this.provider.claimShard({
      shardId,
      ownerId: this.workerId,
      now: this.now(),
      leaseMs: this.dispatchLeaseMs,
    })
    return lease ? this.provider.openShard(lease) : null
  }

  private async heartbeatShardSessions(sessions: ShardDurabilitySession[]): Promise<void> {
    await Promise.all(
      sessions.map((session) =>
        session.heartbeat({
          now: this.now(),
          leaseMs: this.dispatchLeaseMs,
        }),
      ),
    )
  }

  private async releaseShardSessions(sessions: ShardDurabilitySession[]): Promise<void> {
    await Promise.allSettled(sessions.map((session) => session.release()))
  }

  private workflowVersions(): Record<string, { version: number }> {
    this.workflowVersionsCache ??= Object.fromEntries(
      [...this.workflows.values()].map((workflow) => [
        workflow.name,
        { version: workflow.version },
      ]),
    )
    return this.workflowVersionsCache
  }

  private async runActivation(
    activation: ClaimedActivation,
    latest: ActivationInstanceSnapshot,
    effects: EffectRecord[],
    session: ShardDurabilitySession,
    commitBatcher: ActivationCommitBatcher,
    signal: AbortSignal,
    heartbeatControl: ActivationHeartbeatControl,
  ): Promise<void> {
    this.log("debug", "runtime.activation.started", () => ({
      workerId: this.workerId,
      workflowName: activation.workflowName,
      workflowId: activation.workflowId,
      runId: activation.runId,
      activationId: activation.activationId,
      activationKind: activation.kind,
      eventKind: activationEventKind(activation),
      sequence: activation.sequence,
    }))
    if (latest.status !== "running" || latest.sequence !== activation.sequence) {
      await session.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      this.log("debug", "runtime.activation.released", () => ({
        workerId: this.workerId,
        workflowName: activation.workflowName,
        workflowId: activation.workflowId,
        runId: activation.runId,
        activationId: activation.activationId,
        reason: "stale_instance",
      }))
      return
    }

    const workflow = this.workflows.get(latest.workflowName)
    if (!workflow) {
      await session.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      this.log("warn", "runtime.activation.released", () => ({
        workerId: this.workerId,
        workflowName: latest.workflowName,
        workflowId: latest.workflowId,
        runId: latest.runId,
        activationId: activation.activationId,
        reason: "unknown_workflow",
      }))
      return
    }

    if (latest.workflowVersion > workflow.version) {
      await session.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      throw new Error(
        `Workflow ${workflow.name} instance ${latest.workflowId}/${latest.runId} is at newer version ${latest.workflowVersion}; worker only has version ${workflow.version}`,
      )
    }

    if (activation.kind === "migration") {
      const effectLedger = this.activationEffectLedger(effects)
      const childLedger = this.activationChildLedger()
      const next = await this.migrateSnapshot(workflow, latest)
      const commitTime = this.now()
      const waits = this.materializeWaits(
        workflow,
        { workflowId: latest.workflowId, runId: latest.runId, updatedAt: commitTime },
        next,
      )
      await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits, commitTime, session, {
        effects: [...effectLedger.mutations.values()],
        childStarts: [...childLedger.byKey.values()],
      }, commitBatcher)
      return
    }

    const common = trustedJsonCopy(latest.common ?? {})
    const phaseSnapshot = latest.phase
    if (!phaseSnapshot) {
      throw new Error(`Running workflow ${latest.workflowId} has no phase`)
    }

    const phaseDefinition = workflow.phases[phaseSnapshot.name]
    if (!phaseDefinition) {
      throw new Error(`Unknown phase ${phaseSnapshot.name} on workflow ${workflow.name}`)
    }

    const data = trustedJsonCopy(phaseSnapshot.data)
    const effectLedger = this.activationEffectLedger(effects)
    const childLedger = this.activationChildLedger()
    const ctx = this.contextFor(
      workflow,
      latest,
      activation.activationId,
      activation.activationTime,
      signal,
      effectLedger,
      childLedger,
      heartbeatControl,
      session,
    )
    let transition: TransitionCommand
    let consumeSignalId: string | undefined
    let consumeChildRecordId: string | undefined

    if (activation.kind === "run") {
      if (!phaseDefinition.run) {
        throw new Error(`Phase ${phaseSnapshot.name} is not runnable`)
      }
      transition = await phaseDefinition.run({ ctx, common, data })
    } else {
      const event = activation.event
      const waitDefinition = this.waitDefinition(workflow, latest, activation.wait)
      if (event.kind === "signal") {
        if (waitDefinition.kind !== "signal") {
          throw new Error("Signal delivered to non-signal wait")
        }
        consumeSignalId = event.consumeSignalId
        transition = await waitDefinition.handler({
          ctx,
          common,
          data,
          event: trustedJsonCopy(event.payload),
        })
      } else if (event.kind === "timer") {
        transition = await callWaitHandler(waitDefinition, {
          ctx,
          common,
          data,
          event: { firedAt: event.firedAt },
        })
      } else {
        consumeChildRecordId = event.childRecordId
        transition = await callWaitHandler(waitDefinition, {
          ctx,
          common,
          data,
          event: trustedJsonCopy(event.event),
        })
      }
    }

    const next = this.applyTransition(workflow, latest, transition)
    const commitTime = this.now()
    const waits =
      next.status === "running"
        ? this.materializeWaits(
            workflow,
            { workflowId: latest.workflowId, runId: latest.runId, updatedAt: commitTime },
            next,
          )
        : []
    await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits, commitTime, session, {
      consumeSignalId,
      consumeChildRecordId,
      effects: [...effectLedger.mutations.values()],
      childStarts: [...childLedger.byKey.values()],
    }, commitBatcher)
  }

  private async commitOrDiscard(
    latest: ActivationInstanceSnapshot,
    workflow: AnyWorkflow,
    activationId: string,
    next: InstanceStatus<any>,
    waits: DurableWait[],
    now: string,
    session: ShardDurabilitySession,
    options: {
      consumeSignalId?: string
      consumeChildRecordId?: string
      effects?: CheckpointEffectMutation[]
      childStarts?: CheckpointChildStart[]
    } = {},
    commitBatcher?: ActivationCommitBatcher,
  ): Promise<CommitCheckpointResult> {
    const input: CommitActivationInput = {
      workflowId: latest.workflowId,
      runId: latest.runId,
      expectedSequence: latest.sequence,
      activationId,
      workerId: this.workerId,
      workflowVersion: workflow.version,
      next,
      waits,
      now,
      consumeSignalId: options.consumeSignalId,
      consumeChildRecordId: options.consumeChildRecordId,
      effects: compactCheckpointEffectMutations(options.effects),
      childStarts: options.childStarts,
    }
    const result =
      commitBatcher
        ? await commitBatcher.commit(input)
        : (await session.commitActivations([input])).results[0] ??
          { ok: false, sequence: latest.sequence, reason: "missing_commit_result" }

    if (!result.ok) {
      this.log("warn", "runtime.activation.commit_conflict", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        workflowId: latest.workflowId,
        runId: latest.runId,
        activationId,
        expectedSequence: latest.sequence,
        actualSequence: result.sequence,
      }))
      this.count("durable.runtime.activation", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        status: "commit_conflict",
      }))
      await session.releaseActivation({
        activationId,
        workerId: this.workerId,
      })
      if (result.retryable === false) {
        throw errorFromSerialized(
          result.error ?? {
            name: "CommitFailed",
            message: result.reason
              ? `Activation commit failed: ${result.reason}`
              : "Activation commit failed",
          },
        )
      }
      return result
    }

    this.log("info", "runtime.activation.completed", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      workflowId: latest.workflowId,
      runId: latest.runId,
      activationId,
      sequence: result.sequence,
      nextStatus: next.status,
    }))
    this.count("durable.runtime.activation", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      status: "completed",
    }))
    return result
  }

  private activationEffectLedger(effects: EffectRecord[]): ActivationEffectLedger {
    return {
      initial: new Map(effects.map((effect) => [effect.key, effect])),
      mutations: new Map(),
    }
  }

  private activationChildLedger(): ActivationChildLedger {
    return {
      byKey: new Map(),
      keyByRef: new Map(),
      keyByWorkflowId: new Map(),
    }
  }

  private async runCheckpointActivity<T>(
    workflow: AnyWorkflow,
    instance: ActivationInstanceSnapshot,
    activationId: string,
    activationSignal: AbortSignal,
    session: ShardDurabilitySession,
    ledger: ActivationEffectLedger,
    key: string,
    fn: (ctx: ActivityContext) => Promise<T> | T,
    options?: ActivityOptions,
  ): Promise<T> {
    const existing = ledger.mutations.get(key) ?? ledger.initial.get(key)
    if (existing?.status === "completed") {
      this.log("debug", "runtime.activity.memoized", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        workflowId: instance.workflowId,
        runId: instance.runId,
        activationId,
        key,
        durability: "checkpoint",
      }))
      this.count("durable.runtime.activity", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        status: "memoized",
      }))
      return clone(existing.result) as T
    }

    if (existing?.status === "failed") {
      const storedError = existing.error ?? { message: "Activity failed" }
      const error = errorFromSerialized(storedError)
      this.log("warn", "runtime.activity.failed", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        workflowId: instance.workflowId,
        runId: instance.runId,
        activationId,
        key,
        error: storedError,
        durability: "checkpoint",
      }))
      this.count("durable.runtime.activity", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        status: "failed",
      }))
      throw error
    }

    if (existing?.status === "retry_scheduled" && existing.nextAttemptAt > this.now()) {
      throw new ActivityRetryScheduledError(existing.nextAttemptAt)
    }
    if (existing?.status === "pending" && existing.nextAttemptAt && existing.nextAttemptAt > this.now()) {
      throw new ActivityRetryScheduledError(existing.nextAttemptAt)
    }

    const normalized = normalizeLocalActivityOptions(options)
    const firstAttemptStartedAt = effectFirstAttemptStartedAt(existing) ?? this.now()
    const attempt =
      existing?.status === "retry_scheduled"
        ? existing.nextAttempt
        : existing?.attempt ?? 1
    const idempotencyKey =
      existing?.idempotencyKey ?? `${instance.workflowId}/${instance.runId}/${activationId}/${key}`
    let heartbeatDetails =
      existing && "heartbeatDetails" in existing ? clone(existing.heartbeatDetails) : undefined

    this.log("debug", "runtime.activity.reserved", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      workflowId: instance.workflowId,
      runId: instance.runId,
      activationId,
      key,
      attempt,
      durability: "checkpoint",
    }))
    this.count("durable.runtime.activity", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      status: "reserved",
    }))

    const activityContext: ActivityContext = {
      heartbeat: async (details?: JsonValue): Promise<void> => {
        if (activationSignal.aborted) {
          throw abortError()
        }
        heartbeatDetails = details === undefined ? undefined : toJson(details)
        this.log("debug", "runtime.activity.heartbeat", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId,
          key,
          durability: "checkpoint",
        }))
        this.count("durable.runtime.activity", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          status: "heartbeat",
        }))
      },
      heartbeatDetails,
      idempotencyKey,
      attempt,
      signal: activationSignal,
    }

    let result: T
    try {
      if (activationSignal.aborted) {
        throw abortError()
      }
      result = await fn(activityContext)
    } catch (error) {
      if (activationSignal.aborted) {
        throw error
      }
      const serialized = serializeError(error)
      const retry = localRetryDecision(
        {
          attempt,
          firstAttemptStartedAt,
          maxAttempts: normalized.maxAttempts,
          maxElapsedMs: normalized.maxElapsedMs,
          initialIntervalMs: normalized.initialIntervalMs,
          maxIntervalMs: normalized.maxIntervalMs,
          backoffCoefficient: normalized.backoffCoefficient,
          nonRetryableErrorNames: normalized.nonRetryableErrorNames,
        },
        serialized,
        this.now(),
        !isNonRetryableActivityError(error),
      )
      const mutation: CheckpointEffectMutation =
        retry.status === "retry_scheduled"
          ? {
              key,
              status: "retry_scheduled",
              error: serialized,
              nextAttemptAt: retry.nextAttemptAt,
              nextAttempt: retry.nextAttempt,
              heartbeatDetails,
              attempt,
              idempotencyKey,
              firstAttemptStartedAt,
              maxAttempts: normalized.maxAttempts,
              maxElapsedMs: normalized.maxElapsedMs,
              initialIntervalMs: normalized.initialIntervalMs,
              maxIntervalMs: normalized.maxIntervalMs,
              backoffCoefficient: normalized.backoffCoefficient,
              nonRetryableErrorNames: normalized.nonRetryableErrorNames,
            }
          : {
              key,
              status: "failed",
              error: serialized,
              retryable: false,
              heartbeatDetails,
              attempt,
              idempotencyKey,
              firstAttemptStartedAt,
              maxAttempts: normalized.maxAttempts,
              maxElapsedMs: normalized.maxElapsedMs,
              initialIntervalMs: normalized.initialIntervalMs,
              maxIntervalMs: normalized.maxIntervalMs,
              backoffCoefficient: normalized.backoffCoefficient,
              nonRetryableErrorNames: normalized.nonRetryableErrorNames,
            }
      ledger.mutations.set(key, mutation)
      await session.recordActivationFailures([
        {
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId,
          workerId: this.workerId,
          now: this.now(),
          effects: [mutation],
          releaseActivation: true,
        },
      ])
      if (retry.status === "retry_scheduled") {
        this.log("info", "runtime.activity.retry_scheduled", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId,
          key,
          nextAttemptAt: retry.nextAttemptAt,
          durability: "checkpoint",
        }))
        this.count("durable.runtime.activity", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          status: "retry_scheduled",
        }))
        throw new ActivityRetryScheduledError(retry.nextAttemptAt)
      }

      this.log("error", "runtime.activity.failed", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        workflowId: instance.workflowId,
        runId: instance.runId,
        activationId,
        key,
        durability: "checkpoint",
        ...errorFields(error),
      }))
      this.count("durable.runtime.activity", () => ({
        workerId: this.workerId,
        workflowName: workflow.name,
        status: "failed",
      }))
      throw error
    }

    ledger.mutations.set(key, {
      key,
      status: "completed",
      result: toJson(result),
      heartbeatDetails,
      attempt,
      idempotencyKey,
      firstAttemptStartedAt,
      maxAttempts: normalized.maxAttempts,
      maxElapsedMs: normalized.maxElapsedMs,
      initialIntervalMs: normalized.initialIntervalMs,
      maxIntervalMs: normalized.maxIntervalMs,
      backoffCoefficient: normalized.backoffCoefficient,
      nonRetryableErrorNames: normalized.nonRetryableErrorNames,
    })
    this.log("debug", "runtime.activity.completed", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      workflowId: instance.workflowId,
      runId: instance.runId,
      activationId,
      key,
      durability: "checkpoint",
    }))
    this.count("durable.runtime.activity", () => ({
      workerId: this.workerId,
      workflowName: workflow.name,
      status: "completed",
    }))
    return result
  }

  private contextFor(
    workflow: AnyWorkflow,
    instance: ActivationInstanceSnapshot,
    currentActivationId: string,
    activationTime: string,
    activationSignal: AbortSignal,
    effects: ActivationEffectLedger,
    childLedger: ActivationChildLedger,
    heartbeatControl: ActivationHeartbeatControl,
    session: ShardDurabilitySession,
  ): DurableContext {
    return {
      now: () => activationTime,
      activity: async <T>(
        key: string,
        fn: (ctx: ActivityContext) => Promise<T> | T,
        options?: ActivityOptions,
      ): Promise<T> => {
        if (activityDurability(options) === "checkpoint") {
          return this.runCheckpointActivity(
            workflow,
            instance,
            currentActivationId,
            activationSignal,
            session,
            effects,
            key,
            fn,
            options,
          )
        }

        heartbeatControl.required = true
        const reservation = await session.getOrReserveEffect({
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          workerId: this.workerId,
          key,
          now: this.now(),
          options,
        })

        if (reservation.status === "completed") {
          this.log("debug", "runtime.activity.memoized", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            key,
          }))
          this.count("durable.runtime.activity", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            status: "memoized",
          }))
          return clone(reservation.result) as T
        }

        if (reservation.status === "failed") {
          this.log("warn", "runtime.activity.failed", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            key,
            error: reservation.error,
          }))
          this.count("durable.runtime.activity", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            status: "failed",
          }))
          throw new Error(reservation.error.message)
        }

        this.log("debug", "runtime.activity.reserved", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          key,
          attempt: reservation.attempt,
        }))
        this.count("durable.runtime.activity", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          status: "reserved",
        }))

        const activityContext = {
          heartbeat: async (details?: JsonValue): Promise<void> => {
            if (activationSignal.aborted) {
              throw abortError()
            }
            await session.heartbeatEffect({
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              workerId: this.workerId,
              effectId: reservation.effectId,
              attemptId: reservation.attemptId,
              now: this.now(),
              details: details === undefined ? undefined : toJson(details),
            })
            this.log("debug", "runtime.activity.heartbeat", () => ({
              workerId: this.workerId,
              workflowName: workflow.name,
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              effectId: reservation.effectId,
              attemptId: reservation.attemptId,
              key,
            }))
            this.count("durable.runtime.activity", () => ({
              workerId: this.workerId,
              workflowName: workflow.name,
              status: "heartbeat",
            }))
          },
          heartbeatDetails: clone(reservation.heartbeatDetails),
          idempotencyKey: reservation.idempotencyKey,
          attempt: reservation.attempt,
          signal: activationSignal,
        }

        let result: T
        try {
          if (activationSignal.aborted) {
            throw abortError()
          }
          result = await fn(activityContext)
        } catch (error) {
          if (activationSignal.aborted) {
            throw error
          }
          const failure = await session.failEffect({
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            workerId: this.workerId,
            effectId: reservation.effectId,
            attemptId: reservation.attemptId,
            error: serializeError(error),
            now: this.now(),
            retryable: !isNonRetryableActivityError(error),
          })
          if (failure.status === "retry_scheduled") {
            this.log("info", "runtime.activity.retry_scheduled", () => ({
              workerId: this.workerId,
              workflowName: workflow.name,
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              effectId: reservation.effectId,
              attemptId: reservation.attemptId,
              key,
              nextAttemptAt: failure.nextAttemptAt,
            }))
            this.count("durable.runtime.activity", () => ({
              workerId: this.workerId,
              workflowName: workflow.name,
              status: "retry_scheduled",
            }))
            throw new ActivityRetryScheduledError(failure.nextAttemptAt)
          }
          this.log("error", "runtime.activity.failed", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            effectId: reservation.effectId,
            attemptId: reservation.attemptId,
            key,
            ...errorFields(error),
          }))
          this.count("durable.runtime.activity", () => ({
            workerId: this.workerId,
            workflowName: workflow.name,
            status: "failed",
          }))
          throw error
        }

        await session.completeEffect({
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          workerId: this.workerId,
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          result: toJson(result),
          now: this.now(),
        })
        this.log("debug", "runtime.activity.completed", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          key,
        }))
        this.count("durable.runtime.activity", () => ({
          workerId: this.workerId,
          workflowName: workflow.name,
          status: "completed",
        }))
        return result
      },
      child: {
        start: async <W extends AnyWorkflow>(
          key: string,
          childWorkflow: W,
          input: InputOf<W>,
          options: ChildOptions = {},
        ): Promise<ChildHandle<W>> => {
          this.registerWorkflows([childWorkflow])
          const parsedInput = childWorkflow.input.parse(input)
          const startCommand = childWorkflow.initial(parsedInput)
          const now = activationTime
          const childWorkflowId =
            options.workflowId ??
            defaultChildWorkflowId(
              instance.workflowId,
              instance.runId,
              instance.sequence,
              key,
              this.shardCount,
              instance.partitionShard,
            )
          const workflowIdReusePolicy = options.workflowIdReusePolicy ?? "not_running"
          const pendingWorkflowKey = workflowIdReusePolicy === "always"
            ? undefined
            : childLedger.keyByWorkflowId.get(childWorkflowId)
          if (pendingWorkflowKey) {
            const pending = childLedger.byKey.get(pendingWorkflowKey)
            if (pending) {
              return childHandleFromStart<W>(pending)
            }
          }
          const latestChildRun = workflowIdReusePolicy === "always"
            ? undefined
            : (await session.getWorkflowRuns({
                id: childWorkflowId,
                direction: "desc",
                limit: 1,
              })).runs[0]
          const reuseLatestChildRun = latestChildRun &&
            !shouldCreateWorkflowRunForStatus(workflowIdReusePolicy, latestChildRun.status)
          const childRunId = reuseLatestChildRun ? latestChildRun.runId : randomUUID()
          const childInstance = this.initialInstance(
            childWorkflow,
            childWorkflowId,
            childRunId,
            workflowPartitionShard(childWorkflowId, childRunId, this.shardCount),
            startCommand,
            now,
          )
          const handle: ChildHandle<W> = {
            workflowName: childWorkflow.name,
            workflowVersion: childWorkflow.version,
            workflowId: childInstance.workflowId,
            runId: childInstance.runId,
            created: !reuseLatestChildRun,
          } as ChildHandle<W>
          const parentClosePolicy = options.parentClosePolicy ?? "cancel"

          if (childDurability(options) === "checkpoint") {
            const existingForKey = childLedger.byKey.get(key)
            if (existingForKey) {
              return childHandleFromStart<W>(existingForKey)
            }

            const existingRefKey = childRefKey(childInstance.workflowId, childInstance.runId)
            const existingRefKeyOwner = childLedger.keyByRef.get(existingRefKey)
            if (existingRefKeyOwner && existingRefKeyOwner !== key) {
              throw new Error(
                `Child workflow instance already exists in this activation: ${childInstance.workflowId}/${childInstance.runId}`,
              )
            }

            childLedger.byKey.set(key, {
              key,
              workflowName: childWorkflow.name,
              workflowVersion: childWorkflow.version,
              workflowId: childInstance.workflowId,
              runId: childInstance.runId,
              partitionShard: workflowPartitionShard(
                childInstance.workflowId,
                childInstance.runId,
                this.shardCount,
              ),
              common: childInstance.common!,
              phase: childInstance.phase!,
              waits: childInstance.waits,
              parentClosePolicy,
              conflictPolicy: "fail",
              workflowIdReusePolicy,
            })
            childLedger.keyByRef.set(existingRefKey, key)
            childLedger.keyByWorkflowId.set(childInstance.workflowId, key)

            this.log("info", "runtime.child.start", {
              workerId: this.workerId,
              workflowName: workflow.name,
              childWorkflowName: childWorkflow.name,
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              childWorkflowId: handle.workflowId,
              childRunId: handle.runId,
              key,
              durability: "checkpoint",
            })
            this.count("durable.runtime.child", {
              workerId: this.workerId,
              workflowName: workflow.name,
              status: "started",
            })

            return handle
          }

          const eagerHandle = await session.createChildInstance({
            workflowName: childWorkflow.name,
            workflowVersion: childWorkflow.version,
            workflowId: childInstance.workflowId,
            runId: childInstance.runId,
            partitionShard: workflowPartitionShard(
              childInstance.workflowId,
              childInstance.runId,
              this.shardCount,
            ),
            common: childInstance.common!,
            phase: childInstance.phase!,
            waits: childInstance.waits,
            now,
            parentWorkflowId: instance.workflowId,
            parentRunId: instance.runId,
            activationId: currentActivationId,
            workerId: this.workerId,
            leaseNow: this.now(),
            key,
            parentClosePolicy,
            conflictPolicy: "fail",
            workflowIdReusePolicy,
          })

          this.log("info", "runtime.child.start", {
            workerId: this.workerId,
            workflowName: workflow.name,
            childWorkflowName: childWorkflow.name,
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            childWorkflowId: eagerHandle.workflowId,
            childRunId: eagerHandle.runId,
            key,
            durability: "eager",
          })
          this.count("durable.runtime.child", {
            workerId: this.workerId,
            workflowName: workflow.name,
            status: "started",
          })

          return eagerHandle as ChildHandle<W>
        },
        cancel: async (handle: ChildHandle<any>): Promise<void> => {
          const bufferedKey = childLedger.keyByRef.get(childRefKey(handle.workflowId, handle.runId))
          if (bufferedKey) {
            childLedger.keyByRef.delete(childRefKey(handle.workflowId, handle.runId))
            const buffered = childLedger.byKey.get(bufferedKey)
            if (buffered && childLedger.keyByWorkflowId.get(buffered.workflowId) === bufferedKey) {
              childLedger.keyByWorkflowId.delete(buffered.workflowId)
            }
            childLedger.byKey.delete(bufferedKey)
            this.log("info", "runtime.child.cancel", {
              workerId: this.workerId,
              workflowName: workflow.name,
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              childWorkflowId: handle.workflowId,
              childRunId: handle.runId,
              durability: "checkpoint",
            })
            this.count("durable.runtime.child", {
              workerId: this.workerId,
              workflowName: workflow.name,
              status: "canceled",
            })
            return
          }
          await session.cancelChild({
            parentWorkflowId: instance.workflowId,
            parentRunId: instance.runId,
            activationId: currentActivationId,
            workerId: this.workerId,
            workflowId: handle.workflowId,
            runId: handle.runId,
            now: this.now(),
          })
          this.log("info", "runtime.child.cancel", {
            workerId: this.workerId,
            workflowName: workflow.name,
            workflowId: instance.workflowId,
            runId: instance.runId,
            activationId: currentActivationId,
            childWorkflowId: handle.workflowId,
            childRunId: handle.runId,
          })
          this.count("durable.runtime.child", {
            workerId: this.workerId,
            workflowName: workflow.name,
            status: "canceled",
          })
        },
      },
    }
  }

  private async runClaimedActivation(
    claim: RuntimeClaim,
    commitBatcher: ActivationCommitBatcher,
    signal: AbortSignal,
  ): Promise<ActivationTaskOutcome> {
    const { activation, instance, effects } = claim
    const heartbeatControl: ActivationHeartbeatControl = {
      required: requiresActivationHeartbeat(claim),
    }
    try {
      await this.withActivationHeartbeat(
        claim.session,
        activation,
        signal,
        () => heartbeatControl.required,
        (activationSignal) =>
          this.runActivation(
            activation,
            instance,
            effects,
            claim.session,
            commitBatcher,
            activationSignal,
            heartbeatControl,
          ),
      )
      return { kind: "handled", activation }
    } catch (error) {
      await this.releaseActivationQuietly(claim.session, activation.activationId)
      if (isActivityRetryScheduledError(error)) {
        this.log("info", "runtime.activation.retry_scheduled", () => ({
          workerId: this.workerId,
          workflowName: activation.workflowName,
          workflowId: activation.workflowId,
          runId: activation.runId,
          activationId: activation.activationId,
          nextAttemptAt: error.nextAttemptAt,
        }))
        this.count(
          "durable.runtime.activation",
          () => this.activationTags(activation, "retry_scheduled"),
        )
        return { kind: "retry_scheduled", activation }
      }

      this.log("error", "runtime.activation.failed", () => ({
        workerId: this.workerId,
        workflowName: activation.workflowName,
        workflowId: activation.workflowId,
        runId: activation.runId,
        activationId: activation.activationId,
        ...errorFields(error),
      }))
      this.count("durable.runtime.activation", () => this.activationTags(activation, "failed"))
      return { kind: "failed", activation, error }
    }
  }

  private startDispatchShardHeartbeat(sessions: ShardDurabilitySession[]): DispatchHeartbeat {
    let rejectHeartbeat: (error: unknown) => void = () => undefined
    let failed = false
    let inFlight = false
    const failure = new Promise<never>((_resolve, reject) => {
      rejectHeartbeat = reject
    })
    const timer = setInterval(() => {
      if (inFlight || failed) {
        return
      }
      inFlight = true
      void this.heartbeatShardSessions(sessions)
        .catch((error) => {
          if (!failed) {
            failed = true
            rejectHeartbeat(error)
          }
        })
        .finally(() => {
          inFlight = false
        })
    }, this.leaseHeartbeatIntervalMs)
    timer.unref?.()

    return {
      failure,
      stop: () => {
        clearInterval(timer)
      },
    }
  }

  private startQueuedActivationHeartbeat(claims: () => RuntimeClaim[]): QueuedActivationHeartbeat {
    let rejectHeartbeat: (error: unknown) => void = () => undefined
    let failed = false
    let inFlight = false
    const failure = new Promise<never>((_resolve, reject) => {
      rejectHeartbeat = reject
    })
    const timer = setInterval(() => {
      const claimBatch = claims()
      if (claimBatch.length === 0 || inFlight || failed) {
        return
      }
      inFlight = true
      void Promise.all(
        groupRuntimeClaimsBySession(claimBatch).map(({ session, claims }) =>
          session.heartbeatActivations({
            activationIds: [...new Set(claims.map((claim) => claim.activation.activationId))],
            workerId: this.workerId,
            now: this.now(),
            leaseMs: this.activationLeaseMs,
          }),
        ),
      )
        .catch((error) => {
          if (!failed) {
            failed = true
            rejectHeartbeat(error)
          }
        })
        .finally(() => {
          inFlight = false
        })
    }, this.leaseHeartbeatIntervalMs)
    timer.unref?.()

    return {
      failure,
      stop: () => {
        clearInterval(timer)
      },
    }
  }

  private async withActivationHeartbeat<T>(
    session: ShardDurabilitySession,
    activation: ClaimedActivation,
    externalSignal: AbortSignal | undefined,
    shouldHeartbeat: () => boolean,
    fn: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const controller = new AbortController()
    let rejectHeartbeat: (error: unknown) => void = () => undefined
    let heartbeatInFlight = false
    let stopped = false
    const heartbeatFailure = new Promise<never>((_resolve, reject) => {
      rejectHeartbeat = reject
    })
    const failActivation = (error: unknown) => {
      if (!controller.signal.aborted) {
        controller.abort(error)
      }
      rejectHeartbeat(error)
    }
    const onExternalAbort = () => failActivation(abortError())
    externalSignal?.addEventListener("abort", onExternalAbort, { once: true })

    const timer = setInterval(() => {
      if (!shouldHeartbeat() || heartbeatInFlight || stopped) {
        return
      }
      heartbeatInFlight = true
      void session
        .heartbeatActivations({
          activationIds: [activation.activationId],
          workerId: this.workerId,
          now: this.now(),
          leaseMs: this.activationLeaseMs,
        })
        .catch((error) => {
          this.log("warn", "runtime.lease_heartbeat.failure", {
            workerId: this.workerId,
            workflowName: activation.workflowName,
            workflowId: activation.workflowId,
            runId: activation.runId,
            activationId: activation.activationId,
            ...errorFields(error),
          })
          this.count("durable.runtime.lease_heartbeat", {
            workerId: this.workerId,
            workflowName: activation.workflowName,
            status: "failed",
          })
          failActivation(error)
        })
        .finally(() => {
          heartbeatInFlight = false
        })
    }, this.leaseHeartbeatIntervalMs)
    timer.unref?.()

    if (externalSignal?.aborted) {
      failActivation(abortError())
    }

    const work = fn(controller.signal)
    void work.catch(() => undefined)

    try {
      return await Promise.race([work, heartbeatFailure])
    } finally {
      stopped = true
      clearInterval(timer)
      externalSignal?.removeEventListener("abort", onExternalAbort)
    }
  }

  private async releaseActivationQuietly(
    session: ShardDurabilitySession,
    activationId: string,
  ): Promise<void> {
    await session
      .releaseActivation({
        activationId,
        workerId: this.workerId,
      })
      .catch(() => undefined)
  }

  private log(
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: DurableLogFields | (() => DurableLogFields),
  ): void {
    if (!this.hasLogger) {
      return
    }
    logDurable(
      this.observability,
      level,
      event,
      typeof fields === "function" ? fields() : fields,
    )
  }

  private count(name: string, tags?: DurableMetricTags | (() => DurableMetricTags)): void {
    if (!this.hasMetrics) {
      return
    }
    countDurable(
      this.observability,
      name,
      1,
      typeof tags === "function" ? tags() : tags,
    )
  }

  private histogram(name: string, value: number, tags?: DurableMetricTags | (() => DurableMetricTags)): void {
    if (!this.hasMetrics) {
      return
    }
    histogramDurable(
      this.observability,
      name,
      value,
      typeof tags === "function" ? tags() : tags,
    )
  }

  private gauge(name: string, value: number, tags?: DurableMetricTags | (() => DurableMetricTags)): void {
    if (!this.hasMetrics) {
      return
    }
    gaugeDurable(
      this.observability,
      name,
      value,
      typeof tags === "function" ? tags() : tags,
    )
  }

  private activationTags(
    activation: ClaimedActivation,
    status: string,
  ): DurableMetricTags {
    return {
      workerId: this.workerId,
      workflowName: activation.workflowName,
      activationKind: activation.kind,
      eventKind: activationEventKind(activation),
      status,
    }
  }

  private applyTransition(
    workflow: AnyWorkflow,
    instance: ActivationInstanceSnapshot,
    transition: TransitionCommand,
  ): InstanceStatus<any> {
    const current = snapshotFromInstance(instance)
    if (current.status !== "running") {
      throw new Error("Cannot transition a terminal workflow")
    }

    if (transition.kind === "stay") {
      const patch = transition.dataPatch ?? {}
      const nextData =
        isPlainObject(current.phase.data) && isPlainObject(patch)
          ? { ...current.phase.data, ...patch }
          : patch
      const phaseDefinition = workflow.phases[current.phase.name]
      return {
        status: "running",
        common: current.common,
        phase: {
          name: current.phase.name,
          data: toJsonObject(phaseDefinition.state.parse(nextData)),
        },
      }
    }

    if (transition.kind === "go") {
      const phaseDefinition = workflow.phases[transition.phase]
      if (!phaseDefinition) {
        throw new Error(`Unknown phase ${transition.phase} on workflow ${workflow.name}`)
      }

      return {
        status: "running",
        common: current.common,
        phase: {
          name: transition.phase,
          data: toJsonObject(phaseDefinition.state.parse(transition.data)),
        },
      }
    }

    if (transition.kind === "complete") {
      return {
        status: "completed",
        output: workflow.output.parse(transition.output),
      }
    }

    if (transition.kind === "cancel") {
      return { status: "canceled", reason: transition.reason }
    }

    return { status: "failed", error: transition.error }
  }

  private async migrateSnapshot(
    workflow: AnyWorkflow,
    instance: ActivationInstanceSnapshot,
  ): Promise<InstanceStatus<any>> {
    if (instance.workflowVersion > workflow.version) {
      throw new Error(
        `Cannot migrate ${workflow.name} ${instance.workflowId}/${instance.runId} from newer version ${instance.workflowVersion} to worker version ${workflow.version}`,
      )
    }

    if (!instance.phase) {
      throw new Error(`Running workflow ${instance.workflowId} has no phase to migrate`)
    }

    let common = clone(instance.common ?? {})
    let phaseSnapshot = clone(instance.phase)

    for (let version = instance.workflowVersion; version < workflow.version; version += 1) {
      const migration = workflow.migrations?.[version]
      if (!migration) {
        continue
      }

      const result = await migration({
        common,
        phase: phaseSnapshot,
        fromVersion: version,
        toVersion: version + 1,
      })

      common = toJsonObject(result.common ?? common)
      phaseSnapshot = normalizePhaseSnapshot(result.phase ?? phaseSnapshot)
    }

    const phaseDefinition = workflow.phases[phaseSnapshot.name]
    if (!phaseDefinition) {
      throw new Error(
        `Migration for ${workflow.name} did not produce a known phase: ${phaseSnapshot.name}`,
      )
    }

    return {
      status: "running",
      common: toJsonObject(commonSchema(workflow).parse(common)),
      phase: {
        name: phaseSnapshot.name,
        data: toJsonObject(phaseDefinition.state.parse(phaseSnapshot.data)),
      },
    }
  }

  private materializeWaits(
    workflow: AnyWorkflow,
    instance: Pick<PersistedInstance, "workflowId" | "runId" | "updatedAt">,
    snapshot: InstanceStatus<any>,
  ): DurableWait[] {
    if (snapshot.status !== "running") {
      return []
    }

    const waits: DurableWait[] = []
    for (const name of Object.keys(workflow.on ?? {}).sort()) {
      const wait = workflow.on?.[name]
      waits.push({
        kind: "signal",
        name,
        type: name,
        scope: "global",
        ...(wait?.delivery === "future" ? { delivery: wait.delivery } : {}),
      })
    }

    const phaseDefinition = workflow.phases[snapshot.phase.name]
    if (!phaseDefinition) {
      throw new Error(`Unknown phase ${snapshot.phase.name} on workflow ${workflow.name}`)
    }

    if (phaseDefinition.mode === "run") {
      waits.push({ kind: "run", name: "__run", readyAt: instance.updatedAt })
      return waits
    }

    for (const [name, wait] of Object.entries(phaseDefinition.on ?? {}).sort(([left], [right]) =>
      left.localeCompare(right),
    )) {
      if (wait.kind === "signal") {
        waits.push({
          kind: "signal",
          name,
          type: name,
          scope: "phase",
          ...(wait.delivery === "future" ? { delivery: wait.delivery } : {}),
        })
      } else if (wait.kind === "timer") {
        const fireAt = wait.selector({
          common: snapshot.common,
          data: snapshot.phase.data,
        })
        if (fireAt) {
          waits.push({ kind: "timer", name, fireAt })
        }
      } else {
        const handle = wait.selector({
          common: snapshot.common,
          data: snapshot.phase.data,
        })
        if (handle) {
          waits.push({
            kind: "child",
            name,
            workflowName: handle.workflowName,
            workflowVersion: handle.workflowVersion,
            workflowId: handle.workflowId,
            runId: handle.runId,
          })
        }
      }
    }

    return waits
  }

  private waitDefinition(
    workflow: AnyWorkflow,
    instance: ActivationInstanceSnapshot,
    wait: Exclude<DurableWait, { kind: "run" }>,
  ): WaitDefinition {
    if (wait.kind === "signal" && wait.scope === "global") {
      const definition = workflow.on?.[wait.name]
      if (!definition) {
        throw new Error(`Unknown global wait ${wait.name} on workflow ${workflow.name}`)
      }
      return definition
    }

    const phaseName = instance.phase?.name
    if (!phaseName) {
      throw new Error(`Running workflow ${instance.workflowId} has no phase`)
    }

    const definition = workflow.phases[phaseName]?.on?.[wait.name]
    if (!definition) {
      throw new Error(`Unknown wait ${wait.name} on phase ${phaseName}`)
    }
    return definition
  }

  private initialInstance(
    workflow: AnyWorkflow,
    workflowId: string,
    runId: string,
    partitionShard: number,
    startCommand: StartCommand,
    now: string,
  ): PersistedInstance {
    const phaseDefinition = workflow.phases[startCommand.phase]
    if (!phaseDefinition) {
      throw new Error(`Unknown initial phase ${startCommand.phase} on workflow ${workflow.name}`)
    }

    const snapshot: InstanceStatus = {
      status: "running",
      common: toJsonObject(commonSchema(workflow).parse(startCommand.common ?? {})),
      phase: {
        name: startCommand.phase,
        data: toJsonObject(phaseDefinition.state.parse(startCommand.data)),
      },
    }

    return {
      workflowName: workflow.name,
      workflowVersion: workflow.version,
      workflowId,
      runId,
      partitionShard,
      sequence: 0,
      status: "running",
      common: snapshot.common,
      phase: snapshot.phase,
      waits: this.materializeWaits(workflow, { workflowId, runId, updatedAt: now }, snapshot),
      effects: [],
      createdAt: now,
      updatedAt: now,
    }
  }

  private parseSignalPayload(workflow: AnyWorkflow, type: string, payload: unknown): unknown {
    const candidates: SignalWait[] = []
    if (workflow.on?.[type]) {
      candidates.push(workflow.on[type])
    }

    for (const phaseDefinition of Object.values(workflow.phases)) {
      const wait = phaseDefinition.on?.[type]
      if (wait?.kind === "signal") {
        candidates.push(wait)
      }
    }

    if (candidates.length === 0) {
      throw new Error(`Unknown signal ${type} on workflow ${workflow.name}`)
    }

    const uniqueSchemas = new Set(candidates.map((candidate) => candidate.schema))
    if (uniqueSchemas.size !== 1) {
      throw new Error(
        `Ambiguous signal ${type} on workflow ${workflow.name}; send it while the instance is waiting for a matching signal`,
      )
    }

    return candidates[0].schema.parse(payload)
  }

  private parseSignalPayloadForInstance(
    workflow: AnyWorkflow,
    instance: PersistedInstance | null,
    type: string,
    payload: unknown,
  ): unknown {
    if (instance?.status === "running") {
      const wait = instance.waits.find(
        (candidate): candidate is Extract<DurableWait, { kind: "signal" }> =>
          candidate.kind === "signal" && candidate.type === type,
      )
      if (wait) {
        const definition = this.waitDefinition(workflow, instance, wait)
        if (definition.kind !== "signal") {
          throw new Error(`Signal wait ${wait.name} is not a signal definition`)
        }
        return definition.schema.parse(payload)
      }
    }

    return this.parseSignalPayload(workflow, type, payload)
  }

  private async requireInstance(ref: InstanceRef | string): Promise<PersistedInstance> {
    const normalizedRef = normalizeRef(ref)
    const instance = await this.shardSessionForRef(normalizedRef).readInstance(normalizedRef)
    if (!instance) {
      throw new Error(`Unknown workflow instance: ${normalizedRef.workflowId}/${normalizedRef.runId}`)
    }
    return instance
  }

  private shardSessionForRef(ref: InstanceRef): ShardDurabilitySession {
    return this.shardSessionForShard(workflowPartitionShard(ref.workflowId, ref.runId, this.shardCount))
  }

  private shardSessionForWorkflowId(workflowId: string): ShardDurabilitySession {
    return this.shardSessionForShard(workflowPartitionShard(workflowId, "", this.shardCount))
  }

  private shardSessionForShard(shardId: number): ShardDurabilitySession {
    return this.provider.openShard({ shardId })
  }

  private now(): string {
    return this.clock().toISOString()
  }
}

function normalizeRef(ref: InstanceRef | string): InstanceRef {
  return typeof ref === "string" ? { workflowId: ref, runId: "run-1" } : ref
}

function normalizeDispatchShardIds(
  dispatchShardIds: number[] | undefined,
  shardCount: number,
): number[] {
  const shardIds = dispatchShardIds ?? Array.from({ length: shardCount }, (_value, index) => index)
  const uniqueShardIds = [...new Set(shardIds)]
  if (uniqueShardIds.length !== shardIds.length) {
    throw new Error("dispatchShardIds must not contain duplicates")
  }
  for (const shardId of uniqueShardIds) {
    if (!Number.isInteger(shardId) || shardId < 0 || shardId >= shardCount) {
      throw new Error(`dispatchShardIds must be integers between 0 and ${shardCount - 1}`)
    }
  }
  return uniqueShardIds
}

function validateShardId(shardId: number, shardCount: number): number {
  if (!Number.isInteger(shardId) || shardId < 0 || shardId >= shardCount) {
    throw new Error(`shardId must be an integer between 0 and ${shardCount - 1}`)
  }
  return shardId
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
  return value
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`)
  }
  return value
}

function compactCheckpointEffectMutations(
  effects: CheckpointEffectMutation[] | undefined,
): CheckpointEffectMutation[] | undefined {
  const retained = effects?.filter((effect) => effect.status !== "completed")
  return retained && retained.length > 0 ? retained : undefined
}

function activationEventKind(activation: ClaimedActivation): string | undefined {
  return activation.kind === "event" ? activation.event.kind : undefined
}

function commonSchema(workflow: AnyWorkflow): Schema<any> {
  return workflow.common ?? z.object({})
}

function snapshotFromInstance(instance: ActivationInstanceSnapshot): InstanceStatus<any> {
  if (instance.status === "running") {
    return {
      status: "running",
      common: clone(instance.common ?? {}),
      phase: clone(instance.phase ?? { name: "", data: {} }),
    }
  }

  if (instance.status === "completed") {
    return { status: "completed", output: clone(instance.output ?? null) }
  }

  if (instance.status === "canceled") {
    return { status: "canceled", reason: instance.cancelReason ?? "canceled" }
  }

  return { status: "failed", error: clone(instance.error ?? { message: "failed" }) }
}

function trustedJsonCopy<T>(value: T): T {
  return clone(value)
}

function normalizePhaseSnapshot(value: unknown): PhaseSnapshot {
  if (!isPlainObject(value) || typeof value.name !== "string") {
    throw new Error("Migration must return a phase with a string name")
  }

  return {
    name: value.name,
    data: toJsonObject(value.data ?? {}),
  }
}

function pollDelayMs(input: {
  now: string
  nextWakeAt?: string
  activations: number
  minPollIntervalMs: number
  maxPollIntervalMs: number
  jitterRatio: number
}): number {
  const baseDelay =
    input.nextWakeAt === undefined
      ? input.activations > 0
        ? input.minPollIntervalMs
        : input.maxPollIntervalMs
      : new Date(input.nextWakeAt).getTime() - new Date(input.now).getTime()
  const clamped = Math.min(
    input.maxPollIntervalMs,
    Math.max(input.minPollIntervalMs, Number.isFinite(baseDelay) ? baseDelay : input.maxPollIntervalMs),
  )
  const jitter = clamped * input.jitterRatio * (Math.random() * 2 - 1)
  return Math.max(0, Math.round(clamped + jitter))
}

function sleepMs(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(abortError())
  }

  return new Promise((resolve, reject) => {
    let settled = false
    const onAbort = () => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      reject(abortError())
    }
    const timer = setTimeout(() => {
      if (settled) {
        return
      }
      settled = true
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

function abortError(): Error {
  const error = new Error("Worker aborted")
  error.name = "AbortError"
  return error
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError"
}

class ActivityRetryScheduledError extends Error {
  constructor(readonly nextAttemptAt: string) {
    super(`Activity retry scheduled for ${nextAttemptAt}`)
    this.name = "ActivityRetryScheduledError"
  }
}

function isActivityRetryScheduledError(error: unknown): error is ActivityRetryScheduledError {
  return error instanceof Error && error.name === "ActivityRetryScheduledError"
}

function isNonRetryableActivityError(error: unknown): boolean {
  return isNonRetryableError(error) || isAbortError(error)
}

function activityDurability(options?: ActivityOptions): "checkpoint" | "eager" {
  if (
    options?.durability === "eager" ||
    options?.startToCloseTimeoutMs !== undefined ||
    options?.heartbeatTimeoutMs !== undefined
  ) {
    return "eager"
  }
  return "checkpoint"
}

function childDurability(options?: ChildOptions): "checkpoint" | "eager" {
  return options?.durability === "eager" ? "eager" : "checkpoint"
}

function childRefKey(workflowId: string, runId: string): string {
  return `${workflowId}\0${runId}`
}

function childHandleFromStart<W extends AnyWorkflow>(start: CheckpointChildStart): ChildHandle<W> {
  return {
    workflowName: start.workflowName,
    workflowVersion: start.workflowVersion,
    workflowId: start.workflowId,
    runId: start.runId,
    created: false,
  } as ChildHandle<W>
}

function shouldCreateWorkflowRunForStatus(
  policy: "failed_only" | "not_running" | "always",
  status: PersistedInstance["status"],
): boolean {
  if (policy === "always") {
    return true
  }
  if (policy === "not_running") {
    return status !== "running"
  }
  return status === "failed" || status === "canceled"
}

function requiresActivationHeartbeat(claim: ClaimedActivationWithInstance): boolean {
  return claim.lease.scope === "activation"
}

function groupRuntimeClaimsBySession(
  claims: RuntimeClaim[],
): Array<{ session: ShardDurabilitySession; claims: RuntimeClaim[] }> {
  const groups = new Map<ShardDurabilitySession, RuntimeClaim[]>()
  for (const claim of claims) {
    const group = groups.get(claim.session)
    if (group) {
      group.push(claim)
    } else {
      groups.set(claim.session, [claim])
    }
  }
  return [...groups].map(([session, claims]) => ({ session, claims }))
}

type LocalActivityOptions = {
  maxAttempts: number
  maxElapsedMs: number | null
  initialIntervalMs: number
  maxIntervalMs: number
  backoffCoefficient: number
  nonRetryableErrorNames: string[]
}

function normalizeLocalActivityOptions(options?: ActivityOptions): LocalActivityOptions {
  const retry = options?.retry
  const maxAttempts = retry?.maxAttempts ?? 3
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error("activity retry.maxAttempts must be a positive integer")
  }
  const initialIntervalMs = retry?.initialIntervalMs ?? 1_000
  const maxIntervalMs = retry?.maxIntervalMs ?? 30_000
  const backoffCoefficient = retry?.backoffCoefficient ?? 2
  const maxElapsedMs = retry?.maxElapsedMs ?? null
  if (!Number.isInteger(initialIntervalMs) || initialIntervalMs < 0) {
    throw new Error("activity retry.initialIntervalMs must be a non-negative integer")
  }
  if (maxIntervalMs !== null && (!Number.isInteger(maxIntervalMs) || maxIntervalMs < 0)) {
    throw new Error("activity retry.maxIntervalMs must be a non-negative integer when provided")
  }
  if (maxElapsedMs !== null && (!Number.isInteger(maxElapsedMs) || maxElapsedMs <= 0)) {
    throw new Error("activity retry.maxElapsedMs must be a positive integer when provided")
  }
  if (typeof backoffCoefficient !== "number" || backoffCoefficient < 1) {
    throw new Error("activity retry.backoffCoefficient must be at least 1")
  }
  return {
    maxAttempts,
    maxElapsedMs,
    initialIntervalMs,
    maxIntervalMs: maxIntervalMs ?? 30_000,
    backoffCoefficient,
    nonRetryableErrorNames: retry?.nonRetryableErrorNames ?? [],
  }
}

function localRetryDecision(
  effect: {
    attempt: number
    firstAttemptStartedAt: string
    maxAttempts: number
    maxElapsedMs: number | null
    initialIntervalMs: number
    maxIntervalMs: number
    backoffCoefficient: number
    nonRetryableErrorNames: string[]
  },
  error: { name?: string; message: string },
  now: string,
  retryable: boolean,
): { status: "failed" } | { status: "retry_scheduled"; nextAttemptAt: string; nextAttempt: number } {
  if (!retryable || (error.name && effect.nonRetryableErrorNames.includes(error.name))) {
    return { status: "failed" }
  }
  if (effect.attempt >= effect.maxAttempts) {
    return { status: "failed" }
  }
  const rawDelay =
    effect.initialIntervalMs * effect.backoffCoefficient ** Math.max(0, effect.attempt - 1)
  const delayMs = Math.min(effect.maxIntervalMs, Math.max(0, Math.round(rawDelay)))
  const nextAttemptAt = addMs(now, delayMs)
  if (effect.maxElapsedMs !== null) {
    const maxElapsedAt = addMs(effect.firstAttemptStartedAt, effect.maxElapsedMs)
    if (nextAttemptAt > maxElapsedAt) {
      return { status: "failed" }
    }
  }
  return {
    status: "retry_scheduled",
    nextAttemptAt,
    nextAttempt: effect.attempt + 1,
  }
}

function effectFirstAttemptStartedAt(
  effect: EffectRecord | CheckpointEffectMutation | undefined,
): string | undefined {
  if (!effect) {
    return undefined
  }
  if ("firstAttemptStartedAt" in effect) {
    return effect.firstAttemptStartedAt
  }
  return undefined
}

function errorFromSerialized(error: { message: string; name?: string; stack?: string }): Error {
  const value = new Error(error.message)
  value.name = error.name ?? "Error"
  if (error.stack) {
    value.stack = error.stack
  }
  return value
}

function addMs(isoValue: string, ms: number): string {
  return new Date(new Date(isoValue).getTime() + ms).toISOString()
}

function defaultChildWorkflowId(
  parentWorkflowId: string,
  parentRunId: string,
  sequence: number,
  key: string,
  shardCount: number,
  parentShard: number,
): string {
  const base = `${parentWorkflowId}__${sequence}__${safeId(key)}`
  if (workflowPartitionShard(base, "run-1", shardCount) === parentShard) {
    return base
  }
  for (let attempt = 1; attempt <= 10_000; attempt += 1) {
    const candidate = `${base}__shard_${attempt}`
    if (workflowPartitionShard(candidate, "run-1", shardCount) === parentShard) {
      return candidate
    }
  }
  throw new Error(`Could not find shard-affine child workflow id for ${parentWorkflowId}/${parentRunId}/${key}`)
}

function earliestIso(...values: Array<string | undefined>): string | undefined {
  return values.filter(Boolean).sort()[0]
}

function callWaitHandler(
  wait: WaitDefinition,
  args: HandlerArgs<any>,
): Promise<TransitionCommand> | TransitionCommand {
  return wait.handler(args)
}
