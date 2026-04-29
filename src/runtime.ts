import { randomUUID } from "node:crypto"
import { z } from "zod"
import type {
  ClaimedActivation,
  ConflictPolicy,
  DurabilityProvider,
  DurableWait,
  PersistedInstance,
  SignalRecord,
} from "./interface.js"
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

export type DurableRuntimeOptions = {
  workerId?: string
  shardCount?: number
  dispatchLeaseMs?: number
  activationLeaseMs?: number
  leaseHeartbeatIntervalMs?: number
  clock?: () => Date
  workflows?: AnyWorkflow[]
}

export type DrainResult = {
  activations: number
  nextWakeAt?: string
}

export type RunWorkerOptions = {
  maxActivationsPerDrain?: number
  minPollIntervalMs?: number
  maxPollIntervalMs?: number
  jitterRatio?: number
  signal?: AbortSignal
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>
}

export type DrainOptions = {
  maxActivations?: number
  signal?: AbortSignal
}

export class DurableRuntime {
  private readonly workflows = new Map<string, AnyWorkflow>()
  private readonly clock: () => Date
  private readonly workerId: string
  private readonly shardCount: number
  private readonly dispatchLeaseMs: number
  private readonly activationLeaseMs: number
  private readonly leaseHeartbeatIntervalMs: number

  constructor(
    private readonly provider: DurabilityProvider,
    options: DurableRuntimeOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date())
    this.workerId = options.workerId ?? `worker-${randomUUID()}`
    this.shardCount = options.shardCount ?? 1
    this.dispatchLeaseMs = options.dispatchLeaseMs ?? 30_000
    this.activationLeaseMs = options.activationLeaseMs ?? 30_000
    this.leaseHeartbeatIntervalMs =
      options.leaseHeartbeatIntervalMs ??
      Math.max(1, Math.floor(Math.min(this.dispatchLeaseMs, this.activationLeaseMs) / 3))
    this.registerWorkflows(options.workflows ?? [])
  }

  registerWorkflows(workflows: AnyWorkflow[]): void {
    for (const workflow of workflows) {
      this.workflows.set(workflow.name, workflow)
    }
  }

  async start<W extends AnyWorkflow>(
    workflow: W,
    input: InputOf<W>,
    options: { workflowId?: string; runId?: string; conflictPolicy?: ConflictPolicy } = {},
  ): Promise<InstanceRef> {
    this.registerWorkflows([workflow])
    const parsedInput = workflow.input.parse(input)
    const startCommand = workflow.initial(parsedInput)
    const now = this.now()
    const workflowId = options.workflowId ?? `${workflow.name}-${randomUUID()}`
    const runId = options.runId ?? "run-1"
    const instance = this.initialInstance(workflow, workflowId, runId, startCommand, now)

    return this.provider.createInstance({
      workflowName: workflow.name,
      workflowVersion: workflow.version,
      workflowId: instance.workflowId,
      runId: instance.runId,
      partitionShard: workflowPartitionShard(instance.workflowId, instance.runId, this.shardCount),
      common: instance.common!,
      phase: instance.phase!,
      waits: instance.waits,
      now,
      conflictPolicy: options.conflictPolicy,
    })
  }

  async signal<W extends AnyWorkflow>(
    workflow: W,
    ref: InstanceRef | string,
    type: string,
    payload: unknown,
  ): Promise<SignalRecord> {
    this.registerWorkflows([workflow])
    const normalizedRef = normalizeRef(ref)
    const instance = await this.provider.loadInstance(normalizedRef)
    const parsedPayload = this.parseSignalPayloadForInstance(workflow, instance, type, payload)
    return this.provider.appendSignal({
      ...normalizedRef,
      type,
      payload: toJson(parsedPayload),
      receivedAt: this.now(),
    })
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
    return definition.schema.parse(output)
  }

  async drain(options: DrainOptions = {}): Promise<DrainResult> {
    const maxActivations = options.maxActivations ?? 100
    let activations = 0
    let nextWakeAt: string | undefined
    const shardIds = await this.claimDispatchShards()

    if (shardIds.length === 0) {
      return { activations }
    }

    try {
      while (activations < maxActivations) {
        await this.heartbeatDispatchShards(shardIds)
        const claim = await this.provider.claimReadyActivation({
          workerId: this.workerId,
          shardIds,
          workflows: this.workflowVersions(),
          now: this.now(),
          leaseMs: this.activationLeaseMs,
        })

        if (!claim.activation) {
          nextWakeAt = claim.nextWakeAt
          break
        }

        try {
          await this.withLeaseHeartbeats(shardIds, claim.activation, options.signal, (signal) =>
            this.runActivation(claim.activation!, signal),
          )
        } catch (error) {
          await this.releaseActivationQuietly(claim.activation.activationId)
          if (!isActivityRetryScheduledError(error)) {
            throw error
          }
        }
        activations += 1
      }
    } finally {
      await this.releaseDispatchShards(shardIds)
    }

    return nextWakeAt ? { activations, nextWakeAt } : { activations }
  }

  async runWorker(options: RunWorkerOptions = {}): Promise<{ activations: number }> {
    const minPollIntervalMs = options.minPollIntervalMs ?? 10
    const maxPollIntervalMs = options.maxPollIntervalMs ?? 1_000
    const jitterRatio = options.jitterRatio ?? 0.1
    const sleep = options.sleep ?? sleepMs
    let activations = 0

    while (!options.signal?.aborted) {
      let result: DrainResult
      try {
        result = await this.drain({
          maxActivations: options.maxActivationsPerDrain,
          signal: options.signal,
        })
      } catch (error) {
        if (isAbortError(error) || options.signal?.aborted) {
          break
        }
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

      try {
        await sleep(delayMs, options.signal)
      } catch (error) {
        if (isAbortError(error) || options.signal?.aborted) {
          break
        }
        throw error
      }
    }

    return { activations }
  }

  private async claimDispatchShards(): Promise<number[]> {
    const shardIds: number[] = []
    for (let shardId = 0; shardId < this.shardCount; shardId += 1) {
      const lease = await this.provider.claimDispatchShard({
        shardId,
        ownerId: this.workerId,
        now: this.now(),
        leaseMs: this.dispatchLeaseMs,
      })
      if (lease) {
        shardIds.push(shardId)
      }
    }
    return shardIds
  }

  private async heartbeatDispatchShards(shardIds: number[]): Promise<void> {
    await Promise.all(
      shardIds.map((shardId) =>
        this.provider.heartbeatDispatchShard({
          shardId,
          ownerId: this.workerId,
          now: this.now(),
          leaseMs: this.dispatchLeaseMs,
        }),
      ),
    )
  }

  private async releaseDispatchShards(shardIds: number[]): Promise<void> {
    await Promise.allSettled(
      shardIds.map((shardId) =>
        this.provider.releaseDispatchShard({
          shardId,
          ownerId: this.workerId,
        }),
      ),
    )
  }

  private workflowVersions(): Record<string, { version: number }> {
    return Object.fromEntries(
      [...this.workflows.values()].map((workflow) => [
        workflow.name,
        { version: workflow.version },
      ]),
    )
  }

  private async runActivation(activation: ClaimedActivation, signal: AbortSignal): Promise<void> {
    const latest = await this.requireInstance({
      workflowId: activation.workflowId,
      runId: activation.runId,
    })
    if (latest.status !== "running" || latest.sequence !== activation.sequence) {
      await this.provider.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      return
    }

    const workflow = this.workflows.get(latest.workflowName)
    if (!workflow) {
      await this.provider.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      return
    }

    if (latest.workflowVersion > workflow.version) {
      await this.provider.releaseActivation({
        activationId: activation.activationId,
        workerId: this.workerId,
      })
      throw new Error(
        `Workflow ${workflow.name} instance ${latest.workflowId}/${latest.runId} is at newer version ${latest.workflowVersion}; worker only has version ${workflow.version}`,
      )
    }

    if (activation.kind === "migration") {
      const next = await this.migrateSnapshot(workflow, latest)
      const commitTime = this.now()
      const waits = this.materializeWaits(
        workflow,
        { workflowId: latest.workflowId, runId: latest.runId, updatedAt: commitTime },
        next,
      )
      await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits, commitTime)
      return
    }

    const common = commonSchema(workflow).parse(latest.common)
    const phaseSnapshot = latest.phase
    if (!phaseSnapshot) {
      throw new Error(`Running workflow ${latest.workflowId} has no phase`)
    }

    const phaseDefinition = workflow.phases[phaseSnapshot.name]
    if (!phaseDefinition) {
      throw new Error(`Unknown phase ${phaseSnapshot.name} on workflow ${workflow.name}`)
    }

    const data = phaseDefinition.state.parse(phaseSnapshot.data)
    const ctx = this.contextFor(
      workflow,
      latest,
      activation.activationId,
      activation.activationTime,
      signal,
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
          event: waitDefinition.schema.parse(event.payload),
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
          event: event.event,
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
    await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits, commitTime, {
      consumeSignalId,
      consumeChildRecordId,
    })
  }

  private async commitOrDiscard(
    latest: PersistedInstance,
    workflow: AnyWorkflow,
    activationId: string,
    next: InstanceStatus<any>,
    waits: DurableWait[],
    now: string,
    options: { consumeSignalId?: string; consumeChildRecordId?: string } = {},
  ): Promise<void> {
    const result = await this.provider.commitCheckpoint({
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
    })

    if (!result.ok) {
      await this.provider.releaseActivation({
        activationId,
        workerId: this.workerId,
      })
    }
  }

  private contextFor(
    workflow: AnyWorkflow,
    instance: PersistedInstance,
    currentActivationId: string,
    activationTime: string,
    activationSignal: AbortSignal,
  ): DurableContext {
    return {
      now: () => activationTime,
      activity: async <T>(
        key: string,
        fn: (ctx: ActivityContext) => Promise<T> | T,
        options?: ActivityOptions,
      ): Promise<T> => {
        const reservation = await this.provider.getOrReserveEffect({
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          workerId: this.workerId,
          key,
          now: this.now(),
          options,
        })

        if (reservation.status === "completed") {
          return clone(reservation.result) as T
        }

        if (reservation.status === "failed") {
          throw new Error(reservation.error.message)
        }

        const activityContext = {
          heartbeat: async (details?: JsonValue): Promise<void> => {
            if (activationSignal.aborted) {
              throw abortError()
            }
            await this.provider.heartbeatEffect({
              workflowId: instance.workflowId,
              runId: instance.runId,
              activationId: currentActivationId,
              workerId: this.workerId,
              effectId: reservation.effectId,
              attemptId: reservation.attemptId,
              now: this.now(),
              details: details === undefined ? undefined : toJson(details),
            })
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
          const failure = await this.provider.failEffect({
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
            throw new ActivityRetryScheduledError(failure.nextAttemptAt)
          }
          throw error
        }

        await this.provider.completeEffect({
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          workerId: this.workerId,
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          result: toJson(result),
          now: this.now(),
        })
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
            options.workflowId ?? `${instance.workflowId}__${instance.sequence}__${safeId(key)}`
          const childInstance = this.initialInstance(
            childWorkflow,
            childWorkflowId,
            "run-1",
            startCommand,
            now,
          )

          const handle = await this.provider.createChildInstance({
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
            key,
            parentClosePolicy: options.parentClosePolicy ?? "cancel",
            conflictPolicy: options.conflictPolicy ?? "use_existing",
          })

          return handle as ChildHandle<W>
        },
        cancel: async (handle: ChildHandle<any>): Promise<void> => {
          await this.provider.cancelChild({
            parentWorkflowId: instance.workflowId,
            parentRunId: instance.runId,
            activationId: currentActivationId,
            workerId: this.workerId,
            workflowId: handle.workflowId,
            runId: handle.runId,
            now: this.now(),
          })
        },
      },
    }
  }

  private async withLeaseHeartbeats<T>(
    shardIds: number[],
    activation: ClaimedActivation,
    externalSignal: AbortSignal | undefined,
    fn: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const controller = new AbortController()
    let rejectHeartbeat: (error: unknown) => void = () => undefined
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
      void Promise.all([
        this.heartbeatDispatchShards(shardIds),
        this.provider.heartbeatActivation({
          activationId: activation.activationId,
          workerId: this.workerId,
          now: this.now(),
          leaseMs: this.activationLeaseMs,
        }),
      ]).catch(failActivation)
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
      clearInterval(timer)
      externalSignal?.removeEventListener("abort", onExternalAbort)
    }
  }

  private async releaseActivationQuietly(activationId: string): Promise<void> {
    await this.provider
      .releaseActivation({
        activationId,
        workerId: this.workerId,
      })
      .catch(() => undefined)
  }

  private applyTransition(
    workflow: AnyWorkflow,
    instance: PersistedInstance,
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
    instance: PersistedInstance,
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
      waits.push({ kind: "signal", name, type: name, scope: "global" })
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
        waits.push({ kind: "signal", name, type: name, scope: "phase" })
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
    instance: PersistedInstance,
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
    const instance = await this.provider.loadInstance(normalizedRef)
    if (!instance) {
      throw new Error(`Unknown workflow instance: ${normalizedRef.workflowId}/${normalizedRef.runId}`)
    }
    return instance
  }

  private now(): string {
    return this.clock().toISOString()
  }
}

function normalizeRef(ref: InstanceRef | string): InstanceRef {
  return typeof ref === "string" ? { workflowId: ref, runId: "run-1" } : ref
}

function commonSchema(workflow: AnyWorkflow): Schema<any> {
  return workflow.common ?? z.object({})
}

function snapshotFromInstance(instance: PersistedInstance): InstanceStatus<any> {
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
    timer.unref?.()
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

function callWaitHandler(
  wait: WaitDefinition,
  args: HandlerArgs<any>,
): Promise<TransitionCommand> | TransitionCommand {
  return wait.handler(args)
}
