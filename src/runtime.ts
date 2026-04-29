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
  ChildHandle,
  ChildOptions,
  DurableContext,
  HandlerArgs,
  InputOf,
  InstanceRef,
  InstanceStatus,
  JsonObject,
  OutputOf,
  PhaseSnapshot,
  Schema,
  SignalWait,
  StartCommand,
  TransitionCommand,
  WaitDefinition,
} from "./workflow.js"
import {
  clone,
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
  clock?: () => Date
  workflows?: AnyWorkflow[]
}

export class DurableRuntime {
  private readonly workflows = new Map<string, AnyWorkflow>()
  private readonly clock: () => Date
  private readonly workerId: string
  private readonly shardCount: number
  private readonly dispatchLeaseMs: number
  private readonly activationLeaseMs: number

  constructor(
    private readonly provider: DurabilityProvider,
    options: DurableRuntimeOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date())
    this.workerId = options.workerId ?? `worker-${randomUUID()}`
    this.shardCount = options.shardCount ?? 1
    this.dispatchLeaseMs = options.dispatchLeaseMs ?? 30_000
    this.activationLeaseMs = options.activationLeaseMs ?? 30_000
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
    const parsedPayload = this.parseSignalPayload(workflow, type, payload)
    const normalizedRef = normalizeRef(ref)
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

  async drain(options: { maxActivations?: number } = {}): Promise<{ activations: number }> {
    const maxActivations = options.maxActivations ?? 100
    let activations = 0
    const shardIds = await this.claimDispatchShards()

    if (shardIds.length === 0) {
      return { activations }
    }

    while (activations < maxActivations) {
      await this.heartbeatDispatchShards(shardIds)
      const activation = await this.provider.claimReadyActivation({
        workerId: this.workerId,
        shardIds,
        workflows: this.workflowVersions(),
        now: this.now(),
        leaseMs: this.activationLeaseMs,
      })

      if (!activation) {
        break
      }

      await this.runActivation(activation)
      activations += 1
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

  private workflowVersions(): Record<string, { version: number }> {
    return Object.fromEntries(
      [...this.workflows.values()].map((workflow) => [
        workflow.name,
        { version: workflow.version },
      ]),
    )
  }

  private async runActivation(activation: ClaimedActivation): Promise<void> {
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
      const waits = this.materializeWaits(workflow, latest, next)
      await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits)
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
    const ctx = this.contextFor(workflow, latest, activation.activationId, activation.activationTime)
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
    const waits = next.status === "running" ? this.materializeWaits(workflow, latest, next) : []
    await this.commitOrDiscard(latest, workflow, activation.activationId, next, waits, {
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
      now: this.now(),
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
  ): DurableContext {
    return {
      now: () => activationTime,
      activity: async <T>(key: string, fn: () => Promise<T> | T): Promise<T> => {
        const reservation = await this.provider.getOrReserveEffect({
          workflowId: instance.workflowId,
          runId: instance.runId,
          activationId: currentActivationId,
          key,
        })

        if (reservation.status === "completed") {
          return clone(reservation.result) as T
        }

        if (reservation.status === "failed") {
          throw new Error(reservation.error.message)
        }

        try {
          const result = await fn()
          await this.provider.completeEffect({
            workflowId: instance.workflowId,
            runId: instance.runId,
            effectId: reservation.effectId,
            result: toJson(result),
          })
          return result
        } catch (error) {
          await this.provider.failEffect({
            workflowId: instance.workflowId,
            runId: instance.runId,
            effectId: reservation.effectId,
            error: serializeError(error),
          })
          throw error
        }
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
          })

          return handle as ChildHandle<W>
        },
        result: async <W extends AnyWorkflow>(handle: ChildHandle<W>): Promise<OutputOf<W>> => {
          return this.provider.readOutput(handle)
        },
        run: async <W extends AnyWorkflow>(
          key: string,
          childWorkflow: W,
          input: InputOf<W>,
          options: ChildOptions = {},
        ): Promise<OutputOf<W>> => {
          const handle = await this.contextFor(workflow, instance, currentActivationId, activationTime).child.start(
            key,
            childWorkflow,
            input,
            options,
          )
          return this.provider.readOutput(handle)
        },
      },
    }
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

    return candidates[0].schema.parse(payload)
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

function callWaitHandler(
  wait: WaitDefinition,
  args: HandlerArgs<any>,
): Promise<TransitionCommand> | TransitionCommand {
  return wait.handler(args)
}
