import { randomUUID } from "node:crypto"
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs"
import { mkdir, rename, writeFile } from "node:fs/promises"
import { dirname } from "node:path"
import { z } from "zod"

export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }
export type JsonObject = { [key: string]: JsonValue }
export type Schema<T> = z.ZodType<T>

export type SerializedError = {
  message: string
  name?: string
  stack?: string
}

export type PhaseSnapshot = {
  name: string
  data: JsonObject
}

export type InstanceStatus<Output = JsonObject> =
  | { status: "running"; common: JsonObject; phase: PhaseSnapshot }
  | { status: "completed"; output: Output }
  | { status: "canceled"; reason: string }
  | { status: "failed"; error: SerializedError }

export type InstanceRef = {
  workflowId: string
  runId: string
}

export type ChildHandle<W = AnyWorkflow> = InstanceRef & {
  workflowName: string
  workflowVersion: number
  __workflow?: W
}

export type StartCommand<Common = unknown, Data = unknown> = {
  kind: "start"
  common?: Common
  phase: string
  data: Data
}

export type TransitionCommand<Output = unknown> =
  | { kind: "stay"; dataPatch?: unknown }
  | { kind: "go"; phase: string; data: unknown }
  | { kind: "complete"; output: Output }
  | { kind: "cancel"; reason: string }
  | { kind: "fail"; error: SerializedError }

export type DurableContext = {
  now(): string
  activity<T>(key: string, fn: () => Promise<T> | T): Promise<T>
  child: {
    start<W extends AnyWorkflow>(
      key: string,
      workflow: W,
      input: InputOf<W>,
      options?: ChildOptions,
    ): Promise<ChildHandle<W>>
    result<W extends AnyWorkflow>(handle: ChildHandle<W>): Promise<OutputOf<W>>
    run<W extends AnyWorkflow>(
      key: string,
      workflow: W,
      input: InputOf<W>,
      options?: ChildOptions,
    ): Promise<OutputOf<W>>
  }
}

export type ChildOptions = {
  workflowId?: string
}

export type HandlerArgs<Event = unknown> = {
  ctx: DurableContext
  common: any
  data: any
  event: Event
}

export type RunArgs = {
  ctx: DurableContext
  common: any
  data: any
}

export type QueryArgs<Output = unknown> = {
  sequence: number
  snapshot: InstanceStatus<Output>
}

type SignalWait<Event = unknown> = {
  kind: "signal"
  schema: Schema<Event>
  handler: (args: HandlerArgs<Event>) => Promise<TransitionCommand> | TransitionCommand
}

type TimerWait = {
  kind: "timer"
  selector: (args: { common: any; data: any }) => string | null
  handler: (args: HandlerArgs<{ firedAt: string }>) => Promise<TransitionCommand> | TransitionCommand
}

type ChildWait = {
  kind: "child"
  selector: (args: { common: any; data: any }) => ChildHandle | null
  handler: (args: HandlerArgs<ChildEvent>) => Promise<TransitionCommand> | TransitionCommand
}

type WaitDefinition = SignalWait<any> | TimerWait | ChildWait

type PhaseDefinition = {
  state: Schema<any>
  mode: "run" | "on"
  run?: (args: RunArgs) => Promise<TransitionCommand> | TransitionCommand
  on?: Record<string, WaitDefinition>
}

type QueryDefinition = {
  schema: Schema<any>
  handler: (args: QueryArgs<any>) => unknown
}

export type WorkflowDefinition<Input = any, Output = any, Common = any> = {
  name: string
  version: number
  input: Schema<Input>
  output: Schema<Output>
  common?: Schema<Common>
  initial(input: Input): StartCommand<Common, any>
  phases: Record<string, PhaseDefinition>
  on?: Record<string, SignalWait>
  queries?: Record<string, QueryDefinition>
}

export type AnyWorkflow = WorkflowDefinition<any, any, any>
export type InputOf<W> = W extends WorkflowDefinition<infer Input, any, any> ? Input : never
export type OutputOf<W> = W extends WorkflowDefinition<any, infer Output, any> ? Output : never

export type ChildEvent =
  | { ok: true; output: any }
  | { ok: false; error: SerializedError }

export function start<Common, Data>(input: {
  common?: Common
  phase: string
  data: Data
}): StartCommand<Common, Data> {
  return { kind: "start", ...input }
}

export function stay<DataPatch = unknown>(dataPatch?: DataPatch): TransitionCommand {
  return { kind: "stay", dataPatch }
}

export function checkpoint<DataPatch = unknown>(dataPatch?: DataPatch): TransitionCommand {
  return stay(dataPatch)
}

export function go<Data>(phase: string, data: Data): TransitionCommand {
  return { kind: "go", phase, data }
}

export function complete<Output>(output: Output): TransitionCommand<Output> {
  return { kind: "complete", output }
}

export function cancel(reason: string): TransitionCommand {
  return { kind: "cancel", reason }
}

export function fail(error: unknown): TransitionCommand {
  return { kind: "fail", error: serializeError(error) }
}

export function defineWorkflow<Input, Output, Common>(
  definition: WorkflowDefinition<Input, Output, Common>,
): WorkflowDefinition<Input, Output, Common> {
  for (const [name, definitionPhase] of Object.entries(definition.phases)) {
    if ((definitionPhase.run && definitionPhase.on) || (!definitionPhase.run && !definitionPhase.on)) {
      throw new Error(`Phase ${name} must define exactly one of run or on`)
    }
  }

  return definition
}

export function phase(definition: {
  state?: Schema<any>
  run?: (args: RunArgs) => Promise<TransitionCommand> | TransitionCommand
  on?: Record<string, WaitDefinition>
}): PhaseDefinition {
  if ((definition.run && definition.on) || (!definition.run && !definition.on)) {
    throw new Error("phase() requires exactly one of run or on")
  }

  return {
    state: definition.state ?? z.object({}),
    mode: definition.run ? "run" : "on",
    run: definition.run,
    on: definition.on,
  }
}

export function signal<Event>(
  schema: Schema<Event>,
  handler: (args: HandlerArgs<Event>) => Promise<TransitionCommand> | TransitionCommand,
): SignalWait<Event> {
  return { kind: "signal", schema, handler }
}

export function timer(
  selector: (args: { common: any; data: any }) => string | null,
  handler: (args: HandlerArgs<{ firedAt: string }>) => Promise<TransitionCommand> | TransitionCommand,
): TimerWait {
  return { kind: "timer", selector, handler }
}

export function child(
  selector: (args: { common: any; data: any }) => ChildHandle | null,
  handler: (args: HandlerArgs<ChildEvent>) => Promise<TransitionCommand> | TransitionCommand,
): ChildWait {
  return { kind: "child", selector, handler }
}

export function query<Output>(
  schema: Schema<Output>,
  handler: (args: QueryArgs<any>) => Output,
): QueryDefinition {
  return { schema, handler }
}

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

type Store = {
  instances: Record<string, PersistedInstance>
  signals: SignalRecord[]
  children: ChildRecord[]
  nextSignalId: number
  nextEffectId: number
  nextChildId: number
}

type CreateInstanceInput = {
  workflow: AnyWorkflow
  workflowId: string
  runId: string
  common: JsonObject
  phase: PhaseSnapshot
  waits: DurableWait[]
  now: string
  parent?: PersistedInstance["parent"]
  conflictPolicy?: "fail" | "use_existing" | "terminate_existing"
}

type CommitCheckpointInput = {
  workflowId: string
  runId: string
  expectedSequence: number
  activationId: string
  next: InstanceStatus<any>
  waits: DurableWait[]
  now: string
  consumeSignalId?: string
  consumeChildRecordId?: string
}

type ReadyActivation =
  | {
      kind: "run"
      workflow: AnyWorkflow
      instance: PersistedInstance
      activationId: string
      sort: string[]
    }
  | {
      kind: "event"
      workflow: AnyWorkflow
      instance: PersistedInstance
      activationId: string
      waitName: string
      wait: WaitDefinition
      event: ReadyEvent
      sort: string[]
    }

type ReadyEvent =
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

export class JsonFileDurabilityProvider {
  private state: Store

  constructor(private readonly filePath: string) {
    this.state = this.load()
  }

  async createInstance(input: CreateInstanceInput): Promise<InstanceRef> {
    const key = instanceKey(input)
    const existing = this.state.instances[key]
    const conflictPolicy = input.conflictPolicy ?? "fail"

    if (existing && conflictPolicy === "use_existing") {
      return { workflowId: existing.workflowId, runId: existing.runId }
    }

    if (existing && conflictPolicy === "fail") {
      throw new Error(`Workflow instance already exists: ${input.workflowId}/${input.runId}`)
    }

    this.state.instances[key] = {
      workflowName: input.workflow.name,
      workflowVersion: input.workflow.version,
      workflowId: input.workflowId,
      runId: input.runId,
      sequence: 0,
      status: "running",
      common: clone(input.common),
      phase: clone(input.phase),
      waits: clone(input.waits),
      effects: [],
      createdAt: input.now,
      updatedAt: input.now,
      parent: input.parent,
    }

    await this.save()
    return { workflowId: input.workflowId, runId: input.runId }
  }

  async createChildInstance(input: CreateInstanceInput & {
    parentWorkflowId: string
    parentRunId: string
    activationId: string
    key: string
  }): Promise<ChildHandle> {
    const existing = this.state.children.find(
      (record) =>
        record.parentWorkflowId === input.parentWorkflowId &&
        record.parentRunId === input.parentRunId &&
        record.activationId === input.activationId &&
        record.key === input.key,
    )

    if (existing) {
      return childHandle(existing)
    }

    const childRecordId = `child-${this.state.nextChildId++}`
    const ref = { workflowId: input.workflowId, runId: input.runId }
    const key = instanceKey(ref)

    if (this.state.instances[key]) {
      throw new Error(`Child workflow instance already exists: ${input.workflowId}/${input.runId}`)
    }

    this.state.instances[key] = {
      workflowName: input.workflow.name,
      workflowVersion: input.workflow.version,
      workflowId: input.workflowId,
      runId: input.runId,
      sequence: 0,
      status: "running",
      common: clone(input.common),
      phase: clone(input.phase),
      waits: clone(input.waits),
      effects: [],
      createdAt: input.now,
      updatedAt: input.now,
      parent: {
        workflowId: input.parentWorkflowId,
        runId: input.parentRunId,
        childRecordId,
      },
    }

    const record: ChildRecord = {
      childRecordId,
      parentWorkflowId: input.parentWorkflowId,
      parentRunId: input.parentRunId,
      activationId: input.activationId,
      key: input.key,
      workflowName: input.workflow.name,
      workflowVersion: input.workflow.version,
      workflowId: input.workflowId,
      runId: input.runId,
      status: "started",
    }
    this.state.children.push(record)

    await this.save()
    return childHandle(record)
  }

  async loadInstance(ref: InstanceRef): Promise<PersistedInstance | null> {
    return clone(this.state.instances[instanceKey(ref)] ?? null)
  }

  async listInstances(): Promise<PersistedInstance[]> {
    return clone(Object.values(this.state.instances))
  }

  async listSignals(): Promise<SignalRecord[]> {
    return clone(this.state.signals)
  }

  async listChildren(): Promise<ChildRecord[]> {
    return clone(this.state.children)
  }

  async appendSignal(input: {
    workflowId: string
    runId: string
    type: string
    payload: JsonValue
    receivedAt: string
  }): Promise<SignalRecord> {
    const instance = this.state.instances[instanceKey(input)]
    if (!instance || instance.status !== "running") {
      throw new Error(`Cannot signal non-running workflow: ${input.workflowId}/${input.runId}`)
    }

    const signalRecord: SignalRecord = {
      signalId: `signal-${this.state.nextSignalId++}`,
      workflowId: input.workflowId,
      runId: input.runId,
      type: input.type,
      payload: clone(input.payload),
      receivedAt: input.receivedAt,
    }
    this.state.signals.push(signalRecord)
    await this.save()
    return clone(signalRecord)
  }

  async getOrReserveEffect(input: {
    workflowId: string
    runId: string
    activationId: string
    key: string
  }): Promise<
    | { status: "reserved"; effectId: string; idempotencyKey: string }
    | { status: "completed"; result: JsonValue }
    | { status: "failed"; error: SerializedError }
  > {
    const instance = this.requireInstance(input)
    const existing = instance.effects.find(
      (effect) => effect.activationId === input.activationId && effect.key === input.key,
    )

    if (existing?.status === "completed") {
      return { status: "completed", result: clone(existing.result ?? null) }
    }

    if (existing?.status === "failed") {
      return { status: "failed", error: clone(existing.error ?? { message: "Effect failed" }) }
    }

    if (existing) {
      return {
        status: "reserved",
        effectId: existing.effectId,
        idempotencyKey: existing.idempotencyKey,
      }
    }

    const effect: EffectRecord = {
      effectId: `effect-${this.state.nextEffectId++}`,
      activationId: input.activationId,
      key: input.key,
      idempotencyKey: `${input.workflowId}/${input.runId}/${input.activationId}/${input.key}`,
      status: "pending",
    }
    instance.effects.push(effect)
    await this.save()

    return {
      status: "reserved",
      effectId: effect.effectId,
      idempotencyKey: effect.idempotencyKey,
    }
  }

  async completeEffect(input: {
    workflowId: string
    runId: string
    effectId: string
    result: JsonValue
  }): Promise<void> {
    const instance = this.requireInstance(input)
    const effect = instance.effects.find((candidate) => candidate.effectId === input.effectId)
    if (!effect) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }

    effect.status = "completed"
    effect.result = clone(input.result)
    delete effect.error
    await this.save()
  }

  async failEffect(input: {
    workflowId: string
    runId: string
    effectId: string
    error: SerializedError
  }): Promise<void> {
    const instance = this.requireInstance(input)
    const effect = instance.effects.find((candidate) => candidate.effectId === input.effectId)
    if (!effect) {
      throw new Error(`Unknown effect: ${input.effectId}`)
    }

    effect.status = "failed"
    effect.error = clone(input.error)
    await this.save()
  }

  async commitCheckpoint(input: CommitCheckpointInput): Promise<{ ok: boolean; sequence: number }> {
    const instance = this.state.instances[instanceKey(input)]
    if (!instance || instance.status !== "running") {
      return { ok: false, sequence: -1 }
    }

    if (instance.sequence !== input.expectedSequence) {
      return { ok: false, sequence: instance.sequence }
    }

    const nextSequence = instance.sequence + 1
    const signalToConsume = input.consumeSignalId
      ? this.state.signals.find(
          (signalRecord) =>
            signalRecord.signalId === input.consumeSignalId &&
            signalRecord.workflowId === input.workflowId &&
            signalRecord.runId === input.runId &&
            signalRecord.consumedBySequence === undefined,
        )
      : undefined

    if (input.consumeSignalId && !signalToConsume) {
      return { ok: false, sequence: instance.sequence }
    }

    const childToConsume = input.consumeChildRecordId
      ? this.state.children.find(
          (record) =>
            record.childRecordId === input.consumeChildRecordId &&
            record.parentWorkflowId === input.workflowId &&
            record.parentRunId === input.runId &&
            record.deliveredBySequence === undefined,
        )
      : undefined

    if (input.consumeChildRecordId && !childToConsume) {
      return { ok: false, sequence: instance.sequence }
    }

    instance.sequence = nextSequence
    instance.waits = clone(input.waits)
    instance.updatedAt = input.now

    if (input.next.status === "running") {
      instance.status = "running"
      instance.common = clone(input.next.common)
      instance.phase = clone(input.next.phase)
      delete instance.output
      delete instance.error
      delete instance.cancelReason
    } else if (input.next.status === "completed") {
      instance.status = "completed"
      instance.output = clone(toJson(input.next.output))
      delete instance.common
      delete instance.phase
      delete instance.error
      delete instance.cancelReason
    } else if (input.next.status === "canceled") {
      instance.status = "canceled"
      instance.cancelReason = input.next.reason
      delete instance.common
      delete instance.phase
      delete instance.output
      delete instance.error
    } else {
      instance.status = "failed"
      instance.error = clone(input.next.error)
      delete instance.common
      delete instance.phase
      delete instance.output
      delete instance.cancelReason
    }

    if (signalToConsume) {
      signalToConsume.consumedBySequence = nextSequence
    }

    if (childToConsume) {
      childToConsume.deliveredBySequence = nextSequence
    }

    if (instance.parent && instance.status !== "running") {
      const childRecord = this.state.children.find(
        (record) => record.childRecordId === instance.parent?.childRecordId,
      )
      if (childRecord && childRecord.status === "started") {
        childRecord.completedAt = input.now
        if (instance.status === "completed") {
          childRecord.status = "completed"
          childRecord.output = clone(instance.output ?? null)
        } else {
          childRecord.status = "failed"
          childRecord.error =
            instance.status === "failed"
              ? clone(instance.error ?? { message: "Child failed" })
              : { message: instance.cancelReason ?? "Child canceled" }
        }
      }
    }

    await this.save()
    return { ok: true, sequence: nextSequence }
  }

  async readOutput<W extends AnyWorkflow>(handle: ChildHandle<W>): Promise<OutputOf<W>> {
    const instance = this.state.instances[instanceKey(handle)]
    if (!instance) {
      throw new Error(`Unknown child workflow: ${handle.workflowId}/${handle.runId}`)
    }

    if (instance.status !== "completed") {
      throw new Error(`Child workflow is not complete: ${handle.workflowId}/${handle.runId}`)
    }

    return clone(instance.output) as OutputOf<W>
  }

  private requireInstance(ref: InstanceRef): PersistedInstance {
    const instance = this.state.instances[instanceKey(ref)]
    if (!instance) {
      throw new Error(`Unknown workflow instance: ${ref.workflowId}/${ref.runId}`)
    }
    return instance
  }

  private load(): Store {
    if (!existsSync(this.filePath)) {
      return emptyStore()
    }

    return {
      ...emptyStore(),
      ...JSON.parse(readFileSync(this.filePath, "utf8")),
    }
  }

  private async save(): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true })
    const tmpPath = `${this.filePath}.${process.pid}.${Date.now()}.tmp`
    await writeFile(tmpPath, JSON.stringify(this.state, null, 2))
    await rename(tmpPath, this.filePath)
  }
}

export class DurableRuntime {
  private readonly workflows = new Map<string, AnyWorkflow>()
  private readonly clock: () => Date

  constructor(
    private readonly provider: JsonFileDurabilityProvider,
    options: {
      clock?: () => Date
      workflows?: AnyWorkflow[]
    } = {},
  ) {
    this.clock = options.clock ?? (() => new Date())
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
    options: { workflowId?: string; runId?: string } = {},
  ): Promise<InstanceRef> {
    this.registerWorkflows([workflow])
    const parsedInput = workflow.input.parse(input)
    const startCommand = workflow.initial(parsedInput)
    const now = this.now()
    const instance = this.initialInstance(
      workflow,
      options.workflowId ?? `${workflow.name}-${randomUUID()}`,
      options.runId ?? "run-1",
      startCommand,
      now,
    )

    return this.provider.createInstance({
      workflow,
      workflowId: instance.workflowId,
      runId: instance.runId,
      common: instance.common!,
      phase: instance.phase!,
      waits: instance.waits,
      now,
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

    while (activations < maxActivations) {
      const activation = await this.nextActivation()
      if (!activation) {
        break
      }

      await this.runActivation(activation)
      activations += 1
    }

    return { activations }
  }

  private async nextActivation(): Promise<ReadyActivation | null> {
    const instances = await this.provider.listInstances()
    const signals = await this.provider.listSignals()
    const children = await this.provider.listChildren()
    const now = this.now()
    const ready: ReadyActivation[] = []

    for (const instance of instances) {
      if (instance.status !== "running") {
        continue
      }

      const workflow = this.workflows.get(instance.workflowName)
      if (!workflow) {
        continue
      }

      for (const wait of instance.waits) {
        if (wait.kind === "run") {
          ready.push({
            kind: "run",
            workflow,
            instance,
            activationId: activationId(instance, "run", wait.name),
            sort: [wait.readyAt, "run", wait.name, instance.workflowId],
          })
        } else if (wait.kind === "signal") {
          const signalRecord = signals
            .filter(
              (candidate) =>
                candidate.workflowId === instance.workflowId &&
                candidate.runId === instance.runId &&
                candidate.type === wait.type &&
                candidate.consumedBySequence === undefined,
            )
            .sort(compareSignals)[0]

          if (signalRecord) {
            const definition = this.waitDefinition(workflow, instance, wait)
            ready.push({
              kind: "event",
              workflow,
              instance,
              activationId: activationId(instance, "signal", signalRecord.signalId),
              waitName: wait.name,
              wait: definition,
              event: {
                kind: "signal",
                signalId: signalRecord.signalId,
                payload: signalRecord.payload,
                occurredAt: signalRecord.receivedAt,
                consumeSignalId: signalRecord.signalId,
              },
              sort: [signalRecord.receivedAt, "signal", wait.name, signalRecord.signalId],
            })
          }
        } else if (wait.kind === "timer" && wait.fireAt <= now) {
          const definition = this.waitDefinition(workflow, instance, wait)
          ready.push({
            kind: "event",
            workflow,
            instance,
            activationId: activationId(instance, "timer", `${wait.name}:${wait.fireAt}`),
            waitName: wait.name,
            wait: definition,
            event: {
              kind: "timer",
              firedAt: now,
              occurredAt: wait.fireAt,
            },
            sort: [wait.fireAt, "timer", wait.name, `${wait.name}:${wait.fireAt}`],
          })
        } else if (wait.kind === "child") {
          const childRecord = children.find(
            (record) =>
              record.workflowId === wait.workflowId &&
              record.runId === wait.runId &&
              record.status !== "started" &&
              record.deliveredBySequence === undefined,
          )

          if (childRecord) {
            const definition = this.waitDefinition(workflow, instance, wait)
            ready.push({
              kind: "event",
              workflow,
              instance,
              activationId: activationId(instance, "child", childRecord.childRecordId),
              waitName: wait.name,
              wait: definition,
              event: {
                kind: "child",
                childRecordId: childRecord.childRecordId,
                occurredAt: childRecord.completedAt ?? now,
                event:
                  childRecord.status === "completed"
                    ? { ok: true, output: clone(childRecord.output ?? null) }
                    : { ok: false, error: clone(childRecord.error ?? { message: "Child failed" }) },
              },
              sort: [
                childRecord.completedAt ?? now,
                "child",
                wait.name,
                childRecord.childRecordId,
              ],
            })
          }
        }
      }
    }

    return ready.sort(compareActivations)[0] ?? null
  }

  private async runActivation(activation: ReadyActivation): Promise<void> {
    const latest = await this.requireInstance(activation.instance)
    if (latest.status !== "running" || latest.sequence !== activation.instance.sequence) {
      return
    }

    const workflow = activation.workflow
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
    const ctx = this.contextFor(workflow, latest, activation.activationId)
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
      if (event.kind === "signal") {
        if (activation.wait.kind !== "signal") {
          throw new Error("Signal delivered to non-signal wait")
        }
        consumeSignalId = event.consumeSignalId
        transition = await activation.wait.handler({
          ctx,
          common,
          data,
          event: activation.wait.schema.parse(event.payload),
        })
      } else if (event.kind === "timer") {
        transition = await callWaitHandler(activation.wait, {
          ctx,
          common,
          data,
          event: { firedAt: event.firedAt },
        })
      } else {
        consumeChildRecordId = event.childRecordId
        transition = await callWaitHandler(activation.wait, {
          ctx,
          common,
          data,
          event: event.event,
        })
      }
    }

    const next = this.applyTransition(workflow, latest, transition)
    const waits = next.status === "running" ? this.materializeWaits(workflow, latest, next) : []
    await this.provider.commitCheckpoint({
      workflowId: latest.workflowId,
      runId: latest.runId,
      expectedSequence: latest.sequence,
      activationId: activation.activationId,
      next,
      waits,
      now: this.now(),
      consumeSignalId,
      consumeChildRecordId,
    })
  }

  private contextFor(
    workflow: AnyWorkflow,
    instance: PersistedInstance,
    currentActivationId: string,
  ): DurableContext {
    return {
      now: () => this.now(),
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
          const now = this.now()
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
            workflow: childWorkflow,
            workflowId: childInstance.workflowId,
            runId: childInstance.runId,
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
          const handle = await this.contextFor(workflow, instance, currentActivationId).child.start(
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

function emptyStore(): Store {
  return {
    instances: {},
    signals: [],
    children: [],
    nextSignalId: 1,
    nextEffectId: 1,
    nextChildId: 1,
  }
}

function instanceKey(ref: InstanceRef): string {
  return `${ref.workflowId}:${ref.runId}`
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

function childHandle(record: ChildRecord): ChildHandle {
  return {
    workflowName: record.workflowName,
    workflowVersion: record.workflowVersion,
    workflowId: record.workflowId,
    runId: record.runId,
  }
}

function activationId(instance: PersistedInstance, kind: string, eventId: string): string {
  return `${instance.workflowId}/${instance.runId}/${instance.sequence}/${kind}/${eventId}`
}

function compareSignals(left: SignalRecord, right: SignalRecord): number {
  return (
    left.receivedAt.localeCompare(right.receivedAt) ||
    left.type.localeCompare(right.type) ||
    left.signalId.localeCompare(right.signalId)
  )
}

function compareActivations(left: ReadyActivation, right: ReadyActivation): number {
  for (let index = 0; index < left.sort.length; index += 1) {
    const compared = left.sort[index].localeCompare(right.sort[index])
    if (compared !== 0) {
      return compared
    }
  }
  return 0
}

function callWaitHandler(
  wait: WaitDefinition,
  args: HandlerArgs<any>,
): Promise<TransitionCommand> | TransitionCommand {
  return wait.handler(args)
}

function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
    }
  }

  return { message: String(error) }
}

function toJson(value: unknown): JsonValue {
  if (value === undefined) {
    return null
  }

  return clone(value) as JsonValue
}

function toJsonObject(value: unknown): JsonObject {
  if (!isPlainObject(value)) {
    throw new Error("Expected a JSON object")
  }

  return toJson(value) as JsonObject
}

function clone<T>(value: T): T {
  if (value === undefined) {
    return value
  }

  return JSON.parse(JSON.stringify(value)) as T
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function safeId(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "_")
}

// Make it easy to create a store path before the first async provider write.
export function ensureStoreDir(filePath: string): void {
  mkdirSync(dirname(filePath), { recursive: true })
  if (!existsSync(filePath)) {
    writeFileSync(filePath, JSON.stringify(emptyStore(), null, 2))
  }
}
