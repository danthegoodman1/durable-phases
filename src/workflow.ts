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

export class NonRetryableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = new.target.name
  }
}

export class NonRetraybleError extends NonRetryableError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = "NonRetraybleError"
  }
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
  activity<T>(
    key: string,
    fn: (ctx: ActivityContext) => Promise<T> | T,
    options?: ActivityOptions,
  ): Promise<T>
  child: {
    start<W extends AnyWorkflow>(
      key: string,
      workflow: W,
      input: InputOf<W>,
      options?: ChildOptions,
    ): Promise<ChildHandle<W>>
    cancel(handle: ChildHandle<any>): Promise<void>
  }
}

export type ActivityContext = {
  heartbeat(details?: JsonValue): Promise<void>
  heartbeatDetails?: JsonValue
  idempotencyKey: string
  attempt: number
  signal: AbortSignal
}

export type ActivityOptions = {
  durability?: "checkpoint" | "eager"
  startToCloseTimeoutMs?: number | null
  heartbeatTimeoutMs?: number | null
  retry?: ActivityRetryOptions
}

export type ActivityRetryOptions = {
  maxAttempts?: number
  maxElapsedMs?: number | null
  initialIntervalMs?: number
  maxIntervalMs?: number | null
  backoffCoefficient?: number
  nonRetryableErrorNames?: string[]
}

export type ChildOptions = {
  workflowId?: string
  durability?: "checkpoint" | "eager"
  parentClosePolicy?: "cancel" | "abandon"
  conflictPolicy?: "use_existing" | "fail" | "terminate_existing"
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

export type MigrationArgs = {
  common: JsonObject
  phase: PhaseSnapshot
  fromVersion: number
  toVersion: number
}

export type MigrationResult = {
  common?: unknown
  phase?: {
    name: string
    data: unknown
  }
}

export type MigrationDefinition = (args: MigrationArgs) => MigrationResult | Promise<MigrationResult>

export type SignalWait<Event = unknown> = {
  kind: "signal"
  schema: Schema<Event>
  handler: (args: HandlerArgs<Event>) => Promise<TransitionCommand> | TransitionCommand
}

export type TimerWait = {
  kind: "timer"
  selector: (args: { common: any; data: any }) => string | null
  handler: (args: HandlerArgs<{ firedAt: string }>) => Promise<TransitionCommand> | TransitionCommand
}

export type ChildWait = {
  kind: "child"
  selector: (args: { common: any; data: any }) => ChildHandle | null
  handler: (args: HandlerArgs<ChildEvent>) => Promise<TransitionCommand> | TransitionCommand
}

export type WaitDefinition = SignalWait<any> | TimerWait | ChildWait

export type PhaseDefinition = {
  state: Schema<any>
  mode: "run" | "on"
  run?: (args: RunArgs) => Promise<TransitionCommand> | TransitionCommand
  on?: Record<string, WaitDefinition>
}

export type QueryDefinition = {
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
  migrations?: Record<number, MigrationDefinition>
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

export function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
    }
  }

  return { message: String(error) }
}

export function isNonRetryableError(error: unknown): boolean {
  return (
    error instanceof NonRetryableError ||
    (error instanceof Error &&
      (error.name === "NonRetryableError" || error.name === "NonRetraybleError"))
  )
}

export function toJson(value: unknown): JsonValue {
  if (value === undefined) {
    return null
  }

  return clone(value) as JsonValue
}

export function toJsonObject(value: unknown): JsonObject {
  if (!isPlainObject(value)) {
    throw new Error("Expected a JSON object")
  }

  return toJson(value) as JsonObject
}

export function clone<T>(value: T): T {
  if (value === undefined) {
    return value
  }
  return JSON.parse(JSON.stringify(value)) as T
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

export function safeId(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "_")
}
