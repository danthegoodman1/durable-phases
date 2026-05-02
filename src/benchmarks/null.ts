import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import { z } from "zod"
import { DurableRuntime } from "../runtime.js"
import {
  child,
  complete,
  defineWorkflow,
  go,
  phase,
  signal,
  start,
  timer,
  type AnyWorkflow,
  type JsonValue,
} from "../workflow.js"
import type { PersistedInstance } from "../interface.js"
import {
  activityCount,
  createBenchmarkCounters,
  createBenchmarkWorkflows,
  mixedActionCount,
  verifyBenchmarkCounters,
  verifyBenchmarkOutputs,
  type BenchmarkCounters,
} from "./workload.js"
import { NullDurabilityProvider } from "./null-provider.js"

export type NullBenchmarkMode = "mixed" | "bare" | "activity" | "signal" | "timer" | "child"

export type NullBenchmarkOptions = {
  mode: NullBenchmarkMode
  workflows: number
  workflowOffset: number
  workers: number
  shards: number
  activationConcurrency: number
  activationPrefetchLimit: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  unsafeNoClone: boolean
  json: boolean
}

export type NullBenchmarkResult = {
  backend: "null"
  mode: NullBenchmarkMode
  options: NullBenchmarkOptions
  elapsedMs: number
  setupMs: number
  processingMs: number
  verifyMs: number
  rounds: number
  activations: number
  expectedActivations: number
  completedWorkflows: number
  activeWorkers: number
  mixedActions: number
  activationsPerSecond: number
  mixedActionsPerSecond: number
  workflowsPerSecond: number
  processingActivationsPerSecond: number
  processingMixedActionsPerSecond: number
  processingWorkflowsPerSecond: number
  counters: BenchmarkCounters
}

const defaultOptions: NullBenchmarkOptions = {
  mode: "mixed",
  workflows: 250,
  workflowOffset: 0,
  workers: 4,
  shards: 4,
  activationConcurrency: 4,
  activationPrefetchLimit: 32,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  unsafeNoClone: false,
  json: false,
}

async function main(): Promise<void> {
  const options = parseNullBenchmarkArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running null durability benchmark with ${options.workflows} workflows, offset ${options.workflowOffset}, ${options.mode} mode, ${options.workers} workers, ${options.shards} shards, activation concurrency ${options.activationConcurrency}, activation prefetch ${options.activationPrefetchLimit}...\n\n`,
    )
  }
  const result = await runNullBenchmark(options)
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } else {
    printResult(result)
  }
}

export async function runNullBenchmark(
  options: NullBenchmarkOptions,
): Promise<NullBenchmarkResult> {
  const counters = createBenchmarkCounters()
  const provider = new NullDurabilityProvider({ unsafeNoClone: options.unsafeNoClone })
  const workload = createNullBenchmarkWorkload(options.mode, counters, {
    activityDelayMs: options.activityDelayMs,
  })
  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const runtimes = Array.from({ length: options.workers }, (_value, workerIndex) =>
    new DurableRuntime(provider, {
      clock,
      workflows: workload.workflows,
      workerId: `bench-worker-${workerIndex}`,
      shardCount: options.shards,
      dispatchShardIds: dispatchShardIdsForWorker(workerIndex, options.workers, options.shards),
      maxConcurrentActivations: options.activationConcurrency,
      activationPrefetchLimit: options.activationPrefetchLimit,
      activationCommitBatchSize: options.batch,
      activationCommitMaxDelayMs: 0,
      dispatchLeaseMs: 30_000,
      activationLeaseMs: 30_000,
    }),
  )

  let rounds = 0
  let activations = 0
  const expectedActivations = options.workflows * workload.activationsPerWorkflow
  const activeWorkers = new Set<string>()
  const setupStartedAt = performance.now()
  let setupFinishedAt = setupStartedAt
  let processingStartedAt = setupStartedAt
  let processingFinishedAt = setupStartedAt

  try {
    for (let localIndex = 0; localIndex < options.workflows; localIndex += 1) {
      const index = options.workflowOffset + localIndex
      const ref = await runtimes[0]!.start(
        workload.RootWorkflow,
        { index },
        { workflowId: `${options.mode}-bench-${index}` },
      )
      counters.workflowStarts += 1
      if (workload.appendFinishSignal) {
        await runtimes[0]!.signal(
          workload.RootWorkflow,
          ref,
          "finish",
          { signalValue: index + 1_000 },
        )
        counters.signals += 1
      }
    }

    setupFinishedAt = performance.now()
    processingStartedAt = setupFinishedAt

    for (; rounds < options.maxRounds; rounds += 1) {
      const drainResults = await Promise.all(
        runtimes.map((runtime) =>
          runtime.drain({
            maxActivations: options.batch,
            activationPrefetchLimit: options.activationPrefetchLimit,
          }),
        ),
      )
      for (const [workerIndex, result] of drainResults.entries()) {
        if (result.activations > 0) {
          activeWorkers.add(`bench-worker-${workerIndex}`)
        }
      }
      activations += drainResults.reduce((total, result) => total + result.activations, 0)
      if (activations >= expectedActivations) {
        break
      }
      now = new Date(now.getTime() + 1)
    }

    processingFinishedAt = performance.now()
    const verifyStartedAt = processingFinishedAt
    if (activations < expectedActivations) {
      throw new Error(
        `Benchmark did not process enough activations: ${activations}/${expectedActivations} after ${rounds} rounds`,
      )
    }

    const instances = provider.listInstances()
    const completedWorkflows = instances.filter(
      (instance) => instance.workflowName === workload.RootWorkflow.name && instance.status === "completed",
    ).length
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    workload.verify(instances, options.workflows, options.workflowOffset)

    const mixedActions = workload.actionCount(counters, activations)
    const verifyFinishedAt = performance.now()
    const setupMs = setupFinishedAt - setupStartedAt
    const processingMs = processingFinishedAt - processingStartedAt
    const verifyMs = verifyFinishedAt - verifyStartedAt
    const elapsedMs = verifyFinishedAt - setupStartedAt
    const elapsedSeconds = elapsedMs / 1_000
    const processingSeconds = Math.max(processingMs / 1_000, Number.EPSILON)

    return {
      backend: "null",
      mode: options.mode,
      options,
      elapsedMs,
      setupMs,
      processingMs,
      verifyMs,
      rounds: rounds + 1,
      activations,
      expectedActivations,
      completedWorkflows,
      activeWorkers: activeWorkers.size,
      mixedActions,
      activationsPerSecond: activations / elapsedSeconds,
      mixedActionsPerSecond: mixedActions / elapsedSeconds,
      workflowsPerSecond: completedWorkflows / elapsedSeconds,
      processingActivationsPerSecond: activations / processingSeconds,
      processingMixedActionsPerSecond: mixedActions / processingSeconds,
      processingWorkflowsPerSecond: completedWorkflows / processingSeconds,
      counters,
    }
  } finally {
    provider.close()
  }
}

export function parseNullBenchmarkArgs(args: string[]): NullBenchmarkOptions {
  const options = { ...defaultOptions }
  for (let index = 0; index < args.length; index += 1) {
    const raw = args[index]
    const [flag, inlineValue] = raw.split("=", 2)
    const nextValue = () => {
      if (inlineValue !== undefined) {
        return inlineValue
      }
      index += 1
      if (index >= args.length) {
        throw new Error(`Missing value for ${flag}`)
      }
      return args[index]
    }

    if (flag === "--help" || flag === "-h") {
      printHelp()
      process.exit(0)
    } else if (flag === "--mode") {
      options.mode = parseMode(nextValue(), flag)
    } else if (flag === "--workflows") {
      options.workflows = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workflow-offset") {
      options.workflowOffset = parseNonNegativeInteger(nextValue(), flag)
    } else if (flag === "--workers") {
      options.workers = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--shards") {
      options.shards = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activation-concurrency") {
      options.activationConcurrency = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activation-prefetch-limit") {
      options.activationPrefetchLimit = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activity-delay-ms") {
      options.activityDelayMs = parseNonNegativeInteger(nextValue(), flag)
    } else if (flag === "--batch") {
      options.batch = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--max-rounds") {
      options.maxRounds = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--unsafe-no-clone") {
      options.unsafeNoClone = true
    } else if (flag === "--json") {
      options.json = true
    } else {
      throw new Error(`Unknown benchmark option: ${flag}`)
    }
  }
  return options
}

export type NullWorkload = {
  RootWorkflow: AnyWorkflow
  workflows: AnyWorkflow[]
  activationsPerWorkflow: number
  appendFinishSignal: boolean
  verify(instances: PersistedInstance[], expected: number, offset: number): void
  actionCount(counters: BenchmarkCounters, activations: number): number
}

export function createNullBenchmarkWorkload(
  mode: NullBenchmarkMode,
  counters: BenchmarkCounters,
  options: { activityDelayMs: number },
): NullWorkload {
  if (mode === "mixed") {
    const mixed = createBenchmarkWorkflows(counters, { activityDelayMs: options.activityDelayMs })
    return {
      RootWorkflow: mixed.ParentWorkflow,
      workflows: mixed.workflows,
      activationsPerWorkflow: 5,
      appendFinishSignal: true,
      verify: (instances, expected, offset) =>
        verifyBenchmarkOutputsForPrefix(instances, expected, offset, "mixed-bench"),
      actionCount: (currentCounters) => mixedActionCount(currentCounters),
    }
  }

  if (mode === "activity") {
    const RootWorkflow = defineWorkflow({
      name: "bench_activity",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number(), activity: z.boolean() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            await ctx.activity("activity", async () => {
              counters.bootActivities += 1
              return true
            })
            return complete({ index: common.index, activity: true })
          },
        }),
      },
    })
    return {
      RootWorkflow,
      workflows: [RootWorkflow],
      activationsPerWorkflow: 1,
      appendFinishSignal: false,
      verify: (instances, expected, offset) =>
        verifySimpleOutputs(instances, expected, offset, "bench_activity", "activity-bench"),
      actionCount: (currentCounters, activations) => activations + currentCounters.bootActivities,
    }
  }

  if (mode === "signal") {
    const RootWorkflow = defineWorkflow({
      name: "bench_signal",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number(), signalValue: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          state: z.object({}),
          on: {
            finish: signal(z.object({ signalValue: z.number() }), ({ common, event }) =>
              complete({ index: common.index, signalValue: event.signalValue }),
            ),
          },
        }),
      },
    })
    return {
      RootWorkflow,
      workflows: [RootWorkflow],
      activationsPerWorkflow: 1,
      appendFinishSignal: true,
      verify: (instances, expected, offset) =>
        verifySignalOutputs(instances, expected, offset),
      actionCount: (currentCounters, activations) => activations + currentCounters.signals,
    }
  }

  if (mode === "timer") {
    const RootWorkflow = defineWorkflow({
      name: "bench_timer",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number(), fired: z.boolean() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({
          common: { index: input.index },
          phase: "waiting",
          data: { fireAt: "2026-01-01T00:00:00.000Z" },
        })
      },
      phases: {
        waiting: phase({
          state: z.object({ fireAt: z.string() }),
          on: {
            due: timer(
              ({ data }) => data.fireAt,
              ({ common }) => {
                counters.timerHandlers += 1
                return complete({ index: common.index, fired: true })
              },
            ),
          },
        }),
      },
    })
    return {
      RootWorkflow,
      workflows: [RootWorkflow],
      activationsPerWorkflow: 1,
      appendFinishSignal: false,
      verify: (instances, expected, offset) =>
        verifySimpleOutputs(instances, expected, offset, "bench_timer", "timer-bench"),
      actionCount: (currentCounters, activations) => activations + currentCounters.timerHandlers,
    }
  }

  if (mode === "child") {
    const ChildWorkflow = defineWorkflow({
      name: "bench_child_only_child",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ childValue: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: ({ common }) => complete({ childValue: common.index * 10 }),
        }),
      },
    })
    const RootWorkflow = defineWorkflow({
      name: "bench_child_only_parent",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number(), childValue: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            const handle = await ctx.child.start("child", ChildWorkflow, { index: common.index })
            counters.childStarts += 1
            return go("waiting_child", { handle })
          },
        }),
        waiting_child: phase({
          state: z.object({ handle: z.any() }),
          on: {
            child_done: child(
              ({ data }) => data.handle,
              ({ common, event }) => {
                counters.childCompletions += 1
                return complete({
                  index: common.index,
                  childValue: event.ok ? event.output.childValue : -1,
                })
              },
            ),
          },
        }),
      },
    })
    return {
      RootWorkflow,
      workflows: [RootWorkflow, ChildWorkflow],
      activationsPerWorkflow: 3,
      appendFinishSignal: false,
      verify: (instances, expected, offset) =>
        verifyChildOutputs(instances, expected, offset),
      actionCount: (currentCounters, activations) =>
        activations + currentCounters.childStarts + currentCounters.childCompletions,
    }
  }

  return createBareBenchmarkWorkload()
}

function createBareBenchmarkWorkload(): NullWorkload {
  const RootWorkflow = defineWorkflow({
    name: "bench_bare",
    version: 1,
    input: z.object({ index: z.number() }),
    output: z.object({ index: z.number() }),
    common: z.object({ index: z.number() }),
    initial(input) {
      return start({ common: { index: input.index }, phase: "run", data: {} })
    },
    phases: {
      run: phase({
        run: ({ common }) => complete({ index: common.index }),
      }),
    },
  })

  return {
    RootWorkflow,
    workflows: [RootWorkflow],
    activationsPerWorkflow: 1,
    appendFinishSignal: false,
    verify: verifyBareBenchmarkOutputs,
    actionCount: (_counters, activations) => activations,
  }
}

function verifyBareBenchmarkOutputs(
  instances: PersistedInstance[],
  expected: number,
  offset: number,
): void {
  for (let localIndex = 0; localIndex < expected; localIndex += 1) {
    const index = offset + localIndex
    const instance = instances.find((record) => record.workflowId === `bare-bench-${index}`)
    if (!instance) {
      throw new Error(`Missing bare workflow ${index}`)
    }
    if (instance.status !== "completed") {
      throw new Error(`Expected bare workflow ${index} to be completed, got ${instance.status}`)
    }
    const output = instance.output as { index?: number } | undefined
    if (output?.index !== index) {
      throw new Error(`Unexpected bare output for workflow ${index}: ${JSON.stringify(output)}`)
    }
  }
}

function verifyBenchmarkOutputsForPrefix(
  instances: PersistedInstance[],
  expected: number,
  offset: number,
  prefix: string,
): void {
  const rewritten = instances.map((instance) => {
    if (!instance.workflowId.startsWith(`${prefix}-`)) {
      return instance
    }
    const numericSuffix = Number(instance.workflowId.slice(`${prefix}-`.length))
    if (!Number.isInteger(numericSuffix)) {
      return instance
    }
    return {
      ...instance,
      workflowId: `bench-parent-${numericSuffix - offset}`,
      output: rewriteBenchmarkOutput(instance.output, offset),
    }
  })
  verifyBenchmarkOutputs(rewritten, expected)
}

function verifySimpleOutputs(
  instances: PersistedInstance[],
  expected: number,
  offset: number,
  workflowName: string,
  workflowIdPrefix: string,
): void {
  for (let localIndex = 0; localIndex < expected; localIndex += 1) {
    const index = offset + localIndex
    const instance = instances.find((record) => record.workflowId === `${workflowIdPrefix}-${index}`)
    if (!instance) {
      throw new Error(`Missing ${workflowName} workflow ${index}`)
    }
    if (instance.workflowName !== workflowName || instance.status !== "completed") {
      throw new Error(`Expected ${workflowName} workflow ${index} to be completed`)
    }
    const output = instance.output as { index?: number } | undefined
    if (output?.index !== index) {
      throw new Error(`Unexpected output for ${workflowName} workflow ${index}: ${JSON.stringify(output)}`)
    }
  }
}

function verifySignalOutputs(
  instances: PersistedInstance[],
  expected: number,
  offset: number,
): void {
  for (let localIndex = 0; localIndex < expected; localIndex += 1) {
    const index = offset + localIndex
    const instance = instances.find((record) => record.workflowId === `signal-bench-${index}`)
    if (!instance || instance.status !== "completed") {
      throw new Error(`Expected signal workflow ${index} to be completed`)
    }
    const output = instance.output as { index?: number; signalValue?: number } | undefined
    if (output?.index !== index || output.signalValue !== index + 1_000) {
      throw new Error(`Unexpected signal output ${index}: ${JSON.stringify(output)}`)
    }
  }
}

function verifyChildOutputs(
  instances: PersistedInstance[],
  expected: number,
  offset: number,
): void {
  for (let localIndex = 0; localIndex < expected; localIndex += 1) {
    const index = offset + localIndex
    const instance = instances.find((record) => record.workflowId === `child-bench-${index}`)
    if (!instance || instance.status !== "completed") {
      throw new Error(`Expected child parent workflow ${index} to be completed`)
    }
    const output = instance.output as { index?: number; childValue?: number } | undefined
    if (output?.index !== index || output.childValue !== index * 10) {
      throw new Error(`Unexpected child output ${index}: ${JSON.stringify(output)}`)
    }
  }
}

function rewriteBenchmarkOutput(output: JsonValue | undefined, offset: number): JsonValue | undefined {
  if (!isObject(output)) {
    return output
  }
  return {
    ...output,
    index: typeof output.index === "number" ? output.index - offset : output.index,
    signalValue: typeof output.signalValue === "number"
      ? output.signalValue - offset
      : output.signalValue,
    childValue: typeof output.childValue === "number"
      ? output.childValue - offset * 10
      : output.childValue,
  }
}

function parseMode(value: string, flag: string): NullBenchmarkMode {
  if (
    value === "mixed" ||
    value === "bare" ||
    value === "activity" ||
    value === "signal" ||
    value === "timer" ||
    value === "child"
  ) {
    return value
  }
  throw new Error(`${flag} must be mixed, bare, activity, signal, timer, or child`)
}

function parsePositiveInteger(value: string, flag: string): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`)
  }
  return parsed
}

function parseNonNegativeInteger(value: string, flag: string): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${flag} must be a non-negative integer`)
  }
  return parsed
}

function printHelp(): void {
  process.stdout.write(`Null durability benchmark

Runs real TypeScript workflows against a benchmark-only in-memory shard provider.

Options:
  --mode <mixed|bare|activity|signal|timer|child>
                    Workload mode. Default: ${defaultOptions.mode}
  --workflows <n>   Root workflow count. Default: ${defaultOptions.workflows}
  --workflow-offset <n>
                    Starting workflow index. Default: ${defaultOptions.workflowOffset}
  --workers <n>     Logical in-process worker count. Default: ${defaultOptions.workers}
  --shards <n>      Dispatch shard count. Default: ${defaultOptions.shards}
  --activation-concurrency <n>
                    Max concurrent activations per worker. Default: ${defaultOptions.activationConcurrency}
  --activation-prefetch-limit <n>
                    Claimed activations to keep leased ahead of execution. Default: ${defaultOptions.activationPrefetchLimit}
  --activity-delay-ms <n>
                    Async delay inside each mixed-workload activity. Default: ${defaultOptions.activityDelayMs}
  --batch <n>       Max activations per worker drain. Default: ${defaultOptions.batch}
  --max-rounds <n>  Safety cap for drain rounds. Default: ${defaultOptions.maxRounds}
  --unsafe-no-clone Diagnostic only: skip null-provider defensive cloning.
  --json            Print machine-readable JSON.
`)
}

function printResult(result: NullBenchmarkResult): void {
  const activityTotal = activityCount(result.counters)
  process.stdout.write(`Null durability benchmark
  mode: ${result.mode}
  workflows: ${result.options.workflows}
  workflow offset: ${result.options.workflowOffset}
  workers: ${result.options.workers} logical in-process workers
  active workers: ${result.activeWorkers}
  shards: ${result.options.shards}
  activation concurrency: ${result.options.activationConcurrency} per worker
  activation prefetch limit: ${result.options.activationPrefetchLimit}
  activity delay: ${formatMs(result.options.activityDelayMs)}
  batch: ${result.options.batch}
  unsafe no-clone: ${result.options.unsafeNoClone ? "yes" : "no"}
  rounds: ${result.rounds}
  elapsed: ${formatMs(result.elapsedMs)} (${formatMs(result.setupMs)} setup, ${formatMs(result.processingMs)} processing, ${formatMs(result.verifyMs)} verify)

End-to-end throughput:
  workflows/sec: ${formatRate(result.workflowsPerSecond)}
  activations/sec: ${formatRate(result.activationsPerSecond)} (${result.activations} activations)
  actions/sec: ${formatRate(result.mixedActionsPerSecond)} (${result.mixedActions} actions)

Processing-only throughput:
  workflows/sec: ${formatRate(result.processingWorkflowsPerSecond)}
  activations/sec: ${formatRate(result.processingActivationsPerSecond)} (${result.activations} activations)
  actions/sec: ${formatRate(result.processingMixedActionsPerSecond)} (${result.mixedActions} actions)

Action breakdown:
  workflow starts: ${result.counters.workflowStarts}
  signals appended: ${result.counters.signals}
  child starts: ${result.counters.childStarts}
  child completions delivered: ${result.counters.childCompletions}
  timer handlers: ${result.counters.timerHandlers}
  activities: ${activityTotal} (${result.counters.bootActivities} boot, ${result.counters.childActivities} child, ${result.counters.finishActivities} finish)
`)
}

function dispatchShardIdsForWorker(
  workerIndex: number,
  workerCount: number,
  shardCount: number,
): number[] {
  const shardIds: number[] = []
  for (let shardId = 0; shardId < shardCount; shardId += 1) {
    if (shardId % workerCount === workerIndex) {
      shardIds.push(shardId)
    }
  }
  return shardIds
}

function formatMs(value: number): string {
  return `${value.toFixed(1)} ms`
}

function formatRate(value: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: 0,
  })
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isMain(): boolean {
  return process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href
}

if (isMain()) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error)
    process.stderr.write(`${message}\n`)
    process.exitCode = 1
  })
}
