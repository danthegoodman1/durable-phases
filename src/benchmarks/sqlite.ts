import { mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { performance } from "node:perf_hooks"
import { z } from "zod"
import {
  child,
  complete,
  defineWorkflow,
  DurableRuntime,
  go,
  phase,
  signal,
  SqliteDurabilityProvider,
  start,
  timer,
} from "../durable.js"

type BenchmarkOptions = {
  workflows: number
  workers: number
  shards: number
  batch: number
  maxRounds: number
  keepDb: boolean
  json: boolean
}

type BenchmarkCounters = {
  workflowStarts: number
  signals: number
  childStarts: number
  childCompletions: number
  timerHandlers: number
  bootActivities: number
  childActivities: number
  finishActivities: number
}

type BenchmarkResult = {
  options: BenchmarkOptions
  elapsedMs: number
  rounds: number
  activations: number
  completedWorkflows: number
  committedWorkers: number
  mixedActions: number
  activationsPerSecond: number
  mixedActionsPerSecond: number
  workflowsPerSecond: number
  counters: BenchmarkCounters
  dbPath?: string
  dbBytes?: number
}

const defaultOptions: BenchmarkOptions = {
  workflows: 250,
  workers: 4,
  shards: 4,
  batch: 32,
  maxRounds: 10_000,
  keepDb: false,
  json: false,
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running SQLite durability benchmark with ${options.workflows} workflows, ${options.workers} workers, ${options.shards} shards...\n\n`,
    )
  }
  const result = await runSqliteBenchmark(options)
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } else {
    printResult(result)
  }
}

async function runSqliteBenchmark(options: BenchmarkOptions): Promise<BenchmarkResult> {
  const tempDir = await mkdtemp(join(tmpdir(), "durable-bench-"))
  const dbPath = join(tempDir, "bench.sqlite")
  const counters: BenchmarkCounters = {
    workflowStarts: 0,
    signals: 0,
    childStarts: 0,
    childCompletions: 0,
    timerHandlers: 0,
    bootActivities: 0,
    childActivities: 0,
    finishActivities: 0,
  }
  const providers: SqliteDurabilityProvider[] = []

  const ChildWorkflow = defineWorkflow({
    name: "bench_child",
    version: 1,
    input: z.object({ index: z.number() }),
    output: z.object({ childValue: z.number() }),
    common: z.object({ index: z.number() }),
    initial(input) {
      return start({ common: { index: input.index }, phase: "run", data: {} })
    },
    phases: {
      run: phase({
        run: async ({ ctx, common }) => {
          const childValue = await ctx.activity("child_activity", () => {
            counters.childActivities += 1
            return common.index * 10
          })
          return complete({ childValue })
        },
      }),
    },
  })

  const ParentWorkflow = defineWorkflow({
    name: "bench_parent",
    version: 1,
    input: z.object({ index: z.number() }),
    output: z.object({
      index: z.number(),
      childValue: z.number(),
      signalValue: z.number(),
      finished: z.boolean(),
    }),
    common: z.object({ index: z.number() }),
    initial(input) {
      return start({ common: { index: input.index }, phase: "boot", data: {} })
    },
    phases: {
      boot: phase({
        run: async ({ ctx, common }) => {
          await ctx.activity("boot_activity", () => {
            counters.bootActivities += 1
            return true
          })
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
            async ({ event }) => {
              counters.childCompletions += 1
              return go("waiting_signal", {
                childValue: event.ok ? event.output.childValue : -1,
              })
            },
          ),
        },
      }),
      waiting_signal: phase({
        state: z.object({ childValue: z.number() }),
        on: {
          finish: signal(z.object({ signalValue: z.number() }), async ({ ctx, data, event }) =>
            go("waiting_timer", {
              childValue: data.childValue,
              signalValue: event.signalValue,
              wakeAt: ctx.now(),
            }),
          ),
        },
      }),
      waiting_timer: phase({
        state: z.object({
          childValue: z.number(),
          signalValue: z.number(),
          wakeAt: z.string(),
        }),
        on: {
          finish_due: timer(
            ({ data }) => data.wakeAt,
            async ({ ctx, common, data }) => {
              counters.timerHandlers += 1
              await ctx.activity("finish_activity", () => {
                counters.finishActivities += 1
                return true
              })
              return complete({
                index: common.index,
                childValue: data.childValue,
                signalValue: data.signalValue,
                finished: true,
              })
            },
          ),
        },
      }),
    },
  })

  const workflows = [ParentWorkflow, ChildWorkflow]
  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const runtimes = Array.from({ length: options.workers }, (_value, workerIndex) => {
    const provider = new SqliteDurabilityProvider(dbPath)
    providers.push(provider)
    return new DurableRuntime(provider, {
      clock,
      workflows,
      workerId: `bench-worker-${workerIndex}`,
      shardCount: options.shards,
      dispatchShardIds: dispatchShardIdsForWorker(workerIndex, options.workers, options.shards),
      dispatchLeaseMs: 30_000,
      activationLeaseMs: 30_000,
    })
  })

  const startedAt = performance.now()
  let rounds = 0
  let activations = 0
  const signaled = new Set<string>()

  try {
    for (let index = 0; index < options.workflows; index += 1) {
      await runtimes[0].start(
        ParentWorkflow,
        { index },
        { workflowId: `bench-parent-${index}` },
      )
      counters.workflowStarts += 1
    }

    for (; rounds < options.maxRounds; rounds += 1) {
      const drainResults = await Promise.all(
        runtimes.map((runtime) => runtime.drain({ maxActivations: options.batch })),
      )
      activations += drainResults.reduce((total, result) => total + result.activations, 0)

      const instances = await providers[0].listInstances()
      const waitingForSignal = instances.filter(
        (instance) =>
          instance.workflowName === ParentWorkflow.name &&
          instance.status === "running" &&
          instance.phase?.name === "waiting_signal" &&
          !signaled.has(instance.workflowId),
      )

      for (const instance of waitingForSignal) {
        signaled.add(instance.workflowId)
        const index = Number(instance.workflowId.split("-").at(-1))
        await runtimes[0].signal(
          ParentWorkflow,
          { workflowId: instance.workflowId, runId: instance.runId },
          "finish",
          { signalValue: index + 1_000 },
        )
        counters.signals += 1
      }

      const completedWorkflows = instances.filter(
        (instance) => instance.workflowName === ParentWorkflow.name && instance.status === "completed",
      ).length
      if (completedWorkflows === options.workflows) {
        break
      }

      now = new Date(now.getTime() + 1)
    }

    const elapsedMs = performance.now() - startedAt
    const instances = await providers[0].listInstances()
    const completedWorkflows = instances.filter(
      (instance) => instance.workflowName === ParentWorkflow.name && instance.status === "completed",
    ).length
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    verifyCounters(counters, options.workflows)
    const claims = await providers[0].listActivationClaims()
    const committedWorkers = new Set(
      claims
        .filter((claim) => claim.completedBySequence !== undefined)
        .map((claim) => claim.ownerId)
        .filter((ownerId): ownerId is string => Boolean(ownerId)),
    ).size
    const mixedActions =
      counters.workflowStarts +
      counters.signals +
      counters.childStarts +
      counters.childCompletions +
      counters.timerHandlers +
      counters.bootActivities +
      counters.childActivities +
      counters.finishActivities
    const elapsedSeconds = elapsedMs / 1_000

    return {
      options,
      elapsedMs,
      rounds: rounds + 1,
      activations,
      completedWorkflows,
      committedWorkers,
      mixedActions,
      activationsPerSecond: activations / elapsedSeconds,
      mixedActionsPerSecond: mixedActions / elapsedSeconds,
      workflowsPerSecond: completedWorkflows / elapsedSeconds,
      counters,
      dbPath: options.keepDb ? dbPath : undefined,
      dbBytes: options.keepDb ? await sqliteStoreBytes(dbPath) : undefined,
    }
  } finally {
    for (const provider of providers) {
      provider.close()
    }
    if (!options.keepDb) {
      await rm(tempDir, { force: true, maxRetries: 3, recursive: true, retryDelay: 10 })
    }
  }
}

function parseArgs(args: string[]): BenchmarkOptions {
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
    } else if (flag === "--workflows") {
      options.workflows = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workers") {
      options.workers = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--shards") {
      options.shards = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--batch") {
      options.batch = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--max-rounds") {
      options.maxRounds = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--keep-db") {
      options.keepDb = true
    } else if (flag === "--json") {
      options.json = true
    } else {
      throw new Error(`Unknown benchmark option: ${flag}`)
    }
  }
  return options
}

function parsePositiveInteger(value: string, flag: string): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`)
  }
  return parsed
}

function printHelp(): void {
  process.stdout.write(`SQLite durability benchmark

Runs real TypeScript workflows against the SQLite durability provider.

Options:
  --workflows <n>   Parent workflow count. Default: ${defaultOptions.workflows}
  --workers <n>     Logical in-process worker count. Default: ${defaultOptions.workers}
  --shards <n>      Dispatch shard count. Default: ${defaultOptions.shards}
  --batch <n>       Max activations per worker drain. Default: ${defaultOptions.batch}
  --max-rounds <n>  Safety cap for drain rounds. Default: ${defaultOptions.maxRounds}
  --keep-db         Keep the temporary SQLite database and print its path.
  --json            Print machine-readable JSON.
`)
}

function printResult(result: BenchmarkResult): void {
  const activityTotal =
    result.counters.bootActivities +
    result.counters.childActivities +
    result.counters.finishActivities
  process.stdout.write(`SQLite durability benchmark
  workflows: ${result.options.workflows}
  workers: ${result.options.workers} logical in-process workers
  committed workers: ${result.committedWorkers}
  shards: ${result.options.shards}
  batch: ${result.options.batch}
  rounds: ${result.rounds}
  elapsed: ${formatMs(result.elapsedMs)}

Throughput:
  workflows/sec: ${formatRate(result.workflowsPerSecond)}
  activations/sec: ${formatRate(result.activationsPerSecond)} (${result.activations} activations)
  mixed actions/sec: ${formatRate(result.mixedActionsPerSecond)} (${result.mixedActions} actions)

Action breakdown:
  workflow starts: ${result.counters.workflowStarts}
  signals appended: ${result.counters.signals}
  child starts: ${result.counters.childStarts}
  child completions delivered: ${result.counters.childCompletions}
  timer handlers: ${result.counters.timerHandlers}
  activities: ${activityTotal} (${result.counters.bootActivities} boot, ${result.counters.childActivities} child, ${result.counters.finishActivities} finish)
`)

  if (result.dbPath) {
    process.stdout.write(`
SQLite store kept:
  path: ${result.dbPath}
  size: ${result.dbBytes ?? 0} bytes
`)
  }
}

function verifyCounters(counters: BenchmarkCounters, expected: number): void {
  const entries: Array<[keyof BenchmarkCounters, number]> = [
    ["workflowStarts", expected],
    ["signals", expected],
    ["childStarts", expected],
    ["childCompletions", expected],
    ["timerHandlers", expected],
    ["bootActivities", expected],
    ["childActivities", expected],
    ["finishActivities", expected],
  ]
  for (const [name, value] of entries) {
    if (counters[name] !== value) {
      throw new Error(`Expected ${name} to be ${value}, got ${counters[name]}`)
    }
  }
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

async function sqliteStoreBytes(dbPath: string): Promise<number> {
  const paths = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]
  const sizes = await Promise.all(
    paths.map((path) => stat(path).then((info) => info.size).catch(() => 0)),
  )
  return sizes.reduce((total, size) => total + size, 0)
}

function formatMs(value: number): string {
  return `${value.toFixed(1)} ms`
}

function formatRate(value: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: 0,
  })
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error)
  process.stderr.write(`${message}\n`)
  process.exitCode = 1
})
