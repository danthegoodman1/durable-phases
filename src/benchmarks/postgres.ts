import pg from "pg"
import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import { randomUUID } from "node:crypto"
import { DurableRuntime, PostgresDurabilityProvider } from "../durable.js"
import {
  activityCount,
  createBenchmarkCounters,
  createBenchmarkWorkflows,
  mixedActionCount,
  verifyBenchmarkCounters,
  verifyBenchmarkOutputs,
  type BenchmarkCounters,
} from "./workload.js"

const { Pool } = pg

export type PostgresBenchmarkOptions = {
  connectionString: string
  schema: string
  poolSize: number
  workflows: number
  workers: number
  shards: number
  activationConcurrency: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  keepSchema: boolean
  json: boolean
}

export type PostgresBenchmarkResult = {
  backend: "postgres"
  options: Omit<PostgresBenchmarkOptions, "connectionString"> & {
    connectionString: string
  }
  elapsedMs: number
  setupMs: number
  processingMs: number
  verifyMs: number
  rounds: number
  activations: number
  expectedActivations: number
  completedWorkflows: number
  committedWorkers: number
  mixedActions: number
  activationsPerSecond: number
  mixedActionsPerSecond: number
  workflowsPerSecond: number
  processingActivationsPerSecond: number
  processingMixedActionsPerSecond: number
  processingWorkflowsPerSecond: number
  counters: BenchmarkCounters
}

const defaultOptions: PostgresBenchmarkOptions = {
  connectionString:
    process.env.DURABLE_POSTGRES_URL ?? "postgresql://durable:durable@127.0.0.1:55432/durable",
  schema: `durable_bench_${randomUUID().replaceAll("-", "_")}`,
  poolSize: 24,
  workflows: 250,
  workers: 4,
  shards: 4,
  activationConcurrency: 4,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  keepSchema: false,
  json: false,
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running Postgres durability benchmark with ${options.workflows} workflows, ${options.workers} workers, ${options.shards} shards, activation concurrency ${options.activationConcurrency}, pool size ${options.poolSize}, schema ${options.schema}...\n\n`,
    )
  }
  const result = await runPostgresBenchmark(options)
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } else {
    printResult(result)
  }
}

export async function runPostgresBenchmark(
  options: PostgresBenchmarkOptions,
): Promise<PostgresBenchmarkResult> {
  const counters = createBenchmarkCounters()
  const { ParentWorkflow, workflows } = createBenchmarkWorkflows(counters, {
    activityDelayMs: options.activityDelayMs,
  })
  const pool = new Pool({
    connectionString: options.connectionString,
    max: options.poolSize,
  })
  const providers: PostgresDurabilityProvider[] = []
  let schemaInitialized = false

  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const runtimes: DurableRuntime[] = []

  let rounds = 0
  let activations = 0
  const expectedActivations = options.workflows * 5
  const setupStartedAt = performance.now()
  let setupFinishedAt = setupStartedAt
  let processingStartedAt = setupStartedAt
  let processingFinishedAt = setupStartedAt

  try {
    for (let workerIndex = 0; workerIndex < options.workers; workerIndex += 1) {
      const provider = await PostgresDurabilityProvider.create({
        pool,
        schema: options.schema,
      })
      schemaInitialized = true
      providers.push(provider)
      runtimes.push(
        new DurableRuntime(provider, {
          clock,
          workflows,
          workerId: `bench-worker-${workerIndex}`,
          shardCount: options.shards,
          dispatchShardIds: dispatchShardIdsForWorker(
            workerIndex,
            options.workers,
            options.shards,
          ),
          maxConcurrentActivations: options.activationConcurrency,
          dispatchLeaseMs: 30_000,
          activationLeaseMs: 30_000,
        }),
      )
    }

    for (let index = 0; index < options.workflows; index += 1) {
      const ref = await runtimes[0]!.start(
        ParentWorkflow,
        { index },
        { workflowId: `bench-parent-${index}` },
      )
      counters.workflowStarts += 1
      await runtimes[0]!.signal(
        ParentWorkflow,
        ref,
        "finish",
        { signalValue: index + 1_000 },
      )
      counters.signals += 1
    }

    setupFinishedAt = performance.now()
    processingStartedAt = setupFinishedAt

    for (; rounds < options.maxRounds; rounds += 1) {
      const drainResults = await Promise.all(
        runtimes.map((runtime) => runtime.drain({ maxActivations: options.batch })),
      )
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

    const instances = await providers[0]!.listInstances()
    const completedWorkflows = instances.filter(
      (instance) => instance.workflowName === ParentWorkflow.name && instance.status === "completed",
    ).length
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    verifyBenchmarkOutputs(instances, options.workflows)
    verifyBenchmarkCounters(counters, options.workflows)
    const claims = await providers[0]!.listActivationClaims()
    const committedWorkers = new Set(
      claims
        .filter((claim) => claim.completedBySequence !== undefined)
        .map((claim) => claim.ownerId)
        .filter((ownerId): ownerId is string => Boolean(ownerId)),
    ).size
    const mixedActions = mixedActionCount(counters)
    const verifyFinishedAt = performance.now()
    const setupMs = setupFinishedAt - setupStartedAt
    const processingMs = processingFinishedAt - processingStartedAt
    const verifyMs = verifyFinishedAt - verifyStartedAt
    const elapsedMs = verifyFinishedAt - setupStartedAt
    const elapsedSeconds = elapsedMs / 1_000
    const processingSeconds = Math.max(processingMs / 1_000, Number.EPSILON)

    return {
      backend: "postgres",
      options: {
        ...options,
        connectionString: redactConnectionString(options.connectionString),
      },
      elapsedMs,
      setupMs,
      processingMs,
      verifyMs,
      rounds: rounds + 1,
      activations,
      expectedActivations,
      completedWorkflows,
      committedWorkers,
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
    if (!options.keepSchema && schemaInitialized && providers[0]) {
      await providers[0].dropSchema().catch(() => undefined)
    }
    await Promise.all(providers.map((provider) => provider.close()))
    await pool.end()
  }
}

function parseArgs(args: string[]): PostgresBenchmarkOptions {
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
    } else if (flag === "--connection-string") {
      options.connectionString = nextValue()
    } else if (flag === "--schema") {
      options.schema = nextValue()
    } else if (flag === "--pool-size") {
      options.poolSize = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workflows") {
      options.workflows = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workers") {
      options.workers = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--shards") {
      options.shards = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activation-concurrency") {
      options.activationConcurrency = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activity-delay-ms") {
      options.activityDelayMs = parseNonNegativeInteger(nextValue(), flag)
    } else if (flag === "--batch") {
      options.batch = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--max-rounds") {
      options.maxRounds = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--keep-schema") {
      options.keepSchema = true
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

function parseNonNegativeInteger(value: string, flag: string): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${flag} must be a non-negative integer`)
  }
  return parsed
}

function printHelp(): void {
  process.stdout.write(`Postgres durability benchmark

Runs the shared real workflow benchmark against the Postgres durability provider.

Options:
  --connection-string <url>
                    Postgres connection string. Default: DURABLE_POSTGRES_URL or local Docker.
  --schema <name>   Isolated schema to create/drop. Default: generated durable_bench_* schema.
  --pool-size <n>   Shared pg pool size. Default: ${defaultOptions.poolSize}
  --workflows <n>   Parent workflow count. Default: ${defaultOptions.workflows}
  --workers <n>     Logical in-process worker count. Default: ${defaultOptions.workers}
  --shards <n>      Dispatch shard count. Default: ${defaultOptions.shards}
  --activation-concurrency <n>
                    Max concurrent activations per worker. Default: ${defaultOptions.activationConcurrency}
  --activity-delay-ms <n>
                    Async delay inside each activity. Default: ${defaultOptions.activityDelayMs}
  --batch <n>       Max activations per worker drain. Default: ${defaultOptions.batch}
  --max-rounds <n>  Safety cap for drain rounds. Default: ${defaultOptions.maxRounds}
  --keep-schema     Keep the benchmark schema for inspection.
  --json            Print machine-readable JSON.
`)
}

function printResult(result: PostgresBenchmarkResult): void {
  const activityTotal = activityCount(result.counters)
  process.stdout.write(`Postgres durability benchmark
  workflows: ${result.options.workflows}
  workers: ${result.options.workers} logical in-process workers
  committed workers: ${result.committedWorkers}
  shards: ${result.options.shards}
  activation concurrency: ${result.options.activationConcurrency} per worker
  activity delay: ${formatMs(result.options.activityDelayMs)}
  pool size: ${result.options.poolSize}
  schema: ${result.options.schema}${result.options.keepSchema ? " (kept)" : ""}
  batch: ${result.options.batch}
  rounds: ${result.rounds}
  elapsed: ${formatMs(result.elapsedMs)} (${formatMs(result.setupMs)} setup, ${formatMs(result.processingMs)} processing, ${formatMs(result.verifyMs)} verify)

End-to-end throughput:
  workflows/sec: ${formatRate(result.workflowsPerSecond)}
  activations/sec: ${formatRate(result.activationsPerSecond)} (${result.activations} activations)
  mixed actions/sec: ${formatRate(result.mixedActionsPerSecond)} (${result.mixedActions} actions)

Processing-only throughput:
  workflows/sec: ${formatRate(result.processingWorkflowsPerSecond)}
  activations/sec: ${formatRate(result.processingActivationsPerSecond)} (${result.activations} activations)
  mixed actions/sec: ${formatRate(result.processingMixedActionsPerSecond)} (${result.mixedActions} actions)

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

function redactConnectionString(connectionString: string): string {
  return connectionString.replace(/:\/\/([^:/?#]+):([^@/?#]+)@/, "://$1:***@")
}

function formatMs(value: number): string {
  return `${value.toFixed(1)} ms`
}

function formatRate(value: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: 0,
  })
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
