import pg from "pg"
import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import { randomUUID } from "node:crypto"
import type { Pool as PgPool, PoolClient } from "pg"
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
  activationPrefetchLimit: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  keepSchema: boolean
  profileQueries: boolean
  json: boolean
}

type QueryProfilePhase = "setup" | "processing" | "verify" | "cleanup"

type QueryProfileEntry = {
  phase: QueryProfilePhase | "all"
  count: number
  totalMs: number
  avgMs: number
  maxMs: number
  sql: string
}

export type PostgresQueryProfile = {
  totalQueries: number
  queriesPerActivation: number
  totalSqlMs: number
  avgQueryMs: number
  byPhase: Record<QueryProfilePhase, {
    totalQueries: number
    queriesPerActivation: number
    totalSqlMs: number
    avgQueryMs: number
  }>
  topByTotal: QueryProfileEntry[]
  topByCount: QueryProfileEntry[]
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
  queryProfile?: PostgresQueryProfile
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
  activationPrefetchLimit: 32,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  keepSchema: false,
  profileQueries: false,
  json: false,
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running Postgres durability benchmark with ${options.workflows} workflows, ${options.workers} workers, ${options.shards} shards, activation concurrency ${options.activationConcurrency}, activation prefetch ${options.activationPrefetchLimit}, pool size ${options.poolSize}, schema ${options.schema}...\n\n`,
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
  let profilePhase: QueryProfilePhase = "setup"
  const queryProfiler = options.profileQueries
    ? installPostgresQueryProfiler(pool, () => profilePhase)
    : undefined
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
          activationPrefetchLimit: options.activationPrefetchLimit,
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
    profilePhase = "processing"
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
      activations += drainResults.reduce((total, result) => total + result.activations, 0)
      if (activations >= expectedActivations) {
        break
      }
      now = new Date(now.getTime() + 1)
    }

    processingFinishedAt = performance.now()
    profilePhase = "verify"
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

    const result: PostgresBenchmarkResult = {
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
      queryProfile: queryProfiler?.snapshot(activations),
    }
    return result
  } finally {
    profilePhase = "cleanup"
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
    } else if (flag === "--activation-prefetch-limit") {
      options.activationPrefetchLimit = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--activity-delay-ms") {
      options.activityDelayMs = parseNonNegativeInteger(nextValue(), flag)
    } else if (flag === "--batch") {
      options.batch = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--max-rounds") {
      options.maxRounds = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--keep-schema") {
      options.keepSchema = true
    } else if (flag === "--profile-queries") {
      options.profileQueries = true
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
  --activation-prefetch-limit <n>
                    Claimed activations to keep leased ahead of execution. Default: ${defaultOptions.activationPrefetchLimit}
  --activity-delay-ms <n>
                    Async delay inside each activity. Default: ${defaultOptions.activityDelayMs}
  --batch <n>       Max activations per worker drain. Default: ${defaultOptions.batch}
  --max-rounds <n>  Safety cap for drain rounds. Default: ${defaultOptions.maxRounds}
  --keep-schema     Keep the benchmark schema for inspection.
  --profile-queries
                    Include pg query counts and latency grouped by SQL fingerprint.
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
  activation prefetch limit: ${result.options.activationPrefetchLimit}
  activity delay: ${formatMs(result.options.activityDelayMs)}
  pool size: ${result.options.poolSize}
  query profiling: ${result.options.profileQueries ? "on" : "off"}
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

  if (result.queryProfile) {
    process.stdout.write(`
Query profile:
  total queries: ${result.queryProfile.totalQueries}
  queries/activation: ${result.queryProfile.queriesPerActivation.toFixed(1)}
  avg query: ${formatMs(result.queryProfile.avgQueryMs)}
`)
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

function redactConnectionString(connectionString: string): string {
  return connectionString.replace(/:\/\/([^:/?#]+):([^@/?#]+)@/, "://$1:***@")
}

function installPostgresQueryProfiler(
  pool: PgPool,
  currentPhase: () => QueryProfilePhase,
): { snapshot(activations: number): PostgresQueryProfile } {
  const stats = new Map<string, {
    count: number
    maxMs: number
    phase: QueryProfilePhase | "all"
    sql: string
    totalMs: number
  }>()

  const record = (sql: string, elapsedMs: number) => {
    const fingerprint = queryFingerprint(sql)
    for (const phase of ["all", currentPhase()] as const) {
      const key = `${phase}\0${fingerprint}`
      const existing = stats.get(key) ?? {
        count: 0,
        maxMs: 0,
        phase,
        sql: fingerprint,
        totalMs: 0,
      }
      existing.count += 1
      existing.totalMs += elapsedMs
      existing.maxMs = Math.max(existing.maxMs, elapsedMs)
      stats.set(key, existing)
    }
  }

  const wrapQuery = <T extends (...args: any[]) => any>(query: T, receiver: unknown): T =>
    function profiledQuery(...args: Parameters<T>): ReturnType<T> {
      const sql = sqlFromQueryArgs(args)
      const startedAt = performance.now()
      try {
        const result = query.apply(receiver, args)
        return Promise.resolve(result).finally(() => {
          record(sql, performance.now() - startedAt)
        }) as ReturnType<T>
      } catch (error) {
        record(sql, performance.now() - startedAt)
        throw error
      }
    } as T

  const wrapClient = (client: PoolClient): PoolClient => {
    const wrapped = client as PoolClient & { __durableProfileWrapped?: boolean }
    if (wrapped.__durableProfileWrapped) {
      return client
    }
    wrapped.query = wrapQuery(wrapped.query, wrapped)
    wrapped.__durableProfileWrapped = true
    return wrapped
  }

  const originalPoolQuery = pool.query as (...args: any[]) => any
  pool.query = wrapQuery(originalPoolQuery, pool) as PgPool["query"]

  const originalConnect = pool.connect.bind(pool) as (...args: any[]) => any
  pool.connect = function profiledConnect(...args: any[]) {
    if (typeof args[0] === "function") {
      const callback = args[0]
      return originalConnect((error: Error | undefined, client: PoolClient, done: () => void) => {
        callback(error, client ? wrapClient(client) : client, done)
      })
    }
    return Promise.resolve(originalConnect()).then(wrapClient)
  } as PgPool["connect"]

  return {
    snapshot(activations: number): PostgresQueryProfile {
      const entries = [...stats.values()].map((entry) => ({
        phase: entry.phase,
        count: entry.count,
        totalMs: round(entry.totalMs),
        avgMs: round(entry.totalMs / entry.count),
        maxMs: round(entry.maxMs),
        sql: entry.sql,
      }))
      const all = entries.filter((entry) => entry.phase === "all")
      const totals = summarizeProfileEntries(all, activations)
      return {
        ...totals,
        byPhase: {
          setup: summarizeProfileEntries(entries.filter((entry) => entry.phase === "setup"), activations),
          processing: summarizeProfileEntries(
            entries.filter((entry) => entry.phase === "processing"),
            activations,
          ),
          verify: summarizeProfileEntries(entries.filter((entry) => entry.phase === "verify"), activations),
          cleanup: summarizeProfileEntries(entries.filter((entry) => entry.phase === "cleanup"), activations),
        },
        topByTotal: [...entries]
          .sort((left, right) => right.totalMs - left.totalMs)
          .slice(0, 25),
        topByCount: [...entries]
          .sort((left, right) => right.count - left.count)
          .slice(0, 25),
      }
    },
  }
}

function summarizeProfileEntries(
  entries: QueryProfileEntry[],
  activations: number,
): {
  avgQueryMs: number
  queriesPerActivation: number
  totalQueries: number
  totalSqlMs: number
} {
  const totalQueries = entries.reduce((total, entry) => total + entry.count, 0)
  const totalSqlMs = entries.reduce((total, entry) => total + entry.totalMs, 0)
  return {
    totalQueries,
    queriesPerActivation: round(totalQueries / Math.max(activations, 1)),
    totalSqlMs: round(totalSqlMs),
    avgQueryMs: totalQueries > 0 ? round(totalSqlMs / totalQueries) : 0,
  }
}

function sqlFromQueryArgs(args: unknown[]): string {
  const query = args[0]
  if (typeof query === "string") {
    return query
  }
  if (query && typeof query === "object" && "text" in query && typeof query.text === "string") {
    return query.text
  }
  return String(query)
}

function queryFingerprint(sql: string): string {
  return sql
    .replace(/"durable_bench_[a-z0-9_]+"\./g, "SCHEMA.")
    .replace(/durable_bench_[a-z0-9_]+\./g, "SCHEMA.")
    .replace(/\s+/g, " ")
    .trim()
}

function round(value: number): number {
  return Number(value.toFixed(3))
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
