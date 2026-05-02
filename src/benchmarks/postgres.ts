import pg from "pg"
import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import { randomUUID } from "node:crypto"
import { setTimeout as sleep } from "node:timers/promises"
import type { Pool as PgPool, PoolClient, PoolConfig } from "pg"
import { DurableRuntime, PostgresDurabilityProvider } from "../durable.js"
import {
  activityCount,
  createBenchmarkCounters,
  type BenchmarkCounters,
} from "./workload.js"
import {
  createNullBenchmarkWorkload,
  type NullBenchmarkMode,
} from "./null.js"

const { Pool } = pg

export type PostgresBenchmarkOptions = {
  mode: NullBenchmarkMode
  connectionString: string
  schema: string
  physicalPartitions: number
  poolSize: number
  workflows: number
  workers: number
  shards: number
  activationConcurrency: number
  activationPrefetchLimit: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  diagnose: boolean
  diagnosticSampleIntervalMs: number
  keepSchema: boolean
  profileQueries: boolean
  synchronousCommit: "on" | "off"
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
  poolWait: {
    connectCount: number
    totalWaitMs: number
    avgWaitMs: number
    maxWaitMs: number
  }
  byPhase: Record<QueryProfilePhase, {
    totalQueries: number
    queriesPerActivation: number
    totalSqlMs: number
    avgQueryMs: number
  }>
  topByTotal: QueryProfileEntry[]
  topByCount: QueryProfileEntry[]
  topProcessingByTotal: QueryProfileEntry[]
  topProcessingByCount: QueryProfileEntry[]
}

export type PostgresBenchmarkDiagnostics = {
  sampleIntervalMs: number
  sampleCount: number
  maxPoolWaiting: number
  avgPoolWaiting: number
  maxPoolTotal: number
  maxActiveBackends: number
  avgActiveBackends: number
  maxWaitingBackends: number
  waitEventSamples: Record<string, number>
  databaseDelta?: Record<string, number>
  walDelta?: Record<string, number>
  processingCpuMs?: {
    user: number
    system: number
    total: number
  }
  processingEventLoopUtilization?: {
    activeMs: number
    idleMs: number
    utilization: number
  }
  samplerErrors: string[]
}

export type PostgresBenchmarkResult = {
  backend: "postgres"
  mode: NullBenchmarkMode
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
  activeWorkers: number
  mixedActions: number
  activationsPerSecond: number
  mixedActionsPerSecond: number
  workflowsPerSecond: number
  processingActivationsPerSecond: number
  processingMixedActionsPerSecond: number
  processingWorkflowsPerSecond: number
  counters: BenchmarkCounters
  queryProfile?: PostgresQueryProfile
  diagnostics?: PostgresBenchmarkDiagnostics
}

const defaultOptions: PostgresBenchmarkOptions = {
  mode: "mixed",
  connectionString:
    process.env.DURABLE_POSTGRES_URL ?? "postgresql://durable:durable@127.0.0.1:55432/durable",
  schema: `durable_bench_${randomUUID().replaceAll("-", "_")}`,
  physicalPartitions: 1,
  poolSize: 24,
  workflows: 250,
  workers: 4,
  shards: 4,
  activationConcurrency: 4,
  activationPrefetchLimit: 32,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  diagnose: false,
  diagnosticSampleIntervalMs: 25,
  keepSchema: false,
  profileQueries: false,
  synchronousCommit: "on",
  json: false,
}

async function main(): Promise<void> {
  const options = parsePostgresBenchmarkArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running Postgres durability benchmark with ${options.workflows} workflows, ${options.mode} mode, ${options.workers} workers, ${options.shards} shards, activation concurrency ${options.activationConcurrency}, activation prefetch ${options.activationPrefetchLimit}, physical partitions ${options.physicalPartitions}, pool size ${options.poolSize}, synchronous_commit ${options.synchronousCommit}, schema ${options.schema}...\n\n`,
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
  const workload = createNullBenchmarkWorkload(options.mode, counters, {
    activityDelayMs: options.activityDelayMs,
  })
  const pool = new Pool(poolConfig(options.connectionString, options.poolSize, options.synchronousCommit))
  let profilePhase: QueryProfilePhase = "setup"
  const queryProfiler = options.profileQueries || options.diagnose
    ? installPostgresQueryProfiler(pool, () => profilePhase)
    : undefined
  const diagnostics = options.diagnose
    ? await startPostgresDiagnostics({
        connectionString: options.connectionString,
        intervalMs: options.diagnosticSampleIntervalMs,
        pool,
        synchronousCommit: options.synchronousCommit,
      })
    : undefined
  const providers: PostgresDurabilityProvider[] = []
  let schemaInitialized = false

  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const runtimes: DurableRuntime[] = []

  let rounds = 0
  let activations = 0
  const expectedActivations = options.workflows * workload.activationsPerWorkflow
  const setupStartedAt = performance.now()
  let setupFinishedAt = setupStartedAt
  let processingStartedAt = setupStartedAt
  let processingFinishedAt = setupStartedAt
  let processingCpuUsage: NodeJS.CpuUsage | undefined
  let processingEventLoopUtilization:
    | ReturnType<typeof performance.eventLoopUtilization>
    | undefined
  const activeWorkers = new Set<string>()

  try {
    for (let workerIndex = 0; workerIndex < options.workers; workerIndex += 1) {
      const provider = await PostgresDurabilityProvider.create({
        pool,
        schema: options.schema,
        physicalPartitions: options.physicalPartitions,
      })
      schemaInitialized = true
      providers.push(provider)
      runtimes.push(
        new DurableRuntime(provider, {
          clock,
          workflows: workload.workflows,
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
    profilePhase = "processing"
    const processingCpuStartedAt = process.cpuUsage()
    const processingEventLoopStartedAt = performance.eventLoopUtilization()
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
    processingCpuUsage = process.cpuUsage(processingCpuStartedAt)
    processingEventLoopUtilization = performance.eventLoopUtilization(
      processingEventLoopStartedAt,
    )
    profilePhase = "verify"
    const verifyStartedAt = processingFinishedAt
    if (activations < expectedActivations) {
      throw new Error(
        `Benchmark did not process enough activations: ${activations}/${expectedActivations} after ${rounds} rounds`,
      )
    }

    const instances = await providers[0]!.listInstances()
    const completedWorkflows = instances.filter(
      (instance) => instance.workflowName === workload.RootWorkflow.name && instance.status === "completed",
    ).length
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    workload.verify(instances, options.workflows, 0)
    const mixedActions = workload.actionCount(counters, activations)
    const verifyFinishedAt = performance.now()
    const setupMs = setupFinishedAt - setupStartedAt
    const processingMs = processingFinishedAt - processingStartedAt
    const verifyMs = verifyFinishedAt - verifyStartedAt
    const elapsedMs = verifyFinishedAt - setupStartedAt
    const elapsedSeconds = elapsedMs / 1_000
    const processingSeconds = Math.max(processingMs / 1_000, Number.EPSILON)

    const result: PostgresBenchmarkResult = {
      backend: "postgres",
      mode: options.mode,
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
      activeWorkers: activeWorkers.size,
      mixedActions,
      activationsPerSecond: activations / elapsedSeconds,
      mixedActionsPerSecond: mixedActions / elapsedSeconds,
      workflowsPerSecond: completedWorkflows / elapsedSeconds,
      processingActivationsPerSecond: activations / processingSeconds,
      processingMixedActionsPerSecond: mixedActions / processingSeconds,
      processingWorkflowsPerSecond: completedWorkflows / processingSeconds,
      counters,
      queryProfile: queryProfiler?.snapshot(activations),
      diagnostics: await diagnostics?.snapshot({
        processingCpuUsage,
        processingEventLoopUtilization,
      }),
    }
    return result
  } finally {
    profilePhase = "cleanup"
    if (!options.keepSchema && schemaInitialized && providers[0]) {
      await providers[0].dropSchema().catch(() => undefined)
    }
    await Promise.all(providers.map((provider) => provider.close()))
    await diagnostics?.close()
    await pool.end()
  }
}

export function parsePostgresBenchmarkArgs(args: string[]): PostgresBenchmarkOptions {
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
    } else if (flag === "--connection-string") {
      options.connectionString = nextValue()
    } else if (flag === "--schema") {
      options.schema = nextValue()
    } else if (flag === "--physical-partitions") {
      options.physicalPartitions = parsePositiveInteger(nextValue(), flag)
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
    } else if (flag === "--diagnose") {
      options.diagnose = true
      options.profileQueries = true
    } else if (flag === "--diagnostic-sample-interval-ms") {
      options.diagnosticSampleIntervalMs = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--keep-schema") {
      options.keepSchema = true
    } else if (flag === "--profile-queries") {
      options.profileQueries = true
    } else if (flag === "--synchronous-commit") {
      options.synchronousCommit = parseSynchronousCommit(nextValue(), flag)
    } else if (flag === "--json") {
      options.json = true
    } else {
      throw new Error(`Unknown benchmark option: ${flag}`)
    }
  }
  return options
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

function parseSynchronousCommit(value: string, flag: string): PostgresBenchmarkOptions["synchronousCommit"] {
  if (value === "on" || value === "off") {
    return value
  }
  throw new Error(`${flag} must be on or off`)
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
  --mode <mixed|bare|activity|signal|timer|child>
                    Workload mode. Default: ${defaultOptions.mode}
  --connection-string <url>
                    Postgres connection string. Default: DURABLE_POSTGRES_URL or local Docker.
  --schema <name>   Isolated schema to create/drop. Default: generated durable_bench_* schema.
  --physical-partitions <n>
                    Manual physical table partitions for hot provider tables. Default: ${defaultOptions.physicalPartitions}
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
  --diagnose        Sample Postgres activity, pool pressure, WAL/database stats, Node CPU, and event loop use.
  --diagnostic-sample-interval-ms <n>
                    Diagnostic sampler interval. Default: ${defaultOptions.diagnosticSampleIntervalMs}
  --keep-schema     Keep the benchmark schema for inspection.
  --profile-queries
                    Include pg query counts and latency grouped by SQL fingerprint.
  --synchronous-commit <on|off>
                    Diagnostic Postgres synchronous_commit setting. Default: ${defaultOptions.synchronousCommit}
  --json            Print machine-readable JSON.
`)
}

function printResult(result: PostgresBenchmarkResult): void {
  const activityTotal = activityCount(result.counters)
  process.stdout.write(`Postgres durability benchmark
  mode: ${result.mode}
  workflows: ${result.options.workflows}
  workers: ${result.options.workers} logical in-process workers
  active workers: ${result.activeWorkers}
  shards: ${result.options.shards}
  activation concurrency: ${result.options.activationConcurrency} per worker
  activation prefetch limit: ${result.options.activationPrefetchLimit}
  physical partitions: ${result.options.physicalPartitions}
  activity delay: ${formatMs(result.options.activityDelayMs)}
  pool size: ${result.options.poolSize}
  synchronous_commit: ${result.options.synchronousCommit}
  query profiling: ${result.options.profileQueries ? "on" : "off"}
  diagnostics: ${result.options.diagnose ? "on" : "off"}
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
    const processingTop = result.queryProfile.topProcessingByTotal.slice(0, 5)
    process.stdout.write(`
Query profile:
  total queries: ${result.queryProfile.totalQueries}
  queries/activation: ${result.queryProfile.queriesPerActivation.toFixed(1)}
  avg query: ${formatMs(result.queryProfile.avgQueryMs)}
  pool connect waits: ${result.queryProfile.poolWait.connectCount} waits, ${formatMs(result.queryProfile.poolWait.totalWaitMs)} total, ${formatMs(result.queryProfile.poolWait.maxWaitMs)} max
  top processing SQL by total time:
${processingTop.length === 0 ? "    none" : processingTop.map((entry) => `    ${entry.count}x ${formatMs(entry.totalMs)} total, ${formatMs(entry.avgMs)} avg: ${entry.sql}`).join("\n")}
`)
  }
  if (result.diagnostics) {
    const diagnostics = result.diagnostics
    process.stdout.write(`
Diagnostics:
  samples: ${diagnostics.sampleCount} every ${diagnostics.sampleIntervalMs} ms
  pool waiting: avg ${diagnostics.avgPoolWaiting.toFixed(2)}, max ${diagnostics.maxPoolWaiting}
  pool total connections max: ${diagnostics.maxPoolTotal}
  active backends: avg ${diagnostics.avgActiveBackends.toFixed(2)}, max ${diagnostics.maxActiveBackends}
  waiting backends max: ${diagnostics.maxWaitingBackends}
  processing CPU: ${diagnostics.processingCpuMs ? formatMs(diagnostics.processingCpuMs.total) : "n/a"}
  event loop utilization: ${diagnostics.processingEventLoopUtilization ? diagnostics.processingEventLoopUtilization.utilization.toFixed(3) : "n/a"}
  wait events: ${Object.keys(diagnostics.waitEventSamples).length === 0 ? "none sampled" : JSON.stringify(diagnostics.waitEventSamples)}
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

function poolConfig(
  connectionString: string,
  max: number,
  synchronousCommit: PostgresBenchmarkOptions["synchronousCommit"],
): PoolConfig {
  return {
    connectionString,
    max,
    ...(synchronousCommit === "on"
      ? {}
      : { options: `-c synchronous_commit=${synchronousCommit}` }),
  }
}

type DiagnosticSample = {
  poolTotal: number
  poolIdle: number
  poolWaiting: number
  activeBackends: number
  waitingBackends: number
  waitEvents: Record<string, number>
}

async function startPostgresDiagnostics(input: {
  connectionString: string
  intervalMs: number
  pool: PgPool
  synchronousCommit: PostgresBenchmarkOptions["synchronousCommit"]
}): Promise<{
  snapshot(context: {
    processingCpuUsage?: NodeJS.CpuUsage
    processingEventLoopUtilization?: ReturnType<typeof performance.eventLoopUtilization>
  }): Promise<PostgresBenchmarkDiagnostics>
  close(): Promise<void>
}> {
  const samplePool = new Pool(poolConfig(input.connectionString, 1, input.synchronousCommit))
  const samples: DiagnosticSample[] = []
  const samplerErrors: string[] = []
  const initialDatabase = await readDatabaseStats(samplePool, samplerErrors)
  const initialWal = await readWalStats(samplePool, samplerErrors)
  let stopped = false
  let ended = false

  const sample = async () => {
    try {
      const rows = await samplePool.query<{
        count: number
        state: string | null
        wait_event: string | null
        wait_event_type: string | null
      }>(`
        SELECT state, wait_event_type, wait_event, count(*)::int AS count
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY state, wait_event_type, wait_event
      `)
      const waitEvents: Record<string, number> = {}
      let activeBackends = 0
      let waitingBackends = 0
      for (const row of rows.rows) {
        const count = Number(row.count)
        if (row.state === "active") {
          activeBackends += count
        }
        if (row.state === "active" && row.wait_event_type) {
          waitingBackends += count
          const key = `${row.wait_event_type}:${row.wait_event ?? "unknown"}`
          waitEvents[key] = (waitEvents[key] ?? 0) + count
        }
      }
      samples.push({
        poolTotal: input.pool.totalCount,
        poolIdle: input.pool.idleCount,
        poolWaiting: input.pool.waitingCount,
        activeBackends,
        waitingBackends,
        waitEvents,
      })
    } catch (error) {
      samplerErrors.push(error instanceof Error ? error.message : String(error))
    }
  }

  const loop = (async () => {
    while (!stopped) {
      const startedAt = performance.now()
      await sample()
      const delayMs = Math.max(0, input.intervalMs - (performance.now() - startedAt))
      await sleep(delayMs)
    }
  })()

  const stop = async () => {
    stopped = true
    await loop.catch((error: unknown) => {
      samplerErrors.push(error instanceof Error ? error.message : String(error))
    })
  }

  const close = async () => {
    await stop()
    if (!ended) {
      ended = true
      await samplePool.end()
    }
  }

  return {
    async snapshot(context): Promise<PostgresBenchmarkDiagnostics> {
      await stop()
      const finalDatabase = await readDatabaseStats(samplePool, samplerErrors)
      const finalWal = await readWalStats(samplePool, samplerErrors)
      await close()
      return summarizeDiagnostics({
        context,
        finalDatabase,
        finalWal,
        initialDatabase,
        initialWal,
        sampleIntervalMs: input.intervalMs,
        samplerErrors,
        samples,
      })
    },
    close,
  }
}

async function readDatabaseStats(
  pool: PgPool,
  errors: string[],
): Promise<Record<string, number> | undefined> {
  try {
    const result = await pool.query<Record<string, unknown>>(`
      SELECT
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time
      FROM pg_stat_database
      WHERE datname = current_database()
    `)
    return numericStats(result.rows[0])
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error))
    return undefined
  }
}

async function readWalStats(
  pool: PgPool,
  errors: string[],
): Promise<Record<string, number> | undefined> {
  try {
    const result = await pool.query<Record<string, unknown>>(`
      SELECT
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full
      FROM pg_stat_wal
    `)
    return numericStats(result.rows[0])
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error))
    return undefined
  }
}

function summarizeDiagnostics(input: {
  context: {
    processingCpuUsage?: NodeJS.CpuUsage
    processingEventLoopUtilization?: ReturnType<typeof performance.eventLoopUtilization>
  }
  finalDatabase?: Record<string, number>
  finalWal?: Record<string, number>
  initialDatabase?: Record<string, number>
  initialWal?: Record<string, number>
  sampleIntervalMs: number
  samplerErrors: string[]
  samples: DiagnosticSample[]
}): PostgresBenchmarkDiagnostics {
  const samples = input.samples
  const sampleCount = samples.length
  const sum = (field: keyof DiagnosticSample) =>
    samples.reduce((total, sample) => total + Number(sample[field]), 0)
  const max = (field: keyof DiagnosticSample) =>
    samples.reduce((largest, sample) => Math.max(largest, Number(sample[field])), 0)
  const waitEventSamples: Record<string, number> = {}
  for (const sample of samples) {
    for (const [event, count] of Object.entries(sample.waitEvents)) {
      waitEventSamples[event] = (waitEventSamples[event] ?? 0) + count
    }
  }
  const cpu = input.context.processingCpuUsage
  const elu = input.context.processingEventLoopUtilization
  return {
    sampleIntervalMs: input.sampleIntervalMs,
    sampleCount,
    maxPoolWaiting: max("poolWaiting"),
    avgPoolWaiting: sampleCount > 0 ? round(sum("poolWaiting") / sampleCount) : 0,
    maxPoolTotal: max("poolTotal"),
    maxActiveBackends: max("activeBackends"),
    avgActiveBackends: sampleCount > 0 ? round(sum("activeBackends") / sampleCount) : 0,
    maxWaitingBackends: max("waitingBackends"),
    waitEventSamples,
    databaseDelta:
      input.initialDatabase && input.finalDatabase
        ? diffStats(input.initialDatabase, input.finalDatabase)
        : undefined,
    walDelta:
      input.initialWal && input.finalWal
        ? diffStats(input.initialWal, input.finalWal)
        : undefined,
    processingCpuMs: cpu
      ? {
          user: round(cpu.user / 1_000),
          system: round(cpu.system / 1_000),
          total: round((cpu.user + cpu.system) / 1_000),
        }
      : undefined,
    processingEventLoopUtilization: elu
      ? {
          activeMs: round(elu.active),
          idleMs: round(elu.idle),
          utilization: round(elu.utilization),
        }
      : undefined,
    samplerErrors: [...new Set(input.samplerErrors)],
  }
}

function numericStats(row: Record<string, unknown> | undefined): Record<string, number> {
  const stats: Record<string, number> = {}
  if (!row) {
    return stats
  }
  for (const [key, value] of Object.entries(row)) {
    stats[key] = Number(value ?? 0)
  }
  return stats
}

function diffStats(
  before: Record<string, number>,
  after: Record<string, number>,
): Record<string, number> {
  const delta: Record<string, number> = {}
  for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
    delta[key] = round((after[key] ?? 0) - (before[key] ?? 0))
  }
  return delta
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
  const poolWait = {
    connectCount: 0,
    totalWaitMs: 0,
    maxWaitMs: 0,
  }

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
    const startedAt = performance.now()
    const recordConnectWait = () => {
      const elapsedMs = performance.now() - startedAt
      poolWait.connectCount += 1
      poolWait.totalWaitMs += elapsedMs
      poolWait.maxWaitMs = Math.max(poolWait.maxWaitMs, elapsedMs)
    }
    if (typeof args[0] === "function") {
      const callback = args[0]
      return originalConnect((error: Error | undefined, client: PoolClient, done: () => void) => {
        recordConnectWait()
        callback(error, client ? wrapClient(client) : client, done)
      })
    }
    return Promise.resolve(originalConnect()).then((client) => {
      recordConnectWait()
      return wrapClient(client)
    })
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
        poolWait: {
          connectCount: poolWait.connectCount,
          totalWaitMs: round(poolWait.totalWaitMs),
          avgWaitMs:
            poolWait.connectCount > 0 ? round(poolWait.totalWaitMs / poolWait.connectCount) : 0,
          maxWaitMs: round(poolWait.maxWaitMs),
        },
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
        topProcessingByTotal: entries
          .filter((entry) => entry.phase === "processing")
          .sort((left, right) => right.totalMs - left.totalMs)
          .slice(0, 25),
        topProcessingByCount: entries
          .filter((entry) => entry.phase === "processing")
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
