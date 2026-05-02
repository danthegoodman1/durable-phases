import { mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import {
  DurableRuntime,
  SqliteDurabilityProvider,
} from "../durable.js"
import {
  activityCount,
  createBenchmarkCounters,
  type BenchmarkCounters,
} from "./workload.js"
import {
  createNullBenchmarkWorkload,
  type NullBenchmarkMode,
} from "./null.js"

export type BenchmarkOptions = {
  mode: NullBenchmarkMode
  workflows: number
  workers: number
  shards: number
  activationConcurrency: number
  activationPrefetchLimit: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  keepDb: boolean
  profileQueries: boolean
  json: boolean
}

type QueryProfilePhase = "setup" | "processing" | "verify" | "cleanup"

type SqliteQueryProfileEntry = {
  phase: QueryProfilePhase | "all"
  count: number
  totalMs: number
  avgMs: number
  maxMs: number
  sql: string
}

export type SqliteQueryProfile = {
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
  topByTotal: SqliteQueryProfileEntry[]
  topByCount: SqliteQueryProfileEntry[]
  topProcessingByTotal: SqliteQueryProfileEntry[]
  topProcessingByCount: SqliteQueryProfileEntry[]
}

export type BenchmarkResult = {
  backend: "sqlite"
  mode: NullBenchmarkMode
  options: BenchmarkOptions
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
  queryProfile?: SqliteQueryProfile
  dbPath?: string
  dbBytes?: number
}

const defaultOptions: BenchmarkOptions = {
  mode: "mixed",
  workflows: 250,
  workers: 4,
  shards: 4,
  activationConcurrency: 4,
  activationPrefetchLimit: 32,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  keepDb: false,
  profileQueries: false,
  json: false,
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running SQLite durability benchmark with ${options.workflows} workflows, ${options.mode} mode, ${options.workers} workers, ${options.shards} shards, activation concurrency ${options.activationConcurrency}, activation prefetch ${options.activationPrefetchLimit}, SQLite WAL/FULL durability...\n\n`,
    )
  }
  const result = await runSqliteBenchmark(options)
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } else {
    printResult(result)
  }
}

export async function runSqliteBenchmark(options: BenchmarkOptions): Promise<BenchmarkResult> {
  const tempDir = await mkdtemp(join(tmpdir(), "durable-bench-"))
  const dbPath = join(tempDir, "bench.sqlite")
  const counters = createBenchmarkCounters()
  const providers: SqliteDurabilityProvider[] = []
  const workload = createNullBenchmarkWorkload(options.mode, counters, {
    activityDelayMs: options.activityDelayMs,
  })
  let profilePhase: QueryProfilePhase = "setup"
  const queryProfiler = options.profileQueries
    ? createSqliteQueryProfiler(() => profilePhase)
    : undefined
  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const runtimes = Array.from({ length: options.workers }, (_value, workerIndex) => {
    const provider = new SqliteDurabilityProvider(dbPath, {
      sqlProfiler: queryProfiler?.record,
    })
    providers.push(provider)
    return new DurableRuntime(provider, {
      clock,
      workflows: workload.workflows,
      workerId: `bench-worker-${workerIndex}`,
      shardCount: options.shards,
      dispatchShardIds: dispatchShardIdsForWorker(workerIndex, options.workers, options.shards),
      maxConcurrentActivations: options.activationConcurrency,
      activationPrefetchLimit: options.activationPrefetchLimit,
      dispatchLeaseMs: 30_000,
      activationLeaseMs: 30_000,
    })
  })

  let rounds = 0
  let activations = 0
  const expectedActivations = options.workflows * workload.activationsPerWorkflow
  const setupStartedAt = performance.now()
  let setupFinishedAt = setupStartedAt
  let processingStartedAt = setupStartedAt
  let processingFinishedAt = setupStartedAt

  try {
    for (let index = 0; index < options.workflows; index += 1) {
      const ref = await runtimes[0].start(
        workload.RootWorkflow,
        { index },
        { workflowId: `${options.mode}-bench-${index}` },
      )
      counters.workflowStarts += 1
      if (workload.appendFinishSignal) {
        await runtimes[0].signal(
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

    const instances = await providers[0].listInstances()
    const completedWorkflows = instances.filter(
      (instance) => instance.workflowName === workload.RootWorkflow.name && instance.status === "completed",
    ).length
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    workload.verify(instances, options.workflows, 0)
    const claims = await providers[0].listActivationClaims()
    const committedWorkers = new Set(
      claims
        .filter((claim) => claim.completedBySequence !== undefined)
        .map((claim) => claim.ownerId)
        .filter((ownerId): ownerId is string => Boolean(ownerId)),
    ).size
    const mixedActions = workload.actionCount(counters, activations)
    const verifyFinishedAt = performance.now()
    const setupMs = setupFinishedAt - setupStartedAt
    const processingMs = processingFinishedAt - processingStartedAt
    const verifyMs = verifyFinishedAt - verifyStartedAt
    const elapsedMs = verifyFinishedAt - setupStartedAt
    const elapsedSeconds = elapsedMs / 1_000
    const processingSeconds = Math.max(processingMs / 1_000, Number.EPSILON)

    return {
      backend: "sqlite",
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
      dbPath: options.keepDb ? dbPath : undefined,
      dbBytes: options.keepDb ? await sqliteStoreBytes(dbPath) : undefined,
    }
  } finally {
    profilePhase = "cleanup"
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
    } else if (flag === "--mode") {
      options.mode = parseMode(nextValue(), flag)
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
    } else if (flag === "--keep-db") {
      options.keepDb = true
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
  process.stdout.write(`SQLite durability benchmark

Runs real TypeScript workflows against the SQLite durability provider.

Options:
  --mode <mixed|bare|activity|signal|timer|child>
                    Workload mode. Default: ${defaultOptions.mode}
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
  --keep-db         Keep the temporary SQLite database and print its path.
  --profile-queries Record SQLite statement timing/count profile.
  --json            Print machine-readable JSON.
`)
}

function printResult(result: BenchmarkResult): void {
  const activityTotal = activityCount(result.counters)
  process.stdout.write(`SQLite durability benchmark
  mode: ${result.mode}
  workflows: ${result.options.workflows}
  workers: ${result.options.workers} logical in-process workers
  committed workers: ${result.committedWorkers}
  shards: ${result.options.shards}
  activation concurrency: ${result.options.activationConcurrency} per worker
  activation prefetch limit: ${result.options.activationPrefetchLimit}
  activity delay: ${formatMs(result.options.activityDelayMs)}
  SQLite durability: WAL/FULL
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
SQLite statement profile:
  total statements: ${result.queryProfile.totalQueries}
  statements/activation: ${result.queryProfile.queriesPerActivation.toFixed(2)}
  processing statements/activation: ${result.queryProfile.byPhase.processing.queriesPerActivation.toFixed(2)}
  total SQL time: ${formatMs(result.queryProfile.totalSqlMs)}

Top processing statements by total time:
${formatProfileEntries(result.queryProfile.topProcessingByTotal)}
`)
  }

  if (result.dbPath) {
    process.stdout.write(`
SQLite store kept:
  path: ${result.dbPath}
  size: ${result.dbBytes ?? 0} bytes
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

async function sqliteStoreBytes(dbPath: string): Promise<number> {
  const paths = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]
  const sizes = await Promise.all(
    paths.map((path) => stat(path).then((info) => info.size).catch(() => 0)),
  )
  return sizes.reduce((total, size) => total + size, 0)
}

function createSqliteQueryProfiler(phase: () => QueryProfilePhase) {
  const entries: Array<{ phase: QueryProfilePhase; sql: string; durationMs: number }> = []
  return {
    record: (event: { sql: string; durationMs: number }) => {
      entries.push({
        phase: phase(),
        sql: normalizeSql(event.sql),
        durationMs: event.durationMs,
      })
    },
    snapshot: (activations: number): SqliteQueryProfile => {
      const all = summarizeEntries(entries, activations)
      return {
        totalQueries: all.totalQueries,
        queriesPerActivation: all.queriesPerActivation,
        totalSqlMs: all.totalSqlMs,
        avgQueryMs: all.avgQueryMs,
        byPhase: {
          setup: summarizeEntries(entries.filter((entry) => entry.phase === "setup"), activations),
          processing: summarizeEntries(
            entries.filter((entry) => entry.phase === "processing"),
            activations,
          ),
          verify: summarizeEntries(entries.filter((entry) => entry.phase === "verify"), activations),
          cleanup: summarizeEntries(entries.filter((entry) => entry.phase === "cleanup"), activations),
        },
        topByTotal: topEntries(entries, "all", "total"),
        topByCount: topEntries(entries, "all", "count"),
        topProcessingByTotal: topEntries(
          entries.filter((entry) => entry.phase === "processing"),
          "processing",
          "total",
        ),
        topProcessingByCount: topEntries(
          entries.filter((entry) => entry.phase === "processing"),
          "processing",
          "count",
        ),
      }
    },
  }
}

function summarizeEntries(
  entries: Array<{ durationMs: number }>,
  activations: number,
): SqliteQueryProfile["byPhase"][QueryProfilePhase] {
  const totalQueries = entries.length
  const totalSqlMs = entries.reduce((total, entry) => total + entry.durationMs, 0)
  return {
    totalQueries,
    queriesPerActivation: totalQueries / Math.max(activations, 1),
    totalSqlMs,
    avgQueryMs: totalQueries === 0 ? 0 : totalSqlMs / totalQueries,
  }
}

function topEntries(
  entries: Array<{ phase: QueryProfilePhase; sql: string; durationMs: number }>,
  phase: QueryProfilePhase | "all",
  order: "total" | "count",
): SqliteQueryProfileEntry[] {
  const groups = new Map<string, { count: number; totalMs: number; maxMs: number }>()
  for (const entry of entries) {
    const current = groups.get(entry.sql) ?? { count: 0, totalMs: 0, maxMs: 0 }
    current.count += 1
    current.totalMs += entry.durationMs
    current.maxMs = Math.max(current.maxMs, entry.durationMs)
    groups.set(entry.sql, current)
  }
  return [...groups.entries()]
    .map(([sql, value]) => ({
      phase,
      count: value.count,
      totalMs: value.totalMs,
      avgMs: value.totalMs / value.count,
      maxMs: value.maxMs,
      sql,
    }))
    .sort((left, right) =>
      order === "total"
        ? right.totalMs - left.totalMs
        : right.count - left.count || right.totalMs - left.totalMs,
    )
    .slice(0, 10)
}

function normalizeSql(sql: string): string {
  return sql.trim().replace(/\s+/g, " ")
}

function formatProfileEntries(entries: SqliteQueryProfileEntry[]): string {
  if (entries.length === 0) {
    return "  (none)"
  }
  return entries
    .map((entry) =>
      `  ${entry.count}x ${formatMs(entry.totalMs)} total, ${formatMs(entry.avgMs)} avg: ${entry.sql}`,
    )
    .join("\n")
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
