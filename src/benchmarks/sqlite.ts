import { mkdir, mkdtemp, readdir, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { performance } from "node:perf_hooks"
import { pathToFileURL } from "node:url"
import {
  DurableRuntime,
  type DurabilityProvider,
  type LoadInstanceOptions,
  type PersistedInstance,
  SqliteShardFileDurabilityProvider,
  SqliteDurabilityProvider,
  workflowPartitionShard,
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
  sqliteLayout: "single-file" | "shard-files"
  sqliteDirectory?: string
  workflows: number
  workflowOffset: number
  workers: number
  shards: number
  dispatchShardIds?: number[]
  workflowShardIds?: number[]
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
  correct: boolean
  sqliteLayout: BenchmarkOptions["sqliteLayout"]
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

type BenchmarkProvider = DurabilityProvider & {
  close(): void
  listInstances(options?: LoadInstanceOptions): Promise<PersistedInstance[]>
  listActivationClaims(): Promise<
    Array<{
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      kind: "migration" | "run" | "event"
      ownerId?: string
      completedBySequence?: number
    }>
  >
}

const defaultOptions: BenchmarkOptions = {
  mode: "mixed",
  sqliteLayout: "single-file",
  workflows: 250,
  workflowOffset: 0,
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
  assertShardListInRange(options.dispatchShardIds, options.shards, "dispatchShardIds")
  assertShardListInRange(options.workflowShardIds, options.shards, "workflowShardIds")
  const ownsTempDir = options.sqliteDirectory === undefined
  const tempDir = options.sqliteDirectory ?? await mkdtemp(join(tmpdir(), "durable-bench-"))
  await mkdir(tempDir, { recursive: true })
  const dbPath = join(tempDir, "bench.sqlite")
  const counters = createBenchmarkCounters()
  const providers: BenchmarkProvider[] = []
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
    const provider = createBenchmarkProvider(options, tempDir, dbPath, queryProfiler?.record)
    providers.push(provider)
    return new DurableRuntime(provider, {
      clock,
      workflows: workload.workflows,
      workerId: `bench-worker-${workerIndex}`,
      shardCount: options.shards,
      dispatchShardIds: dispatchShardIdsForWorker(
        workerIndex,
        options.workers,
        options.shards,
        options.dispatchShardIds,
      ),
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
    for (let localIndex = 0; localIndex < options.workflows; localIndex += 1) {
      const index = options.workflowOffset + localIndex
      const workflowId = benchmarkWorkflowId(options, index, localIndex)
      const ref = await runtimes[0].start(
        workload.RootWorkflow,
        { index },
        { workflowId },
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

    const verifier = providers[0]
    if (!verifier) {
      throw new Error("Benchmark did not create a provider")
    }
    const instances = normalizeBenchmarkInstanceIds(await verifier.listInstances())
    const completedWorkflows = countExpectedCompletedRootWorkflows(
      instances,
      workload.RootWorkflow.name,
      options,
    )
    if (completedWorkflows !== options.workflows) {
      throw new Error(
        `Benchmark did not complete: ${completedWorkflows}/${options.workflows} workflows finished after ${rounds} rounds`,
      )
    }

    workload.verify(instances, options.workflows, options.workflowOffset)
    workload.verifyCounters(counters, options.workflows)
    const claims = await verifier.listActivationClaims()
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
      correct: true,
      sqliteLayout: options.sqliteLayout,
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
      dbPath: options.keepDb ? (options.sqliteLayout === "shard-files" ? tempDir : dbPath) : undefined,
      dbBytes: options.keepDb ? await sqliteStoreBytes(tempDir, dbPath, options) : undefined,
    }
  } finally {
    profilePhase = "cleanup"
    for (const provider of providers) {
      provider.close()
    }
    if (!options.keepDb && ownsTempDir) {
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
    } else if (flag === "--sqlite-layout") {
      options.sqliteLayout = parseSqliteLayout(nextValue(), flag)
    } else if (flag === "--shard-files") {
      options.sqliteLayout = "shard-files"
    } else if (flag === "--sqlite-dir") {
      options.sqliteDirectory = nextValue()
    } else if (flag === "--workflows") {
      options.workflows = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workflow-offset") {
      options.workflowOffset = parseNonNegativeInteger(nextValue(), flag)
    } else if (flag === "--workers") {
      options.workers = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--shards") {
      options.shards = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--dispatch-shards") {
      options.dispatchShardIds = parseShardList(nextValue(), flag)
    } else if (flag === "--workflow-shards") {
      options.workflowShardIds = parseShardList(nextValue(), flag)
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

function parseSqliteLayout(value: string, flag: string): BenchmarkOptions["sqliteLayout"] {
  if (value === "single-file" || value === "shard-files") {
    return value
  }
  throw new Error(`${flag} must be single-file or shard-files`)
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

function parseShardList(value: string, flag: string): number[] {
  const shards = value.split(",").map((raw) => {
    const trimmed = raw.trim()
    if (trimmed === "") {
      throw new Error(`${flag} must be a comma-separated list of non-negative integers`)
    }
    const parsed = Number(trimmed)
    if (!Number.isInteger(parsed) || parsed < 0) {
      throw new Error(`${flag} must be a comma-separated list of non-negative integers`)
    }
    return parsed
  })
  if (shards.length === 0 || shards.some((shard) => !Number.isInteger(shard))) {
    throw new Error(`${flag} must include at least one shard`)
  }
  return [...new Set(shards)]
}

function assertShardListInRange(shards: number[] | undefined, shardCount: number, name: string): void {
  if (!shards) {
    return
  }
  for (const shardId of shards) {
    if (!Number.isInteger(shardId) || shardId < 0 || shardId >= shardCount) {
      throw new Error(`${name} contains shard ${shardId}, but shard count is ${shardCount}`)
    }
  }
}

function printHelp(): void {
  process.stdout.write(`SQLite durability benchmark

Runs real TypeScript workflows against the SQLite durability provider.

Options:
  --mode <mixed|bare|activity|signal|timer|child>
                    Workload mode. Default: ${defaultOptions.mode}
  --sqlite-layout <single-file|shard-files>
                    SQLite file layout. Default: ${defaultOptions.sqliteLayout}
  --shard-files     Shortcut for --sqlite-layout shard-files.
  --sqlite-dir <path>
                    Existing directory for benchmark database files. Caller owns cleanup.
  --workflows <n>   Parent workflow count. Default: ${defaultOptions.workflows}
  --workflow-offset <n>
                    Starting workflow index. Default: ${defaultOptions.workflowOffset}
  --workers <n>     Logical in-process worker count. Default: ${defaultOptions.workers}
  --shards <n>      Dispatch shard count. Default: ${defaultOptions.shards}
  --dispatch-shards <csv>
                    Optional shard ids this process may claim, e.g. 0,1,2,3.
  --workflow-shards <csv>
                    Optional shard ids used when choosing benchmark workflow IDs.
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
  layout: ${result.sqliteLayout}
  workflows: ${result.options.workflows}
  workflow offset: ${result.options.workflowOffset}
  workers: ${result.options.workers} logical in-process workers
  committed workers: ${result.committedWorkers}
  shards: ${result.options.shards}
  dispatch shards: ${result.options.dispatchShardIds?.join(",") ?? "all"}
  workflow shards: ${result.options.workflowShardIds?.join(",") ?? "all"}
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
  allowedShardIds?: number[],
): number[] {
  const shardIds: number[] = []
  const candidates = allowedShardIds ?? Array.from({ length: shardCount }, (_value, shardId) => shardId)
  for (const [offset, shardId] of candidates.entries()) {
    if (offset % workerCount === workerIndex) {
      shardIds.push(shardId)
    }
  }
  return shardIds
}

function benchmarkWorkflowId(options: BenchmarkOptions, index: number, localIndex: number): string {
  const base = `${options.mode}-bench-${index}`
  const targetShards = options.workflowShardIds
  if (!targetShards || targetShards.length === 0) {
    return base
  }
  const targetShard = targetShards[localIndex % targetShards.length]!
  if (workflowPartitionShard(base, "run-1", options.shards) === targetShard) {
    return base
  }
  for (let attempt = 1; attempt <= 10_000; attempt += 1) {
    const candidate = `${base}__shard_${attempt}`
    if (workflowPartitionShard(candidate, "run-1", options.shards) === targetShard) {
      return candidate
    }
  }
  throw new Error(`Could not find benchmark workflow id for shard ${targetShard}`)
}

function normalizeBenchmarkInstanceIds(instances: PersistedInstance[]): PersistedInstance[] {
  return instances.map((instance) => ({
    ...instance,
    workflowId: stripBenchmarkShardSuffix(instance.workflowId),
  }))
}

function stripBenchmarkShardSuffix(workflowId: string): string {
  return workflowId.replace(/__shard_\d+$/, "")
}

function countExpectedCompletedRootWorkflows(
  instances: PersistedInstance[],
  workflowName: string,
  options: BenchmarkOptions,
): number {
  const expectedWorkflowIds = new Set(
    Array.from({ length: options.workflows }, (_value, localIndex) =>
      `${options.mode}-bench-${options.workflowOffset + localIndex}`,
    ),
  )
  return instances.filter(
    (instance) =>
      instance.workflowName === workflowName &&
      instance.status === "completed" &&
      expectedWorkflowIds.has(instance.workflowId),
  ).length
}

function createBenchmarkProvider(
  options: BenchmarkOptions,
  tempDir: string,
  dbPath: string,
  sqlProfiler?: (event: { sql: string; method: "run" | "get" | "all"; durationMs: number }) => void,
): BenchmarkProvider {
  if (options.sqliteLayout === "shard-files") {
    return new SqliteShardFileDurabilityProvider({
      directory: join(tempDir, "shards"),
      shardCount: options.shards,
      sqlProfiler,
    })
  }
  return new SqliteDurabilityProvider(dbPath, { sqlProfiler })
}

async function sqliteStoreBytes(
  tempDir: string,
  dbPath: string,
  options: BenchmarkOptions,
): Promise<number> {
  if (options.sqliteLayout === "shard-files") {
    return directoryBytes(join(tempDir, "shards"))
  }
  const paths = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]
  const sizes = await Promise.all(
    paths.map((path) => stat(path).then((info) => info.size).catch(() => 0)),
  )
  return sizes.reduce((total, size) => total + size, 0)
}

async function directoryBytes(directory: string): Promise<number> {
  const entries = await readdir(directory, { withFileTypes: true }).catch(() => [])
  const sizes = await Promise.all(
    entries.map(async (entry) => {
      const path = join(directory, entry.name)
      if (entry.isDirectory()) {
        return directoryBytes(path)
      }
      return stat(path).then((info) => info.size).catch(() => 0)
    }),
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
