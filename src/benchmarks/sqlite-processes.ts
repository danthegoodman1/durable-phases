import { spawn } from "node:child_process"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { performance } from "node:perf_hooks"
import { fileURLToPath, pathToFileURL } from "node:url"
import { createBenchmarkCounters, type BenchmarkCounters } from "./workload.js"
import type { BenchmarkOptions, BenchmarkResult } from "./sqlite.js"
import type { NullBenchmarkMode } from "./null.js"

export type SqliteMultiProcessBenchmarkOptions = {
  mode: NullBenchmarkMode
  sqliteLayout: BenchmarkOptions["sqliteLayout"]
  workflows: number
  processes: number
  workersPerProcess: number
  shardsPerProcess: number
  activationConcurrency: number
  activationPrefetchLimit: number
  activityDelayMs: number
  batch: number
  maxRounds: number
  json: boolean
}

export type SqliteProcessResult = {
  processIndex: number
  workflows: number
  workflowOffset: number
  result: BenchmarkResult
}

export type SqliteMultiProcessBenchmarkResult = {
  backend: "sqlite-multiprocess"
  mode: NullBenchmarkMode
  sqliteLayout: BenchmarkOptions["sqliteLayout"]
  options: SqliteMultiProcessBenchmarkOptions
  elapsedMs: number
  processingMs: number
  processes: number
  activeProcesses: number
  committedWorkers: number
  activations: number
  expectedActivations: number
  completedWorkflows: number
  mixedActions: number
  activationsPerSecond: number
  mixedActionsPerSecond: number
  workflowsPerSecond: number
  processingActivationsPerSecond: number
  processingMixedActionsPerSecond: number
  processingWorkflowsPerSecond: number
  counters: BenchmarkCounters
  processResults: SqliteProcessResult[]
}

type SqliteProcessRunner = (
  processIndex: number,
  options: BenchmarkOptions,
) => Promise<BenchmarkResult>

const defaultOptions: SqliteMultiProcessBenchmarkOptions = {
  mode: "mixed",
  sqliteLayout: "shard-files",
  workflows: 1_000,
  processes: 4,
  workersPerProcess: 4,
  shardsPerProcess: 4,
  activationConcurrency: 4,
  activationPrefetchLimit: 32,
  activityDelayMs: 0,
  batch: 32,
  maxRounds: 10_000,
  json: false,
}

async function main(): Promise<void> {
  const options = parseSqliteMultiProcessBenchmarkArgs(process.argv.slice(2))
  if (!options.json) {
    process.stdout.write(
      `Running multi-process SQLite benchmark with ${options.workflows} workflows, ${options.processes} processes, ${options.sqliteLayout} layout, ${options.workersPerProcess} workers/process, ${options.shardsPerProcess} shards/process...\n\n`,
    )
  }
  const result = await runSqliteMultiProcessBenchmark(options)
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
  } else {
    printResult(result)
  }
}

export async function runSqliteMultiProcessBenchmark(
  options: SqliteMultiProcessBenchmarkOptions,
  runner: SqliteProcessRunner = runSqliteBenchmarkProcess,
): Promise<SqliteMultiProcessBenchmarkResult> {
  const processInputs = splitWorkflowRanges(options.workflows, options.processes)
  const totalShards = options.processes * options.shardsPerProcess
  const storeDir = await mkdtemp(join(tmpdir(), "durable-bench-sqlite-shards-"))
  const startedAt = performance.now()
  let processResults: SqliteProcessResult[]
  try {
    processResults = await Promise.all(
      processInputs.map(({ workflows, workflowOffset }, processIndex) => {
        const ownedShardIds = shardIdsForProcess(processIndex, options.shardsPerProcess)
        return runner(processIndex, {
          mode: options.mode,
          sqliteLayout: options.sqliteLayout,
          sqliteDirectory: storeDir,
          workflows,
          workflowOffset,
          workers: options.workersPerProcess,
          shards: totalShards,
          dispatchShardIds: ownedShardIds,
          workflowShardIds: ownedShardIds,
          activationConcurrency: options.activationConcurrency,
          activationPrefetchLimit: options.activationPrefetchLimit,
          activityDelayMs: options.activityDelayMs,
          batch: options.batch,
          maxRounds: options.maxRounds,
          keepDb: false,
          profileQueries: false,
          json: true,
        }).then((result) => ({
          processIndex,
          workflows,
          workflowOffset,
          result,
        }))
      }),
    )
  } finally {
    await rm(storeDir, { force: true, maxRetries: 3, recursive: true, retryDelay: 10 })
  }
  const finishedAt = performance.now()
  const counters = aggregateCounters(processResults.map((item) => item.result.counters))
  const activations = sum(processResults, (item) => item.result.activations)
  const expectedActivations = sum(processResults, (item) => item.result.expectedActivations)
  const completedWorkflows = sum(processResults, (item) => item.result.completedWorkflows)
  const mixedActions = sum(processResults, (item) => item.result.mixedActions)
  const activeProcesses = processResults.filter((item) => item.result.activations > 0).length
  const committedWorkers = sum(processResults, (item) => item.result.committedWorkers)
  const elapsedMs = finishedAt - startedAt
  const processingMs = Math.max(
    ...processResults.map((item) => item.result.processingMs),
    Number.EPSILON,
  )
  const elapsedSeconds = Math.max(elapsedMs / 1_000, Number.EPSILON)
  const processingSeconds = Math.max(processingMs / 1_000, Number.EPSILON)

  return {
    backend: "sqlite-multiprocess",
    mode: options.mode,
    sqliteLayout: options.sqliteLayout,
    options,
    elapsedMs,
    processingMs,
    processes: options.processes,
    activeProcesses,
    committedWorkers,
    activations,
    expectedActivations,
    completedWorkflows,
    mixedActions,
    activationsPerSecond: activations / elapsedSeconds,
    mixedActionsPerSecond: mixedActions / elapsedSeconds,
    workflowsPerSecond: completedWorkflows / elapsedSeconds,
    processingActivationsPerSecond: activations / processingSeconds,
    processingMixedActionsPerSecond: mixedActions / processingSeconds,
    processingWorkflowsPerSecond: completedWorkflows / processingSeconds,
    counters,
    processResults: processResults.sort((left, right) => left.processIndex - right.processIndex),
  }
}

function parseSqliteMultiProcessBenchmarkArgs(args: string[]): SqliteMultiProcessBenchmarkOptions {
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
    } else if (flag === "--workflows") {
      options.workflows = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--processes") {
      options.processes = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--workers-per-process") {
      options.workersPerProcess = parsePositiveInteger(nextValue(), flag)
    } else if (flag === "--shards-per-process") {
      options.shardsPerProcess = parsePositiveInteger(nextValue(), flag)
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
    } else if (flag === "--json") {
      options.json = true
    } else {
      throw new Error(`Unknown benchmark option: ${flag}`)
    }
  }
  return options
}

async function runSqliteBenchmarkProcess(
  processIndex: number,
  options: BenchmarkOptions,
): Promise<BenchmarkResult> {
  const tsxCli = fileURLToPath(new URL("../../node_modules/tsx/dist/cli.mjs", import.meta.url))
  const benchmarkScript = fileURLToPath(new URL("./sqlite.ts", import.meta.url))
  const args = [
    tsxCli,
    benchmarkScript,
    "--json",
    "--mode",
    options.mode,
    "--sqlite-layout",
    options.sqliteLayout,
    "--workflows",
    String(options.workflows),
    "--workflow-offset",
    String(options.workflowOffset),
    "--workers",
    String(options.workers),
    "--shards",
    String(options.shards),
    "--activation-concurrency",
    String(options.activationConcurrency),
    "--activation-prefetch-limit",
    String(options.activationPrefetchLimit),
    "--activity-delay-ms",
    String(options.activityDelayMs),
    "--batch",
    String(options.batch),
    "--max-rounds",
    String(options.maxRounds),
  ]
  if (options.sqliteDirectory) {
    args.push("--sqlite-dir", options.sqliteDirectory)
  }
  if (options.dispatchShardIds && options.dispatchShardIds.length > 0) {
    args.push("--dispatch-shards", options.dispatchShardIds.join(","))
  }
  if (options.workflowShardIds && options.workflowShardIds.length > 0) {
    args.push("--workflow-shards", options.workflowShardIds.join(","))
  }

  const output = await spawnCollect(process.execPath, args, {
    DURABLE_SQLITE_PROCESS_INDEX: String(processIndex),
  })
  return parseJsonOutput(output.stdout)
}

function spawnCollect(
  command: string,
  args: string[],
  env: Record<string, string>,
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""
    child.stdout.setEncoding("utf8")
    child.stderr.setEncoding("utf8")
    child.stdout.on("data", (chunk) => {
      stdout += chunk
    })
    child.stderr.on("data", (chunk) => {
      stderr += chunk
    })
    child.on("error", reject)
    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr })
      } else {
        reject(new Error(`SQLite benchmark subprocess exited with ${code}\n${stderr}\n${stdout}`))
      }
    })
  })
}

function parseJsonOutput(output: string): BenchmarkResult {
  const start = output.indexOf("{")
  const end = output.lastIndexOf("}")
  if (start < 0 || end <= start) {
    throw new Error(`Subprocess did not print JSON: ${output}`)
  }
  return JSON.parse(output.slice(start, end + 1)) as BenchmarkResult
}

function splitWorkflowRanges(
  workflows: number,
  processes: number,
): Array<{ workflows: number; workflowOffset: number }> {
  const base = Math.floor(workflows / processes)
  const remainder = workflows % processes
  let workflowOffset = 0
  return Array.from({ length: processes }, (_value, index) => {
    const processWorkflows = base + (index < remainder ? 1 : 0)
    const range = { workflows: processWorkflows, workflowOffset }
    workflowOffset += processWorkflows
    return range
  })
}

function shardIdsForProcess(processIndex: number, shardsPerProcess: number): number[] {
  const firstShardId = processIndex * shardsPerProcess
  return Array.from({ length: shardsPerProcess }, (_value, offset) => firstShardId + offset)
}

function aggregateCounters(counters: BenchmarkCounters[]): BenchmarkCounters {
  return counters.reduce((total, next) => ({
    workflowStarts: total.workflowStarts + next.workflowStarts,
    signals: total.signals + next.signals,
    childStarts: total.childStarts + next.childStarts,
    childCompletions: total.childCompletions + next.childCompletions,
    timerHandlers: total.timerHandlers + next.timerHandlers,
    bootActivities: total.bootActivities + next.bootActivities,
    childActivities: total.childActivities + next.childActivities,
    finishActivities: total.finishActivities + next.finishActivities,
  }), createBenchmarkCounters())
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

function sum<T>(values: T[], selector: (value: T) => number): number {
  return values.reduce((total, value) => total + selector(value), 0)
}

function printHelp(): void {
  process.stdout.write(`Multi-process SQLite durability benchmark

Launches multiple subprocesses against one shared SQLite shard-file directory,
assigns each subprocess a disjoint shard range, and aggregates throughput. This
measures process-level scaling once SQLite work is split across physical
database files.

Options:
  --mode <mixed|bare|activity|signal|timer|child>
                    Workload mode. Default: ${defaultOptions.mode}
  --sqlite-layout <single-file|shard-files>
                    SQLite file layout per subprocess. Default: ${defaultOptions.sqliteLayout}
  --shard-files     Shortcut for --sqlite-layout shard-files.
  --workflows <n>   Total root workflow count split across processes. Default: ${defaultOptions.workflows}
  --processes <n>   Subprocess count. Default: ${defaultOptions.processes}
  --workers-per-process <n>
                    Logical workers inside each subprocess. Default: ${defaultOptions.workersPerProcess}
  --shards-per-process <n>
                    Dispatch shards inside each subprocess. Default: ${defaultOptions.shardsPerProcess}
  --activation-concurrency <n>
                    Max concurrent activations per worker. Default: ${defaultOptions.activationConcurrency}
  --activation-prefetch-limit <n>
                    Claimed activations to keep ahead of execution. Default: ${defaultOptions.activationPrefetchLimit}
  --activity-delay-ms <n>
                    Async delay inside each mixed-workload activity. Default: ${defaultOptions.activityDelayMs}
  --batch <n>       Max activations per worker drain. Default: ${defaultOptions.batch}
  --max-rounds <n>  Safety cap for child drain rounds. Default: ${defaultOptions.maxRounds}
  --json            Print machine-readable JSON.
`)
}

function printResult(result: SqliteMultiProcessBenchmarkResult): void {
  process.stdout.write(`Multi-process SQLite durability benchmark
  mode: ${result.mode}
  layout: ${result.sqliteLayout}
  workflows: ${result.options.workflows}
  processes: ${result.processes}
  active processes: ${result.activeProcesses}
  workers/process: ${result.options.workersPerProcess}
  shards/process: ${result.options.shardsPerProcess}
  total shards: ${result.options.processes * result.options.shardsPerProcess}
  committed workers: ${result.committedWorkers}
  activation concurrency: ${result.options.activationConcurrency} per worker
  activation prefetch limit: ${result.options.activationPrefetchLimit}
  batch: ${result.options.batch}
  elapsed: ${formatMs(result.elapsedMs)}
  processing wall estimate: ${formatMs(result.processingMs)}

End-to-end aggregate throughput:
  workflows/sec: ${formatRate(result.workflowsPerSecond)}
  activations/sec: ${formatRate(result.activationsPerSecond)} (${result.activations} activations)
  actions/sec: ${formatRate(result.mixedActionsPerSecond)} (${result.mixedActions} actions)

Processing-only aggregate throughput:
  workflows/sec: ${formatRate(result.processingWorkflowsPerSecond)}
  activations/sec: ${formatRate(result.processingActivationsPerSecond)} (${result.activations} activations)
  actions/sec: ${formatRate(result.processingMixedActionsPerSecond)} (${result.mixedActions} actions)
`)
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
