#!/usr/bin/env node
import { spawnSync } from "node:child_process"

const root = new URL("..", import.meta.url)
const goRoot = new URL("../go/", import.meta.url)
const options = parseArgs(process.argv.slice(2))
const providers =
  options.provider === "all"
    ? ["null", "sqlite", "sqlite-shard-file", "postgres"]
    : [options.provider]
const modes =
  options.mode === "all"
    ? ["mixed", "bare", "activity", "signal", "timer", "child"]
    : [options.mode]
const runtimes =
  options.runtime === "all" ? ["ts", "rust", "go"] : [options.runtime]

const results = []
const failures = []
for (const provider of providers) {
  for (const mode of modes) {
    const row = { provider, mode }
    for (const runtime of runtimes) {
      const samples = []
      for (let run = 0; run < options.repeat; run += 1) {
        samples.push(runJson(commandFor(runtime, provider, mode, options)))
      }
      const medianScore = median(samples.map(score))
      const correct = samples.every((sample) => sample.correct === true)
      row[runtime] = { workflowsPerSecond: medianScore, correct }
      if (!correct) {
        failures.push({ provider, mode, runtime, reason: "correct-not-true" })
      }
    }
    results.push(row)
    process.stdout.write(formatRow(row, runtimes))
  }
}

if (options.json) {
  process.stdout.write(`${JSON.stringify({ results, failures }, null, 2)}\n`)
}
if (failures.length > 0) {
  process.stderr.write(
    `Go parity matrix failed for ${failures
      .map((failure) => `${failure.runtime}/${failure.provider}/${failure.mode}`)
      .join(", ")}\n`,
  )
  process.exit(1)
}

function commandFor(runtime, provider, mode, options) {
  if (runtime === "ts") {
    return tsCommand(provider, mode, options)
  }
  if (runtime === "rust") {
    return rustCommand(provider, mode, options)
  }
  if (runtime === "go") {
    return goCommand(provider, mode, options)
  }
  throw new Error(`unknown runtime: ${runtime}`)
}

function tsCommand(provider, mode, options) {
  const args = commonArgs(mode, options)
  if (provider === "null") {
    return { command: "npm", args: ["run", "benchmark:null", "--", ...args], cwd: root }
  }
  if (provider === "sqlite") {
    return { command: "npm", args: ["run", "benchmark:sqlite", "--", "--sqlite-layout", "single-file", ...args], cwd: root }
  }
  if (provider === "sqlite-shard-file") {
    return { command: "npm", args: ["run", "benchmark:sqlite", "--", "--sqlite-layout", "shard-files", ...args], cwd: root }
  }
  if (provider === "postgres") {
    return {
      command: "npm",
      args: ["run", "benchmark:postgres", "--", "--physical-partitions", String(options.physicalPartitions), ...args],
      cwd: root,
    }
  }
  throw new Error(`unknown provider: ${provider}`)
}

function rustCommand(provider, mode, options) {
  return {
    command: "cargo",
    args: [
      "run",
      "--release",
      "-p",
      "durable",
      "--bin",
      "benchmark",
      "--",
      "--provider",
      provider,
      ...commonArgs(mode, options),
      "--physical-partitions",
      String(options.physicalPartitions),
    ],
    cwd: root,
  }
}

function goCommand(provider, mode, options) {
  return {
    command: "go",
    args: [
      "run",
      "./cmd/durable-bench",
      "--provider",
      provider,
      ...commonArgs(mode, options),
      "--physical-partitions",
      String(options.physicalPartitions),
    ],
    cwd: goRoot,
  }
}

function commonArgs(mode, options) {
  return [
    "--mode",
    mode,
    "--workflows",
    String(options.workflows),
    "--workers",
    String(options.workers),
    "--shards",
    String(options.shards),
    "--activation-concurrency",
    String(options.activationConcurrency),
    "--activation-prefetch-limit",
    String(options.activationPrefetchLimit),
    "--batch",
    String(options.batch),
    "--json",
  ]
}

function runJson({ command, args, cwd }) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  })
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed\n${result.stdout}\n${result.stderr}`)
  }
  const start = result.stdout.indexOf("{")
  const end = result.stdout.lastIndexOf("}")
  if (start < 0 || end < start) {
    throw new Error(`command did not print JSON: ${command} ${args.join(" ")}\n${result.stdout}`)
  }
  return JSON.parse(result.stdout.slice(start, end + 1))
}

function score(result) {
  return Number(result.processingWorkflowsPerSecond ?? result.workflowsPerSecond ?? 0)
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.floor(sorted.length / 2)] ?? 0
}

function formatRow(row, runtimes) {
  const scores = runtimes
    .map((runtime) => {
      const entry = row[runtime]
      return `${runtime.toUpperCase()} ${entry.workflowsPerSecond.toFixed(2)} workflows/s correct=${entry.correct}`
    })
    .join(", ")
  return `${row.provider} ${row.mode}: ${scores}\n`
}

function parseArgs(args) {
  const options = {
    runtime: "all",
    provider: "all",
    mode: "all",
    workflows: 250,
    workers: 4,
    shards: 4,
    activationConcurrency: 4,
    activationPrefetchLimit: 32,
    batch: 32,
    physicalPartitions: 4,
    repeat: 3,
    json: false,
  }
  for (let index = 0; index < args.length; index += 1) {
    const flag = args[index]
    const next = () => {
      const value = args[++index]
      if (!value) {
        throw new Error(`${flag} requires a value`)
      }
      return value
    }
    if (flag === "--runtime") {
      options.runtime = oneOf(next(), ["all", "ts", "rust", "go"], flag)
    } else if (flag === "--provider") {
      options.provider = oneOf(next(), ["all", "null", "sqlite", "sqlite-shard-file", "postgres"], flag)
    } else if (flag === "--mode") {
      options.mode = oneOf(next(), ["all", "mixed", "bare", "activity", "signal", "timer", "child"], flag)
    } else if (flag === "--workflows") {
      options.workflows = positive(next(), flag)
    } else if (flag === "--workers") {
      options.workers = positive(next(), flag)
    } else if (flag === "--shards") {
      options.shards = positive(next(), flag)
    } else if (flag === "--activation-concurrency") {
      options.activationConcurrency = positive(next(), flag)
    } else if (flag === "--activation-prefetch-limit") {
      options.activationPrefetchLimit = positive(next(), flag)
    } else if (flag === "--batch") {
      options.batch = positive(next(), flag)
    } else if (flag === "--physical-partitions") {
      options.physicalPartitions = positive(next(), flag)
    } else if (flag === "--repeat") {
      options.repeat = positive(next(), flag)
    } else if (flag === "--json") {
      options.json = true
    } else {
      throw new Error(`unknown argument: ${flag}`)
    }
  }
  return options
}

function positive(value, flag) {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`)
  }
  return parsed
}

function oneOf(value, allowed, flag) {
  if (!allowed.includes(value)) {
    throw new Error(`${flag} must be one of ${allowed.join(", ")}`)
  }
  return value
}
