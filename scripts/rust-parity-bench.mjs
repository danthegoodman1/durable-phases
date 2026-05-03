#!/usr/bin/env node
import { spawnSync } from "node:child_process"

const options = parseArgs(process.argv.slice(2))
const providers =
  options.provider === "all"
    ? ["null", "sqlite", "sqlite-shard-file", "postgres"]
    : [options.provider]
const modes =
  options.mode === "all"
    ? ["mixed", "bare", "activity", "signal", "timer", "child"]
    : [options.mode]

const failures = []
const results = []
for (const provider of providers) {
  for (const mode of modes) {
    const ts = []
    const rust = []
    for (let run = 0; run < options.repeat; run += 1) {
      ts.push(runJson(tsCommand(provider, mode, options)))
      rust.push(runJson(rustCommand(provider, mode, options)))
    }
    if (!ts.every((sample) => sample.correct === true)) {
      failures.push({ provider, mode, runtime: "ts", reason: "correct-not-true" })
    }
    if (!rust.every((sample) => sample.correct === true)) {
      failures.push({ provider, mode, runtime: "rust", reason: "correct-not-true" })
    }
    const tsMedian = median(ts.map(score))
    const rustMedian = median(rust.map(score))
    const ratio = tsMedian === 0 ? 1 : rustMedian / tsMedian
    const row = { provider, mode, tsMedian, rustMedian, ratio }
    results.push(row)
    process.stdout.write(
      `${provider} ${mode}: TS ${tsMedian.toFixed(2)} workflows/s, Rust ${rustMedian.toFixed(2)} workflows/s, ratio ${ratio.toFixed(3)}\n`,
    )
    if (rustMedian < tsMedian) {
      failures.push({ ...row, reason: "rust-slower-than-ts" })
    }
  }
}

if (options.json) {
  process.stdout.write(`${JSON.stringify({ results, failures }, null, 2)}\n`)
}

if (failures.length > 0) {
  process.stderr.write(
    `Rust parity failed for ${failures
      .map((failure) => `${failure.provider}/${failure.mode}`)
      .join(", ")}\n`,
  )
  process.exit(1)
}

function tsCommand(provider, mode, options) {
  const args = [
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
  if (provider === "null") {
    return ["npm", ["run", "benchmark:null", "--", ...args]]
  }
  if (provider === "sqlite") {
    return ["npm", ["run", "benchmark:sqlite", "--", "--sqlite-layout", "single-file", ...args]]
  }
  if (provider === "sqlite-shard-file") {
    return ["npm", ["run", "benchmark:sqlite", "--", "--sqlite-layout", "shard-files", ...args]]
  }
  if (provider === "postgres") {
    return [
      "npm",
      [
        "run",
        "benchmark:postgres",
        "--",
        "--physical-partitions",
        String(options.physicalPartitions),
        ...args,
      ],
    ]
  }
  throw new Error(`unknown provider: ${provider}`)
}

function rustCommand(provider, mode, options) {
  const args = [
    "run",
    "--release",
    "-p",
    "durable",
    "--bin",
    "benchmark",
    "--",
    "--provider",
    provider,
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
    "--physical-partitions",
    String(options.physicalPartitions),
    "--json",
  ]
  return ["cargo", args]
}

function runJson([command, args]) {
  const result = spawnSync(command, args, {
    cwd: new URL("..", import.meta.url),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  })
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed\n${result.stdout}\n${result.stderr}`,
    )
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

function parseArgs(args) {
  const options = {
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
    if (flag === "--provider") {
      options.provider = next()
    } else if (flag === "--mode") {
      options.mode = next()
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
