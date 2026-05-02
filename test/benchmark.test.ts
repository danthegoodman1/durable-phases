import { readFile } from "node:fs/promises"
import { describe, expect, it } from "vitest"
import { runSqliteBenchmark, type BenchmarkOptions } from "../src/benchmarks/sqlite.js"
import { runSqliteMultiProcessBenchmark } from "../src/benchmarks/sqlite-processes.js"

function benchmarkOptions(overrides: Partial<BenchmarkOptions> = {}): BenchmarkOptions {
  return {
    mode: "mixed",
    sqliteLayout: "single-file",
    workflows: 6,
    workflowOffset: 0,
    workers: 2,
    shards: 2,
    activationConcurrency: 2,
    activationPrefetchLimit: 32,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    keepDb: false,
    profileQueries: false,
    json: true,
    ...overrides,
  }
}

describe("SQLite benchmark", () => {
  it("runs real workflows with pre-appended signals and split timing fields", async () => {
    const result = await runSqliteBenchmark(benchmarkOptions())

    expect(result.completedWorkflows).toBe(6)
    expect(result.backend).toBe("sqlite")
    expect(result.mode).toBe("mixed")
    expect(result.sqliteLayout).toBe("single-file")
    expect(result.activations).toBe(result.expectedActivations)
    expect(result.expectedActivations).toBe(30)
    expect(result.mixedActions).toBe(48)
    expect(result.counters).toMatchObject({
      workflowStarts: 6,
      signals: 6,
      childStarts: 6,
      childCompletions: 6,
      timerHandlers: 6,
      bootActivities: 6,
      childActivities: 6,
      finishActivities: 6,
    })
    expect(result.setupMs).toBeGreaterThanOrEqual(0)
    expect(result.processingMs).toBeGreaterThan(0)
    expect(result.verifyMs).toBeGreaterThanOrEqual(0)
    expect(result.processingActivationsPerSecond).toBeGreaterThan(0)
    expect(result.dbPath).toBeUndefined()
  })

  it("runs the same benchmark on shard-file SQLite", async () => {
    const result = await runSqliteBenchmark(benchmarkOptions({
      sqliteLayout: "shard-files",
      workflows: 8,
      workers: 4,
      shards: 4,
    }))

    expect(result.sqliteLayout).toBe("shard-files")
    expect(result.completedWorkflows).toBe(8)
    expect(result.activations).toBe(result.expectedActivations)
    expect(result.counters.childStarts).toBe(8)
    expect(result.counters.childCompletions).toBe(8)
  })

  it("reports SQLite query profile and supports isolated benchmark modes", async () => {
    const result = await runSqliteBenchmark(benchmarkOptions({
      mode: "bare",
      workflows: 4,
      profileQueries: true,
    }))

    expect(result.mode).toBe("bare")
    expect(result.expectedActivations).toBe(4)
    expect(result.queryProfile).toMatchObject({
      totalQueries: expect.any(Number),
      byPhase: {
        processing: {
          totalQueries: expect.any(Number),
        },
      },
      topProcessingByTotal: expect.any(Array),
      topProcessingByCount: expect.any(Array),
    })
  })

  it("does not inspect all instances inside the timed processing loop", async () => {
    const source = await readFile(
      new URL("../src/benchmarks/sqlite.ts", import.meta.url),
      "utf8",
    )
    const loopStart = source.indexOf("for (; rounds < options.maxRounds; rounds += 1)")
    const loopEnd = source.indexOf("processingFinishedAt = performance.now()")
    expect(loopStart).toBeGreaterThanOrEqual(0)
    expect(loopEnd).toBeGreaterThan(loopStart)
    expect(source.slice(loopStart, loopEnd)).not.toContain("listInstances")
  })

  it("aggregates shard-file SQLite subprocess benchmark results", async () => {
    const result = await runSqliteMultiProcessBenchmark({
      mode: "bare",
      sqliteLayout: "shard-files",
      workflows: 4,
      processes: 2,
      workersPerProcess: 1,
      shardsPerProcess: 1,
      activationConcurrency: 1,
      activationPrefetchLimit: 2,
      activityDelayMs: 0,
      batch: 2,
      maxRounds: 20,
      json: true,
    })

    expect(result.backend).toBe("sqlite-multiprocess")
    expect(result.sqliteLayout).toBe("shard-files")
    expect(result.completedWorkflows).toBe(4)
    expect(result.activations).toBe(4)
    expect(result.processResults).toHaveLength(2)
  })
})
