import { readFile } from "node:fs/promises"
import { describe, expect, it } from "vitest"
import { runSqliteBenchmark, type BenchmarkOptions } from "../src/benchmarks/sqlite.js"

function benchmarkOptions(overrides: Partial<BenchmarkOptions> = {}): BenchmarkOptions {
  return {
    workflows: 6,
    workers: 2,
    shards: 2,
    activationConcurrency: 2,
    activationPrefetchLimit: 32,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    keepDb: false,
    json: true,
    ...overrides,
  }
}

describe("SQLite benchmark", () => {
  it("runs real workflows with pre-appended signals and split timing fields", async () => {
    const result = await runSqliteBenchmark(benchmarkOptions())

    expect(result.completedWorkflows).toBe(6)
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
})
