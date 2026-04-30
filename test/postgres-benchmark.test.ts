import pg from "pg"
import { randomUUID } from "node:crypto"
import { describe, expect, it } from "vitest"
import {
  runPostgresBenchmark,
  type PostgresBenchmarkOptions,
} from "../src/benchmarks/postgres.js"

const { Pool } = pg

const connectionString = process.env.DURABLE_POSTGRES_URL
const describeIfPostgres = connectionString ? describe : describe.skip

function benchmarkOptions(
  overrides: Partial<PostgresBenchmarkOptions> = {},
): PostgresBenchmarkOptions {
  return {
    connectionString: connectionString ?? "postgresql://durable:durable@127.0.0.1:55432/durable",
    schema: `durable_bench_test_${randomUUID().replaceAll("-", "_")}`,
    poolSize: 8,
    workflows: 6,
    workers: 2,
    shards: 2,
    activationConcurrency: 2,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    keepSchema: false,
    json: true,
    ...overrides,
  }
}

describeIfPostgres("Postgres benchmark", () => {
  it("runs the shared workflow workload and drops its schema", async () => {
    const schema = `durable_bench_test_${randomUUID().replaceAll("-", "_")}`
    const result = await runPostgresBenchmark(benchmarkOptions({ schema }))

    expect(result.backend).toBe("postgres")
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
    expect(result.processingActivationsPerSecond).toBeGreaterThan(0)

    const pool = new Pool({ connectionString })
    try {
      const check = await pool.query(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
        [schema],
      )
      expect(check.rowCount).toBe(0)
    } finally {
      await pool.end()
    }
  })
})
