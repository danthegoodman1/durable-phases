import pg from "pg"
import { randomUUID } from "node:crypto"
import { describe, expect, it } from "vitest"
import {
  parsePostgresBenchmarkArgs,
  runPostgresBenchmark,
  type PostgresBenchmarkOptions,
} from "../src/benchmarks/postgres.js"
import {
  parsePostgresMultiProcessBenchmarkArgs,
  runPostgresMultiProcessBenchmark,
} from "../src/benchmarks/postgres-processes.js"

const { Pool } = pg

const connectionString = process.env.DURABLE_POSTGRES_URL
const describeIfPostgres = connectionString ? describe : describe.skip

function benchmarkOptions(
  overrides: Partial<PostgresBenchmarkOptions> = {},
): PostgresBenchmarkOptions {
  return {
    mode: "mixed",
    connectionString: connectionString ?? "postgresql://durable:durable@127.0.0.1:55432/durable",
    schema: `durable_bench_test_${randomUUID().replaceAll("-", "_")}`,
    physicalPartitions: 1,
    poolSize: 8,
    workflows: 6,
    workflowOffset: 0,
    workers: 2,
    shards: 2,
    activationConcurrency: 2,
    activationPrefetchLimit: 8,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    diagnose: false,
    diagnosticSampleIntervalMs: 25,
    keepSchema: false,
    profileQueries: false,
    synchronousCommit: "on",
    json: true,
    ...overrides,
  }
}

describeIfPostgres("Postgres benchmark", () => {
  it("runs the shared workflow workload and drops its schema", async () => {
    const schema = `durable_bench_test_${randomUUID().replaceAll("-", "_")}`
    const result = await runPostgresBenchmark(
      benchmarkOptions({ diagnose: true, profileQueries: true, schema }),
    )

    expect(result.backend).toBe("postgres")
    expect(result.mode).toBe("mixed")
    expect(result.correct).toBe(true)
    expect(result.completedWorkflows).toBe(6)
    expect(result.activations).toBe(result.expectedActivations)
    expect(result.expectedActivations).toBe(30)
    expect(result.activeWorkers).toBeGreaterThan(0)
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
    expect(result.queryProfile?.poolWait.connectCount).toBeGreaterThan(0)
    expect(result.queryProfile?.topProcessingByTotal.length).toBeGreaterThan(0)
    expect(result.queryProfile?.topProcessingByCount.length).toBeGreaterThan(0)
    const profileSql = [
      ...(result.queryProfile?.topByCount ?? []),
      ...(result.queryProfile?.topByTotal ?? []),
    ].map((entry) => entry.sql).join("\n")
    const processingSql = [
      ...(result.queryProfile?.topProcessingByCount ?? []),
      ...(result.queryProfile?.topProcessingByTotal ?? []),
    ].map((entry) => entry.sql)
    expect(profileSql).not.toContain("SELECT input.activation_id, i.*")
    expect(processingSql).not.toContain("BEGIN")
    expect(processingSql).not.toContain("COMMIT")
    expect(result.diagnostics?.sampleCount).toBeGreaterThan(0)
    expect(result.diagnostics?.databaseDelta?.xact_commit).toBeGreaterThan(0)

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

  it("supports feature-isolation modes and validates mode flags", async () => {
    expect(() => parsePostgresBenchmarkArgs(["--mode", "fast"]))
      .toThrow("--mode must be mixed, bare, activity, signal, timer, or child")
    expect(parsePostgresBenchmarkArgs([
      "--workflow-offset",
      "5",
      "--dispatch-shards",
      "0,1",
      "--workflow-shards",
      "1",
    ])).toMatchObject({
      workflowOffset: 5,
      dispatchShardIds: [0, 1],
      workflowShardIds: [1],
    })

    const result = await runPostgresBenchmark(
      benchmarkOptions({
        mode: "bare",
        workflows: 4,
        maxRounds: 50,
      }),
    )
    expect(result.mode).toBe("bare")
    expect(result.correct).toBe(true)
    expect(result.completedWorkflows).toBe(4)
    expect(result.expectedActivations).toBe(4)
    expect(result.activations).toBe(4)
  })

  it("aggregates multi-process benchmark results", async () => {
    const result = await runPostgresMultiProcessBenchmark(
      parsePostgresMultiProcessBenchmarkArgs([
        "--mode",
        "bare",
        "--workflows",
        "4",
        "--processes",
        "2",
        "--workers-per-process",
        "1",
        "--shards-per-process",
        "1",
        "--pool-size-per-process",
        "4",
        "--max-rounds",
        "50",
        "--connection-string",
        connectionString!,
      ]),
    )
    expect(result.backend).toBe("postgres-multiprocess")
    expect(result.mode).toBe("bare")
    expect(result.completedWorkflows).toBe(4)
    expect(result.activations).toBe(4)
    expect(result.processResults).toHaveLength(2)
  })
})
