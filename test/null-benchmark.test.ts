import { describe, expect, it } from "vitest"
import { NullDurabilityProvider } from "../src/benchmarks/null-provider.js"
import {
  parseNullBenchmarkArgs,
  runNullBenchmark,
  type NullBenchmarkOptions,
} from "../src/benchmarks/null.js"
import {
  parseNullMultiProcessBenchmarkArgs,
  runNullMultiProcessBenchmark,
  type NullMultiProcessBenchmarkOptions,
} from "../src/benchmarks/null-processes.js"
import { createBenchmarkCounters } from "../src/benchmarks/workload.js"
import type { CommitActivationInput, DurableWait } from "../src/interface.js"

const T0 = "2026-01-01T00:00:00.000Z"
const T1 = "2026-01-01T00:00:01.000Z"
const LONG_LEASE_MS = 60_000

function benchmarkOptions(overrides: Partial<NullBenchmarkOptions> = {}): NullBenchmarkOptions {
  return {
    mode: "mixed",
    workflows: 6,
    workflowOffset: 0,
    workers: 2,
    shards: 2,
    activationConcurrency: 2,
    activationPrefetchLimit: 8,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    unsafeNoClone: false,
    json: true,
    ...overrides,
  }
}

function multiProcessOptions(
  overrides: Partial<NullMultiProcessBenchmarkOptions> = {},
): NullMultiProcessBenchmarkOptions {
  return {
    mode: "mixed",
    workflows: 6,
    processes: 2,
    workersPerProcess: 2,
    shardsPerProcess: 2,
    activationConcurrency: 2,
    activationPrefetchLimit: 8,
    activityDelayMs: 0,
    batch: 4,
    maxRounds: 100,
    unsafeNoClone: false,
    json: true,
    ...overrides,
  }
}

describe("Null durability benchmark", () => {
  it("runs the mixed workflow workload and validates counters", async () => {
    const result = await runNullBenchmark(benchmarkOptions())

    expect(result.backend).toBe("null")
    expect(result.mode).toBe("mixed")
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
  })

  it("runs the bare activation workload", async () => {
    const result = await runNullBenchmark(benchmarkOptions({
      mode: "bare",
      workflows: 10,
    }))

    expect(result.backend).toBe("null")
    expect(result.mode).toBe("bare")
    expect(result.completedWorkflows).toBe(10)
    expect(result.activations).toBe(10)
    expect(result.expectedActivations).toBe(10)
    expect(result.mixedActions).toBe(10)
    expect(result.counters.workflowStarts).toBe(10)
    expect(result.counters.signals).toBe(0)
  })

  it("runs with a workflow offset for subprocess partitioning", async () => {
    const result = await runNullBenchmark(benchmarkOptions({
      mode: "bare",
      workflows: 4,
      workflowOffset: 20,
    }))

    expect(result.completedWorkflows).toBe(4)
    expect(result.activations).toBe(4)
    expect(result.options.workflowOffset).toBe(20)
  })

  it("parses JSON benchmark options and rejects invalid CLI flags", () => {
    expect(parseNullBenchmarkArgs([
      "--mode",
      "bare",
      "--workflows",
      "12",
      "--workflow-offset",
      "8",
      "--workers=3",
      "--shards",
      "3",
      "--activation-concurrency",
      "5",
      "--activation-prefetch-limit",
      "9",
      "--batch",
      "7",
      "--unsafe-no-clone",
      "--json",
    ])).toMatchObject({
      mode: "bare",
      workflows: 12,
      workflowOffset: 8,
      workers: 3,
      shards: 3,
      activationConcurrency: 5,
      activationPrefetchLimit: 9,
      batch: 7,
      unsafeNoClone: true,
      json: true,
    })
    expect(() => parseNullBenchmarkArgs(["--mode", "fast"]))
      .toThrow("--mode must be mixed, bare, activity, signal, timer, or child")
    expect(() => parseNullBenchmarkArgs(["--workflows", "0"])).toThrow("--workflows must be a positive integer")
    expect(() => parseNullBenchmarkArgs(["--workflow-offset", "-1"])).toThrow("--workflow-offset must be a non-negative integer")
    expect(() => parseNullBenchmarkArgs(["--wat"])).toThrow("Unknown benchmark option")
  })

  it("aggregates multi-process null benchmark results", async () => {
    const result = await runNullMultiProcessBenchmark(multiProcessOptions({
      workflows: 7,
      processes: 3,
      mode: "bare",
    }), async (_processIndex, options) => fakeChildResult(options))

    expect(result.backend).toBe("null-multiprocess")
    expect(result.mode).toBe("bare")
    expect(result.processes).toBe(3)
    expect(result.processResults.map((item) => item.workflows)).toEqual([3, 2, 2])
    expect(result.processResults.map((item) => item.workflowOffset)).toEqual([0, 3, 5])
    expect(result.activations).toBe(7)
    expect(result.expectedActivations).toBe(7)
    expect(result.completedWorkflows).toBe(7)
    expect(result.counters.workflowStarts).toBe(7)
    expect(result.processingActivationsPerSecond).toBeGreaterThan(0)
  })

  it.each(["activity", "signal", "timer", "child"] as const)(
    "runs %s feature isolation mode",
    async (mode) => {
      const result = await runNullBenchmark(benchmarkOptions({
        mode,
        workflows: 4,
        workflowOffset: 10,
      }))

      expect(result.backend).toBe("null")
      expect(result.mode).toBe(mode)
      expect(result.completedWorkflows).toBe(4)
      expect(result.activations).toBe(result.expectedActivations)
      expect(result.processingActivationsPerSecond).toBeGreaterThan(0)
    },
  )

  it("parses multi-process benchmark options and rejects invalid flags", () => {
    expect(parseNullMultiProcessBenchmarkArgs([
      "--mode",
      "mixed",
      "--workflows",
      "100",
      "--processes",
      "4",
      "--workers-per-process",
      "2",
      "--shards-per-process",
      "2",
      "--activation-concurrency",
      "8",
      "--activation-prefetch-limit",
      "16",
      "--batch",
      "12",
      "--unsafe-no-clone",
      "--json",
    ])).toMatchObject({
      mode: "mixed",
      workflows: 100,
      processes: 4,
      workersPerProcess: 2,
      shardsPerProcess: 2,
      activationConcurrency: 8,
      activationPrefetchLimit: 16,
      batch: 12,
      unsafeNoClone: true,
      json: true,
    })
    expect(() => parseNullMultiProcessBenchmarkArgs(["--processes", "0"]))
      .toThrow("--processes must be a positive integer")
    expect(() => parseNullMultiProcessBenchmarkArgs(["--mode", "wat"]))
      .toThrow("--mode must be mixed, bare, activity, signal, timer, or child")
  })

  it("requires shard ownership before claiming tasks", async () => {
    const provider = new NullDurabilityProvider()
    await createReadyInstance(provider, "no-shard")

    await expect(
      provider.openShard({ shardId: 0, ownerId: "worker-a", leaseEpoch: 1 }).claimTasks({
        workflows: { null_test: { version: 1 } },
        now: T0,
        leaseMs: LONG_LEASE_MS,
        limit: 1,
      }),
    ).rejects.toThrow("Lost shard lease")
  })

  it("fences stale commits after shard epoch takeover", async () => {
    const provider = new NullDurabilityProvider()
    await createReadyInstance(provider, "stale-epoch")
    const leaseA = await provider.claimShard({
      shardId: 0,
      ownerId: "worker-a",
      now: T0,
      leaseMs: 100,
    })
    expect(leaseA).toMatchObject({ leaseEpoch: 1 })
    const claimA = await provider.openShard(leaseA!).claimTasks({
      workflows: { null_test: { version: 1 } },
      now: T0,
      leaseMs: LONG_LEASE_MS,
      limit: 1,
    })
    expect(claimA.claims).toHaveLength(1)

    const leaseB = await provider.claimShard({
      shardId: 0,
      ownerId: "worker-b",
      now: T1,
      leaseMs: LONG_LEASE_MS,
    })
    expect(leaseB).toMatchObject({ leaseEpoch: 2 })

    await expect(provider.commitCheckpoint({
      workflowId: "stale-epoch",
      runId: "run-1",
      expectedSequence: 0,
      activationId: claimA.claims[0].activation.activationId,
      workerId: "worker-a",
      workflowVersion: 1,
      next: { status: "completed", output: { ok: true } },
      waits: [],
      now: T1,
    })).resolves.toMatchObject({ ok: false, reason: "lost_activation_lease" })
    await expect(provider.loadInstance({ workflowId: "stale-epoch", runId: "run-1" }))
      .resolves.toMatchObject({ status: "running", sequence: 0 })
  })

  it("isolates sibling batch commit conflicts", async () => {
    const provider = new NullDurabilityProvider()
    await createReadyInstance(provider, "sibling-a")
    await createReadyInstance(provider, "sibling-b")
    const lease = await provider.claimShard({
      shardId: 0,
      ownerId: "worker-a",
      now: T0,
      leaseMs: LONG_LEASE_MS,
    })
    const batch = await provider.openShard(lease!).claimTasks({
      workflows: { null_test: { version: 1 } },
      now: T0,
      leaseMs: LONG_LEASE_MS,
      limit: 2,
    })
    expect(batch.claims.map((claim) => claim.activation.workflowId)).toEqual([
      "sibling-a",
      "sibling-b",
    ])

    const results = await provider.commitActivations([
      completeInput(batch.claims[0].activation.activationId, "sibling-a", 1),
      completeInput(batch.claims[1].activation.activationId, "sibling-b", 0),
    ])
    expect(results.results).toEqual([
      expect.objectContaining({ activationId: batch.claims[0].activation.activationId, ok: false }),
      expect.objectContaining({ activationId: batch.claims[1].activation.activationId, ok: true }),
    ])
    await expect(provider.loadInstance({ workflowId: "sibling-a", runId: "run-1" }))
      .resolves.toMatchObject({ status: "running", sequence: 0 })
    await expect(provider.loadInstance({ workflowId: "sibling-b", runId: "run-1" }))
      .resolves.toMatchObject({ status: "completed", sequence: 1 })
  })

  it("requires exact signal consumption and consumes signals once", async () => {
    const provider = new NullDurabilityProvider()
    await createWaitingSignalInstance(provider, "signal-once")
    const signal = await provider.appendSignal({
      workflowId: "signal-once",
      runId: "run-1",
      type: "finish",
      payload: { ok: true },
      receivedAt: T0,
    })
    const lease = await provider.claimShard({
      shardId: 0,
      ownerId: "worker-a",
      now: T0,
      leaseMs: LONG_LEASE_MS,
    })
    const session = provider.openShard(lease!)
    const batch = await session.claimTasks({
      workflows: { null_test: { version: 1 } },
      now: T0,
      leaseMs: LONG_LEASE_MS,
      limit: 1,
    })
    expect(batch.claims).toHaveLength(1)
    const activationId = batch.claims[0].activation.activationId

    await expect(provider.commitCheckpoint({
      ...completeInput(activationId, "signal-once", 0),
      consumeSignalId: "wrong-signal",
    })).resolves.toMatchObject({ ok: false, reason: "activation_event_mismatch" })

    await expect(provider.commitCheckpoint({
      ...completeInput(activationId, "signal-once", 0),
      consumeSignalId: signal.signalId,
    })).resolves.toMatchObject({ ok: true, sequence: 1 })

    const empty = await session.claimTasks({
      workflows: { null_test: { version: 1 } },
      now: T0,
      leaseMs: LONG_LEASE_MS,
      limit: 1,
    })
    expect(empty.claims).toHaveLength(0)
  })
})

async function createReadyInstance(
  provider: NullDurabilityProvider,
  workflowId: string,
): Promise<void> {
  await provider.createInstance({
    workflowName: "null_test",
    workflowVersion: 1,
    workflowId,
    runId: "run-1",
    partitionShard: 0,
    common: {},
    phase: { name: "run", data: {} },
    waits: [runWait()],
    now: T0,
  })
}

async function createWaitingSignalInstance(
  provider: NullDurabilityProvider,
  workflowId: string,
): Promise<void> {
  await provider.createInstance({
    workflowName: "null_test",
    workflowVersion: 1,
    workflowId,
    runId: "run-1",
    partitionShard: 0,
    common: {},
    phase: { name: "waiting", data: {} },
    waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
    now: T0,
  })
}

function completeInput(
  activationId: string,
  workflowId: string,
  expectedSequence: number,
): CommitActivationInput {
  return {
    workflowId,
    runId: "run-1",
    expectedSequence,
    activationId,
    workerId: "worker-a",
    workflowVersion: 1,
    next: { status: "completed", output: { ok: true } },
    waits: [],
    now: T0,
  }
}

function runWait(): DurableWait {
  return { kind: "run", name: "__run", readyAt: T0 }
}

function fakeChildResult(options: NullBenchmarkOptions) {
  const counters = createBenchmarkCounters()
  counters.workflowStarts = options.workflows
  const activations = options.mode === "mixed" ? options.workflows * 5 : options.workflows
  const mixedActions = options.mode === "mixed" ? options.workflows * 8 : activations
  return {
    backend: "null" as const,
    mode: options.mode,
    options,
    elapsedMs: 10,
    setupMs: 1,
    processingMs: 5,
    verifyMs: 1,
    rounds: 1,
    activations,
    expectedActivations: activations,
    completedWorkflows: options.workflows,
    activeWorkers: options.workers,
    mixedActions,
    activationsPerSecond: activations / 0.01,
    mixedActionsPerSecond: mixedActions / 0.01,
    workflowsPerSecond: options.workflows / 0.01,
    processingActivationsPerSecond: activations / 0.005,
    processingMixedActionsPerSecond: mixedActions / 0.005,
    processingWorkflowsPerSecond: options.workflows / 0.005,
    counters,
  }
}
