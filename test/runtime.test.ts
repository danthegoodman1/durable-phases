import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import { z } from "zod"
import {
  cancel,
  type ChildHandle,
  child,
  type CommitActivationInput,
  type CommitActivationsResult,
  complete,
  defineWorkflow,
  type DurableLogger,
  type DurableMetricTags,
  type DurableMetrics,
  DurableRuntime,
  fail,
  go,
  NonRetryableError,
  phase,
  query,
  signal,
  SqliteShardFileDurabilityProvider,
  SqliteDurabilityProvider,
  start,
  stay,
  timer,
  workflowPartitionShard,
} from "../src/durable.js"

const addMs = (iso: string, ms: number) => new Date(new Date(iso).getTime() + ms).toISOString()

const testStoreDirs = new Set<string>()
const testProviders = new Set<SqliteDurabilityProvider>()

async function storePath(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "durable-poc-"))
  testStoreDirs.add(dir)
  return join(dir, "store.sqlite")
}

function testProvider(
  path: string,
  options: ConstructorParameters<typeof SqliteDurabilityProvider>[1] = {},
): SqliteDurabilityProvider {
  const provider = new SqliteDurabilityProvider(path, options)
  testProviders.add(provider)
  return provider
}

function sqlitePragma(provider: SqliteDurabilityProvider, sql: string): unknown {
  return (provider as unknown as {
    db: { pragma(statement: string, options?: { simple?: boolean }): unknown }
  }).db.pragma(sql, { simple: true })
}

function sqliteScalar<T>(provider: SqliteDurabilityProvider, sql: string): T {
  const row = (provider as unknown as {
    db: { prepare(statement: string): { get(): Record<string, T> } }
  }).db.prepare(sql).get()
  return Object.values(row)[0] as T
}

afterEach(async () => {
  for (const provider of testProviders) {
    provider.close()
  }
  testProviders.clear()

  for (const dir of testStoreDirs) {
    await rm(dir, { force: true, maxRetries: 3, recursive: true, retryDelay: 10 })
  }
  testStoreDirs.clear()
})

function manualClock() {
  let now = new Date("2026-01-01T00:00:00.000Z")

  return {
    clock: () => now,
    advance(ms: number) {
      now = new Date(now.getTime() + ms)
    },
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 250): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error("Timed out waiting for condition")
    }
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
}

async function waitForInstanceStatus(
  provider: SqliteDurabilityProvider,
  ref: { workflowId: string; runId: string },
  status: string,
  timeoutMs = 250,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while ((await provider.loadInstance(ref))?.status !== status) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${ref.workflowId}/${ref.runId} to be ${status}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
}

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve
    reject = promiseReject
  })
  return { promise, resolve, reject }
}

type LogEntry = {
  level: keyof DurableLogger
  event: string
  fields?: Record<string, unknown>
}

type MetricEntry = {
  kind: keyof DurableMetrics
  name: string
  value?: number
  tags?: DurableMetricTags
}

function observabilityCollector(options: { throwLogger?: boolean; throwMetrics?: boolean } = {}) {
  const logs: LogEntry[] = []
  const metrics: MetricEntry[] = []
  const logger = {
    debug: (event: string, fields?: Record<string, unknown>) => {
      if (options.throwLogger) {
        throw new Error("logger failed")
      }
      logs.push({ level: "debug", event, fields })
    },
    info: (event: string, fields?: Record<string, unknown>) => {
      if (options.throwLogger) {
        throw new Error("logger failed")
      }
      logs.push({ level: "info", event, fields })
    },
    warn: (event: string, fields?: Record<string, unknown>) => {
      if (options.throwLogger) {
        throw new Error("logger failed")
      }
      logs.push({ level: "warn", event, fields })
    },
    error: (event: string, fields?: Record<string, unknown>) => {
      if (options.throwLogger) {
        throw new Error("logger failed")
      }
      logs.push({ level: "error", event, fields })
    },
  } satisfies DurableLogger
  const metricSink = {
    counter: (name: string, value?: number, tags?: DurableMetricTags) => {
      if (options.throwMetrics) {
        throw new Error("metrics failed")
      }
      metrics.push({ kind: "counter", name, value, tags })
    },
    histogram: (name: string, value: number, tags?: DurableMetricTags) => {
      if (options.throwMetrics) {
        throw new Error("metrics failed")
      }
      metrics.push({ kind: "histogram", name, value, tags })
    },
    gauge: (name: string, value: number, tags?: DurableMetricTags) => {
      if (options.throwMetrics) {
        throw new Error("metrics failed")
      }
      metrics.push({ kind: "gauge", name, value, tags })
    },
  } satisfies DurableMetrics

  return { logger, metrics: metricSink, logs, metricEntries: metrics }
}

function expectEvents(logs: LogEntry[], events: string[]): void {
  for (const event of events) {
    expect(logs.some((entry) => entry.event === event), `missing event ${event}`).toBe(true)
  }
}

function expectNoHighCardinalityMetricTags(metrics: MetricEntry[]): void {
  const forbidden = [
    "workflowId",
    "runId",
    "activationId",
    "signalId",
    "childId",
    "childRecordId",
    "effectId",
    "attemptId",
    "idempotencyKey",
  ]
  for (const metric of metrics) {
    for (const tag of forbidden) {
      expect(metric.tags ?? {}, `${metric.name} must not tag ${tag}`).not.toHaveProperty(tag)
    }
  }
}

function makeWorkflowSuite(counters = { reminders: 0, processed: 0 }) {
  const TestChildWorkflow = defineWorkflow({
    name: "test_child",
    version: 1,
    input: z.object({ value: z.string() }),
    output: z.object({ value: z.string() }),
    common: z.object({ value: z.string() }),
    initial(input) {
      return start({
        common: { value: input.value },
        phase: "done",
        data: {},
      })
    },
    phases: {
      done: phase({
        run: async ({ common }) => complete({ value: `child:${common.value}` }),
      }),
    },
  })

  const TestWorkflow = defineWorkflow({
    name: "test_parent",
    version: 1,
    input: z.object({
      label: z.string(),
      items: z.array(z.string()),
    }),
    output: z.object({
      processed: z.array(z.string()),
      childValue: z.string(),
      reminders: z.number(),
      finishedAt: z.string(),
    }),
    common: z.object({ label: z.string() }),
    initial(input) {
      return start({
        common: { label: input.label },
        phase: "boot",
        data: { items: input.items },
      })
    },
    queries: {
      progress: query(
        z.object({
          sequence: z.number(),
          status: z.string(),
          phase: z.string().optional(),
          cursor: z.number().optional(),
          reminders: z.number().optional(),
        }),
        ({ sequence, snapshot }) => {
          if (snapshot.status !== "running") {
            return { sequence, status: snapshot.status }
          }

          return {
            sequence,
            status: snapshot.status,
            phase: snapshot.phase.name,
            cursor:
              typeof snapshot.phase.data.cursor === "number"
                ? snapshot.phase.data.cursor
                : undefined,
            reminders:
              typeof snapshot.phase.data.reminders === "number"
                ? snapshot.phase.data.reminders
                : undefined,
          }
        },
      ),
    },
    phases: {
      boot: phase({
        state: z.object({ items: z.array(z.string()) }),
        run: async ({ ctx, data }) => {
          return go("waiting", {
            items: data.items,
            reminders: 0,
            wakeAt: addMs(ctx.now(), 1_000),
          })
        },
      }),

      waiting: phase({
        state: z.object({
          items: z.array(z.string()),
          reminders: z.number(),
          wakeAt: z.string(),
        }),
        on: {
          reminder_due: timer(
            ({ data }) => data.wakeAt,
            async ({ ctx, data }) => {
              await ctx.activity("send_reminder", () => {
                counters.reminders += 1
                return { sent: true }
              })

              return stay({
                reminders: data.reminders + 1,
                wakeAt: addMs(ctx.now(), 1_000),
              })
            },
          ),

          begin: signal(z.object({}), async ({ data }) =>
            go("processing", {
              items: data.items,
              cursor: 0,
              processed: [],
              reminders: data.reminders,
            }),
          ),
        },
      }),

      processing: phase({
        state: z.object({
          items: z.array(z.string()),
          cursor: z.number(),
          processed: z.array(z.string()),
          reminders: z.number(),
        }),
        run: async ({ ctx, data }) => {
          if (data.cursor < data.items.length) {
            const value = await ctx.activity(`process_${data.cursor}`, () => {
              counters.processed += 1
              return `${data.items[data.cursor]}!`
            })

            return stay({
              cursor: data.cursor + 1,
              processed: [...data.processed, value],
            })
          }

          const childHandle = await ctx.child.start("child", TestChildWorkflow, {
            value: data.processed.join(","),
          })

          return go("waiting_child", {
            childHandle,
            processed: data.processed,
            reminders: data.reminders,
          })
        },
      }),

      waiting_child: phase({
        state: z.object({
          childHandle: z.object({
            workflowName: z.string(),
            workflowVersion: z.number(),
            workflowId: z.string(),
            runId: z.string(),
          }),
          processed: z.array(z.string()),
          reminders: z.number(),
        }),
        on: {
          child_done: child(
            ({ data }) => data.childHandle,
            async ({ ctx, data, event }) => {
              if (!event.ok) {
                return go("cooldown", {
                  childValue: "child failed",
                  processed: data.processed,
                  reminders: data.reminders,
                  finishAt: addMs(ctx.now(), 1_000),
                })
              }

              return go("cooldown", {
                childValue: event.output.value,
                processed: data.processed,
                reminders: data.reminders,
                finishAt: addMs(ctx.now(), 1_000),
              })
            },
          ),
        },
      }),

      cooldown: phase({
        state: z.object({
          childValue: z.string(),
          processed: z.array(z.string()),
          reminders: z.number(),
          finishAt: z.string(),
        }),
        on: {
          finish_due: timer(
            ({ data }) => data.finishAt,
            async ({ ctx, data }) =>
              complete({
                childValue: data.childValue,
                processed: data.processed,
                reminders: data.reminders,
                finishedAt: ctx.now(),
              }),
          ),
        },
      }),
    },
  })

  return {
    counters,
    workflows: [TestWorkflow, TestChildWorkflow],
    TestWorkflow,
    TestChildWorkflow,
  }
}

async function runConcurrencyScenario(options: {
  workflowCount: number
  workerCount: number
  shardCount: number
  drainBatch: number
  maxRounds: number
  restartRound?: number
}): Promise<void> {
  const path = await storePath()
  const clock = manualClock()
  const sideEffects = {
    boot: [] as number[],
    child: [] as number[],
    finish: [] as number[],
  }
  const ChildWorkflow = defineWorkflow({
    name: "concurrency_child",
    version: 1,
    input: z.object({ index: z.number() }),
    output: z.object({ childValue: z.number() }),
    common: z.object({ index: z.number() }),
    initial(input) {
      return start({ common: { index: input.index }, phase: "run", data: {} })
    },
    phases: {
      run: phase({
        run: async ({ ctx, common }) => {
          const childValue = await ctx.activity("child_once", () => {
            sideEffects.child.push(common.index)
            return common.index * 10
          })
          return complete({ childValue })
        },
      }),
    },
  })
  const ParentWorkflow = defineWorkflow({
    name: "concurrency_parent",
    version: 1,
    input: z.object({ index: z.number() }),
    output: z.object({
      index: z.number(),
      childValue: z.number(),
      signalValue: z.number(),
      finished: z.boolean(),
    }),
    common: z.object({ index: z.number() }),
    initial(input) {
      return start({ common: { index: input.index }, phase: "boot", data: {} })
    },
    phases: {
      boot: phase({
        run: async ({ ctx, common }) => {
          await ctx.activity("boot_once", () => {
            sideEffects.boot.push(common.index)
            return true
          })
          const handle = await ctx.child.start("child", ChildWorkflow, { index: common.index })
          return go("waiting_child", { handle })
        },
      }),
      waiting_child: phase({
        state: z.object({ handle: z.any() }),
        on: {
          child_done: child(
            ({ data }) => data.handle,
            async ({ event }) =>
              go("waiting_signal", {
                childValue: event.ok ? event.output.childValue : -1,
              }),
          ),
        },
      }),
      waiting_signal: phase({
        state: z.object({ childValue: z.number() }),
        on: {
          finish: signal(z.object({ signalValue: z.number() }), async ({ ctx, data, event }) =>
            go("waiting_timer", {
              childValue: data.childValue,
              signalValue: event.signalValue,
              wakeAt: ctx.now(),
            }),
          ),
        },
      }),
      waiting_timer: phase({
        state: z.object({
          childValue: z.number(),
          signalValue: z.number(),
          wakeAt: z.string(),
        }),
        on: {
          finish_due: timer(
            ({ data }) => data.wakeAt,
            async ({ ctx, common, data }) => {
              await ctx.activity("finish_once", () => {
                sideEffects.finish.push(common.index)
                return true
              })
              return complete({
                index: common.index,
                childValue: data.childValue,
                signalValue: data.signalValue,
                finished: true,
              })
            },
          ),
        },
      }),
    },
  })

  const workflows = [ParentWorkflow, ChildWorkflow]
  const providers = Array.from({ length: options.workerCount }, () => testProvider(path))
  let runtimes = providers.map(
    (provider, index) =>
      new DurableRuntime(provider, {
        clock: clock.clock,
        workflows,
        workerId: `concurrency-worker-${index}`,
        shardCount: options.shardCount,
        dispatchShardIds: dispatchShardIdsForWorker(index, options.workerCount, options.shardCount),
        dispatchLeaseMs: 1_000,
        activationLeaseMs: 1_000,
      }),
  )
  for (let index = 0; index < options.workflowCount; index += 1) {
    await runtimes[0].start(
      ParentWorkflow,
      { index },
      { workflowId: `concurrency-parent-${index}` },
    )
  }

  const signaled = new Set<string>()
  let completed = 0
  for (let round = 0; round < options.maxRounds; round += 1) {
    if (options.restartRound === round) {
      providers[0].close()
      providers[0] = testProvider(path)
      runtimes = providers.map(
        (provider, index) =>
          new DurableRuntime(provider, {
            clock: clock.clock,
            workflows,
            workerId: `concurrency-worker-${index}`,
            shardCount: options.shardCount,
            dispatchShardIds: dispatchShardIdsForWorker(index, options.workerCount, options.shardCount),
            dispatchLeaseMs: 1_000,
            activationLeaseMs: 1_000,
          }),
      )
    }

    await Promise.all(
      runtimes.map((runtime) => runtime.drain({ maxActivations: options.drainBatch })),
    )

    const instances = await providers[0].listInstances()
    const waitingForSignal = instances.filter(
      (instance) =>
        instance.workflowName === ParentWorkflow.name &&
        instance.status === "running" &&
        instance.phase?.name === "waiting_signal" &&
        !signaled.has(instance.workflowId),
    )
    for (const instance of waitingForSignal) {
      signaled.add(instance.workflowId)
      const index = Number(instance.workflowId.split("-").at(-1))
      await runtimes[0].signal(
        ParentWorkflow,
        { workflowId: instance.workflowId, runId: instance.runId },
        "finish",
        { signalValue: index + 1_000 },
      )
    }

    completed = instances.filter(
      (instance) => instance.workflowName === ParentWorkflow.name && instance.status === "completed",
    ).length
    if (completed === options.workflowCount) {
      break
    }
  }

  const instances = await providers[0].listInstances()
  const parents = instances.filter((instance) => instance.workflowName === ParentWorkflow.name)
  expect(parents).toHaveLength(options.workflowCount)
  for (const instance of parents) {
    const index = Number(instance.workflowId.split("-").at(-1))
    expect(instance).toMatchObject({
      status: "completed",
      sequence: 4,
      output: {
        index,
        childValue: index * 10,
        signalValue: index + 1_000,
        finished: true,
      },
    })
  }

  expectExactlyOnce(sideEffects.boot, options.workflowCount, "boot")
  expectExactlyOnce(sideEffects.child, options.workflowCount, "child")
  expectExactlyOnce(sideEffects.finish, options.workflowCount, "finish")

  const signals = await providers[0].listSignals()
  expect(signals).toHaveLength(options.workflowCount)
  expect(signals.every((record) => record.consumedBySequence === 3)).toBe(true)

  const children = await providers[0].listChildren()
  expect(children).toHaveLength(options.workflowCount)
  expect(children.every((record) => record.status === "completed")).toBe(true)
  expect(children.every((record) => record.deliveredBySequence === 2)).toBe(true)

  const completedClaims = (await providers[0].listActivationClaims()).filter(
    (claim) => claim.completedBySequence !== undefined,
  )
  const completedOwners = new Set(
    completedClaims.map((claim) => claim.ownerId).filter((ownerId): ownerId is string => Boolean(ownerId)),
  )
  expect(completedOwners.size).toBeGreaterThan(1)
  const completedBySequence = new Map<string, number>()
  for (const claim of completedClaims) {
    const key = `${claim.workflowId}/${claim.runId}/${claim.sequence}`
    completedBySequence.set(key, (completedBySequence.get(key) ?? 0) + 1)
  }
  expect([...completedBySequence.values()].every((count) => count === 1)).toBe(true)
}

function expectExactlyOnce(values: number[], count: number, label: string): void {
  expect(values, `${label} side effects`).toHaveLength(count)
  expect([...values].sort((left, right) => left - right), `${label} side effects`).toEqual(
    Array.from({ length: count }, (_value, index) => index),
  )
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

function workflowIdForDifferentShard(prefix: string, excludedShard: number, shardCount: number): string {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const workflowId = `${prefix}-${attempt}`
    if (workflowPartitionShard(workflowId, "run-1", shardCount) !== excludedShard) {
      return workflowId
    }
  }
  throw new Error(`Could not find workflow id outside shard ${excludedShard}`)
}

function workflowIdForShard(prefix: string, shard: number, shardCount: number): string {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const workflowId = `${prefix}-${attempt}`
    if (workflowPartitionShard(workflowId, "run-1", shardCount) === shard) {
      return workflowId
    }
  }
  throw new Error(`Could not find workflow id for shard ${shard}`)
}

describe("durable workflow PoC", () => {
  it("dedupes starts by workflow id and paginates workflow runs in both directions", async () => {
    const path = await storePath()
    const clock = manualClock()
    const SeriesWorkflow = defineWorkflow({
      name: "series_workflow",
      version: 1,
      input: z.object({ outcome: z.enum(["running", "completed", "failed", "canceled"]) }),
      output: z.object({ ok: z.boolean() }),
      initial(input) {
        if (input.outcome === "running") {
          return start({ phase: "waiting", data: {} })
        }
        return start({ phase: "finish", data: { outcome: input.outcome } })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
        finish: phase({
          state: z.object({ outcome: z.enum(["completed", "failed", "canceled"]) }),
          run: async ({ data }) => {
            if (data.outcome === "failed") {
              return fail({ message: "failed" })
            }
            if (data.outcome === "canceled") {
              return cancel("canceled")
            }
            return complete({ ok: true })
          },
        }),
      },
    })
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [SeriesWorkflow],
    })

    const running = await runtime.start(SeriesWorkflow, { outcome: "running" }, { workflowId: "running-series" })
    const runningReuse = await runtime.start(SeriesWorkflow, { outcome: "completed" }, { workflowId: "running-series" })
    expect(running).toMatchObject({ workflowId: "running-series", created: true })
    expect(runningReuse).toEqual({ ...running, created: false })

    const completed = await runtime.start(SeriesWorkflow, { outcome: "completed" }, { workflowId: "completed-series" })
    await runtime.drain()
    clock.advance(1_000)
    const afterCompleted = await runtime.start(SeriesWorkflow, { outcome: "running" }, { workflowId: "completed-series" })
    expect(afterCompleted.created).toBe(true)
    expect(afterCompleted.runId).not.toBe(completed.runId)

    clock.advance(1_000)
    const failed = await runtime.start(SeriesWorkflow, { outcome: "failed" }, { workflowId: "failed-series" })
    await runtime.drain()
    clock.advance(1_000)
    const afterFailed = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
      workflowId: "failed-series",
      workflowIdReusePolicy: "failed_only",
    })
    expect(afterFailed.created).toBe(true)
    expect(afterFailed.runId).not.toBe(failed.runId)

    clock.advance(1_000)
    const canceled = await runtime.start(SeriesWorkflow, { outcome: "canceled" }, { workflowId: "canceled-series" })
    await runtime.drain()
    clock.advance(1_000)
    const afterCanceled = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
      workflowId: "canceled-series",
      workflowIdReusePolicy: "failed_only",
    })
    expect(afterCanceled.created).toBe(true)
    expect(afterCanceled.runId).not.toBe(canceled.runId)

    const failedOnlyBlocked = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
      workflowId: "completed-series",
      workflowIdReusePolicy: "failed_only",
    })
    expect(failedOnlyBlocked).toEqual({ ...afterCompleted, created: false })

    const alwaysFirst = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
      workflowId: "always-series",
      workflowIdReusePolicy: "always",
    })
    const alwaysSecond = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
      workflowId: "always-series",
      workflowIdReusePolicy: "always",
    })
    expect(alwaysSecond.created).toBe(true)
    expect(alwaysSecond.runId).not.toBe(alwaysFirst.runId)

    const history: string[] = []
    for (let index = 0; index < 3; index += 1) {
      clock.advance(1_000)
      const ref = await runtime.start(SeriesWorkflow, { outcome: "running" }, {
        workflowId: "history-series",
        workflowIdReusePolicy: "always",
      })
      history.push(ref.runId)
    }
    const firstPage = await runtime.getWorkflowRuns({ id: "history-series", limit: 2 })
    expect(firstPage.runs.map((run) => run.runId)).toEqual(history.slice(0, 2))
    expect(firstPage.cursor).toBeDefined()
    const secondPage = await runtime.getWorkflowRuns({ id: "history-series", cursor: firstPage.cursor, limit: 2 })
    expect(secondPage.runs.map((run) => run.runId)).toEqual(history.slice(2))
    expect(secondPage.cursor).toBeUndefined()
    const tail = await runtime.getWorkflowRuns({ id: "history-series", direction: "desc", limit: 1 })
    expect(tail.runs.map((run) => run.runId)).toEqual([history[2]])
    const reverseRest = await runtime.getWorkflowRuns({
      id: "history-series",
      direction: "desc",
      cursor: tail.cursor,
      limit: 2,
    })
    expect(reverseRest.runs.map((run) => run.runId)).toEqual([history[1], history[0]])
    await expect(runtime.getWorkflowRuns({
      id: "history-series",
      direction: "asc",
      cursor: tail.cursor,
      limit: 1,
    })).rejects.toThrow("Invalid workflow runs cursor")
  })

  it("emits runtime and provider observability without high-cardinality metric tags", async () => {
    const path = await storePath()
    const observed = observabilityCollector()
    const calls: string[] = []
    const ObservedWorkflow = defineWorkflow({
      name: "observed_workflow",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({ value: z.string() }), async ({ ctx, event }) => {
              const value = await ctx.activity("record", () => {
                calls.push(event.value)
                return event.value
              }, { durability: "eager" })
              return complete({ value })
            }),
          },
        }),
      },
    })

    const provider = testProvider(path, {
      logger: observed.logger,
      metrics: observed.metrics,
    })
    const runtime = new DurableRuntime(provider, {
      workflows: [ObservedWorkflow],
      workerId: "observability-worker",
      logger: observed.logger,
      metrics: observed.metrics,
    })
    const ref = await runtime.start(ObservedWorkflow, {}, { workflowId: "observability-workflow" })
    await runtime.signal(ObservedWorkflow, ref, "finish", { value: "done" })
    await runtime.drain()

    expect(calls).toEqual(["done"])
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { value: "done" },
    })
    expectEvents(observed.logs, [
      "workflow.start",
      "workflow.signal",
      "runtime.drain.start",
      "runtime.activation.claimed",
      "runtime.activity.completed",
      "runtime.activation.completed",
      "provider.shard.claim",
      "provider.activation.claim",
      "provider.effect.reserve",
      "provider.effect.complete",
      "provider.checkpoint.commit",
    ])
    expect(observed.logs.find((entry) => entry.event === "runtime.drain.start")?.fields).toMatchObject({
      maxConcurrentActivations: 4,
    })
    expect(observed.metricEntries.some((entry) => entry.name === "durable.runtime.activity")).toBe(true)
    expect(
      observed.metricEntries.some((entry) => entry.name === "durable.runtime.activation.in_flight"),
    ).toBe(true)
    expect(observed.metricEntries.some((entry) => entry.name === "durable.provider.checkpoint")).toBe(true)
    expectNoHighCardinalityMetricTags(observed.metricEntries)
  })

  it("swallows logger and metrics failures without affecting workflow execution", async () => {
    const path = await storePath()
    const throwing = observabilityCollector({ throwLogger: true, throwMetrics: true })
    const QuietWorkflow = defineWorkflow({
      name: "throwing_observability",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.activity("once", () => true)
            return complete({ ok: true })
          },
        }),
      },
    })

    const provider = testProvider(path, {
      logger: throwing.logger,
      metrics: throwing.metrics,
    })
    const runtime = new DurableRuntime(provider, {
      workflows: [QuietWorkflow],
      workerId: "throwing-observability-worker",
      logger: throwing.logger,
      metrics: throwing.metrics,
    })
    const ref = await runtime.start(QuietWorkflow, {}, { workflowId: "throwing-observability" })

    await expect(runtime.drain()).resolves.toEqual({ activations: 1 })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true },
    })
  })

  it("emits provider conflict, timeout retry, child cancel, and abandon observability", async () => {
    const observed = observabilityCollector()
    const provider = testProvider(await storePath(), {
      logger: observed.logger,
      metrics: observed.metrics,
    })
    const ref = await provider.createInstance({
      workflowName: "provider_observability",
      workflowVersion: 1,
      workflowId: "provider-observability",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "provider-observability-worker",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "provider-observability-worker",
        shardIds: [0],
        workflows: { provider_observability: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "run" })

    await expect(
      provider.commitCheckpoint({
        ...ref,
        expectedSequence: 1,
        activationId: activation!.activationId,
        workerId: "provider-observability-worker",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 0 })

    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "provider-observability-worker",
      key: "timeout",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 2, initialIntervalMs: 0 } },
    })
    expect(reservation.status).toBe("reserved")
    await expect(
      provider.heartbeatActivation({
        activationId: activation!.activationId,
        workerId: "provider-observability-worker",
        now: "2026-01-01T00:00:01.000Z",
        leaseMs: 60_000,
      }),
    ).rejects.toThrow("Lost activation lease")
    await provider.releaseDispatchShard({
      shardId: 0,
      ownerId: "provider-observability-worker",
    })

    const WaitingChildWorkflow = defineWorkflow({
      name: "observed_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            done: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const CancelParent = defineWorkflow({
      name: "observed_cancel_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            const handle = await ctx.child.start("child", WaitingChildWorkflow, {}, { durability: "eager" })
            await ctx.child.cancel(handle)
            return complete({ ok: true })
          },
        }),
      },
    })
    const AbandonParent = defineWorkflow({
      name: "observed_abandon_parent",
      version: 1,
      input: z.object({}),
      output: z.object({}),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", WaitingChildWorkflow, {}, { parentClosePolicy: "abandon" })
            return cancel("done")
          },
        }),
      },
    })

    const runtime = new DurableRuntime(provider, {
      workflows: [WaitingChildWorkflow, CancelParent, AbandonParent],
      workerId: "provider-child-observability-worker",
      logger: observed.logger,
      metrics: observed.metrics,
    })
    await runtime.start(CancelParent, {}, { workflowId: "observed-cancel-parent" })
    await runtime.start(AbandonParent, {}, { workflowId: "observed-abandon-parent" })
    await runtime.drain({ maxActivations: 2 })

    expectEvents(observed.logs, [
      "provider.checkpoint.conflict",
      "provider.effect.timeout_retry",
      "provider.child.cancel",
      "provider.child.parent_close_abandon",
    ])
    expectNoHighCardinalityMetricTags(observed.metricEntries)
  })

  it("emits worker sleep observability from runWorker", async () => {
    const observed = observabilityCollector()
    const WorkerSleepWorkflow = defineWorkflow({
      name: "observed_worker_sleep",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: { wakeAt: "2026-01-01T00:00:05.000Z" } })
      },
      phases: {
        waiting: phase({
          state: z.object({ wakeAt: z.string() }),
          on: {
            wake: timer(({ data }) => data.wakeAt, async () => complete({ ok: true })),
          },
        }),
      },
    })
    const clock = manualClock()
    const provider = testProvider(await storePath(), {
      logger: observed.logger,
      metrics: observed.metrics,
    })
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [WorkerSleepWorkflow],
      workerId: "observed-worker-loop",
      logger: observed.logger,
      metrics: observed.metrics,
    })
    const abort = new AbortController()
    await runtime.start(WorkerSleepWorkflow, {}, { workflowId: "observed-worker-sleep" })
    await runtime.runWorker({
      signal: abort.signal,
      minPollIntervalMs: 1,
      maxPollIntervalMs: 10_000,
      jitterRatio: 0,
      sleep: async () => {
        abort.abort()
      },
    })

    expectEvents(observed.logs, [
      "runtime.worker.start",
      "runtime.worker.sleep",
      "runtime.worker.stop",
    ])
    expect(observed.metricEntries.some((entry) => entry.name === "durable.runtime.worker.sleep_ms")).toBe(true)
    expectNoHighCardinalityMetricTags(observed.metricEntries)
  })

  it("processes a deterministic multi-worker SQLite concurrency mix exactly once", async () => {
    await runConcurrencyScenario({
      workflowCount: 24,
      workerCount: 4,
      shardCount: 4,
      drainBatch: 4,
      maxRounds: 120,
    })
  })

  it("validates configured dispatch shard assignments", async () => {
    const provider = testProvider(await storePath())
    expect(
      () =>
        new DurableRuntime(provider, {
          shardCount: 2,
          dispatchShardIds: [0, 0],
        }),
    ).toThrow("dispatchShardIds must not contain duplicates")
    expect(
      () =>
        new DurableRuntime(provider, {
          shardCount: 2,
          dispatchShardIds: [2],
        }),
    ).toThrow("dispatchShardIds must be integers between 0 and 1")
    expect(
      () =>
        new SqliteDurabilityProvider(":memory:", {
          synchronous: "normal",
        } as never),
    ).toThrow("SqliteDurabilityProvider uses fixed SQLite synchronous=FULL")
  })

  it("runShardStep processes only the requested shard", async () => {
    const path = await storePath()
    const clock = manualClock()
    const shardCount = 3
    const StepWorkflow = defineWorkflow({
      name: "step_requested_shard",
      version: 1,
      input: z.object({ shard: z.number() }),
      output: z.object({ shard: z.number() }),
      initial(input) {
        return start({ phase: "finish", data: { shard: input.shard } })
      },
      phases: {
        finish: phase({
          state: z.object({ shard: z.number() }),
          run: async ({ data }) => complete({ shard: data.shard }),
        }),
      },
    })
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [StepWorkflow],
      shardCount,
      dispatchShardIds: [0],
      workerId: "step-worker",
    })

    const refs = await Promise.all(
      Array.from({ length: shardCount }, (_value, shard) =>
        runtime.start(
          StepWorkflow,
          { shard },
          { workflowId: workflowIdForShard(`step-requested-${shard}`, shard, shardCount) },
        ),
      ),
    )
    refs.forEach((ref, shard) => {
      expect(runtime.shardForRef(ref)).toBe(shard)
    })

    await expect(runtime.runShardStep({ shardId: 1, maxActivations: 1 })).resolves.toEqual({
      shardId: 1,
      claimedShard: true,
      activations: 1,
    })

    await expect(provider.loadInstance(refs[1])).resolves.toMatchObject({
      status: "completed",
      sequence: 1,
      output: { shard: 1 },
    })
    await expect(provider.loadInstance(refs[0])).resolves.toMatchObject({
      status: "running",
      sequence: 0,
    })
    await expect(provider.loadInstance(refs[2])).resolves.toMatchObject({
      status: "running",
      sequence: 0,
    })
  })

  it("runShardStep reports when another worker owns the shard", async () => {
    const path = await storePath()
    const clock = manualClock()
    const provider = testProvider(path)
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "existing-owner",
      now: clock.clock().toISOString(),
      leaseMs: 60_000,
    })
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      shardCount: 2,
      workerId: "step-worker",
    })

    await expect(runtime.runShardStep({ shardId: 0, maxActivations: 1 })).resolves.toEqual({
      shardId: 0,
      claimedShard: false,
      activations: 0,
    })
  })

  it("runShardStep returns nextWakeAt for a future timer on the requested shard", async () => {
    const path = await storePath()
    const clock = manualClock()
    const shardCount = 3
    const fireAt = addMs(clock.clock().toISOString(), 5_000)
    const TimerWorkflow = defineWorkflow({
      name: "step_future_timer",
      version: 1,
      input: z.object({ fireAt: z.string() }),
      output: z.object({ firedAt: z.string() }),
      initial(input) {
        return start({ phase: "waiting", data: { fireAt: input.fireAt } })
      },
      phases: {
        waiting: phase({
          state: z.object({ fireAt: z.string() }),
          on: {
            wake: timer(({ data }) => data.fireAt, async ({ event }) =>
              complete({ firedAt: event.firedAt }),
            ),
          },
        }),
      },
    })
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [TimerWorkflow],
      shardCount,
      workerId: "timer-step-worker",
    })
    const workflowId = workflowIdForShard("step-future-timer", 2, shardCount)
    const ref = await runtime.start(TimerWorkflow, { fireAt }, { workflowId })

    await expect(runtime.runShardStep({ shardId: runtime.shardForRef(ref), maxActivations: 1 })).resolves.toEqual({
      shardId: 2,
      claimedShard: true,
      activations: 0,
      nextWakeAt: fireAt,
    })
  })

  it("runShardStep validates shard ids", async () => {
    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, { shardCount: 2 })

    await expect(runtime.runShardStep({ shardId: -1 })).rejects.toThrow(
      "shardId must be an integer between 0 and 1",
    )
    await expect(runtime.runShardStep({ shardId: 2 })).rejects.toThrow(
      "shardId must be an integer between 0 and 1",
    )
    await expect(runtime.runShardStep({ shardId: 0.5 })).rejects.toThrow(
      "shardId must be an integer between 0 and 1",
    )
  })

  it("configures file-backed SQLite with WAL and FULL synchronization", async () => {
    const provider = testProvider(await storePath())

    expect(sqlitePragma(provider, "journal_mode")).toBe("wal")
    expect(sqlitePragma(provider, "synchronous")).toBe(2)
    expect(sqlitePragma(provider, "fullfsync")).toBe(0)
    expect(sqlitePragma(provider, "checkpoint_fullfsync")).toBe(0)
  })

  it("keeps default checkpoint children shard-local with SQLite shard files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "durable-poc-shard-files-"))
    testStoreDirs.add(dir)
    const provider = new SqliteShardFileDurabilityProvider({
      directory: join(dir, "shards"),
      shardCount: 4,
    })

    const ChildWorkflow = defineWorkflow({
      name: "sqlite_shard_file_default_child",
      version: 1,
      input: z.object({ value: z.number() }),
      output: z.object({ value: z.number() }),
      common: z.object({ value: z.number() }),
      initial(input) {
        return start({ common: { value: input.value }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: ({ common }) => complete({ value: common.value }),
        }),
      },
    })

    const ParentWorkflow = defineWorkflow({
      name: "sqlite_shard_file_default_parent",
      version: 1,
      input: z.object({ value: z.number() }),
      output: z.object({ childWorkflowId: z.string() }),
      common: z.object({ value: z.number() }),
      initial(input) {
        return start({ common: { value: input.value }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            const handle = await ctx.child.start("child", ChildWorkflow, { value: common.value })
            return go("waiting", { handle })
          },
        }),
        waiting: phase({
          state: z.object({ handle: z.any() }),
          on: {
            child_done: child(
              ({ data }) => data.handle,
              ({ data }) => complete({ childWorkflowId: data.handle.workflowId }),
            ),
          },
        }),
      },
    })

    try {
      const runtime = new DurableRuntime(provider, {
        workflows: [ParentWorkflow, ChildWorkflow],
        workerId: "sqlite-shard-file-worker",
        shardCount: 4,
        dispatchShardIds: [0, 1, 2, 3],
      })
      const ref = await runtime.start(ParentWorkflow, { value: 7 }, { workflowId: "sqlite-shard-file-parent" })
      await runtime.drain({ maxActivations: 8 })
      const parent = await provider.loadInstance(ref)
      expect(parent).toMatchObject({ status: "completed" })
      const output = parent?.output as { childWorkflowId?: string } | undefined
      expect(output?.childWorkflowId).toBeDefined()
      expect(workflowPartitionShard(output!.childWorkflowId!, "run-1", 4)).toBe(
        workflowPartitionShard(ref.workflowId, ref.runId, 4),
      )
    } finally {
      provider.close()
    }
  })

  it("rejects explicit cross-shard checkpoint children with SQLite shard files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "durable-poc-shard-files-"))
    testStoreDirs.add(dir)
    const provider = new SqliteShardFileDurabilityProvider({
      directory: join(dir, "shards"),
      shardCount: 4,
    })

    const ChildWorkflow = defineWorkflow({
      name: "sqlite_shard_file_cross_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: () => complete({ ok: true }),
        }),
      },
    })

    const ParentWorkflow = defineWorkflow({
      name: "sqlite_shard_file_cross_parent",
      version: 1,
      input: z.object({ childWorkflowId: z.string() }),
      output: z.object({ ok: z.boolean() }),
      initial(input) {
        return start({ phase: "run", data: { childWorkflowId: input.childWorkflowId } })
      },
      phases: {
        run: phase({
          state: z.object({ childWorkflowId: z.string() }),
          run: async ({ ctx, data }) => {
            await ctx.child.start("child", ChildWorkflow, {}, { workflowId: data.childWorkflowId })
            return complete({ ok: true })
          },
        }),
      },
    })

    try {
      const runtime = new DurableRuntime(provider, {
        workflows: [ParentWorkflow, ChildWorkflow],
        workerId: "sqlite-shard-file-cross-worker",
        shardCount: 4,
        dispatchShardIds: [0, 1, 2, 3],
      })
      const parentWorkflowId = "sqlite-shard-file-cross-parent"
      const parentShard = workflowPartitionShard(parentWorkflowId, "run-1", 4)
      const childWorkflowId = workflowIdForDifferentShard("sqlite-shard-file-cross-child", parentShard, 4)
      await runtime.start(ParentWorkflow, { childWorkflowId }, { workflowId: parentWorkflowId })

      await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow(
        "SQLite shard-file provider requires local child workflows to stay on parent shard",
      )
    } finally {
      provider.close()
    }
  })

  it("validates activation concurrency and activation limits", async () => {
    const provider = testProvider(await storePath())
    expect(() => new DurableRuntime(provider, { maxConcurrentActivations: 0 })).toThrow(
      "maxConcurrentActivations must be a positive integer",
    )
    expect(() => new DurableRuntime(provider, { maxConcurrentActivations: 1.5 })).toThrow(
      "maxConcurrentActivations must be a positive integer",
    )

    const runtime = new DurableRuntime(provider)
    await expect(runtime.drain({ maxActivations: 0 })).rejects.toThrow(
      "maxActivations must be a positive integer",
    )
    await expect(runtime.drain({ maxConcurrentActivations: 0 })).rejects.toThrow(
      "maxConcurrentActivations must be a positive integer",
    )
    await expect(runtime.runWorker({ maxActivationsPerDrain: 0 })).rejects.toThrow(
      "maxActivationsPerDrain must be a positive integer",
    )
    await expect(runtime.runWorker({ maxConcurrentActivations: 0 })).rejects.toThrow(
      "maxConcurrentActivations must be a positive integer",
    )
  })

  it("parses DB-loaded snapshots at boundaries only during activation", async () => {
    const counts = {
      common: 0,
      waitingState: 0,
      signal: 0,
      nextState: 0,
      output: 0,
    }
    const BoundaryWorkflow = defineWorkflow({
      name: "boundary_parse",
      version: 1,
      input: z.object({ label: z.string() }),
      output: z.object({ ok: z.boolean() }).superRefine(() => {
        counts.output += 1
      }),
      common: z.object({ label: z.string() }).superRefine(() => {
        counts.common += 1
      }),
      initial(input) {
        return start({
          common: { label: input.label },
          phase: "waiting",
          data: { value: 1 },
        })
      },
      phases: {
        waiting: phase({
          state: z.object({ value: z.number() }).superRefine(() => {
            counts.waitingState += 1
          }),
          on: {
            finish: signal(
              z.object({ inc: z.number() }).superRefine(() => {
                counts.signal += 1
              }),
              async ({ data, event }) => go("run_after_signal", { value: data.value + event.inc }),
            ),
          },
        }),
        run_after_signal: phase({
          state: z.object({ value: z.number() }).superRefine(() => {
            counts.nextState += 1
          }),
          run: async () => complete({ ok: true }),
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [BoundaryWorkflow],
      workerId: "boundary-parse-worker",
    })
    const ref = await runtime.start(BoundaryWorkflow, { label: "Ada" }, { workflowId: "boundary-parse" })
    await runtime.signal(BoundaryWorkflow, ref, "finish", { inc: 2 })
    expect(counts).toMatchObject({
      common: 1,
      waitingState: 1,
      signal: 1,
      nextState: 0,
      output: 0,
    })

    counts.common = 0
    counts.waitingState = 0
    counts.nextState = 0
    counts.output = 0
    const signalParsesAfterAppend = counts.signal

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(counts).toMatchObject({
      common: 0,
      waitingState: 0,
      signal: signalParsesAfterAppend,
      nextState: 1,
      output: 0,
    })

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(counts).toMatchObject({
      common: 0,
      waitingState: 0,
      signal: signalParsesAfterAppend,
      nextState: 1,
      output: 1,
    })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true },
    })
  })

  it("does not persist handler mutations without an explicit transition", async () => {
    const MutationWorkflow = defineWorkflow({
      name: "handler_mutation_isolated",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      common: z.object({ value: z.string() }),
      initial() {
        return start({
          common: { value: "original-common" },
          phase: "waiting",
          data: { value: "original-data" },
        })
      },
      phases: {
        waiting: phase({
          state: z.object({ value: z.string() }),
          on: {
            mutate: signal(z.object({}), async ({ common, data }) => {
              common.value = "mutated-common"
              data.value = "mutated-data"
              return stay()
            }),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [MutationWorkflow],
      workerId: "mutation-isolation-worker",
    })
    const ref = await runtime.start(MutationWorkflow, {}, { workflowId: "mutation-isolated" })
    await runtime.signal(MutationWorkflow, ref, "mutate", {})
    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "running",
      common: { value: "original-common" },
      phase: { name: "waiting", data: { value: "original-data" } },
    })
  })

  const soakIt = process.env.DURABLE_SOAK === "1" ? it : it.skip
  soakIt(
    "soaks multi-worker SQLite concurrency with provider restart",
    async () => {
      await runConcurrencyScenario({
        workflowCount: 96,
        workerCount: 6,
        shardCount: 4,
        drainBatch: 8,
        maxRounds: 240,
        restartRound: 3,
      })
    },
    30_000,
  )

  it("persists the initial snapshot and reloads it from the SQLite provider", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows })

    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a", "b", "c"] },
      { workflowId: "parent-1" },
    )

    const persisted = await provider.loadInstance(ref)
    expect(persisted?.sequence).toBe(0)
    expect(persisted?.phase?.name).toBe("boot")
    expect(persisted?.common).toEqual({ label: "Ada" })
    expect(persisted?.waits).toEqual([{ kind: "run", name: "__run", readyAt: clock.clock().toISOString() }])

    const reloaded = await testProvider(path).loadInstance(ref)
    expect(reloaded?.phase?.name).toBe("boot")
    expect(reloaded?.sequence).toBe(0)
  })

  it("survives restart with a pending timer and commits stay() as a checkpoint", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { counters, workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows,
      workerId: "timer-worker",
    })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a"] },
      { workflowId: "timer-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    expect((await provider.loadInstance(ref))?.phase?.name).toBe("waiting")

    clock.advance(1_000)
    const restartedProvider = testProvider(path)
    const restarted = new DurableRuntime(restartedProvider, {
      clock: clock.clock,
      workflows,
      workerId: "timer-worker",
    })
    await restarted.drain({ maxActivations: 1 })

    const persisted = await restartedProvider.loadInstance(ref)
    expect(persisted?.sequence).toBe(2)
    expect(persisted?.phase?.name).toBe("waiting")
    expect(persisted?.phase?.data.reminders).toBe(1)
    expect(counters.reminders).toBe(1)
  })

  it("persists signals and consumes them atomically with a go() checkpoint", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows, workerId: "signal-worker" })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a", "b"] },
      { workflowId: "signal-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    await runtime.signal(TestWorkflow, ref, "begin", {})
    expect((await provider.listSignals())[0].consumedBySequence).toBeUndefined()

    await runtime.drain({ maxActivations: 1 })

    const persisted = await provider.loadInstance(ref)
    expect(persisted?.sequence).toBe(2)
    expect(persisted?.phase?.name).toBe("processing")
    expect((await provider.listSignals())[0].consumedBySequence).toBe(2)
  })

  it("does not append extra SQLite journal entries for duplicate idempotent signals", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const now = "2026-01-01T00:00:00.000Z"
    const ref = await provider.createInstance({
      workflowName: "sqlite_idempotent_signal",
      workflowVersion: 1,
      workflowId: "sqlite-idempotent-signal",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
      now,
    })

    const first = await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: { index: 1 },
      receivedAt: now,
      idempotencyKey: "request-1",
    })
    await expect(
      provider.appendSignal({
        ...ref,
        type: "finish",
        payload: { index: 99 },
        receivedAt: "2026-01-01T00:00:01.000Z",
        idempotencyKey: "request-1",
      }),
    ).resolves.toEqual(first)
    const second = await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: { index: 2 },
      receivedAt: "2026-01-01T00:00:02.000Z",
      idempotencyKey: "request-2",
    })

    expect(sqliteScalar<number>(provider, "SELECT count(*) AS count FROM shard_journal")).toBe(3)
    provider.close()

    const restarted = testProvider(path)
    await expect(restarted.listSignals()).resolves.toEqual([first, second])
  })

  it("uses stay() for the bounded unbound-loop pattern", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { counters, workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows,
      workerId: "loop-worker",
    })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a", "b", "c"] },
      { workflowId: "loop-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    await runtime.signal(TestWorkflow, ref, "begin", {})
    await runtime.drain({ maxActivations: 1 })
    await runtime.drain({ maxActivations: 3 })

    const persisted = await provider.loadInstance(ref)
    expect(persisted?.phase?.name).toBe("processing")
    expect(persisted?.phase?.data.cursor).toBe(3)
    expect(persisted?.phase?.data.processed).toEqual(["a!", "b!", "c!"])
    expect(persisted?.sequence).toBe(5)
    expect(counters.processed).toBe(3)
  })

  it("wakes a parent from a completed child after reconstructing the runtime", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows,
      workerId: "child-worker",
    })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a", "b", "c"] },
      { workflowId: "child-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    await runtime.signal(TestWorkflow, ref, "begin", {})
    await runtime.drain({ maxActivations: 5 })
    expect((await provider.loadInstance(ref))?.phase?.name).toBe("waiting_child")

    const restartedProvider = testProvider(path)
    const restarted = new DurableRuntime(restartedProvider, {
      clock: clock.clock,
      workflows,
      workerId: "child-worker",
    })
    await restarted.drain({ maxActivations: 2 })

    const parent = await restartedProvider.loadInstance(ref)
    const children = await restartedProvider.listChildren()
    expect(children[0].status).toBe("completed")
    expect(children[0].deliveredBySequence).toBe(parent?.sequence)
    expect(parent?.phase?.name).toBe("cooldown")
    expect(parent?.phase?.data.childValue).toBe("child:a!,b!,c!")

    const beforeQuery = parent?.sequence
    const progress = await restarted.query(TestWorkflow, ref, "progress")
    expect(progress.sequence).toBe(beforeQuery)
    expect((await restartedProvider.loadInstance(ref))?.sequence).toBe(beforeQuery)

    clock.advance(1_000)
    await restarted.drain({ maxActivations: 1 })
    const completed = await restartedProvider.loadInstance(ref)
    expect(completed?.status).toBe("completed")
    expect(completed?.output).toMatchObject({
      childValue: "child:a!,b!,c!",
      processed: ["a!", "b!", "c!"],
      reminders: 0,
    })
  })

  it("memoizes completed activities across a failed activation retry", async () => {
    const path = await storePath()
    const clock = manualClock()
    const calls = { activity: 0, shouldThrow: true }
    const UnstableWorkflow = defineWorkflow({
      name: "unstable",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({
          phase: "unstable",
          data: {},
        })
      },
      phases: {
        unstable: phase({
          run: async ({ ctx }) => {
            const result = await ctx.activity("side_effect_once", () => {
              calls.activity += 1
              return { ok: true }
            }, { durability: "eager" })

            if (calls.shouldThrow) {
              calls.shouldThrow = false
              throw new Error("boom after durable effect")
            }

            return complete(result)
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [UnstableWorkflow],
      workerId: "unstable-worker",
    })
    const ref = await runtime.start(UnstableWorkflow, {}, { workflowId: "unstable-1" })

    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("boom after durable effect")
    expect(calls.activity).toBe(1)
    expect((await provider.loadInstance(ref))?.sequence).toBe(0)

    const restartedProvider = testProvider(path)
    const restarted = new DurableRuntime(restartedProvider, {
      clock: clock.clock,
      workflows: [UnstableWorkflow],
      workerId: "unstable-worker",
    })
    await restarted.drain({ maxActivations: 1 })

    expect(calls.activity).toBe(1)
    expect(await restartedProvider.loadInstance(ref)).toMatchObject({
      status: "completed",
      sequence: 1,
      output: { ok: true },
    })
  })

  it("passes manual activity heartbeat context and preserves old no-arg activity calls", async () => {
    const path = await storePath()
    const clock = manualClock()
    const observed: Array<{
      attempt: number
      idempotencyKey: string
      heartbeatDetails: unknown
      aborted: boolean
    }> = []
    let oldStyleCalls = 0
    const ActivityContextWorkflow = defineWorkflow({
      name: "activity_context",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean(), value: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const oldValue = await ctx.activity("old_style", () => {
              oldStyleCalls += 1
              return "old"
            })
            const value = await ctx.activity(
              "with_context",
              async (activity) => {
                observed.push({
                  attempt: activity.attempt,
                  idempotencyKey: activity.idempotencyKey,
                  heartbeatDetails: activity.heartbeatDetails,
                  aborted: activity.signal.aborted,
                })
                await activity.heartbeat({ step: 1 })
                return `${oldValue}:new`
              },
              {
                startToCloseTimeoutMs: 5_000,
                heartbeatTimeoutMs: 1_000,
              },
            )
            return complete({ ok: true, value })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [ActivityContextWorkflow],
      workerId: "activity-context-worker",
    })
    const ref = await runtime.start(ActivityContextWorkflow, {}, { workflowId: "activity-context" })

    await runtime.drain()

    expect(oldStyleCalls).toBe(1)
    expect(observed).toEqual([
      {
        attempt: 1,
        idempotencyKey: `activity-context/${ref.runId}/activity-context/${ref.runId}/0/run/__run/with_context`,
        heartbeatDetails: undefined,
        aborted: false,
      },
    ])
    expect("effects" in (await provider.loadInstance(ref))!).toBe(false)
    const effects = (await provider.loadInstance(ref, { includeEffects: true }))?.effects ?? []
    expect(effects.find((effect) => effect.key === "with_context")).toBeUndefined()
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true, value: "old:new" },
    })
  })

  it("keeps default local activity results checkpoint-durable and re-executes after pre-commit failure", async () => {
    const path = await storePath()
    let calls = 0
    let shouldThrow = true
    const CheckpointActivityWorkflow = defineWorkflow({
      name: "checkpoint_activity_default",
      version: 1,
      input: z.object({}),
      output: z.object({ first: z.number(), second: z.number() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const first = await ctx.activity("local_once", () => {
              calls += 1
              return calls
            })
            const second = await ctx.activity("local_once", () => {
              calls += 1
              return calls
            })
            if (shouldThrow) {
              shouldThrow = false
              throw new Error("crash before checkpoint")
            }
            return complete({ first, second })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      workflows: [CheckpointActivityWorkflow],
      workerId: "checkpoint-activity-worker-a",
    })
    const ref = await runtime.start(
      CheckpointActivityWorkflow,
      {},
      { workflowId: "checkpoint-activity-default" },
    )

    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("crash before checkpoint")
    expect(calls).toBe(1)
    await expect(provider.loadInstance(ref, { includeEffects: true })).resolves.toMatchObject({
      sequence: 0,
      effects: [],
    })

    const secondRuntime = new DurableRuntime(testProvider(path), {
      workflows: [CheckpointActivityWorkflow],
      workerId: "checkpoint-activity-worker-b",
    })
    await secondRuntime.drain({ maxActivations: 1 })

    expect(calls).toBe(2)
    await expect(provider.loadInstance(ref, { includeEffects: true })).resolves.toMatchObject({
      status: "completed",
      output: { first: 2, second: 2 },
      effects: [],
    })
  })

  it("retries ordinary activity failures with default exponential backoff", async () => {
    const path = await storePath()
    const clock = manualClock()
    const attempts: number[] = []
    const RetryWorkflow = defineWorkflow({
      name: "activity_retry_default",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const value = await ctx.activity("flaky", ({ attempt }) => {
              attempts.push(attempt)
              if (attempt < 3) {
                throw new Error(`attempt ${attempt}`)
              }
              return { ok: true }
            })
            return complete(value)
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [RetryWorkflow],
      workerId: "retry-default-worker",
    })
    const ref = await runtime.start(RetryWorkflow, {}, { workflowId: "activity-retry-default" })

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(attempts).toEqual([1])
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "pending",
      attempt: 2,
      maxAttempts: 3,
      initialIntervalMs: 1_000,
      maxIntervalMs: 30_000,
      backoffCoefficient: 2,
      nextAttemptAt: "2026-01-01T00:00:01.000Z",
      lastFailure: { message: "attempt 1", name: "Error" },
    })

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({
      activations: 0,
      nextWakeAt: "2026-01-01T00:00:01.000Z",
    })
    expect(attempts).toEqual([1])

    clock.advance(1_000)
    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(attempts).toEqual([1, 2])
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "pending",
      attempt: 3,
      nextAttemptAt: "2026-01-01T00:00:03.000Z",
      lastFailure: { message: "attempt 2", name: "Error" },
    })

    clock.advance(2_000)
    await runtime.drain({ maxActivations: 1 })
    expect(attempts).toEqual([1, 2, 3])
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true },
    })
  })

  it("honors custom retry backoff and terminally fails when max elapsed time is exceeded", async () => {
    const path = await storePath()
    const clock = manualClock()
    const attempts: number[] = []
    const RetryWindowWorkflow = defineWorkflow({
      name: "activity_retry_window",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.activity(
              "always_fails",
              ({ attempt }) => {
                attempts.push(attempt)
                throw new Error(`boom ${attempt}`)
              },
              {
                retry: {
                  maxAttempts: 5,
                  initialIntervalMs: 1_000,
                  backoffCoefficient: 2,
                  maxIntervalMs: 5_000,
                  maxElapsedMs: 1_500,
                },
              },
            )
            return complete({ ok: true })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [RetryWindowWorkflow],
      workerId: "retry-window-worker",
    })
    const ref = await runtime.start(RetryWindowWorkflow, {}, { workflowId: "activity-retry-window" })

    await runtime.drain({ maxActivations: 1 })
    expect(attempts).toEqual([1])
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      attempt: 2,
      nextAttemptAt: "2026-01-01T00:00:01.000Z",
      maxElapsedMs: 1_500,
    })

    clock.advance(1_000)
    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("boom 2")
    expect(attempts).toEqual([1, 2])
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "failed",
      attempt: 2,
      lastFailure: { message: "boom 2", name: "Error" },
    })
  })

  it("terminally fails ordinary activity errors after max attempts are exhausted", async () => {
    const path = await storePath()
    const attempts: number[] = []
    const MaxAttemptsWorkflow = defineWorkflow({
      name: "activity_max_attempts",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.activity(
              "limited",
              ({ attempt }) => {
                attempts.push(attempt)
                throw new Error(`still failing ${attempt}`)
              },
              { retry: { maxAttempts: 2, initialIntervalMs: 0 } },
            )
            return complete({ ok: true })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      workflows: [MaxAttemptsWorkflow],
      workerId: "max-attempts-worker",
    })
    const ref = await runtime.start(MaxAttemptsWorkflow, {}, { workflowId: "activity-max-attempts" })

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("still failing 2")
    expect(attempts).toEqual([1, 2])
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "failed",
      attempt: 2,
      nextAttemptAt: undefined,
      error: { message: "still failing 2", name: "Error" },
    })
  })

  it("does not retry NonRetryableError or AbortError activity failures", async () => {
    const path = await storePath()
    const nonRetryableCalls: number[] = []
    const abortCalls: number[] = []
    class FatalActivityError extends NonRetryableError {}
    const NonRetryableWorkflow = defineWorkflow({
      name: "activity_non_retryable",
      version: 1,
      input: z.object({ mode: z.enum(["non_retryable", "abort"]) }),
      output: z.object({ ok: z.boolean() }),
      initial(input) {
        return start({ phase: "run", data: { mode: input.mode } })
      },
      phases: {
        run: phase({
          state: z.object({ mode: z.enum(["non_retryable", "abort"]) }),
          run: async ({ ctx, data }) => {
            await ctx.activity(
              data.mode,
              ({ attempt }) => {
                if (data.mode === "non_retryable") {
                  nonRetryableCalls.push(attempt)
                  throw new FatalActivityError("validation failed")
                }
                abortCalls.push(attempt)
                const error = new Error("timed out locally")
                error.name = "AbortError"
                throw error
              },
              { retry: { maxAttempts: 5, initialIntervalMs: 0 } },
            )
            return complete({ ok: true })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      workflows: [NonRetryableWorkflow],
      workerId: "non-retryable-worker",
    })
    const nonRetryableRef = await runtime.start(
      NonRetryableWorkflow,
      { mode: "non_retryable" },
      { workflowId: "activity-non-retryable" },
    )
    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("validation failed")
    expect(nonRetryableCalls).toEqual([1])
    expect((await provider.loadInstance(nonRetryableRef, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "failed",
      attempt: 1,
      nextAttemptAt: undefined,
      error: { name: "FatalActivityError", message: "validation failed" },
    })

    const abortProvider = testProvider(await storePath())
    const abortRuntime = new DurableRuntime(abortProvider, {
      workflows: [NonRetryableWorkflow],
      workerId: "abort-non-retryable-worker",
    })
    const abortRef = await abortRuntime.start(
      NonRetryableWorkflow,
      { mode: "abort" },
      { workflowId: "activity-abort-non-retryable" },
    )
    await expect(abortRuntime.drain({ maxActivations: 1 })).rejects.toThrow("timed out locally")
    expect(abortCalls).toEqual([1])
    expect((await abortProvider.loadInstance(abortRef, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "failed",
      attempt: 1,
      nextAttemptAt: undefined,
      error: { name: "AbortError", message: "timed out locally" },
    })
  })

  it("reclaims a missed-heartbeat activity quickly and retries with prior heartbeat details", async () => {
    const path = await storePath()
    const clock = manualClock()
    const attempts: Array<{
      attempt: number
      attemptMarker: string
      idempotencyKey: string
      heartbeatDetails: unknown
      signal: AbortSignal
    }> = []
    const HeartbeatWorkflow = defineWorkflow({
      name: "heartbeat_retry",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const value = await ctx.activity(
              "download",
              async (activity) => {
                attempts.push({
                  attempt: activity.attempt,
                  attemptMarker: activity.idempotencyKey.endsWith("/download")
                    ? `${activity.attempt}:${activity.idempotencyKey}`
                    : "unexpected",
                  idempotencyKey: activity.idempotencyKey,
                  heartbeatDetails: activity.heartbeatDetails,
                  signal: activity.signal,
                })
                if (activity.attempt === 1) {
                  await activity.heartbeat({ bytes: 128 })
                  await new Promise((_resolve, reject) => {
                    activity.signal.addEventListener(
                      "abort",
                      () => reject(activity.signal.reason ?? new Error("aborted")),
                      { once: true },
                    )
                  })
                }
                return `resumed:${(activity.heartbeatDetails as { bytes?: number } | undefined)?.bytes}`
              },
              {
                heartbeatTimeoutMs: 1_000,
                startToCloseTimeoutMs: 30_000,
                retry: { maxAttempts: 2, initialIntervalMs: 0 },
              },
            )
            return complete({ value })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [HeartbeatWorkflow],
      workerId: "heartbeat-worker-a",
      leaseHeartbeatIntervalMs: 1,
      activationLeaseMs: 60_000,
      dispatchLeaseMs: 60_000,
    })
    const ref = await workerA.start(HeartbeatWorkflow, {}, { workflowId: "heartbeat-retry" })
    const firstDrain = workerA.drain({ maxActivations: 1 })
    await waitFor(() => attempts.length === 1)

    clock.advance(1_000)
    await expect(firstDrain).rejects.toThrow("Lost activation lease")
    await waitFor(() => attempts[0].signal.aborted)

    const workerB = new DurableRuntime(testProvider(path), {
      clock: clock.clock,
      workflows: [HeartbeatWorkflow],
      workerId: "heartbeat-worker-b",
      activationLeaseMs: 60_000,
      dispatchLeaseMs: 60_000,
    })
    await workerB.drain({ maxActivations: 1 })

    expect(attempts).toHaveLength(2)
    expect(attempts[1]).toMatchObject({
      attempt: 2,
      idempotencyKey: attempts[0].idempotencyKey,
      heartbeatDetails: { bytes: 128 },
    })
    expect(attempts[1].attemptMarker).not.toBe(attempts[0].attemptMarker)
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { value: "resumed:128" },
    })
  })

  it("does not terminally fail an activity when worker shutdown aborts local execution", async () => {
    const path = await storePath()
    const calls: Array<{ attempt: number; aborted: boolean }> = []
    const ShutdownWorkflow = defineWorkflow({
      name: "activity_shutdown",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const value = await ctx.activity("cooperative", async (activity) => {
              calls.push({ attempt: activity.attempt, aborted: activity.signal.aborted })
              if (calls.length === 1) {
                await new Promise((_resolve, reject) => {
                  activity.signal.addEventListener(
                    "abort",
                    () => reject(activity.signal.reason ?? new Error("aborted")),
                    { once: true },
                  )
                })
              }
              return "after-shutdown"
            }, { durability: "eager" })
            return complete({ value })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      workflows: [ShutdownWorkflow],
      workerId: "shutdown-worker-a",
      activationLeaseMs: 60_000,
      dispatchLeaseMs: 60_000,
      leaseHeartbeatIntervalMs: 1,
    })
    const ref = await workerA.start(ShutdownWorkflow, {}, { workflowId: "activity-shutdown" })
    const abort = new AbortController()
    const running = workerA.runWorker({
      maxActivationsPerDrain: 1,
      minPollIntervalMs: 1,
      maxPollIntervalMs: 1,
      jitterRatio: 0,
      signal: abort.signal,
    })
    await waitFor(() => calls.length === 1)

    abort.abort()
    await expect(running).resolves.toEqual({ activations: 0 })
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      key: "cooperative",
      status: "pending",
    })

    const workerB = new DurableRuntime(testProvider(path), {
      workflows: [ShutdownWorkflow],
      workerId: "shutdown-worker-b",
      activationLeaseMs: 60_000,
      dispatchLeaseMs: 60_000,
    })
    await workerB.drain({ maxActivations: 1 })

    expect(calls).toEqual([
      { attempt: 1, aborted: false },
      { attempt: 1, aborted: false },
    ])
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { value: "after-shutdown" },
    })
  })

  it("enforces start-to-close timeout independently of heartbeats and fences stale attempts", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "start_to_close_activity",
      workflowVersion: 1,
      workflowId: "start-to-close-activity",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { start_to_close_activity: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "run" })

    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "slow",
      now: "2026-01-01T00:00:00.000Z",
      options: {
        startToCloseTimeoutMs: 1_000,
        heartbeatTimeoutMs: 5_000,
        retry: { maxAttempts: 2, initialIntervalMs: 0 },
      },
    })
    expect(reservation.status).toBe("reserved")
    if (reservation.status !== "reserved") {
      throw new Error("expected reserved effect")
    }

    await provider.heartbeatEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      effectId: reservation.effectId,
      attemptId: reservation.attemptId,
      now: "2026-01-01T00:00:00.500Z",
      details: { bytes: 256 },
    })
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      attempt: 1,
      startToCloseDeadline: "2026-01-01T00:00:01.000Z",
      heartbeatDeadline: "2026-01-01T00:00:05.500Z",
      heartbeatDetails: { bytes: 256 },
    })

    await expect(
      provider.heartbeatActivation({
        activationId: activation!.activationId,
        workerId: "worker-a",
        now: "2026-01-01T00:00:01.000Z",
        leaseMs: 60_000,
      }),
    ).rejects.toThrow("Lost activation lease")
    const timedOut = (await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]
    expect(timedOut).toMatchObject({
      status: "pending",
      attempt: 2,
      heartbeatDetails: { bytes: 256 },
      timedOutAt: "2026-01-01T00:00:01.000Z",
      timeoutKind: "start_to_close",
    })
    expect(timedOut?.attemptStartedAt).toBeUndefined()

    await expect(
      provider.completeEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: reservation.effectId,
        attemptId: reservation.attemptId,
        result: { ok: false },
        now: "2026-01-01T00:00:01.000Z",
      }),
    ).rejects.toThrow("Lost activation lease")

    await provider.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:00:01.001Z",
      leaseMs: 60_000,
    })
    const reclaimed = (
      await provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { start_to_close_activity: { version: 1 } },
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(reclaimed?.activationId).toBe(activation!.activationId)
    const retry = await provider.getOrReserveEffect({
      ...ref,
      activationId: reclaimed!.activationId,
      workerId: "worker-b",
      key: "slow",
      now: "2026-01-01T00:00:01.001Z",
      options: {
        startToCloseTimeoutMs: 1_000,
        heartbeatTimeoutMs: 5_000,
        retry: { maxAttempts: 2, initialIntervalMs: 0 },
      },
    })
    expect(retry).toMatchObject({
      status: "reserved",
      attempt: 2,
      idempotencyKey: reservation.idempotencyKey,
      heartbeatDetails: { bytes: 256 },
    })
    if (retry.status !== "reserved") {
      throw new Error("expected retried effect")
    }
    expect(retry.attemptId).not.toBe(reservation.attemptId)
  })

  it("fails an activity effect terminally when heartbeat timeout attempts are exhausted", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "heartbeat_exhaustion",
      workflowVersion: 1,
      workflowId: "heartbeat-exhaustion",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { heartbeat_exhaustion: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "one-shot",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 1 } },
    })
    expect(reservation.status).toBe("reserved")

    await expect(
      provider.heartbeatActivation({
        activationId: activation!.activationId,
        workerId: "worker-a",
        now: "2026-01-01T00:00:01.000Z",
        leaseMs: 60_000,
      }),
    ).rejects.toThrow("Lost activation lease")
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "failed",
      timeoutKind: "heartbeat",
      error: {
        name: "ActivityTimeoutError",
        message: "Activity one-shot failed due to heartbeat timeout",
      },
    })

    await provider.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:00:01.001Z",
      leaseMs: 60_000,
    })
    const reclaimed = (
      await provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { heartbeat_exhaustion: { version: 1 } },
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 60_000,
      })
    ).activation
    const failed = await provider.getOrReserveEffect({
      ...ref,
      activationId: reclaimed!.activationId,
      workerId: "worker-b",
      key: "one-shot",
      now: "2026-01-01T00:00:01.001Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 1 } },
    })
    expect(failed).toMatchObject({
      status: "failed",
      error: { name: "ActivityTimeoutError" },
    })
  })

  it("does not time out activities with no activity timeout options", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "no_activity_timeout",
      workflowVersion: 1,
      workflowId: "no-activity-timeout",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 120_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { no_activity_timeout: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 120_000,
      })
    ).activation
    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "untimed",
      now: "2026-01-01T00:00:00.000Z",
    })
    expect(reservation).toMatchObject({ status: "reserved", attempt: 1 })
    if (reservation.status !== "reserved") {
      throw new Error("expected reserved effect")
    }

    await provider.heartbeatActivation({
      activationId: activation!.activationId,
      workerId: "worker-a",
      now: "2026-01-01T00:01:00.000Z",
      leaseMs: 120_000,
    })
    await provider.completeEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      effectId: reservation.effectId,
      attemptId: reservation.attemptId,
      result: { ok: true },
      now: "2026-01-01T00:01:00.000Z",
    })
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "completed",
      startToCloseDeadline: undefined,
      heartbeatDeadline: undefined,
    })
  })

  it("does not let a worker without a shard lease expire another worker activity timeout", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "scoped_activity_expiration",
      workflowVersion: 1,
      workflowId: "scoped-activity-expiration",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { scoped_activity_expiration: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "owned-by-a",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 1 } },
    })
    expect(reservation.status).toBe("reserved")

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { scoped_activity_expiration: { version: 1 } },
        now: "2026-01-01T00:00:01.000Z",
        leaseMs: 60_000,
      }),
    ).resolves.toEqual({ activation: null })
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      status: "pending",
      attempt: 1,
      attemptOwnerId: "worker-a",
      attemptStartedAt: "2026-01-01T00:00:00.000Z",
      heartbeatDeadline: "2026-01-01T00:00:01.000Z",
    })
    await expect(
      provider.heartbeatActivation({
        activationId: activation!.activationId,
        workerId: "worker-a",
        now: "2026-01-01T00:00:00.500Z",
        leaseMs: 60_000,
      }),
    ).resolves.toBeUndefined()
  })

  it("only consumes timeout attempts for effects whose own deadline expired", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "parallel_activity_timeout",
      workflowVersion: 1,
      workflowId: "parallel-activity-timeout",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { parallel_activity_timeout: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    const short = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "short",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 1 } },
    })
    const long = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "long",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 10_000, retry: { maxAttempts: 1 } },
    })
    expect(short.status).toBe("reserved")
    expect(long.status).toBe("reserved")
    if (short.status !== "reserved" || long.status !== "reserved") {
      throw new Error("expected reserved effects")
    }

    await expect(
      provider.heartbeatActivation({
        activationId: activation!.activationId,
        workerId: "worker-a",
        now: "2026-01-01T00:00:01.000Z",
        leaseMs: 60_000,
      }),
    ).rejects.toThrow("Lost activation lease")
    const effects = (await provider.loadInstance(ref, { includeEffects: true }))?.effects ?? []
    expect(effects.find((effect) => effect.key === "short")).toMatchObject({
      status: "failed",
      attempt: 1,
      timeoutKind: "heartbeat",
    })
    const longAfterTimeout = effects.find((effect) => effect.key === "long")
    expect(longAfterTimeout).toMatchObject({
      status: "pending",
      attempt: 1,
      attemptOwnerId: undefined,
      attemptStartedAt: undefined,
      heartbeatDeadline: undefined,
    })
    expect(longAfterTimeout?.timeoutKind).toBeUndefined()
    expect(longAfterTimeout?.attemptId).not.toBe(long.attemptId)

    await provider.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:00:01.001Z",
      leaseMs: 60_000,
    })
    const reclaimed = (
      await provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { parallel_activity_timeout: { version: 1 } },
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 60_000,
      })
    ).activation
    const longRetry = await provider.getOrReserveEffect({
      ...ref,
      activationId: reclaimed!.activationId,
      workerId: "worker-b",
      key: "long",
      now: "2026-01-01T00:00:01.001Z",
      options: { heartbeatTimeoutMs: 10_000, retry: { maxAttempts: 1 } },
    })
    expect(longRetry).toMatchObject({
      status: "reserved",
      attempt: 1,
      idempotencyKey: long.idempotencyKey,
    })
  })

  it("keeps the originally reserved timeout policy when an effect is retried", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "activity_policy_lock",
      workflowVersion: 1,
      workflowId: "activity-policy-lock",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activity_policy_lock: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1,
      })
    ).activation
    const first = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "policy",
      now: "2026-01-01T00:00:00.000Z",
    })
    expect(first).toMatchObject({ status: "reserved", attempt: 1 })

    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:00:00.010Z",
      leaseMs: 60_000,
    })
    const reclaimed = (
      await provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { activity_policy_lock: { version: 1 } },
        now: "2026-01-01T00:00:00.010Z",
        leaseMs: 60_000,
      })
    ).activation
    const second = await provider.getOrReserveEffect({
      ...ref,
      activationId: reclaimed!.activationId,
      workerId: "worker-b",
      key: "policy",
      now: "2026-01-01T00:00:00.010Z",
      options: { heartbeatTimeoutMs: 1_000, retry: { maxAttempts: 3 } },
    })
    expect(second).toMatchObject({
      status: "reserved",
      attempt: 1,
      idempotencyKey: first.idempotencyKey,
    })
    expect((await provider.loadInstance(ref, { includeEffects: true }))?.effects?.[0]).toMatchObject({
      heartbeatTimeoutMs: undefined,
      heartbeatDeadline: undefined,
      maxAttempts: 3,
    })
  })

  it("rotates pending effect attempts when an activation lease is reclaimed", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "lease_reclaimed_effect",
      workflowVersion: 1,
      workflowId: "lease-reclaimed-effect",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "stable-worker",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "stable-worker",
        shardIds: [0],
        workflows: { lease_reclaimed_effect: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1,
      })
    ).activation
    const first = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "stable-worker",
      key: "maybe-stale",
      now: "2026-01-01T00:00:00.000Z",
    })
    expect(first.status).toBe("reserved")
    if (first.status !== "reserved") {
      throw new Error("expected reserved effect")
    }

    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "stable-worker",
      now: "2026-01-01T00:00:00.010Z",
      leaseMs: 60_000,
    })
    const reclaimed = (
      await provider.claimReadyActivation({
        workerId: "stable-worker",
        shardIds: [0],
        workflows: { lease_reclaimed_effect: { version: 1 } },
        now: "2026-01-01T00:00:00.010Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(reclaimed?.activationId).toBe(activation!.activationId)
    const second = await provider.getOrReserveEffect({
      ...ref,
      activationId: reclaimed!.activationId,
      workerId: "stable-worker",
      key: "maybe-stale",
      now: "2026-01-01T00:00:00.010Z",
    })
    expect(second).toMatchObject({
      status: "reserved",
      attempt: 1,
      idempotencyKey: first.idempotencyKey,
    })
    if (second.status !== "reserved") {
      throw new Error("expected reclaimed effect")
    }
    expect(second.attemptId).not.toBe(first.attemptId)

    await expect(
      provider.completeEffect({
        ...ref,
        activationId: reclaimed!.activationId,
        workerId: "stable-worker",
        effectId: first.effectId,
        attemptId: first.attemptId,
        result: { stale: true },
        now: "2026-01-01T00:00:00.010Z",
      }),
    ).rejects.toThrow("Lost effect attempt")
  })

  it("uses pending activity timeout deadlines as claim wake hints", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "activity_wake_hint",
      workflowVersion: 1,
      workflowId: "activity-wake-hint",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activity_wake_hint: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "wake",
      now: "2026-01-01T00:00:00.000Z",
      options: { heartbeatTimeoutMs: 1_000 },
    })
    await provider.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:00:00.100Z",
      leaseMs: 60_000,
    })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { activity_wake_hint: { version: 1 } },
        now: "2026-01-01T00:00:00.100Z",
        leaseMs: 60_000,
      }),
    ).resolves.toEqual({
      activation: null,
      nextWakeAt: "2026-01-01T00:00:01.000Z",
    })
  })

  it("migrates a running instance at a checkpoint boundary and recomputes waits", async () => {
    const path = await storePath()
    const clock = manualClock()
    const WorkflowV1 = defineWorkflow({
      name: "migrating_order",
      version: 1,
      input: z.object({ customerId: z.string() }),
      output: z.object({ message: z.string() }),
      common: z.object({ customerId: z.string() }),
      initial(input) {
        return start({
          common: { customerId: input.customerId },
          phase: "waiting",
          data: { salutation: "hello" },
        })
      },
      phases: {
        waiting: phase({
          state: z.object({ salutation: z.string() }),
          on: {
            finish: signal(z.object({ punctuation: z.string() }), async ({ common, data, event }) =>
              complete({
                message: `${data.salutation}, ${common.customerId}${event.punctuation}`,
              }),
            ),
          },
        }),
      },
    })

    const WorkflowV2 = defineWorkflow({
      name: "migrating_order",
      version: 2,
      input: z.object({ customerId: z.string() }),
      output: z.object({ message: z.string() }),
      common: z.object({ customerId: z.string(), plan: z.string() }),
      initial(input) {
        return start({
          common: { customerId: input.customerId, plan: "pro" },
          phase: "waiting_for_finish",
          data: { greeting: "hello", migratedFrom: "initial" },
        })
      },
      migrations: {
        1: ({ common, phase }) => ({
          common: {
            ...common,
            plan: "starter",
          },
          phase: {
            name: "waiting_for_finish",
            data: {
              greeting: phase.data.salutation,
              migratedFrom: phase.name,
            },
          },
        }),
      },
      phases: {
        waiting_for_finish: phase({
          state: z.object({
            greeting: z.string(),
            migratedFrom: z.string(),
          }),
          on: {
            finish: signal(z.object({ punctuation: z.string() }), async ({ common, data, event }) =>
              complete({
                message: `${data.greeting}, ${common.customerId} on ${common.plan}${event.punctuation}`,
              }),
            ),
          },
        }),
      },
    })

    const providerV1 = testProvider(path)
    const runtimeV1 = new DurableRuntime(providerV1, { clock: clock.clock, workflows: [WorkflowV1] })
    const ref = await runtimeV1.start(
      WorkflowV1,
      { customerId: "Ada" },
      { workflowId: "migration-1" },
    )

    expect(await providerV1.loadInstance(ref)).toMatchObject({
      workflowVersion: 1,
      sequence: 0,
      phase: { name: "waiting", data: { salutation: "hello" } },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
    })

    const providerV2 = testProvider(path)
    const runtimeV2 = new DurableRuntime(providerV2, { clock: clock.clock, workflows: [WorkflowV2] })
    await runtimeV2.drain({ maxActivations: 1 })

    expect(await providerV2.loadInstance(ref)).toMatchObject({
      workflowVersion: 2,
      sequence: 1,
      common: { customerId: "Ada", plan: "starter" },
      phase: {
        name: "waiting_for_finish",
        data: { greeting: "hello", migratedFrom: "waiting" },
      },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
    })

    await runtimeV2.signal(WorkflowV2, ref, "finish", { punctuation: "!" })
    await runtimeV2.drain()

    expect(await providerV2.loadInstance(ref)).toMatchObject({
      workflowVersion: 2,
      sequence: 2,
      status: "completed",
      output: { message: "hello, Ada on starter!" },
    })
  })

  it("allows only one worker to process a ready signal for a leased shard", async () => {
    const path = await storePath()
    const clock = manualClock()
    const OneSignalWorkflow = defineWorkflow({
      name: "one_signal",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({
          phase: "waiting",
          data: {},
        })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })

    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [OneSignalWorkflow],
      workerId: "worker-a",
    })
    const workerB = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [OneSignalWorkflow],
      workerId: "worker-b",
    })
    const ref = await workerA.start(OneSignalWorkflow, {}, { workflowId: "single-signal" })
    await workerA.signal(OneSignalWorkflow, ref, "finish", {})

    const [first, second] = await Promise.all([
      workerA.drain({ maxActivations: 1 }),
      workerB.drain({ maxActivations: 1 }),
    ])

    expect(first.activations + second.activations).toBe(1)
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      sequence: 1,
      output: { ok: true },
    })
    expect((await provider.listSignals())[0].consumedBySequence).toBe(1)
  })

  it("deduplicates runtime signal sends with an idempotency key", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const SignalWorkflow = defineWorkflow({
      name: "idempotent_signal_runtime",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const runtime = new DurableRuntime(provider, { workflows: [SignalWorkflow] })
    const ref = await runtime.start(SignalWorkflow, {}, { workflowId: "idempotent-signal-runtime" })

    const first = await runtime.signal(SignalWorkflow, ref, "finish", {}, { idempotencyKey: "send-1" })
    const duplicate = await runtime.signal(SignalWorkflow, ref, "finish", {}, { idempotencyKey: "send-1" })
    expect(duplicate).toEqual(first)

    await runtime.drain()
    const afterConsumed = await runtime.signal(SignalWorkflow, ref, "finish", {}, { idempotencyKey: "send-1" })
    expect(afterConsumed).toMatchObject({ signalId: first.signalId, consumedBySequence: 1 })
    expect(await provider.listSignals()).toHaveLength(1)
  })

  it("supports future-only signal waits from the workflow signal API", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const FutureSignalWorkflow = defineWorkflow({
      name: "future_signal_runtime",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.number() }),
      initial() {
        return start({ phase: "boot", data: {} })
      },
      phases: {
        boot: phase({
          run: async () => go("waiting", {}),
        }),
        waiting: phase({
          on: {
            finish: signal(
              z.object({ value: z.number() }),
              async ({ event }) => complete({ value: event.value }),
              { delivery: "future" },
            ),
          },
        }),
      },
    })
    const runtime = new DurableRuntime(provider, { workflows: [FutureSignalWorkflow] })
    const ref = await runtime.start(FutureSignalWorkflow, {}, { workflowId: "future-signal-runtime" })
    const oldSignal = await runtime.signal(FutureSignalWorkflow, ref, "finish", { value: 1 })

    await runtime.drain({ maxActivations: 1 })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "running",
      phase: { name: "waiting" },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase", delivery: "future" }],
    })
    expect(await runtime.drain()).toEqual({ activations: 0 })

    const newSignal = await runtime.signal(FutureSignalWorkflow, ref, "finish", { value: 2 })
    await runtime.drain()
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { value: 2 },
    })
    const signals = await provider.listSignals()
    expect(signals.find((signal) => signal.signalId === oldSignal.signalId)?.consumedBySequence)
      .toBeUndefined()
    expect(signals.find((signal) => signal.signalId === newSignal.signalId)?.consumedBySequence)
      .toBe(2)
  })

  it("requires shard ownership and lets expired shard leases be reclaimed", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const workflowId = "partitioned"
    const runId = "run-1"
    const shard = workflowPartitionShard(workflowId, runId, 4)

    expect(workflowPartitionShard(workflowId, runId, 4)).toBe(shard)
    expect(shard).toBeGreaterThanOrEqual(0)
    expect(shard).toBeLessThan(4)

    await provider.createInstance({
      workflowName: "partition_test",
      workflowVersion: 1,
      workflowId,
      runId,
      partitionShard: shard,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [shard],
        workflows: { partition_test: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1_000,
      }),
    ).resolves.toEqual({ activation: null })

    await expect(
      provider.claimDispatchShard({
        shardId: shard,
        ownerId: "worker-a",
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({ shardId: shard, ownerId: "worker-a" })

    await expect(
      provider.claimDispatchShard({
        shardId: shard,
        ownerId: "worker-b",
        now: "2026-01-01T00:00:00.500Z",
        leaseMs: 1_000,
      }),
    ).resolves.toBeNull()

    await expect(
      provider.claimDispatchShard({
        shardId: shard,
        ownerId: "worker-b",
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({ shardId: shard, ownerId: "worker-b" })
  })

  it("lets a new shard owner reclaim shard-scoped work after shard lease expiry", async () => {
    const path = await storePath()
    const providerA = testProvider(path)
    const providerB = testProvider(path)

    await providerA.createInstance({
      workflowName: "activation_owner",
      workflowVersion: 1,
      workflowId: "activation-owner",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })

    await expect(
      providerA.claimDispatchShard({
        shardId: 0,
        ownerId: "worker-a",
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 10,
      }),
    ).resolves.toMatchObject({ ownerId: "worker-a" })

    const activationA = (
      await providerA.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activation_owner: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1_000,
      })
    ).activation
    expect(activationA).toMatchObject({ kind: "run", activationId: expect.any(String) })

    await expect(
      providerB.claimDispatchShard({
        shardId: 0,
        ownerId: "worker-b",
        now: "2026-01-01T00:00:00.011Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({ ownerId: "worker-b" })

    await expect(
      providerB.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { activation_owner: { version: 1 } },
        now: "2026-01-01T00:00:00.011Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({
      activation: {
        kind: "run",
        activationId: activationA!.activationId,
      },
    })

    await expect(
      providerA.commitCheckpoint({
        workflowId: "activation-owner",
        runId: "run-1",
        expectedSequence: 0,
        activationId: activationA!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:01.001Z",
      }),
    ).resolves.toMatchObject({ ok: false, reason: "lost_shard_task_lease" })
  })

  it("does not claim competing ready events for the same workflow sequence", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "activation_race",
      workflowVersion: 1,
      workflowId: "activation-race",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [
        { kind: "signal", name: "finish", type: "finish", scope: "phase" },
        { kind: "timer", name: "timeout", fireAt: "2026-01-01T00:00:00.000Z" },
      ],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: {},
      receivedAt: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })

    const first = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activation_race: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(first).toMatchObject({ kind: "event", event: { kind: "signal" } })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activation_race: { version: 1 } },
        now: "2026-01-01T00:00:00.001Z",
        leaseMs: 60_000,
      }),
    ).resolves.toMatchObject({ activation: null })

    await provider.releaseActivation({ activationId: first!.activationId, workerId: "worker-a" })
    const retried = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { activation_race: { version: 1 } },
        now: "2026-01-01T00:00:00.002Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(retried).toMatchObject({
      activationId: first!.activationId,
      event: { kind: "signal" },
    })
  })

  it("batch claims ready activations up to the requested limit with lean snapshots", async () => {
    const provider = testProvider(await storePath())
    for (let index = 0; index < 4; index += 1) {
      await provider.createInstance({
        workflowName: "batch_claim",
        workflowVersion: 1,
        workflowId: `batch-claim-${index}`,
        runId: "run-1",
        partitionShard: 0,
        common: { index },
        phase: { name: "run", data: { index } },
        waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
        now: "2026-01-01T00:00:00.000Z",
      })
    }
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })

    await expect(
      provider.claimReadyActivations({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { batch_claim: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
        limit: 0,
      }),
    ).rejects.toThrow("claimReadyActivations limit must be a positive integer")

    const batch = await provider.claimReadyActivations({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { batch_claim: { version: 1 } },
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
      limit: 2,
    })
    expect(batch.claims).toHaveLength(2)
    expect(batch.nextWakeAt).toBeUndefined()
    expect(batch.claims.map((claim) => claim.activation.workflowId)).toEqual([
      "batch-claim-0",
      "batch-claim-1",
    ])
    expect(batch.claims[0].instance).toMatchObject({
      workflowId: "batch-claim-0",
      runId: "run-1",
      sequence: 0,
      status: "running",
      common: { index: 0 },
      phase: { name: "run", data: { index: 0 } },
    })
    expect("effects" in batch.claims[0].instance).toBe(false)

    const wrapperClaim = await provider.claimReadyActivation({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { batch_claim: { version: 1 } },
      now: "2026-01-01T00:00:00.001Z",
      leaseMs: 60_000,
    })
    expect(wrapperClaim).toMatchObject({
      activation: { workflowId: "batch-claim-2" },
      instance: { workflowId: "batch-claim-2", sequence: 0 },
    })
  })

  it("does not batch-claim competing ready events for the same workflow sequence", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "batch_activation_race",
      workflowVersion: 1,
      workflowId: "batch-activation-race",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [
        { kind: "signal", name: "finish", type: "finish", scope: "phase" },
        { kind: "timer", name: "timeout", fireAt: "2026-01-01T00:00:00.000Z" },
      ],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: {},
      receivedAt: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })

    const batch = await provider.claimReadyActivations({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { batch_activation_race: { version: 1 } },
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
      limit: 2,
    })

    expect(batch.claims).toHaveLength(1)
    expect(batch.claims[0].activation).toMatchObject({
      kind: "event",
      event: { kind: "signal" },
    })
  })

  it("requires live shard ownership to start an eager child workflow", async () => {
    const provider = testProvider(await storePath())
    const parentRef = await provider.createInstance({
      workflowName: "child_start_fence_parent",
      workflowVersion: 1,
      workflowId: "child-start-fence-parent",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { child_start_fence_parent: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "run" })
    await provider.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })

    await expect(
      provider.createChildInstance({
        workflowName: "child_start_fence_child",
        workflowVersion: 1,
        workflowId: "child-start-fence-child",
        runId: "run-1",
        partitionShard: 0,
        common: {},
        phase: { name: "run", data: {} },
        waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
        now: "2026-01-01T00:00:00.000Z",
        parentWorkflowId: parentRef.workflowId,
        parentRunId: parentRef.runId,
        activationId: activation!.activationId,
        workerId: "worker-a",
        leaseNow: "2026-01-01T00:00:00.002Z",
        key: "child",
      }),
    ).rejects.toThrow("Lost shard lease")
  })

  it("reclaims an expired activation lease and reuses completed effects", async () => {
    const path = await storePath()
    const clock = manualClock()
    const calls = { activity: 0, shouldThrow: true }
    const UnstableWorkflow = defineWorkflow({
      name: "lease_retry",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({
          phase: "unstable",
          data: {},
        })
      },
      phases: {
        unstable: phase({
          run: async ({ ctx }) => {
            const result = await ctx.activity("side_effect_once", () => {
              calls.activity += 1
              return { ok: true }
            }, { durability: "eager" })

            if (calls.shouldThrow) {
              calls.shouldThrow = false
              throw new Error("boom after durable effect")
            }

            return complete(result)
          },
        }),
      },
    })

    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [UnstableWorkflow],
      workerId: "lease-worker-a",
      dispatchLeaseMs: 1,
      activationLeaseMs: 1,
    })
    const ref = await workerA.start(UnstableWorkflow, {}, { workflowId: "lease-retry" })

    await expect(workerA.drain({ maxActivations: 1 })).rejects.toThrow("boom after durable effect")
    expect(calls.activity).toBe(1)
    expect((await provider.loadInstance(ref))?.sequence).toBe(0)

    clock.advance(2)
    const workerB = new DurableRuntime(testProvider(path), {
      clock: clock.clock,
      workflows: [UnstableWorkflow],
      workerId: "lease-worker-b",
      dispatchLeaseMs: 1_000,
      activationLeaseMs: 1_000,
    })
    await workerB.drain({ maxActivations: 1 })

    expect(calls.activity).toBe(1)
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      sequence: 1,
      output: { ok: true },
    })
  })

  it("rejects stale or expired checkpoint commits without consuming signals", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const ref = await provider.createInstance({
      workflowName: "commit_conflict",
      workflowVersion: 1,
      workflowId: "commit-conflict",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    const signalRecord = await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: {},
      receivedAt: "2026-01-01T00:00:00.000Z",
    })

    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 10,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { commit_conflict: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 10,
      })
    ).activation
    expect(activation?.kind).toBe("event")

    await expect(
      provider.commitCheckpoint({
        ...ref,
        expectedSequence: 1,
        activationId: activation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
        consumeSignalId: signalRecord.signalId,
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 0 })
    expect((await provider.listSignals())[0].consumedBySequence).toBeUndefined()

    await expect(
      provider.commitCheckpoint({
        ...ref,
        expectedSequence: 0,
        activationId: activation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.011Z",
        consumeSignalId: signalRecord.signalId,
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 0 })
    expect((await provider.listSignals())[0].consumedBySequence).toBeUndefined()
  })

  it("throws when effect mutation targets an unknown effect", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "unknown_effect",
      workflowVersion: 1,
      workflowId: "unknown-effect",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { unknown_effect: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1_000,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "run" })

    await expect(
      provider.completeEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: "effect-missing",
        attemptId: "attempt-missing",
        result: {},
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).rejects.toThrow("Unknown effect: effect-missing")

    await expect(
      provider.failEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: "effect-missing",
        attemptId: "attempt-missing",
        error: { message: "nope" },
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).rejects.toThrow("Unknown effect: effect-missing")

    await expect(
      provider.heartbeatEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: "effect-missing",
        attemptId: "attempt-missing",
        now: "2026-01-01T00:00:00.000Z",
        details: {},
      }),
    ).rejects.toThrow("Unknown effect: effect-missing")
  })

  it("keeps timer event firedAt deterministic across activation retries", async () => {
    const path = await storePath()
    const clock = manualClock()
    const seen: string[] = []
    const seenNow: string[] = []
    let shouldThrow = true
    const TimerWorkflow = defineWorkflow({
      name: "deterministic_timer",
      version: 1,
      input: z.object({}),
      output: z.object({ firedAt: z.string() }),
      initial() {
        return start({
          phase: "waiting",
          data: { wakeAt: "2026-01-01T00:00:01.000Z" },
        })
      },
      phases: {
        waiting: phase({
          state: z.object({ wakeAt: z.string() }),
          on: {
            wake: timer(
              ({ data }) => data.wakeAt,
              async ({ ctx, event }) => {
                seen.push(event.firedAt)
                seenNow.push(ctx.now())
                if (shouldThrow) {
                  shouldThrow = false
                  throw new Error("retry timer")
                }
                return complete({ firedAt: event.firedAt })
              },
            ),
          },
        }),
      },
    })

    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [TimerWorkflow],
      workerId: "timer-a",
      dispatchLeaseMs: 1,
      activationLeaseMs: 1,
    })
    const ref = await workerA.start(TimerWorkflow, {}, { workflowId: "deterministic-timer" })

    clock.advance(1_000)
    await expect(workerA.drain({ maxActivations: 1 })).rejects.toThrow("retry timer")

    clock.advance(10_000)
    const workerB = new DurableRuntime(testProvider(path), {
      clock: clock.clock,
      workflows: [TimerWorkflow],
      workerId: "timer-b",
      dispatchLeaseMs: 1_000,
      activationLeaseMs: 1_000,
    })
    await workerB.drain({ maxActivations: 1 })

    expect(seen).toEqual([
      "2026-01-01T00:00:01.000Z",
      "2026-01-01T00:00:01.000Z",
    ])
    expect(seenNow).toEqual([
      "2026-01-01T00:00:01.000Z",
      "2026-01-01T00:00:01.000Z",
    ])
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { firedAt: "2026-01-01T00:00:01.000Z" },
    })
  })

  it("selects the canonical earliest ready event across child, signal, and timer waits", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    const eventAt = "2026-01-01T00:00:10.000Z"
    const parentRef = await provider.createInstance({
      workflowName: "ordering_parent",
      workflowVersion: 1,
      workflowId: "ordering-parent",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "setup", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const setupActivation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { ordering_parent: { version: 1 }, ordering_child: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(setupActivation).toMatchObject({ kind: "run", workflowId: "ordering-parent" })

    await provider.createChildInstance({
      workflowName: "ordering_child",
      workflowVersion: 1,
      workflowId: "ordering-child",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
      parentWorkflowId: parentRef.workflowId,
      parentRunId: parentRef.runId,
      activationId: setupActivation!.activationId,
      workerId: "worker-a",
      leaseNow: "2026-01-01T00:00:00.000Z",
      key: "child",
    })
    await provider.commitCheckpoint({
      ...parentRef,
      expectedSequence: 0,
      activationId: setupActivation!.activationId,
      workerId: "worker-a",
      workflowVersion: 1,
      next: {
        status: "running",
        common: {},
        phase: { name: "waiting", data: {} },
      },
      waits: [
        {
          kind: "child",
          name: "child_done",
          workflowName: "ordering_child",
          workflowVersion: 1,
          workflowId: "ordering-child",
          runId: "run-1",
        },
        { kind: "signal", name: "signal_done", type: "signal_done", scope: "phase" },
        { kind: "timer", name: "timer_done", fireAt: eventAt },
      ],
      now: "2026-01-01T00:00:00.000Z",
    })
    const childRun = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { ordering_parent: { version: 1 }, ordering_child: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(childRun).toMatchObject({ kind: "run", workflowId: "ordering-child" })
    await provider.commitCheckpoint({
      workflowId: "ordering-child",
      runId: "run-1",
      expectedSequence: 0,
      activationId: childRun!.activationId,
      workerId: "worker-a",
      workflowVersion: 1,
      next: { status: "completed", output: { child: true } },
      waits: [],
      now: eventAt,
    })
    await provider.appendSignal({
      ...parentRef,
      type: "signal_done",
      payload: {},
      receivedAt: eventAt,
    })

    const parentEvent = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { ordering_parent: { version: 1 }, ordering_child: { version: 1 } },
        now: eventAt,
        leaseMs: 60_000,
      })
    ).activation

    expect(parentEvent).toMatchObject({
      kind: "event",
      workflowId: "ordering-parent",
      waitName: "child_done",
      event: { kind: "child" },
    })
  })

  it("returns nextWakeAt for future timers and preserves timer readiness across provider restart", async () => {
    const path = await storePath()
    const provider = testProvider(path)
    await provider.createInstance({
      workflowName: "future_timer",
      workflowVersion: 1,
      workflowId: "future-timer",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [{ kind: "timer", name: "wake", fireAt: "2026-01-01T00:00:05.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { future_timer: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      }),
    ).resolves.toEqual({
      activation: null,
      nextWakeAt: "2026-01-01T00:00:05.000Z",
    })

    const restartedProvider = testProvider(path)
    await restartedProvider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-b",
      now: "2026-01-01T00:01:00.000Z",
      leaseMs: 60_000,
    })
    await expect(
      restartedProvider.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { future_timer: { version: 1 } },
        now: "2026-01-01T00:01:00.000Z",
        leaseMs: 60_000,
      }),
    ).resolves.toMatchObject({
      activation: {
        kind: "event",
        event: { kind: "timer", firedAt: "2026-01-01T00:00:05.000Z" },
      },
    })
  })

  it("runWorker sleeps from nextWakeAt and uses bounded polling for later signals", async () => {
    const timerClock = manualClock()
    const TimerWorkflow = defineWorkflow({
      name: "worker_timer_sleep",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: { wakeAt: "2026-01-01T00:00:05.000Z" } })
      },
      phases: {
        waiting: phase({
          state: z.object({ wakeAt: z.string() }),
          on: {
            wake: timer(({ data }) => data.wakeAt, async () => complete({ ok: true })),
          },
        }),
      },
    })

    const timerRuntime = new DurableRuntime(testProvider(await storePath()), {
      clock: timerClock.clock,
      workflows: [TimerWorkflow],
      workerId: "timer-worker",
    })
    const timerAbort = new AbortController()
    const timerSleeps: number[] = []
    await timerRuntime.start(TimerWorkflow, {}, { workflowId: "worker-timer-sleep" })
    await timerRuntime.runWorker({
      signal: timerAbort.signal,
      minPollIntervalMs: 10,
      maxPollIntervalMs: 10_000,
      jitterRatio: 0,
      sleep: async (ms) => {
        timerSleeps.push(ms)
        timerAbort.abort()
      },
    })
    expect(timerSleeps).toEqual([5_000])

    const signalClock = manualClock()
    const SignalWorkflow = defineWorkflow({
      name: "worker_signal_poll",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const signalProvider = testProvider(await storePath())
    const signalRuntime = new DurableRuntime(signalProvider, {
      clock: signalClock.clock,
      workflows: [SignalWorkflow],
      workerId: "signal-poll-worker",
    })
    const ref = await signalRuntime.start(SignalWorkflow, {}, { workflowId: "worker-signal-poll" })
    const signalAbort = new AbortController()
    const signalSleeps: number[] = []
    await signalRuntime.runWorker({
      signal: signalAbort.signal,
      maxActivationsPerDrain: 1,
      minPollIntervalMs: 1,
      maxPollIntervalMs: 50,
      jitterRatio: 0,
      sleep: async (ms) => {
        signalSleeps.push(ms)
        if (signalSleeps.length === 1) {
          await signalRuntime.signal(SignalWorkflow, ref, "finish", {})
        } else {
          signalAbort.abort()
        }
      },
    })
    expect(signalSleeps[0]).toBe(50)
    expect(await signalProvider.loadInstance(ref)).toMatchObject({ status: "completed" })
  })

  it("runWorker keeps dispatch shard leases while idle and releases them on shutdown", async () => {
    const clock = manualClock()
    const StickyShardWorkflow = defineWorkflow({
      name: "worker_sticky_shard",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: { wakeAt: "2026-01-01T00:00:05.000Z" } })
      },
      phases: {
        waiting: phase({
          state: z.object({ wakeAt: z.string() }),
          on: {
            wake: timer(({ data }) => data.wakeAt, async () => complete({ ok: true })),
          },
        }),
      },
    })

    const path = await storePath()
    const provider = testProvider(path)
    const competitor = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [StickyShardWorkflow],
      workerId: "sticky-worker",
      shardCount: 1,
      dispatchLeaseMs: 30_000,
      leaseHeartbeatIntervalMs: 5,
    })
    await runtime.start(StickyShardWorkflow, {}, { workflowId: "sticky-worker-shard" })
    const abort = new AbortController()

    await runtime.runWorker({
      signal: abort.signal,
      minPollIntervalMs: 1,
      maxPollIntervalMs: 10_000,
      jitterRatio: 0,
      sleep: async () => {
        await expect(
          competitor.claimDispatchShard({
            shardId: 0,
            ownerId: "competing-worker",
            now: clock.clock().toISOString(),
            leaseMs: 30_000,
          }),
        ).resolves.toBeNull()
        abort.abort()
      },
    })

    await expect(
      competitor.claimDispatchShard({
        shardId: 0,
        ownerId: "competing-worker",
        now: clock.clock().toISOString(),
        leaseMs: 30_000,
      }),
    ).resolves.toMatchObject({ ownerId: "competing-worker" })
  })

  it("runWorker releases dispatch shard leases after a fatal activation error", async () => {
    const FailingWorkerWorkflow = defineWorkflow({
      name: "worker_fatal_release",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async () => {
            throw new Error("fatal worker activation")
          },
        }),
      },
    })

    const path = await storePath()
    const provider = testProvider(path)
    const competitor = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      workflows: [FailingWorkerWorkflow],
      workerId: "fatal-release-worker",
      shardCount: 1,
    })
    await runtime.start(FailingWorkerWorkflow, {}, { workflowId: "fatal-release" })

    await expect(runtime.runWorker({ maxActivationsPerDrain: 1 })).rejects.toThrow(
      "fatal worker activation",
    )
    await expect(
      competitor.claimDispatchShard({
        shardId: 0,
        ownerId: "after-fatal-worker",
        now: new Date("2026-01-01T00:00:00.000Z").toISOString(),
        leaseMs: 30_000,
      }),
    ).resolves.toMatchObject({ ownerId: "after-fatal-worker" })
  })

  it("runs multiple ready activations concurrently by default", async () => {
    const started: number[] = []
    let inFlight = 0
    let maxInFlight = 0
    let releaseImmediately = false
    const releases = new Map<number, () => void>()
    const ConcurrentWorkflow = defineWorkflow({
      name: "default_activation_concurrency",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            started.push(common.index)
            inFlight += 1
            maxInFlight = Math.max(maxInFlight, inFlight)
            if (!releaseImmediately) {
              await new Promise<void>((resolve) => {
                releases.set(common.index, resolve)
              })
            }
            inFlight -= 1
            return complete({ index: common.index })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ConcurrentWorkflow],
      workerId: "default-concurrency-worker",
    })
    const refs = await Promise.all(
      Array.from({ length: 4 }, (_value, index) =>
        runtime.start(ConcurrentWorkflow, { index }, { workflowId: `default-concurrency-${index}` }),
      ),
    )

    const draining = runtime.drain({ maxActivations: 4 })
    await waitFor(() => started.length >= 2)
    expect(maxInFlight).toBeGreaterThanOrEqual(2)
    releaseImmediately = true
    for (const release of releases.values()) {
      release()
    }
    await expect(draining).resolves.toEqual({ activations: 4 })
    await expect(Promise.all(refs.map((ref) => provider.loadInstance(ref)))).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: "completed", output: { index: 0 } }),
        expect.objectContaining({ status: "completed", output: { index: 1 } }),
        expect.objectContaining({ status: "completed", output: { index: 2 } }),
        expect.objectContaining({ status: "completed", output: { index: 3 } }),
      ]),
    )
  })

  it("sends sibling activation completions through one provider commit batch", async () => {
    class TrackingCommitProvider extends SqliteDurabilityProvider {
      readonly commitBatchSizes: number[] = []

      override async commitActivations(
        inputs: CommitActivationInput[],
      ): Promise<CommitActivationsResult> {
        this.commitBatchSizes.push(inputs.length)
        return super.commitActivations(inputs)
      }
    }

    const started: number[] = []
    const release = deferred()
    const BatchedCommitWorkflow = defineWorkflow({
      name: "sqlite_batched_commit_runtime",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            started.push(common.index)
            if (started.length === 2) {
              release.resolve()
            }
            await release.promise
            return complete({ index: common.index })
          },
        }),
      },
    })

    const provider = new TrackingCommitProvider(await storePath())
    testProviders.add(provider)
    const runtime = new DurableRuntime(provider, {
      workflows: [BatchedCommitWorkflow],
      workerId: "sqlite-batched-commit-worker",
      maxConcurrentActivations: 2,
      activationCommitBatchSize: 2,
      activationCommitMaxDelayMs: 1_000,
    })
    const refs = await Promise.all(
      [0, 1].map((index) =>
        runtime.start(BatchedCommitWorkflow, { index }, { workflowId: `sqlite-batched-commit-${index}` }),
      ),
    )

    await expect(runtime.drain({ maxActivations: 2 })).resolves.toEqual({ activations: 2 })
    expect(provider.commitBatchSizes).toContain(2)
    await expect(Promise.all(refs.map((ref) => provider.loadInstance(ref)))).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: "completed", output: { index: 0 } }),
        expect.objectContaining({ status: "completed", output: { index: 1 } }),
      ]),
    )
  })

  it("prefetches activation claims ahead of execution slots and heartbeats queued claims", async () => {
    const clock = manualClock()
    const started: number[] = []
    const releaseFirst = deferred()
    const PrefetchWorkflow = defineWorkflow({
      name: "activation_prefetch",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            started.push(common.index)
            if (common.index === 0) {
              await releaseFirst.promise
            }
            return complete({ index: common.index })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [PrefetchWorkflow],
      workerId: "prefetch-worker",
      maxConcurrentActivations: 1,
      activationPrefetchLimit: 3,
      activationLeaseMs: 10,
      leaseHeartbeatIntervalMs: 1,
    })
    const refs = await Promise.all(
      Array.from({ length: 3 }, (_value, index) =>
        runtime.start(PrefetchWorkflow, { index }, { workflowId: `prefetch-${index}` }),
      ),
    )

    const draining = runtime.drain({ maxActivations: 3 })
    await waitFor(() => started.length === 1)

    let ownedClaims = (await provider.listActivationClaims()).filter(
      (claim) => claim.ownerId === "prefetch-worker",
    )
    const deadline = Date.now() + 250
    while (ownedClaims.length < 3 && Date.now() <= deadline) {
      await new Promise((resolve) => setTimeout(resolve, 1))
      ownedClaims = (await provider.listActivationClaims()).filter(
        (claim) => claim.ownerId === "prefetch-worker",
      )
    }
    expect(ownedClaims).toHaveLength(3)
    expect(started).toEqual([0])

    clock.advance(8)
    await new Promise((resolve) => setTimeout(resolve, 5))
    clock.advance(8)
    await new Promise((resolve) => setTimeout(resolve, 5))
    await expect(
      provider.claimDispatchShard({
        shardId: 0,
        ownerId: "prefetch-competitor",
        now: clock.clock().toISOString(),
        leaseMs: 10,
      }),
    ).resolves.toBeNull()

    releaseFirst.resolve()
    await expect(draining).resolves.toEqual({ activations: 3 })
    await expect(Promise.all(refs.map((ref) => provider.loadInstance(ref)))).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: "completed", output: { index: 0 } }),
        expect.objectContaining({ status: "completed", output: { index: 1 } }),
        expect.objectContaining({ status: "completed", output: { index: 2 } }),
      ]),
    )
  })

  it("releases prefetched queued activations when a running activation fails", async () => {
    let badAttempts = 0
    const FailPrefetchWorkflow = defineWorkflow({
      name: "activation_prefetch_failure",
      version: 1,
      input: z.object({ kind: z.enum(["bad", "ok"]) }),
      output: z.object({ kind: z.string() }),
      common: z.object({ kind: z.enum(["bad", "ok"]) }),
      initial(input) {
        return start({ common: { kind: input.kind }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            if (common.kind === "bad") {
              badAttempts += 1
              if (badAttempts === 1) {
                throw new Error("prefetch failure")
              }
            }
            return complete({ kind: common.kind })
          },
        }),
      },
    })

    const path = await storePath()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      workflows: [FailPrefetchWorkflow],
      workerId: "prefetch-failure-worker",
      maxConcurrentActivations: 1,
      activationPrefetchLimit: 3,
    })
    await runtime.start(FailPrefetchWorkflow, { kind: "bad" }, { workflowId: "a-prefetch-bad" })
    const okRefs = await Promise.all([
      runtime.start(FailPrefetchWorkflow, { kind: "ok" }, { workflowId: "b-prefetch-ok" }),
      runtime.start(FailPrefetchWorkflow, { kind: "ok" }, { workflowId: "c-prefetch-ok" }),
    ])

    await expect(runtime.drain({ maxActivations: 3 })).rejects.toThrow("prefetch failure")
    const queuedClaims = (await provider.listActivationClaims()).filter((claim) =>
      okRefs.some((ref) => ref.workflowId === claim.workflowId),
    )
    expect(queuedClaims).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ ownerId: undefined }),
        expect.objectContaining({ ownerId: undefined }),
      ]),
    )

    const recovery = new DurableRuntime(testProvider(path), {
      workflows: [FailPrefetchWorkflow],
      workerId: "prefetch-recovery-worker",
      maxConcurrentActivations: 2,
    })
    await expect(recovery.drain({ maxActivations: 3 })).resolves.toEqual({ activations: 3 })
    await expect(Promise.all(okRefs.map((ref) => provider.loadInstance(ref)))).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: "completed" }),
        expect.objectContaining({ status: "completed" }),
      ]),
    )
  })

  it("enforces drain and runWorker activation concurrency overrides", async () => {
    const started: number[] = []
    let inFlight = 0
    let maxInFlight = 0
    let releaseImmediately = false
    const releases = new Map<number, () => void>()
    const LimitedWorkflow = defineWorkflow({
      name: "limited_activation_concurrency",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            started.push(common.index)
            inFlight += 1
            maxInFlight = Math.max(maxInFlight, inFlight)
            if (!releaseImmediately) {
              await new Promise<void>((resolve) => {
                releases.set(common.index, resolve)
              })
            }
            inFlight -= 1
            return complete({ index: common.index })
          },
        }),
      },
    })

    const drainProvider = testProvider(await storePath())
    const drainRuntime = new DurableRuntime(drainProvider, {
      workflows: [LimitedWorkflow],
      workerId: "drain-concurrency-worker",
      maxConcurrentActivations: 8,
    })
    await Promise.all(
      Array.from({ length: 6 }, (_value, index) =>
        drainRuntime.start(LimitedWorkflow, { index }, { workflowId: `drain-limit-${index}` }),
      ),
    )
    const draining = drainRuntime.drain({
      maxActivations: 6,
      maxConcurrentActivations: 2,
    })
    await waitFor(() => started.length === 2)
    expect(maxInFlight).toBe(2)
    releaseImmediately = true
    for (const release of releases.values()) {
      release()
    }
    await expect(draining).resolves.toEqual({ activations: 6 })
    expect(maxInFlight).toBeLessThanOrEqual(2)

    started.length = 0
    inFlight = 0
    maxInFlight = 0
    releaseImmediately = false
    releases.clear()
    const workerProvider = testProvider(await storePath())
    const workerRuntime = new DurableRuntime(workerProvider, {
      workflows: [LimitedWorkflow],
      workerId: "worker-concurrency-worker",
      maxConcurrentActivations: 1,
    })
    await Promise.all(
      Array.from({ length: 4 }, (_value, index) =>
        workerRuntime.start(LimitedWorkflow, { index }, { workflowId: `worker-limit-${index}` }),
      ),
    )
    const abort = new AbortController()
    const worker = workerRuntime.runWorker({
      signal: abort.signal,
      maxActivationsPerDrain: 4,
      maxConcurrentActivations: 3,
      minPollIntervalMs: 1,
      maxPollIntervalMs: 1,
      jitterRatio: 0,
      sleep: async () => {
        abort.abort()
      },
    })
    await waitFor(() => started.length === 3)
    expect(maxInFlight).toBe(3)
    releaseImmediately = true
    for (const release of releases.values()) {
      release()
    }
    await expect(worker).resolves.toEqual({ activations: 4 })
    expect(maxInFlight).toBeLessThanOrEqual(3)
  })

  it("does not let a long activity block another activation in the same worker", async () => {
    const slowStarted = deferred()
    const releaseSlow = deferred()
    let fastCompleted = false
    const ActivityConcurrencyWorkflow = defineWorkflow({
      name: "activity_slot_concurrency",
      version: 1,
      input: z.object({ kind: z.enum(["slow", "fast"]) }),
      output: z.object({ kind: z.string() }),
      common: z.object({ kind: z.enum(["slow", "fast"]) }),
      initial(input) {
        return start({ common: { kind: input.kind }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            if (common.kind === "slow") {
              await ctx.activity("slow", async () => {
                slowStarted.resolve()
                await releaseSlow.promise
                return "slow"
              })
            } else {
              await ctx.activity("fast", async () => {
                fastCompleted = true
                return "fast"
              })
            }
            return complete({ kind: common.kind })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ActivityConcurrencyWorkflow],
      workerId: "activity-slot-worker",
      maxConcurrentActivations: 2,
    })
    const slowRef = await runtime.start(ActivityConcurrencyWorkflow, { kind: "slow" }, { workflowId: "a-slow" })
    const fastRef = await runtime.start(ActivityConcurrencyWorkflow, { kind: "fast" }, { workflowId: "b-fast" })
    const draining = runtime.drain({ maxActivations: 2 })

    await slowStarted.promise
    await waitFor(() => fastCompleted)
    await waitForInstanceStatus(provider, fastRef, "completed")
    expect(await provider.loadInstance(fastRef)).toMatchObject({ status: "completed" })
    expect(await provider.loadInstance(slowRef)).toMatchObject({ status: "running" })
    releaseSlow.resolve()
    await expect(draining).resolves.toEqual({ activations: 2 })
    expect(await provider.loadInstance(slowRef)).toMatchObject({ status: "completed" })
  })

  it("does not execute competing events for one workflow sequence concurrently", async () => {
    const clock = manualClock()
    let inFlight = 0
    let maxInFlight = 0
    const CompetingWorkflow = defineWorkflow({
      name: "competing_event_runtime",
      version: 1,
      input: z.object({}),
      output: z.object({ winner: z.string() }),
      initial() {
        return start({ phase: "waiting", data: { wakeAt: "2026-01-01T00:00:00.000Z" } })
      },
      phases: {
        waiting: phase({
          state: z.object({ wakeAt: z.string() }),
          on: {
            finish: signal(z.object({}), async () => {
              inFlight += 1
              maxInFlight = Math.max(maxInFlight, inFlight)
              await new Promise((resolve) => setTimeout(resolve, 1))
              inFlight -= 1
              return complete({ winner: "signal" })
            }),
            wake: timer(
              ({ data }) => data.wakeAt,
              async () => {
                inFlight += 1
                maxInFlight = Math.max(maxInFlight, inFlight)
                await new Promise((resolve) => setTimeout(resolve, 1))
                inFlight -= 1
                return complete({ winner: "timer" })
              },
            ),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [CompetingWorkflow],
      workerId: "competing-event-worker",
      maxConcurrentActivations: 4,
    })
    const ref = await runtime.start(CompetingWorkflow, {}, { workflowId: "competing-event-runtime" })
    await runtime.signal(CompetingWorkflow, ref, "finish", {})

    await expect(runtime.drain({ maxActivations: 2 })).resolves.toEqual({ activations: 1 })
    expect(maxInFlight).toBe(1)
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "completed" })
  })

  it("keeps retry scheduling local and aborts sibling activations on handler errors", async () => {
    const retryCalls: string[] = []
    const RetryAndSiblingWorkflow = defineWorkflow({
      name: "concurrent_retry_sibling",
      version: 1,
      input: z.object({ kind: z.enum(["retry", "ok"]) }),
      output: z.object({ kind: z.string() }),
      common: z.object({ kind: z.enum(["retry", "ok"]) }),
      initial(input) {
        return start({ common: { kind: input.kind }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            if (common.kind === "retry") {
              await ctx.activity(
                "retry",
                () => {
                  retryCalls.push("retry")
                  throw new Error("retry me")
                },
                { retry: { maxAttempts: 2, initialIntervalMs: 1 } },
              )
            }
            return complete({ kind: common.kind })
          },
        }),
      },
    })

    const retryProvider = testProvider(await storePath())
    const retryRuntime = new DurableRuntime(retryProvider, {
      workflows: [RetryAndSiblingWorkflow],
      workerId: "retry-sibling-worker",
      maxConcurrentActivations: 2,
    })
    const retryRef = await retryRuntime.start(RetryAndSiblingWorkflow, { kind: "retry" }, { workflowId: "a-retry" })
    const okRef = await retryRuntime.start(RetryAndSiblingWorkflow, { kind: "ok" }, { workflowId: "b-ok" })
    await expect(retryRuntime.drain({ maxActivations: 2 })).resolves.toEqual({ activations: 2 })
    expect(retryCalls).toEqual(["retry"])
    expect(await retryProvider.loadInstance(okRef)).toMatchObject({ status: "completed" })
    expect(await retryProvider.loadInstance(retryRef)).toMatchObject({
      status: "running",
      sequence: 0,
    })

    const siblingStarted = deferred()
    let badAttempts = 0
    let siblingAttempts = 0
    let siblingAborted = false
    const ErrorWorkflow = defineWorkflow({
      name: "concurrent_error_abort",
      version: 1,
      input: z.object({ kind: z.enum(["bad", "sibling"]) }),
      output: z.object({ kind: z.string() }),
      common: z.object({ kind: z.enum(["bad", "sibling"]) }),
      initial(input) {
        return start({ common: { kind: input.kind }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx, common }) => {
            if (common.kind === "bad") {
              badAttempts += 1
              if (badAttempts === 1) {
                await siblingStarted.promise
                throw new Error("boom")
              }
              return complete({ kind: "bad" })
            }
            await ctx.activity("cooperative", async ({ signal: activitySignal }) => {
              siblingAttempts += 1
              if (siblingAttempts > 1) {
                return "recovered"
              }
              siblingStarted.resolve()
              await new Promise((_resolve, reject) => {
                activitySignal.addEventListener(
                  "abort",
                  () => {
                    siblingAborted = true
                    reject(activitySignal.reason ?? new Error("aborted"))
                  },
                  { once: true },
                )
              })
            })
            return complete({ kind: common.kind })
          },
        }),
      },
    })

    const errorPath = await storePath()
    const errorProvider = testProvider(errorPath)
    const errorRuntime = new DurableRuntime(errorProvider, {
      workflows: [ErrorWorkflow],
      workerId: "error-abort-worker",
      maxConcurrentActivations: 2,
    })
    const badRef = await errorRuntime.start(ErrorWorkflow, { kind: "bad" }, { workflowId: "a-bad" })
    const siblingRef = await errorRuntime.start(ErrorWorkflow, { kind: "sibling" }, { workflowId: "b-sibling" })
    await expect(errorRuntime.drain({ maxActivations: 2 })).rejects.toThrow("boom")
    expect(siblingAborted).toBe(true)
    expect(await errorProvider.loadInstance(badRef)).toMatchObject({ status: "running", sequence: 0 })
    expect(await errorProvider.loadInstance(siblingRef)).toMatchObject({ status: "running", sequence: 0 })

    const recoveryRuntime = new DurableRuntime(testProvider(errorPath), {
      workflows: [ErrorWorkflow],
      workerId: "error-abort-recovery-worker",
      maxConcurrentActivations: 2,
    })
    await expect(recoveryRuntime.drain({ maxActivations: 2 })).resolves.toEqual({ activations: 2 })
    expect(badAttempts).toBe(2)
    expect(siblingAttempts).toBe(2)
    expect(await errorProvider.loadInstance(badRef)).toMatchObject({ status: "completed" })
    expect(await errorProvider.loadInstance(siblingRef)).toMatchObject({ status: "completed" })
  })

  it("heartbeats long activations and releases dispatch shards after drain", async () => {
    const clock = manualClock()
    let releaseHandler: (() => void) | undefined
    const handlerStarted = new Promise<void>((resolve) => {
      releaseHandler = resolve
    })
    const LongWorkflow = defineWorkflow({
      name: "long_activation",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async () => {
            await handlerStarted
            return complete({ ok: true })
          },
        }),
      },
    })

    const path = await storePath()
    const provider = testProvider(path)
    const workerA = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [LongWorkflow],
      workerId: "long-worker-a",
      dispatchLeaseMs: 10,
      activationLeaseMs: 10,
      leaseHeartbeatIntervalMs: 1,
    })
    const ref = await workerA.start(LongWorkflow, {}, { workflowId: "long-activation" })
    const draining = workerA.drain({ maxActivations: 1 })

    await new Promise((resolve) => setTimeout(resolve, 5))
    clock.advance(8)
    await new Promise((resolve) => setTimeout(resolve, 5))
    const workerBProvider = testProvider(path)
    await expect(
      workerBProvider.claimDispatchShard({
        shardId: workflowPartitionShard(ref.workflowId, ref.runId, 1),
        ownerId: "long-worker-b",
        now: clock.clock().toISOString(),
        leaseMs: 10,
      }),
    ).resolves.toBeNull()

    releaseHandler?.()
    await expect(draining).resolves.toEqual({ activations: 1 })
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "completed" })
    await expect(
      workerBProvider.claimDispatchShard({
        shardId: 0,
        ownerId: "long-worker-b",
        now: clock.clock().toISOString(),
        leaseMs: 10,
      }),
    ).resolves.toMatchObject({ ownerId: "long-worker-b" })
  })

  it("heartbeats each concurrent activation lease independently", async () => {
    const clock = manualClock()
    const started: number[] = []
    const releases = new Map<number, () => void>()
    const ConcurrentLeaseWorkflow = defineWorkflow({
      name: "concurrent_activation_lease",
      version: 1,
      input: z.object({ index: z.number() }),
      output: z.object({ index: z.number() }),
      common: z.object({ index: z.number() }),
      initial(input) {
        return start({ common: { index: input.index }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => {
            started.push(common.index)
            await new Promise<void>((resolve) => {
              releases.set(common.index, resolve)
            })
            return complete({ index: common.index })
          },
        }),
      },
    })

    const path = await storePath()
    const provider = testProvider(path)
    const runtime = new DurableRuntime(provider, {
      clock: clock.clock,
      workflows: [ConcurrentLeaseWorkflow],
      workerId: "multi-lease-worker",
      dispatchLeaseMs: 10,
      activationLeaseMs: 10,
      leaseHeartbeatIntervalMs: 1,
      maxConcurrentActivations: 2,
    })
    const refs = await Promise.all([
      runtime.start(ConcurrentLeaseWorkflow, { index: 0 }, { workflowId: "multi-lease-0" }),
      runtime.start(ConcurrentLeaseWorkflow, { index: 1 }, { workflowId: "multi-lease-1" }),
    ])
    const draining = runtime.drain({ maxActivations: 2 })
    await waitFor(() => started.length === 2)

    const claims = (await provider.listActivationClaims()).filter(
      (claim) => claim.ownerId === "multi-lease-worker",
    )
    expect(claims).toHaveLength(2)

    clock.advance(8)
    await new Promise((resolve) => setTimeout(resolve, 10))
    clock.advance(8)
    await new Promise((resolve) => setTimeout(resolve, 10))

    const competingProvider = testProvider(path)
    await expect(
      competingProvider.claimDispatchShard({
        shardId: 0,
        ownerId: "multi-lease-competitor",
        now: clock.clock().toISOString(),
        leaseMs: 10,
      }),
    ).resolves.toBeNull()

    for (const release of releases.values()) {
      release()
    }
    await expect(draining).resolves.toEqual({ activations: 2 })
    await expect(Promise.all(refs.map((ref) => provider.loadInstance(ref)))).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: "completed", output: { index: 0 } }),
        expect.objectContaining({ status: "completed", output: { index: 1 } }),
      ]),
    )
  })

  it("requires a live activation lease for effect mutation and prevents terminal overwrites", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "effect_authority",
      workflowVersion: 1,
      workflowId: "effect-authority",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { effect_authority: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 100,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "run" })

    const reservation = await provider.getOrReserveEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      key: "once",
      now: "2026-01-01T00:00:00.000Z",
    })
    expect(reservation.status).toBe("reserved")
    if (reservation.status !== "reserved") {
      throw new Error("expected reserved effect")
    }

    await provider.completeEffect({
      ...ref,
      activationId: activation!.activationId,
      workerId: "worker-a",
      effectId: reservation.effectId,
      attemptId: reservation.attemptId,
      result: { ok: true },
      now: "2026-01-01T00:00:00.000Z",
    })
    await expect(
      provider.failEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: reservation.effectId,
        attemptId: reservation.attemptId,
        error: { message: "late" },
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).rejects.toThrow("Effect is already terminal")
    await expect(
      provider.completeEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-b",
        effectId: reservation.effectId,
        attemptId: reservation.attemptId,
        result: { ok: false },
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).rejects.toThrow("Lost activation lease")
    await expect(
      provider.heartbeatEffect({
        ...ref,
        activationId: activation!.activationId,
        workerId: "worker-a",
        effectId: reservation.effectId,
        attemptId: reservation.attemptId,
        now: "2026-01-01T00:00:00.101Z",
      }),
    ).rejects.toThrow("Lost activation lease")
  })

  it("rejects checkpoint commits with the wrong activation or selected event", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "commit_event_match",
      workflowVersion: 1,
      workflowId: "commit-event-match",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    const otherRef = await provider.createInstance({
      workflowName: "commit_event_match",
      workflowVersion: 1,
      workflowId: "commit-event-other",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:10:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    const firstSignal = await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: { n: 1 },
      receivedAt: "2026-01-01T00:00:00.000Z",
    })
    const secondSignal = await provider.appendSignal({
      ...ref,
      type: "finish",
      payload: { n: 2 },
      receivedAt: "2026-01-01T00:00:01.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const activation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { commit_event_match: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(activation).toMatchObject({ kind: "event" })

    await expect(
      provider.commitCheckpoint({
        ...otherRef,
        expectedSequence: 0,
        activationId: activation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 0 })
    await expect(
      provider.commitCheckpoint({
        ...ref,
        expectedSequence: 0,
        activationId: activation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
        consumeSignalId: secondSignal.signalId,
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 0 })
    expect((await provider.listSignals()).find((record) => record.signalId === firstSignal.signalId)?.consumedBySequence).toBeUndefined()
    expect((await provider.listSignals()).find((record) => record.signalId === secondSignal.signalId)?.consumedBySequence).toBeUndefined()
  })

  it("rejects checkpoint commits that consume a different child record than the claimed event", async () => {
    const provider = testProvider(await storePath())
    const parentRef = await provider.createInstance({
      workflowName: "child_consume_parent",
      workflowVersion: 1,
      workflowId: "child-consume-parent",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "setup", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const setupActivation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { child_consume_parent: { version: 1 }, child_consume_child: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(setupActivation).toMatchObject({ kind: "run", workflowId: "child-consume-parent" })

    await provider.createChildInstance({
      workflowName: "child_consume_child",
      workflowVersion: 1,
      workflowId: "child-consume-1",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
      parentWorkflowId: parentRef.workflowId,
      parentRunId: parentRef.runId,
      activationId: setupActivation!.activationId,
      workerId: "worker-a",
      leaseNow: "2026-01-01T00:00:00.000Z",
      key: "child-1",
    })
    await provider.createChildInstance({
      workflowName: "child_consume_child",
      workflowVersion: 1,
      workflowId: "child-consume-2",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:10:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
      parentWorkflowId: parentRef.workflowId,
      parentRunId: parentRef.runId,
      activationId: setupActivation!.activationId,
      workerId: "worker-a",
      leaseNow: "2026-01-01T00:00:00.000Z",
      key: "child-2",
    })
    await provider.commitCheckpoint({
      ...parentRef,
      expectedSequence: 0,
      activationId: setupActivation!.activationId,
      workerId: "worker-a",
      workflowVersion: 1,
      next: {
        status: "running",
        common: {},
        phase: { name: "waiting", data: {} },
      },
      waits: [
        {
          kind: "child",
          name: "child_done",
          workflowName: "child_consume_child",
          workflowVersion: 1,
          workflowId: "child-consume-1",
          runId: "run-1",
        },
      ],
      now: "2026-01-01T00:00:00.000Z",
    })
    const childRun = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { child_consume_parent: { version: 1 }, child_consume_child: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(childRun).toMatchObject({ kind: "run", workflowId: "child-consume-1" })
    await provider.commitCheckpoint({
      workflowId: "child-consume-1",
      runId: "run-1",
      expectedSequence: 0,
      activationId: childRun!.activationId,
      workerId: "worker-a",
      workflowVersion: 1,
      next: { status: "completed", output: { ok: true } },
      waits: [],
      now: "2026-01-01T00:00:00.000Z",
    })

    const parentActivation = (
      await provider.claimReadyActivation({
        workerId: "worker-a",
        shardIds: [0],
        workflows: { child_consume_parent: { version: 1 }, child_consume_child: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    const wrongChild = (await provider.listChildren()).find((record) => record.workflowId === "child-consume-2")
    expect(parentActivation).toMatchObject({
      kind: "event",
      event: { kind: "child", childRecordId: expect.not.stringMatching(wrongChild!.childRecordId) },
    })

    await expect(
      provider.commitCheckpoint({
        ...parentRef,
        expectedSequence: 1,
        activationId: parentActivation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
        consumeChildRecordId: wrongChild!.childRecordId,
      }),
    ).resolves.toMatchObject({ ok: false, sequence: 1 })
    expect(
      (await provider.listChildren()).find((record) => record.childRecordId === wrongChild!.childRecordId)
        ?.deliveredBySequence,
    ).toBeUndefined()
  })

  it("blocks migration while an old-version activation is uncompleted", async () => {
    const provider = testProvider(await storePath())
    const ref = await provider.createInstance({
      workflowName: "migration_pin",
      workflowVersion: 1,
      workflowId: "migration-pin",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "run", data: {} },
      waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
      now: "2026-01-01T00:00:00.000Z",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-v1",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
    const oldActivation = (
      await provider.claimReadyActivation({
        workerId: "worker-v1",
        shardIds: [0],
        workflows: { migration_pin: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      })
    ).activation
    expect(oldActivation).toMatchObject({ kind: "run" })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-v1",
        shardIds: [0],
        workflows: { migration_pin: { version: 2 } },
        now: "2026-01-01T00:00:00.002Z",
        leaseMs: 60_000,
      }),
    ).resolves.toMatchObject({ activation: null })

    await provider.commitCheckpoint({
      ...ref,
      expectedSequence: 0,
      activationId: oldActivation!.activationId,
      workerId: "worker-v1",
      workflowVersion: 1,
      next: {
        status: "running",
        common: {},
        phase: { name: "waiting", data: {} },
      },
      waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
      now: "2026-01-01T00:00:00.002Z",
    })

    await expect(
      provider.claimReadyActivation({
        workerId: "worker-v1",
        shardIds: [0],
        workflows: { migration_pin: { version: 2 } },
        now: "2026-01-01T00:00:00.002Z",
        leaseMs: 60_000,
      }),
    ).resolves.toMatchObject({ activation: { kind: "migration" } })
  })

  it("validates duplicate signal names against the current durable wait", async () => {
    const FirstSchema = z.object({ a: z.string() })
    const SecondSchema = z.object({ b: z.string() })
    const DuplicateSignalWorkflow = defineWorkflow({
      name: "duplicate_signal",
      version: 1,
      input: z.object({}),
      output: z.object({ b: z.string() }),
      initial() {
        return start({ phase: "second", data: {} })
      },
      phases: {
        first: phase({
          on: {
            same: signal(FirstSchema, async ({ event }) => complete({ b: event.a })),
          },
        }),
        second: phase({
          on: {
            same: signal(SecondSchema, async ({ event }) => complete({ b: event.b })),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [DuplicateSignalWorkflow],
      workerId: "duplicate-signal-worker",
    })
    const ref = await runtime.start(DuplicateSignalWorkflow, {}, { workflowId: "duplicate-signal" })
    await expect(runtime.signal(DuplicateSignalWorkflow, ref, "same", { b: "current" })).resolves.toMatchObject({
      type: "same",
    })
    await runtime.drain()
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { b: "current" },
    })
  })

  it("rejects ambiguous duplicate signal names when there is no current matching wait", async () => {
    const FirstSchema = z.object({ a: z.string() })
    const SecondSchema = z.object({ b: z.string() })
    const AmbiguousSignalWorkflow = defineWorkflow({
      name: "ambiguous_signal",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "idle", data: {} })
      },
      phases: {
        idle: phase({
          run: async () => stay(),
        }),
        first: phase({
          on: {
            same: signal(FirstSchema, async () => complete({ ok: true })),
          },
        }),
        second: phase({
          on: {
            same: signal(SecondSchema, async () => complete({ ok: true })),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [AmbiguousSignalWorkflow],
      workerId: "ambiguous-signal-worker",
    })
    const ref = await runtime.start(AmbiguousSignalWorkflow, {}, { workflowId: "ambiguous-signal" })
    await expect(runtime.signal(AmbiguousSignalWorkflow, ref, "same", { a: "x" })).rejects.toThrow(
      "Ambiguous signal same",
    )
  })

  it("ctx.child.cancel cancels a running child and delivers a failed child event", async () => {
    const WaitingChildWorkflow = defineWorkflow({
      name: "cancel_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const CancelParentWorkflow = defineWorkflow({
      name: "cancel_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ errorName: z.string() }),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            const handle = await ctx.child.start("child", WaitingChildWorkflow, {})
            return go("canceling", { handle })
          },
        }),
        canceling: phase({
          state: z.object({ handle: z.any() }),
          run: async ({ ctx, data }) => {
            await ctx.child.cancel(data.handle)
            return go("waiting", { handle: data.handle })
          },
        }),
        waiting: phase({
          state: z.object({ handle: z.any() }),
          on: {
            child_done: child(
              ({ data }) => data.handle,
              async ({ event }) => complete({ errorName: event.ok ? "none" : (event.error.name ?? "") }),
            ),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [CancelParentWorkflow, WaitingChildWorkflow],
      workerId: "cancel-child-worker",
    })
    const ref = await runtime.start(CancelParentWorkflow, {}, { workflowId: "cancel-parent" })
    await runtime.drain({ maxActivations: 3 })

    const children = await provider.listChildren()
    expect(children).toHaveLength(1)
    expect(children[0]).toMatchObject({
      status: "failed",
      error: { name: "ChildCanceled", message: "Child canceled by parent" },
      deliveredBySequence: 3,
    })
    expect(await provider.loadInstance({ workflowId: children[0].workflowId, runId: children[0].runId })).toMatchObject({
      status: "canceled",
      cancelReason: "Child canceled by parent",
    })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { errorName: "ChildCanceled" },
    })
  })

  it("ctx.child.cancel is idempotent after child completion and does not overwrite output", async () => {
    const FastChildWorkflow = defineWorkflow({
      name: "completed_cancel_child",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async () => complete({ value: "done" }),
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "completed_cancel_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            const handle = await ctx.child.start("child", FastChildWorkflow, {})
            return go("waiting", { handle })
          },
        }),
        waiting: phase({
          state: z.object({ handle: z.any() }),
          on: {
            child_done: child(
              ({ data }) => data.handle,
              async ({ event, data }) => go("cancel_completed", { handle: data.handle, value: event.ok ? event.output.value : "failed" }),
            ),
          },
        }),
        cancel_completed: phase({
          state: z.object({ handle: z.any(), value: z.string() }),
          run: async ({ ctx, data }) => {
            await ctx.child.cancel(data.handle)
            return complete({ value: data.value })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, FastChildWorkflow],
      workerId: "cancel-completed-child-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "completed-cancel-parent" })
    await runtime.drain({ maxActivations: 4 })

    const childRecord = (await provider.listChildren())[0]
    expect(childRecord).toMatchObject({ status: "completed", output: { value: "done" } })
    expect(await provider.loadInstance({ workflowId: childRecord.workflowId, runId: childRecord.runId })).toMatchObject({
      status: "completed",
      output: { value: "done" },
    })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { value: "done" },
    })
  })

  it("parent cancellation cancels started children by default", async () => {
    const WaitingChildWorkflow = defineWorkflow({
      name: "parent_close_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "parent_close_cancel",
      version: 1,
      input: z.object({}),
      output: z.object({}),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", WaitingChildWorkflow, {})
            return cancel("stop")
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, WaitingChildWorkflow],
      workerId: "parent-close-cancel-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "parent-close-cancel" })
    await runtime.drain({ maxActivations: 1 })

    const childRecord = (await provider.listChildren())[0]
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "canceled", cancelReason: "stop" })
    expect(childRecord).toMatchObject({
      status: "failed",
      parentClosePolicy: "cancel",
      deliveredBySequence: 1,
      error: { name: "ParentClosed" },
    })
    expect(await provider.loadInstance({ workflowId: childRecord.workflowId, runId: childRecord.runId })).toMatchObject({
      status: "canceled",
    })
  })

  it("parent failure cancels started children by default", async () => {
    const WaitingChildWorkflow = defineWorkflow({
      name: "parent_fail_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "parent_fail_cancel",
      version: 1,
      input: z.object({}),
      output: z.object({}),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", WaitingChildWorkflow, {})
            return fail(new Error("boom"))
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, WaitingChildWorkflow],
      workerId: "parent-fail-cancel-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "parent-fail-cancel" })
    await runtime.drain({ maxActivations: 1 })

    const childRecord = (await provider.listChildren())[0]
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "failed" })
    expect(childRecord).toMatchObject({
      status: "failed",
      parentClosePolicy: "cancel",
      deliveredBySequence: 1,
      error: { name: "ParentClosed" },
    })
  })

  it("parent cancellation cascades through cancel-policy child trees", async () => {
    const GrandchildWorkflow = defineWorkflow({
      name: "cascade_grandchild",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const ChildWorkflow = defineWorkflow({
      name: "cascade_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            const grandchild = await ctx.child.start("grandchild", GrandchildWorkflow, {})
            return go("waiting", { grandchild })
          },
        }),
        waiting: phase({
          state: z.object({ grandchild: z.any() }),
          on: {
            grandchild_done: child(
              ({ data }) => data.grandchild,
              async ({ event }) => complete({ ok: event.ok }),
            ),
          },
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "cascade_parent",
      version: 1,
      input: z.object({}),
      output: z.object({}),
      initial() {
        return start({ phase: "start", data: {} })
      },
      phases: {
        start: phase({
          run: async ({ ctx }) => {
            const childHandle = await ctx.child.start("child", ChildWorkflow, {})
            return go("waiting", { childHandle })
          },
        }),
        waiting: phase({
          state: z.object({ childHandle: z.any() }),
          on: {
            stop: signal(z.object({}), async () => cancel("stop")),
            child_done: child(
              ({ data }) => data.childHandle,
              async () => complete({}),
            ),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, ChildWorkflow, GrandchildWorkflow],
      workerId: "cascade-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "cascade-parent" })
    await runtime.drain({ maxActivations: 2 })

    const beforeCancel = await provider.listChildren()
    expect(beforeCancel).toHaveLength(2)
    expect(beforeCancel.map((record) => record.status).sort()).toEqual(["started", "started"])

    await runtime.signal(ParentWorkflow, ref, "stop", {})
    await runtime.drain({ maxActivations: 1 })

    const childRecords = await provider.listChildren()
    const childRecord = childRecords.find((record) => record.workflowName === "cascade_child")
    const grandchildRecord = childRecords.find((record) => record.workflowName === "cascade_grandchild")
    expect(childRecord).toMatchObject({ status: "failed", error: { name: "ParentClosed" } })
    expect(grandchildRecord).toMatchObject({ status: "failed", error: { name: "ParentClosed" } })
    expect(await provider.loadInstance({ workflowId: childRecord!.workflowId, runId: childRecord!.runId })).toMatchObject({
      status: "canceled",
    })
    expect(await provider.loadInstance({ workflowId: grandchildRecord!.workflowId, runId: grandchildRecord!.runId })).toMatchObject({
      status: "canceled",
    })
  })

  it("parentClosePolicy abandon leaves children running and detached after parent cancellation", async () => {
    const WaitingChildWorkflow = defineWorkflow({
      name: "abandon_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "waiting", data: {} })
      },
      phases: {
        waiting: phase({
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "abandon_parent",
      version: 1,
      input: z.object({}),
      output: z.object({}),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", WaitingChildWorkflow, {}, { parentClosePolicy: "abandon" })
            return cancel("stop")
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, WaitingChildWorkflow],
      workerId: "abandon-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "abandon-parent" })
    await runtime.drain({ maxActivations: 1 })

    const childRecord = (await provider.listChildren())[0]
    expect(childRecord).toMatchObject({
      status: "abandoned",
      parentClosePolicy: "abandon",
      deliveredBySequence: 1,
    })
    expect(await provider.loadInstance({ workflowId: childRecord.workflowId, runId: childRecord.runId })).toMatchObject({
      status: "running",
    })

    await runtime.signal(WaitingChildWorkflow, { workflowId: childRecord.workflowId, runId: childRecord.runId }, "finish", {})
    await runtime.drain({ maxActivations: 1 })
    expect(await provider.loadInstance({ workflowId: childRecord.workflowId, runId: childRecord.runId })).toMatchObject({
      status: "completed",
      output: { ok: true },
    })
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "canceled", cancelReason: "stop" })
    expect((await provider.listChildren())[0]).toMatchObject({ status: "abandoned" })
  })

  it("buffers checkpoint child starts until parent checkpoint commit", async () => {
    const path = await storePath()
    let shouldThrow = true
    const handles: ChildHandle[] = []
    const ChildWorkflow = defineWorkflow({
      name: "checkpoint_buffered_child",
      version: 1,
      input: z.object({ value: z.string() }),
      output: z.object({ ok: z.boolean() }),
      initial(input) {
        return start({ phase: "waiting", data: { value: input.value } })
      },
      phases: {
        waiting: phase({
          state: z.object({ value: z.string() }),
          on: {
            finish: signal(z.object({}), async () => complete({ ok: true })),
          },
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "checkpoint_buffered_child_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ childWorkflowId: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const handle = await ctx.child.start("child", ChildWorkflow, { value: "one" })
            handles.push(handle)
            if (shouldThrow) {
              shouldThrow = false
              throw new Error("crash before parent checkpoint")
            }
            return complete({ childWorkflowId: handle.workflowId })
          },
        }),
      },
    })

    const provider = testProvider(path)
    const runtimeA = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, ChildWorkflow],
      workerId: "checkpoint-child-worker-a",
    })
    const ref = await runtimeA.start(ParentWorkflow, {}, { workflowId: "checkpoint-child-parent" })

    await expect(runtimeA.drain({ maxActivations: 1 })).rejects.toThrow("crash before parent checkpoint")
    expect(await provider.listChildren()).toEqual([])
    expect(await provider.loadInstance({ workflowId: handles[0].workflowId, runId: handles[0].runId })).toBeNull()

    const runtimeB = new DurableRuntime(testProvider(path), {
      workflows: [ParentWorkflow, ChildWorkflow],
      workerId: "checkpoint-child-worker-b",
    })
    await runtimeB.drain({ maxActivations: 1 })

    expect(handles).toHaveLength(2)
    expect(handles[1].workflowId).toBe(handles[0].workflowId)
    expect(handles[1].runId).not.toBe(handles[0].runId)
    expect(await provider.listChildren()).toHaveLength(1)
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { childWorkflowId: handles[0].workflowId },
    })
    expect(await provider.loadInstance(handles[1])).toMatchObject({
      status: "running",
      phase: { name: "waiting", data: { value: "one" } },
    })
  })

  it("fails checkpoint child start commit conflicts as non-retryable", async () => {
    const ChildWorkflow = defineWorkflow({
      name: "checkpoint_child_commit_conflict_child",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async () => complete({ ok: true }),
        }),
      },
    })
    const ParentWorkflow = defineWorkflow({
      name: "checkpoint_child_commit_conflict_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", ChildWorkflow, {}, { workflowId: "existing-child-instance" })
            return complete({ ok: true })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    await provider.createInstance({
      workflowName: ChildWorkflow.name,
      workflowVersion: ChildWorkflow.version,
      workflowId: "existing-child-instance",
      runId: "run-1",
      partitionShard: 0,
      common: {},
      phase: { name: "waiting", data: {} },
      waits: [],
      now: "2026-01-01T00:00:00.000Z",
    })
    const runtime = new DurableRuntime(provider, {
      workflows: [ParentWorkflow, ChildWorkflow],
      workerId: "checkpoint-child-conflict-worker",
    })
    const ref = await runtime.start(ParentWorkflow, {}, { workflowId: "checkpoint-child-conflict-parent" })

    await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({ activations: 1 })
    expect(await provider.loadInstance(ref)).toMatchObject({ status: "completed", sequence: 1 })
    expect(
      (await provider.listChildren()).filter((childRecord) =>
        childRecord.parentWorkflowId === ref.workflowId && childRecord.parentRunId === ref.runId,
      ),
    ).toHaveLength(1)
  })

  it("applies child workflow id reuse policies for repeated starts", async () => {
    const ChildWorkflow = defineWorkflow({
      name: "conflict_child",
      version: 1,
      input: z.object({ value: z.string() }),
      output: z.object({ value: z.string() }),
      common: z.object({ value: z.string() }),
      initial(input) {
        return start({ common: { value: input.value }, phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ common }) => complete({ value: common.value }),
        }),
      },
    })
    const ReuseParent = defineWorkflow({
      name: "reuse_child_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ same: z.boolean(), secondCreated: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const first = await ctx.child.start("first", ChildWorkflow, { value: "first" }, { workflowId: "reuse-child" })
            const second = await ctx.child.start("second", ChildWorkflow, { value: "second" }, { workflowId: "reuse-child" })
            return complete({
              same: first.workflowId === second.workflowId && first.runId === second.runId,
              secondCreated: second.created,
            })
          },
        }),
      },
    })
    const AlwaysParent = defineWorkflow({
      name: "always_child_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ different: z.boolean(), secondCreated: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const first = await ctx.child.start("first", ChildWorkflow, { value: "first" }, {
              workflowId: "always-child",
              workflowIdReusePolicy: "always",
            })
            const second = await ctx.child.start("second", ChildWorkflow, { value: "second" }, {
              workflowId: "always-child",
              workflowIdReusePolicy: "always",
            })
            return complete({
              different: first.runId !== second.runId,
              secondCreated: second.created,
            })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ChildWorkflow, ReuseParent, AlwaysParent],
      workerId: "child-reuse-worker",
    })

    const reuseRef = await runtime.start(ReuseParent, {}, { workflowId: "reuse-parent" })
    await runtime.drain({ maxActivations: 10 })
    expect(await provider.loadInstance(reuseRef)).toMatchObject({
      status: "completed",
      output: { same: true, secondCreated: false },
    })
    expect((await provider.listChildren()).filter((record) => record.workflowId === "reuse-child")).toHaveLength(1)

    const alwaysRef = await runtime.start(AlwaysParent, {}, { workflowId: "always-parent" })
    await runtime.drain({ maxActivations: 10 })
    expect(await provider.loadInstance(alwaysRef)).toMatchObject({
      status: "completed",
      output: { different: true, secondCreated: true },
    })
    expect((await provider.listChildren()).filter((record) => record.workflowId === "always-child")).toHaveLength(2)
  })

  it("exposes only ctx.child.start and ctx.child.cancel", async () => {
    let childKeys: string[] = []
    const NoChildRunWorkflow = defineWorkflow({
      name: "no_child_run",
      version: 1,
      input: z.object({}),
      output: z.object({ ok: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            childKeys = Object.keys(ctx.child).sort()
            return complete({ ok: !("run" in ctx.child) && !("result" in ctx.child) })
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [NoChildRunWorkflow],
      workerId: "no-child-run-worker",
    })
    const ref = await runtime.start(NoChildRunWorkflow, {}, { workflowId: "no-child-run" })
    await runtime.drain()
    expect(childKeys).toEqual(["cancel", "start"])
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true },
    })
  })
})
