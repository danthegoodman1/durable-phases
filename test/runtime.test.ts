import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import { z } from "zod"
import {
  cancel,
  checkpoint,
  child,
  complete,
  defineWorkflow,
  DurableRuntime,
  fail,
  go,
  NonRetryableError,
  phase,
  query,
  signal,
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

function testProvider(path: string): SqliteDurabilityProvider {
  const provider = new SqliteDurabilityProvider(path)
  testProviders.add(provider)
  return provider
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

            return checkpoint({
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

describe("durable workflow PoC", () => {
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

  it("uses checkpoint() for the bounded unbound-loop pattern", async () => {
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
            })

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
        idempotencyKey: "activity-context/run-1/activity-context/run-1/0/run/__run/with_context",
        heartbeatDetails: undefined,
        aborted: false,
      },
    ])
    const effects = (await provider.loadInstance(ref))?.effects ?? []
    expect(effects.find((effect) => effect.key === "with_context")).toMatchObject({
      status: "completed",
      attempt: 1,
      heartbeatAt: "2026-01-01T00:00:00.000Z",
      heartbeatDetails: { step: 1 },
    })
    expect(await provider.loadInstance(ref)).toMatchObject({
      status: "completed",
      output: { ok: true, value: "old:new" },
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
      attempt: 2,
      nextAttemptAt: "2026-01-01T00:00:01.000Z",
      maxElapsedMs: 1_500,
    })

    clock.advance(1_000)
    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("boom 2")
    expect(attempts).toEqual([1, 2])
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(nonRetryableRef))?.effects[0]).toMatchObject({
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
    expect((await abortProvider.loadInstance(abortRef))?.effects[0]).toMatchObject({
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
            })
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    const timedOut = (await provider.loadInstance(ref))?.effects[0]
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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
    const effects = (await provider.loadInstance(ref))?.effects ?? []
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
      timeoutKind: undefined,
    })
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
    expect((await provider.loadInstance(ref))?.effects[0]).toMatchObject({
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

  it("does not let a new shard owner steal an unexpired activation lease", async () => {
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
    ).resolves.toMatchObject({ activation: null })

    await expect(
      providerB.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { activation_owner: { version: 1 } },
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({
      activation: {
        kind: "run",
        activationId: activationA!.activationId,
      },
    })
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
            })

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
    ).resolves.toEqual({ ok: false, sequence: 0 })
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
    ).resolves.toEqual({ ok: false, sequence: 0 })
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
      phase: { name: "waiting", data: {} },
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
      activationId: "setup",
      key: "child",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
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
    ).resolves.toEqual({ ok: false, sequence: 0 })
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
    ).resolves.toEqual({ ok: false, sequence: 0 })
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
      phase: { name: "waiting", data: {} },
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
      activationId: "setup",
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
      activationId: "setup",
      key: "child-2",
    })
    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
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
        expectedSequence: 0,
        activationId: parentActivation!.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { ok: true } },
        waits: [],
        now: "2026-01-01T00:00:00.000Z",
        consumeChildRecordId: wrongChild!.childRecordId,
      }),
    ).resolves.toEqual({ ok: false, sequence: 0 })
    expect(wrongChild?.deliveredBySequence).toBeUndefined()
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
      leaseMs: 1,
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

    await provider.claimDispatchShard({
      shardId: 0,
      ownerId: "worker-v2",
      now: "2026-01-01T00:00:00.002Z",
      leaseMs: 60_000,
    })
    await expect(
      provider.claimReadyActivation({
        workerId: "worker-v2",
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
        workerId: "worker-v2",
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

  it("applies child conflict policies for repeated starts", async () => {
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
    const UseExistingParent = defineWorkflow({
      name: "use_existing_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ same: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            const first = await ctx.child.start("child", ChildWorkflow, { value: "first" }, { workflowId: "use-existing-child" })
            const second = await ctx.child.start("child", ChildWorkflow, { value: "second" }, { workflowId: "use-existing-child" })
            return complete({ same: first.workflowId === second.workflowId && first.runId === second.runId })
          },
        }),
      },
    })
    const FailParent = defineWorkflow({
      name: "fail_conflict_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ failed: z.boolean() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", ChildWorkflow, { value: "first" }, { workflowId: "fail-child" })
            try {
              await ctx.child.start("child", ChildWorkflow, { value: "second" }, { workflowId: "fail-child", conflictPolicy: "fail" })
              return complete({ failed: false })
            } catch {
              return complete({ failed: true })
            }
          },
        }),
      },
    })
    const TerminateParent = defineWorkflow({
      name: "terminate_conflict_parent",
      version: 1,
      input: z.object({}),
      output: z.object({ value: z.string() }),
      initial() {
        return start({ phase: "run", data: {} })
      },
      phases: {
        run: phase({
          run: async ({ ctx }) => {
            await ctx.child.start("child", ChildWorkflow, { value: "first" }, { workflowId: "terminate-child" })
            const second = await ctx.child.start("child", ChildWorkflow, { value: "second" }, {
              workflowId: "terminate-child",
              conflictPolicy: "terminate_existing",
            })
            return go("waiting", { handle: second })
          },
        }),
        waiting: phase({
          state: z.object({ handle: z.any() }),
          on: {
            done: child(
              ({ data }) => data.handle,
              async ({ event }) => complete({ value: event.ok ? event.output.value : "failed" }),
            ),
          },
        }),
      },
    })

    const provider = testProvider(await storePath())
    const runtime = new DurableRuntime(provider, {
      workflows: [ChildWorkflow, UseExistingParent, FailParent, TerminateParent],
      workerId: "child-conflict-worker",
    })

    const useExistingRef = await runtime.start(UseExistingParent, {}, { workflowId: "use-existing-parent" })
    await runtime.drain({ maxActivations: 10 })
    expect(await provider.loadInstance(useExistingRef)).toMatchObject({
      status: "completed",
      output: { same: true },
    })

    const failRef = await runtime.start(FailParent, {}, { workflowId: "fail-conflict-parent" })
    await runtime.drain({ maxActivations: 10 })
    expect(await provider.loadInstance(failRef)).toMatchObject({
      status: "completed",
      output: { failed: true },
    })

    const terminateRef = await runtime.start(TerminateParent, {}, { workflowId: "terminate-conflict-parent" })
    await runtime.drain({ maxActivations: 10 })
    expect(await provider.loadInstance(terminateRef)).toMatchObject({
      status: "completed",
      output: { value: "second" },
    })
    expect((await provider.listChildren()).filter((record) => record.workflowId === "terminate-child")).toHaveLength(1)
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
