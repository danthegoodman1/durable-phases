import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import { z } from "zod"
import {
  checkpoint,
  child,
  complete,
  defineWorkflow,
  DurableRuntime,
  go,
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
    ).resolves.toBeNull()

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

    const activationA = await providerA.claimReadyActivation({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { activation_owner: { version: 1 } },
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1_000,
    })
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
    ).resolves.toBeNull()

    await expect(
      providerB.claimReadyActivation({
        workerId: "worker-b",
        shardIds: [0],
        workflows: { activation_owner: { version: 1 } },
        now: "2026-01-01T00:00:01.001Z",
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({
      kind: "run",
      activationId: activationA!.activationId,
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
    const activation = await provider.claimReadyActivation({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { commit_conflict: { version: 1 } },
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 10,
    })
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

    await expect(
      provider.completeEffect({
        workflowId: "missing",
        runId: "run-1",
        effectId: "effect-missing",
        result: {},
      }),
    ).rejects.toThrow("Unknown effect: effect-missing")

    await expect(
      provider.failEffect({
        workflowId: "missing",
        runId: "run-1",
        effectId: "effect-missing",
        error: { message: "nope" },
      }),
    ).rejects.toThrow("Unknown effect: effect-missing")

    await expect(
      provider.heartbeatEffect({
        effectId: "effect-missing",
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
    const childRun = await provider.claimReadyActivation({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { ordering_parent: { version: 1 }, ordering_child: { version: 1 } },
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 60_000,
    })
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

    const parentEvent = await provider.claimReadyActivation({
      workerId: "worker-a",
      shardIds: [0],
      workflows: { ordering_parent: { version: 1 }, ordering_child: { version: 1 } },
      now: eventAt,
      leaseMs: 60_000,
    })

    expect(parentEvent).toMatchObject({
      kind: "event",
      workflowId: "ordering-parent",
      waitName: "child_done",
      event: { kind: "child" },
    })
  })
})
