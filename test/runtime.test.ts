import { mkdtemp } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { describe, expect, it } from "vitest"
import { z } from "zod"
import {
  checkpoint,
  child,
  complete,
  defineWorkflow,
  DurableRuntime,
  go,
  JsonFileDurabilityProvider,
  phase,
  query,
  signal,
  start,
  stay,
  timer,
} from "../src/durable.js"

const addMs = (iso: string, ms: number) => new Date(new Date(iso).getTime() + ms).toISOString()

async function storePath(): Promise<string> {
  return join(await mkdtemp(join(tmpdir(), "durable-poc-")), "store.json")
}

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
        state: z.object({}),
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
  it("persists the initial snapshot and reloads it from the JSON provider", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = new JsonFileDurabilityProvider(path)
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

    const reloaded = await new JsonFileDurabilityProvider(path).loadInstance(ref)
    expect(reloaded?.phase?.name).toBe("boot")
    expect(reloaded?.sequence).toBe(0)
  })

  it("survives restart with a pending timer and commits stay() as a checkpoint", async () => {
    const path = await storePath()
    const clock = manualClock()
    const { counters, workflows, TestWorkflow } = makeWorkflowSuite()
    const provider = new JsonFileDurabilityProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a"] },
      { workflowId: "timer-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    expect((await provider.loadInstance(ref))?.phase?.name).toBe("waiting")

    clock.advance(1_000)
    const restartedProvider = new JsonFileDurabilityProvider(path)
    const restarted = new DurableRuntime(restartedProvider, { clock: clock.clock, workflows })
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
    const provider = new JsonFileDurabilityProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows })
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
    const provider = new JsonFileDurabilityProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows })
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
    const provider = new JsonFileDurabilityProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows })
    const ref = await runtime.start(
      TestWorkflow,
      { label: "Ada", items: ["a", "b", "c"] },
      { workflowId: "child-parent" },
    )

    await runtime.drain({ maxActivations: 1 })
    await runtime.signal(TestWorkflow, ref, "begin", {})
    await runtime.drain({ maxActivations: 5 })
    expect((await provider.loadInstance(ref))?.phase?.name).toBe("waiting_child")

    const restartedProvider = new JsonFileDurabilityProvider(path)
    const restarted = new DurableRuntime(restartedProvider, { clock: clock.clock, workflows })
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
      common: z.object({}),
      initial() {
        return start({
          common: {},
          phase: "unstable",
          data: {},
        })
      },
      phases: {
        unstable: phase({
          state: z.object({}),
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

    const provider = new JsonFileDurabilityProvider(path)
    const runtime = new DurableRuntime(provider, { clock: clock.clock, workflows: [UnstableWorkflow] })
    const ref = await runtime.start(UnstableWorkflow, {}, { workflowId: "unstable-1" })

    await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow("boom after durable effect")
    expect(calls.activity).toBe(1)
    expect((await provider.loadInstance(ref))?.sequence).toBe(0)

    const restartedProvider = new JsonFileDurabilityProvider(path)
    const restarted = new DurableRuntime(restartedProvider, {
      clock: clock.clock,
      workflows: [UnstableWorkflow],
    })
    await restarted.drain({ maxActivations: 1 })

    expect(calls.activity).toBe(1)
    expect(await restartedProvider.loadInstance(ref)).toMatchObject({
      status: "completed",
      sequence: 1,
      output: { ok: true },
    })
  })
})
