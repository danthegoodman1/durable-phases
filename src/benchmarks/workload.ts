import { setTimeout as sleep } from "node:timers/promises"
import { z } from "zod"
import {
  child,
  complete,
  defineWorkflow,
  go,
  phase,
  signal,
  start,
  timer,
  type PersistedInstance,
} from "../durable.js"

export type BenchmarkCounters = {
  workflowStarts: number
  signals: number
  childStarts: number
  childCompletions: number
  timerHandlers: number
  bootActivities: number
  childActivities: number
  finishActivities: number
}

export function createBenchmarkCounters(): BenchmarkCounters {
  return {
    workflowStarts: 0,
    signals: 0,
    childStarts: 0,
    childCompletions: 0,
    timerHandlers: 0,
    bootActivities: 0,
    childActivities: 0,
    finishActivities: 0,
  }
}

export function createBenchmarkWorkflows(
  counters: BenchmarkCounters,
  options: { activityDelayMs: number },
) {
  const ChildWorkflow = defineWorkflow({
    name: "bench_child",
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
          const childValue = await ctx.activity("child_activity", async () => {
            await benchmarkDelay(options.activityDelayMs)
            counters.childActivities += 1
            return common.index * 10
          })
          return complete({ childValue })
        },
      }),
    },
  })

  const ParentWorkflow = defineWorkflow({
    name: "bench_parent",
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
          await ctx.activity("boot_activity", async () => {
            await benchmarkDelay(options.activityDelayMs)
            counters.bootActivities += 1
            return true
          })
          const handle = await ctx.child.start("child", ChildWorkflow, { index: common.index })
          counters.childStarts += 1
          return go("waiting_child", { handle })
        },
      }),
      waiting_child: phase({
        state: z.object({ handle: z.any() }),
        on: {
          child_done: child(
            ({ data }) => data.handle,
            async ({ event }) => {
              counters.childCompletions += 1
              return go("waiting_signal", {
                childValue: event.ok ? event.output.childValue : -1,
              })
            },
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
              counters.timerHandlers += 1
              await ctx.activity("finish_activity", async () => {
                await benchmarkDelay(options.activityDelayMs)
                counters.finishActivities += 1
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

  return {
    ParentWorkflow,
    ChildWorkflow,
    workflows: [ParentWorkflow, ChildWorkflow],
  }
}

export function mixedActionCount(counters: BenchmarkCounters): number {
  return (
    counters.workflowStarts +
    counters.signals +
    counters.childStarts +
    counters.childCompletions +
    counters.timerHandlers +
    counters.bootActivities +
    counters.childActivities +
    counters.finishActivities
  )
}

export function activityCount(counters: BenchmarkCounters): number {
  return counters.bootActivities + counters.childActivities + counters.finishActivities
}

export function verifyBenchmarkOutputs(instances: PersistedInstance[], expected: number): void {
  for (let index = 0; index < expected; index += 1) {
    const instance = instances.find((record) => record.workflowId === `bench-parent-${index}`)
    if (!instance) {
      throw new Error(`Missing parent workflow ${index}`)
    }
    if (instance.status !== "completed") {
      throw new Error(`Expected parent workflow ${index} to be completed, got ${instance.status}`)
    }
    const output = instance.output as {
      index?: number
      childValue?: number
      signalValue?: number
      finished?: boolean
    } | undefined
    if (
      output?.index !== index ||
      output.childValue !== index * 10 ||
      output.signalValue !== index + 1_000 ||
      output.finished !== true
    ) {
      throw new Error(`Unexpected output for parent workflow ${index}: ${JSON.stringify(output)}`)
    }
  }
}

export function verifyBenchmarkCounters(counters: BenchmarkCounters, expected: number): void {
  const entries: Array<[keyof BenchmarkCounters, number]> = [
    ["workflowStarts", expected],
    ["signals", expected],
    ["childStarts", expected],
    ["childCompletions", expected],
    ["timerHandlers", expected],
    ["bootActivities", expected],
    ["childActivities", expected],
    ["finishActivities", expected],
  ]
  for (const [name, value] of entries) {
    if (counters[name] !== value) {
      throw new Error(`Expected ${name} to be ${value}, got ${counters[name]}`)
    }
  }
}

async function benchmarkDelay(ms: number): Promise<void> {
  if (ms > 0) {
    await sleep(ms)
  }
}
