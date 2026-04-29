/*
 * Shows two immediate run-phase handoffs around a durable signal:
 * start enters a run phase immediately, the workflow then waits for an
 * `approved` signal, and that signal transitions into another immediate run
 * phase that records an activity and completes.
 */

import { z } from "zod"
import {
  complete,
  defineWorkflow,
  go,
  phase,
  query,
  signal,
  start,
} from "../durable.js"
import { cleanupDemoStore, committed, demoRuntime } from "./_shared.js"

const ImmediateApprovalWorkflow = defineWorkflow({
  name: "demo_immediate_approval",
  version: 1,
  input: z.object({ name: z.string() }),
  output: z.object({ message: z.string(), approvedAt: z.string() }),
  common: z.object({ name: z.string() }),

  initial(input) {
    return start({
      common: { name: input.name },
      phase: "boot_immediately",
      data: {},
    })
  },

  queries: {
    status: query(
      z.object({
        sequence: z.number(),
        status: z.string(),
        phase: z.string().optional(),
      }),
      ({ sequence, snapshot }) => ({
        sequence,
        status: snapshot.status,
        phase: snapshot.status === "running" ? snapshot.phase.name : undefined,
      }),
    ),
  },

  phases: {
    boot_immediately: phase({
      run: async ({ ctx }) => {
        return go("waiting_for_approval", {
          enteredAt: ctx.now(),
        })
      },
    }),

    waiting_for_approval: phase({
      state: z.object({ enteredAt: z.string() }),
      on: {
        approved: signal(z.object({ message: z.string() }), async ({ event }) => {
          return go("acknowledge_immediately", {
            message: event.message,
          })
        }),
      },
    }),

    acknowledge_immediately: phase({
      state: z.object({ message: z.string() }),
      run: async ({ ctx, common, data }) => {
        await ctx.activity("record_approval", () => ({
          name: common.name,
          message: data.message,
          recordedAt: ctx.now(),
        }))

        return complete({
          message: `${common.name}: ${data.message}`,
          approvedAt: ctx.now(),
        })
      },
    }),
  },
})

export async function runImmediateAndSignalDemo(): Promise<void> {
  const demoName = "immediate-and-signal"
  const { runtime, provider } = await demoRuntime(demoName, [ImmediateApprovalWorkflow])

  try {
    const ref = await runtime.start(
      ImmediateApprovalWorkflow,
      { name: "Ada" },
      { workflowId: "immediate-demo" },
    )
    await runtime.drain()
    console.log(
      "immediate + signal: after immediate boot",
      await runtime.query(ImmediateApprovalWorkflow, ref, "status"),
    )

    await runtime.signal(ImmediateApprovalWorkflow, ref, "approved", {
      message: "ship it",
    })
    await runtime.drain()
    console.log("immediate + signal: completed", await committed(provider, ref))
  } finally {
    provider.close()
    await cleanupDemoStore(demoName)
  }
}
