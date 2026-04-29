/*
 * Shows a local child workflow wait across runtime restart:
 * the parent starts a child from an immediate phase, persists a child wait,
 * and after reconstruction the child completion wakes and completes the parent.
 */

import { z } from "zod"
import {
  child,
  complete,
  defineWorkflow,
  DurableRuntime,
  go,
  phase,
  SqliteDurabilityProvider,
  start,
} from "../durable.js"
import { cleanupDemoStore, committed, demoRuntime, demoStorePath } from "./_shared.js"

const GreetingChildWorkflow = defineWorkflow({
  name: "demo_greeting_child",
  version: 1,
  input: z.object({ name: z.string() }),
  output: z.object({ greeting: z.string() }),
  common: z.object({ name: z.string() }),

  initial(input) {
    return start({
      common: { name: input.name },
      phase: "compose",
      data: {},
    })
  },

  phases: {
    compose: phase({
      run: async ({ ctx, common }) => {
        const greeting = await ctx.activity("compose_greeting", () => ({
          greeting: `Hello, ${common.name}!`,
        }))

        return complete(greeting)
      },
    }),
  },
})

const GreetingParentWorkflow = defineWorkflow({
  name: "demo_greeting_parent",
  version: 1,
  input: z.object({ name: z.string() }),
  output: z.object({ greeting: z.string(), completedAt: z.string() }),
  common: z.object({ name: z.string() }),

  initial(input) {
    return start({
      common: { name: input.name },
      phase: "start_child",
      data: {},
    })
  },

  phases: {
    start_child: phase({
      run: async ({ ctx, common }) => {
        const greeting = await ctx.child.start("greeting", GreetingChildWorkflow, {
          name: common.name,
        })

        return go("waiting_for_child", {
          greeting,
        })
      },
    }),

    waiting_for_child: phase({
      state: z.object({
        greeting: z.object({
          workflowName: z.string(),
          workflowVersion: z.number(),
          workflowId: z.string(),
          runId: z.string(),
        }),
      }),
      on: {
        greeting_finished: child(
          ({ data }) => data.greeting,
          async ({ ctx, event }) => {
            if (!event.ok) {
              return complete({
                greeting: "child failed",
                completedAt: ctx.now(),
              })
            }

            return complete({
              greeting: event.output.greeting,
              completedAt: ctx.now(),
            })
          },
        ),
      },
    }),
  },
})

export async function runChildWorkflowDemo(): Promise<void> {
  const demoName = "child-workflow"
  const workflows = [GreetingParentWorkflow, GreetingChildWorkflow]
  const first = await demoRuntime(demoName, workflows)
  let restartedProvider: SqliteDurabilityProvider | undefined

  try {
    const ref = await first.runtime.start(
      GreetingParentWorkflow,
      { name: "Ada" },
      { workflowId: "child-demo" },
    )
    await first.runtime.drain({ maxActivations: 1 })
    console.log("child workflow: parent waiting", await committed(first.provider, ref))

    first.provider.close()
    restartedProvider = new SqliteDurabilityProvider(demoStorePath(demoName))
    const restarted = new DurableRuntime(restartedProvider, {
      clock: first.clock,
      workflows,
      workerId: first.workerId,
    })
    await restarted.drain()
    console.log("child workflow: completed after restart", await committed(restartedProvider, ref))
  } finally {
    first.provider.close()
    restartedProvider?.close()
    await cleanupDemoStore(demoName)
  }
}
