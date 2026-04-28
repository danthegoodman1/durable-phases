/*
 * Shows a durable timer wait and `stay()` checkpoint across runtime restart:
 * the workflow persists a pending timer, the runtime/provider are recreated,
 * the clock advances, and the timer handler patches phase data with `stay()`.
 */

import { z } from "zod"
import {
  complete,
  defineWorkflow,
  DurableRuntime,
  JsonFileDurabilityProvider,
  phase,
  query,
  signal,
  start,
  stay,
  timer,
} from "../durable.js"
import { addMs, demoRuntime, demoStorePath } from "./_shared.js"

const ReminderWorkflow = defineWorkflow({
  name: "demo_timer_stay",
  version: 1,
  input: z.object({ name: z.string() }),
  output: z.object({ remindersSent: z.number() }),
  common: z.object({ name: z.string() }),

  initial(input) {
    return start({
      common: { name: input.name },
      phase: "waiting",
      data: {
        remindersSent: 0,
        nextReminderAt: "2026-01-01T00:00:01.000Z",
      },
    })
  },

  queries: {
    progress: query(
      z.object({
        sequence: z.number(),
        status: z.string(),
        remindersSent: z.number().optional(),
      }),
      ({ sequence, snapshot }) => ({
        sequence,
        status: snapshot.status,
        remindersSent:
          snapshot.status === "running" && typeof snapshot.phase.data.remindersSent === "number"
            ? snapshot.phase.data.remindersSent
            : undefined,
      }),
    ),
  },

  phases: {
    waiting: phase({
      state: z.object({
        remindersSent: z.number(),
        nextReminderAt: z.string(),
      }),
      on: {
        reminder_due: timer(
          ({ data }) => data.nextReminderAt,
          async ({ ctx, common, data }) => {
            await ctx.activity(`send_reminder_${data.remindersSent + 1}`, () => ({
              name: common.name,
              sentAt: ctx.now(),
            }))

            return stay({
              remindersSent: data.remindersSent + 1,
              nextReminderAt: addMs(ctx.now(), 1_000),
            })
          },
        ),

        done: signal(z.object({}), async ({ data }) => {
          return complete({
            remindersSent: data.remindersSent,
          })
        }),
      },
    }),
  },
})

export async function runTimerStayRestartDemo(): Promise<void> {
  const first = await demoRuntime("timer-stay-restart", [ReminderWorkflow])

  const ref = await first.runtime.start(
    ReminderWorkflow,
    { name: "Ada" },
    { workflowId: "timer-demo" },
  )
  console.log("timer + stay: pending timer", await first.runtime.query(ReminderWorkflow, ref, "progress"))

  first.advance(1_000)
  const restarted = new DurableRuntime(new JsonFileDurabilityProvider(demoStorePath("timer-stay-restart")), {
    clock: first.clock,
    workflows: [ReminderWorkflow],
  })
  await restarted.drain()
  console.log("timer + stay: after restart and timer", await restarted.query(ReminderWorkflow, ref, "progress"))
}
