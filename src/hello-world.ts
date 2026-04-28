import { rm } from "node:fs/promises"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
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
} from "./durable.js"

const addMs = (iso: string, ms: number) => new Date(new Date(iso).getTime() + ms).toISOString()

const GreetingWorkflow = defineWorkflow({
  name: "hello_child_greeting",
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
      state: z.object({}),
      run: async ({ ctx, common }) => {
        const greeting = await ctx.activity("compose_greeting", () => ({
          greeting: `Hello, ${common.name}!`,
        }))

        return complete(greeting)
      },
    }),
  },
})

export const HelloWorldWorkflow = defineWorkflow({
  name: "hello_world_poc",
  version: 1,
  input: z.object({
    name: z.string(),
    items: z.array(z.string()),
  }),
  output: z.object({
    greeting: z.string(),
    processed: z.array(z.string()),
    remindersSent: z.number(),
    finishedAt: z.string(),
  }),
  common: z.object({
    name: z.string(),
  }),

  initial(input) {
    return start({
      common: { name: input.name },
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
        processedCount: z.number().optional(),
        remindersSent: z.number().optional(),
      }),
      ({ sequence, snapshot }) => {
        if (snapshot.status !== "running") {
          return { sequence, status: snapshot.status }
        }

        return {
          sequence,
          status: snapshot.status,
          phase: snapshot.phase.name,
          processedCount: Array.isArray(snapshot.phase.data.processed)
            ? snapshot.phase.data.processed.length
            : 0,
          remindersSent:
            typeof snapshot.phase.data.remindersSent === "number"
              ? snapshot.phase.data.remindersSent
              : 0,
        }
      },
    ),
  },

  phases: {
    boot: phase({
      state: z.object({ items: z.array(z.string()) }),
      run: async ({ ctx, data }) => {
        return go("waiting_for_start", {
          items: data.items,
          remindersSent: 0,
          nextReminderAt: addMs(ctx.now(), 1_000),
        })
      },
    }),

    waiting_for_start: phase({
      state: z.object({
        items: z.array(z.string()),
        remindersSent: z.number(),
        nextReminderAt: z.string(),
      }),
      on: {
        reminder_due: timer(
          ({ data }) => data.nextReminderAt,
          async ({ ctx, common, data }) => {
            await ctx.activity("send_start_reminder", () => ({
              sentTo: common.name,
              sentAt: ctx.now(),
            }))

            return stay({
              remindersSent: data.remindersSent + 1,
              nextReminderAt: addMs(ctx.now(), 1_000),
            })
          },
        ),

        start_processing: signal(z.object({ ok: z.boolean() }), async ({ data, event }) => {
          if (!event.ok) {
            return stay({})
          }

          return go("processing", {
            items: data.items,
            cursor: 0,
            processed: [],
            remindersSent: data.remindersSent,
          })
        }),
      },
    }),

    processing: phase({
      state: z.object({
        items: z.array(z.string()),
        cursor: z.number(),
        processed: z.array(z.string()),
        remindersSent: z.number(),
      }),
      run: async ({ ctx, common, data }) => {
        const chunk = data.items.slice(data.cursor, data.cursor + 2)
        if (chunk.length > 0) {
          const processedChunk = await ctx.activity(`process_${data.cursor}`, () =>
            chunk.map((item: string) => `${item.toUpperCase()} for ${common.name}`),
          )

          return checkpoint({
            cursor: data.cursor + chunk.length,
            processed: [...data.processed, ...processedChunk],
          })
        }

        const greeting = await ctx.child.start("greeting", GreetingWorkflow, {
          name: common.name,
        })

        return go("waiting_for_greeting", {
          greeting,
          processed: data.processed,
          remindersSent: data.remindersSent,
        })
      },
    }),

    waiting_for_greeting: phase({
      state: z.object({
        greeting: z.object({
          workflowName: z.string(),
          workflowVersion: z.number(),
          workflowId: z.string(),
          runId: z.string(),
        }),
        processed: z.array(z.string()),
        remindersSent: z.number(),
      }),
      on: {
        greeting_finished: child(
          ({ data }) => data.greeting,
          async ({ ctx, data, event }) => {
            if (!event.ok) {
              return complete({
                greeting: "Greeting failed",
                processed: data.processed,
                remindersSent: data.remindersSent,
                finishedAt: ctx.now(),
              })
            }

            return go("cooldown", {
              greeting: event.output.greeting,
              processed: data.processed,
              remindersSent: data.remindersSent,
              finishAt: addMs(ctx.now(), 1_000),
            })
          },
        ),
      },
    }),

    cooldown: phase({
      state: z.object({
        greeting: z.string(),
        processed: z.array(z.string()),
        remindersSent: z.number(),
        finishAt: z.string(),
      }),
      on: {
        finish_due: timer(
          ({ data }) => data.finishAt,
          async ({ ctx, data }) => {
            return complete({
              greeting: data.greeting,
              processed: data.processed,
              remindersSent: data.remindersSent,
              finishedAt: ctx.now(),
            })
          },
        ),
      },
    }),
  },
})

export async function runHelloWorldDemo(): Promise<void> {
  const storePath = resolve(".durable-demo/hello-world.json")
  await rm(storePath, { force: true })

  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const workflows = [HelloWorldWorkflow, GreetingWorkflow]
  const runtime = new DurableRuntime(new JsonFileDurabilityProvider(storePath), {
    clock,
    workflows,
  })

  const ref = await runtime.start(
    HelloWorldWorkflow,
    { name: "Ada", items: ["alpha", "bravo", "charlie", "delta", "echo"] },
    { workflowId: "hello-demo" },
  )
  await runtime.drain()
  console.log("after boot", await runtime.query(HelloWorldWorkflow, ref, "progress"))

  now = new Date(now.getTime() + 1_000)
  await runtime.drain()
  console.log("after durable reminder", await runtime.query(HelloWorldWorkflow, ref, "progress"))

  const restarted = new DurableRuntime(new JsonFileDurabilityProvider(storePath), {
    clock,
    workflows,
  })
  await restarted.signal(HelloWorldWorkflow, ref, "start_processing", { ok: true })
  await restarted.drain()
  console.log("after restart + processing", await restarted.query(HelloWorldWorkflow, ref, "progress"))

  now = new Date(now.getTime() + 1_000)
  await restarted.drain()
  console.log("completed", await restarted.query(HelloWorldWorkflow, ref, "progress"))
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runHelloWorldDemo()
}
