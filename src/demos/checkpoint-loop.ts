/*
 * Shows the bounded unbound-loop pattern with `checkpoint()`:
 * each immediate activation processes a small chunk, checkpoints progress into
 * phase data, and the runtime re-enters the same run phase until complete.
 */

import { z } from "zod"
import {
  checkpoint,
  complete,
  defineWorkflow,
  phase,
  start,
} from "../durable.js"
import { cleanupDemoStore, committed, demoRuntime } from "./_shared.js"

const BatchWorkflow = defineWorkflow({
  name: "demo_checkpoint_loop",
  version: 1,
  input: z.object({ items: z.array(z.string()) }),
  output: z.object({ processed: z.array(z.string()) }),

  initial(input) {
    return start({
      phase: "process_batch",
      data: {
        cursor: 0,
        items: input.items,
        processed: [],
      },
    })
  },

  phases: {
    process_batch: phase({
      state: z.object({
        cursor: z.number(),
        items: z.array(z.string()),
        processed: z.array(z.string()),
      }),
      run: async ({ ctx, data }) => {
        const chunk = data.items.slice(data.cursor, data.cursor + 2)
        if (chunk.length === 0) {
          return complete({
            processed: data.processed,
          })
        }

        const processedChunk = await ctx.activity(`process_${data.cursor}`, () =>
          chunk.map((item: string) => item.toUpperCase()),
        )

        return checkpoint({
          cursor: data.cursor + chunk.length,
          processed: [...data.processed, ...processedChunk],
        })
      },
    }),
  },
})

export async function runCheckpointLoopDemo(): Promise<void> {
  const demoName = "checkpoint-loop"
  const { runtime, provider } = await demoRuntime(demoName, [BatchWorkflow])

  try {
    const ref = await runtime.start(
      BatchWorkflow,
      { items: ["alpha", "bravo", "charlie", "delta", "echo"] },
      { workflowId: "loop-demo" },
    )
    await runtime.drain()
    console.log("checkpoint loop: completed", await committed(provider, ref))
  } finally {
    provider.close()
    await cleanupDemoStore(demoName)
  }
}
