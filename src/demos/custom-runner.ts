/*
 * Shows a custom runner driving the public shard-step API. Each loop owns one
 * shard at a time, runs one bounded activation, then yields back to the caller.
 */

import { z } from "zod"
import {
  complete,
  defineWorkflow,
  DurableRuntime,
  type InstanceRef,
  type PersistedInstance,
  phase,
  SqliteDurabilityProvider,
  start,
  stay,
  workflowPartitionShard,
} from "../durable.js"
import { cleanupDemoStore, committed, demoStorePath } from "./_shared.js"

const shardCount = 3

const CustomRunnerWorkflow = defineWorkflow({
  name: "demo_custom_runner",
  version: 1,
  input: z.object({ items: z.array(z.string()) }),
  output: z.object({ processed: z.array(z.string()) }),

  initial(input) {
    return start({
      phase: "process",
      data: {
        cursor: 0,
        items: input.items,
        processed: [],
      },
    })
  },

  phases: {
    process: phase({
      state: z.object({
        cursor: z.number(),
        items: z.array(z.string()),
        processed: z.array(z.string()),
      }),
      run: async ({ data }) => {
        const item = data.items[data.cursor]
        if (item === undefined) {
          return complete({ processed: data.processed })
        }

        return stay({
          cursor: data.cursor + 1,
          processed: [...data.processed, item.toUpperCase()],
        })
      },
    }),
  },
})

export async function runCustomRunnerDemo(): Promise<void> {
  const demoName = "custom-runner"
  const storePath = demoStorePath(demoName)
  await cleanupDemoStore(demoName)

  // In a hosted runtime, the platform clock would normally be real time. The
  // demo keeps it manual so the runner can advance deterministically while it
  // polls for completion.
  let now = new Date("2026-01-01T00:00:00.000Z")
  const provider = new SqliteDurabilityProvider(storePath)
  const runtime = new DurableRuntime(provider, {
    clock: () => now,
    workflows: [CustomRunnerWorkflow],
    workerId: "custom-runner",
    shardCount,
  })

  // This AbortController stands in for whatever lifecycle signal a host gives
  // a custom runtime: cancellation, action duration, or simply a choice to run
  // one bounded step and return.
  const controller = new AbortController()

  // Start one tiny loop per shard. Each loop repeatedly "kicks" its shard by
  // calling runShardStep; the durable shard lease decides who may actually work.
  const shardLoops = Array.from({ length: shardCount }, (_value, shardId) =>
    runShardLoop(runtime, shardId, controller.signal),
  )

  try {
    const refs: Array<{ shardId: number; ref: InstanceRef }> = []
    for (let shardId = 0; shardId < shardCount; shardId += 1) {
      // Demo-only placement hack: normally workflow IDs come from business
      // identity, then the runtime hashes the ref to decide which shard to kick.
      // Here we choose IDs by shard only so the output shows one workflow per
      // runner loop.
      const workflowId = workflowIdForShard(shardId)
      const ref = await runtime.start(
        CustomRunnerWorkflow,
        { items: [`item-${shardId}-a`, `item-${shardId}-b`, `item-${shardId}-c`] },
        { workflowId },
      )
      refs.push({ shardId, ref })
    }

    const mapping = refs.map(({ shardId, ref }) => ({
      shardId,
      workflowId: ref.workflowId,
      runId: ref.runId,
    }))
    console.log("custom runner: shard mapping", JSON.stringify(mapping, null, 2))

    // The runner loops are already active above, so waiting here is just the
    // demo's observer. A real host would usually persist/emit results elsewhere.
    await waitForCompleted(provider, refs.map((item) => item.ref))

    // Stop the loops after all work is done. This mirrors a hosted runner
    // returning after it has no more immediate work, rather than running forever.
    controller.abort()
    const activationsByShard = await Promise.all(shardLoops)
    const outputs = await Promise.all(
      refs.map(async ({ shardId, ref }) => ({
        shardId,
        workflowId: ref.workflowId,
        result: await committed(provider, ref),
      })),
    )
    console.log("custom runner: shard activations", activationsByShard)
    console.log("custom runner: completed", JSON.stringify(outputs, null, 2))
  } finally {
    // Cleanup is intentionally defensive: abort and await loop settlement even
    // if the observer or a workflow handler fails.
    controller.abort()
    await Promise.allSettled(shardLoops)
    provider.close()
    await cleanupDemoStore(demoName)
  }

  function advance(ms: number): void {
    now = new Date(now.getTime() + ms)
  }

  async function waitForCompleted(
    completedProvider: SqliteDurabilityProvider,
    refs: InstanceRef[],
  ): Promise<PersistedInstance[]> {
    const deadline = Date.now() + 5_000
    while (Date.now() <= deadline) {
      const instances = await Promise.all(refs.map((ref) => completedProvider.loadInstance(ref)))
      if (instances.every((instance) => instance?.status === "completed")) {
        return instances as PersistedInstance[]
      }
      advance(1)
      await sleep(5)
    }
    throw new Error("Timed out waiting for custom runner demo workflows")
  }
}

async function runShardLoop(
  runtime: DurableRuntime,
  shardId: number,
  signal: AbortSignal,
): Promise<number> {
  let activations = 0
  while (!signal.aborted) {
    try {
      // This is the public custom-runner primitive. It claims one shard, runs at
      // most one activation, commits through the provider, then releases.
      const result = await runtime.runShardStep({
        shardId,
        maxActivations: 1,
        maxConcurrentActivations: 1,
        activationPrefetchLimit: 1,
        signal,
      })
      activations += result.activations
      if (!result.claimedShard || result.activations === 0) {
        // No lease or no ready work. A serverless adapter could return here and
        // rely on nextWakeAt/watchdogs; this local demo just idles briefly.
        await sleep(10, signal)
      }
    } catch (error) {
      if (signal.aborted) {
        break
      }
      throw error
    }
  }
  return activations
}

function workflowIdForShard(shardId: number): string {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const workflowId = `custom-runner-${shardId}-${attempt}`
    if (workflowPartitionShard(workflowId, "run-1", shardCount) === shardId) {
      return workflowId
    }
  }
  throw new Error(`Could not find workflow id for shard ${shardId}`)
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve()
      return
    }
    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener("abort", done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener("abort", done, { once: true })
  })
}
