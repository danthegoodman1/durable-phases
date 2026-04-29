/*
 * Shows checkpoint-boundary migration:
 * a v1 worker creates a running instance, then a v2 worker loads the same
 * durable store, applies migration 1 -> 2, recomputes waits for the migrated
 * phase, and then handles a signal using the v2 workflow code.
 */

import { z } from "zod"
import {
  complete,
  defineWorkflow,
  DurableRuntime,
  phase,
  signal,
  SqliteDurabilityProvider,
  start,
} from "../durable.js"
import { cleanupDemoStore, committed, demoRuntime, demoStorePath } from "./_shared.js"

const MigratingOrderV1 = defineWorkflow({
  name: "demo_migrating_order",
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
        finish: signal(z.object({ punctuation: z.string() }), async ({ common, data, event }) => {
          return complete({
            message: `${data.salutation}, ${common.customerId}${event.punctuation}`,
          })
        }),
      },
    }),
  },
})

const MigratingOrderV2 = defineWorkflow({
  name: "demo_migrating_order",
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
        finish: signal(z.object({ punctuation: z.string() }), async ({ common, data, event }) => {
          return complete({
            message: `${data.greeting}, ${common.customerId} on ${common.plan}${event.punctuation}`,
          })
        }),
      },
    }),
  },
})

export async function runMigrationDemo(): Promise<void> {
  const demoName = "migration"
  const first = await demoRuntime(demoName, [MigratingOrderV1])
  let upgradedProvider: SqliteDurabilityProvider | undefined

  try {
    const ref = await first.runtime.start(
      MigratingOrderV1,
      { customerId: "Ada" },
      { workflowId: "migration-demo" },
    )
    console.log("migration: v1 persisted", await committed(first.provider, ref))

    first.provider.close()
    upgradedProvider = new SqliteDurabilityProvider(demoStorePath(demoName))
    const upgradedRuntime = new DurableRuntime(upgradedProvider, {
      clock: first.clock,
      workflows: [MigratingOrderV2],
      workerId: first.workerId,
    })

    await upgradedRuntime.drain({ maxActivations: 1 })
    console.log("migration: after v2 migration checkpoint", await committed(upgradedProvider, ref))

    await upgradedRuntime.signal(MigratingOrderV2, ref, "finish", { punctuation: "!" })
    await upgradedRuntime.drain()
    console.log("migration: completed on v2", await committed(upgradedProvider, ref))
  } finally {
    first.provider.close()
    upgradedProvider?.close()
    await cleanupDemoStore(demoName)
  }
}
