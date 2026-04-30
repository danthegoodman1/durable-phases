import { randomUUID } from "node:crypto"
import { describe, expect, it } from "vitest"
import {
  DurableRuntime,
  PostgresDurabilityProvider,
  complete,
  defineWorkflow,
  phase,
  start,
} from "../src/durable.js"
import { z } from "zod"

const connectionString = process.env.DURABLE_POSTGRES_URL
const describeIfPostgres = connectionString ? describe : describe.skip

function schemaName(): string {
  return `durable_test_${randomUUID().replaceAll("-", "_")}`
}

async function withProvider<T>(
  fn: (provider: PostgresDurabilityProvider, schema: string) => Promise<T>,
): Promise<T> {
  if (!connectionString) {
    throw new Error("DURABLE_POSTGRES_URL is required")
  }
  const schema = schemaName()
  const provider = await PostgresDurabilityProvider.create({ connectionString, schema, poolSize: 8 })
  try {
    return await fn(provider, schema)
  } finally {
    await provider.dropSchema().catch(() => undefined)
    await provider.close()
  }
}

describeIfPostgres("PostgresDurabilityProvider", () => {
  it("validates schema names", async () => {
    await expect(
      PostgresDurabilityProvider.create({
        connectionString,
        schema: "bad-name",
      }),
    ).rejects.toThrow("schema must be a valid identifier")
  })

  it("supports multiple provider instances over one schema", async () => {
    await withProvider(async (providerA, schema) => {
      const providerB = await PostgresDurabilityProvider.create({
        connectionString,
        schema,
        poolSize: 4,
      })
      try {
        await providerA.createInstance({
          workflowName: "pg_multi",
          workflowVersion: 1,
          workflowId: "pg-multi",
          runId: "run-1",
          partitionShard: 0,
          common: {},
          phase: { name: "run", data: {} },
          waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
          now: "2026-01-01T00:00:00.000Z",
        })
        await providerB.claimDispatchShard({
          shardId: 0,
          ownerId: "worker-b",
          now: "2026-01-01T00:00:00.000Z",
          leaseMs: 60_000,
        })
        const batch = await providerB.claimReadyActivations({
          workerId: "worker-b",
          shardIds: [0],
          workflows: { pg_multi: { version: 1 } },
          now: "2026-01-01T00:00:00.000Z",
          leaseMs: 60_000,
          limit: 4,
        })
        expect(batch.claims).toHaveLength(1)
        expect(batch.claims[0].instance).toMatchObject({
          workflowId: "pg-multi",
          sequence: 0,
          status: "running",
        })
      } finally {
        await providerB.close()
      }
    })
  })

  it("serializes concurrent schema initialization for shared stores", async () => {
    if (!connectionString) {
      throw new Error("DURABLE_POSTGRES_URL is required")
    }
    const schema = schemaName()
    const providers = await Promise.all(
      Array.from({ length: 4 }, () =>
        PostgresDurabilityProvider.create({ connectionString, schema, poolSize: 2 }),
      ),
    )
    try {
      await providers[0].createInstance({
        workflowName: "pg_schema_init",
        workflowVersion: 1,
        workflowId: "pg-schema-init",
        runId: "run-1",
        partitionShard: 0,
        common: {},
        phase: { name: "run", data: {} },
        waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
        now: "2026-01-01T00:00:00.000Z",
      })
      await expect(providers[3].loadInstance({ workflowId: "pg-schema-init", runId: "run-1" }))
        .resolves.toMatchObject({
          workflowId: "pg-schema-init",
          status: "running",
        })
    } finally {
      await providers[0]?.dropSchema().catch(() => undefined)
      await Promise.all(providers.map((provider) => provider.close()))
    }
  })

  it("swallows throwing observability sinks", async () => {
    await withProvider(async (_provider, schema) => {
      const throwing = await PostgresDurabilityProvider.create({
        connectionString,
        schema,
        logger: {
          debug() {
            throw new Error("logger failed")
          },
          info() {
            throw new Error("logger failed")
          },
          warn() {
            throw new Error("logger failed")
          },
          error() {
            throw new Error("logger failed")
          },
        },
        metrics: {
          counter() {
            throw new Error("metrics failed")
          },
          histogram() {
            throw new Error("metrics failed")
          },
          gauge() {
            throw new Error("metrics failed")
          },
        },
      })
      try {
        await expect(
          throwing.createInstance({
            workflowName: "pg_obs",
            workflowVersion: 1,
            workflowId: "pg-obs",
            runId: "run-1",
            partitionShard: 0,
            common: {},
            phase: { name: "run", data: {} },
            waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
            now: "2026-01-01T00:00:00.000Z",
          }),
        ).resolves.toEqual({ workflowId: "pg-obs", runId: "run-1" })
      } finally {
        await throwing.close()
      }
    })
  })

  it("runs through DurableRuntime", async () => {
    await withProvider(async (provider) => {
      const Workflow = defineWorkflow({
        name: "pg_runtime_smoke",
        version: 1,
        input: z.object({ value: z.number() }),
        output: z.object({ value: z.number() }),
        common: z.object({ value: z.number() }),
        initial(input) {
          return start({ common: { value: input.value }, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: async ({ ctx, common }) => {
              const value = await ctx.activity("value", () => common.value + 1)
              return complete({ value })
            },
          }),
        },
      })
      const runtime = new DurableRuntime(provider, {
        workflows: [Workflow],
        workerId: "pg-runtime-worker",
      })
      const ref = await runtime.start(Workflow, { value: 41 }, { workflowId: "pg-runtime" })
      await expect(runtime.drain()).resolves.toMatchObject({ activations: 1 })
      await expect(provider.loadInstance(ref)).resolves.toMatchObject({
        status: "completed",
        output: { value: 42 },
      })
    })
  })
})
