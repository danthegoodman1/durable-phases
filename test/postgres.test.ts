import { randomUUID } from "node:crypto"
import pg from "pg"
import { describe, expect, it } from "vitest"
import {
  DurableRuntime,
  PostgresDurabilityProvider,
  complete,
  defineWorkflow,
  phase,
  start,
} from "../src/durable.js"
import { workflowPartitionShard } from "../src/interface.js"
import { z } from "zod"

const { Pool } = pg
const connectionString = process.env.DURABLE_POSTGRES_URL
const describeIfPostgres = connectionString ? describe : describe.skip

function schemaName(): string {
  return `durable_test_${randomUUID().replaceAll("-", "_")}`
}

async function withProvider<T>(
  fn: (provider: PostgresDurabilityProvider, schema: string) => Promise<T>,
  options: { physicalPartitions?: number } = {},
): Promise<T> {
  if (!connectionString) {
    throw new Error("DURABLE_POSTGRES_URL is required")
  }
  const schema = schemaName()
  const provider = await PostgresDurabilityProvider.create({
    connectionString,
    schema,
    physicalPartitions: options.physicalPartitions,
    poolSize: 8,
  })
  try {
    return await fn(provider, schema)
  } finally {
    await provider.dropSchema().catch(() => undefined)
    await provider.close()
  }
}

async function scalar<T>(schema: string, sql: string, params: unknown[] = []): Promise<T> {
  if (!connectionString) {
    throw new Error("DURABLE_POSTGRES_URL is required")
  }
  const pool = new Pool({ connectionString, max: 1 })
  try {
    const result = await pool.query(sql.replaceAll("{schema}", `"${schema}"`), params)
    return Object.values(result.rows[0])[0] as T
  } finally {
    await pool.end()
  }
}

function partitionSuffix(workflowId: string, runId: string, partitionCount: number): string {
  const partition = workflowPartitionShard(workflowId, runId, partitionCount)
  const width = Math.max(2, String(partitionCount - 1).length)
  return `p${String(partition).padStart(width, "0")}`
}

function refForPhysicalPartition(
  prefix: string,
  partition: number,
  partitionCount: number,
): { workflowId: string; runId: string } {
  for (let index = 0; index < 10_000; index += 1) {
    const ref = { workflowId: `${prefix}-${index}`, runId: "run-1" }
    if (workflowPartitionShard(ref.workflowId, ref.runId, partitionCount) === partition) {
      return ref
    }
  }
  throw new Error(`Unable to find workflow id for physical partition ${partition}`)
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

  it("validates and persists physical partition metadata", async () => {
    await expect(
      PostgresDurabilityProvider.create({
        connectionString,
        schema: schemaName(),
        physicalPartitions: 0,
      }),
    ).rejects.toThrow("physicalPartitions must be a positive integer")
    await expect(
      PostgresDurabilityProvider.create({
        connectionString,
        schema: schemaName(),
        physicalPartitions: 1.5,
      }),
    ).rejects.toThrow("physicalPartitions must be a positive integer")

    await withProvider(async (_provider, schema) => {
      await expect(
        scalar<string>(
          schema,
          `SELECT value FROM {schema}.provider_metadata WHERE key = 'physical_partition_count'`,
        ),
      ).resolves.toBe("1")
    })

    await withProvider(
      async (_provider, schema) => {
        await expect(
          scalar<string>(
            schema,
            `SELECT value FROM {schema}.provider_metadata WHERE key = 'physical_partition_count'`,
          ),
        ).resolves.toBe("4")
        await expect(
          PostgresDurabilityProvider.create({
            connectionString,
            schema,
            physicalPartitions: 2,
          }),
        ).rejects.toThrow("physical partition count mismatch")
      },
      { physicalPartitions: 4 },
    )
  })

  it("stores execution snapshots on live activation tasks", async () => {
    await withProvider(async (provider, schema) => {
      await expect(
        scalar<number>(
          schema,
          `
          SELECT count(*)::int
          FROM information_schema.columns
          WHERE table_schema = $1
            AND table_name = 'activation_tasks_p00'
            AND column_name LIKE 'instance_%'
          `,
          [schema],
        ),
      ).resolves.toBeGreaterThan(0)

      await provider.createInstance({
        workflowName: "pg_snapshot_task",
        workflowVersion: 1,
        workflowId: "pg-snapshot-task",
        runId: "run-1",
        partitionShard: 0,
        common: { value: 42 },
        phase: { name: "run", data: { step: "ready" } },
        waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
        now: "2026-01-01T00:00:00.000Z",
      })
      await expect(
        scalar<string>(
          schema,
          `
          SELECT instance_common_json->>'value'
          FROM {schema}.activation_tasks_p00
          WHERE workflow_id = $1 AND run_id = $2
          `,
          ["pg-snapshot-task", "run-1"],
        ),
      ).resolves.toBe("42")
    })
  })

  it("routes hot workflow data to deterministic physical tables", async () => {
    await withProvider(
      async (provider, schema) => {
        const parent = { workflowId: "route-parent", runId: "run-parent" }
        const child = { workflowId: "route-child", runId: "run-child" }
        await provider.createInstance({
          workflowName: "pg_route_parent",
          workflowVersion: 1,
          workflowId: parent.workflowId,
          runId: parent.runId,
          partitionShard: 0,
          common: {},
          phase: { name: "run", data: {} },
          waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
          now: "2026-01-01T00:00:00.000Z",
        })
        await provider.appendSignal({
          ...parent,
          type: "finish",
          payload: { ok: true },
          receivedAt: "2026-01-01T00:00:00.000Z",
        })
        await provider.claimDispatchShard({
          shardId: 0,
          ownerId: "route-worker",
          now: "2026-01-01T00:00:00.000Z",
          leaseMs: 60_000,
        })
        const claim = (
          await provider.claimReadyActivations({
            workerId: "route-worker",
            shardIds: [0],
            workflows: { pg_route_parent: { version: 1 } },
            now: "2026-01-01T00:00:00.000Z",
            leaseMs: 60_000,
            limit: 1,
          })
        ).claims[0]
        expect(claim).toBeDefined()
        const reservation = await provider.getOrReserveEffect({
          ...parent,
          activationId: claim.activation.activationId,
          workerId: "route-worker",
          key: "slow",
          now: "2026-01-01T00:00:00.000Z",
          options: {
            startToCloseTimeoutMs: 60_000,
            heartbeatTimeoutMs: 30_000,
          },
        })
        expect(reservation.status).toBe("reserved")
        await provider.createChildInstance({
          workflowName: "pg_route_child",
          workflowVersion: 1,
          workflowId: child.workflowId,
          runId: child.runId,
          partitionShard: 0,
          common: {},
          phase: { name: "run", data: {} },
          waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
          now: "2026-01-01T00:00:00.000Z",
          parentWorkflowId: parent.workflowId,
          parentRunId: parent.runId,
          activationId: claim.activation.activationId,
          workerId: "route-worker",
          leaseNow: "2026-01-01T00:00:00.000Z",
          key: "child",
        })

        const parentSuffix = partitionSuffix(parent.workflowId, parent.runId, 4)
        const childSuffix = partitionSuffix(child.workflowId, child.runId, 4)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.instances_${parentSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBe(1)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.signals_${parentSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBe(1)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.effects_${parentSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBe(1)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.activity_deadlines_${parentSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBe(1)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.activation_tasks_${parentSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBeGreaterThan(0)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.children_${parentSuffix} WHERE parent_workflow_id = $1 AND parent_run_id = $2`,
            [parent.workflowId, parent.runId],
          ),
        ).resolves.toBe(1)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.instances_${childSuffix} WHERE workflow_id = $1 AND run_id = $2`,
            [child.workflowId, child.runId],
          ),
        ).resolves.toBe(1)
      },
      { physicalPartitions: 4 },
    )
  })

  it("claims ready activations in canonical order across physical partitions", async () => {
    await withProvider(
      async (provider) => {
        const late = refForPhysicalPartition("pg-order-late", 0, 4)
        const early = refForPhysicalPartition("pg-order-early", 1, 4)
        for (const [ref, readyAt] of [
          [late, "2026-01-01T00:00:10.000Z"],
          [early, "2026-01-01T00:00:00.000Z"],
        ] as const) {
          await provider.createInstance({
            workflowName: "pg_order",
            workflowVersion: 1,
            workflowId: ref.workflowId,
            runId: ref.runId,
            partitionShard: 0,
            common: {},
            phase: { name: "run", data: {} },
            waits: [{ kind: "run", name: "__run", readyAt }],
            now: "2026-01-01T00:00:00.000Z",
          })
        }
        await provider.claimDispatchShard({
          shardId: 0,
          ownerId: "order-worker",
          now: "2026-01-01T00:00:11.000Z",
          leaseMs: 60_000,
        })
        const first = await provider.claimReadyActivations({
          workerId: "order-worker",
          shardIds: [0],
          shardCount: 1,
          workflows: { pg_order: { version: 1 } },
          now: "2026-01-01T00:00:11.000Z",
          leaseMs: 60_000,
          limit: 1,
        })
        expect(first.claims).toHaveLength(1)
        expect(first.claims[0].activation.workflowId).toBe(early.workflowId)

        const second = await provider.claimReadyActivations({
          workerId: "order-worker",
          shardIds: [0],
          shardCount: 1,
          workflows: { pg_order: { version: 1 } },
          now: "2026-01-01T00:00:11.000Z",
          leaseMs: 60_000,
          limit: 1,
        })
        expect(second.claims).toHaveLength(1)
        expect(second.claims[0].activation.workflowId).toBe(late.workflowId)
      },
      { physicalPartitions: 4 },
    )
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

  it("reclaims shard-scoped tasks by shard epoch and fences stale commits", async () => {
    await withProvider(async (providerA, schema) => {
      const providerB = await PostgresDurabilityProvider.create({
        connectionString,
        schema,
        poolSize: 4,
      })
      try {
        await providerA.createInstance({
          workflowName: "pg_epoch",
          workflowVersion: 1,
          workflowId: "pg-epoch",
          runId: "run-1",
          partitionShard: 0,
          common: {},
          phase: { name: "run", data: {} },
          waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
          now: "2026-01-01T00:00:00.000Z",
        })
        await expect(
          providerA.claimDispatchShard({
            shardId: 0,
            ownerId: "worker-a",
            now: "2026-01-01T00:00:00.000Z",
            leaseMs: 100,
          }),
        ).resolves.toMatchObject({ leaseEpoch: 1 })
        const first = await providerA.claimReadyActivations({
          workerId: "worker-a",
          shardIds: [0],
          workflows: { pg_epoch: { version: 1 } },
          now: "2026-01-01T00:00:00.000Z",
          leaseMs: 60_000,
          limit: 1,
        })
        expect(first.claims).toHaveLength(1)
        expect(first.claims[0].lease).toEqual({ scope: "shard", shardId: 0, epoch: 1 })

        await expect(
          providerB.claimDispatchShard({
            shardId: 0,
            ownerId: "worker-b",
            now: "2026-01-01T00:00:01.000Z",
            leaseMs: 60_000,
          }),
        ).resolves.toMatchObject({ leaseEpoch: 2 })
        const reclaimed = await providerB.claimReadyActivations({
          workerId: "worker-b",
          shardIds: [0],
          workflows: { pg_epoch: { version: 1 } },
          now: "2026-01-01T00:00:01.000Z",
          leaseMs: 60_000,
          limit: 1,
        })
        expect(reclaimed.claims).toHaveLength(1)
        expect(reclaimed.claims[0].activation.activationId).toBe(first.claims[0].activation.activationId)
        expect(reclaimed.claims[0].lease).toEqual({ scope: "shard", shardId: 0, epoch: 2 })

        await expect(
          providerA.commitCheckpoint({
            workflowId: "pg-epoch",
            runId: "run-1",
            expectedSequence: 0,
            activationId: first.claims[0].activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { stale: true } },
            waits: [],
            now: "2026-01-01T00:00:01.000Z",
          }),
        ).resolves.toMatchObject({ ok: false, reason: "lost_activation_lease" })
      } finally {
        await providerB.close()
      }
    })
  })

  it("does not heartbeat shard-scoped activations from the runtime", async () => {
    await withProvider(async (provider) => {
      let activationHeartbeats = 0
      const originalHeartbeatActivations = provider.heartbeatActivations.bind(provider)
      provider.heartbeatActivations = async (input) => {
        activationHeartbeats += 1
        await originalHeartbeatActivations(input)
      }
      const Workflow = defineWorkflow({
        name: "pg_shard_scoped_runtime",
        version: 1,
        input: z.object({}),
        output: z.object({ ok: z.boolean() }),
        common: z.object({}),
        initial() {
          return start({ common: {}, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: async () => {
              await new Promise((resolve) => setTimeout(resolve, 25))
              return complete({ ok: true })
            },
          }),
        },
      })
      const runtime = new DurableRuntime(provider, {
        workflows: [Workflow],
        workerId: "pg-shard-scoped-runtime",
        leaseHeartbeatIntervalMs: 1,
        activationLeaseMs: 5,
        dispatchLeaseMs: 1_000,
      })
      const ref = await runtime.start(Workflow, {}, { workflowId: "pg-shard-scoped-runtime" })
      await expect(runtime.drain()).resolves.toMatchObject({ activations: 1 })
      await expect(provider.loadInstance(ref)).resolves.toMatchObject({ status: "completed" })
      expect(activationHeartbeats).toBe(0)
    })
  })

  it("heartbeats shard-scoped activations once eager activity fencing is needed", async () => {
    await withProvider(async (provider) => {
      let activationHeartbeats = 0
      const originalHeartbeatActivations = provider.heartbeatActivations.bind(provider)
      provider.heartbeatActivations = async (input) => {
        activationHeartbeats += 1
        await originalHeartbeatActivations(input)
      }
      const Workflow = defineWorkflow({
        name: "pg_shard_scoped_eager_activity",
        version: 1,
        input: z.object({}),
        output: z.object({ ok: z.boolean() }),
        common: z.object({}),
        initial() {
          return start({ common: {}, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: async ({ ctx }) => {
              await ctx.activity(
                "eager",
                async () => {
                  await new Promise((resolve) => setTimeout(resolve, 25))
                  return true
                },
                { startToCloseTimeoutMs: 1_000, retry: { maxAttempts: 1 } },
              )
              return complete({ ok: true })
            },
          }),
        },
      })
      const runtime = new DurableRuntime(provider, {
        workflows: [Workflow],
        workerId: "pg-shard-scoped-eager-runtime",
        leaseHeartbeatIntervalMs: 1,
        activationLeaseMs: 5,
        dispatchLeaseMs: 1_000,
      })
      const ref = await runtime.start(Workflow, {}, { workflowId: "pg-shard-scoped-eager" })
      await expect(runtime.drain()).resolves.toMatchObject({ activations: 1 })
      await expect(provider.loadInstance(ref)).resolves.toMatchObject({ status: "completed" })
      expect(activationHeartbeats).toBeGreaterThan(0)
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

  it("compacts completed activation tasks and checkpoint-local effects", async () => {
    await withProvider(async (provider, schema) => {
      const Workflow = defineWorkflow({
        name: "pg_runtime_compact",
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
        workerId: "pg-compact-worker",
      })
      const ref = await runtime.start(Workflow, { value: 41 }, { workflowId: "pg-compact" })
      await expect(runtime.drain()).resolves.toMatchObject({ activations: 1 })
      const suffix = partitionSuffix(ref.workflowId, ref.runId, 1)
      await expect(
        scalar<number>(
          schema,
          `SELECT count(*)::int FROM {schema}.activation_tasks_${suffix} WHERE workflow_id = $1 AND run_id = $2`,
          [ref.workflowId, ref.runId],
        ),
      ).resolves.toBe(0)
      await expect(
        scalar<number>(
          schema,
          `SELECT count(*)::int FROM {schema}.effects_${suffix} WHERE workflow_id = $1 AND run_id = $2`,
          [ref.workflowId, ref.runId],
        ),
      ).resolves.toBe(0)
      await expect(provider.loadInstance(ref, { includeEffects: true })).resolves.toMatchObject({
        status: "completed",
        output: { value: 42 },
        effects: [],
      })
    })
  })
})
