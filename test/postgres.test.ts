import { randomUUID } from "node:crypto"
import pg from "pg"
import { describe, expect, it } from "vitest"
import {
  DurableRuntime,
  PostgresDurabilityProvider,
  child,
  complete,
  defineWorkflow,
  go,
  phase,
  start,
  workflowPartitionShard,
} from "../src/durable.js"
import { z } from "zod"

const { Pool } = pg
const connectionString = process.env.DURABLE_POSTGRES_URL
const describeIfPostgres = connectionString ? describe : describe.skip

function schemaName(): string {
  return `durable_test_${randomUUID().replaceAll("-", "_")}`
}

async function withProvider<T>(
  fn: (provider: PostgresDurabilityProvider, schema: string) => Promise<T>,
  options: { physicalPartitions?: number; snapshotInterval?: number } = {},
): Promise<T> {
  if (!connectionString) {
    throw new Error("DURABLE_POSTGRES_URL is required")
  }
  const schema = schemaName()
  const provider = await PostgresDurabilityProvider.create({
    connectionString,
    schema,
    physicalPartitions: options.physicalPartitions,
    snapshotInterval: options.snapshotInterval,
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

describeIfPostgres("PostgresDurabilityProvider append store", () => {
  it("validates schema names and persists append-store metadata", async () => {
    await expect(
      PostgresDurabilityProvider.create({
        connectionString,
        schema: "bad-name",
      }),
    ).rejects.toThrow("schema must be a valid identifier")

    await expect(
      PostgresDurabilityProvider.create({
        connectionString,
        schema: schemaName(),
        physicalPartitions: 0,
      }),
    ).rejects.toThrow("physicalPartitions must be a positive integer")

    await withProvider(
      async (_provider, schema) => {
        await expect(
          scalar<string>(
            schema,
            `SELECT value FROM {schema}.provider_metadata WHERE key = 'postgres_storage_shape'`,
          ),
        ).resolves.toBe("append_store_v1")
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
        ).rejects.toThrow("physical_partition_count mismatch")
      },
      { physicalPartitions: 4 },
    )
  })

  it("creates shard journal tables instead of SQL scheduler hot tables", async () => {
    await withProvider(async (_provider, schema) => {
      await expect(
        scalar<number>(
          schema,
          `
          SELECT count(*)::int
          FROM information_schema.tables
          WHERE table_schema = $1
            AND table_name IN (
              'provider_metadata',
              'dispatch_shards',
              'shard_heads_p00',
              'shard_journal_p00',
              'shard_snapshots_p00'
            )
          `,
          [schema],
        ),
      ).resolves.toBe(5)
      await expect(
        scalar<number>(
          schema,
          `
          SELECT count(*)::int
          FROM information_schema.tables
          WHERE table_schema = $1
            AND table_name IN (
              'activation_tasks',
              'activation_tasks_p00',
              'workflow_state',
              'workflow_state_p00',
              'effects_p00',
              'activity_deadlines_p00',
              'inbox_p00',
              'outbox_p00'
            )
          `,
          [schema],
        ),
      ).resolves.toBe(0)
    })
  })

  it("recovers workflow state from shard snapshots and journal tails", async () => {
    await withProvider(
      async (provider, schema) => {
        await provider.createInstance({
          workflowName: "pg_recover",
          workflowVersion: 1,
          workflowId: "pg-recover",
          runId: "run-1",
          partitionShard: 0,
          common: { value: 42 },
          phase: { name: "run", data: { step: "ready" } },
          waits: [{ kind: "run", name: "__run", readyAt: "2026-01-01T00:00:00.000Z" }],
          now: "2026-01-01T00:00:00.000Z",
        })
        await provider.appendSignal({
          workflowId: "pg-recover",
          runId: "run-1",
          type: "finish",
          payload: { ok: true },
          receivedAt: "2026-01-01T00:00:01.000Z",
        })
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.shard_journal_p00 WHERE shard_id = 0`,
          ),
        ).resolves.toBe(2)
        await expect(
          scalar<number>(
            schema,
            `SELECT count(*)::int FROM {schema}.shard_snapshots_p00 WHERE shard_id = 0`,
          ),
        ).resolves.toBeGreaterThan(0)

        const restarted = await PostgresDurabilityProvider.create({
          connectionString,
          schema,
          snapshotInterval: 1,
        })
        try {
          await expect(restarted.loadInstance({ workflowId: "pg-recover", runId: "run-1" }))
            .resolves.toMatchObject({
              workflowName: "pg_recover",
              common: { value: 42 },
            })
          await expect(restarted.listSignals()).resolves.toHaveLength(1)
        } finally {
          await restarted.close()
        }
      },
      { snapshotInterval: 1 },
    )
  })

  it("does not append extra journal entries for duplicate idempotent signals", async () => {
    await withProvider(async (provider, schema) => {
      await provider.createInstance({
        workflowName: "pg_idempotent_signal",
        workflowVersion: 1,
        workflowId: "pg-idempotent-signal",
        runId: "run-1",
        partitionShard: 0,
        common: {},
        phase: { name: "waiting", data: {} },
        waits: [{ kind: "signal", name: "finish", type: "finish", scope: "phase" }],
        now: "2026-01-01T00:00:00.000Z",
      })

      const first = await provider.appendSignal({
        workflowId: "pg-idempotent-signal",
        runId: "run-1",
        type: "finish",
        payload: { sender: "first" },
        receivedAt: "2026-01-01T00:00:01.000Z",
        idempotencyKey: "request-1",
      })
      await expect(
        provider.appendSignal({
          workflowId: "pg-idempotent-signal",
          runId: "run-1",
          type: "finish",
          payload: { sender: "duplicate" },
          receivedAt: "2026-01-01T00:00:02.000Z",
          idempotencyKey: "request-1",
        }),
      ).resolves.toEqual(first)

      const secondProvider = await PostgresDurabilityProvider.create({
        connectionString,
        schema,
      })
      try {
        await expect(
          secondProvider.appendSignal({
            workflowId: "pg-idempotent-signal",
            runId: "run-1",
            type: "finish",
            payload: { sender: "second-provider" },
            receivedAt: "2026-01-01T00:00:03.000Z",
            idempotencyKey: "request-1",
          }),
        ).resolves.toEqual(first)
      } finally {
        await secondProvider.close()
      }

      await expect(
        scalar<number>(
          schema,
          `SELECT count(*)::int FROM {schema}.shard_journal_p00 WHERE shard_id = 0`,
        ),
      ).resolves.toBe(2)
    })
  })

  it("uses shard epochs to fence stale commits after shard takeover", async () => {
    await withProvider(async (provider) => {
      await provider.createInstance({
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
      const leaseA = await provider.claimShard({
        shardId: 0,
        ownerId: "worker-a",
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 1_000,
      })
      expect(leaseA?.leaseEpoch).toBe(1)
      const sessionA = provider.openShard(leaseA!)
      const claimA = (await sessionA.claimTasks({
        workflows: { pg_epoch: { version: 1 } },
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 30_000,
        limit: 1,
      })).claims[0]
      expect(claimA).toBeDefined()

      const leaseB = await provider.claimShard({
        shardId: 0,
        ownerId: "worker-b",
        now: "2026-01-01T00:00:02.000Z",
        leaseMs: 30_000,
      })
      expect(leaseB?.leaseEpoch).toBe(2)
      const stale = await sessionA.commitActivations([{
        workflowId: "pg-epoch",
        runId: "run-1",
        expectedSequence: 0,
        activationId: claimA!.activation.activationId,
        workerId: "worker-a",
        workflowVersion: 1,
        next: { status: "completed", output: { stale: true } },
        waits: [],
        now: "2026-01-01T00:00:02.001Z",
      }])
      expect(stale.results[0]).toMatchObject({
        ok: false,
        reason: "lost_shard_task_lease",
      })

      const sessionB = provider.openShard(leaseB!)
      const claimB = (await sessionB.claimTasks({
        workflows: { pg_epoch: { version: 1 } },
        now: "2026-01-01T00:00:02.002Z",
        leaseMs: 30_000,
        limit: 1,
      })).claims[0]
      expect(claimB?.activation.activationId).toBe(claimA?.activation.activationId)
      const committed = await sessionB.commitActivations([{
        workflowId: "pg-epoch",
        runId: "run-1",
        expectedSequence: 0,
        activationId: claimB!.activation.activationId,
        workerId: "worker-b",
        workflowVersion: 1,
        next: { status: "completed", output: { stale: false } },
        waits: [],
        now: "2026-01-01T00:00:02.003Z",
      }])
      expect(committed.results[0]).toMatchObject({ ok: true, sequence: 1 })
      await expect(provider.loadInstance({ workflowId: "pg-epoch", runId: "run-1" }))
        .resolves.toMatchObject({
          status: "completed",
          output: { stale: false },
        })
    })
  })

  it("keeps default child workflow ids shard-affine", async () => {
    await withProvider(async (provider) => {
      const ChildWorkflow = defineWorkflow({
        name: "pg_affine_child",
        version: 1,
        input: z.object({ value: z.number() }),
        output: z.object({ value: z.number() }),
        common: z.object({ value: z.number() }),
        initial(input) {
          return start({ common: input, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: ({ common }) => complete({ value: common.value + 1 }),
          }),
        },
      })
      const ParentWorkflow = defineWorkflow({
        name: "pg_affine_parent",
        version: 1,
        input: z.object({ value: z.number() }),
        output: z.object({ value: z.number() }),
        common: z.object({ value: z.number() }),
        initial(input) {
          return start({ common: input, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: async ({ ctx, common }) => {
              const handle = await ctx.child.start("child", ChildWorkflow, { value: common.value })
              return go("waiting", { handle })
            },
          }),
          waiting: phase({
            state: z.object({ handle: z.any() }),
            on: {
              done: child(
                ({ data }) => data.handle,
                ({ event }) => complete({ value: event.ok ? event.output.value : -1 }),
              ),
            },
          }),
        },
      })
      const runtime = new DurableRuntime(provider, {
        workflows: [ParentWorkflow, ChildWorkflow],
        workerId: "pg-affine-worker",
        shardCount: 4,
      })
      const ref = await runtime.start(ParentWorkflow, { value: 9 }, { workflowId: "pg-affine-parent" })
      await runtime.drain({ maxActivations: 10 })
      const instances = await provider.listInstances()
      const parent = instances.find((instance) => instance.workflowId === ref.workflowId)!
      const childInstance = instances.find((instance) => instance.workflowName === ChildWorkflow.name)!
      expect(childInstance.partitionShard).toBe(parent.partitionShard)
      await expect(provider.loadInstance(ref)).resolves.toMatchObject({
        status: "completed",
        output: { value: 10 },
      })
    })
  })

  it("rejects explicit cross-shard local child workflow ids in the append-store fast path", async () => {
    await withProvider(async (provider) => {
      const ChildWorkflow = defineWorkflow({
        name: "pg_cross_shard_child",
        version: 1,
        input: z.object({}),
        output: z.object({ ok: z.boolean() }),
        common: z.object({}),
        initial() {
          return start({ common: {}, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: () => complete({ ok: true }),
          }),
        },
      })
      const ParentWorkflow = defineWorkflow({
        name: "pg_cross_shard_parent",
        version: 1,
        input: z.object({ childWorkflowId: z.string() }),
        output: z.object({ ok: z.boolean() }),
        common: z.object({ childWorkflowId: z.string() }),
        initial(input) {
          return start({ common: input, phase: "run", data: {} })
        },
        phases: {
          run: phase({
            run: async ({ ctx, common }) => {
              await ctx.child.start("child", ChildWorkflow, {}, { workflowId: common.childWorkflowId })
              return complete({ ok: true })
            },
          }),
        },
      })
      const parentWorkflowId = workflowIdForShard("pg-cross-parent", 0, 4)
      const childWorkflowId = workflowIdForShard("pg-cross-child", 1, 4)
      const runtime = new DurableRuntime(provider, {
        workflows: [ParentWorkflow, ChildWorkflow],
        workerId: "pg-cross-worker",
        shardCount: 4,
      })
      await runtime.start(ParentWorkflow, { childWorkflowId }, { workflowId: parentWorkflowId })
      await expect(runtime.drain({ maxActivations: 1 })).rejects.toThrow(
        "shard-affine",
      )
    })
  })
})

function workflowIdForShard(prefix: string, shardId: number, shardCount: number): string {
  for (let index = 0; index < 10_000; index += 1) {
    const candidate = `${prefix}-${index}`
    if (workflowPartitionShard(candidate, "run-1", shardCount) === shardId) {
      return candidate
    }
  }
  throw new Error(`Unable to find workflow id for shard ${shardId}`)
}
