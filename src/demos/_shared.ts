import { rm } from "node:fs/promises"
import { resolve } from "node:path"
import {
  AnyWorkflow,
  DurableRuntime,
  InstanceRef,
  PersistedInstance,
  SqliteDurabilityProvider,
} from "../durable.js"

export const addMs = (iso: string, ms: number) => new Date(new Date(iso).getTime() + ms).toISOString()

export type DemoRuntime = {
  runtime: DurableRuntime
  provider: SqliteDurabilityProvider
  workerId: string
  clock(): Date
  advance(ms: number): void
}

export function demoStorePath(name: string): string {
  return resolve(`.durable-demo/${name}.sqlite`)
}

export async function cleanupDemoStore(name: string): Promise<void> {
  const storePath = demoStorePath(name)
  const legacyJsonPath = resolve(`.durable-demo/${name}.json`)
  await Promise.all(
    [storePath, `${storePath}-wal`, `${storePath}-shm`, `${storePath}-journal`, legacyJsonPath].map((path) =>
      rm(path, { force: true, maxRetries: 3, retryDelay: 10 }),
    ),
  )
}

export async function demoRuntime(name: string, workflows: AnyWorkflow[]): Promise<DemoRuntime> {
  const storePath = demoStorePath(name)
  await cleanupDemoStore(name)

  let now = new Date("2026-01-01T00:00:00.000Z")
  const clock = () => now
  const provider = new SqliteDurabilityProvider(storePath)
  const workerId = `${name}-worker`
  const runtime = new DurableRuntime(provider, { clock, workflows, workerId })

  return {
    runtime,
    provider,
    workerId,
    clock,
    advance(ms) {
      now = new Date(now.getTime() + ms)
    },
  }
}

export async function committed(provider: SqliteDurabilityProvider, ref: InstanceRef): Promise<unknown> {
  return summarize(await provider.loadInstance(ref))
}

function summarize(instance: PersistedInstance | null): unknown {
  if (!instance) {
    return null
  }

  if (instance.status === "running") {
    return {
      workflowVersion: instance.workflowVersion,
      sequence: instance.sequence,
      status: instance.status,
      phase: instance.phase?.name,
      data: instance.phase?.data,
      waits: instance.waits,
    }
  }

  return {
    workflowVersion: instance.workflowVersion,
    sequence: instance.sequence,
    status: instance.status,
    output: instance.output,
    reason: instance.cancelReason,
    error: instance.error,
  }
}
