/*
 * Shows all signal styles in one workflow:
 * static signals (`configure`, `cancel`), one state-derived signal type
 * (`provider_result:${jobId}`), and one dynamic wait per approver.
 */

import { z } from "zod"
import { cancel, complete, defineWorkflow, go, phase, signal, start, stay } from "../durable.js"
import { cleanupDemoStore, committed, demoRuntime } from "./_shared.js"

const DynamicSignalWorkflow = defineWorkflow({
  name: "demo_dynamic_signals",
  version: 1,
  input: z.object({ requestId: z.string() }),
  output: z.object({
    jobId: z.string(),
    approvals: z.record(z.string(), z.string()),
  }),
  initial(input) {
    return start({
      phase: "waiting_config",
      data: { requestId: input.requestId },
    })
  },
  phases: {
    waiting_config: phase({
      state: z.object({ requestId: z.string() }),
      on: {
        configure: signal(
          z.object({ jobId: z.string(), approvers: z.array(z.string()) }),
          async ({ event }) => go("waiting_provider", {
            jobId: event.jobId,
            approvers: event.approvers,
            approvals: {},
          }),
        ),
        cancel: signal(z.object({ reason: z.string() }), async ({ event }) => cancel(event.reason)),
      },
    }),
    waiting_provider: phase({
      state: z.object({
        jobId: z.string(),
        approvers: z.array(z.string()),
        approvals: z.record(z.string(), z.string()),
      }),
      on: {
        provider_result: signal({
          schema: z.object({ ok: z.boolean() }),
          type: ({ data }) => `provider_result:${data.jobId}`,
          meta: ({ data }) => ({ jobId: data.jobId }),
          handler: async ({ data }) => go("waiting_approvals", {
            jobId: data.jobId,
            pending: data.approvers,
            approvals: data.approvals,
          }),
        }),
        cancel: signal(z.object({ reason: z.string() }), async ({ event }) => cancel(event.reason)),
      },
    }),
    waiting_approvals: phase({
      state: z.object({
        jobId: z.string(),
        pending: z.array(z.string()),
        approvals: z.record(z.string(), z.string()),
      }),
      on: {
        approval: signal.each({
          items: ({ data }) => data.pending as string[],
          schema: z.object({ decision: z.string() }),
          name: (approverId) => `approval:${approverId}`,
          type: (approverId) => `approval:${approverId}`,
          handler: async ({ data, event, wait }) => {
            const approverId = z.string().parse(wait?.meta)
            const approvals = { ...data.approvals, [approverId]: event.decision }
            const pending = data.pending.filter((id: string) => id !== approverId)
            return pending.length === 0
              ? complete({ jobId: data.jobId, approvals })
              : stay({ pending, approvals })
          },
        }),
        cancel: signal(z.object({ reason: z.string() }), async ({ event }) => cancel(event.reason)),
      },
    }),
  },
})

export async function runDynamicSignalsDemo(): Promise<void> {
  const demoName = "dynamic-signals"
  const first = await demoRuntime(demoName, [DynamicSignalWorkflow])
  try {
    const ref = await first.runtime.start(
      DynamicSignalWorkflow,
      { requestId: "request-1" },
      { workflowId: "dynamic-signals-demo" },
    )

    await first.runtime.signal(DynamicSignalWorkflow, ref, "configure", {
      jobId: "job-1",
      approvers: ["ada", "grace"],
    })
    await first.runtime.drain()

    const restarted = await demoRuntime(demoName, [DynamicSignalWorkflow])
    await restarted.runtime.signal(DynamicSignalWorkflow, ref, "provider_result:job-1", { ok: true })
    await restarted.runtime.drain()
    await restarted.runtime.signal(DynamicSignalWorkflow, ref, "approval:ada", { decision: "yes" })
    await restarted.runtime.drain()
    await restarted.runtime.signal(DynamicSignalWorkflow, ref, "approval:grace", { decision: "yes" })
    await restarted.runtime.drain()

    console.log("dynamic signals: completed", await committed(restarted.provider, ref))
  } finally {
    await cleanupDemoStore(demoName)
  }
}
