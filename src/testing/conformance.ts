import { describe, expect, it } from "vitest"
import { z } from "zod"
import type {
  ClaimedActivation,
  CommitCheckpointInput,
  DurabilityProvider,
  DurableWait,
  EffectReservation,
  ReadyEvent,
} from "../interface.js"
import { DurableRuntime } from "../runtime.js"
import type { ActivityOptions, InstanceRef, InstanceStatus, JsonObject } from "../workflow.js"
import {
  child,
  complete,
  defineWorkflow,
  go,
  phase,
  signal,
  start,
  stay,
  timer,
} from "../workflow.js"

export type ConformanceProviderHandle = {
  provider: DurabilityProvider
  close?: () => void | Promise<void>
}

export type DurabilityProviderConformanceStore = {
  createProvider(): Promise<ConformanceProviderHandle> | ConformanceProviderHandle
  cleanup(): Promise<void> | void
}

export type DurabilityProviderConformanceFactory = {
  name: string
  createStore(): Promise<DurabilityProviderConformanceStore> | DurabilityProviderConformanceStore
}

type ScopedStore = {
  createProvider(): Promise<DurabilityProvider>
}

type EventActivation = Extract<ClaimedActivation, { kind: "event" }>
type SignalEventActivation = EventActivation & { event: Extract<ReadyEvent, { kind: "signal" }> }
type ChildEventActivation = EventActivation & { event: Extract<ReadyEvent, { kind: "child" }> }

const T0 = "2026-01-01T00:00:00.000Z"
const T1 = "2026-01-01T00:00:01.000Z"
const T2 = "2026-01-01T00:00:02.000Z"
const T5 = "2026-01-01T00:00:05.000Z"
const LONG_LEASE_MS = 60_000
const WORKFLOWS = {
  conformance: { version: 1 },
  conformance_child: { version: 1 },
  conformance_other: { version: 1 },
}

export function describeDurabilityProviderConformance(
  factory: DurabilityProviderConformanceFactory,
): void {
  describe(`${factory.name} durability provider conformance`, () => {
    it("creates, loads, conflicts, replaces waits, and writes terminal checkpoints", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const ref = await createConformanceInstance(provider, {
          workflowId: "lifecycle",
          common: { value: "original" },
          phase: { name: "boot", data: { step: 1 } },
        })

        await expect(
          provider.createInstance({
            workflowName: "conformance",
            workflowVersion: 1,
            workflowId: ref.workflowId,
            runId: ref.runId,
            partitionShard: 0,
            common: { value: "fail" },
            phase: { name: "boot", data: { step: 2 } },
            waits: [runWait()],
            now: T0,
            conflictPolicy: "fail",
          }),
        ).rejects.toThrow()

        await expect(
          provider.createInstance({
            workflowName: "conformance",
            workflowVersion: 1,
            workflowId: ref.workflowId,
            runId: ref.runId,
            partitionShard: 0,
            common: { value: "ignored" },
            phase: { name: "boot", data: { step: 3 } },
            waits: [runWait()],
            now: T0,
            conflictPolicy: "use_existing",
          }),
        ).resolves.toEqual(ref)
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          workflowName: "conformance",
          workflowVersion: 1,
          workflowId: "lifecycle",
          runId: "run-1",
          sequence: 0,
          status: "running",
          common: { value: "original" },
          phase: { name: "boot", data: { step: 1 } },
          waits: [runWait()],
        })

        await provider.createInstance({
          workflowName: "conformance",
          workflowVersion: 1,
          workflowId: ref.workflowId,
          runId: ref.runId,
          partitionShard: 0,
          common: { value: "replacement" },
          phase: { name: "boot", data: { step: 4 } },
          waits: [runWait()],
          now: T1,
          conflictPolicy: "terminate_existing",
        })
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          sequence: 0,
          status: "running",
          common: { value: "replacement" },
          phase: { name: "boot", data: { step: 4 } },
        })

        await ownShard(provider, "worker-a")
        const activation = requireActivation(await claim(provider, "worker-a"))
        expect(activation).toMatchObject({ kind: "run", sequence: 0 })

        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: running({ value: "replacement" }, "waiting", { ready: false }),
            waits: [signalWait("finish")],
            now: T1,
          }),
        ).resolves.toEqual({ ok: true, sequence: 1 })
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          sequence: 1,
          status: "running",
          phase: { name: "waiting", data: { ready: false } },
          waits: [signalWait("finish")],
        })

        await provider.appendSignal({
          ...ref,
          type: "finish",
          payload: { ok: true },
          receivedAt: T2,
        })
        const event = requireSignalEventActivation(await claim(provider, "worker-a", T2))
        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 1,
            activationId: event.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { ok: true } },
            waits: [],
            now: T2,
            consumeSignalId: event.event.consumeSignalId,
          }),
        ).resolves.toEqual({ ok: true, sequence: 2 })
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          sequence: 2,
          status: "completed",
          output: { ok: true },
          waits: [],
        })
      })
    })

    it("enforces dispatch shard leases and stable activation reclaim", async () => {
      await withStore(factory, async (store) => {
        const providerA = await store.createProvider()
        const providerB = await store.createProvider()
        await createConformanceInstance(providerA, { workflowId: "lease-run" })

        await expect(claim(providerA, "worker-a")).resolves.toMatchObject({ activation: null })

        const shardA = await providerA.claimDispatchShard({
          shardId: 0,
          ownerId: "worker-a",
          now: T0,
          leaseMs: 100,
        })
        expect(shardA).toMatchObject({ shardId: 0, ownerId: "worker-a" })
        await expect(
          providerB.claimDispatchShard({
            shardId: 0,
            ownerId: "worker-b",
            now: T0,
            leaseMs: 100,
          }),
        ).resolves.toBeNull()
        await providerA.heartbeatDispatchShard({
          shardId: 0,
          ownerId: "worker-a",
          now: T0,
          leaseMs: 1_000,
        })
        await providerA.releaseDispatchShard({ shardId: 0, ownerId: "worker-a" })
        await expect(
          providerB.claimDispatchShard({
            shardId: 0,
            ownerId: "worker-b",
            now: T0,
            leaseMs: 100,
          }),
        ).resolves.toMatchObject({ ownerId: "worker-b" })

        await providerB.releaseDispatchShard({ shardId: 0, ownerId: "worker-b" })
        await providerA.claimDispatchShard({
          shardId: 0,
          ownerId: "worker-a",
          now: T0,
          leaseMs: 100,
        })
        const first = requireActivation(await claim(providerA, "worker-a", T0, 100))
        await expect(
          providerA.heartbeatActivation({
            activationId: first.activationId,
            workerId: "worker-a",
            now: T1,
            leaseMs: 100,
          }),
        ).rejects.toThrow()

        await expect(
          providerB.claimDispatchShard({
            shardId: 0,
            ownerId: "worker-b",
            now: T1,
            leaseMs: LONG_LEASE_MS,
          }),
        ).resolves.toMatchObject({ ownerId: "worker-b" })
        const reclaimed = requireActivation(await claim(providerB, "worker-b", T1))
        expect(reclaimed.activationId).toBe(first.activationId)
        expect(reclaimed).toMatchObject({ kind: "run", sequence: 0 })
      })
    })

    it("discovers signal, timer, migration, and ordered child readiness from durable indexes", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        await ownShard(provider, "worker-a")

        await createConformanceInstance(provider, {
          workflowId: "signal-ready",
          waits: [signalWait("finish")],
        })
        await provider.appendSignal({
          workflowId: "signal-ready",
          runId: "run-1",
          type: "finish",
          payload: { value: 42 },
          receivedAt: T1,
        })
        const signalActivation = requireSignalEventActivation(await claim(provider, "worker-a", T1))
        expect(signalActivation).toMatchObject({
          workflowId: "signal-ready",
          waitName: "finish",
          event: { kind: "signal", payload: { value: 42 } },
        })
        await provider.commitCheckpoint({
          workflowId: "signal-ready",
          runId: "run-1",
          expectedSequence: 0,
          activationId: signalActivation.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: { status: "completed", output: { value: 42 } },
          waits: [],
          now: T1,
          consumeSignalId: signalActivation.event.consumeSignalId,
        })

        await createConformanceInstance(provider, {
          workflowId: "timer-ready",
          waits: [{ kind: "timer", name: "wake", fireAt: T5 }],
        })
        await expect(claim(provider, "worker-a", T2)).resolves.toMatchObject({
          activation: null,
          nextWakeAt: T5,
        })
        const timerActivation = requireEventActivation(await claim(provider, "worker-a", T5))
        expect(timerActivation).toMatchObject({
          workflowId: "timer-ready",
          waitName: "wake",
          event: { kind: "timer", firedAt: T5 },
          activationTime: T5,
        })
        await provider.releaseActivation({
          activationId: timerActivation.activationId,
          workerId: "worker-a",
        })

        await createConformanceInstance(provider, {
          workflowName: "conformance_other",
          workflowId: "migration-ready",
          workflowVersion: 1,
          waits: [signalWait("never")],
        })
        const migration = requireActivation(
          await claim(provider, "worker-a", T0, LONG_LEASE_MS, {
            conformance_other: { version: 2 },
          }),
        )
        expect(migration).toMatchObject({
          kind: "migration",
          workflowName: "conformance_other",
          workflowId: "migration-ready",
          sequence: 0,
        })
        await provider.releaseActivation({
          activationId: migration.activationId,
          workerId: "worker-a",
        })

        const orderingRef = await createConformanceInstance(provider, {
          workflowId: "ordered-parent",
          waits: [runWait()],
        })
        const setup = requireActivation(await claim(provider, "worker-a", T0))
        const childHandle = await provider.createChildInstance({
          workflowName: "conformance_child",
          workflowVersion: 1,
          workflowId: "ordered-child",
          runId: "run-1",
          partitionShard: 0,
          common: {},
          phase: { name: "run", data: {} },
          waits: [runWait()],
          now: T0,
          parentWorkflowId: orderingRef.workflowId,
          parentRunId: orderingRef.runId,
          activationId: setup.activationId,
          workerId: "worker-a",
          leaseNow: T0,
          key: "child",
        })
        await provider.commitCheckpoint({
          ...orderingRef,
          expectedSequence: 0,
          activationId: setup.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: running({}, "waiting", {}),
          waits: [
            childWait("child_done", childHandle),
            signalWait("signal_done"),
            { kind: "timer", name: "timer_done", fireAt: T1 },
          ],
          now: T0,
        })

        const childRun = requireActivation(await claim(provider, "worker-a", T0))
        expect(childRun).toMatchObject({ kind: "run", workflowId: "ordered-child" })
        await provider.commitCheckpoint({
          workflowId: childHandle.workflowId,
          runId: childHandle.runId,
          expectedSequence: 0,
          activationId: childRun.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: { status: "completed", output: { child: true } },
          waits: [],
          now: T1,
        })
        await provider.appendSignal({
          ...orderingRef,
          type: "signal_done",
          payload: {},
          receivedAt: T1,
        })

        const ordered = requireEventActivation(await claim(provider, "worker-a", T1))
        expect(ordered).toMatchObject({
          workflowId: "ordered-parent",
          waitName: "child_done",
          event: { kind: "child" },
        })
      })
    })

    it("blocks migrations until older non-migration activations complete", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const ref = await createConformanceInstance(provider, {
          workflowName: "conformance_other",
          workflowId: "migration-blocked",
          workflowVersion: 1,
          waits: [runWait()],
        })
        await ownShard(provider, "worker-a")

        const runActivation = requireActivation(
          await claim(provider, "worker-a", T0, LONG_LEASE_MS, {
            conformance_other: { version: 1 },
          }),
        )
        await expect(
          claim(provider, "worker-a", T0, LONG_LEASE_MS, {
            conformance_other: { version: 2 },
          }),
        ).resolves.toMatchObject({ activation: null })

        await provider.commitCheckpoint({
          ...ref,
          expectedSequence: 0,
          activationId: runActivation.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: running({}, "waiting", {}),
          waits: [signalWait("finish")],
          now: T1,
        })

        const migration = requireActivation(
          await claim(provider, "worker-a", T1, LONG_LEASE_MS, {
            conformance_other: { version: 2 },
          }),
        )
        expect(migration).toMatchObject({ kind: "migration", workflowId: "migration-blocked" })
      })
    })

    it("makes checkpoint commits authoritative and non-destructive on conflicts", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const ref = await createConformanceInstance(provider, {
          workflowId: "checkpoint-authority",
          waits: [signalWait("finish")],
        })
        await ownShard(provider, "worker-a")
        const firstSignal = await provider.appendSignal({
          ...ref,
          type: "finish",
          payload: { index: 1 },
          receivedAt: T0,
        })
        const secondSignal = await provider.appendSignal({
          ...ref,
          type: "finish",
          payload: { index: 2 },
          receivedAt: T1,
        })
        const activation = requireSignalEventActivation(await claim(provider, "worker-a", T0))
        expect(activation.event.consumeSignalId).toBe(firstSignal.signalId)

        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { wrong: true } },
            waits: [],
            now: T1,
            consumeSignalId: secondSignal.signalId,
          }),
        ).resolves.toEqual({ ok: false, sequence: 0 })
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          sequence: 0,
          status: "running",
        })

        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { index: 1 } },
            waits: [],
            now: T1,
            consumeSignalId: activation.event.consumeSignalId,
          }),
        ).resolves.toEqual({ ok: true, sequence: 1 })

        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { stale: true } },
            waits: [],
            now: T2,
            consumeSignalId: activation.event.consumeSignalId,
          }),
        ).resolves.toEqual({ ok: false, sequence: 1 })
        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          sequence: 1,
          status: "completed",
          output: { index: 1 },
        })
      })
    })

    it("rejects lost activation leases and leaves claimed signals consumable", async () => {
      await withStore(factory, async (store) => {
        const providerA = await store.createProvider()
        const providerB = await store.createProvider()
        const ref = await createConformanceInstance(providerA, {
          workflowId: "lost-lease-signal",
          waits: [signalWait("finish")],
        })
        await providerA.appendSignal({
          ...ref,
          type: "finish",
          payload: { ok: true },
          receivedAt: T0,
        })
        await providerA.claimDispatchShard({
          shardId: 0,
          ownerId: "worker-a",
          now: T0,
          leaseMs: 100,
        })
        const activationA = requireSignalEventActivation(await claim(providerA, "worker-a", T0, 100))
        await expect(
          providerA.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activationA.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { worker: "a" } },
            waits: [],
            now: T1,
            consumeSignalId: activationA.event.consumeSignalId,
          }),
        ).resolves.toEqual({ ok: false, sequence: 0 })

        await providerB.claimDispatchShard({
          shardId: 0,
          ownerId: "worker-b",
          now: T1,
          leaseMs: LONG_LEASE_MS,
        })
        const activationB = requireSignalEventActivation(await claim(providerB, "worker-b", T1))
        expect(activationB.activationId).toBe(activationA.activationId)
        await expect(
          providerB.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activationB.activationId,
            workerId: "worker-b",
            workflowVersion: 1,
            next: { status: "completed", output: { worker: "b" } },
            waits: [],
            now: T1,
            consumeSignalId: activationB.event.consumeSignalId,
          }),
        ).resolves.toEqual({ ok: true, sequence: 1 })
        await expect(providerB.loadInstance(ref)).resolves.toMatchObject({
          status: "completed",
          output: { worker: "b" },
        })
      })
    })

    it("blocks competing ready events for the same sequence until the activation is released", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const ref = await createConformanceInstance(provider, {
          workflowId: "competing-events",
          waits: [signalWait("finish"), { kind: "timer", name: "wake", fireAt: T0 }],
        })
        await provider.appendSignal({
          ...ref,
          type: "finish",
          payload: {},
          receivedAt: T0,
        })
        await ownShard(provider, "worker-a")
        const signalActivation = requireEventActivation(await claim(provider, "worker-a", T0))
        expect(signalActivation.event.kind).toBe("signal")

        await expect(claim(provider, "worker-a", T0)).resolves.toMatchObject({ activation: null })
        await provider.releaseActivation({
          activationId: signalActivation.activationId,
          workerId: "worker-a",
        })
        const reclaimed = requireEventActivation(await claim(provider, "worker-a", T0))
        expect(reclaimed.activationId).toBe(signalActivation.activationId)
      })
    })

    it("memoizes completed effects and fences terminal or failed effects", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "effects-memo")
        const reservation = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "work",
            now: T0,
          }),
        )
        expect(reservation.attempt).toBe(1)
        await provider.heartbeatEffect({
          ...ref,
          activationId: activation.activationId,
          workerId: "worker-a",
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          now: T0,
          details: { progress: 1 },
        })
        await provider.completeEffect({
          ...ref,
          activationId: activation.activationId,
          workerId: "worker-a",
          effectId: reservation.effectId,
          attemptId: reservation.attemptId,
          result: { ok: true },
          now: T0,
        })
        await expect(
          provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "work",
            now: T1,
          }),
        ).resolves.toEqual({ status: "completed", result: { ok: true } })
        await expect(
          provider.failEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            effectId: reservation.effectId,
            attemptId: reservation.attemptId,
            error: { name: "LateFailure", message: "too late" },
            now: T1,
          }),
        ).rejects.toThrow()

        const failed = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "terminal-failure",
            now: T1,
            options: { retry: { maxAttempts: 1 } },
          }),
        )
        await expect(
          provider.failEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            effectId: failed.effectId,
            attemptId: failed.attemptId,
            error: { name: "Boom", message: "boom" },
            now: T1,
          }),
        ).resolves.toEqual({ status: "failed" })
        await expect(
          provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "terminal-failure",
            now: T1,
          }),
        ).resolves.toEqual({
          status: "failed",
          error: { name: "Boom", message: "boom" },
        })
      })
    })

    it("schedules effect retries with backoff and restores heartbeat details on the next attempt", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "effects-retry")
        const options: ActivityOptions = {
          retry: {
            maxAttempts: 2,
            initialIntervalMs: 500,
            maxIntervalMs: 500,
            backoffCoefficient: 1,
          },
        }
        const first = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "retry",
            now: T0,
            options,
          }),
        )
        await provider.heartbeatEffect({
          ...ref,
          activationId: activation.activationId,
          workerId: "worker-a",
          effectId: first.effectId,
          attemptId: first.attemptId,
          now: T0,
          details: { offset: 10 },
        })
        const retry = await provider.failEffect({
          ...ref,
          activationId: activation.activationId,
          workerId: "worker-a",
          effectId: first.effectId,
          attemptId: first.attemptId,
          error: { name: "RetryMe", message: "try again" },
          now: T0,
        })
        expect(retry).toEqual({
          status: "retry_scheduled",
          nextAttempt: 2,
          nextAttemptAt: addMs(T0, 500),
        })
        await provider.releaseActivation({
          activationId: activation.activationId,
          workerId: "worker-a",
        })
        await expect(claim(provider, "worker-a", T0)).resolves.toMatchObject({
          activation: null,
          nextWakeAt: addMs(T0, 500),
        })
        const reclaimed = requireActivation(await claim(provider, "worker-a", addMs(T0, 500)))
        expect(reclaimed.activationId).toBe(activation.activationId)
        const second = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: reclaimed.activationId,
            workerId: "worker-a",
            key: "retry",
            now: addMs(T0, 500),
            options,
          }),
        )
        expect(second.attempt).toBe(2)
        expect(second.idempotencyKey).toBe(first.idempotencyKey)
        expect(second.attemptId).not.toBe(first.attemptId)
        expect(second.heartbeatDetails).toEqual({ offset: 10 })
      })
    })

    it("expires heartbeat and start-to-close activity attempts and fences stale attempt mutations", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "effects-timeout", {
          shardLeaseMs: LONG_LEASE_MS,
          activationLeaseMs: LONG_LEASE_MS,
        })
        const first = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            key: "heartbeat-timeout",
            now: T0,
            options: {
              heartbeatTimeoutMs: 500,
              retry: { maxAttempts: 2, initialIntervalMs: 0 },
            },
          }),
        )
        await provider.heartbeatEffect({
          ...ref,
          activationId: activation.activationId,
          workerId: "worker-a",
          effectId: first.effectId,
          attemptId: first.attemptId,
          now: addMs(T0, 250),
          details: { page: 3 },
        })
        await provider.heartbeatActivation({
          activationId: activation.activationId,
          workerId: "worker-a",
          now: addMs(T0, 600),
          leaseMs: LONG_LEASE_MS,
        })
        await expect(
          provider.heartbeatActivation({
            activationId: activation.activationId,
            workerId: "worker-a",
            now: addMs(T0, 800),
            leaseMs: LONG_LEASE_MS,
          }),
        ).rejects.toThrow()
        const retryActivation = requireActivation(await claim(provider, "worker-a", addMs(T0, 800)))
        const second = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: retryActivation.activationId,
            workerId: "worker-a",
            key: "heartbeat-timeout",
            now: addMs(T0, 800),
            options: {
              heartbeatTimeoutMs: 500,
              retry: { maxAttempts: 2, initialIntervalMs: 0 },
            },
          }),
        )
        expect(second.attempt).toBe(2)
        expect(second.heartbeatDetails).toEqual({ page: 3 })
        await expect(
          provider.completeEffect({
            ...ref,
            activationId: activation.activationId,
            workerId: "worker-a",
            effectId: first.effectId,
            attemptId: first.attemptId,
            result: { late: true },
            now: addMs(T0, 800),
          }),
        ).rejects.toThrow()

        const startClose = requireReserved(
          await provider.getOrReserveEffect({
            ...ref,
            activationId: retryActivation.activationId,
            workerId: "worker-a",
            key: "start-close-timeout",
            now: T1,
            options: {
              startToCloseTimeoutMs: 500,
              heartbeatTimeoutMs: 5_000,
              retry: { maxAttempts: 1 },
            },
          }),
        )
        await provider.heartbeatEffect({
          ...ref,
          activationId: retryActivation.activationId,
          workerId: "worker-a",
          effectId: startClose.effectId,
          attemptId: startClose.attemptId,
          now: addMs(T1, 250),
          details: { still: "running" },
        })
        await expect(
          provider.heartbeatActivation({
            activationId: retryActivation.activationId,
            workerId: "worker-a",
            now: addMs(T1, 500),
            leaseMs: LONG_LEASE_MS,
          }),
        ).rejects.toThrow()
        const failedActivation = requireActivation(await claim(provider, "worker-a", addMs(T1, 500)))
        await expect(
          provider.getOrReserveEffect({
            ...ref,
            activationId: failedActivation.activationId,
            workerId: "worker-a",
            key: "start-close-timeout",
            now: addMs(T1, 500),
          }),
        ).resolves.toMatchObject({
          status: "failed",
          error: { name: "ActivityTimeoutError" },
        })
      })
    })

    it("requires live parent activation leases for child starts and implements child conflict policies", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "child-conflicts", {
          activationLeaseMs: 100,
        })
        await expect(
          provider.createChildInstance({
            ...childCreateInput(ref, activation.activationId, "late", "late-child"),
            workerId: "worker-a",
            leaseNow: T1,
          }),
        ).rejects.toThrow()

        await provider.heartbeatActivation({
          activationId: activation.activationId,
          workerId: "worker-a",
          now: T0,
          leaseMs: LONG_LEASE_MS,
        })
        const first = await provider.createChildInstance(
          childCreateInput(ref, activation.activationId, "same-key", "same-child"),
        )
        await expect(
          provider.createChildInstance({
            ...childCreateInput(ref, activation.activationId, "same-key", "ignored-child"),
            conflictPolicy: "use_existing",
          }),
        ).resolves.toEqual(first)
        await expect(
          provider.createChildInstance({
            ...childCreateInput(ref, activation.activationId, "same-key", "fail-child"),
            conflictPolicy: "fail",
          }),
        ).rejects.toThrow()

        const replacement = await provider.createChildInstance({
          ...childCreateInput(ref, activation.activationId, "same-key", "replacement-child"),
          conflictPolicy: "terminate_existing",
        })
        expect(replacement.workflowId).toBe("replacement-child")
        await expect(provider.loadInstance(first)).resolves.toBeNull()
        await expect(provider.loadInstance(replacement)).resolves.toMatchObject({
          status: "running",
          parent: {
            workflowId: ref.workflowId,
            runId: ref.runId,
          },
        })

        const explicit = await provider.createChildInstance(
          childCreateInput(ref, activation.activationId, "explicit-a", "explicit-child"),
        )
        await expect(
          provider.createChildInstance({
            ...childCreateInput(ref, activation.activationId, "explicit-b", explicit.workflowId),
            conflictPolicy: "fail",
          }),
        ).rejects.toThrow()
      })
    })

    it("delivers child completions and child cancellations through claimed child events", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "child-completion")
        const childHandle = await provider.createChildInstance(
          childCreateInput(ref, activation.activationId, "child", "child-completion-run"),
        )
        await provider.commitCheckpoint({
          ...ref,
          expectedSequence: 0,
          activationId: activation.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: running({}, "waiting-child", { childHandle: jsonChildHandle(childHandle) }),
          waits: [childWait("child_done", childHandle)],
          now: T0,
        })

        const childRun = requireActivation(await claim(provider, "worker-a", T0))
        await provider.commitCheckpoint({
          workflowId: childHandle.workflowId,
          runId: childHandle.runId,
          expectedSequence: 0,
          activationId: childRun.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: { status: "completed", output: { child: "done" } },
          waits: [],
          now: T1,
        })
        const childEvent = requireChildEventActivation(await claim(provider, "worker-a", T1))
        expect(childEvent).toMatchObject({
          workflowId: ref.workflowId,
          event: { kind: "child", event: { ok: true, output: { child: "done" } } },
        })
        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 1,
            activationId: childEvent.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "completed", output: { child: "done" } },
            waits: [],
            now: T1,
            consumeChildRecordId: childEvent.event.childRecordId,
          }),
        ).resolves.toEqual({ ok: true, sequence: 2 })

        const { ref: cancelRef, activation: cancelSetup } = await activeRun(provider, "child-cancel")
        const canceledChild = await provider.createChildInstance({
          ...childCreateInput(cancelRef, cancelSetup.activationId, "child", "child-cancel-run"),
          waits: [signalWait("never")],
        })
        await provider.commitCheckpoint({
          ...cancelRef,
          expectedSequence: 0,
          activationId: cancelSetup.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: running({}, "can-cancel", { canceledChild: jsonChildHandle(canceledChild) }),
          waits: [runWait(T1), childWait("child_done", canceledChild)],
          now: T0,
        })
        const cancelActivation = requireActivation(await claim(provider, "worker-a", T1))
        expect(cancelActivation).toMatchObject({ kind: "run", workflowId: cancelRef.workflowId })
        await provider.cancelChild({
          parentWorkflowId: cancelRef.workflowId,
          parentRunId: cancelRef.runId,
          activationId: cancelActivation.activationId,
          workerId: "worker-a",
          workflowId: canceledChild.workflowId,
          runId: canceledChild.runId,
          now: T1,
        })
        await provider.commitCheckpoint({
          ...cancelRef,
          expectedSequence: 1,
          activationId: cancelActivation.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: running({}, "waiting-child", { canceledChild: jsonChildHandle(canceledChild) }),
          waits: [childWait("child_done", canceledChild)],
          now: T1,
        })
        const canceledEvent = requireEventActivation(await claim(provider, "worker-a", T1))
        expect(canceledEvent).toMatchObject({
          workflowId: cancelRef.workflowId,
          event: {
            kind: "child",
            event: { ok: false, error: { name: "ChildCanceled" } },
          },
        })
      })
    })

    it("applies parent close cancel and abandon policies atomically", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const { ref, activation } = await activeRun(provider, "parent-close")
        const cancelChildHandle = await provider.createChildInstance(
          childCreateInput(ref, activation.activationId, "cancel-child", "close-cancel-child"),
        )
        const abandonChildHandle = await provider.createChildInstance({
          ...childCreateInput(ref, activation.activationId, "abandon-child", "close-abandon-child"),
          parentClosePolicy: "abandon",
        })

        await expect(
          provider.commitCheckpoint({
            ...ref,
            expectedSequence: 0,
            activationId: activation.activationId,
            workerId: "worker-a",
            workflowVersion: 1,
            next: { status: "canceled", reason: "parent canceled" },
            waits: [],
            now: T1,
          }),
        ).resolves.toEqual({ ok: true, sequence: 1 })
        await expect(provider.loadInstance(cancelChildHandle)).resolves.toMatchObject({
          status: "canceled",
          cancelReason: "Child canceled because parent canceled",
        })
        await expect(provider.loadInstance(abandonChildHandle)).resolves.toMatchObject({
          status: "running",
        })

        const abandonRun = requireActivation(await claim(provider, "worker-a", T1))
        expect(abandonRun).toMatchObject({ workflowId: abandonChildHandle.workflowId })
        await provider.commitCheckpoint({
          workflowId: abandonChildHandle.workflowId,
          runId: abandonChildHandle.runId,
          expectedSequence: 0,
          activationId: abandonRun.activationId,
          workerId: "worker-a",
          workflowVersion: 1,
          next: { status: "completed", output: { abandoned: true } },
          waits: [],
          now: T2,
        })
        await expect(claim(provider, "worker-a", T2)).resolves.toMatchObject({ activation: null })
      })
    })

    it("runs a real workflow through DurableRuntime with activities, child waits, signals, and timers", async () => {
      await withStore(factory, async (store) => {
        const provider = await store.createProvider()
        const clock = manualClock(T0)
        let activityCount = 0
        const ChildWorkflow = defineWorkflow({
          name: "conformance_runtime_child",
          version: 1,
          input: z.object({ value: z.string() }),
          output: z.object({ value: z.string() }),
          initial(input) {
            return start({ phase: "run", data: { value: input.value } })
          },
          phases: {
            run: phase({
              state: z.object({ value: z.string() }),
              run: async ({ data }) => complete({ value: `child:${data.value}` }),
            }),
          },
        })
        const ParentWorkflow = defineWorkflow({
          name: "conformance_runtime_parent",
          version: 1,
          input: z.object({ value: z.string() }),
          output: z.object({ result: z.string(), activities: z.number(), finishedAt: z.string() }),
          initial(input) {
            return start({ phase: "boot", data: { value: input.value } })
          },
          phases: {
            boot: phase({
              state: z.object({ value: z.string() }),
              run: async ({ ctx, data }) => {
                const activityValue = await ctx.activity("boot-activity", async () => {
                  activityCount += 1
                  return `${data.value}:activity`
                })
                const handle = await ctx.child.start("child", ChildWorkflow, {
                  value: activityValue,
                })
                return go("waiting_child", { handle, activityValue })
              },
            }),
            waiting_child: phase({
              state: z.object({
                activityValue: z.string(),
                handle: z.object({
                  workflowName: z.string(),
                  workflowVersion: z.number(),
                  workflowId: z.string(),
                  runId: z.string(),
                }),
              }),
              on: {
                child_done: child(
                  ({ data }) => data.handle,
                  async ({ data, event }) => {
                    if (!event.ok) {
                      return complete({
                        result: "child failed",
                        activities: activityCount,
                        finishedAt: T0,
                      })
                    }
                    return go("waiting_signal", {
                      activityValue: data.activityValue,
                      childValue: event.output.value,
                    })
                  },
                ),
              },
            }),
            waiting_signal: phase({
              state: z.object({ activityValue: z.string(), childValue: z.string() }),
              on: {
                finish: signal(z.object({ suffix: z.string() }), async ({ ctx, data, event }) =>
                  go("waiting_timer", {
                    result: `${data.childValue}${event.suffix}`,
                    fireAt: addMs(ctx.now(), 1_000),
                  }),
                ),
              },
            }),
            waiting_timer: phase({
              state: z.object({ result: z.string(), fireAt: z.string() }),
              on: {
                done: timer(
                  ({ data }) => data.fireAt,
                  async ({ ctx, data }) => {
                    await ctx.activity("finish-activity", async () => {
                      activityCount += 1
                      return "done"
                    })
                    return complete({
                      result: data.result,
                      activities: activityCount,
                      finishedAt: ctx.now(),
                    })
                  },
                ),
              },
            }),
          },
        })

        const runtime = new DurableRuntime(provider, {
          workerId: "runtime-worker",
          workflows: [ParentWorkflow, ChildWorkflow],
          clock: clock.clock,
          shardCount: 1,
          dispatchLeaseMs: LONG_LEASE_MS,
          activationLeaseMs: LONG_LEASE_MS,
        })
        const ref = await runtime.start(ParentWorkflow, { value: "hello" }, { workflowId: "runtime-smoke" })
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toMatchObject({ activations: 1 })
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toMatchObject({ activations: 1 })
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toMatchObject({ activations: 1 })
        await runtime.signal(ParentWorkflow, ref, "finish", { suffix: "!" })
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toMatchObject({ activations: 1 })
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toEqual({
          activations: 0,
          nextWakeAt: addMs(T0, 1_000),
        })
        clock.advance(1_000)
        await expect(runtime.drain({ maxActivations: 1 })).resolves.toMatchObject({ activations: 1 })

        await expect(provider.loadInstance(ref)).resolves.toMatchObject({
          status: "completed",
          output: {
            result: "child:hello:activity!",
            activities: 2,
            finishedAt: addMs(T0, 1_000),
          },
        })
      })
    })
  })
}

async function withStore<T>(
  factory: DurabilityProviderConformanceFactory,
  fn: (store: ScopedStore) => Promise<T>,
): Promise<T> {
  const store = await factory.createStore()
  const handles: ConformanceProviderHandle[] = []
  try {
    return await fn({
      createProvider: async () => {
        const handle = await store.createProvider()
        handles.push(handle)
        return handle.provider
      },
    })
  } finally {
    for (const handle of [...handles].reverse()) {
      await handle.close?.()
    }
    await store.cleanup()
  }
}

function addMs(iso: string, ms: number): string {
  return new Date(new Date(iso).getTime() + ms).toISOString()
}

function manualClock(initial: string) {
  let now = new Date(initial)
  return {
    clock: () => now,
    advance(ms: number) {
      now = new Date(now.getTime() + ms)
    },
  }
}

function runWait(readyAt = T0): DurableWait {
  return { kind: "run", name: "__run", readyAt }
}

function signalWait(type: string, name = type): DurableWait {
  return { kind: "signal", name, type, scope: "phase" }
}

function childWait(
  name: string,
  handle: { workflowName: string; workflowVersion: number; workflowId: string; runId: string },
): DurableWait {
  return {
    kind: "child",
    name,
    workflowName: handle.workflowName,
    workflowVersion: handle.workflowVersion,
    workflowId: handle.workflowId,
    runId: handle.runId,
  }
}

function running(common: JsonObject, phaseName: string, data: JsonObject): InstanceStatus {
  return {
    status: "running",
    common,
    phase: { name: phaseName, data },
  }
}

async function createConformanceInstance(
  provider: DurabilityProvider,
  input: Partial<{
    workflowName: string
    workflowVersion: number
    workflowId: string
    runId: string
    common: JsonObject
    phase: { name: string; data: JsonObject }
    waits: DurableWait[]
    now: string
  }> = {},
): Promise<InstanceRef> {
  return provider.createInstance({
    workflowName: input.workflowName ?? "conformance",
    workflowVersion: input.workflowVersion ?? 1,
    workflowId: input.workflowId ?? "conformance-instance",
    runId: input.runId ?? "run-1",
    partitionShard: 0,
    common: input.common ?? {},
    phase: input.phase ?? { name: "run", data: {} },
    waits: input.waits ?? [runWait()],
    now: input.now ?? T0,
  })
}

async function ownShard(
  provider: DurabilityProvider,
  workerId: string,
  now = T0,
  leaseMs = LONG_LEASE_MS,
): Promise<void> {
  await expect(
    provider.claimDispatchShard({
      shardId: 0,
      ownerId: workerId,
      now,
      leaseMs,
    }),
  ).resolves.toMatchObject({ shardId: 0, ownerId: workerId })
}

async function claim(
  provider: DurabilityProvider,
  workerId: string,
  now = T0,
  leaseMs = LONG_LEASE_MS,
  workflows: Record<string, { version: number }> = WORKFLOWS,
) {
  return provider.claimReadyActivation({
    workerId,
    shardIds: [0],
    workflows,
    now,
    leaseMs,
  })
}

function requireActivation(result: Awaited<ReturnType<typeof claim>>): ClaimedActivation {
  expect(result.activation).not.toBeNull()
  return result.activation!
}

function requireEventActivation(result: Awaited<ReturnType<typeof claim>>): EventActivation {
  const activation = requireActivation(result)
  expect(activation.kind).toBe("event")
  return activation as EventActivation
}

function requireSignalEventActivation(
  result: Awaited<ReturnType<typeof claim>>,
): SignalEventActivation {
  const activation = requireEventActivation(result)
  expect(activation.event.kind).toBe("signal")
  return activation as SignalEventActivation
}

function requireChildEventActivation(
  result: Awaited<ReturnType<typeof claim>>,
): ChildEventActivation {
  const activation = requireEventActivation(result)
  expect(activation.event.kind).toBe("child")
  return activation as ChildEventActivation
}

function requireReserved(reservation: EffectReservation): Extract<EffectReservation, { status: "reserved" }> {
  expect(reservation.status).toBe("reserved")
  return reservation as Extract<EffectReservation, { status: "reserved" }>
}

async function activeRun(
  provider: DurabilityProvider,
  workflowId: string,
  options: {
    shardLeaseMs?: number
    activationLeaseMs?: number
  } = {},
): Promise<{ ref: InstanceRef; activation: ClaimedActivation }> {
  const ref = await createConformanceInstance(provider, { workflowId, waits: [runWait()] })
  await ownShard(provider, "worker-a", T0, options.shardLeaseMs ?? LONG_LEASE_MS)
  const activation = requireActivation(
    await claim(provider, "worker-a", T0, options.activationLeaseMs ?? LONG_LEASE_MS),
  )
  expect(activation).toMatchObject({ kind: "run", workflowId })
  return { ref, activation }
}

function childCreateInput(
  parentRef: InstanceRef,
  activationId: string,
  key: string,
  workflowId: string,
) {
  return {
    workflowName: "conformance_child",
    workflowVersion: 1,
    workflowId,
    runId: "run-1",
    partitionShard: 0,
    common: {},
    phase: { name: "run", data: {} },
    waits: [runWait()],
    now: T0,
    parentWorkflowId: parentRef.workflowId,
    parentRunId: parentRef.runId,
    activationId,
    workerId: "worker-a",
    leaseNow: T0,
    key,
  } satisfies Parameters<DurabilityProvider["createChildInstance"]>[0]
}

function jsonChildHandle(handle: {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
}): JsonObject {
  return {
    workflowName: handle.workflowName,
    workflowVersion: handle.workflowVersion,
    workflowId: handle.workflowId,
    runId: handle.runId,
  }
}

function commitInput(
  ref: InstanceRef,
  activation: ClaimedActivation,
  next: InstanceStatus,
  waits: DurableWait[],
  now = T1,
): CommitCheckpointInput {
  return {
    ...ref,
    expectedSequence: activation.sequence,
    activationId: activation.activationId,
    workerId: "worker-a",
    workflowVersion: 1,
    next,
    waits,
    now,
  }
}
