# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). The TypeScript runtime is backed by a SQLite durability
provider with shard leases, activation leases, atomic checkpoint commits,
durable ready indexes, durable waits, a signal inbox, child records, and
activation-scoped effects. `DurableRuntime.drain()` is useful for tests and
manual pumping; `DurableRuntime.runWorker()` adds bounded polling, wake hints,
lease heartbeats, and bounded activation concurrency for long-running workers.

```bash
npm run demo
npm test
```

The demos in [`src/demos/`](src/demos/) are intentionally small:

- immediate `run` phases plus signal delivery
- timer waits that survive runtime reconstruction plus `stay()`
- `checkpoint()` and the bounded unbound-loop pattern
- a tiny local child workflow
- checkpoint-boundary workflow migration

This is still intentionally small. Activity retry policy, the public activity
heartbeat API, and local child close policies are implemented in the TypeScript
prototype; remote children and tenant-aware partitioning are left for later
steps.

## SQLite benchmark

The SQLite benchmark runs real workflows with activities, signals, timers, and
child completions. These are local sanity numbers, not a production guarantee.

```bash
npm run benchmark -- --activation-concurrency 4
```

Measured on this workspace with 250 workflows, batch 32, and the default
SQLite provider:

| workers | shards | activation concurrency | workflows/sec | activations/sec | mixed actions/sec |
| --- | --- | --- | ---: | ---: | ---: |
| 4 | 4 | 1 | 55.1 | 275.6 | 440.9 |
| 4 | 4 | 4 | 53.2 | 266.2 | 425.9 |
| 1 | 1 | 1 | 17.4 | 87.0 | 139.2 |
| 1 | 1 | 4 | 16.9 | 84.3 | 134.9 |

This benchmark is mostly SQLite/CPU-bound, so higher activation concurrency does
not improve this particular throughput row. The concurrency path is still useful
for workers with long in-flight async activations because one blocked activation
no longer occupies the entire worker.

## Provider conformance

New TypeScript durability providers should run the shared Vitest conformance
harness. The helper lives outside the main runtime barrel so production imports
do not load Vitest:

```ts
import { describeDurabilityProviderConformance } from "./src/testing/conformance.js"

describeDurabilityProviderConformance({
  name: "MyDurabilityProvider",
  async createStore() {
    const sharedStore = await createIsolatedStore()
    return {
      async createProvider() {
        const provider = new MyDurabilityProvider(sharedStore)
        return { provider, close: () => provider.close?.() }
      },
      cleanup: () => sharedStore.destroy(),
    }
  },
})
```
