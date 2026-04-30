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
npm run benchmark -- --activation-concurrency 4 --sqlite-synchronous full
```

The benchmark now reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

Measured on this workspace with 250 workflows, 4 workers, 4 shards, batch 32,
and zero artificial activity delay:

| SQLite synchronous | activation concurrency | e2e workflows/sec | e2e activations/sec | e2e mixed actions/sec | processing activations/sec | processing mixed actions/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| full | 1 | 265 | 1,323 | 2,116 | 1,407 | 2,251 |
| full | 4 | 274 | 1,372 | 2,195 | 1,461 | 2,337 |
| normal | 4 | 317 | 1,586 | 2,537 | 1,672 | 2,675 |

This benchmark is mostly SQLite/CPU-bound, so higher activation concurrency does
not improve this particular throughput row. The concurrency path is still useful
for workers with long in-flight async activations because one blocked activation
no longer occupies the entire worker.

With 100 workflows and a 5 ms async delay inside each activity, the same local
machine measured:

| activation concurrency | e2e activations/sec | e2e mixed actions/sec | processing activations/sec | processing mixed actions/sec |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 874 | 1,398 | 921 | 1,474 |
| 4 | 1,461 | 2,337 | 1,611 | 2,578 |

`synchronous=full` is the conservative default. `synchronous=normal` is available
for deployments that accept SQLite's weaker crash window in exchange for higher
write throughput.

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
