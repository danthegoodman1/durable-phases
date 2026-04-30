# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). The TypeScript runtime ships with SQLite and Postgres
durability providers with shard leases, activation leases, atomic checkpoint
commits, durable ready indexes, durable waits, a signal inbox, child records,
and activation-scoped effects. `DurableRuntime.drain()` is useful for tests and
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

## Local Postgres

Postgres support is opt-in for local development and tests because it requires
Docker:

```bash
npm run postgres:up
npm run test:postgres
npm run benchmark:postgres -- --json
npm run postgres:down
```

The Compose file defaults to the pinned official image
`${POSTGRES_IMAGE:-postgres:18.3}` on local port `${POSTGRES_PORT:-55432}`.

`PostgresDurabilityProvider.create(...)` accepts a connection string or shared
`pg.Pool`, schema name, pool size, statement/lock timeouts, and optional
observability sinks. The provider uses explicit transactions, row locks,
`FOR UPDATE SKIP LOCKED`, conflict-aware upserts, partial hot-path indexes, and
an advisory transaction lock for concurrent schema initialization.

## Benchmarks

The SQLite and Postgres benchmarks run the same real workflows with activities,
signals, timers, and child completions. These are local sanity numbers, not a
production guarantee.

```bash
npm run benchmark -- --activation-concurrency 4 --sqlite-synchronous full
npm run benchmark:postgres -- --activation-concurrency 4 --json
```

The benchmark now reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

Measured on this workspace with 250 workflows, 4 workers, 4 shards, batch 32,
activation concurrency 4, and zero artificial activity delay:

| Provider | durability mode | e2e workflows/sec | e2e activations/sec | e2e mixed actions/sec | processing activations/sec | processing mixed actions/sec |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SQLite | synchronous=full | 263 | 1,313 | 2,100 | 1,419 | 2,271 |
| SQLite | synchronous=normal | 339 | 1,695 | 2,712 | 1,788 | 2,861 |
| Postgres | Docker postgres:18.3, pool=24 | 92 | 459 | 735 | 637 | 1,019 |

The no-delay workload is mostly local DB/CPU-bound, so higher activation
concurrency does not necessarily improve that particular throughput row. The
concurrency path is still useful for workers with long in-flight async
activations because one blocked activation no longer occupies the entire worker.

With SQLite `synchronous=full`, 100 workflows, and a 5 ms async delay inside
each activity, the same local machine measured:

| activation concurrency | e2e activations/sec | e2e mixed actions/sec | processing activations/sec | processing mixed actions/sec |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 874 | 1,398 | 921 | 1,474 |
| 4 | 1,461 | 2,337 | 1,611 | 2,578 |

`synchronous=full` is SQLite's conservative default. `synchronous=normal` is
available for deployments that accept SQLite's weaker crash window in exchange
for higher write throughput. The Postgres row is from local Docker on the same
machine, so it includes client/server and container overhead.

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
