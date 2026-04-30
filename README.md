# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). The TypeScript runtime ships with SQLite and Postgres
durability providers with shard leases, activation leases, atomic checkpoint
commits, durable ready indexes, durable waits, a signal inbox, child records,
and activation-scoped effects. Short inline TypeScript activities default to
checkpoint durability, so their results are committed with the workflow
checkpoint; heartbeat or timeout activities use eager per-attempt durability.
Child starts also default to checkpoint durability, so local children are
created atomically with the parent checkpoint unless `durability: "eager"` is
requested.
`DurableRuntime.drain()` is useful for tests and manual pumping;
`DurableRuntime.runWorker()` adds bounded polling, wake hints, lease heartbeats,
activation prefetch, batched activation commits, and bounded activation
concurrency for long-running workers.

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
`pg.Pool`, schema name, pool size, statement/lock timeouts,
`physicalPartitions`, and optional observability sinks. The provider uses
explicit transactions, row locks, `FOR UPDATE SKIP LOCKED`, conflict-aware
upserts, a unified `activation_tasks` table for ready work plus leases, partial
hot-path indexes, and an advisory transaction lock for concurrent schema
initialization.

`physicalPartitions` is fixed when a schema is created and is persisted in
provider metadata. Hot workflow/run tables are manually suffixed
(`instances_p00`, `signals_p00`, `activation_tasks_p00`, and so on), while
dispatch shards remain logical worker leases. The runtime API does not change;
the provider routes each workflow/run to its physical table set internally.

## Benchmarks

The SQLite and Postgres benchmarks run the same real workflows with activities,
signals, timers, and child completions. These are local sanity numbers, not a
production guarantee.

```bash
npm run benchmark -- --activation-concurrency 4 --sqlite-synchronous full
npm run benchmark:postgres -- --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:postgres -- --physical-partitions 4 --json
npm run benchmark:postgres -- --profile-queries --json
npm run benchmark:postgres:diagnose -- --physical-partitions 4 --workflows 1000 --workers 16 --shards 16 --pool-size 64 --json
```

The benchmark reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

Measured on this workspace with 1,000 workflows, zero artificial activity delay,
activation concurrency 4, activation prefetch 32, batch 32, and
`synchronous_commit=on`:

| Provider | workers/shards | physical partitions | pool | e2e workflows/sec | e2e activations/sec | processing activations/sec | processing mixed actions/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Postgres Docker postgres:18.3 | 4/4 | 1 | 24 | 201 | 1,005 | 1,814 | 2,902 |
| Postgres Docker postgres:18.3 | 4/4 | 4 | 24 | 213 | 1,066 | 2,046 | 3,274 |
| Postgres Docker postgres:18.3 | 16/16 | 1 | 64 | 258 | 1,291 | 3,861 | 6,178 |
| Postgres Docker postgres:18.3 | 16/16 | 4 | 64 | 273 | 1,365 | 3,898 | 6,237 |
| Postgres Docker postgres:18.3 | 32/32 | 1 | 96 | 294 | 1,469 | 3,980 | 6,368 |
| Postgres Docker postgres:18.3 | 32/32 | 4 | 96 | 270 | 1,351 | 3,788 | 6,060 |

`npm run benchmark:postgres:diagnose` enables query profiling plus lightweight
sampling of pool pressure, active Postgres wait events, WAL/database deltas,
Node CPU, and event loop utilization. The profiled 16-worker,
`physicalPartitions=4` row measured 3,677 processing activations/sec with 4.3
processing SQL calls per activation. The sampler saw no pg pool waiters; the
remaining local contention showed up mostly as WAL sync/write and lightweight
lock wait samples.

The no-delay workload is mostly local DB/CPU-bound, so higher activation
concurrency does not necessarily improve that particular throughput row. The
concurrency path is still useful for workers with long in-flight async
activations because one blocked activation no longer occupies the entire worker.

`synchronous=full` is SQLite's conservative default. `synchronous=normal` is
available for deployments that accept SQLite's weaker crash window in exchange
for higher write throughput. The Postgres rows are from local Docker on the same
machine, so they include client/server and container overhead.

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
