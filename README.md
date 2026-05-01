# Durable Phases

Durable Phases is a TypeScript runtime for phase-based durable execution. It
turns workflow phases, durable waits, signals, timers, child workflows, and
activities into checkpointed state backed by a `DurabilityProvider`.

The runtime ships with production-shaped SQLite and Postgres providers. Both
support shard leases, activation leases, atomic checkpoint commits, durable work
indexes, signal inboxes, child records, and activation-scoped effects.

## Quickstart

```bash
npm run demo
npm test
npm run build
```

The demos in [`src/demos/`](src/demos/) cover:

- immediate `run` phases plus signal delivery
- timer waits that survive runtime reconstruction plus `stay()`
- bounded unbound-loop processing with `stay()`
- a tiny local child workflow
- checkpoint-boundary workflow migration

Short inline TypeScript activities and local child starts default to checkpoint
durability, so they are persisted atomically with the workflow checkpoint.
Heartbeat or timeout activities use eager per-attempt durability. Remote
children, tenant-aware partitioning, and external activity workers are left for
later steps.

## SQLite

The SQLite provider is file-backed by default and is configured for crash
durability with WAL mode, `synchronous=FULL`, foreign keys, and `busy_timeout`.
`:memory:` stores remain available for tests and local experiments, but they are
not crash-durable.

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
npm run benchmark:sqlite -- --workflows 1000 --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:postgres -- --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:postgres -- --physical-partitions 4 --json
npm run benchmark:postgres -- --profile-queries --json
npm run benchmark:postgres:diagnose -- --physical-partitions 4 --workflows 1000 --workers 16 --shards 16 --pool-size 64 --json
```

The benchmark reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

Measured on this workspace with 1,000 workflows, zero artificial activity delay,
activation concurrency 4, activation prefetch 32, and batch 32. SQLite uses
file-backed WAL/FULL durability; Postgres uses `synchronous_commit=on`:

| Provider | workers/shards | physical partitions | pool | e2e workflows/sec | e2e activations/sec | processing activations/sec | processing mixed actions/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite WAL/FULL | 4/4 | n/a | n/a | 650 | 3,249 | 3,726 | 5,961 |
| SQLite WAL/FULL | 16/16 | n/a | n/a | 822 | 4,108 | 4,891 | 7,825 |
| Postgres Docker postgres:18.3 | 4/4 | 1 | 24 | 194 | 968 | 1,780 | 2,848 |
| Postgres Docker postgres:18.3 | 4/4 | 4 | 24 | 222 | 1,109 | 2,035 | 3,255 |
| Postgres Docker postgres:18.3 | 16/16 | 1 | 64 | 267 | 1,334 | 3,966 | 6,345 |
| Postgres Docker postgres:18.3 | 16/16 | 4 | 64 | 280 | 1,400 | 4,129 | 6,607 |
| Postgres Docker postgres:18.3 | 32/32 | 1 | 96 | 263 | 1,314 | 3,912 | 6,259 |
| Postgres Docker postgres:18.3 | 32/32 | 4 | 96 | 298 | 1,491 | 4,064 | 6,503 |

`npm run benchmark:postgres:diagnose` enables query profiling plus lightweight
sampling of pool pressure, active Postgres wait events, WAL/database deltas,
Node CPU, and event loop utilization.

The no-delay workload is mostly local DB/CPU-bound, so higher activation
concurrency does not necessarily improve that particular throughput row. The
concurrency path is still useful for workers with long in-flight async
activations because one blocked activation no longer occupies the entire worker.

The Postgres rows are from local Docker on the same machine, so they include
client/server and container overhead.

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
