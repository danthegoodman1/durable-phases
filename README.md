# Durable Phases

Durable Phases is a TypeScript runtime for phase-based durable execution. It
turns workflow phases, durable waits, signals, timers, child workflows, and
activities into checkpointed state backed by a `DurabilityProvider`.

The runtime ships with production-shaped SQLite and Postgres providers. The
provider boundary is now shard-native: the runtime owns a dispatch shard, opens
a shard session, claims shard-local tasks, executes activations, and commits
append-friendly state mutations back to that same shard. Normal checkpoint-local
work is fenced by shard ownership; eager heartbeat/timeout activities still keep
their own durable attempt fencing.

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
upserts, shard-epoch task ownership, partial hot-path indexes, and an advisory
transaction lock for concurrent schema initialization. Current projections live
in `workflow_state`, compact execution records append to `workflow_history`, and
live `tasks` rows carry execution snapshots so shard-local claims do not need a
follow-up state read on the normal hot path. Cross-shard child workflow work is
represented with idempotent `outbox`/`inbox` rows: checkpoint child starts,
child completions, and child cancellations are materialized by the owning
target shard rather than by direct cross-shard state writes.

`physicalPartitions` is fixed when a schema is created and is persisted in
provider metadata. Hot workflow/run tables are manually suffixed
(`workflow_state_p00`, `workflow_history_p00`, `tasks_p00`, `inbox_p00`,
`outbox_p00`, and so on), while dispatch shards remain logical worker leases.
The runtime API does not change; the provider routes each workflow/run to its
physical table set internally.

## Benchmarks

The SQLite and Postgres benchmarks run the same real workflows with activities,
signals, timers, and child completions. These are local sanity numbers, not a
production guarantee.

```bash
npm run benchmark:sqlite -- --workflows 1000 --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:postgres -- --workflows 1000 --workers 16 --shards 16 --pool-size 64 --activation-concurrency 4 --activation-prefetch-limit 32 --batch 32 --physical-partitions 4 --json
npm run benchmark:postgres -- --profile-queries --json
npm run benchmark:postgres:diagnose -- --physical-partitions 4 --workflows 1000 --workers 16 --shards 16 --pool-size 64 --json
npm run benchmark:null -- --workflows 1000 --mode mixed --json
npm run benchmark:null -- --workflows 10000 --mode bare --json
npm run benchmark:null:processes -- --processes 4 --workflows 4000 --mode mixed --json
```

The benchmark reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

`benchmark:null` uses a benchmark-only in-memory shard provider that is not
exported from the runtime package. It is useful for estimating TypeScript
runtime headroom without SQLite/Postgres IO, not for durability validation.
`benchmark:null:processes` launches multiple isolated null-provider subprocesses
and aggregates throughput to show whether the ceiling scales with Node process
count.

Measured on this workspace with 1,000 workflows and zero artificial activity
delay. SQLite uses file-backed WAL/FULL durability; Postgres uses
`synchronous_commit=on`:

| Provider | workers/shards | activation concurrency | prefetch / drain batch | physical partitions | pool | e2e workflows/sec | e2e activations/sec | processing activations/sec | processing mixed actions/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite WAL/FULL single file | 4/4 | 4 | 32 / 32 | n/a | n/a | 404 | 2,022 | 2,201 | 3,521 |
| SQLite WAL/FULL single file | 16/16 | 4 | 32 / 32 | n/a | n/a | 782 | 3,912 | 4,642 | 7,428 |
| Postgres Docker postgres:18.3 | 4/4 | 4 | 32 / 32 | 1 | 24 | 147 | 736 | 1,234 | 1,974 |
| Postgres Docker postgres:18.3 | 4/4 | 4 | 32 / 32 | 4 | 24 | 159 | 794 | 1,357 | 2,172 |
| Postgres Docker postgres:18.3 | 16/16 | 4 | 32 / 32 | 1 | 64 | 244 | 1,219 | 3,210 | 5,135 |
| Postgres Docker postgres:18.3 | 16/16 | 4 | 32 / 32 | 4 | 64 | 245 | 1,225 | 3,412 | 5,459 |
| Postgres Docker postgres:18.3 | 32/32 | 4 | 32 / 32 | 1 | 96 | 237 | 1,183 | 2,766 | 4,426 |
| Postgres Docker postgres:18.3 | 32/32 | 4 | 32 / 32 | 4 | 96 | 228 | 1,139 | 2,697 | 4,315 |

`npm run benchmark:postgres:diagnose` enables query profiling plus lightweight
sampling of pool pressure, active Postgres wait events, WAL/database deltas,
Node CPU, and event loop utilization.

A profiled run of the 16/16, 4-partition Postgres row measured 4.47
processing SQL calls per activation. Profiling adds overhead, so use the table
above for normal throughput comparisons.

The no-delay workload is mostly local DB/CPU-bound. Higher activation
concurrency can improve throughput when it gives the runtime enough completed
work to coalesce provider commits, but it eventually runs into the local
Postgres/container ceiling. It is also useful for workflows with long in-flight
async activations because one blocked activation no longer occupies the entire
worker.

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
