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

The default SQLite provider is a single file configured for crash durability
with WAL mode, `synchronous=FULL`, foreign keys, and `busy_timeout`. `:memory:`
stores remain available for tests and local experiments, but they are not
crash-durable.

SQLite now uses the shared shard-owned append engine: the hot path keeps
workflow projections, task queues, timers, signals, child records, and eager
effect state in memory after shard recovery. Durable writes append fenced
mutation batches to `shard_journal`, with periodic `shard_snapshots` for bounded
startup replay. SQLite is no longer used as the task scheduler; processing SQL
is append/replay plus shard lease writes, not joins over ready/effect tables.

`SqliteShardFileDurabilityProvider` maps each dispatch shard to its own
WAL/FULL SQLite append store. It is useful for local scaling experiments where
separate workers or processes own disjoint shard ranges. Local child workflow
IDs are shard-affine by default; explicit cross-shard local child IDs are still
rejected in this provider until a deliberate cross-file outbox/inbox handoff is
added.

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
`physicalPartitions`, and optional observability sinks. The provider uses the
same shared append/replay shard engine as SQLite: each logical shard has a
current in-memory projection, durable journal rows, and periodic snapshots.
Shard-owner mutations row-lock that shard's head, catch up from the journal,
apply the shared engine mutation, and append one fenced journal entry. Postgres
is no longer used as the task scheduler in this path.

`physicalPartitions` is fixed when a schema is created and is persisted in
provider metadata. Shard journals, shard heads, and snapshots are manually
suffixed (`shard_journal_p00`, `shard_heads_p00`, `shard_snapshots_p00`, and so
on), while dispatch shards remain logical worker leases. Local child workflow
IDs are shard-affine by default. Explicit cross-shard local child IDs are
rejected in this provider until distributed child placement is reintroduced
deliberately.

## Benchmarks

The benchmarks run real workflows with activities, signals, timers, and child
completions. These are local sanity numbers, not a production guarantee.

```bash
npm run benchmark:sqlite -- --workflows 1000 --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:sqlite -- --shard-files --workflows 1000 --workers 16 --shards 16 --activation-concurrency 4 --activation-prefetch-limit 32 --batch 32 --json
npm run benchmark:sqlite -- --mode signal --profile-queries --json
npm run benchmark:postgres -- --workflows 1000 --workers 16 --shards 16 --pool-size 64 --activation-concurrency 4 --activation-prefetch-limit 32 --batch 32 --physical-partitions 4 --json
npm run benchmark:postgres -- --profile-queries --json
npm run benchmark:postgres:processes -- --processes 4 --workflows 4000 --json
npm run benchmark:null -- --workflows 1000 --mode mixed --json
npm run benchmark:null -- --workflows 10000 --mode bare --json
npm run benchmark:null:processes -- --processes 4 --workflows 4000 --mode mixed --json
```

The SQLite, Postgres, and null benchmarks support workload modes
`mixed|bare|activity|signal|timer|child`. `mixed` is the headline workload.
The benchmark reports setup, processing, and verification time separately.
Processing throughput excludes one-time workflow creation, final debug-store
verification, and result loading.

`benchmark:null` uses a benchmark-only in-memory shard provider that is not
exported from the runtime package. It is useful for estimating TypeScript
runtime headroom without SQLite/Postgres IO, not for durability validation.
`benchmark:null:processes` launches multiple isolated null-provider subprocesses
and aggregates throughput to show whether the ceiling scales with Node process
count.

Fresh rows from this pass, measured on this workspace with zero artificial
activity delay. SQLite uses file-backed WAL/FULL durability; Postgres uses the
append-store shard engine with `synchronous_commit=on`:

| Provider | workload | shape | activation concurrency | prefetch / drain batch | e2e activations/sec | processing activations/sec | processing mixed actions/sec |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Null in-memory | mixed, 1k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 15,010 | 17,421 | 27,873 |
| Null in-memory | bare, 10k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 4,659 | 15,940 | 15,940 |
| SQLite WAL/FULL single file | mixed, 1k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 2,268 | 2,489 | 3,983 |
| SQLite WAL/FULL shard files | mixed, 1k workflows | 16 workers / 16 shards | 4 | 32 / 32 | 4,165 | 7,474 | 11,958 |
| SQLite WAL/FULL shard files | mixed, 4k workflows | 4 processes / 16 shards | 4 | 32 / 32 | 6,130 | 10,128 | 16,205 |
| Postgres append store | mixed, 1k workflows | 4 workers / 4 shards / 1 partition | 4 | 32 / 32 | 827 | 1,762 | 2,820 |
| Postgres append store | mixed, 1k workflows | 16 workers / 16 shards / 1 partition | 4 | 32 / 32 | 1,184 | 5,674 | 9,078 |
| Postgres append store | mixed, 1k workflows | 16 workers / 16 shards / 4 partitions | 4 | 32 / 32 | 1,145 | 5,457 | 8,731 |
| Postgres append store | mixed, 1k workflows | 32 workers / 32 shards / 4 partitions | 4 | 32 / 32 | 1,223 | 5,990 | 9,585 |
| Postgres append store | mixed, 4k workflows | 4 processes / 16 shards / 4 partitions | 4 | 32 / 32 | 2,384 | 6,632 | 10,611 |

SQLite profiling on the 100-workflow mixed workload reported about `1.15`
processing SQL statements per activation. Postgres profiling on the 1k mixed
workload with 16 workers, 16 shards, and 4 physical partitions reported about
`2.96` processing SQL statements per activation. Both processing hot paths were
journal catch-up, journal append, snapshot/head maintenance, and shard lease
updates; neither used SQL task-discovery joins or ready-table scans.

Postgres uses the same append-store shard engine as SQLite. Use
`benchmark:postgres:processes` to measure multiple Node processes against one
shared Postgres schema with disjoint shard ranges.

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
