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

The benchmarks run real workflows with activities, signals, timers, and child
completions. These are local sanity numbers, not a production guarantee.

```bash
npm run benchmark:sqlite -- --workflows 1000 --activation-concurrency 4 --activation-prefetch-limit 32 --json
npm run benchmark:sqlite -- --shard-files --workflows 1000 --workers 16 --shards 16 --activation-concurrency 4 --activation-prefetch-limit 32 --batch 32 --json
npm run benchmark:sqlite -- --mode signal --profile-queries --json
npm run benchmark:postgres -- --workflows 1000 --workers 16 --shards 16 --pool-size 64 --activation-concurrency 4 --activation-prefetch-limit 32 --batch 32 --physical-partitions 4 --json
npm run benchmark:postgres -- --profile-queries --json
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
activity delay. SQLite uses file-backed WAL/FULL durability:

| Provider | workload | workers/shards | activation concurrency | prefetch / drain batch | e2e activations/sec | processing activations/sec | processing mixed actions/sec |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Null in-memory | mixed, 1k workflows | 4/4 | 4 | 32 / 32 | 14,230 | 16,406 | 26,249 |
| Null in-memory | bare, 10k workflows | 4/4 | 4 | 32 / 32 | 4,558 | 15,232 | 15,232 |
| SQLite WAL/FULL single file | mixed, 1k workflows | 4/4 | 4 | 32 / 32 | 2,253 | 2,472 | 3,955 |
| SQLite WAL/FULL single file | mixed, 1k workflows | 16/16 | 4 | 32 / 32 | 5,475 | 7,018 | 11,229 |
| SQLite WAL/FULL shard files | mixed, 1k workflows | 4/4 | 4 | 32 / 32 | 2,200 | 2,488 | 3,980 |
| SQLite WAL/FULL shard files | mixed, 1k workflows | 16/16 | 4 | 32 / 32 | 4,329 | 7,122 | 11,395 |

SQLite profiling on the 100-workflow mixed workload reported about `1.15`
processing SQL statements per activation. The processing hot path was journal
catch-up, journal append, and shard lease updates; it did not use SQL joins or
ready-table scans.

Postgres remains on the existing shard-session SQL provider path until the next
gated milestone ports it to the same append-store contract. Use the Postgres
commands above to measure your local Docker setup, but the table intentionally
does not claim a Postgres gain from this SQLite/null architecture pass.

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
