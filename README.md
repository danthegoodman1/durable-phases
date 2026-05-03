# Durable Phases

Durable Phases is a TypeScript, Rust, and Go runtime for phase-based durable
execution. It turns workflow phases, durable waits, signals, timers, child
workflows, and activities into checkpointed state backed by a
`DurabilityProvider`.

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
cd go && go generate ./... && go test ./...
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

The TypeScript `PostgresDurabilityProvider.create(...)` accepts a connection
string or shared `pg.Pool`, schema name, pool size, statement/lock timeouts,
`physicalPartitions`, and optional observability sinks. The Rust
`PostgresDurabilityProvider` now opens a small `tokio-postgres` client pool per
provider, persists schema metadata, and uses the same append/replay shard
projection as SQLite. Postgres is no longer used as the task scheduler in this
path.

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
| Null in-memory | mixed, 1k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 14,951 | 17,343 | 27,748 |
| Null in-memory | bare, 10k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 4,675 | 16,772 | 16,772 |
| SQLite WAL/FULL single file | mixed, 1k workflows | 4 workers / 4 shards | 4 | 32 / 32 | 2,248 | 2,457 | 3,932 |
| SQLite WAL/FULL shard files | mixed, 1k workflows | 16 workers / 16 shards | 4 | 32 / 32 | 5,061 | 7,560 | 12,096 |
| SQLite WAL/FULL shard files | mixed, 4k workflows | 4 processes / 16 shards | 4 | 32 / 32 | 6,358 | 9,997 | 15,995 |
| Postgres append store | mixed, 1k workflows | 4 workers / 4 shards / 1 partition | 4 | 32 / 32 | 1,321 | 2,000 | 3,200 |
| Postgres append store | mixed, 1k workflows | 16 workers / 16 shards / 1 partition | 4 | 32 / 32 | 2,554 | 7,582 | 12,131 |
| Postgres append store | mixed, 1k workflows | 16 workers / 16 shards / 4 partitions | 4 | 32 / 32 | 2,518 | 7,329 | 11,726 |
| Postgres append store | mixed, 1k workflows | 32 workers / 32 shards / 4 partitions | 4 | 32 / 32 | 2,612 | 9,667 | 15,468 |
| Postgres append store | mixed, 4k workflows | 4 processes / 16 shards / 4 partitions | 4 | 32 / 32 | 4,073 | 8,882 | 14,212 |

SQLite profiling on the 100-workflow mixed workload reported about `1.09`
processing SQL statements per activation. Postgres profiling on the 1k mixed
workload with 16 workers, 16 shards, and 4 physical partitions reported about
`0.81` processing SQL statements per activation. Both processing hot paths were
journal catch-up, journal append, snapshot/head maintenance, and shard lease
updates; neither used SQL task-discovery joins or ready-table scans.

Postgres uses the same append-store shard engine as SQLite. Use
`benchmark:postgres:processes` to measure multiple Node processes against one
shared Postgres schema with disjoint shard ranges.

### Runtime parity

The Go runtime lives in [`go/`](go/). It has its own `go:generate` workflow
authoring path, reusable provider conformance tests, SQLite single-file,
SQLite shard-file, and Postgres append-store providers. The Go module does not
include a JSON/file durability provider. The Rust crate also no longer exposes
a JSON-file provider; examples and restart tests use the null, SQLite, SQLite
shard-file, or Postgres providers.

```bash
cd go
go generate ./...
go test ./...
go run ./cmd/durable-demo immediate-and-signal
go run ./cmd/durable-bench --provider sqlite --mode mixed --workflows 100 --json
cd ..
npm run benchmark:full-parity -- --provider all --mode all --workflows 20 --workers 2 --shards 2 --repeat 1 --physical-partitions 2 --json
```

Latest local TS/Rust/Go parity smoke, `20` workflows, `2` workers, `2` shards,
repeat `1`, with Postgres using local Docker and `physicalPartitions=2`.
Throughput columns are `processingWorkflowsPerSecond`; multiplier columns are
relative to TypeScript. All rows reported `correct=true`.

| Provider | Mode | TypeScript | Rust | Rust / TS | Go | Go / TS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Null | mixed | 982.67 | 7,062.36 | 7.19x | 5,450.46 | 5.55x |
| Null | bare | 1,861.48 | 45,091.66 | 24.22x | 21,786.49 | 11.70x |
| Null | activity | 1,658.97 | 36,204.53 | 21.82x | 30,458.79 | 18.36x |
| Null | signal | 1,113.80 | 31,356.12 | 28.15x | 22,174.99 | 19.91x |
| Null | timer | 1,659.93 | 34,297.96 | 20.66x | 25,028.66 | 15.08x |
| Null | child | 1,200.20 | 12,058.49 | 10.05x | 8,061.67 | 6.72x |
| SQLite single-file | mixed | 209.38 | 775.46 | 3.70x | 285.84 | 1.37x |
| SQLite single-file | bare | 661.01 | 1,723.12 | 2.61x | 1,294.08 | 1.96x |
| SQLite single-file | activity | 601.42 | 6,553.80 | 10.90x | 1,464.57 | 2.44x |
| SQLite single-file | signal | 668.65 | 3,053.94 | 4.57x | 783.89 | 1.17x |
| SQLite single-file | timer | 694.60 | 2,908.24 | 4.19x | 1,504.24 | 2.17x |
| SQLite single-file | child | 335.27 | 2,517.83 | 7.51x | 555.82 | 1.66x |
| SQLite shard-file | mixed | 209.58 | 1,181.41 | 5.64x | 467.55 | 2.23x |
| SQLite shard-file | bare | 688.15 | 4,882.96 | 7.10x | 1,550.33 | 2.25x |
| SQLite shard-file | activity | 588.23 | 1,537.34 | 2.61x | 2,159.85 | 3.67x |
| SQLite shard-file | signal | 690.36 | 4,307.95 | 6.24x | 1,231.39 | 1.78x |
| SQLite shard-file | timer | 689.08 | 5,732.37 | 8.32x | 2,095.03 | 3.04x |
| SQLite shard-file | child | 329.91 | 1,714.60 | 5.20x | 901.06 | 2.73x |
| Postgres | mixed | 176.41 | 720.99 | 4.09x | 141.99 | 0.80x |
| Postgres | bare | 541.28 | 2,441.69 | 4.51x | 491.82 | 0.91x |
| Postgres | activity | 531.10 | 2,263.58 | 4.26x | 588.19 | 1.11x |
| Postgres | signal | 535.72 | 2,602.77 | 4.86x | 331.44 | 0.62x |
| Postgres | timer | 534.73 | 2,292.68 | 4.29x | 536.52 | 1.00x |
| Postgres | child | 272.86 | 1,133.09 | 4.15x | 265.76 | 0.97x |

## Provider conformance

Rust providers should run `durable::testing::conformance`, which now covers
lifecycle, ordered batch claims, lean reads, activation lease fencing with
signal preservation, and eager effect retry/reclaim behavior for null, SQLite,
SQLite shard-file, and Postgres when `DURABLE_POSTGRES_URL` is set.

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

Go providers should run
[`testing/conformance.AssertProviderConformance`](go/testing/conformance/conformance.go).
The in-tree Go suite applies it to the shared memory shard engine, SQLite
single-file, SQLite shard-file, and Postgres when `DURABLE_POSTGRES_URL` is set:

```go
conformance.AssertProviderConformance(t, conformance.Factory{
  Name: "MyProvider",
  NewStore: func(t *testing.T) conformance.Store {
    return conformance.Store{
      New: func(t *testing.T) conformance.ProviderHandle {
        provider := newMyProvider(t)
        return conformance.ProviderHandle{Provider: provider, Close: provider.Close}
      },
    }
  },
})
```
