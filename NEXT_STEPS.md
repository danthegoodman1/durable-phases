# Next Steps: Shard-Local Children And High-Throughput Runtime Shape

This file captures the next architecture direction for the TypeScript runtime.
The goal is to keep the workflow authoring DX stable while making the execution
model honest about shard locality and ready for much higher throughput.

## Current Decision

The workflow shard should be the serialization point for a workflow run.

An `on:` event competes once it is durably visible on that workflow's shard.
Within that shard, events are ordered deterministically by the provider's
canonical ready-event key:

```text
visibility time on workflow shard
+ wait kind
+ wait name
+ durable event id
```

This avoids global occurrence-time ordering across shards. A remote child
completion does not beat a parent timer merely because it completed earlier on a
different shard; it joins the parent's race when the completion message is
materialized on the parent shard. If we ever need global occurrence-time
fairness, it should be an explicit slower mode with watermarks/barriers, not the
default high-throughput path.

## Step 1: Make Local Child Execution Shard-Local By Default

### Goal

Local child workflows should execute on the same shard as their parent unless
the caller explicitly opts into distributed/remote placement later.

This makes the common child fan-in path fast and intuitive:

```text
parent child record
child instance
child tasks
child completion
parent wakeup
```

all live on one shard. Child completions can then fairly compete with parent
timers and signals under shard-local visibility ordering.

### Proposed Semantics

Default child start:

```ts
await ctx.child.start("child", ChildWorkflow, input)
```

means:

```text
placement: "local"
```

Rules:

- Runtime-generated child workflow IDs are deterministic and shard-affine.
- Checkpoint-local child starts stay on the parent shard.
- If the caller supplies an explicit `workflowId` that hashes to the parent
  shard, it is allowed.
- If the caller supplies an explicit `workflowId` that hashes to another shard,
  reject by default with a clear error.
- A future explicit distributed mode may allow remote placement:

```ts
await ctx.child.start("child", ChildWorkflow, input, {
  placement: "distributed",
})
```

Distributed placement would use outbox/inbox delivery and visibility-time
semantics. It should not be the default.

### Implementation Tasks

- Add `placement?: "local" | "distributed"` to `ChildOptions`, but only support
  `"local"` for local TypeScript child workflows in the next pass unless we
  deliberately keep the option internal.
- Make runtime child ID generation shard-affine for default IDs.
- Enforce explicit child `workflowId` locality in the runtime before provider
  commit, not only in specific providers.
- Keep eager child starts local by default too.
- Update Postgres so local child starts use the same-shard fast path rather than
  outbox/inbox unless distributed placement is explicitly requested later.
- Keep SQLite shard-file behavior aligned: same-shard only, no cross-file child
  transaction.
- Update `SPEC.md` to say local child placement is the default and remote child
  placement is future/explicit.

### Tests

- Default child IDs hash to the parent shard for multiple shard counts.
- Default child execution completes without outbox/inbox on Postgres local path.
- Explicit same-shard child workflow ID succeeds.
- Explicit cross-shard child workflow ID fails clearly by default.
- Child completion competes with same-shard timers/signals by canonical ordering.
- Parent close cancel/abandon remains atomic for local children.
- Existing child conflict policies still work on the local fast path.

## Step 2: Stop Treating SQL Tables As The Execution Engine

### Problem

The current providers are shard-native in shape, but they still use SQL tables
as the execution engine. Hot-path commits and claims still validate by querying
and joining mutable relational tables:

```text
state table
+ task table
+ signal table
+ child table
+ shard lease table
+ effect/deadline tables
```

That is correct and production-shaped, but it is unlikely to reach
100k/sec or 1M/sec. The next wall is not just SQLite or Postgres; it is the
runtime/provider contract that requires storage to perform too much per
activation.

### Target Shape

Move toward shard-owned memory plus append/batch persistence:

```text
own shard lease
recover shard-local projection into memory
claim tasks from in-memory shard queue
execute activations
append compact mutation batch
update in-memory projection
persist/checkpoint projection deltas
enqueue shard-local and cross-shard messages
```

The database should persist ordered shard mutations and snapshots. It should not
be asked to rediscover and join the whole execution state for every activation.

### Provider Contract Direction

Replace provider methods that behave like global SQL operations with shard-log
operations.

Sketch:

```ts
type ShardDurabilitySession = {
  shardId: number
  epoch: number

  recoverShard(input): Promise<ShardSnapshot>
  appendMutationBatch(input: {
    epoch: number
    expectedShardSequence: number
    mutations: ShardMutation[]
  }): Promise<{ ok: true; shardSequence: number } | { ok: false; reason: string }>

  readInstance(ref): Promise<PersistedInstance | null>
  appendSignal(input): Promise<void>
  heartbeat(): Promise<void>
  release(): Promise<void>
}
```

Runtime-owned shard state would include:

- workflow current projections
- live task queues
- signal inbox state
- child records
- eager activity/effect attempts
- outbox/inbox messages
- timers and retry wakeups

Storage-owned durable state would include:

- append-only or append-mostly shard mutation log
- periodic compact shard snapshots
- durable outbox/inbox records
- optional debug/audit history

### Backend Fit

This shape can be backed by multiple stores:

- **RocksDB:** good single-node/shard-local diagnostic backend; useful for
  measuring append/batch ceiling, but replication/failover would still be ours.
- **FoundationDB:** strong fit for ordered key ranges, transactions, and
  shard-local mutation batches.
- **Cassandra/Scylla:** strong fit for append-heavy, partition-local writes if
  we design around LWTs sparingly and avoid global ordering.
- **Postgres/SQLite:** can remain supported, but should not be the benchmark for
  million/sec architecture limits unless used mainly as append/snapshot storage.

### Milestones

1. Define the shard mutation log format.
2. Build an in-memory shard executor behind the existing workflow DX.
3. Implement a null/in-memory append-log provider to measure runtime ceiling.
4. Implement a RocksDB or FoundationDB prototype to measure storage ceiling.
5. Port one production provider to the new append/batch contract.
6. Keep the current SQL providers as compatibility/correctness baselines until
   the new path passes conformance and benchmarks.

### Tests

- Shard recovery reconstructs current projections from snapshot plus mutations.
- Shard epoch takeover fences stale append batches.
- Per-workflow sequence CAS is enforced in memory and durably by append batch.
- Signals, timers, local children, parent close policies, and eager activity
  retries survive restart.
- Cross-shard messages are idempotent and visibility-ordered on the target shard.
- Benchmarks separate:
  - runtime-only ceiling
  - append-log provider ceiling
  - production durability provider ceiling

### Success Criteria

- Hot path has no SQL joins or relational rediscovery.
- Normal activation commit is one append/batch write per shard batch.
- Local child workflows stay shard-local by default.
- Cross-shard work is explicit message delivery with documented visibility-time
  semantics.
- Benchmarks show material movement beyond the current SQLite/Postgres ceilings,
  or clearly prove the remaining ceiling is the chosen storage backend.

## Open Questions

- Do we expose `placement` now, or keep it internal until distributed child
  placement exists?
- Which prototype backend comes first: RocksDB for fast local learning, or
  FoundationDB/Cassandra for the real distributed target?
- What snapshot cadence keeps recovery fast without bloating write amplification?
- How much debug/audit history belongs in the hot log versus an optional side
  stream?
