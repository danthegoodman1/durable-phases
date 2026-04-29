# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). The TypeScript runtime is backed by a SQLite durability
provider with shard leases, activation leases, atomic checkpoint commits,
durable ready indexes, durable waits, a signal inbox, child records, and
activation-scoped effects. `DurableRuntime.drain()` is useful for tests and
manual pumping; `DurableRuntime.runWorker()` adds bounded polling, wake hints,
and lease heartbeats for long-running workers.

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
heartbeat API, remote children, and tenant-aware partitioning are left for later
steps.
