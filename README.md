# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). It uses a primitive JSON-file durability provider so the
important behavior is easy to inspect: current snapshot, durable waits, signal
inbox, child records, and activation-scoped effects.

```bash
npm run demo
npm test
```

The demos in [`src/demos/`](src/demos/) are intentionally small:

- immediate `run` phases plus signal delivery
- timer waits that survive runtime reconstruction plus `stay()`
- `checkpoint()` and the bounded unbound-loop pattern
- a tiny local child workflow

This is intentionally not production-ready. It does not implement migrations,
dispatch shards, leases, retries, heartbeats, remote children, or multi-writer
storage.
