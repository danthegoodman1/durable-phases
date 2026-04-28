# Durable Execution PoC

A small TypeScript prototype of the phase-based durable execution model in
[`SPEC.md`](SPEC.md). It uses a primitive JSON-file durability provider so the
important behavior is easy to inspect: current snapshot, durable waits, signal
inbox, child records, and activation-scoped effects.

```bash
npm run demo
npm test
```

The hello-world demo in [`src/hello-world.ts`](src/hello-world.ts) shows:

- immediate `run` phases
- timer waits that survive runtime reconstruction
- signal delivery and atomic consumption
- `stay()` and `checkpoint()` as committed transitions
- the bounded unbound-loop pattern
- a tiny local child workflow

This is intentionally not production-ready. It does not implement migrations,
dispatch shards, leases, retries, heartbeats, `.any(...)`, remote children, or
multi-writer storage.
