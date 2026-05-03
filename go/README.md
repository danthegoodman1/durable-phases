# Durable Phases Go

This module is the Go SDK/runtime for durable-phases. It is intentionally shard-native: execution goes through the shared in-memory shard engine and durable providers replay append-only shard journals into warm shard projections.

## Packages

- `durable`: workflow/runtime API, transitions, waits, activities, child workflow handles, provider interfaces, and observability hooks.
- `durable/internal/shardengine`: shared shard engine used by tests and durable append-store providers.
- `durable/providers/sqlite`: single-file SQLite provider and shard-file SQLite provider.
- `durable/providers/postgres`: Postgres append-store provider.
- `durable/cmd/durable-gen`: `go:generate` annotation generator.

There is no Go JSON durability provider. SQLite and Postgres are the durable providers.

## Generate

Add a directive to a normal Go source file:

```go
//go:generate go run github.com/danthegoodman1/durable-phases/go/cmd/durable-gen
```

Annotate ordinary declarations:

```go
//durable:workflow name=example version=1 input=Input output=Output common=Common
type ExampleWorkflow struct{}

//durable:phase name=boot run state=Boot
func (ExampleWorkflow) Boot() {}

//durable:timer name=deadline at=DeadlineAt
func (WaitingHandlers) Deadline() {}
```

Then run:

```sh
go generate ./...
```

See `examples/generated` for a generated workflow-contract package covering phases, signals, timers, children, queries, and migrations.

## Verify

```sh
go generate ./...
go test ./...
go run ./cmd/durable-demo immediate-and-signal
go run ./cmd/durable-bench --provider sqlite --mode mixed --workflows 100 --json
```

Postgres tests and benchmarks run when `DURABLE_POSTGRES_URL` is set.

## Conformance

Reusable provider conformance lives in `testing/conformance`. The in-tree suite
runs it against:

- shared memory shard engine
- SQLite single-file append store
- SQLite shard-file append store
- Postgres append store when `DURABLE_POSTGRES_URL` is set

The suite covers lifecycle/conflicts, ordered shard claims, lean loads, shard
lease fencing, activation reclaim, signal/timer/migration/child readiness,
non-destructive checkpoint conflicts, eager activity memoization/retry/timeout,
child conflict policies, and parent-close behavior.

## Local Parity Numbers

Latest local TS/Rust/Go smoke:

```sh
npm run benchmark:full-parity -- --provider all --mode all --workflows 20 --workers 2 --shards 2 --repeat 1 --physical-partitions 2 --json
```

For the Go providers, every mode reported `correct=true` and these processing
workflow rates:

| Provider | mixed | bare | activity | signal | timer | child |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite single-file | 285.84 | 1,294.08 | 1,464.57 | 783.89 | 1,504.24 | 555.82 |
| SQLite shard-file | 467.55 | 1,550.33 | 2,159.85 | 1,231.39 | 2,095.03 | 901.06 |
| Postgres | 141.99 | 491.82 | 588.19 | 331.44 | 536.52 | 265.76 |
