# Durable Phase Workflow — concise implementation spec

## 1. Goal

Provide durable execution with:

```text
workflow-like authoring
+ state-machine safety
+ bounded replay
+ typed child workflows
+ first-class checkpoints
+ explicit code/version migration
```

The runtime owns the durable loop. User code only defines phases, waits, handlers, and transitions.

This system does **not** snapshot the process stack or rely on unbounded event-history replay.

Implementations should validate data at trust boundaries. Public workflow
inputs, signal payloads, child inputs, query outputs, migrations, and handler
transition outputs are external/user-code boundaries and should be parsed with
their schemas. Provider-loaded snapshots and claimed event payloads are trusted
durable JSON and do not need to be reparsed on every activation, though runtimes
must still preserve handler isolation so mutating `common` or `data` directly
does not persist without an explicit transition.

---

## 2. Core model

A workflow instance is always one of:

```ts
type InstanceStatus<Output> =
  | { status: "running"; common: unknown; phase: PhaseSnapshot }
  | { status: "completed"; output: Output }
  | { status: "canceled"; reason: string }
  | { status: "failed"; error: SerializedError }
```

A running instance has:

```ts
type PhaseSnapshot = {
  name: string
  data: JsonObject
}
```

The durable state is therefore:

```text
common workflow metadata
+ current phase name
+ current phase data
```

Terminal states are runtime statuses, not user-defined phases.

---

## 3. User-facing API

```ts
const Workflow = defineWorkflow({
  name: "WorkflowName",
  version: 1,

  input: InputSchema,
  output: OutputSchema,
  common: CommonSchema, // optional; defaults to an empty object schema

  initial(input) {
    return start({
      common: { ... }, // optional when the workflow has no common state
      phase: "some_phase",
      data: { ... },
    })
  },

  phases: {
    some_phase: phase({
      state: SomePhaseSchema,

      on: {
        event_name: signal(EventSchema, handler),
        timer_name: timer(selector, handler),
        child_name: child(selector, handler),
      },
    }),

    immediate_phase: phase({
      state: ImmediatePhaseSchema,

      run: async handler,
    }),
  },

  on: {
    global_cancel: signal(CancelSchema, handler),
  },

  queries: {
    summary: query(SummarySchema, ({ snapshot, sequence }) => ({
      sequence,
      status: snapshot.status,
    })),
  },

  migrations: {
    1: migrateV1ToV2,
    2: migrateV2ToV3,
  },
})
```

A phase has exactly one mode:

```ts
phase({ state?, on })
```

or:

```ts
phase({ state?, run })
```

If `state` is omitted, it is treated as an empty object schema.

If workflow `common` is omitted, it is treated as an empty object schema, and `start(...)` may omit `common`.

`on` phases wait for external/durable events.

`run` phases execute immediately when entered and must return a transition.

### Queries

Queries are typed, non-mutating reads of a single workflow instance's latest committed snapshot.

```ts
queries: {
  progress: query(ProgressSchema, ({ snapshot, sequence }) => {
    if (snapshot.status !== "running") {
      return {
        sequence,
        status: snapshot.status,
      }
    }

    return {
      sequence,
      status: snapshot.status,
      phase: snapshot.phase.name,
      progress: deriveProgress(snapshot.common, snapshot.phase),
    }
  }),
}
```

Query rules:

```text
queries read one committed snapshot at a specific sequence
queries may run concurrently with handler activations
queries never observe partial handler effects
queries never observe uncommitted transition output
queries do not create activations
queries do not consume signals
queries do not run activities or child workflows
queries do not checkpoint
```

If a handler is running from sequence `N`, a concurrent query may read sequence `N` while the handler is still in flight. If the handler commits sequence `N + 1` while the query runs, the query still returns data derived from whichever committed sequence it loaded.

Queries are for operational or typed point reads of one workflow instance. They are not a replacement for product read models, projections, search indexes, or list views. Workflows may update external read models through activities when clients need rich queryability.

---

## 4. Handler context

Every handler receives:

```ts
type HandlerArgs<Common, PhaseData, Event> = {
  ctx: DurableContext
  common: Common
  data: PhaseData
  event: Event
}
```

For `run` phases, there is no external event:

```ts
type RunArgs<Common, PhaseData> = {
  ctx: DurableContext
  common: Common
  data: PhaseData
}
```

Handlers return a transition command.

Query handlers receive:

```ts
type QueryArgs<Output> = {
  sequence: number
  snapshot: InstanceStatus<Output>
}
```

Query handlers do not receive `DurableContext`.

---

## 5. Transition commands

Handlers may only finish by returning one of:

```ts
stay(dataPatch?: Partial<CurrentPhaseData>)
go("next_phase", nextPhaseData)
complete(output)
cancel(reason)
fail(error)
```

Examples:

```ts
return stay({
  nextReminderAt: daysFromNow(7),
})
```

```ts
return go("waiting_for_signature", {
  contractId: contract.id,
  nextReminderAt: daysFromNow(3),
})
```

```ts
return complete({
  customerId,
  activatedAt: ctx.now(),
})
```

A transition is the checkpoint. The runtime validates and persists it atomically.

---

## 6. Durable waits

A wait is declared inside a phase.

### Signal wait

```ts
document_uploaded: signal(DocumentSchema, async ({ data, event }) => {
  const documents = addDocument(data.documents, event)

  if (hasAllRequiredDocuments(documents)) {
    return go("running_kyc", {
      documents: toCompleteDocumentSet(documents),
    })
  }

  return stay({ documents })
})
```

A signal is an external event sent to a workflow instance.

Signals are durable. The runtime accepts and persists every signal sent to a live workflow instance, even if the current phase is not listening for it yet.

```ts
type SignalRecord = {
  signalId: string
  workflowId: string
  runId: string
  type: string
  payload: JsonObject
  receivedAt: string
  consumedBySequence?: number
}
```

The signal inbox is logically unbounded for execution semantics.

The hot lookup must support the current instance plus:

```text
consumed
+ type
+ receivedAt
+ signalId
```

For example, a storage engine may index:

```text
workflowId, runId, consumed, type, receivedAt, signalId
```

When a phase starts listening, the runtime consumes the first unconsumed signal that matches the declared type and schema.

Signal consumption and the resulting checkpoint commit are atomic. If the handler fails or the checkpoint does not commit, the signal remains unconsumed.

### Timer wait

```ts
reminder_due: timer(
  ({ data }) => data.nextReminderAt,
  async ({ ctx, common, data }) => {
    await ctx.activity("send_reminder", () =>
      sendReminder(common.customerId)
    )

    return stay({
      nextReminderAt: daysFromNow(7),
    })
  }
)
```

The selector returns an ISO timestamp or `null`.

If it returns `null`, no timer is scheduled.

### Child wait

```ts
kyc_finished: child(
  ({ data }) => data.kyc,
  async ({ event }) => {
    if (!event.ok) {
      return cancel("KYC workflow failed")
    }

    if (event.output.status === "failed") {
      return cancel(event.output.reason)
    }

    return go("waiting_for_signature", {
      contractId: event.output.contractId,
      nextReminderAt: daysFromNow(3),
    })
  }
)
```

The child event shape is:

```ts
type ChildEvent<W> =
  | { ok: true; output: OutputOf<W> }
  | { ok: false; error: SerializedError }
```

### Wait ordering

A phase's `on` map is a durable race across its declared signal, timer, and child waits.

For example:

```ts
on: {
  document_uploaded: signal(DocumentSchema, async ({ data, event }) => {
    return stay({ documents: addDocument(data.documents, event) })
  }),

  customer_canceled: signal(CancelSchema, async ({ event }) => {
    return cancel(event.reason)
  }),

  reminder_due: timer(
    ({ data }) => data.nextReminderAt,
    async () => {
      return stay({ nextReminderAt: daysFromNow(7) })
    }
  ),
}
```

Each wait has its own handler. The runtime still consumes only the wait that wins the race.

When more than one wait is ready, the runtime chooses the earliest ready event by a canonical ordering key.

For signals, `receivedAt` is assigned by the runtime when the signal is accepted into the durable inbox.

For timers, the comparable time is the scheduled fire time.

For child completions, the comparable time is the runtime-recorded completion time.

If two events have the same time, the runtime sorts the full ordering tuple lexicographically:

```text
time
+ wait kind
+ wait name
+ durable event id
```

This makes phase wait selection deterministic even when two external events have the same timestamp.

---

## 7. Durable context

```ts
type DurableContext = {
  now(): string

  activity<T>(
    key: string,
    fn: () => Promise<T>,
    options?: ActivityOptions
  ): Promise<T>

  child: {
    start<W extends WorkflowDefinition<any, any>>(
      key: string,
      workflow: W,
      input: InputOf<W>,
      options?: ChildOptions
    ): Promise<ChildHandle<W>>

    cancel(handle: ChildHandle<any>): Promise<void>
  }
}
```

All external side effects must go through `ctx.activity` or child workflows.

Do **not** allow arbitrary side effects in handlers outside runtime APIs.

---

## 8. Activities

Activities represent durable side effects.

```ts
const contract = await ctx.activity("create_contract", () =>
  createContract(common.customerId)
)
```

Activity semantics:

```text
key is stable within the current handler activation
completed activity results are memoized
pending activities are persisted
failed activities may retry according to policy
completed activities are not re-executed after their result is committed
```

A handler activation is one invocation of a `signal`, `timer`, `child`, or `run` handler from a specific checkpoint sequence.

For `on` phases, the activation is scoped to the selected durable event:

```text
workflowId
+ runId
+ starting sequence
+ selected signal/timer/child event id
+ user effect key
```

For `run` phases, the activation is scoped to entering the run phase at a specific checkpoint sequence.

Any returned transition, including `stay(...)`, commits a new checkpoint and ends the current activation. Future events in the same phase get a fresh effect namespace.

Activities are still at-least-once at the external-system boundary. The runtime must provide a stable idempotency key to the activity, and integrations should use it.

Activity options:

```ts
type ActivityOptions = {
  startToCloseTimeout?: Duration
  heartbeatTimeout?: Duration
  retry?: RetryPolicy
}
```

Long-running activities may heartbeat through the runtime:

```ts
await ctx.activity(
  "transcode_video",
  async ({ heartbeat }) => {
    for await (const progress of transcodeVideo()) {
      await heartbeat({ frame: progress.frame })
    }
  },
  {
    heartbeatTimeout: minutes(1),
  }
)
```

Heartbeats are durable effect metadata. They do not checkpoint workflow phase state and they do not make partial activity results visible to workflow code.

If a worker dies during a long-running activity, the provider detects heartbeat timeout and makes the activity eligible for retry according to policy. The retry uses the same effect key and idempotency key.

Heartbeat details are for activity retry/resume logic and observability. Activity code may use the last heartbeat details to resume external work when the runtime supports it, but workflow determinism must not depend on heartbeat details.

### Bounded activation rule

Execution history is bounded at checkpoint boundaries, not inside a single handler activation.

A handler should perform a small, bounded number of durable effects before returning a transition. If work may grow without a small bound, process one bounded chunk, store progress in phase data, and return `stay(...)` to checkpoint.

`stay(...)` only checkpoints after the handler returns. It does not checkpoint in the middle of a loop.

Example:

```ts
process_batch: phase({
  state: z.object({
    cursor: z.string().nullable(),
    processedCount: z.number(),
  }),

  run: async ({ ctx, data }) => {
    const batch = await ctx.activity("load_batch", () =>
      loadBatch(data.cursor, { limit: 100 })
    )

    for (const item of batch.items) {
      await ctx.activity(`process_${item.id}`, () =>
        processItem(item)
      )
    }

    if (batch.nextCursor) {
      return stay({
        cursor: batch.nextCursor,
        processedCount: data.processedCount + batch.items.length,
      })
    }

    return go("done", {
      processedCount: data.processedCount + batch.items.length,
    })
  },
})
```

The next activation starts from the new checkpoint and gets a fresh effect namespace.

---

## 9. Child workflows

A child workflow must be a typed local workflow definition.

Do this:

```ts
const handle = await ctx.child.start("kyc", KycWorkflow, {
  customerId: common.customerId,
  documents: data.documents,
})
```

Not this:

```ts
ctx.child.start("KycWorkflow", input)
```

The workflow object carries both compile-time and runtime information:

```ts
type WorkflowDefinition<Input, Output> = {
  name: string
  version: number
  input: Schema<Input>
  output: Schema<Output>
}
```

A handle is typed:

```ts
type ChildHandle<W> = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string
  __type?: W
}
```

Use a phase-level `child(...)` wait to receive the typed child completion event:

```ts
kyc_finished: child(
  ({ data }) => data.kyc,
  async ({ event }) => {
    if (!event.ok) {
      return cancel("KYC workflow failed")
    }

    return complete(event.output)
  }
)
```

Child options:

```ts
type ChildOptions = {
  workflowId?: string
  parentClosePolicy?: "cancel" | "abandon"
  conflictPolicy?: "use_existing" | "fail" | "terminate_existing"
}
```

Default:

```ts
{
  parentClosePolicy: "cancel",
  conflictPolicy: "use_existing"
}
```

`ctx.child.cancel(handle)` cancels a running local child workflow. If the child is already terminal, cancellation is idempotent and does not overwrite the child's terminal state.

All child workflows must be registered locally:

```ts
registerWorkflows([
  OnboardingWorkflow,
  KycWorkflow,
  SignatureWorkflow,
])
```

---

## 10. Runtime execution algorithm

### Starting an instance

```text
validate input
call initial(input)
validate common
validate initial phase data
persist instance as running
schedule waits or run immediate phase
```

### Running an `on` phase

```text
load instance
validate/migrate state
compute wait set from phase declarations
persist wait set
sleep
```

The wait set is durable runtime state. User code only declares waits; the runtime is responsible for persisting, scheduling, and reconciling them.

After any committed checkpoint, the runtime must be able to reconstruct or materialize all current waits from:

```text
workflow definition
+ current snapshot
+ current sequence
```

When a signal arrives:

```text
validate envelope
append to durable signal inbox
wake any instances with matching current waits
```

When an unconsumed signal, timer, or child result is ready:

```text
load instance
verify event matches current wait set or global handler
select the earliest ready event by the wait ordering rules
invoke matching handler
execute durable activities/children through effect ledger
receive transition command
validate new state/output
persist checkpoint atomically with signal consumption, if any
increment sequence
compact the completed activation's effect ledger
schedule next phase
```

### Running a `run` phase

```text
load instance
validate/migrate state
invoke run handler
execute durable activities/children through effect ledger
receive transition command
validate new state/output
persist checkpoint atomically
increment sequence
compact the completed activation's effect ledger
schedule next phase
```

If a workflow needs to wait for a child result, it should transition to an `on` phase with a durable `child(...)` wait. Already-completed effects return memoized results when a handler activation is retried.

Execution history is bounded at checkpoint boundaries. A committed transition means the previous activation's effects are no longer required for correctness, though they may be copied to audit history.

Long loops inside one handler can still create a large current effect ledger. See the bounded activation rule for the chunking pattern.

---

## 11. Persistence model

Each workflow instance stores:

```ts
type PersistedInstance = {
  workflowName: string
  workflowVersion: number
  workflowId: string
  runId: string

  sequence: number

  status:
    | "running"
    | "completed"
    | "canceled"
    | "failed"

  common?: JsonObject

  phase?: {
    name: string
    data: JsonObject
  }

  output?: JsonObject
  error?: SerializedError
  cancelReason?: string

  waits: DurableWait[]
  effects: EffectRecord[]
  children: ChildRecord[]

  createdAt: string
  updatedAt: string
}
```

Signals are stored as separate durable inbox records, not necessarily embedded in the instance row. The inbox is part of execution state, not audit history.

`sequence` increments at every checkpoint.

The runtime may retain full audit history separately, but execution must only require:

```text
current snapshot
+ current wait set
+ unconsumed signal inbox
+ current effect ledger
+ child handles
```

Old history is not required for correctness.

---

## 12. Durability provider contract

The durability provider is the runtime-facing storage boundary.

It does not run user code, validate schemas, or understand workflow definitions. It stores durable workflow state and provides the atomic operations the runtime needs for correctness.

Single-instance queries use the provider's committed snapshot reads. They do not claim activations or participate in checkpoint commits.

Conceptually:

```ts
type DurabilityProvider = {
  createInstance(input: CreateInstanceInput): Promise<CreateInstanceResult>
  loadInstance(ref: InstanceRef, options?: LoadInstanceOptions): Promise<PersistedInstance | null>

  appendSignal(input: AppendSignalInput): Promise<SignalRecord>

  claimDispatchShard?(input: ClaimDispatchShardInput): Promise<DispatchShardLease | null>
  heartbeatDispatchShard?(input: HeartbeatDispatchShardInput): Promise<void>
  releaseDispatchShard?(input: ReleaseDispatchShardInput): Promise<void>

  claimReadyActivations(input: ClaimReadyActivationsInput): Promise<ClaimReadyActivationsResult>
  claimReadyActivation(input: ClaimReadyActivationInput): Promise<ClaimReadyActivationResult>
  heartbeatActivations(input: HeartbeatActivationsInput): Promise<void>
  heartbeatActivation(input: HeartbeatActivationInput): Promise<void>
  releaseActivations(input: ReleaseActivationsInput): Promise<void>
  releaseActivation(input: ReleaseActivationInput): Promise<void>

  getOrReserveEffect(input: ReserveEffectInput): Promise<EffectReservation>
  heartbeatEffect(input: HeartbeatEffectInput): Promise<void>
  completeEffect(input: CompleteEffectInput): Promise<void>
  failEffect(input: FailEffectInput): Promise<FailEffectResult>

  commitCheckpoint(input: CommitCheckpointInput): Promise<CommitCheckpointResult>
}
```

### Operational observability

The TypeScript runtime and bundled SQLite/Postgres providers accept optional best-effort observability sinks:

```ts
type DurableLogger = {
  debug(event: string, fields?: Record<string, unknown>): void
  info(event: string, fields?: Record<string, unknown>): void
  warn(event: string, fields?: Record<string, unknown>): void
  error(event: string, fields?: Record<string, unknown>): void
}

type DurableMetrics = {
  counter(name: string, value?: number, tags?: MetricTags): void
  histogram(name: string, value: number, tags?: MetricTags): void
  gauge(name: string, value: number, tags?: MetricTags): void
}

type MetricTags = Record<string, string | number | boolean>
```

Logger and metrics failures must be swallowed by the runtime/provider and must not affect workflow execution.

Metrics use stable low-cardinality names and tags. Tags may include values such as `workerId`, `workflowName`, `activationKind`, `eventKind`, `status`, `reason`, and `shardId`. High-cardinality IDs such as `workflowId`, `runId`, `activationId`, `signalId`, child IDs, effect IDs, attempt IDs, and idempotency keys may appear in logs but must not be emitted as metric tags.

The core lifecycle should be observable:

```text
workflow start/signal/query
worker loop start/stop/sleep/error
drain start/end and next wake
shard claim/heartbeat/release
activation claim/reclaim/start/complete/conflict/failure/release
activity reserve/memoize/heartbeat/complete/fail/retry/timeout
checkpoint commit success/conflict
child start/cancel/parent-close cancel/abandon
```

### Instance creation

`createInstance(...)` must atomically:

```text
verify workflow id conflict policy
write initial instance snapshot
write initial wait set or enqueue initial run activation
initialize sequence to 0
```

If the requested workflow id already exists, the provider applies the requested conflict policy before creating or returning an instance.

### Snapshot reads

`loadInstance(...)` returns the latest committed instance snapshot visible to
the caller. By default this is a lean snapshot and does not include the effect
ledger. Debug and conformance callers that need effects request them explicitly:

```ts
type LoadInstanceOptions = {
  includeEffects?: boolean
}
```

The read must be consistent for one committed sequence. It may run concurrently with a claimed activation for the same instance and must not expose partial effect records, pending transition output, or uncommitted checkpoint writes.

The provider may offer stronger read modes, but the portable runtime contract only requires snapshot consistency for a committed sequence.

### Signal append

`appendSignal(...)` must:

```text
assign runtime receivedAt
assign stable signalId
persist the signal as unconsumed
wake matching current waits if possible
```

The wakeup is an optimization. Correctness only requires the durable inbox record; a reconciler may discover matching signals later.

### Partitioning and dispatch shards

The authoritative execution state for one workflow run should live in one provider partition.

The partition key is conceptually:

```text
tenantId
+ workflowId
+ runId
```

Co-locate the state needed for single-run execution:

```text
instance snapshot
current wait set
unconsumed signal inbox
current activation effects
child handles
activation claims
```

This lets `commitCheckpoint(...)` remain a single-partition compare-and-swap instead of a distributed transaction.

Global dispatch structures should be secondary indexes of pointers, not the authoritative state:

```text
due timer index
ready activation index
child completion index
signal wakeup index
```

Scalable providers should divide dispatch work into shards:

```text
dispatchShard = hash(tenantId, workflowId, runId) % shardCount
```

Workers lease dispatch shards and poll only the shards they own. Shard leases are short and heartbeated.

Long-running workers should retain owned shard leases across idle poll sleeps
and repeated drain cycles, heartbeating those leases until shutdown or fatal
error. One-shot drain calls may still claim and release shard leases per call.

TypeScript workers may optionally be configured with a fixed subset of dispatch shard IDs. This is useful for production deployments that assign shard ranges outside the runtime and for tests that need to prove multiple workers are actually committing activations from the same durable store.

Worker identity and local execution concurrency are separate concerns. `workerId`
is the durable lease owner recorded on shard leases, activation leases, and effect
attempt ownership. Activation concurrency is a local worker setting that controls
how many claimed activations that worker may execute at once.

TypeScript workers also have an activation prefetch limit. Prefetching controls
how many claimed activation leases the worker may hold ahead of execution, while
activation concurrency still controls how many handlers run at the same time.
Workers must heartbeat both queued and running activation leases and release
queued claims on shutdown, abort, or fatal handler error.

Activities currently execute inside activation slots. A long-running activity
therefore occupies one activation slot until it completes, fails, retries, or is
aborted. A later dedicated activity executor may add a separate activity
concurrency limit without changing the durable worker identity model.

```ts
type DispatchShardLease = {
  shardId: string
  ownerId: string
  leaseUntil: string
}
```

Shard leases assign polling responsibility, not correctness ownership. If a worker dies, the shard lease expires and another worker may poll that shard.

Small or embedded providers may omit explicit shard leasing and poll directly. They must still preserve the same activation-lease and checkpoint-CAS invariants.

The TypeScript SQLite provider uses WAL mode, foreign keys, `busy_timeout`, and
a conservative `synchronous: "full"` default. `synchronous: "normal"` may be
opted into when a deployment prefers higher write throughput and accepts
SQLite's weaker crash-recovery window for the most recent transactions.

The TypeScript Postgres provider uses a pooled `pg` client, explicit
transactions, row locks, `FOR UPDATE SKIP LOCKED` for concurrent claims,
conflict-aware upserts, JSONB payload/snapshot columns, partial indexes for hot
pending/ready paths, statement and lock timeouts, and an advisory transaction
lock for concurrent schema initialization. Provider startup must not rebuild or
rewrite live ready indexes; readiness is maintained transactionally by instance
creation, signal append, child completion/cancel, and checkpoint commits.

### Activation claiming

`claimReadyActivations(...)` returns up to `limit` ready handler activations plus
their current lean instance snapshots. `claimReadyActivation(...)` is a
compatibility wrapper over batch limit `1`.

Ready activations come from:

```text
ready signals in the durable inbox
due timers
completed children
run phases that should execute immediately
```

When more than one wait is ready for an instance, the provider and runtime must select the winner using the canonical wait ordering rules.

Claims are leased. If a worker crashes, another worker may claim the activation after the lease expires.
Providers should expose batch heartbeat/release helpers so a worker can manage
prefetched activation leases without issuing one provider call per queued claim.

In a sharded provider, claim calls should verify that the caller owns the dispatch shard for the requested work, or otherwise use an equivalent mechanism that prevents all workers from scanning all shards.

The instance snapshot returned with a claim is for activation execution, not
debug introspection. It includes identity, version, current sequence, status,
common state, phase/output/error, waits, parent link, and timestamps. It does
not need to include effect ledger records. Full `loadInstance(...)` reads may
include provider-specific debug details such as effects.

```ts
type ClaimReadyActivationsInput = ClaimReadyActivationInput & {
  limit: number
}

type ClaimedActivationWithInstance = {
  activation: ClaimedActivation
  instance: ActivationInstanceSnapshot
}

type ClaimReadyActivationsResult = {
  claims: ClaimedActivationWithInstance[]
  nextWakeAt?: string
}

type ClaimedActivation =
  | {
      kind: "event"
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      event: DurableEvent
      leaseUntil: string
    }
  | {
      kind: "run"
      activationId: string
      workflowId: string
      runId: string
      sequence: number
      leaseUntil: string
    }
```

The `activationId` is stable for the claimed instance sequence and selected event. Retries of the same activation use the same effect namespace.

### Effect reservation

`getOrReserveEffect(...)` scopes the user effect key to the current activation.

```ts
type EffectReservation<T = JsonValue> =
  | {
      status: "reserved"
      effectId: string
      idempotencyKey: string
      heartbeatDetails?: JsonValue
    }
  | {
      status: "completed"
      result: T
    }
  | {
      status: "failed"
      error: SerializedError
    }
```

If an effect is already completed for the activation and key, the provider returns the stored result and the runtime does not execute the effect again.

If the effect is new, the provider creates a pending effect record and returns a stable idempotency key. The runtime passes that key to the activity or child-start integration.

`heartbeatEffect(...)` records liveness and optional progress details for a running effect.

```ts
type HeartbeatEffectInput = {
  effectId: string
  details?: JsonValue
}
```

The provider stores the latest heartbeat time and details. If an effect has a `heartbeatTimeout` and no heartbeat arrives before it expires, the provider may mark the current attempt timed out and make the effect reservable again according to retry policy.

Heartbeat timeout does not consume a workflow signal, commit a workflow checkpoint, or change phase data.

`completeEffect(...)` persists the successful result for the reserved effect.

`failEffect(...)` records the failure and retry metadata. If retry policy allows another attempt, a later `getOrReserveEffect(...)` may return `reserved` again with the same idempotency key. If the retry policy is exhausted, it returns the terminal failure.

Activities are still at-least-once at the external boundary. If the external system succeeds but the provider does not record completion, the activity may be retried with the same idempotency key.

### Checkpoint commit

`commitCheckpoint(...)` is the primary atomic operation.

```ts
type CommitCheckpointInput = {
  workflowId: string
  runId: string
  expectedSequence: number
  activationId: string

  next: InstanceStatus<JsonObject>
  waits: DurableWait[]
  children: ChildRecord[]

  consumeSignalId?: string
}
```

It must atomically:

```text
verify instance is still at expectedSequence
verify activation lease is held or still valid
consume the selected signal, if any
write the new instance snapshot or terminal state
replace the current wait set
persist child handle updates
increment sequence
mark the activation complete
make the completed activation's effects compactable
enqueue or wake the next activation if the next phase is run-immediate
```

If `expectedSequence` does not match, the commit fails without partial writes. The runtime must discard the handler result, reload the instance, and retry from the winning durable state.

Signal consumption must be committed with the checkpoint. If the checkpoint fails, the signal remains unconsumed.

### Provider invariants

The provider must guarantee:

```text
per-instance sequence compare-and-swap
atomic checkpoint commit
atomic signal consumption with checkpoint commit
stable runtime ordering fields for signals, timers, and child completions
single-partition authoritative state per workflow run
secondary dispatch indexes that can be reconciled from authoritative state
optional leased dispatch shards for scalable polling
leased activation claims
effect memoization scoped by activation id + user effect key
durable heartbeat metadata for running effects
heartbeat timeout detection for retryable long-running effects
fresh effect namespace after every committed transition
recoverability from snapshot + waits + inbox + current effects + children
```

The provider may keep audit history, but execution must not require old activations after their checkpoint has committed.

---

## 13. Versioning and migration

Each persisted snapshot records the workflow version that last validated it.

An instance may advance to a newer workflow version only at a checkpoint boundary. If the current snapshot validates against the target schemas, no migration function is required. Otherwise the version change needs an explicit migration path.

The DX should stay smooth:

```text
no version branching inside handlers
no history replay migrations
no required migration when durable state shape did not change
```

A workflow version change may provide checkpoint migrations:

```ts
migrations: {
  1: migrate(({ common, phase }) => ({
    common,
    phase: migratePhaseV1ToV2(phase),
  })),

  2: migrate(({ common, phase }) => ({
    common: migrateCommonV2ToV3(common),
    phase,
  })),
}
```

The key is the source version. When migration is required, the runtime applies migrations in order until the snapshot reaches the worker's workflow version.

Migration rules:

```text
migrations run only at checkpoint boundaries
migrations transform common + phase snapshot
migrations must be deterministic
migrations must validate migrated state against the target schemas
pending activations stay pinned to their original workflow version
workers for old versions must remain available until pending activations drain
after migration, waits are recomputed from the migrated snapshot
inbox signals are not migrated unless the signal schema/type changed
effect records and child handles keep their original durable identity
```

This avoids whole-history replay problems while still permitting code upgrades.

---

## 14. Determinism rules

Handlers may be retried from the current checkpoint.

Therefore:

```text
no raw network calls in handlers
no raw database writes in handlers
no raw Date.now()
no raw Math.random()
no mutation outside returned transition
```

Use:

```ts
ctx.activity(...)
ctx.child.start(...)
ctx.child.cancel(...)
ctx.now()
```

The only durable mutation is the returned transition command.

---

## 15. Example

Companion translations:

- [Rust example](RUST_EXAMPLE.md)
- [Go example](GO_EXAMPLE.md)

### KYC child workflow

```ts
const KycWorkflow = defineWorkflow({
  name: "kyc",
  version: 1,

  input: KycInputSchema,
  output: KycOutputSchema,
  common: z.object({
    customerId: z.string(),
  }),

  initial(input) {
    return start({
      common: {
        customerId: input.customerId,
      },
      phase: "submitting",
      data: {
        documents: input.documents,
      },
    })
  },

  phases: {
    submitting: phase({
      state: z.object({
        documents: CompleteDocumentSetSchema,
      }),

      run: async ({ ctx, common, data }) => {
        const job = await ctx.activity("submit_kyc", () =>
          submitKyc(common.customerId, data.documents)
        )

        return go("waiting_for_provider", {
          providerJobId: job.id,
          nextPollAt: minutesFromNow(10),
        })
      },
    }),

    waiting_for_provider: phase({
      state: z.object({
        providerJobId: z.string(),
        nextPollAt: z.string(),
      }),

      on: {
        provider_webhook: signal(KycOutputSchema, async ({ event }) => {
          return complete(event)
        }),

        poll_due: timer(
          ({ data }) => data.nextPollAt,
          async ({ ctx, data }) => {
            const result = await ctx.activity("poll_kyc", () =>
              pollKycProvider(data.providerJobId)
            )

            if (result.done) {
              return complete(result.value)
            }

            return stay({
              nextPollAt: minutesFromNow(10),
            })
          }
        ),
      },
    }),
  },
})
```

### Parent onboarding workflow

```ts
const OnboardingWorkflow = defineWorkflow({
  name: "customer_onboarding",
  version: 1,

  input: z.object({
    customerId: z.string(),
  }),

  output: z.object({
    customerId: z.string(),
    activatedAt: z.string(),
  }),

  common: z.object({
    customerId: z.string(),
  }),

  initial(input) {
    return start({
      common: {
        customerId: input.customerId,
      },
      phase: "waiting_for_documents",
      data: {
        documents: emptyDocumentSet(),
        nextReminderAt: daysFromNow(7),
      },
    })
  },

  on: {
    customer_canceled: signal(
      z.object({ reason: z.string() }),
      async ({ event }) => {
        return cancel(event.reason)
      }
    ),
  },

  phases: {
    waiting_for_documents: phase({
      state: z.object({
        documents: PartialDocumentSetSchema,
        nextReminderAt: z.string(),
      }),

      on: {
        document_uploaded: signal(DocumentSchema, async ({ ctx, common, data, event }) => {
          const documents = addDocument(data.documents, event)

          if (!hasAllRequiredDocuments(documents)) {
            return stay({ documents })
          }

          const kyc = await ctx.child.start("kyc", KycWorkflow, {
            customerId: common.customerId,
            documents: toCompleteDocumentSet(documents),
          })

          return go("waiting_for_kyc", {
            kyc,
          })
        }),

        reminder_due: timer(
          ({ data }) => data.nextReminderAt,
          async ({ ctx, common }) => {
            await ctx.activity("send_document_reminder", () =>
              sendDocumentReminder(common.customerId)
            )

            return stay({
              nextReminderAt: daysFromNow(7),
            })
          }
        ),
      },
    }),

    waiting_for_kyc: phase({
      state: z.object({
        kyc: childHandle(KycWorkflow),
      }),

      on: {
        kyc_finished: child(
          ({ data }) => data.kyc,
          async ({ event }) => {
            if (!event.ok) {
              return cancel("KYC workflow failed")
            }

            if (event.output.status === "failed") {
              return cancel(event.output.reason)
            }

            return go("waiting_for_signature", {
              contractId: event.output.contractId,
              nextReminderAt: daysFromNow(3),
            })
          }
        ),
      },
    }),

    waiting_for_signature: phase({
      state: z.object({
        contractId: z.string(),
        nextReminderAt: z.string(),
      }),

      on: {
        contract_signed: signal(z.object({}), async ({ ctx, common, data }) => {
          await ctx.activity("activate_account", () =>
            activateAccount(common.customerId, data.contractId)
          )

          return complete({
            customerId: common.customerId,
            activatedAt: ctx.now(),
          })
        }),

        reminder_due: timer(
          ({ data }) => data.nextReminderAt,
          async ({ ctx, common, data }) => {
            await ctx.activity("send_signature_reminder", () =>
              sendSignatureReminder(common.customerId, data.contractId)
            )

            return stay({
              nextReminderAt: daysFromNow(3),
            })
          }
        ),
      },
    }),
  },
})
```

## 16. The final abstraction

The developer writes:

```text
typed phases
+ typed durable waits
+ typed transitions
+ typed child workflow calls
```

The runtime provides:

```text
checkpointing
effect memoization
timer scheduling
signal delivery
child orchestration
bounded replay
history compaction
version migration
```

The durable truth is always the latest validated snapshot, not an unbounded replay log and not a frozen process image.
