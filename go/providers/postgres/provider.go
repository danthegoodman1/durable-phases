package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSnapshotInterval = 512

type Options struct {
	ConnectionString   string
	Pool               *pgxpool.Pool
	Schema             string
	PhysicalPartitions int
	SnapshotInterval   int64
	StatementTimeout   time.Duration
	LockTimeout        time.Duration
}

type Provider struct {
	mu                 sync.Mutex
	pool               *pgxpool.Pool
	ownsPool           bool
	engine             *shardengine.Provider
	schema             string
	physicalPartitions int
	snapshotInterval   int64
	closed             bool
}

type journalRow struct {
	createdAt time.Time
	shardID   int
	entryID   int64
	raw       []byte
}

func New(ctx context.Context, options Options) (*Provider, error) {
	schema, err := normalizeSchema(options.Schema)
	if err != nil {
		return nil, err
	}
	partitions := options.PhysicalPartitions
	if partitions == 0 {
		partitions = 1
	}
	if partitions < 0 {
		return nil, fmt.Errorf("physical partitions must be positive")
	}
	pool := options.Pool
	ownsPool := false
	if pool == nil {
		conn := options.ConnectionString
		if conn == "" {
			conn = "postgresql://durable:durable@127.0.0.1:55432/durable"
		}
		config, err := pgxpool.ParseConfig(conn)
		if err != nil {
			return nil, err
		}
		configurePool(config, options)
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, err
		}
		ownsPool = true
	} else if options.StatementTimeout > 0 || options.LockTimeout > 0 {
		return nil, fmt.Errorf("statement and lock timeout options require a provider-owned pgx pool")
	}
	provider := &Provider{
		pool:               pool,
		ownsPool:           ownsPool,
		engine:             shardengine.New(),
		schema:             schema,
		physicalPartitions: partitions,
		snapshotInterval:   options.SnapshotInterval,
	}
	if provider.snapshotInterval <= 0 {
		provider.snapshotInterval = defaultSnapshotInterval
	}
	if err := provider.ensureSchema(ctx); err != nil {
		if ownsPool {
			pool.Close()
		}
		return nil, err
	}
	if err := provider.reload(ctx); err != nil {
		if ownsPool {
			pool.Close()
		}
		return nil, err
	}
	return provider, nil
}

func configurePool(config *pgxpool.Config, options Options) {
	previous := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if previous != nil {
			if err := previous(ctx, conn); err != nil {
				return err
			}
		}
		if options.StatementTimeout > 0 {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", options.StatementTimeout.Milliseconds())); err != nil {
				return err
			}
		}
		if options.LockTimeout > 0 {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET lock_timeout = %d", options.LockTimeout.Milliseconds())); err != nil {
				return err
			}
		}
		return nil
	}
}

func (p *Provider) ensureSchema(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(p.schema))); err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.provider_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
)
`, quoteIdent(p.schema))); err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.dispatch_shards (
  shard_id INTEGER PRIMARY KEY,
  owner_id TEXT,
  lease_until TIMESTAMPTZ,
  lease_epoch BIGINT NOT NULL DEFAULT 0
)
`, quoteIdent(p.schema))); err != nil {
		return err
	}
	if err := p.verifyMetadata(ctx, "postgres_storage_shape", "go_append_store_v3"); err != nil {
		return err
	}
	if err := p.verifyMetadata(ctx, "physical_partition_count", fmt.Sprint(p.physicalPartitions)); err != nil {
		return err
	}
	for partition := 0; partition < p.physicalPartitions; partition++ {
		if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER PRIMARY KEY,
  last_entry_id BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL
)
`, p.table("shard_heads", partition))); err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER NOT NULL,
  entry_id BIGINT NOT NULL,
  operation_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (shard_id, entry_id)
)
`, p.table("shard_journal", partition))); err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER PRIMARY KEY,
  last_entry_id BIGINT NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
)
`, p.table("shard_snapshots", partition))); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) verifyMetadata(ctx context.Context, key, expected string) error {
	table := fmt.Sprintf("%s.provider_metadata", quoteIdent(p.schema))
	var actual string
	err := p.pool.QueryRow(ctx, fmt.Sprintf(`SELECT value FROM %s WHERE key = $1`, table), key).Scan(&actual)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = p.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`, table), key, expected)
		return err
	}
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("PostgresDurabilityProvider metadata mismatch for %s: expected %s, found %s", key, expected, actual)
	}
	return nil
}

func (p *Provider) reload(ctx context.Context) error {
	var rows []journalRow
	for partition := 0; partition < p.physicalPartitions; partition++ {
		query := fmt.Sprintf(`SELECT shard_id, entry_id, operation_json, created_at FROM %s ORDER BY created_at, shard_id, entry_id`, p.table("shard_journal", partition))
		dbRows, err := p.pool.Query(ctx, query)
		if err != nil {
			return err
		}
		for dbRows.Next() {
			var item journalRow
			if err := dbRows.Scan(&item.shardID, &item.entryID, &item.raw, &item.createdAt); err != nil {
				dbRows.Close()
				return err
			}
			rows = append(rows, item)
		}
		dbRows.Close()
		if err := dbRows.Err(); err != nil {
			return err
		}
	}
	sortRows(rows)
	for _, item := range rows {
		var operation shardengine.JournalOperation
		if err := json.Unmarshal(item.raw, &operation); err != nil {
			return err
		}
		if err := shardengine.ApplyJournalOperation(ctx, p.engine, operation); err != nil {
			return err
		}
	}
	return nil
}

func mutate[T any](p *Provider, ctx context.Context, shardID int, operation shardengine.JournalOperation, apply func() (T, error), shouldAppend func(T) bool) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var zero T
	if p.closed {
		return zero, fmt.Errorf("postgres provider is closed")
	}
	out, err := apply()
	if err != nil {
		return out, err
	}
	if shouldAppend != nil && !shouldAppend(out) {
		return out, nil
	}
	return out, p.appendJournalLocked(ctx, shardID, operation)
}

func (p *Provider) mutateVoid(ctx context.Context, shardID int, operation shardengine.JournalOperation, apply func() error) error {
	_, err := mutate(p, ctx, shardID, operation, func() (struct{}, error) {
		return struct{}{}, apply()
	}, nil)
	return err
}

func (p *Provider) appendJournalLocked(ctx context.Context, shardID int, operation shardengine.JournalOperation) error {
	raw, err := json.Marshal(operation)
	if err != nil {
		return err
	}
	createdAt := time.Now().UTC()
	partition := p.partitionForShard(shardID)
	var entryID int64
	err = p.pool.QueryRow(ctx, fmt.Sprintf(`
WITH next_head AS (
  INSERT INTO %s AS h (shard_id, last_entry_id, updated_at)
  VALUES ($1, 1, $3)
  ON CONFLICT (shard_id) DO UPDATE
  SET last_entry_id = h.last_entry_id + 1,
      updated_at = $3
  RETURNING last_entry_id
)
INSERT INTO %s (shard_id, entry_id, operation_json, created_at)
SELECT $1, last_entry_id, $2, $3
FROM next_head
RETURNING entry_id
`, p.table("shard_heads", partition), p.table("shard_journal", partition)), shardID, string(raw), createdAt).Scan(&entryID)
	if err != nil {
		return err
	}
	if p.snapshotInterval > 0 && entryID%p.snapshotInterval == 0 {
		snapshotRaw, err := json.Marshal(p.engine.Snapshot())
		if err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (shard_id, last_entry_id, snapshot_json, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (shard_id) DO UPDATE SET
  last_entry_id = EXCLUDED.last_entry_id,
  snapshot_json = EXCLUDED.snapshot_json,
  created_at = EXCLUDED.created_at
`, p.table("shard_snapshots", partition)), shardID, entryID, string(snapshotRaw), createdAt); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) ClaimShard(ctx context.Context, input durable.ClaimDispatchShardInput) (*durable.ShardLease, error) {
	operation, err := shardengine.NewJournalOperation("claimShard", input)
	if err != nil {
		return nil, err
	}
	return mutate(p, ctx, input.ShardID, operation, func() (*durable.ShardLease, error) {
		return p.engine.ClaimShard(ctx, input)
	}, func(lease *durable.ShardLease) bool { return lease != nil })
}
func (p *Provider) OpenShard(input durable.OpenShardInput) durable.ShardDurabilitySession {
	return &session{provider: p, inner: p.engine.OpenShard(input)}
}
func (p *Provider) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.StartWorkflowResult, error) {
	operation, err := shardengine.NewJournalOperation("createInstance", input)
	if err != nil {
		return durable.StartWorkflowResult{}, err
	}
	return mutate(p, ctx, input.PartitionShard, operation, func() (durable.StartWorkflowResult, error) {
		return p.engine.CreateInstance(ctx, input)
	}, nil)
}
func (p *Provider) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	operation, err := shardengine.NewJournalOperation("createChildInstance", input)
	if err != nil {
		return durable.ChildHandleAny{}, err
	}
	return mutate(p, ctx, input.PartitionShard, operation, func() (durable.ChildHandleAny, error) {
		parent, _ := p.engine.LoadInstance(ctx, durable.InstanceRef{WorkflowID: input.ParentWorkflowID, RunID: input.ParentRunID}, durable.LoadInstanceOptions{})
		if parent != nil && input.PartitionShard != parent.PartitionShard {
			return durable.ChildHandleAny{}, fmt.Errorf("Postgres provider requires local child workflow starts to be shard-affine")
		}
		return p.engine.CreateChildInstance(ctx, input)
	}, nil)
}
func (p *Provider) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	operation, err := shardengine.NewJournalOperation("cancelChild", input)
	if err != nil {
		return err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.ParentWorkflowID, RunID: input.ParentRunID})
	return p.mutateVoid(ctx, shardID, operation, func() error {
		return p.engine.CancelChild(ctx, input)
	})
}
func (p *Provider) LoadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return p.engine.LoadInstance(ctx, ref, options)
}
func (p *Provider) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	operation, err := shardengine.NewJournalOperation("appendSignal", input)
	if err != nil {
		return durable.SignalRecord{}, err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID})
	return mutate(p, ctx, shardID, operation, func() (durable.SignalRecord, error) {
		return p.engine.AppendSignal(ctx, input)
	}, nil)
}
func (p *Provider) ClaimReadyActivations(ctx context.Context, shardIDs []int, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	operation, err := shardengine.NewJournalOperation("claimReadyActivations", shardengine.ClaimReadyActivationsOperationInput{ShardIDs: shardIDs, Input: input})
	if err != nil {
		return durable.ClaimShardTasksResult{}, err
	}
	shardID := 0
	if len(shardIDs) > 0 {
		shardID = shardIDs[0]
	}
	return mutate(p, ctx, shardID, operation, func() (durable.ClaimShardTasksResult, error) {
		return p.engine.ClaimReadyActivations(ctx, shardIDs, input)
	}, func(out durable.ClaimShardTasksResult) bool { return len(out.Claims) > 0 })
}
func (p *Provider) HeartbeatActivations(ctx context.Context, activationIDs []string, workerID string, now time.Time, lease time.Duration) error {
	operation, err := shardengine.NewJournalOperation("heartbeatActivations", shardengine.HeartbeatActivationsOperationInput{ActivationIDs: activationIDs, WorkerID: workerID, Now: now, Lease: lease})
	if err != nil {
		return err
	}
	return p.mutateVoid(ctx, p.shardForActivationIDs(activationIDs), operation, func() error {
		return p.engine.HeartbeatActivations(ctx, activationIDs, workerID, now, lease)
	})
}
func (p *Provider) ReleaseActivations(ctx context.Context, activationIDs []string, workerID string) error {
	operation, err := shardengine.NewJournalOperation("releaseActivations", shardengine.ReleaseActivationsOperationInput{ActivationIDs: activationIDs, WorkerID: workerID})
	if err != nil {
		return err
	}
	return p.mutateVoid(ctx, p.shardForActivationIDs(activationIDs), operation, func() error {
		return p.engine.ReleaseActivations(ctx, activationIDs, workerID)
	})
}
func (p *Provider) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	operation, err := shardengine.NewJournalOperation("getOrReserveEffect", input)
	if err != nil {
		return durable.EffectReservation{}, err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID})
	return mutate(p, ctx, shardID, operation, func() (durable.EffectReservation, error) {
		return p.engine.GetOrReserveEffect(ctx, input)
	}, func(out durable.EffectReservation) bool { return out.Status == "reserved" })
}
func (p *Provider) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	operation, err := shardengine.NewJournalOperation("heartbeatEffect", input)
	if err != nil {
		return err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID})
	return p.mutateVoid(ctx, shardID, operation, func() error {
		return p.engine.HeartbeatEffect(ctx, input)
	})
}
func (p *Provider) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	operation, err := shardengine.NewJournalOperation("completeEffect", input)
	if err != nil {
		return err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID})
	return p.mutateVoid(ctx, shardID, operation, func() error {
		return p.engine.CompleteEffect(ctx, input)
	})
}
func (p *Provider) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	operation, err := shardengine.NewJournalOperation("failEffect", input)
	if err != nil {
		return durable.FailEffectResult{}, err
	}
	shardID := p.shardForRef(durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID})
	return mutate(p, ctx, shardID, operation, func() (durable.FailEffectResult, error) {
		return p.engine.FailEffect(ctx, input)
	}, nil)
}
func (p *Provider) CommitActivations(ctx context.Context, inputs []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	operation, err := shardengine.NewJournalOperation("commitActivations", inputs)
	if err != nil {
		return durable.CommitActivationsResult{}, err
	}
	shardID := 0
	if len(inputs) > 0 {
		shardID = p.shardForRef(durable.InstanceRef{WorkflowID: inputs[0].WorkflowID, RunID: inputs[0].RunID})
	}
	return mutate(p, ctx, shardID, operation, func() (durable.CommitActivationsResult, error) {
		for _, input := range inputs {
			parent, _ := p.engine.LoadInstance(ctx, durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID}, durable.LoadInstanceOptions{})
			if parent != nil {
				for _, child := range input.ChildStarts {
					if child.PartitionShard != parent.PartitionShard {
						f := false
						return durable.CommitActivationsResult{Results: []durable.CommitCheckpointResult{{OK: false, Sequence: input.ExpectedSequence, Reason: "cross_shard_child_start", Retryable: &f, Error: durable.SerializedError{Message: "Postgres provider requires commit-local children to stay on the parent shard"}, ActivationID: input.ActivationID}}}, nil
					}
				}
			}
		}
		return p.engine.CommitActivations(ctx, inputs)
	}, func(out durable.CommitActivationsResult) bool {
		for _, result := range out.Results {
			if result.OK {
				return true
			}
		}
		return false
	})
}
func (p *Provider) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	out, err := p.CommitActivations(ctx, []durable.CommitCheckpointInput{input})
	if err != nil {
		return durable.CommitCheckpointResult{}, err
	}
	if len(out.Results) == 0 {
		return durable.CommitCheckpointResult{OK: false, Sequence: -1, Reason: "missing_commit_result"}, nil
	}
	result := out.Results[0]
	result.ActivationID = ""
	return result, nil
}
func (p *Provider) RecordActivationFailures(ctx context.Context, inputs []durable.RecordActivationFailureInput) error {
	if len(inputs) == 0 {
		return nil
	}
	operation, err := shardengine.NewJournalOperation("recordActivationFailures", inputs)
	if err != nil {
		return err
	}
	return p.mutateVoid(ctx, p.shardForActivation(inputs[0].ActivationID), operation, func() error {
		return p.engine.RecordActivationFailures(ctx, inputs)
	})
}
func (p *Provider) ListInstances(ctx context.Context, options durable.LoadInstanceOptions) ([]durable.PersistedInstance, error) {
	return p.engine.ListInstances(ctx, options)
}
func (p *Provider) GetWorkflowRuns(ctx context.Context, input durable.GetWorkflowRunsInput) (durable.GetWorkflowRunsResult, error) {
	return p.engine.GetWorkflowRuns(ctx, input)
}
func (p *Provider) ListSignals(ctx context.Context) ([]durable.SignalRecord, error) {
	return p.engine.ListSignals(ctx)
}
func (p *Provider) ListChildren(ctx context.Context) ([]durable.ChildRecord, error) {
	return p.engine.ListChildren(ctx)
}
func (p *Provider) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.ownsPool {
		p.pool.Close()
	}
	return nil
}

type session struct {
	provider *Provider
	inner    durable.ShardDurabilitySession
}

func (s *session) ShardID() int      { return s.inner.ShardID() }
func (s *session) OwnerID() string   { return s.inner.OwnerID() }
func (s *session) LeaseEpoch() int64 { return s.inner.LeaseEpoch() }
func (s *session) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.StartWorkflowResult, error) {
	return s.provider.CreateInstance(ctx, input)
}
func (s *session) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	return s.provider.CreateChildInstance(ctx, input)
}
func (s *session) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	return s.provider.CancelChild(ctx, input)
}
func (s *session) ReadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return s.inner.ReadInstance(ctx, ref, options)
}
func (s *session) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	return s.provider.AppendSignal(ctx, input)
}
func (s *session) ClaimTasks(ctx context.Context, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	operation, err := shardengine.NewSessionJournalOperation("claimShardTasks", s.openInput(), input)
	if err != nil {
		return durable.ClaimShardTasksResult{}, err
	}
	return mutate(s.provider, ctx, s.ShardID(), operation, func() (durable.ClaimShardTasksResult, error) {
		return s.inner.ClaimTasks(ctx, input)
	}, func(out durable.ClaimShardTasksResult) bool { return len(out.Claims) > 0 })
}
func (s *session) Heartbeat(ctx context.Context, now time.Time, lease time.Duration) error {
	operation, err := shardengine.NewSessionJournalOperation("heartbeatDispatchShard", s.openInput(), shardengine.HeartbeatDispatchShardOperationInput{Now: now, Lease: lease})
	if err != nil {
		return err
	}
	return s.provider.mutateVoid(ctx, s.ShardID(), operation, func() error {
		return s.inner.Heartbeat(ctx, now, lease)
	})
}
func (s *session) Release(ctx context.Context) error {
	operation, err := shardengine.NewJournalOperation("releaseDispatchShard", durable.ReleaseDispatchShardInput{ShardID: s.ShardID(), OwnerID: s.OwnerID()})
	if err != nil {
		return err
	}
	return s.provider.mutateVoid(ctx, s.ShardID(), operation, func() error {
		return s.inner.Release(ctx)
	})
}
func (s *session) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	return s.provider.GetOrReserveEffect(ctx, input)
}
func (s *session) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	return s.provider.HeartbeatEffect(ctx, input)
}
func (s *session) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	return s.provider.CompleteEffect(ctx, input)
}
func (s *session) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	return s.provider.FailEffect(ctx, input)
}
func (s *session) CommitActivations(ctx context.Context, input []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	return s.provider.CommitActivations(ctx, input)
}
func (s *session) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	return s.provider.CommitCheckpoint(ctx, input)
}
func (s *session) RecordActivationFailures(ctx context.Context, input []durable.RecordActivationFailureInput) error {
	return s.provider.RecordActivationFailures(ctx, input)
}
func (s *session) ReleaseActivation(ctx context.Context, activationID string, workerID string) error {
	return s.provider.ReleaseActivations(ctx, []string{activationID}, workerID)
}

func (s *session) openInput() durable.OpenShardInput {
	return durable.OpenShardInput{ShardID: s.ShardID(), OwnerID: s.OwnerID(), LeaseEpoch: s.LeaseEpoch()}
}

func (p *Provider) table(base string, partition int) string {
	return fmt.Sprintf("%s.%s", quoteIdent(p.schema), quoteIdent(fmt.Sprintf("%s_p%02d", base, partition)))
}
func (p *Provider) partitionForShard(shardID int) int {
	if p.physicalPartitions <= 1 {
		return 0
	}
	if shardID < 0 {
		shardID = -shardID
	}
	return shardID % p.physicalPartitions
}

func (p *Provider) shardForRef(ref durable.InstanceRef) int {
	if shardID, ok := p.engine.ShardForRef(ref); ok {
		return shardID
	}
	return 0
}

func (p *Provider) shardForActivation(activationID string) int {
	if shardID, ok := p.engine.ShardForActivation(activationID); ok {
		return shardID
	}
	return 0
}

func (p *Provider) shardForActivationIDs(activationIDs []string) int {
	for _, activationID := range activationIDs {
		if shardID, ok := p.engine.ShardForActivation(activationID); ok {
			return shardID
		}
	}
	return 0
}

func normalizeSchema(schema string) (string, error) {
	if schema == "" {
		schema = "durable_go"
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(schema) {
		return "", fmt.Errorf("PostgresDurabilityProvider schema must be a valid identifier")
	}
	return schema, nil
}
func quoteIdent(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
func sortRows(rows []journalRow) {
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].createdAt.Equal(rows[j].createdAt) {
			return rows[i].createdAt.Before(rows[j].createdAt)
		}
		if rows[i].shardID != rows[j].shardID {
			return rows[i].shardID < rows[j].shardID
		}
		return rows[i].entryID < rows[j].entryID
	})
}
