package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultSnapshotInterval = 512

type Options struct {
	ConnectionString   string
	DB                 *sql.DB
	Schema             string
	PhysicalPartitions int
	SnapshotInterval   int64
	StatementTimeout   time.Duration
	LockTimeout        time.Duration
}

type Provider struct {
	mu                 sync.Mutex
	db                 *sql.DB
	ownsDB             bool
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
	db := options.DB
	owns := false
	if db == nil {
		conn := options.ConnectionString
		if conn == "" {
			conn = "postgresql://durable:durable@127.0.0.1:55432/durable"
		}
		db, err = sql.Open("pgx", conn)
		if err != nil {
			return nil, err
		}
		owns = true
	}
	provider := &Provider{
		db:                 db,
		ownsDB:             owns,
		engine:             shardengine.New(),
		schema:             schema,
		physicalPartitions: partitions,
		snapshotInterval:   options.SnapshotInterval,
	}
	if provider.snapshotInterval <= 0 {
		provider.snapshotInterval = defaultSnapshotInterval
	}
	if err := provider.configure(ctx, options); err != nil {
		if owns {
			_ = db.Close()
		}
		return nil, err
	}
	if err := provider.ensureSchema(ctx); err != nil {
		if owns {
			_ = db.Close()
		}
		return nil, err
	}
	if err := provider.reload(ctx); err != nil {
		if owns {
			_ = db.Close()
		}
		return nil, err
	}
	return provider, nil
}

func (p *Provider) configure(ctx context.Context, options Options) error {
	if options.StatementTimeout > 0 {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("SET statement_timeout = %d", options.StatementTimeout.Milliseconds())); err != nil {
			return err
		}
	}
	if options.LockTimeout > 0 {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf("SET lock_timeout = %d", options.LockTimeout.Milliseconds())); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) ensureSchema(ctx context.Context) error {
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(p.schema))); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.provider_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS %s.dispatch_shards (
  shard_id INTEGER PRIMARY KEY,
  owner_id TEXT,
  lease_until TIMESTAMPTZ,
  lease_epoch BIGINT NOT NULL DEFAULT 0
);
`, quoteIdent(p.schema), quoteIdent(p.schema))); err != nil {
		return err
	}
	if err := p.verifyMetadata(ctx, "postgres_storage_shape", "go_append_store_v2"); err != nil {
		return err
	}
	if err := p.verifyMetadata(ctx, "physical_partition_count", fmt.Sprint(p.physicalPartitions)); err != nil {
		return err
	}
	for partition := 0; partition < p.physicalPartitions; partition++ {
		if _, err := p.db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER PRIMARY KEY,
  last_entry_id BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER NOT NULL,
  entry_id BIGINT NOT NULL,
  operation_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (shard_id, entry_id)
);
CREATE TABLE IF NOT EXISTS %s (
  shard_id INTEGER PRIMARY KEY,
  last_entry_id BIGINT NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
`, p.table("shard_heads", partition), p.table("shard_journal", partition), p.table("shard_snapshots", partition))); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) verifyMetadata(ctx context.Context, key, expected string) error {
	table := fmt.Sprintf("%s.provider_metadata", quoteIdent(p.schema))
	var actual string
	err := p.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT value FROM %s WHERE key = $1`, table), key).Scan(&actual)
	if err == sql.ErrNoRows {
		_, err = p.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`, table), key, expected)
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
	var latestRaw []byte
	var latestAt time.Time
	var latestEntry int64
	for partition := 0; partition < p.physicalPartitions; partition++ {
		var raw string
		var at time.Time
		var entry int64
		err := p.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT snapshot_json, created_at, last_entry_id FROM %s ORDER BY created_at DESC LIMIT 1`, p.table("shard_snapshots", partition))).Scan(&raw, &at, &entry)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}
		if latestRaw == nil || at.After(latestAt) {
			latestRaw = []byte(raw)
			latestAt = at
			latestEntry = entry
		}
	}
	if latestRaw != nil {
		var snapshot shardengine.Snapshot
		if err := json.Unmarshal(latestRaw, &snapshot); err != nil {
			return err
		}
		if err := p.engine.Restore(snapshot); err != nil {
			return err
		}
	}
	var rows []journalRow
	for partition := 0; partition < p.physicalPartitions; partition++ {
		query := fmt.Sprintf(`SELECT shard_id, entry_id, operation_json, created_at FROM %s WHERE entry_id > $1 ORDER BY created_at, shard_id, entry_id`, p.table("shard_journal", partition))
		dbRows, err := p.db.QueryContext(ctx, query, latestEntry)
		if err != nil {
			return err
		}
		for dbRows.Next() {
			var item journalRow
			if err := dbRows.Scan(&item.shardID, &item.entryID, &item.raw, &item.createdAt); err != nil {
				_ = dbRows.Close()
				return err
			}
			rows = append(rows, item)
		}
		if err := dbRows.Close(); err != nil {
			return err
		}
	}
	sortRows(rows)
	for _, item := range rows {
		var snapshot shardengine.Snapshot
		if err := json.Unmarshal(item.raw, &snapshot); err != nil {
			return err
		}
		if err := p.engine.Restore(snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) persist(ctx context.Context, shardID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("postgres provider is closed")
	}
	raw, err := json.Marshal(p.engine.Snapshot())
	if err != nil {
		return err
	}
	partition := p.partitionForShard(shardID)
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var entryID int64
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
WITH next_head AS (
  INSERT INTO %s AS h (shard_id, last_entry_id, updated_at)
  VALUES ($1, 1, NOW())
  ON CONFLICT (shard_id) DO UPDATE
  SET last_entry_id = h.last_entry_id + 1,
      updated_at = NOW()
  RETURNING last_entry_id
)
INSERT INTO %s (shard_id, entry_id, operation_json, created_at)
SELECT $1, last_entry_id, $2, NOW()
FROM next_head
RETURNING entry_id
`, p.table("shard_heads", partition), p.table("shard_journal", partition)), shardID, string(raw)).Scan(&entryID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if p.snapshotInterval > 0 && entryID%p.snapshotInterval == 0 {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (shard_id, last_entry_id, snapshot_json, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (shard_id) DO UPDATE SET
  last_entry_id = EXCLUDED.last_entry_id,
  snapshot_json = EXCLUDED.snapshot_json,
  created_at = EXCLUDED.created_at
`, p.table("shard_snapshots", partition)), shardID, entryID, string(raw)); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (p *Provider) ClaimShard(ctx context.Context, input durable.ClaimDispatchShardInput) (*durable.ShardLease, error) {
	lease, err := p.engine.ClaimShard(ctx, input)
	if err != nil || lease == nil {
		return lease, err
	}
	return lease, p.persist(ctx, input.ShardID)
}
func (p *Provider) OpenShard(input durable.OpenShardInput) durable.ShardDurabilitySession {
	return &session{provider: p, inner: p.engine.OpenShard(input)}
}
func (p *Provider) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.InstanceRef, error) {
	out, err := p.engine.CreateInstance(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx, input.PartitionShard)
}
func (p *Provider) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	parent, _ := p.engine.LoadInstance(ctx, durable.InstanceRef{WorkflowID: input.ParentWorkflowID, RunID: input.ParentRunID}, durable.LoadInstanceOptions{})
	if parent != nil && input.PartitionShard != parent.PartitionShard {
		return durable.ChildHandleAny{}, fmt.Errorf("Postgres provider requires local child workflow starts to be shard-affine")
	}
	out, err := p.engine.CreateChildInstance(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx, input.PartitionShard)
}
func (p *Provider) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	if err := p.engine.CancelChild(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx, durable.WorkflowPartitionShard(input.ParentWorkflowID, input.ParentRunID, 1))
}
func (p *Provider) LoadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return p.engine.LoadInstance(ctx, ref, options)
}
func (p *Provider) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	out, err := p.engine.AppendSignal(ctx, input)
	if err != nil {
		return out, err
	}
	instance, _ := p.engine.LoadInstance(ctx, durable.InstanceRef{WorkflowID: input.WorkflowID, RunID: input.RunID}, durable.LoadInstanceOptions{})
	shard := 0
	if instance != nil {
		shard = instance.PartitionShard
	}
	return out, p.persist(ctx, shard)
}
func (p *Provider) ClaimReadyActivations(ctx context.Context, shardIDs []int, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	out, err := p.engine.ClaimReadyActivations(ctx, shardIDs, input)
	if err != nil {
		return out, err
	}
	if len(out.Claims) > 0 {
		return out, p.persist(ctx, shardIDs[0])
	}
	return out, nil
}
func (p *Provider) HeartbeatActivations(ctx context.Context, activationIDs []string, workerID string, now time.Time, lease time.Duration) error {
	if err := p.engine.HeartbeatActivations(ctx, activationIDs, workerID, now, lease); err != nil {
		return err
	}
	return p.persist(ctx, 0)
}
func (p *Provider) ReleaseActivations(ctx context.Context, activationIDs []string, workerID string) error {
	if err := p.engine.ReleaseActivations(ctx, activationIDs, workerID); err != nil {
		return err
	}
	return p.persist(ctx, 0)
}
func (p *Provider) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	out, err := p.engine.GetOrReserveEffect(ctx, input)
	if err != nil {
		return out, err
	}
	if out.Status == "reserved" {
		return out, p.persist(ctx, 0)
	}
	return out, nil
}
func (p *Provider) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	if err := p.engine.HeartbeatEffect(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx, 0)
}
func (p *Provider) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	if err := p.engine.CompleteEffect(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx, 0)
}
func (p *Provider) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	out, err := p.engine.FailEffect(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx, 0)
}
func (p *Provider) CommitActivations(ctx context.Context, inputs []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
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
	out, err := p.engine.CommitActivations(ctx, inputs)
	if err != nil {
		return out, err
	}
	for _, result := range out.Results {
		if result.OK {
			return out, p.persist(ctx, 0)
		}
	}
	return out, nil
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
	if err := p.engine.RecordActivationFailures(ctx, inputs); err != nil {
		return err
	}
	if len(inputs) == 0 {
		return nil
	}
	return p.persist(ctx, 0)
}
func (p *Provider) ListInstances(ctx context.Context, options durable.LoadInstanceOptions) ([]durable.PersistedInstance, error) {
	return p.engine.ListInstances(ctx, options)
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
	if p.ownsDB {
		return p.db.Close()
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
func (s *session) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.InstanceRef, error) {
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
	out, err := s.inner.ClaimTasks(ctx, input)
	if err != nil {
		return out, err
	}
	if len(out.Claims) > 0 {
		return out, s.provider.persist(ctx, s.ShardID())
	}
	return out, nil
}
func (s *session) Heartbeat(ctx context.Context, now time.Time, lease time.Duration) error {
	if err := s.inner.Heartbeat(ctx, now, lease); err != nil {
		return err
	}
	return s.provider.persist(ctx, s.ShardID())
}
func (s *session) Release(ctx context.Context) error {
	if err := s.inner.Release(ctx); err != nil {
		return err
	}
	return s.provider.persist(ctx, s.ShardID())
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
	if err := s.inner.ReleaseActivation(ctx, activationID, workerID); err != nil {
		return err
	}
	return s.provider.persist(ctx, s.ShardID())
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
