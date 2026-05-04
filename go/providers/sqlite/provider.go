package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	_ "modernc.org/sqlite"
)

const defaultSnapshotInterval = 512

type Options struct {
	BusyTimeout      time.Duration
	SnapshotInterval int64
}

type Provider struct {
	mu               sync.Mutex
	db               *sql.DB
	engine           *shardengine.Provider
	filePath         string
	snapshotInterval int64
	closed           bool
}

func New(path string, options Options) (*Provider, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	provider := &Provider{
		db:               db,
		engine:           shardengine.New(),
		filePath:         path,
		snapshotInterval: options.SnapshotInterval,
	}
	if provider.snapshotInterval <= 0 {
		provider.snapshotInterval = defaultSnapshotInterval
	}
	if err := provider.configure(options); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := provider.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := provider.reload(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return provider, nil
}

func (p *Provider) configure(options Options) error {
	busy := options.BusyTimeout
	if busy <= 0 {
		busy = 5 * time.Second
	}
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		fmt.Sprintf("PRAGMA busy_timeout=%d", busy.Milliseconds()),
	}
	for _, stmt := range stmts {
		if _, err := p.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) ensureSchema(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS provider_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_shards (
  shard_id INTEGER PRIMARY KEY,
  owner_id TEXT,
  lease_until TEXT,
  lease_epoch INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS shard_journal (
  entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shard_snapshots (
  snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
  last_entry_id INTEGER NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS shard_journal_entry ON shard_journal(entry_id);
`)
	return err
}

func (p *Provider) reload(ctx context.Context) error {
	var lastEntry int64
	var raw string
	err := p.db.QueryRowContext(ctx, `SELECT last_entry_id, snapshot_json FROM shard_snapshots WHERE snapshot_id = 1`).Scan(&lastEntry, &raw)
	if err == nil {
		var snapshot shardengine.Snapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return err
		}
		if err := p.engine.Restore(snapshot); err != nil {
			return err
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	rows, err := p.db.QueryContext(ctx, `SELECT operation_json FROM shard_journal WHERE entry_id > ? ORDER BY entry_id`, lastEntry)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var snapshot shardengine.Snapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return err
		}
		if err := p.engine.Restore(snapshot); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (p *Provider) persist(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("sqlite provider is closed")
	}
	snapshot := p.engine.Snapshot()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `INSERT INTO shard_journal (operation_json, created_at) VALUES (?, ?)`, string(raw), createdAt)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	entryID, err := result.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if p.snapshotInterval > 0 && entryID%p.snapshotInterval == 0 {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO shard_snapshots (snapshot_id, last_entry_id, snapshot_json, created_at)
VALUES (1, ?, ?, ?)
ON CONFLICT(snapshot_id) DO UPDATE SET
  last_entry_id = excluded.last_entry_id,
  snapshot_json = excluded.snapshot_json,
  created_at = excluded.created_at
`, entryID, string(raw), createdAt); err != nil {
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
	return lease, p.persist(ctx)
}

func (p *Provider) OpenShard(input durable.OpenShardInput) durable.ShardDurabilitySession {
	return &session{provider: p, inner: p.engine.OpenShard(input)}
}

func (p *Provider) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.StartWorkflowResult, error) {
	out, err := p.engine.CreateInstance(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx)
}

func (p *Provider) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	out, err := p.engine.CreateChildInstance(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx)
}

func (p *Provider) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	if err := p.engine.CancelChild(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx)
}

func (p *Provider) LoadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return p.engine.LoadInstance(ctx, ref, options)
}

func (p *Provider) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	out, err := p.engine.AppendSignal(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx)
}

func (p *Provider) ClaimReadyActivations(ctx context.Context, shardIDs []int, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	out, err := p.engine.ClaimReadyActivations(ctx, shardIDs, input)
	if err != nil {
		return out, err
	}
	if len(out.Claims) > 0 {
		return out, p.persist(ctx)
	}
	return out, nil
}

func (p *Provider) HeartbeatActivations(ctx context.Context, activationIDs []string, workerID string, now time.Time, lease time.Duration) error {
	if err := p.engine.HeartbeatActivations(ctx, activationIDs, workerID, now, lease); err != nil {
		return err
	}
	return p.persist(ctx)
}

func (p *Provider) ReleaseActivations(ctx context.Context, activationIDs []string, workerID string) error {
	if err := p.engine.ReleaseActivations(ctx, activationIDs, workerID); err != nil {
		return err
	}
	return p.persist(ctx)
}

func (p *Provider) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	out, err := p.engine.GetOrReserveEffect(ctx, input)
	if err != nil {
		return out, err
	}
	if out.Status == "reserved" {
		return out, p.persist(ctx)
	}
	return out, nil
}

func (p *Provider) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	if err := p.engine.HeartbeatEffect(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx)
}

func (p *Provider) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	if err := p.engine.CompleteEffect(ctx, input); err != nil {
		return err
	}
	return p.persist(ctx)
}

func (p *Provider) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	out, err := p.engine.FailEffect(ctx, input)
	if err != nil {
		return out, err
	}
	return out, p.persist(ctx)
}

func (p *Provider) CommitActivations(ctx context.Context, inputs []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	out, err := p.engine.CommitActivations(ctx, inputs)
	if err != nil {
		return out, err
	}
	for _, result := range out.Results {
		if result.OK {
			return out, p.persist(ctx)
		}
	}
	return out, nil
}

func (p *Provider) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	out, err := p.engine.CommitCheckpoint(ctx, input)
	if err != nil {
		return out, err
	}
	if out.OK {
		return out, p.persist(ctx)
	}
	return out, nil
}

func (p *Provider) RecordActivationFailures(ctx context.Context, inputs []durable.RecordActivationFailureInput) error {
	if err := p.engine.RecordActivationFailures(ctx, inputs); err != nil {
		return err
	}
	if len(inputs) == 0 {
		return nil
	}
	return p.persist(ctx)
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
	return p.db.Close()
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
	out, err := s.inner.ClaimTasks(ctx, input)
	if err != nil {
		return out, err
	}
	if len(out.Claims) > 0 {
		return out, s.provider.persist(ctx)
	}
	return out, nil
}
func (s *session) Heartbeat(ctx context.Context, now time.Time, lease time.Duration) error {
	if err := s.inner.Heartbeat(ctx, now, lease); err != nil {
		return err
	}
	return s.provider.persist(ctx)
}
func (s *session) Release(ctx context.Context) error {
	if err := s.inner.Release(ctx); err != nil {
		return err
	}
	return s.provider.persist(ctx)
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
	return s.provider.persist(ctx)
}

type ShardFileOptions struct {
	Directory        string
	ShardCount       int
	BusyTimeout      time.Duration
	SnapshotInterval int64
	FilenameForShard func(int) string
}

type ShardFileProvider struct {
	shardCount int
	providers  []*Provider
}

func NewShardFile(options ShardFileOptions) (*ShardFileProvider, error) {
	if options.ShardCount <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}
	if err := os.MkdirAll(options.Directory, 0o755); err != nil {
		return nil, err
	}
	out := &ShardFileProvider{shardCount: options.ShardCount, providers: make([]*Provider, options.ShardCount)}
	for shardID := 0; shardID < options.ShardCount; shardID++ {
		name := fmt.Sprintf("shard-%d.sqlite", shardID)
		if options.FilenameForShard != nil {
			name = options.FilenameForShard(shardID)
		}
		provider, err := New(filepath.Join(options.Directory, name), Options{BusyTimeout: options.BusyTimeout, SnapshotInterval: options.SnapshotInterval})
		if err != nil {
			return nil, err
		}
		out.providers[shardID] = provider
	}
	return out, nil
}

func (p *ShardFileProvider) providerForShard(shardID int) (*Provider, error) {
	if shardID < 0 || shardID >= p.shardCount {
		return nil, fmt.Errorf("shard %d outside configured shard count %d", shardID, p.shardCount)
	}
	return p.providers[shardID], nil
}

func (p *ShardFileProvider) providerForRef(workflowID, runID string) (*Provider, error) {
	return p.providerForShard(durable.WorkflowPartitionShard(workflowID, runID, p.shardCount))
}

func (p *ShardFileProvider) ClaimShard(ctx context.Context, input durable.ClaimDispatchShardInput) (*durable.ShardLease, error) {
	provider, err := p.providerForShard(input.ShardID)
	if err != nil {
		return nil, err
	}
	return provider.ClaimShard(ctx, input)
}
func (p *ShardFileProvider) OpenShard(input durable.OpenShardInput) durable.ShardDurabilitySession {
	provider, err := p.providerForShard(input.ShardID)
	if err != nil {
		return failedSession{err: err}
	}
	return provider.OpenShard(input)
}
func (p *ShardFileProvider) CreateInstance(ctx context.Context, input durable.CreateInstanceInput) (durable.StartWorkflowResult, error) {
	provider, err := p.providerForShard(input.PartitionShard)
	if err != nil {
		return durable.StartWorkflowResult{}, err
	}
	return provider.CreateInstance(ctx, input)
}
func (p *ShardFileProvider) CreateChildInstance(ctx context.Context, input durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	parentShard := durable.WorkflowPartitionShard(input.ParentWorkflowID, input.ParentRunID, p.shardCount)
	if input.PartitionShard != parentShard {
		return durable.ChildHandleAny{}, fmt.Errorf("SQLite shard-file provider requires local child workflow starts to be shard-affine")
	}
	provider, err := p.providerForShard(input.PartitionShard)
	if err != nil {
		return durable.ChildHandleAny{}, err
	}
	return provider.CreateChildInstance(ctx, input)
}
func (p *ShardFileProvider) CancelChild(ctx context.Context, input durable.CancelChildInput) error {
	provider, err := p.providerForRef(input.ParentWorkflowID, input.ParentRunID)
	if err != nil {
		return err
	}
	return provider.CancelChild(ctx, input)
}
func (p *ShardFileProvider) LoadInstance(ctx context.Context, ref durable.InstanceRef, options durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	provider, err := p.providerForRef(ref.WorkflowID, ref.RunID)
	if err != nil {
		return nil, err
	}
	return provider.LoadInstance(ctx, ref, options)
}
func (p *ShardFileProvider) AppendSignal(ctx context.Context, input durable.AppendSignalInput) (durable.SignalRecord, error) {
	provider, err := p.providerForRef(input.WorkflowID, input.RunID)
	if err != nil {
		return durable.SignalRecord{}, err
	}
	return provider.AppendSignal(ctx, input)
}
func (p *ShardFileProvider) ClaimReadyActivations(ctx context.Context, shardIDs []int, input durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	var out durable.ClaimShardTasksResult
	for _, shardID := range shardIDs {
		provider, err := p.providerForShard(shardID)
		if err != nil {
			return out, err
		}
		result, err := provider.ClaimReadyActivations(ctx, []int{shardID}, input)
		if err != nil {
			return out, err
		}
		out.Claims = append(out.Claims, result.Claims...)
		if out.NextWakeAt.IsZero() || (!result.NextWakeAt.IsZero() && result.NextWakeAt.Before(out.NextWakeAt)) {
			out.NextWakeAt = result.NextWakeAt
		}
	}
	return out, nil
}
func (p *ShardFileProvider) HeartbeatActivations(ctx context.Context, activationIDs []string, workerID string, now time.Time, lease time.Duration) error {
	for shardID := range p.providers {
		if err := p.providers[shardID].HeartbeatActivations(ctx, activationIDs, workerID, now, lease); err != nil {
			return err
		}
	}
	return nil
}
func (p *ShardFileProvider) ReleaseActivations(ctx context.Context, activationIDs []string, workerID string) error {
	for _, provider := range p.providers {
		_ = provider.ReleaseActivations(ctx, activationIDs, workerID)
	}
	return nil
}
func (p *ShardFileProvider) GetOrReserveEffect(ctx context.Context, input durable.ReserveEffectInput) (durable.EffectReservation, error) {
	provider, err := p.providerForRef(input.WorkflowID, input.RunID)
	if err != nil {
		return durable.EffectReservation{}, err
	}
	return provider.GetOrReserveEffect(ctx, input)
}
func (p *ShardFileProvider) HeartbeatEffect(ctx context.Context, input durable.HeartbeatEffectInput) error {
	provider, err := p.providerForRef(input.WorkflowID, input.RunID)
	if err != nil {
		return err
	}
	return provider.HeartbeatEffect(ctx, input)
}
func (p *ShardFileProvider) CompleteEffect(ctx context.Context, input durable.CompleteEffectInput) error {
	provider, err := p.providerForRef(input.WorkflowID, input.RunID)
	if err != nil {
		return err
	}
	return provider.CompleteEffect(ctx, input)
}
func (p *ShardFileProvider) FailEffect(ctx context.Context, input durable.FailEffectInput) (durable.FailEffectResult, error) {
	provider, err := p.providerForRef(input.WorkflowID, input.RunID)
	if err != nil {
		return durable.FailEffectResult{}, err
	}
	return provider.FailEffect(ctx, input)
}
func (p *ShardFileProvider) CommitActivations(ctx context.Context, inputs []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	out := durable.CommitActivationsResult{Results: make([]durable.CommitCheckpointResult, 0, len(inputs))}
	for _, input := range inputs {
		result, err := p.CommitCheckpoint(ctx, input)
		if err != nil {
			return out, err
		}
		result.ActivationID = input.ActivationID
		out.Results = append(out.Results, result)
	}
	return out, nil
}
func (p *ShardFileProvider) CommitCheckpoint(ctx context.Context, input durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	parentShard := durable.WorkflowPartitionShard(input.WorkflowID, input.RunID, p.shardCount)
	for _, child := range input.ChildStarts {
		if child.PartitionShard != parentShard {
			f := false
			return durable.CommitCheckpointResult{OK: false, Sequence: input.ExpectedSequence, Reason: "cross_shard_child_start", Retryable: &f, Error: durable.SerializedError{Message: "SQLite shard-file provider requires commit-local children to stay on the parent shard"}}, nil
		}
	}
	provider, err := p.providerForShard(parentShard)
	if err != nil {
		return durable.CommitCheckpointResult{}, err
	}
	return provider.CommitCheckpoint(ctx, input)
}
func (p *ShardFileProvider) RecordActivationFailures(ctx context.Context, inputs []durable.RecordActivationFailureInput) error {
	for _, input := range inputs {
		provider, err := p.providerForRef(input.WorkflowID, input.RunID)
		if err != nil {
			return err
		}
		if err := provider.RecordActivationFailures(ctx, []durable.RecordActivationFailureInput{input}); err != nil {
			return err
		}
	}
	return nil
}
func (p *ShardFileProvider) ListInstances(ctx context.Context, options durable.LoadInstanceOptions) ([]durable.PersistedInstance, error) {
	var out []durable.PersistedInstance
	for _, provider := range p.providers {
		items, err := provider.ListInstances(ctx, options)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkflowID < out[j].WorkflowID })
	return out, nil
}

func (p *ShardFileProvider) GetWorkflowRuns(ctx context.Context, input durable.GetWorkflowRunsInput) (durable.GetWorkflowRunsResult, error) {
	provider, err := p.providerForRef(input.ID, "")
	if err != nil {
		return durable.GetWorkflowRunsResult{}, err
	}
	return provider.GetWorkflowRuns(ctx, input)
}
func (p *ShardFileProvider) ListSignals(ctx context.Context) ([]durable.SignalRecord, error) {
	var out []durable.SignalRecord
	for _, provider := range p.providers {
		items, err := provider.ListSignals(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}
func (p *ShardFileProvider) ListChildren(ctx context.Context) ([]durable.ChildRecord, error) {
	var out []durable.ChildRecord
	for _, provider := range p.providers {
		items, err := provider.ListChildren(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}
func (p *ShardFileProvider) Close(ctx context.Context) error {
	for _, provider := range p.providers {
		_ = provider.Close(ctx)
	}
	return nil
}

type failedSession struct{ err error }

func (f failedSession) ShardID() int      { return 0 }
func (f failedSession) OwnerID() string   { return "" }
func (f failedSession) LeaseEpoch() int64 { return 0 }
func (f failedSession) CreateInstance(context.Context, durable.CreateInstanceInput) (durable.StartWorkflowResult, error) {
	return durable.StartWorkflowResult{}, f.err
}
func (f failedSession) CreateChildInstance(context.Context, durable.CreateChildInstanceInput) (durable.ChildHandleAny, error) {
	return durable.ChildHandleAny{}, f.err
}
func (f failedSession) CancelChild(context.Context, durable.CancelChildInput) error { return f.err }
func (f failedSession) ReadInstance(context.Context, durable.InstanceRef, durable.LoadInstanceOptions) (*durable.PersistedInstance, error) {
	return nil, f.err
}
func (f failedSession) AppendSignal(context.Context, durable.AppendSignalInput) (durable.SignalRecord, error) {
	return durable.SignalRecord{}, f.err
}
func (f failedSession) ClaimTasks(context.Context, durable.ClaimShardTasksInput) (durable.ClaimShardTasksResult, error) {
	return durable.ClaimShardTasksResult{}, f.err
}
func (f failedSession) Heartbeat(context.Context, time.Time, time.Duration) error { return f.err }
func (f failedSession) Release(context.Context) error                             { return f.err }
func (f failedSession) GetOrReserveEffect(context.Context, durable.ReserveEffectInput) (durable.EffectReservation, error) {
	return durable.EffectReservation{}, f.err
}
func (f failedSession) HeartbeatEffect(context.Context, durable.HeartbeatEffectInput) error {
	return f.err
}
func (f failedSession) CompleteEffect(context.Context, durable.CompleteEffectInput) error {
	return f.err
}
func (f failedSession) FailEffect(context.Context, durable.FailEffectInput) (durable.FailEffectResult, error) {
	return durable.FailEffectResult{}, f.err
}
func (f failedSession) CommitActivations(context.Context, []durable.CommitCheckpointInput) (durable.CommitActivationsResult, error) {
	return durable.CommitActivationsResult{}, f.err
}
func (f failedSession) CommitCheckpoint(context.Context, durable.CommitCheckpointInput) (durable.CommitCheckpointResult, error) {
	return durable.CommitCheckpointResult{}, f.err
}
func (f failedSession) RecordActivationFailures(context.Context, []durable.RecordActivationFailureInput) error {
	return f.err
}
func (f failedSession) ReleaseActivation(context.Context, string, string) error { return f.err }
