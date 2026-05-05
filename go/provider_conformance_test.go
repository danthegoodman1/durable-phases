package durable_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	postgresprovider "github.com/danthegoodman1/durable-phases/go/providers/postgres"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
	"github.com/danthegoodman1/durable-phases/go/testing/conformance"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMemoryProviderConformance(t *testing.T) {
	conformance.AssertProviderConformance(t, conformance.Factory{
		Name: "MemoryShardEngine",
		NewStore: func(t *testing.T) conformance.Store {
			t.Helper()
			provider := shardengine.New()
			return conformance.Store{
				New: func(t *testing.T) conformance.ProviderHandle {
					t.Helper()
					return conformance.ProviderHandle{Provider: provider, Close: provider.Close}
				},
				Cleanup: provider.Close,
			}
		},
	})
}

func TestSQLiteProviderConformance(t *testing.T) {
	conformance.AssertProviderConformance(t, conformance.Factory{
		Name: "SQLiteSingleFile",
		NewStore: func(t *testing.T) conformance.Store {
			t.Helper()
			path := filepath.Join(t.TempDir(), "conformance.sqlite")
			var provider *sqliteprovider.Provider
			return conformance.Store{
				New: func(t *testing.T) conformance.ProviderHandle {
					t.Helper()
					if provider == nil {
						var err error
						provider, err = sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 3})
						if err != nil {
							t.Fatal(err)
						}
					}
					return conformance.ProviderHandle{Provider: provider}
				},
				Cleanup: func(ctx context.Context) error {
					if provider != nil {
						return provider.Close(ctx)
					}
					return nil
				},
			}
		},
	})
}

func TestSQLiteStartSendSignalJournalUsesCombinedOperation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "start-send-journal.sqlite")
	provider, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 1000})
	if err != nil {
		t.Fatal(err)
	}
	input := durable.StartSendSignalInput{
		CreateInstanceInput: durable.CreateInstanceInput{
			WorkflowName: "journal", WorkflowVersion: 1, WorkflowID: "journal-start-send", RunID: "run-1", PartitionShard: 0,
			Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}, Now: time.Now().UTC(),
			WorkflowIDReusePolicy: durable.WorkflowIDReusePolicyNotRunning,
		},
		SignalType: "finish", SignalPayload: map[string]any{"ok": true}, SignalReceivedAt: time.Now().UTC(), SignalIdempotencyKey: "run-1",
	}
	started, err := provider.StartSendSignal(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !started.Created {
		t.Fatalf("startSendSignal did not create: %#v", started)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw string
	if err := db.QueryRowContext(ctx, `SELECT operation_json FROM shard_journal ORDER BY entry_id`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var operation struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal([]byte(raw), &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Op != "startSendSignal" {
		t.Fatalf("journal op = %q, want startSendSignal; raw=%s", operation.Op, raw)
	}

	replayed, err := sqliteprovider.New(path, sqliteprovider.Options{SnapshotInterval: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer replayed.Close(ctx)
	signals, err := replayed.ListSignals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 || signals[0].SignalID != started.Signal.SignalID {
		t.Fatalf("replayed signals = %#v, want %s", signals, started.Signal.SignalID)
	}
}

func TestSQLiteShardFileProviderConformance(t *testing.T) {
	conformance.AssertProviderConformance(t, conformance.Factory{
		Name: "SQLiteShardFile",
		NewStore: func(t *testing.T) conformance.Store {
			t.Helper()
			var provider *sqliteprovider.ShardFileProvider
			return conformance.Store{
				New: func(t *testing.T) conformance.ProviderHandle {
					t.Helper()
					if provider == nil {
						var err error
						provider, err = sqliteprovider.NewShardFile(sqliteprovider.ShardFileOptions{Directory: t.TempDir(), ShardCount: 1, SnapshotInterval: 3})
						if err != nil {
							t.Fatal(err)
						}
					}
					return conformance.ProviderHandle{Provider: provider}
				},
				Cleanup: func(ctx context.Context) error {
					if provider != nil {
						return provider.Close(ctx)
					}
					return nil
				},
			}
		},
	})
}

func TestPostgresProviderConformanceWhenConfigured(t *testing.T) {
	url := os.Getenv("DURABLE_POSTGRES_URL")
	if url == "" {
		t.Skip("DURABLE_POSTGRES_URL is not set")
	}
	schema := fmt.Sprintf("durable_go_conformance_%d", time.Now().UnixNano())
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conformance.AssertProviderConformance(t, conformance.Factory{
		Name: "Postgres",
		NewStore: func(t *testing.T) conformance.Store {
			t.Helper()
			var provider *postgresprovider.Provider
			return conformance.Store{
				New: func(t *testing.T) conformance.ProviderHandle {
					t.Helper()
					if provider == nil {
						var err error
						provider, err = postgresprovider.New(context.Background(), postgresprovider.Options{
							ConnectionString:   url,
							Schema:             schema,
							PhysicalPartitions: 2,
							SnapshotInterval:   3,
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					return conformance.ProviderHandle{Provider: provider}
				},
				Cleanup: func(ctx context.Context) error {
					if provider != nil {
						_ = provider.Close(ctx)
					}
					_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
					return err
				},
			}
		},
	})
}
