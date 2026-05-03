package durable_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	postgresprovider "github.com/danthegoodman1/durable-phases/go/providers/postgres"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
	"github.com/danthegoodman1/durable-phases/go/testing/conformance"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
					_, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
					return err
				},
			}
		},
	})
}
