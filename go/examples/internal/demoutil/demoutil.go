package demoutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
)

var Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type Clock struct {
	now time.Time
}

func NewClock() *Clock {
	return &Clock{now: Start}
}

func (c *Clock) Now() time.Time {
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type DemoRuntime struct {
	Runtime  *durable.Runtime
	Provider *sqliteprovider.Provider
	Clock    *Clock
	WorkerID string
	Name     string
}

func NewRuntime(ctx context.Context, name string, workflows []durable.Workflow) (*DemoRuntime, error) {
	if err := CleanupStore(name); err != nil {
		return nil, err
	}
	provider, err := sqliteprovider.New(StorePath(name), sqliteprovider.Options{})
	if err != nil {
		return nil, err
	}
	clock := NewClock()
	workerID := name + "-worker"
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:  workerID,
		Clock:     clock.Now,
		Workflows: workflows,
	})
	if err != nil {
		_ = provider.Close(ctx)
		return nil, err
	}
	return &DemoRuntime{
		Runtime:  runtime,
		Provider: provider,
		Clock:    clock,
		WorkerID: workerID,
		Name:     name,
	}, nil
}

func Restart(ctx context.Context, name string, clock *Clock, workerID string, workflows []durable.Workflow) (*sqliteprovider.Provider, *durable.Runtime, error) {
	provider, err := sqliteprovider.New(StorePath(name), sqliteprovider.Options{})
	if err != nil {
		return nil, nil, err
	}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:  workerID,
		Clock:     clock.Now,
		Workflows: workflows,
	})
	if err != nil {
		_ = provider.Close(ctx)
		return nil, nil, err
	}
	return provider, runtime, nil
}

func StorePath(name string) string {
	return filepath.Join(".durable-demo", name+".sqlite")
}

func CleanupStore(name string) error {
	storePath := StorePath(name)
	paths := []string{
		storePath,
		storePath + "-wal",
		storePath + "-shm",
		storePath + "-journal",
		filepath.Join(".durable-demo", name+".json"),
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.MkdirAll(filepath.Dir(storePath), 0o755)
}

func Committed(ctx context.Context, provider durable.DurabilityProvider, ref durable.InstanceRef) (any, error) {
	instance, err := provider.LoadInstance(ctx, ref, durable.LoadInstanceOptions{})
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, nil
	}
	if instance.Status == "running" {
		out := map[string]any{
			"workflowVersion": instance.WorkflowVersion,
			"sequence":        instance.Sequence,
			"status":          instance.Status,
			"waits":           summarizeWaits(instance.Waits),
		}
		if instance.Phase != nil {
			out["phase"] = instance.Phase.Name
			out["data"] = instance.Phase.Data
		}
		return out, nil
	}
	out := map[string]any{
		"workflowVersion": instance.WorkflowVersion,
		"sequence":        instance.Sequence,
		"status":          instance.Status,
	}
	if instance.Output != nil {
		out["output"] = instance.Output
	}
	if instance.CancelReason != "" {
		out["reason"] = instance.CancelReason
	}
	if instance.Error.Message != "" {
		out["error"] = instance.Error
	}
	return out, nil
}

func PrintJSON(label string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Printf("%s %v\n", label, value)
		return
	}
	fmt.Printf("%s %s\n", label, string(data))
}

func ISO(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func summarizeWaits(waits []durable.DurableWait) []map[string]any {
	out := make([]map[string]any, 0, len(waits))
	for _, wait := range waits {
		item := map[string]any{
			"kind": wait.Kind,
			"name": wait.Name,
		}
		if !wait.ReadyAt.IsZero() {
			item["readyAt"] = ISO(wait.ReadyAt)
		}
		if wait.Type != "" {
			item["type"] = wait.Type
		}
		if wait.Scope != "" {
			item["scope"] = wait.Scope
		}
		if !wait.FireAt.IsZero() {
			item["fireAt"] = ISO(wait.FireAt)
		}
		if wait.WorkflowName != "" {
			item["workflowName"] = wait.WorkflowName
		}
		if wait.WorkflowVersion != 0 {
			item["workflowVersion"] = wait.WorkflowVersion
		}
		if wait.WorkflowID != "" {
			item["workflowId"] = wait.WorkflowID
		}
		if wait.RunID != "" {
			item["runId"] = wait.RunID
		}
		out = append(out, item)
	}
	return out
}
