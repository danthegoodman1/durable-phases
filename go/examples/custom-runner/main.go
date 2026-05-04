package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/examples/internal/demoutil"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
)

const shardCount = 3

type CustomRunnerInput struct {
	Items []string `json:"items"`
}

type CustomRunnerOutput struct {
	Processed []string `json:"processed"`
}

type ProcessState struct {
	Cursor    int      `json:"cursor"`
	Items     []string `json:"items"`
	Processed []string `json:"processed"`
}

type CustomRunnerWorkflow struct{}

func (CustomRunnerWorkflow) Name() string { return "demo_custom_runner" }
func (CustomRunnerWorkflow) Version() int { return 1 }

func (CustomRunnerWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[CustomRunnerInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Phase: durable.PhaseSnapshot{
			Name: "process",
			Data: ProcessState{Cursor: 0, Items: input.Items, Processed: []string{}},
		},
	}, nil
}

func (CustomRunnerWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "process" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}

func (CustomRunnerWorkflow) DispatchRun(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	data, err := durable.DecodeJSON[ProcessState](phase.Data)
	if err != nil {
		return durable.Fail(err), nil
	}
	if data.Cursor >= len(data.Items) {
		return durable.Complete(CustomRunnerOutput{Processed: data.Processed}), nil
	}
	nextProcessed := append(append([]string(nil), data.Processed...), strings.ToUpper(data.Items[data.Cursor]))
	return durable.Stay(durable.PhaseSnapshot{
		Name: "process",
		Data: ProcessState{Cursor: data.Cursor + 1, Items: data.Items, Processed: nextProcessed},
	}), nil
}

func (CustomRunnerWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, _ durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
}

func (CustomRunnerWorkflow) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (CustomRunnerWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type shardRef struct {
	ShardID int
	Ref     durable.StartWorkflowResult
}

type shardMapping struct {
	ShardID    int    `json:"shardId"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type shardLoopSummary struct {
	ShardID     int `json:"shardId"`
	Activations int `json:"activations"`
}

type shardLoopResult struct {
	Summary shardLoopSummary
	Err     error
}

type workflowOutput struct {
	ShardID    int    `json:"shardId"`
	WorkflowID string `json:"workflowId"`
	Result     any    `json:"result"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	const demoName = "custom-runner"
	ctx := context.Background()
	if err := demoutil.CleanupStore(demoName); err != nil {
		return err
	}
	provider, err := sqliteprovider.New(demoutil.StorePath(demoName), sqliteprovider.Options{})
	if err != nil {
		return err
	}
	defer func() {
		_ = provider.Close(context.Background())
		_ = demoutil.CleanupStore(demoName)
	}()

	workflow := CustomRunnerWorkflow{}
	clock := demoutil.NewClock()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:   demoName,
		ShardCount: shardCount,
		Clock:      clock.Now,
		Workflows:  []durable.Workflow{workflow},
	})
	if err != nil {
		return err
	}

	// This context stands in for whatever lifecycle signal a host gives a
	// custom runner: cancellation, action duration, or a choice to run one
	// bounded step and return.
	workerCtx, cancelLoops := context.WithCancel(ctx)
	loopResults := make(chan shardLoopResult, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		go func(shardID int) {
			activations, err := runShardLoop(workerCtx, runtime, shardID)
			loopResults <- shardLoopResult{
				Summary: shardLoopSummary{ShardID: shardID, Activations: activations},
				Err:     err,
			}
		}(shardID)
	}
	defer cancelLoops()

	refs := make([]shardRef, 0, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		// Demo-only placement hack: normally workflow IDs come from business
		// identity, then the runtime hashes the ref to decide which shard to kick.
		// Here we choose IDs by shard only so the output shows one workflow per
		// runner loop.
		workflowID := workflowIDForShard(shardID)
		ref, err := runtime.Start(ctx, workflow, CustomRunnerInput{
			Items: []string{
				fmt.Sprintf("item-%d-a", shardID),
				fmt.Sprintf("item-%d-b", shardID),
				fmt.Sprintf("item-%d-c", shardID),
			},
		}, durable.StartOptions{WorkflowID: workflowID})
		if err != nil {
			return err
		}
		refs = append(refs, shardRef{ShardID: runtime.ShardForRef(ref), Ref: ref})
	}

	mapping := make([]shardMapping, 0, len(refs))
	for _, item := range refs {
		mapping = append(mapping, shardMapping{
			ShardID:    item.ShardID,
			WorkflowID: item.Ref.WorkflowID,
			RunID:      item.Ref.RunID,
		})
	}
	demoutil.PrintJSON("custom runner: shard mapping", mapping)

	if _, err := waitForCompleted(ctx, provider, refs); err != nil {
		return err
	}
	activationsByShard, err := collectShardLoops(cancelLoops, loopResults)
	if err != nil {
		return err
	}

	outputs := make([]workflowOutput, 0, len(refs))
	for _, item := range refs {
		committed, err := demoutil.Committed(ctx, provider, item.Ref)
		if err != nil {
			return err
		}
		outputs = append(outputs, workflowOutput{
			ShardID:    item.ShardID,
			WorkflowID: item.Ref.WorkflowID,
			Result:     committed,
		})
	}
	demoutil.PrintJSON("custom runner: shard activations", activationsByShard)
	demoutil.PrintJSON("custom runner: completed", outputs)
	return nil
}

func runShardLoop(ctx context.Context, runtime *durable.Runtime, shardID int) (int, error) {
	activations := 0
	for ctx.Err() == nil {
		// This is the public custom-runner primitive. It claims one shard, runs
		// at most one activation, commits through the provider, then releases.
		result, err := runtime.RunShardStep(ctx, durable.RunShardStepOptions{
			ShardID:                  shardID,
			MaxActivations:           1,
			MaxConcurrentActivations: 1,
			ActivationPrefetchLimit:  1,
		})
		activations += result.Activations
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return activations, err
		}
		if !result.ClaimedShard || result.Activations == 0 {
			// No lease or no ready work. A hosted scheduler could persist
			// result.NextWakeAt and return; this local demo idles briefly.
			if err := sleep(ctx, 10*time.Millisecond); err != nil && ctx.Err() == nil {
				return activations, err
			}
		}
	}
	return activations, nil
}

func workflowIDForShard(shardID int) string {
	for attempt := 0; attempt < 10_000; attempt++ {
		workflowID := fmt.Sprintf("custom-runner-%d-%d", shardID, attempt)
		if durable.WorkflowPartitionShard(workflowID, "run-1", shardCount) == shardID {
			return workflowID
		}
	}
	panic(fmt.Sprintf("could not find workflow id for shard %d", shardID))
}

func waitForCompleted(ctx context.Context, provider durable.DurabilityProvider, refs []shardRef) ([]durable.PersistedInstance, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		instances := make([]durable.PersistedInstance, 0, len(refs))
		for _, item := range refs {
			instance, err := provider.LoadInstance(ctx, item.Ref.InstanceRef, durable.LoadInstanceOptions{})
			if err != nil {
				return nil, err
			}
			if instance != nil {
				instances = append(instances, *instance)
			}
		}
		if len(instances) == len(refs) {
			allCompleted := true
			for _, instance := range instances {
				if instance.Status != "completed" {
					allCompleted = false
					break
				}
			}
			if allCompleted {
				return instances, nil
			}
		}
		if err := sleep(ctx, 5*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("timed out waiting for custom runner demo workflows")
}

func collectShardLoops(cancel context.CancelFunc, results <-chan shardLoopResult) ([]shardLoopSummary, error) {
	cancel()
	summaries := make([]shardLoopSummary, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		result := <-results
		if result.Err != nil {
			return nil, result.Err
		}
		summaries = append(summaries, result.Summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].ShardID < summaries[j].ShardID
	})
	return summaries, nil
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
