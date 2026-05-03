package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/examples/internal/demoutil"
)

//go:generate go run github.com/danthegoodman1/durable-phases/go/cmd/durable-gen

type BatchInput struct {
	Items []string `json:"items"`
}

type BatchOutput struct {
	Processed []string `json:"processed"`
}

type ProcessBatch struct {
	Cursor    int      `json:"cursor"`
	Items     []string `json:"items"`
	Processed []string `json:"processed"`
}

//durable:workflow name=demo_checkpoint_loop version=1 input=BatchInput output=BatchOutput
type BatchWorkflow struct{}

//durable:phase name=process_batch run state=ProcessBatch
func (BatchWorkflow) ProcessBatchPhase() {}

func (BatchWorkflow) Name() string { return BatchWorkflowContract.Name }
func (BatchWorkflow) Version() int { return BatchWorkflowContract.Version }

func (BatchWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[BatchInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Phase: durable.PhaseSnapshot{
			Name: "process_batch",
			Data: ProcessBatch{Cursor: 0, Items: input.Items, Processed: []string{}},
		},
	}, nil
}

func (BatchWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "process_batch" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}

func (BatchWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	data, err := durable.DecodeJSON[ProcessBatch](phase.Data)
	if err != nil {
		return durable.Fail(err), nil
	}
	if data.Cursor >= len(data.Items) {
		return durable.Complete(BatchOutput{Processed: data.Processed}), nil
	}
	end := data.Cursor + 2
	if end > len(data.Items) {
		end = len(data.Items)
	}
	chunk := append([]string(nil), data.Items[data.Cursor:end]...)
	processedChunk, err := durable.Activity[[]string](ctx, dctx, fmt.Sprintf("process_%d", data.Cursor), func(context.Context) ([]string, error) {
		out := make([]string, len(chunk))
		for index, item := range chunk {
			out[index] = strings.ToUpper(item)
		}
		return out, nil
	})
	if err != nil {
		return durable.Fail(err), nil
	}
	nextProcessed := append(append([]string(nil), data.Processed...), processedChunk...)
	return durable.Stay(durable.PhaseSnapshot{
		Name: "process_batch",
		Data: ProcessBatch{Cursor: end, Items: data.Items, Processed: nextProcessed},
	}), nil
}

func (BatchWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, _ durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
}

func (BatchWorkflow) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (BatchWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func main() {
	ctx := context.Background()
	workflow := BatchWorkflow{}
	demo, err := demoutil.NewRuntime(ctx, "stay-loop", []durable.Workflow{workflow})
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = demo.Provider.Close(context.Background())
		_ = demoutil.CleanupStore("stay-loop")
	}()

	ref, err := demo.Runtime.Start(ctx, workflow, BatchInput{Items: []string{"alpha", "bravo", "charlie", "delta", "echo"}}, durable.StartOptions{WorkflowID: "loop-demo"})
	if err != nil {
		exit(err)
	}
	if _, err := demo.Runtime.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	committed, err := demoutil.Committed(ctx, demo.Provider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("stay loop: completed", committed)
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
