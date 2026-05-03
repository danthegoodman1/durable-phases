package main

import (
	"context"
	"fmt"
	"os"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/examples/internal/demoutil"
)

//go:generate go run github.com/danthegoodman1/durable-phases/go/cmd/durable-gen

type GreetingInput struct {
	Name string `json:"name"`
}

type GreetingChildOutput struct {
	Greeting string `json:"greeting"`
}

type GreetingParentOutput struct {
	Greeting    string `json:"greeting"`
	CompletedAt string `json:"completedAt"`
}

type GreetingCommon struct {
	Name string `json:"name"`
}

type ComposeGreeting struct{}

type StartChild struct{}

type WaitingForChild struct {
	Greeting durable.ChildHandleAny `json:"greeting"`
}

//durable:workflow name=demo_greeting_child version=1 input=GreetingInput output=GreetingChildOutput common=GreetingCommon
type GreetingChildWorkflow struct{}

//durable:phase name=compose run state=ComposeGreeting
func (GreetingChildWorkflow) ComposePhase() {}

//durable:workflow name=demo_greeting_parent version=1 input=GreetingInput output=GreetingParentOutput common=GreetingCommon
type GreetingParentWorkflow struct {
	Child durable.Workflow
}

//durable:phase name=start_child run state=StartChild
func (GreetingParentWorkflow) StartChildPhase() {}

//durable:phase name=waiting_for_child state=WaitingForChild
func (GreetingParentWorkflow) WaitingForChildPhase() {}

//durable:child name=greeting_finished handle=Greeting
func (GreetingParentWorkflow) GreetingFinished() {}

func (GreetingChildWorkflow) Name() string { return GreetingChildWorkflowContract.Name }
func (GreetingChildWorkflow) Version() int { return GreetingChildWorkflowContract.Version }

func (GreetingChildWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[GreetingInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: GreetingCommon{Name: input.Name},
		Phase:  durable.PhaseSnapshot{Name: "compose", Data: ComposeGreeting{}},
	}, nil
}

func (GreetingChildWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "compose" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}

func (GreetingChildWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, rawCommon durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	common, err := durable.DecodeJSON[GreetingCommon](rawCommon)
	if err != nil {
		return durable.Fail(err), nil
	}
	greeting, err := durable.Activity[GreetingChildOutput](ctx, dctx, "compose_greeting", func(context.Context) (GreetingChildOutput, error) {
		return GreetingChildOutput{Greeting: fmt.Sprintf("Hello, %s!", common.Name)}, nil
	})
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(greeting), nil
}

func (GreetingChildWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, _ durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
}

func (GreetingChildWorkflow) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (GreetingChildWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func (w GreetingParentWorkflow) Name() string { return GreetingParentWorkflowContract.Name }
func (w GreetingParentWorkflow) Version() int { return GreetingParentWorkflowContract.Version }

func (w GreetingParentWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[GreetingInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: GreetingCommon{Name: input.Name},
		Phase:  durable.PhaseSnapshot{Name: "start_child", Data: StartChild{}},
	}, nil
}

func (w GreetingParentWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	switch phase.Name {
	case "start_child":
		return []durable.DurableWait{durable.RunWait(now)}, nil
	case "waiting_for_child":
		data, err := durable.DecodeJSON[WaitingForChild](phase.Data)
		if err != nil {
			return nil, err
		}
		return []durable.DurableWait{durable.ChildWait("greeting_finished", data.Greeting)}, nil
	default:
		return nil, nil
	}
}

func (w GreetingParentWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, rawCommon durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	common, err := durable.DecodeJSON[GreetingCommon](rawCommon)
	if err != nil {
		return durable.Fail(err), nil
	}
	handle, err := dctx.ChildStart(ctx, "greeting", w.Child, GreetingInput{Name: common.Name}, durable.DefaultChildOptions())
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Go(durable.PhaseSnapshot{
		Name: "waiting_for_child",
		Data: WaitingForChild{Greeting: handle},
	}), nil
}

func (w GreetingParentWorkflow) DispatchEvent(_ context.Context, dctx *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "greeting_finished" {
		return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
	}
	if event.Event == nil || !event.Event.OK {
		return durable.Complete(GreetingParentOutput{Greeting: "child failed", CompletedAt: demoutil.ISO(dctx.Now())}), nil
	}
	output, err := durable.DecodeJSON[GreetingChildOutput](event.Event.Output)
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(GreetingParentOutput{
		Greeting:    output.Greeting,
		CompletedAt: demoutil.ISO(dctx.Now()),
	}), nil
}

func (w GreetingParentWorkflow) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (w GreetingParentWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func main() {
	ctx := context.Background()
	child := GreetingChildWorkflow{}
	parent := GreetingParentWorkflow{Child: child}
	workflows := []durable.Workflow{parent, child}
	demo, err := demoutil.NewRuntime(ctx, "child-workflow", workflows)
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = demoutil.CleanupStore("child-workflow")
	}()
	defer func() {
		_ = demo.Provider.Close(context.Background())
	}()

	ref, err := demo.Runtime.Start(ctx, parent, GreetingInput{Name: "Ada"}, durable.StartOptions{WorkflowID: "child-demo"})
	if err != nil {
		exit(err)
	}
	if _, err := demo.Runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		exit(err)
	}
	committed, err := demoutil.Committed(ctx, demo.Provider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("child workflow: parent waiting", committed)

	if err := demo.Provider.Close(ctx); err != nil {
		exit(err)
	}
	restartedProvider, restarted, err := demoutil.Restart(ctx, "child-workflow", demo.Clock, demo.WorkerID, workflows)
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = restartedProvider.Close(context.Background())
	}()
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	committed, err = demoutil.Committed(ctx, restartedProvider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("child workflow: completed after restart", committed)
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
