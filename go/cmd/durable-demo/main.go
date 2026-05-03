package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
)

var demoStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type fixedClock struct {
	now time.Time
}

func (c *fixedClock) Now() time.Time          { return c.now }
func (c *fixedClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type demoWorkflow struct {
	name    string
	version int
	initial func(context.Context, durable.JSON) (durable.Start, error)
	waits   func(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error)
	run     func(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error)
	event   func(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error)
	query   func(context.Context, string, durable.QueryContext) (durable.JSON, error)
	migrate func(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error)
}

func (w demoWorkflow) Name() string { return w.name }
func (w demoWorkflow) Version() int {
	if w.version == 0 {
		return 1
	}
	return w.version
}
func (w demoWorkflow) Initial(ctx context.Context, input durable.JSON) (durable.Start, error) {
	return w.initial(ctx, input)
}
func (w demoWorkflow) MaterializeWaits(ctx context.Context, common durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	return w.waits(ctx, common, phase, now)
}
func (w demoWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, common durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	if w.run == nil {
		return durable.Fail(durable.Errf("no run handler")), nil
	}
	return w.run(ctx, dctx, common, phase)
}
func (w demoWorkflow) DispatchEvent(ctx context.Context, dctx *durable.Context, common durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if w.event == nil {
		return durable.Fail(durable.Errf("no event handler")), nil
	}
	return w.event(ctx, dctx, common, phase, waitName, event)
}
func (w demoWorkflow) Query(ctx context.Context, name string, query durable.QueryContext) (durable.JSON, error) {
	if w.query == nil {
		return query.Snapshot, nil
	}
	return w.query(ctx, name, query)
}
func (w demoWorkflow) Migrate(ctx context.Context, from int, args durable.MigrationArgs) (*durable.MigrationResult, error) {
	if w.migrate == nil {
		return nil, nil
	}
	return w.migrate(ctx, from, args)
}

func main() {
	ctx := context.Background()
	name := "immediate-and-signal"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	var err error
	switch name {
	case "immediate-and-signal":
		err = immediateAndSignal(ctx)
	case "timer-stay-restart":
		err = timerStayRestart(ctx)
	case "stay-loop":
		err = stayLoop(ctx)
	case "child-workflow":
		err = childWorkflow(ctx)
	case "migration":
		err = migration(ctx)
	default:
		err = fmt.Errorf("unknown demo %q", name)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func immediateAndSignal(ctx context.Context) error {
	clock := &fixedClock{now: demoStart}
	provider := shardengine.New()
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo", Clock: clock.Now})
	if err != nil {
		return err
	}
	workflow := demoWorkflow{
		name: "demo_immediate_signal",
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
		},
		waits: func(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
			if phase.Name == "boot" {
				return []durable.DurableWait{durable.RunWait(now)}, nil
			}
			return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
		},
		run: func(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
			return durable.Go(durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}), nil
		},
		event: func(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, _ string, event durable.ReadyEvent) (durable.Transition, error) {
			return durable.Complete(event.Payload), nil
		},
	}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "demo-immediate-signal"})
	if err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		return err
	}
	if _, err := runtime.Signal(ctx, workflow, ref, "finish", map[string]any{"ok": true}); err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		return err
	}
	return printInstance(ctx, provider, ref)
}

func timerStayRestart(ctx context.Context) error {
	clock := &fixedClock{now: demoStart}
	dir, err := os.MkdirTemp("", "durable-go-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "demo.sqlite")
	provider, err := sqliteprovider.New(path, sqliteprovider.Options{})
	if err != nil {
		return err
	}
	workflow := timerWorkflow(demoStart.Add(time.Minute))
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo", Clock: clock.Now, Workflows: []durable.Workflow{workflow}})
	if err != nil {
		return err
	}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "demo-timer-restart"})
	if err != nil {
		return err
	}
	if err := provider.Close(ctx); err != nil {
		return err
	}
	restarted, err := sqliteprovider.New(path, sqliteprovider.Options{})
	if err != nil {
		return err
	}
	defer restarted.Close(ctx)
	clock.Advance(time.Minute)
	runtime, err = durable.NewRuntime(restarted, durable.RuntimeOptions{WorkerID: "demo-restarted", Clock: clock.Now, Workflows: []durable.Workflow{workflow}})
	if err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		return err
	}
	return printInstance(ctx, restarted, ref)
}

func stayLoop(ctx context.Context) error {
	clock := &fixedClock{now: demoStart}
	provider := shardengine.New()
	workflow := demoWorkflow{
		name: "demo_stay_loop",
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "loop", Data: map[string]any{"n": float64(0)}}}, nil
		},
		waits: func(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
			if phase.Name == "loop" {
				return []durable.DurableWait{durable.RunWait(now)}, nil
			}
			return nil, nil
		},
		run: func(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
			data, err := durable.DecodeJSON[map[string]float64](phase.Data)
			if err != nil {
				return durable.Fail(err), nil
			}
			next := data["n"] + 1
			if next >= 3 {
				return durable.Complete(map[string]any{"loops": next}), nil
			}
			return durable.Stay(durable.PhaseSnapshot{Name: "loop", Data: map[string]any{"n": next}}), nil
		},
	}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo", Clock: clock.Now, Workflows: []durable.Workflow{workflow}})
	if err != nil {
		return err
	}
	ref, err := runtime.Start(ctx, workflow, map[string]any{}, durable.StartOptions{WorkflowID: "demo-stay-loop"})
	if err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 3}); err != nil {
		return err
	}
	return printInstance(ctx, provider, ref)
}

func childWorkflow(ctx context.Context) error {
	clock := &fixedClock{now: demoStart}
	provider := shardengine.New()
	child := demoWorkflow{
		name: "demo_child",
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
		},
		waits: func(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
			if phase.Name == "boot" {
				return []durable.DurableWait{durable.RunWait(now)}, nil
			}
			return nil, nil
		},
		run: func(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
			return durable.Complete(map[string]any{"child": "done"}), nil
		},
	}
	parent := demoWorkflow{
		name: "demo_parent",
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
		},
		waits: func(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
			if phase.Name == "boot" {
				return []durable.DurableWait{durable.RunWait(now)}, nil
			}
			handle, err := durable.DecodeJSON[durable.ChildHandleAny](phase.Data)
			if err != nil {
				return nil, err
			}
			return []durable.DurableWait{durable.ChildWait("child_done", handle)}, nil
		},
		run: func(ctx context.Context, dctx *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
			handle, err := dctx.ChildStart(ctx, "child", child, map[string]any{}, durable.DefaultChildOptions())
			if err != nil {
				return durable.Fail(err), nil
			}
			return durable.Go(durable.PhaseSnapshot{Name: "waiting_child", Data: handle}), nil
		},
		event: func(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, _ string, event durable.ReadyEvent) (durable.Transition, error) {
			return durable.Complete(event.Event.Output), nil
		},
	}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo", Clock: clock.Now, Workflows: []durable.Workflow{parent, child}})
	if err != nil {
		return err
	}
	ref, err := runtime.Start(ctx, parent, map[string]any{}, durable.StartOptions{WorkflowID: "demo-child-workflow"})
	if err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 3}); err != nil {
		return err
	}
	return printInstance(ctx, provider, ref)
}

func migration(ctx context.Context) error {
	clock := &fixedClock{now: demoStart}
	provider := shardengine.New()
	v1 := migrationWorkflow(1)
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo-v1", Clock: clock.Now, Workflows: []durable.Workflow{v1}})
	if err != nil {
		return err
	}
	ref, err := runtime.Start(ctx, v1, map[string]any{}, durable.StartOptions{WorkflowID: "demo-migration"})
	if err != nil {
		return err
	}
	v2 := migrationWorkflow(2)
	runtime, err = durable.NewRuntime(provider, durable.RuntimeOptions{WorkerID: "demo-v2", Clock: clock.Now, Workflows: []durable.Workflow{v2}})
	if err != nil {
		return err
	}
	if _, err := runtime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		return err
	}
	return printInstance(ctx, provider, ref)
}

func printInstance(ctx context.Context, provider durable.DurabilityProvider, ref durable.InstanceRef) error {
	instance, err := provider.LoadInstance(ctx, ref, durable.LoadInstanceOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("%s/%s status=%s output=%v common=%v\n", ref.WorkflowID, ref.RunID, instance.Status, instance.Output, instance.Common)
	return nil
}

func timerWorkflow(due time.Time) durable.Workflow {
	return demoWorkflow{
		name: "demo_timer_restart",
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{}, Phase: durable.PhaseSnapshot{Name: "waiting", Data: map[string]any{}}}, nil
		},
		waits: func(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
			return []durable.DurableWait{durable.TimerWait("wake", due)}, nil
		},
		event: func(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
			return durable.Complete(map[string]any{"woke": true}), nil
		},
	}
}

func migrationWorkflow(version int) durable.Workflow {
	return demoWorkflow{
		name:    "demo_migration",
		version: version,
		initial: func(context.Context, durable.JSON) (durable.Start, error) {
			return durable.Start{Common: map[string]any{"version": float64(version)}, Phase: durable.PhaseSnapshot{Name: "hold", Data: map[string]any{}}}, nil
		},
		waits: func(context.Context, durable.JSON, durable.PhaseSnapshot, time.Time) ([]durable.DurableWait, error) {
			return nil, nil
		},
		migrate: func(_ context.Context, from int, args durable.MigrationArgs) (*durable.MigrationResult, error) {
			phase := durable.PhaseSnapshot{Name: args.Phase.Name, Data: map[string]any{"from": float64(from)}}
			return &durable.MigrationResult{Common: map[string]any{"version": float64(from + 1), "migrated": true}, Phase: &phase}, nil
		},
	}
}
