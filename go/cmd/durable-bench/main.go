package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/internal/shardengine"
	postgresprovider "github.com/danthegoodman1/durable-phases/go/providers/postgres"
	sqliteprovider "github.com/danthegoodman1/durable-phases/go/providers/sqlite"
)

type options struct {
	provider                string
	mode                    string
	workflows               int
	workers                 int
	shards                  int
	activationConcurrency   int
	activationPrefetchLimit int
	batch                   int
	physicalPartitions      int
	json                    bool
}

type counters struct {
	Workflows        int `json:"workflows"`
	Activities       int `json:"activities"`
	Signals          int `json:"signals"`
	Timers           int `json:"timers"`
	ChildStarts      int `json:"childStarts"`
	ChildCompletions int `json:"childCompletions"`
}

type result struct {
	Provider                     string   `json:"provider"`
	Mode                         string   `json:"mode"`
	Workflows                    int      `json:"workflows"`
	Activations                  int      `json:"activations"`
	Correct                      bool     `json:"correct"`
	DurationMillis               float64  `json:"durationMillis"`
	WorkflowsPerSecond           float64  `json:"workflowsPerSecond"`
	ProcessingWorkflowsPerSecond float64  `json:"processingWorkflowsPerSecond"`
	Counters                     counters `json:"counters"`
}

type benchWorkflow struct {
	mode     string
	counters *counters
	child    durable.Workflow
}

func (w benchWorkflow) Name() string { return "bench_main" }
func (w benchWorkflow) Version() int { return 1 }
func (w benchWorkflow) Initial(_ context.Context, input durable.JSON) (durable.Start, error) {
	index, err := durable.DecodeJSON[map[string]float64](input)
	if err != nil {
		return durable.Start{}, err
	}
	phase := "boot"
	if w.mode == "signal" {
		phase = "waiting_signal"
	}
	if w.mode == "timer" {
		phase = "waiting_timer"
	}
	return durable.Start{
		Common: map[string]any{"index": index["index"]},
		Phase:  durable.PhaseSnapshot{Name: phase, Data: map[string]any{}},
	}, nil
}
func (w benchWorkflow) MaterializeWaits(_ context.Context, common durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	switch phase.Name {
	case "boot":
		return []durable.DurableWait{durable.RunWait(now)}, nil
	case "waiting_signal":
		return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
	case "waiting_timer":
		return []durable.DurableWait{durable.TimerWait("finish_due", now)}, nil
	case "waiting_child":
		handle, err := durable.DecodeJSON[durable.ChildHandleAny](phase.Data)
		if err != nil {
			return nil, err
		}
		return []durable.DurableWait{durable.ChildWait("child_done", handle)}, nil
	default:
		return nil, nil
	}
}
func (w benchWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, common durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	switch w.mode {
	case "bare":
		w.counters.Workflows++
		return durable.Complete(common), nil
	case "activity":
		value, err := durable.Activity[map[string]any](ctx, dctx, "activity", func(context.Context) (map[string]any, error) {
			w.counters.Activities++
			return common.(map[string]any), nil
		})
		if err != nil {
			return durable.Fail(err), nil
		}
		w.counters.Workflows++
		return durable.Complete(value), nil
	case "child", "mixed":
		if w.mode == "mixed" {
			if _, err := durable.Activity[map[string]any](ctx, dctx, "boot_activity", func(context.Context) (map[string]any, error) {
				w.counters.Activities++
				return common.(map[string]any), nil
			}); err != nil {
				return durable.Fail(err), nil
			}
		}
		handle, err := dctx.ChildStart(ctx, "child", w.child, common, durable.DefaultChildOptions())
		if err != nil {
			return durable.Fail(err), nil
		}
		w.counters.ChildStarts++
		return durable.Go(durable.PhaseSnapshot{Name: "waiting_child", Data: handle}), nil
	default:
		return durable.Fail(fmt.Errorf("mode %s has no run handler", w.mode)), nil
	}
}
func (w benchWorkflow) DispatchEvent(ctx context.Context, dctx *durable.Context, common durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	switch event.Kind {
	case "signal":
		w.counters.Signals++
		if w.mode == "mixed" {
			return durable.Go(durable.PhaseSnapshot{
				Name: "waiting_timer",
				Data: map[string]any{
					"child":  phase.Data,
					"signal": event.Payload,
				},
			}), nil
		}
		w.counters.Workflows++
		return durable.Complete(event.Payload), nil
	case "timer":
		w.counters.Timers++
		if w.mode == "mixed" {
			if _, err := durable.Activity[map[string]any](ctx, dctx, "finish_activity", func(context.Context) (map[string]any, error) {
				w.counters.Activities++
				return common.(map[string]any), nil
			}); err != nil {
				return durable.Fail(err), nil
			}
		}
		w.counters.Workflows++
		return durable.Complete(common), nil
	case "child":
		w.counters.ChildCompletions++
		if w.mode == "mixed" {
			return durable.Go(durable.PhaseSnapshot{Name: "waiting_signal", Data: phase.Data}), nil
		}
		w.counters.Workflows++
		if event.Event == nil {
			return durable.Fail(fmt.Errorf("missing child event")), nil
		}
		return durable.Complete(event.Event.Output), nil
	default:
		return durable.Fail(fmt.Errorf("unexpected event %s for %s", event.Kind, waitName)), nil
	}
	return durable.Fail(fmt.Errorf("unhandled event %s", event.Kind)), nil
}
func (w benchWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w benchWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

type benchChildWorkflow struct {
	counters *counters
}

func (w benchChildWorkflow) Name() string { return "bench_child" }
func (w benchChildWorkflow) Version() int { return 1 }
func (w benchChildWorkflow) Initial(_ context.Context, input durable.JSON) (durable.Start, error) {
	return durable.Start{Common: input, Phase: durable.PhaseSnapshot{Name: "boot", Data: map[string]any{}}}, nil
}
func (w benchChildWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "boot" {
		return []durable.DurableWait{durable.RunWait(now)}, nil
	}
	return nil, nil
}
func (w benchChildWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, common durable.JSON, _ durable.PhaseSnapshot) (durable.Transition, error) {
	value, err := durable.Activity[map[string]any](ctx, dctx, "child_activity", func(context.Context) (map[string]any, error) {
		w.counters.Activities++
		return common.(map[string]any), nil
	})
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(value), nil
}
func (w benchChildWorkflow) DispatchEvent(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot, string, durable.ReadyEvent) (durable.Transition, error) {
	return durable.Fail(fmt.Errorf("child workflow has no event handler")), nil
}
func (w benchChildWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (w benchChildWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func main() {
	opts := parse()
	ctx := context.Background()
	provider, cleanup, err := openProvider(ctx, opts)
	if err != nil {
		fatal(err)
	}
	defer cleanup()
	counts := &counters{}
	child := benchChildWorkflow{counters: counts}
	workflow := benchWorkflow{mode: opts.mode, counters: counts, child: child}
	runtime, err := durable.NewRuntime(provider, durable.RuntimeOptions{
		WorkerID:                  "go-bench",
		ShardCount:                opts.shards,
		MaxConcurrentActivations:  opts.activationConcurrency,
		ActivationPrefetchLimit:   opts.activationPrefetchLimit,
		ActivationCommitBatchSize: opts.batch,
		Workflows:                 []durable.Workflow{workflow, child},
		Clock:                     func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		fatal(err)
	}
	started := time.Now()
	refs := make([]durable.InstanceRef, 0, opts.workflows)
	for i := 0; i < opts.workflows; i++ {
		ref, err := runtime.Start(ctx, workflow, map[string]any{"index": float64(i)}, durable.StartOptions{WorkflowID: fmt.Sprintf("bench-%d", i)})
		if err != nil {
			fatal(err)
		}
		refs = append(refs, ref)
		if opts.mode == "signal" || opts.mode == "mixed" {
			if _, err := runtime.Signal(ctx, workflow, ref, "finish", map[string]any{"index": float64(i)}); err != nil {
				fatal(err)
			}
		}
	}
	activations := 0
	for {
		drained, err := runtime.Drain(ctx, durable.DrainOptions{
			MaxActivations:            max(opts.batch, opts.activationPrefetchLimit),
			MaxConcurrentActivations:  opts.activationConcurrency,
			ActivationPrefetchLimit:   opts.activationPrefetchLimit,
			ActivationCommitBatchSize: opts.batch,
		})
		if err != nil {
			fatal(err)
		}
		activations += drained.Activations
		if drained.Activations == 0 {
			break
		}
	}
	elapsed := time.Since(started)
	correct := true
	for _, ref := range refs {
		instance, err := provider.LoadInstance(ctx, ref, durable.LoadInstanceOptions{})
		if err != nil {
			fatal(err)
		}
		if instance == nil || instance.Status != "completed" {
			correct = false
			break
		}
	}
	out := result{
		Provider:                     opts.provider,
		Mode:                         opts.mode,
		Workflows:                    opts.workflows,
		Activations:                  activations,
		Correct:                      correct,
		DurationMillis:               float64(elapsed.Microseconds()) / 1000,
		WorkflowsPerSecond:           float64(opts.workflows) / elapsed.Seconds(),
		ProcessingWorkflowsPerSecond: float64(opts.workflows) / elapsed.Seconds(),
		Counters:                     *counts,
	}
	if opts.json {
		encoded, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(encoded))
		return
	}
	fmt.Printf("Go %s/%s: %.2f workflows/s, correct=%v, activations=%d\n", out.Provider, out.Mode, out.WorkflowsPerSecond, out.Correct, out.Activations)
}

func parse() options {
	opts := options{}
	flag.StringVar(&opts.provider, "provider", "null", "null, sqlite, sqlite-shard-file, or postgres")
	flag.StringVar(&opts.mode, "mode", "mixed", "mixed, bare, activity, signal, timer, or child")
	flag.IntVar(&opts.workflows, "workflows", 250, "workflow count")
	flag.IntVar(&opts.workers, "workers", 1, "accepted for parity with TS/Rust")
	flag.IntVar(&opts.shards, "shards", 4, "dispatch shard count")
	flag.IntVar(&opts.activationConcurrency, "activation-concurrency", 4, "activation concurrency")
	flag.IntVar(&opts.activationPrefetchLimit, "activation-prefetch-limit", 32, "activation prefetch limit")
	flag.IntVar(&opts.batch, "batch", 32, "commit batch size")
	flag.IntVar(&opts.physicalPartitions, "physical-partitions", 4, "postgres physical partitions")
	flag.BoolVar(&opts.json, "json", false, "print JSON")
	flag.Parse()
	if opts.workflows <= 0 || opts.shards <= 0 || opts.activationConcurrency <= 0 || opts.activationPrefetchLimit <= 0 || opts.batch <= 0 {
		fatal(fmt.Errorf("numeric options must be positive"))
	}
	switch opts.mode {
	case "mixed", "bare", "activity", "signal", "timer", "child":
	default:
		fatal(fmt.Errorf("unknown mode %q", opts.mode))
	}
	return opts
}

func openProvider(ctx context.Context, opts options) (durable.DurabilityProvider, func(), error) {
	switch opts.provider {
	case "null":
		provider := shardengine.New()
		return provider, func() { _ = provider.Close(context.Background()) }, nil
	case "sqlite":
		dir, err := os.MkdirTemp("", "durable-go-bench-*")
		if err != nil {
			return nil, nil, err
		}
		provider, err := sqliteprovider.New(filepath.Join(dir, "bench.sqlite"), sqliteprovider.Options{})
		if err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
		return provider, func() {
			_ = provider.Close(context.Background())
			_ = os.RemoveAll(dir)
		}, nil
	case "sqlite-shard-file":
		dir, err := os.MkdirTemp("", "durable-go-bench-shards-*")
		if err != nil {
			return nil, nil, err
		}
		provider, err := sqliteprovider.NewShardFile(sqliteprovider.ShardFileOptions{Directory: dir, ShardCount: opts.shards})
		if err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
		return provider, func() {
			_ = provider.Close(context.Background())
			_ = os.RemoveAll(dir)
		}, nil
	case "postgres":
		conn := os.Getenv("DURABLE_POSTGRES_URL")
		if conn == "" {
			return nil, nil, fmt.Errorf("DURABLE_POSTGRES_URL is required for postgres benchmarks")
		}
		provider, err := postgresprovider.New(ctx, postgresprovider.Options{
			ConnectionString:   conn,
			Schema:             fmt.Sprintf("durable_go_bench_%d", time.Now().UnixNano()),
			PhysicalPartitions: opts.physicalPartitions,
		})
		if err != nil {
			return nil, nil, err
		}
		return provider, func() { _ = provider.Close(context.Background()) }, nil
	default:
		return nil, nil, fmt.Errorf("unknown provider %q", opts.provider)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
