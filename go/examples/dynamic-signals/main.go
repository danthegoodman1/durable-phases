package main

import (
	"context"
	"fmt"
	"time"

	durable "github.com/danthegoodman1/durable-phases/go"
	"github.com/danthegoodman1/durable-phases/go/examples/internal/demoutil"
)

type DynamicWorkflow struct{}

type DynamicInput struct {
	RequestID string `json:"requestId"`
}

type DynamicState struct {
	JobID     string            `json:"jobId"`
	Pending   []string          `json:"pending"`
	Approvals map[string]string `json:"approvals"`
}

func (DynamicWorkflow) Name() string { return "demo_dynamic_signals" }
func (DynamicWorkflow) Version() int { return 1 }
func (DynamicWorkflow) Query(context.Context, string, durable.QueryContext) (durable.JSON, error) {
	return nil, nil
}
func (DynamicWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}
func (DynamicWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[DynamicInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	_ = input
	return durable.Start{Phase: durable.PhaseSnapshot{Name: "waiting", Data: DynamicState{Approvals: map[string]string{}}}}, nil
}
func (DynamicWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	data, err := durable.DecodeJSON[DynamicState](phase.Data)
	if err != nil {
		return nil, err
	}
	waits := []durable.DurableWait{
		durable.SignalWait("configure", "configure", false),
		durable.SignalWait("cancel", "cancel", false),
	}
	if data.JobID == "" {
		return waits, nil
	}
	waits = append(waits, durable.SignalWaitWithOptions("provider:"+data.JobID, "provider_result:"+data.JobID, false, durable.SignalWaitOptions{
		Handler: "provider_result",
		Meta:    data.JobID,
	}))
	for _, approverID := range data.Pending {
		waits = append(waits, durable.SignalWaitWithOptions("approval:"+approverID, "approval:"+approverID, false, durable.SignalWaitOptions{
			Handler: "approval",
			Meta:    approverID,
		}))
	}
	return waits, nil
}
func (DynamicWorkflow) DispatchRun(context.Context, *durable.Context, durable.JSON, durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("unexpected run")), nil
}
func (DynamicWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	data, err := durable.DecodeJSON[DynamicState](phase.Data)
	if err != nil {
		return durable.Transition{}, err
	}
	switch waitName {
	case "configure":
		payload, err := durable.DecodeJSON[struct {
			JobID     string   `json:"jobId"`
			Approvers []string `json:"approvers"`
		}](event.Payload)
		if err != nil {
			return durable.Transition{}, err
		}
		return durable.Stay(durable.PhaseSnapshot{Name: "waiting", Data: DynamicState{
			JobID: payload.JobID, Pending: payload.Approvers, Approvals: map[string]string{},
		}}), nil
	case "cancel":
		return durable.Cancel("canceled"), nil
	case "provider_result":
		return durable.Stay(phase), nil
	case "approval":
		approverID, err := durable.WaitMeta[string](event)
		if err != nil {
			return durable.Transition{}, err
		}
		payload, err := durable.DecodeJSON[struct {
			Decision string `json:"decision"`
		}](event.Payload)
		if err != nil {
			return durable.Transition{}, err
		}
		data.Approvals[approverID] = payload.Decision
		pending := data.Pending[:0]
		for _, id := range data.Pending {
			if id != approverID {
				pending = append(pending, id)
			}
		}
		data.Pending = pending
		if len(data.Pending) == 0 {
			return durable.Complete(map[string]any{"jobId": data.JobID, "approvals": data.Approvals}), nil
		}
		return durable.Stay(durable.PhaseSnapshot{Name: "waiting", Data: data}), nil
	default:
		return durable.Fail(durable.Errf("unexpected wait %s", waitName)), nil
	}
}

func main() {
	ctx := context.Background()
	name := "go-dynamic-signals"
	workflow := DynamicWorkflow{}
	demo, err := demoutil.NewRuntime(ctx, name, []durable.Workflow{workflow})
	if err != nil {
		panic(err)
	}
	defer demoutil.CleanupStore(name)

	ref, err := demo.Runtime.Start(ctx, workflow, DynamicInput{RequestID: "request-1"}, durable.StartOptions{WorkflowID: "dynamic-signals-demo"})
	if err != nil {
		panic(err)
	}
	if _, err := demo.Runtime.Signal(ctx, workflow, ref, "configure", map[string]any{
		"jobId":     "job-1",
		"approvers": []string{"ada", "grace"},
	}); err != nil {
		panic(err)
	}
	if _, err := demo.Runtime.Drain(ctx, durable.DrainOptions{}); err != nil {
		panic(err)
	}
	provider, restarted, err := demoutil.Restart(ctx, name, demo.Clock, "go-dynamic-signals-restarted", []durable.Workflow{workflow})
	if err != nil {
		panic(err)
	}
	if _, err := restarted.Signal(ctx, workflow, ref, "provider_result:job-1", map[string]any{"ok": true}); err != nil {
		panic(err)
	}
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		panic(err)
	}
	for _, approver := range []string{"ada", "grace"} {
		if _, err := restarted.Signal(ctx, workflow, ref, "approval:"+approver, map[string]any{"decision": "yes"}); err != nil {
			panic(err)
		}
		if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
			panic(err)
		}
	}
	committed, err := demoutil.Committed(ctx, provider, ref)
	if err != nil {
		panic(err)
	}
	demoutil.PrintJSON("dynamic signals: completed", committed)
	fmt.Println()
}
