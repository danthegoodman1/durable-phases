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

type ReminderInput struct {
	Name string `json:"name"`
}

type ReminderOutput struct {
	RemindersSent int `json:"remindersSent"`
}

type ReminderCommon struct {
	Name string `json:"name"`
}

type ReminderWaiting struct {
	RemindersSent int       `json:"remindersSent"`
	NextReminder  time.Time `json:"nextReminderAt"`
}

type DoneSignal struct{}

type ProgressQuery struct {
	Sequence      int64  `json:"sequence"`
	Status        string `json:"status"`
	RemindersSent *int   `json:"remindersSent,omitempty"`
}

type ReminderRecord struct {
	Name   string `json:"name"`
	SentAt string `json:"sentAt"`
}

//durable:workflow name=demo_timer_stay version=1 input=ReminderInput output=ReminderOutput common=ReminderCommon
type ReminderWorkflow struct{}

//durable:phase name=waiting state=ReminderWaiting
func (ReminderWorkflow) WaitingPhase() {}

//durable:timer name=reminder_due at=NextReminder
func (ReminderWorkflow) ReminderDue() {}

//durable:signal name=done payload=DoneSignal
func (ReminderWorkflow) Done() {}

//durable:query name=progress output=ProgressQuery
func (ReminderWorkflow) Progress() {}

func (ReminderWorkflow) Name() string { return ReminderWorkflowContract.Name }
func (ReminderWorkflow) Version() int { return ReminderWorkflowContract.Version }

func (ReminderWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[ReminderInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: ReminderCommon{Name: input.Name},
		Phase: durable.PhaseSnapshot{
			Name: "waiting",
			Data: ReminderWaiting{RemindersSent: 0, NextReminder: demoutil.Start.Add(time.Second)},
		},
	}, nil
}

func (ReminderWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	if phase.Name != "waiting" {
		return nil, nil
	}
	data, err := durable.DecodeJSON[ReminderWaiting](phase.Data)
	if err != nil {
		return nil, err
	}
	return []durable.DurableWait{
		durable.TimerWait("reminder_due", data.NextReminder),
		durable.SignalWait("done", "done", false),
	}, nil
}

func (ReminderWorkflow) DispatchRun(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("phase %q has no run handler", phase.Name)), nil
}

func (ReminderWorkflow) DispatchEvent(ctx context.Context, dctx *durable.Context, rawCommon durable.JSON, phase durable.PhaseSnapshot, waitName string, _ durable.ReadyEvent) (durable.Transition, error) {
	data, err := durable.DecodeJSON[ReminderWaiting](phase.Data)
	if err != nil {
		return durable.Fail(err), nil
	}
	switch waitName {
	case "reminder_due":
		common, err := durable.DecodeJSON[ReminderCommon](rawCommon)
		if err != nil {
			return durable.Fail(err), nil
		}
		key := fmt.Sprintf("send_reminder_%d", data.RemindersSent+1)
		if _, err := durable.Activity[ReminderRecord](ctx, dctx, key, func(context.Context) (ReminderRecord, error) {
			return ReminderRecord{Name: common.Name, SentAt: demoutil.ISO(dctx.Now())}, nil
		}); err != nil {
			return durable.Fail(err), nil
		}
		return durable.Stay(durable.PhaseSnapshot{
			Name: "waiting",
			Data: ReminderWaiting{
				RemindersSent: data.RemindersSent + 1,
				NextReminder:  dctx.Now().Add(time.Second),
			},
		}), nil
	case "done":
		return durable.Complete(ReminderOutput{RemindersSent: data.RemindersSent}), nil
	default:
		return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
	}
}

func (ReminderWorkflow) Query(_ context.Context, name string, query durable.QueryContext) (durable.JSON, error) {
	if name != "progress" {
		return nil, durable.Errf("unknown query %q", name)
	}
	out := ProgressQuery{Sequence: query.Sequence, Status: query.Snapshot.Status}
	if query.Snapshot.Status == "running" && query.Snapshot.Phase != nil {
		data, err := durable.DecodeJSON[ReminderWaiting](query.Snapshot.Phase.Data)
		if err != nil {
			return nil, err
		}
		out.RemindersSent = &data.RemindersSent
	}
	return out, nil
}

func (ReminderWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func main() {
	ctx := context.Background()
	workflow := ReminderWorkflow{}
	workflows := []durable.Workflow{workflow}
	demo, err := demoutil.NewRuntime(ctx, "timer-stay-restart", workflows)
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = demoutil.CleanupStore("timer-stay-restart")
	}()
	defer func() {
		_ = demo.Provider.Close(context.Background())
	}()

	ref, err := demo.Runtime.Start(ctx, workflow, ReminderInput{Name: "Ada"}, durable.StartOptions{WorkflowID: "timer-demo"})
	if err != nil {
		exit(err)
	}
	progress, err := demo.Runtime.Query(ctx, workflow, ref, "progress")
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("timer + stay: pending timer", progress)

	demo.Clock.Advance(time.Second)
	if err := demo.Provider.Close(ctx); err != nil {
		exit(err)
	}
	restartedProvider, restarted, err := demoutil.Restart(ctx, "timer-stay-restart", demo.Clock, demo.WorkerID, workflows)
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = restartedProvider.Close(context.Background())
	}()
	if _, err := restarted.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	progress, err = restarted.Query(ctx, workflow, ref, "progress")
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("timer + stay: after restart and timer", progress)
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
