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

type ImmediateInput struct {
	Name string `json:"name"`
}

type ImmediateOutput struct {
	Message    string `json:"message"`
	ApprovedAt string `json:"approvedAt"`
}

type ImmediateCommon struct {
	Name string `json:"name"`
}

type BootImmediately struct{}

type WaitingForApproval struct {
	EnteredAt string `json:"enteredAt"`
}

type AcknowledgeImmediately struct {
	Message string `json:"message"`
}

type ApprovedSignal struct {
	Message string `json:"message"`
}

type ApprovalStatus struct {
	Sequence int64  `json:"sequence"`
	Status   string `json:"status"`
	Phase    string `json:"phase,omitempty"`
}

type ApprovalRecord struct {
	Name       string `json:"name"`
	Message    string `json:"message"`
	RecordedAt string `json:"recordedAt"`
}

//durable:workflow name=demo_immediate_approval version=1 input=ImmediateInput output=ImmediateOutput common=ImmediateCommon
type ImmediateApprovalWorkflow struct{}

//durable:phase name=boot_immediately run state=BootImmediately
func (ImmediateApprovalWorkflow) BootImmediatelyPhase() {}

//durable:phase name=waiting_for_approval state=WaitingForApproval
func (ImmediateApprovalWorkflow) WaitingForApprovalPhase() {}

//durable:signal name=approved payload=ApprovedSignal
func (ImmediateApprovalWorkflow) Approved() {}

//durable:phase name=acknowledge_immediately run state=AcknowledgeImmediately
func (ImmediateApprovalWorkflow) AcknowledgeImmediatelyPhase() {}

//durable:query name=status output=ApprovalStatus
func (ImmediateApprovalWorkflow) Status() {}

func (ImmediateApprovalWorkflow) Name() string { return ImmediateApprovalWorkflowContract.Name }
func (ImmediateApprovalWorkflow) Version() int { return ImmediateApprovalWorkflowContract.Version }

func (ImmediateApprovalWorkflow) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[ImmediateInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: ImmediateCommon{Name: input.Name},
		Phase:  durable.PhaseSnapshot{Name: "boot_immediately", Data: BootImmediately{}},
	}, nil
}

func (ImmediateApprovalWorkflow) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, now time.Time) ([]durable.DurableWait, error) {
	switch phase.Name {
	case "boot_immediately", "acknowledge_immediately":
		return []durable.DurableWait{durable.RunWait(now)}, nil
	case "waiting_for_approval":
		return []durable.DurableWait{durable.SignalWait("approved", "approved", false)}, nil
	default:
		return nil, nil
	}
}

func (ImmediateApprovalWorkflow) DispatchRun(ctx context.Context, dctx *durable.Context, rawCommon durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	switch phase.Name {
	case "boot_immediately":
		return durable.Go(durable.PhaseSnapshot{
			Name: "waiting_for_approval",
			Data: WaitingForApproval{EnteredAt: demoutil.ISO(dctx.Now())},
		}), nil
	case "acknowledge_immediately":
		common, err := durable.DecodeJSON[ImmediateCommon](rawCommon)
		if err != nil {
			return durable.Fail(err), nil
		}
		data, err := durable.DecodeJSON[AcknowledgeImmediately](phase.Data)
		if err != nil {
			return durable.Fail(err), nil
		}
		if _, err := durable.Activity[ApprovalRecord](ctx, dctx, "record_approval", func(context.Context) (ApprovalRecord, error) {
			return ApprovalRecord{
				Name:       common.Name,
				Message:    data.Message,
				RecordedAt: demoutil.ISO(dctx.Now()),
			}, nil
		}); err != nil {
			return durable.Fail(err), nil
		}
		return durable.Complete(ImmediateOutput{
			Message:    fmt.Sprintf("%s: %s", common.Name, data.Message),
			ApprovedAt: demoutil.ISO(dctx.Now()),
		}), nil
	default:
		return durable.Fail(durable.Errf("unknown run phase %q", phase.Name)), nil
	}
}

func (ImmediateApprovalWorkflow) DispatchEvent(_ context.Context, _ *durable.Context, _ durable.JSON, _ durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "approved" {
		return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
	}
	payload, err := durable.DecodeJSON[ApprovedSignal](event.Payload)
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Go(durable.PhaseSnapshot{
		Name: "acknowledge_immediately",
		Data: AcknowledgeImmediately{Message: payload.Message},
	}), nil
}

func (ImmediateApprovalWorkflow) Query(_ context.Context, name string, query durable.QueryContext) (durable.JSON, error) {
	if name != "status" {
		return nil, durable.Errf("unknown query %q", name)
	}
	status := ApprovalStatus{Sequence: query.Sequence, Status: query.Snapshot.Status}
	if query.Snapshot.Status == "running" && query.Snapshot.Phase != nil {
		status.Phase = query.Snapshot.Phase.Name
	}
	return status, nil
}

func (ImmediateApprovalWorkflow) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func main() {
	ctx := context.Background()
	workflow := ImmediateApprovalWorkflow{}
	demo, err := demoutil.NewRuntime(ctx, "immediate-and-signal", []durable.Workflow{workflow})
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = demo.Provider.Close(context.Background())
		_ = demoutil.CleanupStore("immediate-and-signal")
	}()

	ref, err := demo.Runtime.Start(ctx, workflow, ImmediateInput{Name: "Ada"}, durable.StartOptions{WorkflowID: "immediate-demo"})
	if err != nil {
		exit(err)
	}
	if _, err := demo.Runtime.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	status, err := demo.Runtime.Query(ctx, workflow, ref, "status")
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("immediate + signal: after immediate boot", status)

	if _, err := demo.Runtime.Signal(ctx, workflow, ref, "approved", ApprovedSignal{Message: "ship it"}); err != nil {
		exit(err)
	}
	if _, err := demo.Runtime.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	committed, err := demoutil.Committed(ctx, demo.Provider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("immediate + signal: completed", committed)
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
