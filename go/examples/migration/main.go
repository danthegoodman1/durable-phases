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

type OrderInput struct {
	CustomerID string `json:"customerId"`
}

type OrderOutput struct {
	Message string `json:"message"`
}

type OrderCommonV1 struct {
	CustomerID string `json:"customerId"`
}

type OrderCommonV2 struct {
	CustomerID string `json:"customerId"`
	Plan       string `json:"plan"`
}

type WaitingV1 struct {
	Salutation string `json:"salutation"`
}

type WaitingForFinishV2 struct {
	Greeting     string `json:"greeting"`
	MigratedFrom string `json:"migratedFrom"`
}

type FinishSignal struct {
	Punctuation string `json:"punctuation"`
}

//durable:workflow name=demo_migrating_order version=1 input=OrderInput output=OrderOutput common=OrderCommonV1
type MigratingOrderV1 struct{}

//durable:phase name=waiting state=WaitingV1
func (MigratingOrderV1) WaitingPhase() {}

//durable:signal name=finish payload=FinishSignal
func (MigratingOrderV1) Finish() {}

//durable:workflow name=demo_migrating_order version=2 input=OrderInput output=OrderOutput common=OrderCommonV2
type MigratingOrderV2 struct{}

//durable:phase name=waiting_for_finish state=WaitingForFinishV2
func (MigratingOrderV2) WaitingForFinishPhase() {}

//durable:signal name=finish payload=FinishSignal
func (MigratingOrderV2) Finish() {}

//durable:migration from=1
func (MigratingOrderV2) FromOne() {}

func (MigratingOrderV1) Name() string { return MigratingOrderV1Contract.Name }
func (MigratingOrderV1) Version() int { return MigratingOrderV1Contract.Version }

func (MigratingOrderV1) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[OrderInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: OrderCommonV1{CustomerID: input.CustomerID},
		Phase:  durable.PhaseSnapshot{Name: "waiting", Data: WaitingV1{Salutation: "hello"}},
	}, nil
}

func (MigratingOrderV1) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "waiting" {
		return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
	}
	return nil, nil
}

func (MigratingOrderV1) DispatchRun(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("phase %q has no run handler", phase.Name)), nil
}

func (MigratingOrderV1) DispatchEvent(_ context.Context, _ *durable.Context, rawCommon durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "finish" {
		return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
	}
	common, err := durable.DecodeJSON[OrderCommonV1](rawCommon)
	if err != nil {
		return durable.Fail(err), nil
	}
	data, err := durable.DecodeJSON[WaitingV1](phase.Data)
	if err != nil {
		return durable.Fail(err), nil
	}
	payload, err := durable.DecodeJSON[FinishSignal](event.Payload)
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(OrderOutput{
		Message: fmt.Sprintf("%s, %s%s", data.Salutation, common.CustomerID, payload.Punctuation),
	}), nil
}

func (MigratingOrderV1) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (MigratingOrderV1) Migrate(context.Context, int, durable.MigrationArgs) (*durable.MigrationResult, error) {
	return nil, nil
}

func (MigratingOrderV2) Name() string { return MigratingOrderV2Contract.Name }
func (MigratingOrderV2) Version() int { return MigratingOrderV2Contract.Version }

func (MigratingOrderV2) Initial(_ context.Context, raw durable.JSON) (durable.Start, error) {
	input, err := durable.DecodeJSON[OrderInput](raw)
	if err != nil {
		return durable.Start{}, err
	}
	return durable.Start{
		Common: OrderCommonV2{CustomerID: input.CustomerID, Plan: "pro"},
		Phase: durable.PhaseSnapshot{
			Name: "waiting_for_finish",
			Data: WaitingForFinishV2{Greeting: "hello", MigratedFrom: "initial"},
		},
	}, nil
}

func (MigratingOrderV2) MaterializeWaits(_ context.Context, _ durable.JSON, phase durable.PhaseSnapshot, _ time.Time) ([]durable.DurableWait, error) {
	if phase.Name == "waiting_for_finish" {
		return []durable.DurableWait{durable.SignalWait("finish", "finish", false)}, nil
	}
	return nil, nil
}

func (MigratingOrderV2) DispatchRun(_ context.Context, _ *durable.Context, _ durable.JSON, phase durable.PhaseSnapshot) (durable.Transition, error) {
	return durable.Fail(durable.Errf("phase %q has no run handler", phase.Name)), nil
}

func (MigratingOrderV2) DispatchEvent(_ context.Context, _ *durable.Context, rawCommon durable.JSON, phase durable.PhaseSnapshot, waitName string, event durable.ReadyEvent) (durable.Transition, error) {
	if waitName != "finish" {
		return durable.Fail(durable.Errf("unexpected wait %q", waitName)), nil
	}
	common, err := durable.DecodeJSON[OrderCommonV2](rawCommon)
	if err != nil {
		return durable.Fail(err), nil
	}
	data, err := durable.DecodeJSON[WaitingForFinishV2](phase.Data)
	if err != nil {
		return durable.Fail(err), nil
	}
	payload, err := durable.DecodeJSON[FinishSignal](event.Payload)
	if err != nil {
		return durable.Fail(err), nil
	}
	return durable.Complete(OrderOutput{
		Message: fmt.Sprintf("%s, %s on %s%s", data.Greeting, common.CustomerID, common.Plan, payload.Punctuation),
	}), nil
}

func (MigratingOrderV2) Query(_ context.Context, _ string, query durable.QueryContext) (durable.JSON, error) {
	return query.Snapshot, nil
}

func (MigratingOrderV2) Migrate(_ context.Context, from int, args durable.MigrationArgs) (*durable.MigrationResult, error) {
	if from != 1 {
		return nil, durable.Errf("unsupported migration from %d", from)
	}
	common, err := durable.DecodeJSON[OrderCommonV1](args.Common)
	if err != nil {
		return nil, err
	}
	data, err := durable.DecodeJSON[WaitingV1](args.Phase.Data)
	if err != nil {
		return nil, err
	}
	phase := durable.PhaseSnapshot{
		Name: "waiting_for_finish",
		Data: WaitingForFinishV2{Greeting: data.Salutation, MigratedFrom: args.Phase.Name},
	}
	return &durable.MigrationResult{
		Common: OrderCommonV2{CustomerID: common.CustomerID, Plan: "starter"},
		Phase:  &phase,
	}, nil
}

func main() {
	ctx := context.Background()
	v1 := MigratingOrderV1{}
	demo, err := demoutil.NewRuntime(ctx, "migration", []durable.Workflow{v1})
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = demoutil.CleanupStore("migration")
	}()
	defer func() {
		_ = demo.Provider.Close(context.Background())
	}()

	ref, err := demo.Runtime.Start(ctx, v1, OrderInput{CustomerID: "Ada"}, durable.StartOptions{WorkflowID: "migration-demo"})
	if err != nil {
		exit(err)
	}
	committed, err := demoutil.Committed(ctx, demo.Provider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("migration: v1 persisted", committed)

	if err := demo.Provider.Close(ctx); err != nil {
		exit(err)
	}
	v2 := MigratingOrderV2{}
	upgradedProvider, upgradedRuntime, err := demoutil.Restart(ctx, "migration", demo.Clock, demo.WorkerID, []durable.Workflow{v2})
	if err != nil {
		exit(err)
	}
	defer func() {
		_ = upgradedProvider.Close(context.Background())
	}()
	if _, err := upgradedRuntime.Drain(ctx, durable.DrainOptions{MaxActivations: 1}); err != nil {
		exit(err)
	}
	committed, err = demoutil.Committed(ctx, upgradedProvider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("migration: after v2 migration checkpoint", committed)

	if _, err := upgradedRuntime.Signal(ctx, v2, ref, "finish", FinishSignal{Punctuation: "!"}); err != nil {
		exit(err)
	}
	if _, err := upgradedRuntime.Drain(ctx, durable.DrainOptions{}); err != nil {
		exit(err)
	}
	committed, err = demoutil.Committed(ctx, upgradedProvider, ref)
	if err != nil {
		exit(err)
	}
	demoutil.PrintJSON("migration: completed on v2", committed)
}

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
