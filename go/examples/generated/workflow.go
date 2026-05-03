package generated

import "time"

//go:generate go run github.com/danthegoodman1/durable-phases/go/cmd/durable-gen

type Input struct {
	AccountID string
}

type Output struct {
	Approved bool
}

type Common struct {
	AccountID string
}

type Boot struct{}

type Waiting struct {
	DeadlineAt time.Time
	Child      string
}

//durable:workflow name=generated_example version=2 input=Input output=Output common=Common
type ExampleWorkflow struct{}

//durable:phase name=boot run state=Boot
func (ExampleWorkflow) Boot() {}

//durable:phase name=waiting state=Waiting
type WaitingHandlers struct{}

//durable:signal name=approve payload=Input
func (WaitingHandlers) Approve() {}

//durable:global_signal name=cancel payload=Input
func (ExampleWorkflow) Cancel() {}

//durable:timer name=deadline at=DeadlineAt
func (WaitingHandlers) Deadline() {}

//durable:child name=child_done handle=Child
func (WaitingHandlers) ChildDone() {}

//durable:query name=status output=Output
func (ExampleWorkflow) Status() {}

//durable:migration from=1
func (ExampleWorkflow) FromOne() {}
