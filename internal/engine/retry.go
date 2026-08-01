package engine

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// This file is the decision tree architecture.md §11 draws and nothing had ever executed.
//
// A connector states a [fault.Class], which is a FACT about what happened. Everything downstream of
// that — retry, wait, dead-letter, drop, stop, stall — is BEHAVIOUR the engine computes from
// (class, capabilities, policy). Keeping the two apart is what stops a connector author from
// choosing the pipeline's failure policy by picking an error type, and it is why [route] is a pure
// function: no I/O, no clock of its own, no engine state.

// disposition is what happens to a unit of work that failed.
type disposition uint8

const (
	// dispRetry: wait, then present the same records again.
	dispRetry disposition = iota
	// dispDeadLetter: route on the failed edge, then settle Abandoned.
	dispDeadLetter
	// dispDrop: discard, count, settle Abandoned.
	dispDrop
	// dispStop: stop the pipeline, settle nothing.
	dispStop
	// dispStall: settle nothing and block, because the write MAY have landed.
	dispStall
)

var dispositionNames = [...]string{
	dispRetry: "retry", dispDeadLetter: "dead_letter", dispDrop: "drop",
	dispStop: "stop", dispStall: "stall",
}

func (d disposition) String() string {
	if int(d) < len(dispositionNames) {
		return dispositionNames[d]
	}
	return "stop"
}

// attempt is the retry state of one unit of work.
//
// count holds COUNTED attempts only. Throttled and NotConnected deliberately do not increment it:
// both are the remote saying "not now", neither is the record's fault, and spending a retry budget
// on them is how a source politely honouring a 429 reached TerminalStop against an upstream that
// was working perfectly. Their wait is bounded by Backoff.MaxElapsed and nothing else.
type attempt struct {
	count   int
	started time.Time
}

// routing is one decision plus what an operator needs to understand it.
type routing struct {
	disp  disposition
	delay time.Duration

	// reason is from telemetry's closed vocabulary, so a metric label, a status condition and a log
	// line all say the same word (design rule R9).
	reason string
}

// uncountedFloor is the wait for an uncounted fault whose policy supplies no usable backoff.
//
// RetryPolicy.Validate only checks the backoff fields when MaxAttempts > 1, so a policy of
// {MaxAttempts: 1} is valid with a zero Backoff. A counted fault under that policy goes terminal
// immediately, which is correct — but an uncounted one keeps waiting, and waiting for zero is a
// busy loop that pins a core while an upstream throttles us.
const uncountedFloor = 100 * time.Millisecond

// route decides what to do about one fault.
//
// idempotent is the SINK's declared capability, which is why an indeterminate write is resolvable
// at all: re-sending to a sink that absorbs duplicates is safe, and to one that does not it is a
// choice only the operator can make.
//
// now is a parameter rather than a call to time.Now so the whole tree is testable without sleeping.
func route(err error, idempotent bool, p fault.RetryPolicy, a *attempt, now time.Time) routing {
	class := fault.ClassOf(err)

	// UNCLASSIFIED IS ALWAYS A DEFECT, and pkg/fault says what to do about it: "the engine treats it
	// as PermanentInternal". Without this line a bare error returned by a connector — the single
	// most likely mistake a connector author makes — was retried four times and then routed by the
	// terminal policy, which for a dead-lettering pipeline meant a record discarded because of a
	// bug in the connector rather than anything wrong with the record.
	//
	// The counter this is supposed to increment, telemetry.MUnclassified, has nowhere to go yet:
	// noopMetrics refuses every registration rather than returning a dangling handle. The
	// classification is the load-bearing half and it happens here.
	if class == fault.Unclassified {
		class = fault.PermanentInternal
	}

	// INDETERMINATE IS DECIDED BY CAPABILITY FIRST, POLICY SECOND. The write may have landed, so
	// this is the one branch where "try again" is not obviously safe. An idempotent sink absorbs
	// the duplicate and the fault becomes an ordinary retry; without that capability the answer is
	// the operator's, and its default is to fail loud.
	if class == fault.Indeterminate && !idempotent {
		switch p.OnIndeterminate {
		case fault.IndeterminateRetry:
			// Fall through to the retry arithmetic: duplicates accepted, explicitly.
		case fault.IndeterminateDeadLetter:
			return routing{disp: dispDeadLetter, reason: telemetry.ReasonIndeterminateWrite}
		default:
			return routing{disp: dispStall, reason: telemetry.ReasonIndeterminateWrite}
		}
	}

	// A terminal class cannot be helped by trying again. Going straight to the terminal disposition
	// rather than through the attempt budget is what makes a poison record fail in one attempt
	// instead of four.
	if class.Terminal() {
		return terminalRouting(p, class)
	}

	if class.Counted() {
		a.count++
	}

	// MaxElapsed bounds the TOTAL time spent retrying, counted and uncounted alike. It is the only
	// bound an uncounted fault has, so checking it before the attempt budget is what keeps a
	// permanently throttled pipeline from waiting forever.
	if p.Backoff.MaxElapsed > 0 && !a.started.IsZero() && now.Sub(a.started) >= p.Backoff.MaxElapsed {
		return terminalRouting(p, class)
	}

	// Next is 1-based on the attempt that just failed. An uncounted fault has not spent one, so it
	// asks for the first delay rather than the zeroth, which Next rejects.
	n := a.count
	if n < 1 {
		n = 1
	}
	delay, more := p.Next(n)
	if class.Counted() && !more {
		return terminalRouting(p, class)
	}
	if !more {
		// Uncounted and out of the schedule: hold at the ceiling. This is the state that surfaces
		// as sustained backoff on a RUNNING pipeline rather than as a dead one.
		delay = p.Backoff.Max
	}
	// A Retry-After from the remote wins over our schedule. It is the one number in this function
	// that somebody else measured.
	if hint, ok := fault.RetryAfterOf(err); ok {
		delay = hint
	}
	if delay <= 0 {
		delay = uncountedFloor
	}

	reason := telemetry.ReasonSustainedBackoff
	if class == fault.NotConnected {
		reason = telemetry.ReasonNotConnected
	}
	return routing{disp: dispRetry, delay: delay, reason: reason}
}

// terminalRouting maps the policy's terminal onto a disposition.
//
// TerminalInvalid cannot appear here — RetryPolicy.Validate refuses it at build time and its whole
// purpose is to make "retry forever" inexpressible — so it collapses into stop rather than growing
// a fifth behaviour nobody specified.
func terminalRouting(p fault.RetryPolicy, class fault.Class) routing {
	reason := telemetry.ReasonRetriesExhausted
	if class.Terminal() {
		reason = telemetry.ReasonTerminalFault
	}
	switch p.Terminal {
	case fault.TerminalDeadLetter:
		return routing{disp: dispDeadLetter, reason: reason}
	case fault.TerminalDrop:
		return routing{disp: dispDrop, reason: reason}
	default:
		return routing{disp: dispStop, reason: reason}
	}
}
