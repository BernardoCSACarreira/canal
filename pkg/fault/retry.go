package fault

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// RETRY-SAFETY OBLIGATION. Stated here because the compiler cannot enforce it, and
// nothing else does yet either — asserting it with injected faults is a named case
// for ADR 0023's conformance kit, which is not built:
//
//	A connector may return TransientUpstream ONLY when it knows the effect did not
//	land. If the remote MAY have applied the write, the class is Indeterminate,
//	PermanentUpstream or DuplicateIdempotent — never TransientUpstream.
//
// This is driver.ErrBadConn's rule ("should NOT be returned if there's a possibility
// that the database server might have performed the operation") generalised, and it
// is design rules R4 and R5 in one sentence. [Indeterminate] exists so that obeying
// this rule does not require lying.

// RetryPolicy is the complete retry contract.
//
// There is no "retry forever" value: [Terminal] has no valid zero, so a policy that
// never gives up cannot be constructed. A framework whose default is unbounded
// livelocks on a poison record, and the community-reported symptom is a pipeline
// making no progress. canal refuses the option.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the first. It must be at
	// least 1; 1 means no retry.
	MaxAttempts int `json:"max_attempts"`

	Backoff Backoff `json:"backoff"`

	// Terminal is what happens when the attempts are exhausted. Required:
	// [TerminalInvalid] is rejected by Validate.
	Terminal Terminal `json:"terminal"`

	// OnIndeterminate decides what happens to a record whose write may or may not
	// have landed, when the sink is not idempotent. Default [IndeterminateStall].
	OnIndeterminate Indeterminacy `json:"on_indeterminate"`
}

// Terminal is what the engine does with a record whose retries are exhausted.
type Terminal uint8

const (
	// TerminalInvalid is the zero value; Validate rejects it. Its existence is what
	// makes "retry forever" inexpressible rather than merely discouraged.
	TerminalInvalid Terminal = iota
	// TerminalDeadLetter routes the record on the node's Failed edge and settles it
	// Abandoned.
	TerminalDeadLetter
	// TerminalDrop discards the record, counts it, and settles it Abandoned.
	TerminalDrop
	// TerminalStop stops the pipeline and settles nothing.
	TerminalStop
)

var terminalNames = [...]string{
	TerminalInvalid:    "invalid",
	TerminalDeadLetter: "dead_letter",
	TerminalDrop:       "drop",
	TerminalStop:       "stop",
}

// String returns the stable snake_case token for t.
func (t Terminal) String() string {
	if int(t) < len(terminalNames) {
		return terminalNames[t]
	}
	return "invalid"
}

// Indeterminacy is what the engine does with an [Indeterminate] write against a
// non-idempotent sink.
type Indeterminacy uint8

const (
	// IndeterminateStall is the default: settle nothing, block the lane loudly, and
	// raise a degraded condition naming the record. Failing loud on an ambiguous
	// write is the correct default for a data-movement tool.
	IndeterminateStall Indeterminacy = iota
	// IndeterminateRetry accepts possible duplicates.
	IndeterminateRetry
	// IndeterminateDeadLetter accepts possible duplicates in the dead-letter route.
	IndeterminateDeadLetter
)

var indeterminacyNames = [...]string{
	IndeterminateStall:      "stall",
	IndeterminateRetry:      "retry",
	IndeterminateDeadLetter: "dead_letter",
}

// String returns the stable snake_case token for i.
func (i Indeterminacy) String() string {
	if int(i) < len(indeterminacyNames) {
		return indeterminacyNames[i]
	}
	return "stall"
}

// Backoff is full-jitter exponential — the only strategy canal offers — plus an
// escalation.
//
// Following database/sql's retry loop, the engine precedes the final attempt with a
// forced reconnect, because an escalation is more useful than one more identical
// try.
type Backoff struct {
	Initial time.Duration `json:"initial"`
	Max     time.Duration `json:"max"`
	// MaxElapsed bounds the total time spent retrying. Zero means bounded only by
	// MaxAttempts.
	MaxElapsed time.Duration `json:"max_elapsed,omitempty"`
	Multiplier float64       `json:"multiplier"`
}

// DefaultBackoff is the policy table canal applies unless the operator overrides it:
// full-jitter exponential from 100ms to 5s, doubling.
var DefaultBackoff = Backoff{Initial: 100 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2}

// DefaultRetry is the core-supplied default, labelled as core-supplied in
// Negotiated.Defaults so an operator can see it was not their choice (design rule
// R10).
//
// The terminal default is [TerminalStop], and the reasoning is worth stating because
// it is not the obvious choice. [TerminalDeadLetter] is the better behaviour, but it
// is only valid when the graph HAS a route for failed records — Build refuses a
// dead-lettering policy with nowhere to dead-letter, because a dead letter with no
// destination is silent loss wearing a safe-looking name. [TerminalDrop] loses data by
// design. So the only terminal a two-node pipeline can be given WITHOUT the operator
// having made a choice, and without losing a record, is to stop loudly.
//
// An operator who adds a failed edge should set dead_letter explicitly; the
// negotiation labels this value as core-supplied precisely so they can see they have
// not.
var DefaultRetry = RetryPolicy{
	MaxAttempts:     4,
	Backoff:         DefaultBackoff,
	Terminal:        TerminalStop,
	OnIndeterminate: IndeterminateStall,
}

// Validate reports every way p is unusable, as one error.
func (p RetryPolicy) Validate() error {
	var errs []error
	if p.MaxAttempts < 1 {
		errs = append(errs, errors.New("retry: max_attempts must be at least 1"))
	}
	if p.Terminal == TerminalInvalid {
		errs = append(errs, errors.New("retry: terminal is required; there is no unbounded retry"))
	}
	if p.MaxAttempts > 1 {
		if p.Backoff.Initial <= 0 {
			errs = append(errs, errors.New("retry: backoff.initial must be positive"))
		}
		if p.Backoff.Max < p.Backoff.Initial {
			errs = append(errs, errors.New("retry: backoff.max must be at least backoff.initial"))
		}
		if p.Backoff.Multiplier < 1 {
			errs = append(errs, errors.New("retry: backoff.multiplier must be at least 1"))
		}
	}
	return errors.Join(errs...)
}

// Next returns how long to wait before the given attempt and whether another attempt
// is permitted at all. attempt is 1-based and names the attempt that just failed, so
// Next(1) is the delay before attempt 2.
//
// The delay is full-jitter: uniform in [0, exponential], which is the only strategy
// canal offers because a table of strategies is a table of ways to get it wrong.
func (p RetryPolicy) Next(attempt int) (time.Duration, bool) {
	if attempt < 1 || attempt >= p.MaxAttempts {
		return 0, false
	}
	m := p.Backoff.Multiplier
	if !(m >= 1) { // also catches NaN, which no comparison against 1 would
		m = 1
	}
	exp := float64(p.Backoff.Initial) * math.Pow(m, float64(attempt-1))

	// THE CEILING IS ALWAYS APPLIED, including when the policy names none.
	//
	// Validate guarantees Max >= Initial > 0 for any policy that retries, so a configured pipeline
	// never reaches the second branch. But this is a public method on a public type and an
	// unvalidated value is reachable — and without a ceiling the exponential grows without bound:
	// at attempt 20 with a multiplier of 10 it passes 1e28, and converting a float64 that large to
	// int64 is IMPLEMENTATION-DEFINED. arm64 saturates to MaxInt64 and yields a 218-year delay;
	// x86 returns the integer indefinite value, which is NEGATIVE, and rand.Int64N panics on it.
	// A retry helper that panics on one architecture and sleeps for two centuries on another is not
	// a difference worth preserving.
	ceiling := float64(p.Backoff.Max)
	if p.Backoff.Max <= 0 {
		ceiling = float64(unboundedCeiling)
	}
	if !(exp <= ceiling) { // also catches NaN and +Inf
		exp = ceiling
	}
	if exp <= 0 {
		return 0, true
	}
	return time.Duration(rand.Int64N(int64(exp)) + 1), true
}

// unboundedCeiling bounds the backoff of a policy that never went through Validate.
//
// It is arithmetic safety rather than a policy choice: a validated policy always has its own Max
// and never sees this value.
const unboundedCeiling = time.Hour
