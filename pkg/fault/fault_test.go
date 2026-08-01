package fault

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// pkg/fault is the package the engine's entire failure behaviour is computed from, and it had no
// test of its own. Class.Terminal and Class.Counted decide whether a record is retried, how many
// attempts it costs and which terminal disposition it reaches; every one of those was asserted only
// indirectly, through internal/engine's routing table — which tests the ENGINE's reading of this
// package rather than the package.

// allClasses is every declared class, in declaration order. Kept as a literal rather than derived
// from the names table, so that adding a class to one and not the other is what fails.
var allClasses = []Class{
	Unclassified, TransientUpstream, TransientInternal, Throttled, Indeterminate,
	PermanentUpstream, PermanentInternal, PermanentMapping, PermanentContract,
	DuplicateIdempotent, ClockSkew, Fenced, NotConnected, EndOfInput,
}

// A class with no name renders as "unclassified", which means a metric label, a wire token and an
// i18n key all silently collapse onto the wrong value. The names table is a sparse array literal, so
// a class added past its end is exactly this bug and nothing else would catch it.
func TestEveryClassHasItsOwnName(t *testing.T) {
	if len(allClasses) != len(classNames) {
		t.Fatalf("%d classes declared but %d names; a class added to one and not the other renders as unclassified",
			len(allClasses), len(classNames))
	}
	seen := map[string]Class{}
	for _, c := range allClasses {
		name := c.String()
		if name == "" {
			t.Errorf("class %d has an empty name", c)
			continue
		}
		if c != Unclassified && name == "unclassified" {
			t.Errorf("class %d renders as %q, so it has no entry of its own", c, name)
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("classes %d and %d both render as %q; the token is the metric label and the wire form",
				prev, c, name)
		}
		seen[name] = c
	}
}

// Terminal is "can retrying ever help". The engine goes straight to the terminal disposition for a
// true, skipping the attempt budget entirely, so the exact set is load-bearing.
func TestTerminalIsExactlyThePermanentClasses(t *testing.T) {
	want := map[Class]bool{
		PermanentUpstream: true, PermanentInternal: true,
		PermanentMapping: true, PermanentContract: true,
	}
	for _, c := range allClasses {
		if got := c.Terminal(); got != want[c] {
			t.Errorf("%s.Terminal() is %v, want %v", c, got, want[c])
		}
	}
}

// Counted is "does this spend a retry attempt". The three uncounted classes are flow control rather
// than failure, and getting this wrong is what made a source honouring a 429 reach TerminalStop
// against an upstream that was working perfectly.
func TestCountedIsExactlyTheFailureClasses(t *testing.T) {
	uncounted := map[Class]bool{Throttled: true, NotConnected: true, EndOfInput: true}
	for _, c := range allClasses {
		if got := c.Counted(); got == uncounted[c] {
			t.Errorf("%s.Counted() is %v; the uncounted classes are throttled, not_connected and end_of_input", c, got)
		}
	}
}

// Blame is the closed three-value ownership answer a UI groups by. BlameUnknown is the default arm,
// so a class that falls through gets grouped under nothing.
func TestEveryClassBlamesSomebody(t *testing.T) {
	for _, c := range allClasses {
		if b := c.Blames(); b == BlameUnknown {
			t.Errorf("%s blames nobody; it will not appear under any grouping in a UI", c)
		}
	}
}

func TestEveryOpAndBlameHasAName(t *testing.T) {
	for i, name := range opNames {
		if name == "" {
			t.Errorf("op %d has no name", i)
		}
		if got := Op(i).String(); got != name {
			t.Errorf("Op(%d).String() is %q, want %q", i, got, name)
		}
	}
	for i, name := range blameNames {
		if name == "" {
			t.Errorf("blame %d has no name", i)
		}
	}
}

// OUTERMOST, NOT INNERMOST, and the doc explains why: a wrapper that deliberately re-classifies must
// win over the original, or re-classification by wrapping is impossible.
func TestClassOfPrefersTheOutermostClass(t *testing.T) {
	inner := Transient(OpRead, errors.New("connection reset"))
	outer := &Fault{Class: PermanentUpstream, Op: OpRead, Err: inner}

	if got := ClassOf(outer); got != PermanentUpstream {
		t.Errorf("ClassOf is %s, want permanent_upstream: the re-classifying wrapper must win", got)
	}
	if got := ClassOf(fmt.Errorf("context: %w", inner)); got != TransientUpstream {
		t.Errorf("ClassOf through a non-fault wrapper is %s, want transient_upstream", got)
	}
	if got := ClassOf(errors.New("bare")); got != Unclassified {
		t.Errorf("ClassOf of a bare error is %s, want unclassified", got)
	}
	if got := ClassOf(nil); got != Unclassified {
		t.Errorf("ClassOf(nil) is %s, want unclassified", got)
	}
}

// A connector wrapping a rate-limit fault must not lose its Retry-After, which is the whole reason
// this is a chain walk rather than a field read.
func TestRetryAfterOfFindsAHintThroughWrapping(t *testing.T) {
	inner := Throttle(OpWrite, 30*time.Second, errors.New("429"))

	if d, ok := RetryAfterOf(inner); !ok || d != 30*time.Second {
		t.Errorf("got (%v, %v), want (30s, true)", d, ok)
	}

	// Wrapped by a fault carrying no hint of its own: the inner one still counts.
	outer := &Fault{Class: TransientUpstream, Op: OpWrite, Err: inner}
	if d, ok := RetryAfterOf(outer); !ok || d != 30*time.Second {
		t.Errorf("through a hintless wrapper: got (%v, %v), want (30s, true)", d, ok)
	}

	// The OUTERMOST hint wins when both carry one.
	outerHint := &Fault{Class: Throttled, Op: OpWrite, RetryAfter: time.Second, Err: inner}
	if d, _ := RetryAfterOf(outerHint); d != time.Second {
		t.Errorf("got %v, want the outermost hint of 1s", d)
	}

	if _, ok := RetryAfterOf(Transient(OpWrite, errors.New("no hint"))); ok {
		t.Error("a fault with no hint reported one")
	}
}

// errors.Is against the package sentinels must work for a fault a CONNECTOR constructed, or every
// author who builds their own NotConnected gets a control-flow bug that looks like a data bug.
func TestSentinelsMatchConnectorConstructedFaults(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sentinel error
		own      error
	}{
		{"not connected", ErrNotConnected, New(NotConnected, OpWrite, errors.New("socket closed"))},
		{"end of input", ErrEndOfInput, New(EndOfInput, OpRead, nil)},
		{"fenced", ErrFenced, New(Fenced, OpPersist, errors.New("lease lapsed"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.own, tc.sentinel) {
				t.Errorf("a connector's own %v does not match the sentinel", tc.name)
			}
			if !errors.Is(fmt.Errorf("wrapped: %w", tc.own), tc.sentinel) {
				t.Errorf("%v does not match through a wrapper", tc.name)
			}
			if errors.Is(Transient(OpRead, nil), tc.sentinel) {
				t.Errorf("a transient fault matched the %v sentinel", tc.name)
			}
		})
	}
}

// A target carrying distinguishing detail is asking for an exact match on that detail, not a
// class-level one.
func TestIsHonoursDistinguishingFields(t *testing.T) {
	f := &Fault{Class: Fenced, Op: OpPersist, Lane: "lane-a", Node: "n1", Record: 7}

	if !errors.Is(f, &Fault{Class: Fenced}) {
		t.Error("a bare class target did not match")
	}
	if !errors.Is(f, &Fault{Class: Fenced, Lane: "lane-a"}) {
		t.Error("a matching lane did not match")
	}
	for _, target := range []*Fault{
		{Class: Fenced, Lane: "lane-b"},
		{Class: Fenced, Node: "n2"},
		{Class: Fenced, Record: 8},
		{Class: Fenced, Op: OpRead},
		{Class: TransientUpstream},
	} {
		if errors.Is(f, target) {
			t.Errorf("matched a target with different detail: %+v", target)
		}
	}
	if errors.Is(f, errors.New("not a fault")) {
		t.Error("matched a non-fault target")
	}
}

func TestErrorRendersClassOpAndOneMessage(t *testing.T) {
	f := &Fault{Class: PermanentMapping, Op: OpEncode, User: "field x is unencodable"}
	if got, want := f.Error(), "permanent_mapping/encode: field x is unencodable"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Dev is used only when User is absent: one string cannot serve both audiences, and the
	// operator-facing one wins when both exist.
	both := &Fault{Class: PermanentMapping, Op: OpEncode, User: "u", Dev: "d"}
	if got := both.Error(); got != "permanent_mapping/encode: u" {
		t.Errorf("got %q, want the user message", got)
	}
	devOnly := &Fault{Class: PermanentMapping, Op: OpEncode, Dev: "d"}
	if got := devOnly.Error(); got != "permanent_mapping/encode: d" {
		t.Errorf("got %q, want the dev message", got)
	}
}

// --- the retry policy ---------------------------------------------------------------------------

func TestValidateNamesEveryWayAPolicyIsUnusable(t *testing.T) {
	good := RetryPolicy{MaxAttempts: 4, Backoff: DefaultBackoff, Terminal: TerminalStop}
	if err := good.Validate(); err != nil {
		t.Fatalf("the default shape is invalid: %v", err)
	}

	for _, tc := range []struct {
		name string
		p    RetryPolicy
		want string
	}{
		{"no attempts", RetryPolicy{MaxAttempts: 0, Backoff: DefaultBackoff, Terminal: TerminalStop}, "max_attempts"},
		{"no terminal", RetryPolicy{MaxAttempts: 4, Backoff: DefaultBackoff}, "terminal"},
		{"zero initial", RetryPolicy{MaxAttempts: 4, Terminal: TerminalStop,
			Backoff: Backoff{Max: time.Second, Multiplier: 2}}, "backoff.initial"},
		{"max below initial", RetryPolicy{MaxAttempts: 4, Terminal: TerminalStop,
			Backoff: Backoff{Initial: time.Second, Max: time.Millisecond, Multiplier: 2}}, "backoff.max"},
		{"multiplier below one", RetryPolicy{MaxAttempts: 4, Terminal: TerminalStop,
			Backoff: Backoff{Initial: time.Second, Max: time.Minute, Multiplier: 0.5}}, "backoff.multiplier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Fatalf("accepted an unusable policy: %+v", tc.p)
			}
			if !contains(err.Error(), tc.want) {
				t.Errorf("the error does not name %q: %v", tc.want, err)
			}
		})
	}

	// MaxAttempts of 1 means no retry, so the backoff is never consulted and is not checked.
	if err := (RetryPolicy{MaxAttempts: 1, Terminal: TerminalStop}).Validate(); err != nil {
		t.Errorf("a no-retry policy with no backoff was refused: %v", err)
	}
}

// There is deliberately no "retry forever", so Next must stop at MaxAttempts and must refuse an
// attempt number below one.
func TestNextRespectsTheAttemptBudget(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3, Backoff: DefaultBackoff, Terminal: TerminalStop}

	for _, attempt := range []int{-1, 0} {
		if _, more := p.Next(attempt); more {
			t.Errorf("Next(%d) permitted another attempt", attempt)
		}
	}
	for attempt := 1; attempt < p.MaxAttempts; attempt++ {
		if _, more := p.Next(attempt); !more {
			t.Errorf("Next(%d) refused although the budget is %d", attempt, p.MaxAttempts)
		}
	}
	if _, more := p.Next(p.MaxAttempts); more {
		t.Error("Next permitted an attempt past the budget; retry-forever must stay inexpressible")
	}
}

// Full jitter is uniform in [1, exponential], which is the only strategy canal offers. The bound
// matters: a delay of zero is a busy loop and a delay above the ceiling is an unbounded wait.
func TestNextIsFullJitterWithinTheCeiling(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts: 40,
		Backoff:     Backoff{Initial: 100 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2},
		Terminal:    TerminalStop,
	}
	for attempt := 1; attempt < 30; attempt++ {
		exp := float64(p.Backoff.Initial) * math.Pow(p.Backoff.Multiplier, float64(attempt-1))
		if exp > float64(p.Backoff.Max) {
			exp = float64(p.Backoff.Max)
		}
		for i := 0; i < 200; i++ {
			d, more := p.Next(attempt)
			if !more {
				t.Fatalf("attempt %d: refused inside the budget", attempt)
			}
			if d <= 0 {
				t.Fatalf("attempt %d: delay %v is a busy loop", attempt, d)
			}
			if float64(d) > exp {
				t.Fatalf("attempt %d: delay %v exceeds the ceiling %v", attempt, d, time.Duration(exp))
			}
			if d > p.Backoff.Max {
				t.Fatalf("attempt %d: delay %v exceeds backoff.max %v", attempt, d, p.Backoff.Max)
			}
		}
	}
}

// Next must not panic on a policy nobody validated. Validate is the gate at build time, but this is
// a public method on a public type and an unvalidated value is reachable.
func TestNextSurvivesAnUnvalidatedPolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    RetryPolicy
	}{
		{"no ceiling and a large multiplier", RetryPolicy{
			MaxAttempts: 100,
			Backoff:     Backoff{Initial: time.Second, Multiplier: 10},
		}},
		{"everything zero", RetryPolicy{MaxAttempts: 5}},
		{"negative initial", RetryPolicy{MaxAttempts: 5, Backoff: Backoff{Initial: -time.Second, Multiplier: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for attempt := 1; attempt < tc.p.MaxAttempts; attempt++ {
				d, _ := tc.p.Next(attempt)
				if d < 0 {
					t.Fatalf("attempt %d: negative delay %v", attempt, d)
				}
			}
		})
	}
}

func TestRecordFaultRendersAsAFault(t *testing.T) {
	rf := RecordFault{
		Record: record.RecordID(42), Class: PermanentMapping, Op: OpWrite,
		User: "unencodable", Dev: "field x",
	}
	f := rf.Fault()
	if f.Class != PermanentMapping || f.Op != OpWrite || f.Record != 42 {
		t.Errorf("the identity did not survive: %+v", f)
	}
	if f.User != "unencodable" || f.Dev != "field x" {
		t.Errorf("the messages did not survive: %+v", f)
	}
	// It has to route like any other fault, which is the whole reason the conversion exists.
	if ClassOf(f) != PermanentMapping || !ClassOf(f).Terminal() {
		t.Error("the rendered fault does not classify")
	}
}

func TestDefaultRetryIsUsable(t *testing.T) {
	if err := DefaultRetry.Validate(); err != nil {
		t.Fatalf("the core-supplied default is invalid: %v", err)
	}
	// The terminal default is stop, and the reasoning is that it is the only terminal a two-node
	// pipeline can be given without the operator having chosen and without losing a record.
	if DefaultRetry.Terminal != TerminalStop {
		t.Errorf("the default terminal is %s, want stop", DefaultRetry.Terminal)
	}
	if DefaultRetry.OnIndeterminate != IndeterminateStall {
		t.Errorf("the default indeterminacy is %s, want stall", DefaultRetry.OnIndeterminate)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// The exponential must stay a usable duration however absurd the policy, because converting an
// out-of-range float64 to int64 is implementation-defined: arm64 saturates, x86 produces a negative
// and rand.Int64N panics on it. This is the test that would have failed on Linux and passed on the
// machine it was written on.
func TestNextIsBoundedWithoutACeiling(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 200, Backoff: Backoff{Initial: time.Second, Multiplier: 10}}
	for attempt := 1; attempt < p.MaxAttempts; attempt++ {
		d, more := p.Next(attempt)
		if !more {
			t.Fatalf("attempt %d: refused inside the budget", attempt)
		}
		if d <= 0 {
			t.Fatalf("attempt %d: delay %v", attempt, d)
		}
		if d > unboundedCeiling {
			t.Fatalf("attempt %d: delay %v exceeds the safety ceiling %v", attempt, d, unboundedCeiling)
		}
	}
}

// A multiplier of NaN survives every comparison against 1, so the guard has to be written as the
// negation of the condition it wants rather than as the condition it does not.
func TestNextSurvivesANaNMultiplier(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts: 5,
		Backoff:     Backoff{Initial: time.Second, Max: time.Minute, Multiplier: math.NaN()},
	}
	for attempt := 1; attempt < p.MaxAttempts; attempt++ {
		if d, _ := p.Next(attempt); d <= 0 || d > time.Minute {
			t.Fatalf("attempt %d: delay %v is outside [1, max]", attempt, d)
		}
	}
}
