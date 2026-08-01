package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
)

// The routing tree is a pure function of (class, capability, policy), so it is tested as one: a
// table, no clock, no sleeping, no engine. Every row here is a sentence from pkg/fault's own
// documentation turned into an assertion, because until this file every one of those sentences was
// a promise nothing kept.

func policy(max int, term fault.Terminal, ind fault.Indeterminacy) fault.RetryPolicy {
	return fault.RetryPolicy{
		MaxAttempts:     max,
		Backoff:         fault.Backoff{Initial: 10 * time.Millisecond, Max: 40 * time.Millisecond, Multiplier: 2},
		Terminal:        term,
		OnIndeterminate: ind,
	}
}

func TestRouteSpendsAnAttemptOnlyWhenTheClassSaysSo(t *testing.T) {
	// Three attempts means two retries and then the terminal disposition.
	p := policy(3, fault.TerminalStop, fault.IndeterminateStall)
	now := time.Now()

	t.Run("a counted class exhausts the budget", func(t *testing.T) {
		a := &attempt{started: now}
		for i := 1; i <= 2; i++ {
			got := route(fault.Transient(fault.OpWrite, errors.New("503")), false, p, a, now)
			if got.disp != dispRetry {
				t.Fatalf("attempt %d: got %s, want retry", i, got.disp)
			}
			if a.count != i {
				t.Errorf("attempt %d: counter is %d, want %d", i, a.count, i)
			}
		}
		got := route(fault.Transient(fault.OpWrite, errors.New("503")), false, p, a, now)
		if got.disp != dispStop {
			t.Errorf("after the budget: got %s, want stop", got.disp)
		}
		if got.reason != "retries_exhausted" {
			t.Errorf("reason is %q, want retries_exhausted", got.reason)
		}
	})

	// Throttled is the whole reason Counted exists. A source politely honouring a 429 must not burn
	// its retry budget on an upstream that is working perfectly.
	t.Run("throttling never exhausts it", func(t *testing.T) {
		a := &attempt{started: now}
		for i := 0; i < 50; i++ {
			got := route(fault.Throttle(fault.OpWrite, 0, errors.New("429")), false, p, a, now)
			if got.disp != dispRetry {
				t.Fatalf("round %d: got %s, want retry — throttling stopped a healthy pipeline", i, got.disp)
			}
		}
		if a.count != 0 {
			t.Errorf("throttling spent %d attempts; it must spend none", a.count)
		}
	})

	t.Run("not_connected never exhausts it either", func(t *testing.T) {
		a := &attempt{started: now}
		for i := 0; i < 10; i++ {
			if got := route(fault.New(fault.NotConnected, fault.OpWrite, errors.New("gone")),
				false, p, a, now); got.disp != dispRetry {
				t.Fatalf("round %d: got %s, want retry", i, got.disp)
			}
		}
		if a.count != 0 {
			t.Errorf("not_connected spent %d attempts; it must spend none", a.count)
		}
	})
}

// A terminal class must reach its disposition in ONE decision. Running it through the attempt
// budget would wait 4 times for an answer that cannot change, which is the difference between a
// poison record costing milliseconds and costing a minute.
func TestRouteDoesNotRetryAClassThatCannotBeHelped(t *testing.T) {
	p := policy(4, fault.TerminalDrop, fault.IndeterminateStall)
	now := time.Now()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"mapping", fault.Mapping(fault.OpWrite, errors.New("field is not encodable"))},
		{"permanent upstream", fault.Permanent(fault.OpWrite, errors.New("403"))},
		{"contract", fault.Contract(fault.OpWrite, errors.New("counts do not add up"))},
		{"internal", fault.Bug(fault.OpWrite, errors.New("impossible state"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &attempt{started: now}
			got := route(tc.err, false, p, a, now)
			if got.disp != dispDrop {
				t.Errorf("got %s, want drop (the policy's terminal)", got.disp)
			}
			if got.reason != "terminal_fault" {
				t.Errorf("reason is %q, want terminal_fault — exhausted and never-retryable are different events", got.reason)
			}
			if a.count != 0 {
				t.Errorf("a terminal class spent %d attempts; it must spend none", a.count)
			}
		})
	}
}

// Indeterminate is the branch the whole class vocabulary exists for: the write MAY have landed.
func TestRouteResolvesIndeterminateByCapabilityThenPolicy(t *testing.T) {
	now := time.Now()
	ind := fault.Unknown(fault.OpWrite, errors.New("timed out after the request was sent"))

	t.Run("an idempotent sink absorbs the duplicate, so it is an ordinary retry", func(t *testing.T) {
		a := &attempt{started: now}
		got := route(ind, true, policy(4, fault.TerminalStop, fault.IndeterminateStall), a, now)
		if got.disp != dispRetry {
			t.Errorf("got %s, want retry: an idempotent sink is exactly what makes this safe", got.disp)
		}
	})

	t.Run("without idempotency the default is to fail loud", func(t *testing.T) {
		a := &attempt{started: now}
		got := route(ind, false, policy(4, fault.TerminalStop, fault.IndeterminateStall), a, now)
		if got.disp != dispStall {
			t.Errorf("got %s, want stall", got.disp)
		}
		if got.reason != "indeterminate_write" {
			t.Errorf("reason is %q, want indeterminate_write", got.reason)
		}
	})

	t.Run("the operator may choose duplicates", func(t *testing.T) {
		a := &attempt{started: now}
		if got := route(ind, false, policy(4, fault.TerminalStop, fault.IndeterminateRetry), a, now); got.disp != dispRetry {
			t.Errorf("got %s, want retry", got.disp)
		}
	})

	t.Run("the operator may choose the dead-letter route", func(t *testing.T) {
		a := &attempt{started: now}
		if got := route(ind, false, policy(4, fault.TerminalStop, fault.IndeterminateDeadLetter), a, now); got.disp != dispDeadLetter {
			t.Errorf("got %s, want dead_letter", got.disp)
		}
	})
}

// The remote's own number wins over ours. It is the one value in the tree somebody else measured.
func TestRouteHonoursRetryAfter(t *testing.T) {
	p := policy(4, fault.TerminalStop, fault.IndeterminateStall)
	now := time.Now()
	a := &attempt{started: now}

	got := route(fault.Throttle(fault.OpWrite, 7*time.Second, errors.New("429")), false, p, a, now)
	if got.disp != dispRetry {
		t.Fatalf("got %s, want retry", got.disp)
	}
	if got.delay != 7*time.Second {
		t.Errorf("delay is %v, want the 7s the remote asked for; the schedule's ceiling is %v",
			got.delay, p.Backoff.Max)
	}
}

// MaxElapsed is the ONLY bound an uncounted fault has. Without it a permanently throttled upstream
// waits forever, which is the failure mode the uncounted classes create by design.
func TestRouteBoundsAnUncountedWaitByElapsedTime(t *testing.T) {
	p := policy(4, fault.TerminalStop, fault.IndeterminateStall)
	p.Backoff.MaxElapsed = time.Minute
	start := time.Now()
	a := &attempt{started: start}

	if got := route(fault.Throttle(fault.OpWrite, 0, errors.New("429")), false, p, a, start.Add(30*time.Second)); got.disp != dispRetry {
		t.Errorf("at 30s of a 60s budget: got %s, want retry", got.disp)
	}
	got := route(fault.Throttle(fault.OpWrite, 0, errors.New("429")), false, p, a, start.Add(61*time.Second))
	if got.disp != dispStop {
		t.Errorf("past the elapsed budget: got %s, want stop", got.disp)
	}
	if a.count != 0 {
		t.Errorf("the uncounted class spent %d attempts on its way to the bound", a.count)
	}
}

// RetryPolicy.Validate only checks the backoff when MaxAttempts > 1, so {MaxAttempts: 1} with a zero
// Backoff is a VALID policy. A counted fault under it stops at once, which is correct — but an
// uncounted one keeps waiting, and waiting for zero pins a core.
func TestRouteNeverBusyLoopsOnAValidZeroBackoff(t *testing.T) {
	p := fault.RetryPolicy{MaxAttempts: 1, Terminal: fault.TerminalStop}
	if err := p.Validate(); err != nil {
		t.Fatalf("this policy is supposed to be valid: %v", err)
	}
	now := time.Now()

	a := &attempt{started: now}
	got := route(fault.Throttle(fault.OpWrite, 0, errors.New("429")), false, p, a, now)
	if got.disp != dispRetry {
		t.Fatalf("got %s, want retry: throttling is uncounted, so MaxAttempts does not apply", got.disp)
	}
	if got.delay <= 0 {
		t.Errorf("delay is %v, which is a busy loop", got.delay)
	}

	// The counted case under the same policy must NOT retry: 1 means no retry.
	b := &attempt{started: now}
	if got := route(fault.Transient(fault.OpWrite, errors.New("503")), false, p, b, now); got.disp != dispStop {
		t.Errorf("max_attempts=1 gave %s, want stop; 1 means no retry", got.disp)
	}
}

func TestRouteMapsEveryTerminal(t *testing.T) {
	now := time.Now()
	poison := fault.Mapping(fault.OpWrite, errors.New("unencodable"))

	for _, tc := range []struct {
		term fault.Terminal
		want disposition
	}{
		{fault.TerminalDeadLetter, dispDeadLetter},
		{fault.TerminalDrop, dispDrop},
		{fault.TerminalStop, dispStop},
		// Validate refuses this one at build time. It collapses into stop rather than growing a
		// fifth behaviour nobody specified.
		{fault.TerminalInvalid, dispStop},
	} {
		t.Run(tc.term.String(), func(t *testing.T) {
			a := &attempt{started: now}
			got := route(poison, false, policy(4, tc.term, fault.IndeterminateStall), a, now)
			if got.disp != tc.want {
				t.Errorf("terminal %s routed to %s, want %s", tc.term, got.disp, tc.want)
			}
		})
	}
}

// An unclassified error is a connector defect. It must still be routable, and it must be treated as
// the class vocabulary says: PermanentInternal, which is terminal.
func TestRouteTreatsAnUnclassifiedErrorAsATerminalDefect(t *testing.T) {
	now := time.Now()
	a := &attempt{started: now}
	got := route(errors.New("a bare error from a connector"), false,
		policy(4, fault.TerminalStop, fault.IndeterminateStall), a, now)
	if got.disp != dispStop {
		t.Errorf("got %s, want stop", got.disp)
	}
	if a.count != 0 {
		t.Errorf("an unclassified error spent %d attempts", a.count)
	}
}
