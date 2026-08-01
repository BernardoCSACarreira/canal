package engine

import (
	"fmt"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// CONDITIONS, AND THE ONE RULE THAT MAKES THEM WORTH HAVING.
//
// The architecture states it as an invariant with a test behind it: CondSourceReady true MUST NEVER
// BE ABLE TO IMPLY CondProgressing true. A pipeline whose source and sink are both connected and
// whose durable cursor has not moved for an hour has to render as unhealthy, because a UI that
// cannot distinguish *the endpoint answered* from *your data arrived* is actively misleading.
//
// The invariant is kept structurally rather than by care: readiness is computed from whether Open
// returned, and progressing is computed from [runner.lastCheckpoint] — the times canal's own durable
// cursor moved — and from nothing else. There is no input in common, so no amount of connection
// health can raise Progressing, and the test that asserts it drives a real pipeline into exactly
// that state rather than constructing the document by hand.
//
// A phase is ONE value and conditions are ORTHOGONAL, which is what lets "running, connected, forty
// minutes behind, sink returning 429s" be said at all. The legacy vocabulary maps on with no second
// enum: healthy is PhaseRunning with Degraded false, degraded is PhaseRunning with Degraded true
// plus a reason and a last fault, and terminal is PhaseFailed.

// progressWindow is how long a durable cursor may stand still before Progressing goes false,
// expressed in flush intervals because that is the cadence at which a cursor CAN move.
//
// Three intervals rather than one: a single missed tick is a busy machine, three in a row is a
// pipeline that is not making progress. The floor keeps a sub-second flush interval from making the
// condition flap on scheduling noise alone.
func (r *runner) progressWindow() time.Duration {
	iv := r.deps.FlushInterval
	if iv <= 0 {
		iv = time.Second
	}
	if w := 3 * iv; w > time.Second {
		return w
	}
	return time.Second
}

// conditions computes the closed condition set and returns it in the declared order.
//
// Transition times are preserved across calls: a condition whose status and reason are unchanged
// keeps the moment it last CHANGED, which is the only thing that field can usefully mean. Computing
// it as "now" would make every scrape look like a transition.
func (r *runner) conditions(now time.Time, f laneFacts) []telemetry.Condition {
	r.status.mu.Lock()
	defer r.status.mu.Unlock()

	if r.status.conditions == nil {
		r.status.conditions = map[telemetry.ConditionType]telemetry.Condition{}
	}
	gen := r.p.spec.Revision
	fresh := r.computeLocked(now, f)

	out := make([]telemetry.Condition, 0, len(telemetry.ConditionTypes))
	for _, t := range telemetry.ConditionTypes {
		c := fresh[t]
		c.Type = t
		c.ObservedGeneration = gen
		c.LastTransitionTime = now
		if prev, ok := r.status.conditions[t]; ok && prev.Status == c.Status && prev.Reason == c.Reason {
			c.LastTransitionTime = prev.LastTransitionTime
		}
		r.status.conditions[t] = c
		out = append(out, c)
	}
	return out
}

// computeLocked evaluates every condition. It runs under status.mu.
func (r *runner) computeLocked(now time.Time, f laneFacts) map[telemetry.ConditionType]telemetry.Condition {
	cond := func(s telemetry.Status, reason, msg string) telemetry.Condition {
		return telemetry.Condition{Status: s, Reason: reason, Message: msg}
	}
	out := map[telemetry.ConditionType]telemetry.Condition{}

	// CONFIGURED is structurally true in this build and says so rather than pretending to check.
	// A *Pipeline cannot exist without a spec that validated and negotiated — Build returns nil
	// otherwise — so there is no state in this process in which it is false. It becomes falsifiable
	// the day a control plane can hold a stored spec that failed validation, and the field exists now
	// so that day does not require a new condition type.
	out[telemetry.CondConfigured] = cond(telemetry.StatusTrue, telemetry.ReasonApplied,
		"the spec was validated and negotiated")

	// SPEC APPLIED compares the stored revision with the applied one. Both are the running spec's
	// today, for the reason PipelineStatus.Generation records — so the call below is trivially equal
	// and the projection it calls is not. specApplied is where the answer lives, it is tested with
	// divergent revisions, and the day a control plane can hold a revision this process has not
	// applied it is already correct.
	out[telemetry.CondSpecApplied] = specApplied(r.p.spec.Revision, r.p.spec.Revision)

	switch {
	case f.total == 0:
		out[telemetry.CondAssigned] = cond(telemetry.StatusFalse, telemetry.ReasonNoLanes,
			"no lanes are assigned to this worker, so there is nothing to read")
	default:
		out[telemetry.CondAssigned] = cond(telemetry.StatusTrue, telemetry.ReasonAssigned,
			fmt.Sprintf("%d lanes assigned, %d finished", f.total, f.finished))
	}

	// READINESS IS A STATEMENT ABOUT Open AND NOTHING ELSE. It must not consult a cursor, a rate or a
	// record count, because the moment it does it starts implying progress.
	out[telemetry.CondSourceReady] = readiness(r.status.sourceReady, "source")
	out[telemetry.CondSinkReady] = readiness(r.status.sinkReady, "sink")

	// PROGRESSING IS A STATEMENT ABOUT THE DURABLE CURSOR AND NOTHING ELSE. Not that the connection is
	// up, not that records are being read: that canal's own record of progress moved.
	out[telemetry.CondProgressing] = r.progressing(now, f)

	switch {
	case f.total == 0:
		out[telemetry.CondCaughtUp] = cond(telemetry.StatusUnknown, telemetry.ReasonNoLanes,
			"there are no lanes to be caught up with")
	case f.caughtUp:
		out[telemetry.CondCaughtUp] = cond(telemetry.StatusTrue, telemetry.ReasonCaughtUp,
			"every lane's durable cursor has reached its delivered prefix")
	default:
		out[telemetry.CondCaughtUp] = cond(telemetry.StatusFalse, telemetry.ReasonProgressing,
			fmt.Sprintf("%d records are in flight", f.inFlight))
	}

	if f.blocked > 0 {
		out[telemetry.CondBackpressured] = cond(telemetry.StatusTrue, telemetry.ReasonBudgetExhausted,
			fmt.Sprintf("%d of %d lanes are at their in-flight budget", f.blocked, f.total))
	} else {
		out[telemetry.CondBackpressured] = cond(telemetry.StatusFalse, telemetry.ReasonHealthy,
			"no lane is waiting on its in-flight budget")
	}

	out[telemetry.CondDegraded] = r.degraded(f)
	return out
}

// specApplied answers "did my config change take effect?", which the architecture names as the
// question one surveyed status API structurally cannot answer.
//
// stored is the revision the control plane holds; applied is the one this process is running.
func specApplied(stored, applied uint64) telemetry.Condition {
	if stored == applied {
		return telemetry.Condition{Status: telemetry.StatusTrue, Reason: telemetry.ReasonApplied,
			Message: fmt.Sprintf("revision %d is the running spec", applied)}
	}
	return telemetry.Condition{Status: telemetry.StatusFalse, Reason: telemetry.ReasonPending,
		Message: fmt.Sprintf("revision %d is stored but %d is running", stored, applied)}
}

func readiness(open bool, what string) telemetry.Condition {
	if open {
		return telemetry.Condition{Status: telemetry.StatusTrue, Reason: telemetry.ReasonConnected,
			Message: "every " + what + " opened"}
	}
	return telemetry.Condition{Status: telemetry.StatusFalse, Reason: telemetry.ReasonNotConnected,
		Message: "not every " + what + " is open"}
}

// progressing answers whether the DURABLE cursor advanced inside the window.
//
// Its only inputs are the lane count, the finished count and lastAdvance. Deliberately: see the
// invariant at the top of this file.
func (r *runner) progressing(now time.Time, f laneFacts) telemetry.Condition {
	c := func(s telemetry.Status, reason, msg string) telemetry.Condition {
		return telemetry.Condition{Status: s, Reason: reason, Message: msg}
	}
	switch {
	case f.total == 0:
		return c(telemetry.StatusFalse, telemetry.ReasonNoLanes, "no lanes are assigned")
	case f.finished == f.total:
		// Every lane read to its end and its final cursor is durable. Not progressing, and not a
		// stall: a bounded pipeline that finished is the success case, and paging somebody about it
		// is how an alert signal gets turned off.
		return c(telemetry.StatusFalse, telemetry.ReasonDrained,
			fmt.Sprintf("all %d lanes finished", f.total))
	}

	window := r.progressWindow()
	if !f.lastAdvance.IsZero() && now.Sub(f.lastAdvance) <= window {
		return c(telemetry.StatusTrue, telemetry.ReasonProgressing,
			fmt.Sprintf("a durable cursor advanced %s ago", now.Sub(f.lastAdvance).Round(time.Millisecond)))
	}
	if f.lastAdvance.IsZero() && now.Sub(r.started) <= window {
		return c(telemetry.StatusUnknown, telemetry.ReasonStarting,
			"no cursor has advanced yet and the pipeline has only just started")
	}

	// NOT PROGRESSING. Caught up is a separate condition and stays separate: a lane with nothing to
	// read is reported here as stalled and there as caught up, and it is the pair that a health
	// banner reads. Collapsing them would mean a genuinely stuck pipeline could hide behind "idle".
	since := "ever"
	if !f.lastAdvance.IsZero() {
		since = now.Sub(f.lastAdvance).Round(time.Second).String() + " ago"
	}
	switch {
	case f.blocked > 0:
		return c(telemetry.StatusFalse, telemetry.ReasonBudgetExhausted,
			fmt.Sprintf("%d lanes are at their in-flight budget; last durable advance %s", f.blocked, since))
	case f.caughtUp:
		return c(telemetry.StatusFalse, telemetry.ReasonCaughtUp,
			fmt.Sprintf("nothing is outstanding; last durable advance %s", since))
	default:
		return c(telemetry.StatusFalse, telemetry.ReasonStalled,
			fmt.Sprintf("%d records are in flight and no durable cursor advanced in %s (last %s)",
				f.inFlight, window, since))
	}
}

// degraded folds every "running, but" into one condition, most severe first.
func (r *runner) degraded(f laneFacts) telemetry.Condition {
	c := func(s telemetry.Status, reason, msg string) telemetry.Condition {
		return telemetry.Condition{Status: s, Reason: reason, Message: msg}
	}
	if err := r.firstError(); err != nil {
		return c(telemetry.StatusTrue, telemetry.ReasonTerminalFault, err.Error())
	}
	// A DOWNGRADE IS DEGRADED FOR THE PIPELINE'S WHOLE LIFE. The operator signed a waiver saying they
	// know the guarantee is weaker than they asked for; the UI never stops saying so.
	if n := len(r.p.negotiated.Downgrades); n > 0 {
		return c(telemetry.StatusTrue, telemetry.ReasonDowngradeAcknowledged,
			fmt.Sprintf("%d acknowledged downgrades are in force", n))
	}
	if leaks := r.p.ledger.Leaks(); len(leaks) > 0 {
		return c(telemetry.StatusTrue, telemetry.ReasonGroupLeaked,
			fmt.Sprintf("%d settlement groups were abandoned; their records may replay", len(leaks)))
	}
	if f.abandoned > 0 {
		return c(telemetry.StatusTrue, telemetry.ReasonRetriesExhausted,
			fmt.Sprintf("%d records reached a terminal disposition and will not be delivered", f.abandoned))
	}
	return c(telemetry.StatusFalse, telemetry.ReasonHealthy, "no fault, waiver or abandoned record")
}

// publishConditions exports the set as canal_condition.
//
// EVERY CONDITION GETS A SERIES PER STATUS, one of them 1 and the other two 0, which is what makes
// `canal_condition{condition="spec_applied",status="false"} == 1` an alert expression rather than an
// absence somebody has to remember to check for. Nine types times three statuses is 27 series per
// pipeline, bounded by construction because both label values come from closed sets.
//
// This is the one place omit-don't-zero does not apply: a condition's status is always known — that
// is what StatusUnknown is for — so a missing series would mean the exporter broke, not that the
// quantity could not be measured.
func (r *runner) publishConditions(conds []telemetry.Condition) {
	o := r.p.obs
	if o == nil || o.conditions == nil {
		return
	}
	for _, c := range conds {
		for _, s := range []telemetry.Status{telemetry.StatusTrue, telemetry.StatusFalse, telemetry.StatusUnknown} {
			v := 0.0
			if c.Status == s {
				v = 1
			}
			o.conditions.Set(v, o.pipeline, string(c.Type), string(s))
		}
	}
}
