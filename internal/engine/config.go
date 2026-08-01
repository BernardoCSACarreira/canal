package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE CONFIG WATCH, AND WHY IT REPORTS RATHER THAN APPLIES.
//
// Deps.Config has been declared and unread since Deps was written, and its entry on the inert
// allowlist named the consequence exactly: nothing in the standalone shape holds a second revision,
// so CondSpecApplied compared a number with itself and could not be false. The one condition whose
// stated job is answering "did my config change take effect?" always answered yes.
//
// This file gives it a second number. A worker with a store.ConfigStore polls the stored revision of
// its own pipeline, watches for changes when the store offers a watch, and publishes what it last
// saw. The condition then compares the STORED revision with the APPLIED one and can finally say no.
//
// IT DOES NOT APPLY THE NEW SPEC, and that is a decision rather than an omission. Applying means
// re-negotiating a guarantee, re-resolving connectors, and restarting lanes whose in-flight records
// are settled against the old topology — Pipeline lifecycle work, not a watcher's. Reporting is
// nonetheless the whole job of a condition: an operator who pushed a revision and sees spec_applied
// false with both numbers in the message knows their change is real, knows it has not landed, and
// knows restarting this worker is what lands it. Before this file they saw true and learnt nothing.
//
// EVERY FAILURE HERE IS INERT ON THE DATA PATH. The watch runs on its own goroutine, touches no
// record, and holds no lock the run loop takes. A config store that is down, slow or lying costs a
// less confident condition and nothing else, which is the same rule store.StatusStore.Report states
// for status reporting and for the same reason: the data plane keeps running with the entire control
// plane down.

// configView returns this pipeline's last observation of the stored revision, or nil when there has
// been none — either because the worker has no config store, or because Run has not started yet.
func (p *Pipeline) configView() *configView { return p.config.Load() }

// configView is one observation of the control plane's stored revision.
//
// It is immutable and published by pointer swap, so a status read never sees a half-updated
// observation and never blocks the goroutine producing them.
type configView struct {
	// revision is the stored revision, and known says whether it is one at all.
	//
	// THE FLAG IS NOT REDUNDANT WITH A ZERO REVISION. Zero is a legal stored revision — cmd/canal's
	// file projection returns whatever the operator wrote in the file, and an operator who has never
	// touched the field wrote zero — so the number cannot also carry "nothing was ever read". It
	// stays true across a failed read, because "revision 7, four minutes ago" is worth rendering as
	// long as the condition beside it says the number is stale; it goes false on a deletion, because
	// then there genuinely is no stored revision.
	revision uint64
	known    bool

	// deleted records that the store answered ErrNoSpec: the pipeline this worker is running is no
	// longer stored. It is not an error — the store answered — which is why it is its own field.
	deleted bool

	// err is the last read's failure, if it failed.
	err error

	// okAt is when the store last ANSWERED and at is when it was last ASKED. Two fields because the
	// interesting number when a store is down is how long it has been down, and the time of the read
	// that just failed is always now.
	okAt time.Time
	at   time.Time
}

// generation reports the stored revision to render, falling back to the applied one.
//
// A view with no stored revision to report — never read, or read and withdrawn — has nothing better
// to offer than the running spec's own. That is not a claim the two agree; the condition beside it
// says whether they do. It is the document's Generation field having to hold a number.
func (v *configView) generation(applied uint64) uint64 {
	if v == nil || !v.known {
		return applied
	}
	return v.revision
}

// cursor is the revision a watch resumes from: what was last observed, or nothing.
func (v *configView) cursor() uint64 {
	if v == nil || !v.known {
		return 0
	}
	return v.revision
}

// configCondition renders [telemetry.CondSpecApplied] from the last observation.
//
// The arithmetic lives in specApplied and stays there. This function is only about the four answers
// arithmetic cannot give: there is no control plane, there is one and nobody has asked it yet, there
// is one and it did not answer, and there is one and it says the pipeline is gone.
//
// hasStore IS A SEPARATE ARGUMENT AND NOT INFERRED FROM A NIL VIEW, because a nil view means two
// opposite things — nothing to read, or not read yet — and collapsing them would make a pipeline
// that has been built and not started claim its config is applied on the strength of never having
// looked.
func configCondition(hasStore bool, v *configView, applied uint64) telemetry.Condition {
	switch {
	case !hasStore:
		// NO CONFIG STORE IS A TRUE ANSWER, not a missing one. The spec canal loaded is the only
		// revision that exists, so the running spec is by construction the stored one. The message
		// differs from the compared case on purpose: a reader has to be able to tell "I checked" from
		// "there is nothing to check against".
		return telemetry.Condition{Status: telemetry.StatusTrue, Reason: telemetry.ReasonApplied,
			Message: fmt.Sprintf("revision %d is running and no config store holds another", applied)}

	case v == nil:
		return unread(applied)

	case v.deleted:
		return telemetry.Condition{Status: telemetry.StatusFalse, Reason: telemetry.ReasonSpecDeleted,
			Message: fmt.Sprintf("revision %d is running but the pipeline is no longer stored", applied)}

	case v.err != nil:
		// MEASURED FROM THE LAST ANSWER, NOT THE LAST ATTEMPT. The read that just failed happened
		// now, so timing from it would report every outage as a second old however long it had been
		// going on — which is the one number an operator reads this message for.
		since := "never"
		if !v.okAt.IsZero() {
			since = time.Since(v.okAt).Round(time.Second).String() + " ago"
		}
		return telemetry.Condition{Status: telemetry.StatusUnknown,
			Reason: telemetry.ReasonConfigStoreUnreachable,
			Message: fmt.Sprintf("revision %d is running; the config store last answered %s: %v",
				applied, since, v.err)}

	case !v.known:
		// A VIEW THAT IS NEITHER AN ANSWER NOR A FAILURE HAS OBSERVED NOTHING, and falling through
		// would compare its zero revision — a confident "revision 0 is stored but 5 is running" from
		// a value that holds no revision at all. readConfig cannot produce one today; this is here so
		// that an edit which can is caught by the arm rather than by an operator.
		return unread(applied)
	}
	return specApplied(v.revision, applied)
}

// unread is the answer for a store nobody has got a revision out of yet.
func unread(applied uint64) telemetry.Condition {
	return telemetry.Condition{Status: telemetry.StatusUnknown, Reason: telemetry.ReasonStarting,
		Message: fmt.Sprintf("revision %d is running; the config store has not been read yet", applied)}
}

// watchConfig starts the config watch and returns the function that stops it.
//
// It returns a no-op for a worker with no config store, which is the standalone shape: there is no
// second revision to poll for and no goroutine worth having.
func (r *runner) watchConfig(ctx context.Context) func() {
	if r.deps.Config == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.configLoop(ctx)
	}()
	// Stopping WAITS. A watch goroutine still calling into a store the host is about to close is the
	// same class of bug the control loop's stop channel exists to prevent.
	return func() {
		cancel()
		<-done
	}
}

// configLoop polls the stored revision, and takes a watch as a hint when the store offers one.
//
// THE TIMER IS THE PROTOCOL AND THE WATCH IS AN OPTIMISATION. store.ConfigStore.Watch says so in as
// many words — "a watch is a convenience, never a correctness dependency" — and this loop is written
// so that a store whose Watch returns an error, or returns a channel and then never sends on it, is
// indistinguishable from one with no watch at all except in how quickly it notices. Every event does
// nothing but trigger the same read the timer would have done.
func (r *runner) configLoop(ctx context.Context) {
	r.readConfig(ctx)

	// The watch resumes from the revision just observed, so a change between that read and this call
	// is delivered rather than skipped.
	var events <-chan store.ConfigEvent
	if ch, err := r.deps.Config.Watch(ctx, r.p.configView().cursor()); err != nil {
		// LOGGED ONCE, AT DEBUG. A store with no watch is a supported deployment — the file-projected
		// config store in cmd/canal is one — so this is not a warning, and repeating it every tick
		// would make the log useless.
		r.deps.Log.Debug("the config store offers no watch; the reconcile timer carries it",
			"pipeline", r.p.spec.ID, "error", err)
	} else {
		events = ch
	}

	t := time.NewTicker(r.deps.ConfigInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.readConfig(ctx)
		case e, ok := <-events:
			if !ok {
				// A closed watch is not a reason to stop watching. The timer keeps the observation
				// current; nilling the channel stops this case spinning on a closed one.
				events = nil
				continue
			}
			// SOMEBODY ELSE'S PIPELINE IS NOT NEWS. A watch is store-wide by signature, and a
			// deployment holding four hundred pipelines would otherwise re-read this one's spec four
			// hundred times per change.
			if e.Tenant != r.p.spec.Tenant || e.Pipeline != r.p.spec.ID {
				continue
			}
			r.readConfig(ctx)
		}
	}
}

// readConfig takes one observation and publishes it.
func (r *runner) readConfig(ctx context.Context) {
	s, rev, err := r.deps.Config.Get(ctx, r.p.spec.Tenant, r.p.spec.ID)

	// A cancelled read is the shutdown, not an unreachable store. Publishing it would leave the final
	// status document reporting the control plane as down because the process was stopping.
	if err != nil && ctx.Err() != nil {
		return
	}

	now := time.Now()
	prev := r.p.configView()
	v := &configView{at: now}
	if prev != nil {
		v.revision, v.known, v.okAt = prev.revision, prev.known, prev.okAt
	}

	switch {
	case errors.Is(err, store.ErrNoSpec):
		// THE STORE ANSWERED, so this counts as an answer — and there is no stored revision left to
		// carry forward, which is what separates a withdrawal from a stale reading.
		v.deleted, v.known, v.okAt = true, false, now
	case err != nil:
		v.err = err
	case s.Tenant != r.p.spec.Tenant || s.ID != r.p.spec.ID:
		// THE STORE ANSWERED WITH SOMEBODY ELSE'S SPEC. Trusting the revision beside it would report
		// another pipeline's generation as this one's, which is worse than reporting nothing: the
		// number looks right and is about the wrong pipeline.
		v.err = fmt.Errorf("the config store answered %s/%s for a read of %s/%s",
			s.Tenant, s.ID, r.p.spec.Tenant, r.p.spec.ID)
	default:
		v.revision, v.known, v.okAt = rev, true, now
	}
	r.p.config.Store(v)
}
