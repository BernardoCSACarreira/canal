package engine_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// CondSpecApplied COULD NOT BE FALSE, and that is the defect these tests close.
//
// The condition's stated job is answering "did my config change take effect?", which one surveyed
// status API structurally cannot. canal's could not either: Deps.Config was declared and unread, so
// the engine compared the running spec's revision with itself and reported applied every time. The
// projection was right and had nothing to project.
//
// These drive the comparison through a REAL pipeline against a real store, per design rule R8, and
// each one covers a different way the second number can be absent — moved, unreadable, withdrawn,
// or nowhere at all — because the four have different operator responses and reporting one for
// another is the whole failure mode.

// configFixture is a running pipeline whose sink never returns, so the run stays alive for as long
// as the config underneath it is being changed.
type configFixture struct {
	p    *engine.Pipeline
	done chan error
	stop context.CancelFunc
}

func startConfigFixture(t *testing.T, cfg store.ConfigStore, s spec.Spec,
	tune ...func(*engine.Deps),
) *configFixture {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)

	// The sink blocks in Write, which holds the pipeline in PhaseRunning without a sleep anywhere.
	release := make(chan struct{})
	c := &collector{after: func(int) { <-release }}
	name := registerCollector(t, "config_watch", c)

	// pipelineSpec supplies the graph; the caller supplies the identity and revision, because that is
	// what the store is keyed on.
	full := pipelineSpec(name, path)
	full.Tenant, full.ID, full.Revision = s.Tenant, s.ID, s.Revision

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}

	deps := engine.Deps{
		State: st, Worker: "test", Config: cfg,
		// Short enough that the reconcile timer alone carries every assertion here, which is what
		// lets the declined-watch case be tested at all.
		ConfigInterval: 5 * time.Millisecond,
		FlushInterval:  5 * time.Millisecond,
		GracePeriod:    time.Second,
	}
	for _, f := range tune {
		f(&deps)
	}
	p, _, diags := engine.Build(context.Background(), registry.Default, full, deps)
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	f := &configFixture{p: p, done: make(chan error, 1)}
	go func() { f.done <- p.Run(ctx) }()

	// Guarded, because a test that stops the fixture itself must not have the cleanup stop it again.
	var once sync.Once
	f.stop = func() {
		once.Do(func() {
			// STOPPING WAITS FOR THE OPENS, AND NOT BECAUSE ANY TEST HERE CARES.
			//
			// Cancelling while openSources is running trips a race that has nothing to do with the
			// config watch: sandbox abandons the in-flight Open and leaks its goroutine — its
			// documented cost — Run returns the abandoned error, and the host then calls Close, which
			// enters the connector while the abandoned Open is still assigning its fields. Every
			// connector in the module is exposed; linefile writes s.f in Open and reads it in Close
			// with nothing between them.
			//
			// It reproduces on main with no config store anywhere in the picture, by sweeping a cancel
			// across the startup window. These tests happen to sit in that window because they cancel
			// as soon as a config read lands, which is milliseconds in. Waited out here rather than
			// papered over: the fix belongs in the engine, where Close has to learn that a node with
			// an abandoned call outstanding cannot be closed.
			f.awaitStarted(t)

			close(release)
			cancel()
			<-f.done
			p.Close(context.Background())
			st.Close()
		})
	}
	t.Cleanup(f.stop)
	return f
}

// await polls until the spec_applied condition satisfies want, and reports what it saw if it never
// does. It gates on the condition itself rather than on elapsed time.
func (f *configFixture) await(t *testing.T, what string, want func(telemetry.Condition) bool) telemetry.Condition {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last telemetry.Condition
	for time.Now().Before(deadline) {
		s := f.p.Status(telemetry.StatusQuery{})
		last = conditionOf(t, s, telemetry.CondSpecApplied)
		if want(last) {
			return last
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("spec_applied never became %s; it is %s/%s: %s", what, last.Status, last.Reason, last.Message)
	return last
}

// awaitStarted polls until the pipeline is past PhaseStarting, whichever way it went.
//
// Bounded, and it does NOT fail on the bound: a pipeline still opening after ten seconds is a
// different problem, and the test that is on its way out is not the one to report it.
func (f *configFixture) awaitStarted(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f.p.Status(telemetry.StatusQuery{}).Phase != telemetry.PhaseStarting {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitPhase polls until the pipeline reaches a phase.
func (f *configFixture) awaitPhase(t *testing.T, want telemetry.Phase) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last telemetry.Phase
	for time.Now().Before(deadline) {
		if last = f.p.Status(telemetry.StatusQuery{}).Phase; last == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("the pipeline is %s and never reached %s", last, want)
}

// THE TEST THE CONDITION EXISTS FOR. An operator stores a new revision, this worker keeps running
// the old one, and the status document has to say so — with both numbers, because "your config did
// not apply" without naming what is stored and what is running is an alert nobody can act on.
func TestAStoredRevisionThisWorkerHasNotAppliedIsReportedAsNotApplied(t *testing.T) {
	ctx := context.Background()
	cfg := memstore.NewConfig()

	stored := spec.Spec{Tenant: "acme", ID: "p1"}
	rev, err := cfg.Put(ctx, stored, 0)
	if err != nil {
		t.Fatalf("seeding the config store: %v", err)
	}
	stored.Revision = rev

	f := startConfigFixture(t, cfg, stored)

	// First: the running spec IS the stored one, so the condition is true — and true because it was
	// compared, not because there was nothing to compare with.
	applied := f.await(t, "true", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusTrue })
	if applied.Reason != telemetry.ReasonApplied {
		t.Errorf("a matching revision is %s/%s, want true/applied", applied.Status, applied.Reason)
	}
	if s := f.p.Status(telemetry.StatusQuery{}); s.Generation != rev || s.ObservedGeneration != rev {
		t.Errorf("generations are %d/%d, want %d/%d", s.Generation, s.ObservedGeneration, rev, rev)
	}

	// Then somebody stores a change. Nothing about this process changes.
	next, err := cfg.Put(ctx, stored, rev)
	if err != nil {
		t.Fatalf("storing the next revision: %v", err)
	}

	pending := f.await(t, "false", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusFalse })
	if pending.Reason != telemetry.ReasonPending {
		t.Errorf("a stored revision ahead of the applied one is %s/%s, want false/pending",
			pending.Status, pending.Reason)
	}
	// The message is what an operator reads. Both numbers or it says nothing useful.
	for _, want := range []string{"1", "2"} {
		if !strings.Contains(pending.Message, want) {
			t.Errorf("the message %q does not name revision %s; the operator needs both the stored "+
				"and the running one to know which way round the divergence is", pending.Message, want)
		}
	}

	s := f.p.Status(telemetry.StatusQuery{})
	if s.Generation != next {
		t.Errorf("Generation is %d, want the STORED revision %d", s.Generation, next)
	}
	if s.ObservedGeneration != rev {
		t.Errorf("ObservedGeneration is %d, want the APPLIED revision %d", s.ObservedGeneration, rev)
	}
	if s.Generation == s.ObservedGeneration {
		t.Error("the two generations are equal while the store holds a revision this process has " +
			"not applied; they are two numbers or the condition above cannot mean anything")
	}

	// AND THE PIPELINE STILL RUNS. The whole control plane could be down and the data path would be
	// unaffected; a divergent revision is a report, not a stop.
	//
	// Gated on the phase rather than read off the snapshot above. The config watch starts BEFORE the
	// opens, so both revisions can be observed while the pipeline is still starting — under load that
	// is often — and asserting the phase from a snapshot timed by a config transition asserts the
	// scheduler, not the engine.
	f.awaitPhase(t, telemetry.PhaseRunning)
}

// A CONFIG STORE THAT DOES NOT ANSWER IS UNKNOWN, NOT APPLIED. This is the honesty case: reporting
// true would mean "your config is live" on the strength of a read that failed, and a status document
// that answers confidently when it does not know is the failure mode the whole read model is built
// against.
func TestAnUnreachableConfigStoreIsUnknownRatherThanApplied(t *testing.T) {
	down := errors.New("dial tcp 127.0.0.1:5432: connection refused")
	f := startConfigFixture(t, failingConfig{err: fault.Transient(fault.OpRead, down)},
		spec.Spec{Tenant: "acme", ID: "p1", Revision: 4})

	// Gated on the REASON and not just the status, because "not read yet" is also unknown — which is
	// the point of that state and would make this test pass before the store was ever called.
	c := f.await(t, "unreachable", func(c telemetry.Condition) bool {
		return c.Reason == telemetry.ReasonConfigStoreUnreachable
	})
	if c.Status != telemetry.StatusUnknown {
		t.Errorf("an unreachable store is %s/%s, want unknown/config_store_unreachable",
			c.Status, c.Reason)
	}
	if !strings.Contains(c.Message, "connection refused") {
		t.Errorf("the message %q does not carry the store's own error; without it an operator "+
			"cannot tell a dead control plane from a misconfigured one", c.Message)
	}

	s := f.p.Status(telemetry.StatusQuery{})
	if s.Generation != 4 {
		t.Errorf("Generation is %d; with no answer from the store the document falls back to the "+
			"running revision %d rather than to zero", s.Generation, 4)
	}

	// AND THE PIPELINE STILL STARTS. The watch runs before the opens do — which is why the condition
	// above went unreachable while the phase was still "starting" — so this is the assertion that a
	// dead control plane does not hold up the data plane behind it.
	f.awaitPhase(t, telemetry.PhaseRunning)
}

// A WITHDRAWN SPEC IS ITS OWN ANSWER. Deleted and unreachable both arrive as an error from Get and
// mean opposite things: one says the control plane cannot be asked, the other says it answered and
// the answer is that this pipeline is gone. Collapsing them would page somebody about a network
// while their colleague was deleting a pipeline.
func TestAWithdrawnSpecIsReportedAsDeletedRatherThanUnreachable(t *testing.T) {
	ctx := context.Background()
	cfg := memstore.NewConfig()

	stored := spec.Spec{Tenant: "acme", ID: "p1"}
	rev, err := cfg.Put(ctx, stored, 0)
	if err != nil {
		t.Fatalf("seeding the config store: %v", err)
	}
	stored.Revision = rev

	f := startConfigFixture(t, cfg, stored)
	f.await(t, "true", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusTrue })

	if err := cfg.Delete(ctx, "acme", "p1", rev); err != nil {
		t.Fatalf("deleting the spec: %v", err)
	}

	c := f.await(t, "false", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusFalse })
	if c.Reason != telemetry.ReasonSpecDeleted {
		t.Errorf("a deleted spec is %s/%s, want false/spec_deleted: an unreachable store and a "+
			"withdrawn pipeline need different responses and so need different reasons",
			c.Status, c.Reason)
	}
}

// A STORE WITH NO WATCH IS A SUPPORTED DEPLOYMENT. store.ConfigStore.Watch says a watch is a
// convenience and never a correctness dependency; cmd/canal's file projection declines it outright.
// If the loop needed one, that binary's condition would be permanently stuck at whatever it saw
// first — which is the same silence Deps.Config being unread produced.
func TestADeclinedWatchIsCarriedByTheReconcileTimer(t *testing.T) {
	ctx := context.Background()
	inner := memstore.NewConfig()
	cfg := noWatchConfig{ConfigStore: inner}

	stored := spec.Spec{Tenant: "acme", ID: "p1"}
	rev, err := inner.Put(ctx, stored, 0)
	if err != nil {
		t.Fatalf("seeding the config store: %v", err)
	}
	stored.Revision = rev

	f := startConfigFixture(t, cfg, stored)
	f.await(t, "true", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusTrue })

	if _, err := inner.Put(ctx, stored, rev); err != nil {
		t.Fatalf("storing the next revision: %v", err)
	}
	c := f.await(t, "false", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusFalse })
	if c.Reason != telemetry.ReasonPending {
		t.Errorf("with the watch declined the timer reported %s/%s, want false/pending", c.Status, c.Reason)
	}
}

// A WORKER WITH NO CONFIG STORE STILL REPORTS TRUE, and must say why in a way a reader can tell
// apart from a comparison that was actually made. This is the standalone shape — `canal run --spec
// -` reads from a pipe and has no stored copy — and it is the one case where "applied" is a
// statement about there being nothing else in existence.
func TestNoConfigStoreReportsAppliedAndSaysThereIsNothingToCompare(t *testing.T) {
	f := startConfigFixture(t, nil, spec.Spec{Tenant: "acme", ID: "p1", Revision: 3})

	c := f.await(t, "true", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusTrue })
	if c.Reason != telemetry.ReasonApplied {
		t.Errorf("with no config store the condition is %s/%s, want true/applied", c.Status, c.Reason)
	}
	if !strings.Contains(c.Message, "no config store") {
		t.Errorf("the message %q reads as though a comparison happened; with no store the honest "+
			"claim is that there is no second revision, not that two agreed", c.Message)
	}
	if s := f.p.Status(telemetry.StatusQuery{}); s.Generation != 3 || s.ObservedGeneration != 3 {
		t.Errorf("generations are %d/%d, want 3/3 from the running spec", s.Generation, s.ObservedGeneration)
	}
}

// A CONFIG STORE ANSWERING ABOUT THE WRONG PIPELINE IS NOT AN ANSWER. The revision beside a spec
// belongs to that spec, and reporting it as this pipeline's generation would put a plausible,
// wrong number in front of an operator — worse than an absent one, which at least reads as absent.
func TestAConfigStoreThatAnswersAboutAnotherPipelineIsNotBelieved(t *testing.T) {
	wrong := spec.Spec{Tenant: "acme", ID: "somebody-else", Revision: 99}
	f := startConfigFixture(t, failingConfig{spec: wrong, revision: 99},
		spec.Spec{Tenant: "acme", ID: "p1", Revision: 4})

	c := f.await(t, "unreachable", func(c telemetry.Condition) bool {
		return c.Reason == telemetry.ReasonConfigStoreUnreachable
	})
	if c.Status != telemetry.StatusUnknown {
		t.Errorf("a mismatched answer is %s/%s, want unknown/config_store_unreachable", c.Status, c.Reason)
	}
	if !strings.Contains(c.Message, "somebody-else") {
		t.Errorf("the message %q does not name the spec the store answered with", c.Message)
	}
	if s := f.p.Status(telemetry.StatusQuery{}); s.Generation == 99 {
		t.Error("Generation is 99, which is another pipeline's revision reported as this one's")
	}
}

// A PIPELINE THAT HAS NOT STARTED HAS NOT ASKED, and must not answer as though it had. This is the
// pending document's version of the same claim the whole condition used to make: reporting applied
// on the strength of never having looked. It is unknown here and true only for a worker that has no
// store to look at.
func TestABuiltPipelineWithAConfigStoreDoesNotClaimItsSpecIsApplied(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 2)
	name := registerCollector(t, "config_pending", &collector{})

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	s := pipelineSpec(name, path)
	s.Revision = 5

	for _, tc := range []struct {
		what   string
		cfg    store.ConfigStore
		status telemetry.Status
		reason string
	}{
		{"with a config store", memstore.NewConfig(), telemetry.StatusUnknown, telemetry.ReasonStarting},
		{"with none", nil, telemetry.StatusTrue, telemetry.ReasonApplied},
	} {
		p, _, diags := engine.Build(context.Background(), registry.Default, s,
			engine.Deps{State: st, Worker: "test", Config: tc.cfg, GracePeriod: time.Second})
		if diags.HasErrors() {
			t.Fatalf("Build: %v", diags)
		}

		doc := p.Status(telemetry.StatusQuery{})
		if doc.Phase != telemetry.PhasePending {
			t.Fatalf("%s: phase is %s, want pending", tc.what, doc.Phase)
		}
		c := conditionOf(t, doc, telemetry.CondSpecApplied)
		if c.Status != tc.status || c.Reason != tc.reason {
			t.Errorf("a built, never-started pipeline %s reports %s/%s, want %s/%s",
				tc.what, c.Status, c.Reason, tc.status, tc.reason)
		}
		p.Close(context.Background())
	}
}

// A CANCELLED READ IS THE SHUTDOWN, NOT AN UNREACHABLE STORE. Every stopping pipeline has a config
// read in flight or about to be, and publishing its context error would leave the final status
// document — the one an operator reads to find out how the run ended — blaming the control plane for
// the fact that the process was asked to stop.
func TestStoppingDoesNotLeaveTheDocumentBlamingTheConfigStore(t *testing.T) {
	cfg := &cancelAwareConfig{spec: spec.Spec{Tenant: "acme", ID: "p1", Revision: 6}, rev: 6}
	f := startConfigFixture(t, cfg, spec.Spec{Tenant: "acme", ID: "p1", Revision: 6})

	f.await(t, "true", func(c telemetry.Condition) bool { return c.Status == telemetry.StatusTrue })

	// Gated on the second call having STARTED, not on a sleep: that read is the one this test is
	// about, and stopping before the reconcile timer fires would make the whole thing vacuous.
	deadline := time.Now().Add(10 * time.Second)
	for cfg.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	// The second read is now blocked in Get and will come back with the context's error.
	f.stop()

	c := conditionOf(t, f.p.Status(telemetry.StatusQuery{}), telemetry.CondSpecApplied)
	if c.Reason == telemetry.ReasonConfigStoreUnreachable {
		t.Errorf("the final document reports the config store unreachable (%s) because the run was "+
			"cancelled; the shutdown is not an outage", c.Message)
	}
	if c.Status != telemetry.StatusTrue {
		t.Errorf("the final document reports spec_applied %s/%s, want the last real observation "+
			"true/applied", c.Status, c.Reason)
	}
	if calls := cfg.calls.Load(); calls < 2 {
		t.Errorf("Get was called %d times; the cancelled read this test is about never happened", calls)
	}
}

// A CALLER WHO SETS Deps.Config AND NOTHING ELSE MUST GET A WORKING PIPELINE. ConfigInterval drives
// a time.Ticker, and time.NewTicker panics on a non-positive duration — so the default is not a
// convenience here, it is the difference between the config watch working and the run dying on its
// first line. Every other test in this file sets the interval explicitly, which is exactly how that
// would go unnoticed.
func TestSettingOnlyTheConfigStoreIsEnoughToRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 2)

	c := &collector{}
	name := registerCollector(t, "config_default", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test", Config: memstore.NewConfig(), GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run with a config store and no ConfigInterval: %v", err)
	}
	if got := len(c.got()); got != 2 {
		t.Errorf("the pipeline delivered %d of 2 records", got)
	}
}

// ONE DOCUMENT REPORTS ONE STORED REVISION. Generation and the spec_applied message are computed at
// opposite ends of Status, and the config watch publishes to the atomic between them — so loading
// the observation twice yields a document that reads "generation 9" beside "revision 8 is stored",
// contradicting itself in exactly the field pair this whole feature exists to produce.
//
// The store hands back a new revision on every call and the watch runs flat out, so the two loads
// land on different observations constantly rather than once in a blue moon. Putting the second load
// back where it was — Generation at the top of Status, the condition's inside computeLocked — fails
// this twenty times out of twenty. Moving the two loads onto ADJACENT LINES instead fails it once in
// twenty, which is worth knowing: the size of the window is the whole difference between a race this
// catches and one it does not, so an injection has to reproduce the real distance and not just the
// shape.
func TestOneDocumentReportsOneStoredRevision(t *testing.T) {
	cfg := &advancingConfig{spec: spec.Spec{Tenant: "acme", ID: "p1"}}
	f := startConfigFixture(t, cfg, spec.Spec{Tenant: "acme", ID: "p1", Revision: 1},
		func(d *engine.Deps) { d.ConfigInterval = 100 * time.Microsecond })

	f.await(t, "false", func(c telemetry.Condition) bool { return c.Reason == telemetry.ReasonPending })

	for i := 0; i < 20000; i++ {
		s := f.p.Status(telemetry.StatusQuery{})
		c := conditionOf(t, s, telemetry.CondSpecApplied)
		if c.Reason != telemetry.ReasonPending {
			continue
		}
		var stored, running uint64
		if n, err := fmt.Sscanf(c.Message, "revision %d is stored but %d is running", &stored, &running); n != 2 {
			t.Fatalf("the message %q no longer names both revisions (%v); this test reads them out "+
				"of it, so a reworded message must be reflected here", c.Message, err)
		}
		if stored != s.Generation {
			t.Fatalf("read %d: the document says generation %d and its own spec_applied condition "+
				"says revision %d is stored.\n  message: %s\n"+
				"  one document must report one observation of the config store", i, s.Generation, stored, c.Message)
		}
		if running != s.ObservedGeneration {
			t.Fatalf("read %d: the document says observedGeneration %d and the message says %d is running",
				i, s.ObservedGeneration, running)
		}
	}
	if got := cfg.calls.Load(); got < 100 {
		t.Errorf("the config store was read %d times; the watch was not publishing fast enough for "+
			"this test to have raced anything", got)
	}
}

// --- store doubles -------------------------------------------------------------------------------

// advancingConfig hands back a different revision on every call, so a document built from two
// separate loads of the observation is wrong almost every time rather than once in a while.
type advancingConfig struct {
	spec  spec.Spec
	calls atomic.Int64
}

func (c *advancingConfig) Get(context.Context, record.TenantID, record.PipelineID) (spec.Spec, uint64, error) {
	// Starts at 2, so it is never equal to the fixture's applied revision of 1 and the condition is
	// always the false/pending arm this test reads its numbers out of.
	return c.spec, uint64(c.calls.Add(1)) + 1, nil
}
func (c *advancingConfig) List(context.Context, record.TenantID) ([]spec.Summary, error) {
	return nil, nil
}
func (c *advancingConfig) Put(context.Context, spec.Spec, uint64) (uint64, error) { return 0, nil }
func (c *advancingConfig) Delete(context.Context, record.TenantID, record.PipelineID, uint64) error {
	return nil
}
func (c *advancingConfig) Watch(context.Context, uint64) (<-chan store.ConfigEvent, error) {
	return nil, errors.New("this store has no watch")
}

// cancelAwareConfig answers the first Get and then blocks until its context is cancelled, which is
// the state a config read is in when a running pipeline is stopped mid-poll.
type cancelAwareConfig struct {
	spec  spec.Spec
	rev   uint64
	calls atomic.Int64
}

func (c *cancelAwareConfig) Get(ctx context.Context, _ record.TenantID, _ record.PipelineID) (spec.Spec, uint64, error) {
	if c.calls.Add(1) == 1 {
		return c.spec, c.rev, nil
	}
	<-ctx.Done()
	return spec.Spec{}, 0, ctx.Err()
}
func (c *cancelAwareConfig) List(context.Context, record.TenantID) ([]spec.Summary, error) {
	return nil, nil
}
func (c *cancelAwareConfig) Put(context.Context, spec.Spec, uint64) (uint64, error) { return 0, nil }
func (c *cancelAwareConfig) Delete(context.Context, record.TenantID, record.PipelineID, uint64) error {
	return nil
}
func (c *cancelAwareConfig) Watch(context.Context, uint64) (<-chan store.ConfigEvent, error) {
	return nil, errors.New("this store has no watch")
}

// failingConfig answers every Get the same way: with err if it is set, and otherwise with a spec
// that may not be the one that was asked for.
type failingConfig struct {
	err      error
	spec     spec.Spec
	revision uint64
}

func (f failingConfig) Get(context.Context, record.TenantID, record.PipelineID) (spec.Spec, uint64, error) {
	return f.spec, f.revision, f.err
}
func (f failingConfig) List(context.Context, record.TenantID) ([]spec.Summary, error) {
	return nil, f.err
}
func (f failingConfig) Put(context.Context, spec.Spec, uint64) (uint64, error) { return 0, f.err }
func (f failingConfig) Delete(context.Context, record.TenantID, record.PipelineID, uint64) error {
	return f.err
}
func (f failingConfig) Watch(context.Context, uint64) (<-chan store.ConfigEvent, error) {
	return nil, errors.New("this store has no watch")
}

// noWatchConfig is a real store with its watch taken away: cmd/canal's file projection in miniature.
type noWatchConfig struct{ *memstore.ConfigStore }

func (noWatchConfig) Watch(context.Context, uint64) (<-chan store.ConfigEvent, error) {
	return nil, errors.New("this store has no watch")
}
