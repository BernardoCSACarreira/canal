package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// RECOVERY IS THE HALF THAT MAKES A LOST CONFIRMATION SELF-REPAIRING.
//
// The checkpoint is written BEFORE the commit, so a crash between them leaves committables in the
// store that the destination may or may not hold. Without recovery those are orphaned staged
// artifacts, and the next checkpoint advances a cursor past records nobody published. These tests
// crash the pipeline in exactly that window and assert the next run resolves it.

// twoPhase is a Committer whose every step a test can control and observe.
type twoPhase struct {
	mu sync.Mutex

	staged   []string // handed to Write, not yet published
	staging  []string // minted into a committable, awaiting a commit
	pub      []string // published
	prepared int
	commits  int

	// commitErr fails Commit, which is the crash window this file is about.
	commitErr error

	// resolveAs is the disposition ResolveStale answers with on the next run. Nil means the sink
	// has no StaleResolver and the engine must fall back to AbortStale.
	resolveAs *connector.Disposition
	aborted   int
	resolved  int
	restored  int
}

func (s *twoPhase) Open(context.Context, connector.SinkRuntime, connector.Opening) error { return nil }
func (s *twoPhase) Close(context.Context) error                                          { return nil }

func (s *twoPhase) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ln := range strings.Split(strings.TrimSuffix(string(req.Body), "\n"), "\n") {
		if ln != "" {
			s.staged = append(s.staged, ln)
		}
	}
	return connector.AllWritten(req.Count), nil
}

func (s *twoPhase) PrepareCommit(_ context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepared++
	if len(s.staged) == 0 && len(s.staging) == 0 {
		return nil, nil
	}
	s.staging = append(s.staging, s.staged...)
	s.staged = nil
	// One committable covering everything staged, re-minted under the newest id each time — the
	// shape the subsuming contract exists for.
	return []connector.Committable{{
		Handle:  record.Blob{Version: 1, Bytes: []byte("upload-1")},
		Records: int64(len(s.staging)),
	}}, nil
}

func (s *twoPhase) Commit(_ context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	if s.commitErr != nil {
		return nil, s.commitErr
	}
	out := make([]connector.CommitOutcome, 0, len(cs))
	for _, c := range cs {
		out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionCommitted})
	}
	s.pub = append(s.pub, s.staging...)
	s.staging = nil
	return out, nil
}

func (s *twoPhase) AbortStale(_ context.Context, cs []connector.Committable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborted += len(cs)
	return nil
}

func (s *twoPhase) ResolveStale(_ context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolved += len(cs)
	out := make([]connector.CommitOutcome, 0, len(cs))
	for _, c := range cs {
		out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: *s.resolveAs})
	}
	return out, nil
}

func (s *twoPhase) SnapshotState(context.Context, uint64) ([]record.Blob, error) {
	return []record.Blob{{Version: 1, Bytes: []byte("open-upload")}}, nil
}

func (s *twoPhase) RestoreState(_ context.Context, bs []record.Blob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restored += len(bs)
	return nil
}

func (s *twoPhase) read(fn func(*twoPhase)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

// registerTwoPhase registers the sink with exactly the optional tiers the test asks for.
func registerTwoPhase(t *testing.T, prefix string, s *twoPhase, resolves bool) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, sinkSeq.Add(1))
	caps := connector.SinkCaps{
		Caps:           connector.Caps{APIVersion: connector.APIVersion},
		Modes:          []connector.DestMode{connector.DestAppend},
		MaxConcurrency: 1,
		Idempotent:     true,
		Commits:        true,
		KeepsState:     true,
		ResolvesStale:  resolves,
	}
	registry.AddSink(registry.Default, registry.SinkDef[*twoPhase]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Two phase",
			Summary: "Stages on Write and publishes on Commit.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: caps,
		New:  func(context.Context, *config.Config) (*twoPhase, error) { return s, nil },
	})
	return name
}

func twoPhaseSpec(sinkName, path string) spec.Spec {
	s := pipelineSpec(sinkName, path)
	s.Tenant, s.ID = "acme", "recov"
	return s
}

// runTwoPhase runs one generation against a shared state directory.
func runTwoPhase(t *testing.T, s spec.Spec, stateDir string) error {
	t.Helper()
	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{
		State: st, Worker: "test", FlushInterval: 10 * time.Millisecond, GracePeriod: 2 * time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return p.Run(ctx)
}

// TestACommitLeftInDoubtIsResolvedOnTheNextRun is the crash window the protocol is ordered around.
//
// Generation one writes the checkpoint and then fails its commit, which is precisely the state a
// crash between those two steps leaves behind. Generation two must find the committable, hand it
// back to the sink, and act on the answer — rather than starting clean and leaving a staged
// artifact at the destination that nothing will ever publish or reclaim.
func TestACommitLeftInDoubtIsResolvedOnTheNextRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 6)
	stateDir := filepath.Join(dir, "state")

	sink := &twoPhase{commitErr: fault.Transient(fault.OpCommitSink, errors.New("the warehouse timed out"))}
	committed := connector.DispositionCommitted
	sink.resolveAs = &committed
	name := registerTwoPhase(t, "twophase_doubt", sink, true)

	// Generation one: the checkpoint lands, the commit does not.
	if err := runTwoPhase(t, twoPhaseSpec(name, path), stateDir); err == nil {
		t.Fatal("generation one should have failed on its commit")
	}

	// The committable is IN THE CHECKPOINT. That is the whole point of writing it before committing.
	cp := readCheckpointAt(t, stateDir, "acme", "recov")
	if len(cp.Committables) == 0 {
		t.Fatalf("no committable was persisted, so a crash here would orphan the staged data:\n%+v", cp.Header)
	}

	// Generation two: recovery must offer it back before a single record moves.
	sink.read(func(s *twoPhase) { s.commitErr = nil })
	before := 0
	sink.read(func(s *twoPhase) { before = s.resolved })
	_ = runTwoPhase(t, twoPhaseSpec(name, path), stateDir)

	sink.read(func(s *twoPhase) {
		if s.resolved <= before {
			t.Error("the recovered committable was never offered back to the sink")
		}
		if s.aborted != 0 {
			t.Errorf("AbortStale was called %d times although the sink implements StaleResolver, "+
				"which the core is documented to prefer", s.aborted)
		}
		if s.restored == 0 {
			t.Error("writer state was never restored, so an open upload would be leaked")
		}
	})
}

// A sink WITHOUT StaleResolver falls back to AbortStale, which is the weaker answer and is
// sufficient only because its abort cannot partially succeed.
func TestRecoveryFallsBackToAbortStale(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 6)
	stateDir := filepath.Join(dir, "state")

	sink := &twoPhase{commitErr: fault.Transient(fault.OpCommitSink, errors.New("timeout"))}
	name := registerTwoPhase(t, "twophase_abort", sink, false)

	if err := runTwoPhase(t, twoPhaseSpec(name, path), stateDir); err == nil {
		t.Fatal("generation one should have failed on its commit")
	}
	sink.read(func(s *twoPhase) { s.commitErr = nil })
	_ = runTwoPhase(t, twoPhaseSpec(name, path), stateDir)

	sink.read(func(s *twoPhase) {
		if s.aborted == 0 {
			t.Error("AbortStale was never called for a sink with no StaleResolver")
		}
		if s.resolved != 0 {
			t.Errorf("ResolveStale was called %d times on a sink that does not implement it", s.resolved)
		}
	})
}

// IN DOUBT MEANS DO NOT START. Neither committing nor rolling back is safe, so the honest answer is
// to refuse rather than to guess — rolling back what landed loses data, committing what did not
// creates it.
func TestRecoveryRefusesToStartWhenTheSinkCannotTell(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 6)
	stateDir := filepath.Join(dir, "state")

	sink := &twoPhase{commitErr: fault.Transient(fault.OpCommitSink, errors.New("timeout"))}
	inDoubt := connector.DispositionRetryLater
	sink.resolveAs = &inDoubt
	name := registerTwoPhase(t, "twophase_indoubt", sink, true)

	if err := runTwoPhase(t, twoPhaseSpec(name, path), stateDir); err == nil {
		t.Fatal("generation one should have failed on its commit")
	}
	sink.read(func(s *twoPhase) { s.commitErr = nil })

	err := runTwoPhase(t, twoPhaseSpec(name, path), stateDir)
	if err == nil {
		t.Fatal("the pipeline started although a recovered committable was in doubt")
	}
	for _, want := range []string{"cannot tell", "resolve it at the destination"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q, so it does not tell the operator what to do:\n%v", want, err)
		}
	}
}

// readCheckpointAt decodes the checkpoint for an arbitrary pipeline.
func readCheckpointAt(t *testing.T, stateDir string, tenant record.TenantID, pipeline record.PipelineID) engine.Checkpoint {
	t.Helper()
	st, err := wal.Open(stateDir)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	defer st.Close()
	key := store.CheckpointKey(tenant, pipeline)
	got, err := st.Get(context.Background(), []store.Key{key})
	if err != nil {
		t.Fatalf("reading the checkpoint: %v", err)
	}
	v, ok := got[key.String()]
	if !ok {
		t.Fatal("no checkpoint was ever written")
	}
	var cp engine.Checkpoint
	if err := json.Unmarshal(v.Value, &cp); err != nil {
		t.Fatalf("decoding the checkpoint: %v", err)
	}
	return cp
}
