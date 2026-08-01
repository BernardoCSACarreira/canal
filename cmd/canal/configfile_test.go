package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// THE FILE IS THE CONTROL PLANE ON A LAPTOP. Without this projection `canal run` would have no
// config store, CondSpecApplied would be structurally true in the only binary this module ships, and
// the condition would be exactly as inert from an operator's chair as it was before Deps.Config had
// a reader anywhere.

func writeSpecFile(t *testing.T, dir string, s spec.Spec) string {
	t.Helper()
	path := filepath.Join(dir, "pipeline.json")
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshalling the spec: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// The operator's own `revision` field is the stored revision, re-read from disk. Bumping it is what
// makes canal report that the process is running something older.
func TestTheFilesRevisionFieldIsTheStoredRevision(t *testing.T) {
	dir := t.TempDir()
	s := spec.Spec{Tenant: "acme", ID: "p1", Revision: 3}
	path := writeSpecFile(t, dir, s)

	f := newSpecFile(path, s)
	got, rev, err := f.Get(context.Background(), "acme", "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rev != 3 || got.Revision != 3 {
		t.Errorf("Get returned revision %d (spec says %d), want 3", rev, got.Revision)
	}

	// The edit an operator makes.
	s.Revision = 4
	writeSpecFile(t, dir, s)
	if _, rev, err = f.Get(context.Background(), "acme", "p1"); err != nil {
		t.Fatalf("Get after the edit: %v", err)
	}
	if rev != 4 {
		t.Errorf("Get returned revision %d after the file was edited to 4; the file is re-read on "+
			"every call or the condition can never notice a change", rev)
	}
}

// A REMOVED FILE IS A WITHDRAWN SPEC AND A CORRUPT ONE IS NOT. Half a spec on disk is what an
// editor's write looks like for a few milliseconds, and reporting it as a deletion would flap the
// condition between "your config was withdrawn" and "applied" every time somebody saves.
func TestAMissingFileIsDeletedAndAnUnparseableOneIsTransient(t *testing.T) {
	dir := t.TempDir()
	s := spec.Spec{Tenant: "acme", ID: "p1", Revision: 1}
	path := writeSpecFile(t, dir, s)
	f := newSpecFile(path, s)

	if err := os.WriteFile(path, []byte("{ this is not json"), 0o644); err != nil {
		t.Fatalf("corrupting the file: %v", err)
	}
	_, _, err := f.Get(context.Background(), "acme", "p1")
	if errors.Is(err, store.ErrNoSpec) {
		t.Error("a file that does not parse was reported as store.ErrNoSpec, which the engine renders " +
			"as 'the pipeline is no longer stored'. A partial write during a save is not a deletion")
	}
	if fault.ClassOf(err) != fault.TransientUpstream {
		t.Errorf("an unparseable file classified as %s, want transient: it is expected to parse on "+
			"the next read", fault.ClassOf(err))
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("removing the file: %v", err)
	}
	if _, _, err := f.Get(context.Background(), "acme", "p1"); !errors.Is(err, store.ErrNoSpec) {
		t.Errorf("a removed file gave %v, want store.ErrNoSpec: the stored spec is genuinely gone", err)
	}
}

// The projection is SCOPED to the pipeline it was built for. An operator who edits the file into a
// different pipeline gets "no longer stored" rather than a revision belonging to something this
// process is not running.
func TestTheProjectionAnswersOnlyForItsOwnPipeline(t *testing.T) {
	dir := t.TempDir()
	s := spec.Spec{Tenant: "acme", ID: "p1", Revision: 1}
	path := writeSpecFile(t, dir, s)
	f := newSpecFile(path, s)

	if _, _, err := f.Get(context.Background(), "acme", "somebody-else"); !errors.Is(err, store.ErrNoSpec) {
		t.Errorf("Get for another pipeline gave %v, want store.ErrNoSpec", err)
	}

	// And when the file itself is edited into another pipeline.
	writeSpecFile(t, dir, spec.Spec{Tenant: "acme", ID: "p2", Revision: 9})
	if _, rev, err := f.Get(context.Background(), "acme", "p1"); !errors.Is(err, store.ErrNoSpec) {
		t.Errorf("the file now holds p2 and Get for p1 returned revision %d (%v); reporting another "+
			"pipeline's revision is worse than reporting none, because the number looks right", rev, err)
	}
}

// Writes refuse, and the watch declines. A DECLINED WATCH IS A SUPPORTED DEPLOYMENT: the engine's
// config loop falls back to its reconcile timer, which is what store.ConfigStore.Watch's "never a
// correctness dependency" is for. This is the implementation that makes that claim load-bearing.
func TestTheFileProjectionIsReadOnlyAndHasNoWatch(t *testing.T) {
	dir := t.TempDir()
	s := spec.Spec{Tenant: "acme", ID: "p1", Revision: 1}
	f := newSpecFile(writeSpecFile(t, dir, s), s)
	ctx := context.Background()

	if _, err := f.Put(ctx, s, 1); err == nil {
		t.Error("Put succeeded against a read-only projection")
	}
	if err := f.Delete(ctx, "acme", "p1", 1); err == nil {
		t.Error("Delete succeeded against a read-only projection")
	}
	ch, err := f.Watch(ctx, 0)
	if err == nil {
		t.Error("Watch returned no error; declining is what sends the engine to its reconcile timer")
	}
	if ch != nil {
		t.Error("Watch returned a channel alongside an error; a consumer that ranges over it would " +
			"block forever waiting for events nothing will send")
	}
}

// List renders the one pipeline the file holds, scoped to its tenant.
func TestListReturnsTheOnePipelineTheFileHolds(t *testing.T) {
	dir := t.TempDir()
	s := spec.Spec{Tenant: "acme", ID: "p1", Revision: 2, Title: "the one"}
	f := newSpecFile(writeSpecFile(t, dir, s), s)

	got, err := f.List(context.Background(), "acme")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].ID != "p1" || got[0].Revision != 2 {
		t.Fatalf("List returned %+v, want one summary for p1 at revision 2", got)
	}
	if got[0].UpdatedAt.IsZero() {
		t.Error("the summary has no UpdatedAt; the file's modification time is the only one it has")
	}

	other, err := f.List(context.Background(), "someone-else")
	if err != nil {
		t.Fatalf("List for another tenant: %v", err)
	}
	if len(other) != 0 {
		t.Errorf("List returned %d specs for a tenant the file does not hold", len(other))
	}
}
