package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// specFile projects the --spec file as a read-only [store.ConfigStore].
//
// THE FILE IS THE CONTROL PLANE ON A LAPTOP, which is what the architecture means by "-f
// pipelines.yaml projected in read-only". Without it `canal run` would have no config store at all,
// CondSpecApplied would be structurally true in the only binary this module ships, and the condition
// would be exactly as inert from an operator's chair as it was before it had a reader.
//
// WHAT COUNTS AS A REVISION IS THE OPERATOR'S OWN `revision` FIELD, re-read from disk, and nothing
// else. Not the modification time, which changes when nothing did; not a content hash, which is not
// a number anyone can order or resume from. A spec's revision is a declared fact about a spec, so
// editing the file and bumping the field is what makes canal report that the process is running
// something older — and editing it without bumping the field is the operator saying this is the same
// revision, which canal is in no position to second-guess.
//
// It is deliberately NOT a hot reload. Nothing here restarts anything; see internal/engine/config.go
// for why applying a revision is lifecycle work and reporting it is not.
type specFile struct {
	path   string
	tenant record.TenantID
	id     record.PipelineID
}

// newSpecFile projects path, scoped to the pipeline that was loaded from it.
//
// The scope matters: Get answers ErrNoSpec for anything else, so an operator who edits the file into
// a DIFFERENT pipeline gets "the pipeline is no longer stored" rather than a revision belonging to
// something this process is not running.
func newSpecFile(path string, s spec.Spec) *specFile {
	return &specFile{path: path, tenant: s.Tenant, id: s.ID}
}

// Get re-reads the file. A file that has been removed, or that no longer holds this pipeline, is
// [store.ErrNoSpec] — both are the stored spec being gone, and neither is the store being down.
func (f *specFile) Get(_ context.Context, t record.TenantID, id record.PipelineID) (spec.Spec, uint64, error) {
	s, err := f.read()
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return spec.Spec{}, 0, fault.Permanent(fault.OpRead, store.ErrNoSpec)
	case err != nil:
		// A FILE THAT EXISTS AND DOES NOT PARSE IS NOT A DELETION. Half a spec on disk during an
		// editor's write is transient, and reporting it as a deletion would flap the condition
		// between "your config was withdrawn" and "applied" every time somebody saves.
		return spec.Spec{}, 0, fault.Transient(fault.OpRead, err)
	case s.Tenant != t || s.ID != id:
		return spec.Spec{}, 0, fault.Permanent(fault.OpRead, store.ErrNoSpec)
	}
	return s, s.Revision, nil
}

// List returns the one pipeline this file holds, if it belongs to the tenant asked about.
func (f *specFile) List(_ context.Context, t record.TenantID) ([]spec.Summary, error) {
	s, err := f.read()
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fault.Transient(fault.OpRead, err)
	}
	if s.Tenant != t {
		return nil, nil
	}
	// The modification time is the only "updated at" a file has. It is honest here in a way it would
	// not be as a revision: a listing renders it, and nothing compares it.
	var updated time.Time
	if st, err := os.Stat(f.path); err == nil {
		updated = st.ModTime()
	}
	return []spec.Summary{s.Summarise(updated)}, nil
}

// Put refuses. The file belongs to whoever is editing it.
func (f *specFile) Put(_ context.Context, _ spec.Spec, _ uint64) (uint64, error) {
	return 0, fault.Contract(fault.OpPersist, fmt.Errorf(
		"canal run projects %s read-only; edit the file to change the stored spec", f.path))
}

// Delete refuses, for the same reason.
func (f *specFile) Delete(_ context.Context, _ record.TenantID, _ record.PipelineID, _ uint64) error {
	return fault.Contract(fault.OpPersist, fmt.Errorf(
		"canal run projects %s read-only; delete the file to withdraw the stored spec", f.path))
}

// Watch declines.
//
// A DECLINED WATCH IS A SUPPORTED DEPLOYMENT, not a degraded one, and this is the implementation that
// proves it: the engine's config loop falls back to its reconcile timer, and store.ConfigStore.Watch
// says a watch is a convenience and never a correctness dependency precisely so that it can. Watching
// the file properly means fsnotify, which is a third-party dependency this module does not have, and
// polling the modification time on a timer is the timer that already exists.
func (f *specFile) Watch(_ context.Context, _ uint64) (<-chan store.ConfigEvent, error) {
	return nil, fault.Contract(fault.OpRead, fmt.Errorf(
		"canal run has no file watcher; the config store is re-read on the reconcile interval"))
}

// read parses the file as it is now.
func (f *specFile) read() (spec.Spec, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return spec.Spec{}, err
	}
	return decodeSpec(data, f.path)
}
