// Package filesink appends encoded bytes to a file on the local filesystem.
//
// It is the second real sink in canal, and the first one whose output survives the process. Until
// it existed the only place a pipeline could write was standard output, which means every claim
// about durability at the destination was being tested against a file descriptor the test harness
// happened to own.
//
// # Durability is earned at Flush, and it is declared, not configured
//
// This sink implements [connector.Flusher]. Write appends to a buffer and returns; Flush writes it
// through and fsyncs. The core knows — SinkCaps.Flushes says so — and therefore does NOT settle a
// record when Write returns. It holds the record, and its ledger reference with it, until the flush
// that makes it safe. That is the whole point of the capability: the durability boundary is where
// the sink says it is, and the engine's accounting follows.
//
// It fsynced on every Write before the engine could drive a Flusher. That was correct and very
// slow: one fsync per request, which for a framed codec is one per batch and for an unframed one is
// one per record. Now it is one per checkpoint interval, and the records in between are explicitly
// not-yet-durable rather than quietly assumed to be.
//
// There is deliberately no `sync: false` and no way to turn any of this off, because
// [connector.SinkCaps] is fixed at REGISTRATION while config is per node: a sink that is durable at
// one point under one config and another point under another cannot describe itself honestly to the
// negotiation, and the negotiation is the whole mechanism by which canal avoids promising a
// guarantee it cannot keep.
//
// # Append only
//
// The file is opened O_APPEND and never truncated. A sink that truncated on open would destroy
// everything a previous run had written the moment it restarted — and at-least-once delivery means
// restarts are ROUTINE, not exceptional. There is no truncate option for the same reason there is
// no sync option.
package filesink

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// Name is the registered name.
const Name = "file"

func init() { Register(registry.Default) }

// Register adds this sink to r.
//
// Exported as well as run from init so a test, or a binary assembling an explicit registry, can
// choose its catalogue rather than inherit one by import side effect.
func Register(r *registry.Registry) {
	registry.AddSink(r, registry.SinkDef[*sink]{
		Meta: registry.Meta{
			Name:    Name,
			Version: "1.0.0",
			Title:   "File",
			Summary: "Appends encoded bytes to a file, fsynced before the write returns.",
			Notes: "Append only and always fsynced. Neither is configurable: a sink whose durability " +
				"depends on its config cannot describe itself honestly to the negotiation.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec().
			Field(config.Field{
				Name:        "path",
				Title:       "Path",
				Type:        config.TypeString,
				Description: "File to append to. Parent directories are created if missing.",
			}),
		Caps: connector.SinkCaps{
			Caps:  connector.Caps{APIVersion: connector.APIVersion},
			Modes: []connector.DestMode{connector.DestAppend},

			// One writer. Two goroutines appending to one descriptor would interleave partial
			// writes, and the ordering a file's readers assume would stop holding.
			MaxConcurrency: 1,

			// Appending the same bytes twice appends them twice. Saying otherwise would let the
			// negotiation offer effectively-once on top of a sink that cannot collapse anything.
			Idempotent: false,

			// A write either lands whole or does not land: there is no per-record failure a
			// filesystem reports, so there is nothing honest to put in WriteResult.Failed.
			PartialFailure: false,

			// Durability is earned here, not when Write returns. The engine holds every record
			// until the flush that covers it.
			Flushes: true,
		},
		New: func(_ context.Context, cfg *config.Config) (*sink, error) {
			path, err := config.Get[string](cfg, "path")
			if err != nil {
				return nil, err
			}
			// New does NO I/O — the file is opened in Open. Constructing is allowed to fail on
			// config, never on the world being unavailable.
			return &sink{path: path}, nil
		},
	})
}

type sink struct {
	path string

	// mu guards the writer against a core that ever raises MaxConcurrency above one. The caps say
	// one, so this is cheap insurance rather than a load-bearing lock, and it means a bug in the
	// core corrupts nothing here.
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func (s *sink) Open(_ context.Context, _ connector.SinkRuntime, _ connector.Opening) error {
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fault.Transient(fault.OpOpen, fmt.Errorf("creating %s: %w", dir, err))
		}
	}
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		// Transient, so Open is retried with backoff: a missing mount or a momentarily full disk
		// is exactly the case where trying again in a second is the right answer.
		return fault.Transient(fault.OpOpen, fmt.Errorf("opening %s: %w", s.path, err))
	}

	s.mu.Lock()
	s.f, s.w = f, bufio.NewWriter(f)
	s.mu.Unlock()

	// The directory entry for a file that was just CREATED is metadata separate from the file, and
	// a crash can leave a written file with no name pointing at it. Syncing the parent is what
	// makes the file itself durable, and it is the same mistake pkg/store/wal documents at length.
	return syncParent(s.path)
}

func (s *sink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w == nil {
		return connector.WriteResult{}, fault.Bug(fault.OpWrite,
			fmt.Errorf("filesink: Write before Open"))
	}

	if _, err := s.w.Write(req.Body); err != nil {
		// The buffer may hold a partial request now, so the effect is not known to have been
		// avoided. Indeterminate rather than Transient: claiming the write definitely did not land
		// would violate the retry-safety obligation in pkg/fault.
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite,
			fmt.Errorf("writing to %s: %w", s.path, err))
	}

	// ACCEPTED, NOT DURABLE. A clean return here means the bytes are in this process's buffer and
	// nothing more. Because SinkCaps.Flushes is declared, the core reads it that way and withholds
	// settlement until Flush; a sink that returned this WITHOUT declaring Flushes would be telling
	// the engine to advance a cursor past data still in userspace.
	return connector.AllWritten(req.Count), nil
}

// Flush writes the buffer through and fsyncs. Only after it returns cleanly are the records it
// covers durable, and only then does the core settle them.
//
// reason is not consulted. A file has nothing to finalise differently at end of input — no manifest
// to write, no undersized artifact to seal — so every reason means the same thing here. A staging
// sink is where the distinction earns its place.
func (s *sink) Flush(_ context.Context, _ connector.FlushReason) (connector.WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return connector.WriteResult{}, fault.Bug(fault.OpFlush,
			fmt.Errorf("filesink: Flush before Open"))
	}
	if err := s.w.Flush(); err != nil {
		return connector.WriteResult{}, fault.Unknown(fault.OpFlush,
			fmt.Errorf("flushing %s: %w", s.path, err))
	}
	if err := s.f.Sync(); err != nil {
		return connector.WriteResult{}, fault.Unknown(fault.OpFlush,
			fmt.Errorf("syncing %s: %w", s.path, err))
	}
	// An empty WriteResult means "everything you were holding is durable". The core knows which
	// records those are; this sink does not track them, and does not need to.
	return connector.WriteResult{}, nil
}

// Close flushes and closes. It is safe on a sink that was never opened, which the core relies on
// after a failed Open and after config validation.
func (s *sink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	s.f, s.w = nil, nil
	return err
}
