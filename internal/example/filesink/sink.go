// Package filesink appends encoded bytes to a file on the local filesystem.
//
// It is the second real sink in canal, and the first one whose output survives the process. Until
// it existed the only place a pipeline could write was standard output, which means every claim
// about durability at the destination was being tested against a file descriptor the test harness
// happened to own.
//
// # A clean return means durable, and there is no option to make that false
//
// Every Write flushes the buffer and fsyncs the file before returning. There is deliberately no
// `sync: false` for throughput, and the reason is that [connector.SinkCaps] is fixed at
// REGISTRATION while config is per node: a sink that is durable-on-return under one config and not
// under another cannot describe itself honestly to the negotiation, and the negotiation is the
// whole mechanism by which canal avoids promising a guarantee it cannot keep. Design rule R4 says
// an acknowledgement means durable; a config flag that quietly turns that off is R4's original
// violation with a knob on it.
//
// The cost is one fsync per request, and the answer to it is batching: a framed codec puts a whole
// batch in one request, so the fsync amortises over as many records as the batch holds.
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
	if err := s.w.Flush(); err != nil {
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite,
			fmt.Errorf("flushing %s: %w", s.path, err))
	}
	if err := s.f.Sync(); err != nil {
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite,
			fmt.Errorf("syncing %s: %w", s.path, err))
	}

	// Only NOW is a clean return honest. Returning before the fsync would tell the core these
	// records are durable while they sit in a page cache, and the core would advance a cursor past
	// them — design rule R4 in one function.
	return connector.AllWritten(req.Count), nil
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
