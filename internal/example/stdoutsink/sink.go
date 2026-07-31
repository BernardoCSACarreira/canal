// Package stdoutsink writes encoded bytes to standard output.
//
// It is the smallest useful sink: three methods and a registration block. It sees no offset, no position,
// no lane and no callback, because a sink has no progress awareness whatsoever — which is why a new sink
// cannot get progress wrong.
package stdoutsink

import (
	"bufio"
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

func init() {
	registry.AddSink(registry.Default, registry.SinkDef[*sink]{
		Meta: registry.Meta{
			Name:    "stdout",
			Version: "1.0.0",
			Title:   "Standard output",
			Summary: "Writes encoded bytes to standard output.",
			Support: registry.SupportCommunity,
		},
		// The registry appends codec, batching, retry, when_full, max_in_flight and dedupe.
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 1,
			Modes:          []connector.DestMode{connector.DestAppend},
			Idempotent:     false,
			PartialFailure: false,
		},
		New: func(context.Context, *config.Config) (*sink, error) { return &sink{}, nil },
	})
}

type sink struct {
	f *os.File
	w *bufio.Writer
}

func (k *sink) Open(_ context.Context, _ connector.SinkRuntime, _ connector.Opening) error {
	k.f = os.Stdout
	k.w = bufio.NewWriter(k.f)
	return nil
}

func (k *sink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if _, err := k.w.Write(req.Body); err != nil {
		return connector.WriteResult{}, fault.Transient(fault.OpWrite, err)
	}
	// A CLEAN RETURN MEANS DURABLE. This sink does not implement Flusher, so the core settles on this
	// return and on nothing else — which makes the flush and the sync mandatory HERE, not deferred to
	// Close.
	//
	// Returning nil with bytes still sitting in an unflushed writer is design rule R4's original
	// violation in three lines of example code, and it is why this comment exists.
	if err := k.w.Flush(); err != nil {
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite, err)
	}
	if err := k.f.Sync(); err != nil && !syncUnsupported(err) {
		// A partial write may already have landed, so the class is Indeterminate and never Transient.
		// Claiming Transient here would violate the retry-safety obligation.
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite, err)
	}
	return connector.AllWritten(req.Count), nil
}

func (k *sink) Close(context.Context) error {
	if k.w == nil {
		return nil
	}
	return k.w.Flush()
}

// syncUnsupported reports whether an fsync error means "this file descriptor has nothing to sync"
// rather than "the sync failed".
//
// stdout is routinely a pipe, /dev/null or a terminal, and none of them can be fsynced. The kernel
// says so with a different errno on each platform and each target, and the guard here previously
// tolerated only os.ErrInvalid — so `canal | head`, `canal > /dev/null` and running attached to a
// terminal each returned an Indeterminate fault with Written=0 for a batch whose bytes had already
// left the process. That is the worst possible answer: the engine must then assume the write may or
// may not have landed, on the happy path, every time.
//
// Measured on darwin/arm64: /dev/null gives ENODEV, a pipe gives EBADF, a regular file gives nil.
// Linux answers EINVAL for the first two. ENOTTY and ENOTSUP appear on other Unixes.
//
// EBADF is in the list, which is worth justifying because it usually means a genuinely bad
// descriptor: k.w.Flush() has already succeeded on this same descriptor immediately above, so the
// bytes reached it. A descriptor that accepts a write and rejects an fsync is a pipe, not a
// programming error.
func syncUnsupported(err error) bool {
	return errors.Is(err, os.ErrInvalid) ||
		errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EBADF)
}
