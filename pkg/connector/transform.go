package connector

import (
	"context"
	"errors"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Transform is the only node kind between source and sink.
//
// The full vocabulary lives in what the transform puts in out: N-to-0 filters, N-to-N
// maps, N-to-M expands or regroups. No extra vocabulary, no separate interfaces per case,
// no one-to-one restriction — a one-to-one-only transform contract is why one surveyed
// ecosystem's transform library is stunted and why conditional application needed its own
// proposal.
//
// Records placed in out MUST come from out.Derive(an in-record) or
// out.Merge(in-records...). A freshly stamped record belongs to no group, and admitting
// one would break settlement. The core ENFORCES this: a record in out whose group is not
// one of in's groups fails the conformance kit at build time and is
// fault.PermanentContract at runtime.
type Transform interface {
	Open(ctx context.Context, rt TransformRuntime) error

	// Apply reads in, starting at index from, and writes out.
	//
	// If out fills, Apply returns [ErrOutputFull] after placing what fits; the engine
	// drains out and calls Apply again with the SAME in and from advanced past what was
	// consumed. Without a continuation, a one-to-N expansion silently drops everything past
	// the cap.
	//
	// consumed is how many records of in, counting from from, were fully handled.
	Apply(ctx context.Context, in *record.Batch, from int, out *record.Batch) (consumed int, err error)

	Close(ctx context.Context) error
}

// ErrOutputFull is the continuation signal from [Transform.Apply]. It is a fault so that
// it classifies like everything else and so errors.Is works through wrapping.
var ErrOutputFull = fault.New(fault.TransientInternal, fault.OpTransform, errOutputFull)

var errOutputFull = errors.New("transform output batch is full; call Apply again")

// StatefulTransform is a SEPARATE interface, not an embedding.
//
// Nesting an interface inside another interface is what prevented one surveyed sink API
// from evolving, and it is forbidden throughout canal. Its presence is declared by
// TransformCaps.KeepsState.
type StatefulTransform interface {
	SnapshotState(ctx context.Context, id uint64) ([]record.Blob, error)
	RestoreState(ctx context.Context, bs []record.Blob) error
}
