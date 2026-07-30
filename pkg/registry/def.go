package registry

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// Meta is the human-facing half of every component definition. It is embedded rather than
// repeated so that adding a documentation field is one edit rather than nine.
type Meta struct {
	// Name is the registry key and the wire identifier. IMMUTABLE FOREVER: it appears in
	// persisted specs and in checkpoint headers.
	Name string

	// Version is the connector's own semver, recorded in every checkpoint header so an operator
	// can see which build wrote the state they are looking at.
	Version string

	Title   string
	Summary string
	Docs    string

	// Notes is where a source deriving Origin.Key documents the derivation (design rule R5).
	// Declaring SourceCaps.StableKeys with empty Notes fails registration lint, because a
	// derived key nobody can reason about is a key nobody can trust.
	Notes string

	Support Support
}

// SourceDef is what a connector package registers.
//
// New receives PRE-PARSED, PRE-VALIDATED, PRE-DEFAULTED config — there is no Configure callback
// and no map re-parsed inside the connector — and New must NOT do I/O. I/O belongs in Open, which
// the engine retries with backoff.
//
// The type parameter S exists for exactly one reason: it lets [AddSource] verify at registration
// time that the declared capabilities match the concrete type that will implement them, without
// instantiating anything and without reflection. `var z S` works because method sets belong to
// types.
//
// S is the RETURN TYPE OF New, so it is inferred from the func literal and is never a phantom. A
// phantom type parameter naming a type nobody returns interrogates the wrong method set.
type SourceDef[S connector.Source] struct {
	Meta

	Spec *config.Spec
	Caps connector.SourceCaps

	New func(ctx context.Context, cfg *config.Config) (S, error)
}

// SinkDef is the sink half. Same shape, same rules.
type SinkDef[K connector.Sink] struct {
	Meta

	Spec *config.Spec
	Caps connector.SinkCaps

	New func(ctx context.Context, cfg *config.Config) (K, error)
}

// TransformDef defines a transform.
type TransformDef[T connector.Transform] struct {
	Meta

	Spec *config.Spec
	Caps connector.TransformCaps

	New func(ctx context.Context, cfg *config.Config) (T, error)
}

// BufferDef defines a buffer.
type BufferDef[B connector.Buffer] struct {
	Meta

	Spec *config.Spec
	Caps connector.BufferCaps

	New func(ctx context.Context, cfg *config.Config) (B, error)
}

// EncoderDef defines an encoder. Every kind has a Caps struct embedding connector.Caps with an
// APIVersion; none is exempt, because a kind whose contract cannot be versioned is a kind that
// will break silently.
type EncoderDef[E connector.Encoder] struct {
	Meta

	Spec *config.Spec
	Caps connector.CodecCaps

	New func(ctx context.Context, cfg *config.Config) (E, error)
}

// DecoderDef defines a decoder.
type DecoderDef[D connector.Decoder] struct {
	Meta

	Spec *config.Spec
	Caps connector.CodecCaps

	New func(ctx context.Context, cfg *config.Config) (D, error)
}

// FramerDef defines a framer.
type FramerDef[F connector.Framer] struct {
	Meta

	Spec *config.Spec
	Caps connector.FramerCaps

	New func(ctx context.Context, cfg *config.Config) (F, error)
}

// DeframerDef defines a deframer.
type DeframerDef[D connector.Deframer] struct {
	Meta

	Spec *config.Spec
	Caps connector.FramerCaps

	New func(ctx context.Context, cfg *config.Config) (D, error)
}

// CompressorDef defines a compressor. The matching decompressor is discovered by name from the
// same definition when the component implements both, which is why there is no separate
// DecompressorDef: one artefact, one name, one version.
type CompressorDef[C connector.Compressor] struct {
	Meta

	Spec *config.Spec
	Caps connector.CompressorCaps

	New func(ctx context.Context, cfg *config.Config) (C, error)
}
