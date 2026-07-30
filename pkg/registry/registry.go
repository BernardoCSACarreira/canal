package registry

import (
	"context"
	"sort"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// SourceEntry is a registered source, with its generic definition erased to a factory the engine
// can call without knowing the concrete type.
//
// Erasure happens exactly once, at registration, AFTER the capability cross-check has interrogated
// the concrete type. That ordering is the whole reason the type parameter exists.
type SourceEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.SourceCaps
	Descriptor Descriptor

	// New constructs the source from already-validated config and does no I/O.
	New func(ctx context.Context, cfg *config.Config) (connector.Source, error)
}

// SinkEntry is a registered sink.
type SinkEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.SinkCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Sink, error)
}

// TransformEntry is a registered transform.
type TransformEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.TransformCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Transform, error)
}

// BufferEntry is a registered buffer.
type BufferEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.BufferCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Buffer, error)
}

// EncoderEntry is a registered encoder.
type EncoderEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.CodecCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Encoder, error)
}

// DecoderEntry is a registered decoder.
type DecoderEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.CodecCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Decoder, error)
}

// FramerEntry is a registered framer.
type FramerEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.FramerCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Framer, error)
}

// DeframerEntry is a registered deframer.
type DeframerEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.FramerCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Deframer, error)
}

// CompressorEntry is a registered compressor.
type CompressorEntry struct {
	Meta
	Spec       *config.Spec
	Caps       connector.CompressorCaps
	Descriptor Descriptor

	New func(ctx context.Context, cfg *config.Config) (connector.Compressor, error)
}

// Registry holds component definitions keyed by kind and name.
//
// It is a value type with derivation methods rather than a package-level map, so a test, a
// sandboxed tenant, or a deployment that must not offer a shell-exec sink gets its own registry
// instead of mutating process-global state.
//
// A Registry is safe for concurrent READS after the init phase, which is when every Add happens.
// The Add functions are not safe against concurrent reads and are not meant to be: registration is
// an init-time act.
type Registry struct {
	sources     map[string]SourceEntry
	sinks       map[string]SinkEntry
	transforms  map[string]TransformEntry
	buffers     map[string]BufferEntry
	encoders    map[string]EncoderEntry
	decoders    map[string]DecoderEntry
	framers     map[string]FramerEntry
	deframers   map[string]DeframerEntry
	compressors map[string]CompressorEntry
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		sources:     map[string]SourceEntry{},
		sinks:       map[string]SinkEntry{},
		transforms:  map[string]TransformEntry{},
		buffers:     map[string]BufferEntry{},
		encoders:    map[string]EncoderEntry{},
		decoders:    map[string]DecoderEntry{},
		framers:     map[string]FramerEntry{},
		deframers:   map[string]DeframerEntry{},
		compressors: map[string]CompressorEntry{},
	}
}

// Default is the process registry. Connector packages call registry.AddSource(registry.Default,
// def) from an init function.
//
// An in-tree connector still requires a blank import, which is one edit to one file whose only
// content is imports. "Zero core edits" holds literally for out-of-tree connectors, which is the
// case the extensibility constraint is about.
var Default = New()

// Source looks up a registered source.
func (r *Registry) Source(name string) (SourceEntry, bool) { e, ok := r.sources[name]; return e, ok }

// Sink looks up a registered sink.
func (r *Registry) Sink(name string) (SinkEntry, bool) { e, ok := r.sinks[name]; return e, ok }

// Transform looks up a registered transform.
func (r *Registry) Transform(name string) (TransformEntry, bool) {
	e, ok := r.transforms[name]
	return e, ok
}

// Buffer looks up a registered buffer.
func (r *Registry) Buffer(name string) (BufferEntry, bool) { e, ok := r.buffers[name]; return e, ok }

// Encoder looks up a registered encoder.
func (r *Registry) Encoder(name string) (EncoderEntry, bool) {
	e, ok := r.encoders[name]
	return e, ok
}

// Decoder looks up a registered decoder.
func (r *Registry) Decoder(name string) (DecoderEntry, bool) {
	e, ok := r.decoders[name]
	return e, ok
}

// Framer looks up a registered framer.
func (r *Registry) Framer(name string) (FramerEntry, bool) { e, ok := r.framers[name]; return e, ok }

// Deframer looks up a registered deframer.
func (r *Registry) Deframer(name string) (DeframerEntry, bool) {
	e, ok := r.deframers[name]
	return e, ok
}

// Compressor looks up a registered compressor.
func (r *Registry) Compressor(name string) (CompressorEntry, bool) {
	e, ok := r.compressors[name]
	return e, ok
}

// Has reports whether a component of the given kind and name is registered. Graph validation asks
// this rather than switching on a name, which is why adding a node kind is not a contract change.
func (r *Registry) Has(k Kind, name string) bool {
	_, ok := r.Descriptor(k, name)
	return ok
}

// Descriptor returns the instantiation-free projection for one component.
func (r *Registry) Descriptor(k Kind, name string) (Descriptor, bool) {
	switch k {
	case KindSource:
		if e, ok := r.sources[name]; ok {
			return e.Descriptor, true
		}
	case KindSink:
		if e, ok := r.sinks[name]; ok {
			return e.Descriptor, true
		}
	case KindTransform:
		if e, ok := r.transforms[name]; ok {
			return e.Descriptor, true
		}
	case KindBuffer:
		if e, ok := r.buffers[name]; ok {
			return e.Descriptor, true
		}
	case KindEncoder:
		if e, ok := r.encoders[name]; ok {
			return e.Descriptor, true
		}
	case KindDecoder:
		if e, ok := r.decoders[name]; ok {
			return e.Descriptor, true
		}
	case KindFramer:
		if e, ok := r.framers[name]; ok {
			return e.Descriptor, true
		}
	case KindDeframer:
		if e, ok := r.deframers[name]; ok {
			return e.Descriptor, true
		}
	case KindCompressor:
		if e, ok := r.compressors[name]; ok {
			return e.Descriptor, true
		}
	}
	return Descriptor{}, false
}

// Descriptors returns every component's projection, ordered by kind then name so that a rendered
// list and a golden-file fixture cannot disagree.
func (r *Registry) Descriptors() []Descriptor {
	var out []Descriptor
	for _, e := range r.sources {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.sinks {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.transforms {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.buffers {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.encoders {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.decoders {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.framers {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.deframers {
		out = append(out, e.Descriptor)
	}
	for _, e := range r.compressors {
		out = append(out, e.Descriptor)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Names returns the registered names of one kind, sorted.
func (r *Registry) Names(k Kind) []string {
	var out []string
	for _, d := range r.Descriptors() {
		if d.Kind == k {
			out = append(out, d.Name)
		}
	}
	return out
}

// Clone returns an independent copy of r. Nothing in the copy aliases the original's maps, so a
// test that registers a fake cannot leak into another test.
func (r *Registry) Clone() *Registry {
	c := New()
	for k, v := range r.sources {
		c.sources[k] = v
	}
	for k, v := range r.sinks {
		c.sinks[k] = v
	}
	for k, v := range r.transforms {
		c.transforms[k] = v
	}
	for k, v := range r.buffers {
		c.buffers[k] = v
	}
	for k, v := range r.encoders {
		c.encoders[k] = v
	}
	for k, v := range r.decoders {
		c.decoders[k] = v
	}
	for k, v := range r.framers {
		c.framers[k] = v
	}
	for k, v := range r.deframers {
		c.deframers[k] = v
	}
	for k, v := range r.compressors {
		c.compressors[k] = v
	}
	return c
}

// With returns a registry containing everything in r plus everything in other, with other winning
// a name collision. It does not modify either argument.
func (r *Registry) With(other *Registry) *Registry {
	c := r.Clone()
	if other == nil {
		return c
	}
	for k, v := range other.sources {
		c.sources[k] = v
	}
	for k, v := range other.sinks {
		c.sinks[k] = v
	}
	for k, v := range other.transforms {
		c.transforms[k] = v
	}
	for k, v := range other.buffers {
		c.buffers[k] = v
	}
	for k, v := range other.encoders {
		c.encoders[k] = v
	}
	for k, v := range other.decoders {
		c.decoders[k] = v
	}
	for k, v := range other.framers {
		c.framers[k] = v
	}
	for k, v := range other.deframers {
		c.deframers[k] = v
	}
	for k, v := range other.compressors {
		c.compressors[k] = v
	}
	return c
}

// Without returns a registry with the named components of one kind removed — for a deployment that
// must not offer, say, a shell-exec sink. It does not modify r.
func (r *Registry) Without(k Kind, names ...string) *Registry {
	c := r.Clone()
	for _, n := range names {
		switch k {
		case KindSource:
			delete(c.sources, n)
		case KindSink:
			delete(c.sinks, n)
		case KindTransform:
			delete(c.transforms, n)
		case KindBuffer:
			delete(c.buffers, n)
		case KindEncoder:
			delete(c.encoders, n)
		case KindDecoder:
			delete(c.decoders, n)
		case KindFramer:
			delete(c.framers, n)
		case KindDeframer:
			delete(c.deframers, n)
		case KindCompressor:
			delete(c.compressors, n)
		}
	}
	return c
}
