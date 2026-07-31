// Package codec holds the codecs canal ships with.
//
// These are the first registered components in the tree that are neither a source nor a sink, and
// they exist because nothing bridged the two: [internal/example/linefile] produces records and
// [internal/example/stdoutsink] consumes Request.Body, so until now no configuration of the shipped
// components could move a record from one to the other.
//
// # Why an encoder and a framer rather than one "ndjson"
//
// ndjson is a JSON encoder plus a newline framer, and the registry models those as two kinds for a
// reason: json+newline is ndjson, json+length-prefix is something else, and raw+newline is a plain
// log tail. Shipping a single monolithic "ndjson" component would collapse that distinction in the
// very first codec written against the interface, and the first example is the one everybody copies.
//
// So: two encoders and one framer, none of which knows about the others.
//
//	raw     + newline   a byte stream, one payload per line
//	json    + newline   ndjson
//
// # Registration
//
// Importing this package registers all three into [registry.Default]. That is the same init-time
// pattern a third-party codec uses, and nothing here is privileged: these are ordinary components
// that happen to live in the same module.
package codec

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

func init() {
	Register(registry.Default)
}

// Register adds the shipped codecs to r.
//
// Exported so a test, or a binary that wants an explicitly-built registry rather than the default
// one, can register them without an import side effect.
func Register(r *registry.Registry) {
	registerRaw(r)
	registerJSON(r)
	registerNewline(r)
}

// --- raw ----------------------------------------------------------------------

func registerRaw(r *registry.Registry) {
	registry.AddEncoder(r, registry.EncoderDef[*rawEncoder]{
		Meta: registry.Meta{
			Name: "raw", Version: "1.0.0", Title: "Raw bytes",
			Summary: "Passes a record's byte payload through unchanged.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.CodecCaps{
			Caps: connector.Caps{APIVersion: connector.APIVersion},
			// Bytes in, bytes out. It cannot express anything else, and says so rather than
			// silently stringifying a structured record.
			Accepts:  []record.Kind{record.KindBytes},
			Produces: []record.Kind{record.KindBytes},
		},
		New: func(context.Context, *config.Config) (*rawEncoder, error) { return &rawEncoder{}, nil },
	})
}

// rawEncoder passes a byte payload through untouched.
//
// It is the identity encoder, and it is what makes a log-tail pipeline expressible: a source that
// already produces bytes should not have to pretend to be structured to reach a byte sink.
type rawEncoder struct{}

func (*rawEncoder) Open(context.Context, connector.CodecRuntime) error { return nil }
func (*rawEncoder) ContentType() string                                { return "application/octet-stream" }
func (*rawEncoder) Close(context.Context) error                        { return nil }

func (*rawEncoder) Encode(_ context.Context, dst []byte, r *record.Record) ([]byte, error) {
	b, ok := r.Payload.Bytes()
	if !ok {
		// A structured-only payload has no byte view, and there is no honest thing to write. The
		// alternative — reaching for fmt on a Value — would emit Go syntax into a data pipeline.
		return dst, errNoBytes
	}
	return append(dst, b...), nil
}

// --- json ---------------------------------------------------------------------

func registerJSON(r *registry.Registry) {
	registry.AddEncoder(r, registry.EncoderDef[*jsonEncoder]{
		Meta: registry.Meta{
			Name: "json", Version: "1.0.0", Title: "JSON",
			Summary: "Encodes a record's structured payload as one JSON value.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.CodecCaps{
			Caps: connector.Caps{APIVersion: connector.APIVersion},
			// Every kind is accepted, but two are lossy on the way out and the caps say which:
			// a Decimal becomes a string to keep its precision, and Bytes becomes base64. A codec
			// that cannot round-trip a kind declares it here rather than losing it quietly.
			Accepts: []record.Kind{
				record.KindNull, record.KindBool, record.KindInt, record.KindUint,
				record.KindFloat, record.KindString, record.KindBytes, record.KindTime,
				record.KindDecimal, record.KindList, record.KindMap,
			},
			Produces: []record.Kind{record.KindBytes},
		},
		New: func(context.Context, *config.Config) (*jsonEncoder, error) { return &jsonEncoder{}, nil },
	})
}

// jsonEncoder writes a record's structured payload as one JSON value.
//
// It is NOT self-delimiting: a bare JSON value is not a line, and pairing it with a framer is the
// caller's choice. json+newline is ndjson.
type jsonEncoder struct{}

func (*jsonEncoder) Open(context.Context, connector.CodecRuntime) error { return nil }
func (*jsonEncoder) ContentType() string                                { return "application/json" }
func (*jsonEncoder) Close(context.Context) error                        { return nil }

func (*jsonEncoder) Encode(_ context.Context, dst []byte, r *record.Record) ([]byte, error) {
	v, ok := r.Payload.Structured()
	if !ok {
		// Passing raw bytes through a JSON encoder would emit whatever they are and call it JSON,
		// producing a stream that is invalid the first time a line is not already JSON. Use the raw
		// encoder for a byte payload; that is what it is for.
		return dst, errNoStructure
	}
	return appendValue(dst, v)
}

// --- newline --------------------------------------------------------------------

func registerNewline(r *registry.Registry) {
	registry.AddFramer(r, registry.FramerDef[*newlineFramer]{
		Meta: registry.Meta{
			Name: "newline", Version: "1.0.0", Title: "Newline delimited",
			Summary: "Terminates every payload with a single \\n.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.FramerCaps{
			Caps: connector.Caps{APIVersion: connector.APIVersion},
			// No per-request suffix: every payload carries its own delimiter, so a request is
			// complete after its last frame and a truncated request is still parseable up to the
			// last newline.
			Terminated: false,
		},
		New: func(context.Context, *config.Config) (*newlineFramer, error) { return &newlineFramer{}, nil },
	})
}

// newlineFramer terminates each payload with \n.
//
// Terminates rather than separates. A separator would leave the last record without one, so a reader
// could not tell a complete final record from a truncated one — which is the whole reason ndjson is
// terminator-delimited.
type newlineFramer struct{}

func (*newlineFramer) Frame(dst []byte, payload []byte) ([]byte, error) {
	dst = append(dst, payload...)
	return append(dst, '\n'), nil
}

// Terminator is nil: the delimiter is per-payload, not per-request.
func (*newlineFramer) Terminator() []byte { return nil }
