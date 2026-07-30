package connector

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Encoder turns one record into bytes. Registered by name; a connector never names one,
// and a codec never names a connector.
//
// Every codec gets Open with a runtime, and every hot method takes a context. Without them
// a schema-registry-backed Avro or protobuf codec — the central justification for
// pluggable codecs existing at all — cannot be written.
type Encoder interface {
	Open(ctx context.Context, rt CodecRuntime) error

	// Encode appends to dst and returns the extended slice, so the engine reuses one buffer
	// per node. The encoder must not retain dst.
	Encode(ctx context.Context, dst []byte, r *record.Record) ([]byte, error)

	// ContentType is what the engine puts on Request.ContentType.
	ContentType() string

	Close(ctx context.Context) error
}

// Decoder turns one frame into zero or more records.
//
// One frame to many records is in the signature, which correctly handles a JSON array in
// one frame, a multi-record log message, or a multiline log entry.
//
// It appends through dst.Derive(carrier), where carrier is the record that carried the
// frame, so settlement accounting for a one-to-N deframing is the same mechanism as for a
// one-to-N transform and needs no special case.
type Decoder interface {
	Open(ctx context.Context, rt CodecRuntime) error
	Decode(ctx context.Context, frame []byte, carrier *record.Record, dst *record.Batch) error
	Close(ctx context.Context) error
}

// Framer delimits an encoded payload.
type Framer interface {
	// Frame appends one delimited payload to dst and returns the extended slice.
	Frame(dst []byte, payload []byte) ([]byte, error)

	// Terminator is appended once per request, after the last frame. Nil for most framers;
	// needed for, say, a JSON array's closing bracket.
	Terminator() []byte
}

// Deframer splits a byte stream into frames.
//
// The signature is deliberately bufio.SplitFunc's, so every existing Go splitter is a canal
// deframer and no author learns a new shape.
type Deframer interface {
	Split(data []byte, atEOF bool) (advance int, frame []byte, err error)
}

// Compressor compresses a request body.
type Compressor interface {
	// Compress appends the compressed form of src to dst and returns the extended slice.
	Compress(dst []byte, src []byte) ([]byte, error)

	// ContentEncoding is what the engine puts on Request.ContentEncoding.
	ContentEncoding() string
}

// Decompressor is the inverse of [Compressor], used on the source side.
type Decompressor interface {
	Decompress(dst []byte, src []byte) ([]byte, error)
}

// CodecCaps declares what a codec can carry, so an impossible pairing is refused at submit
// time rather than discovered on record one.
//
// Every registered component kind has a Caps struct with an APIVersion — not only sources
// and sinks. A kind with no capability struct is a kind whose contract cannot be versioned.
type CodecCaps struct {
	Caps

	// Accepts and Produces are the value kinds this codec can round-trip. A codec that
	// cannot express the nil-versus-Null distinction says so here rather than losing the
	// distinction silently.
	Accepts  []record.Kind `json:"accepts,omitempty"`
	Produces []record.Kind `json:"produces,omitempty"`

	CarriesMeta   bool `json:"carries_meta"`
	CarriesChange bool `json:"carries_change"`
	CarriesSchema bool `json:"carries_schema"`

	// SelfDelimiting means the codec needs no framer, and Build refuses to attach one.
	SelfDelimiting bool `json:"self_delimiting"`

	// NeedsRuntime means the codec uses CodecRuntime, typically for a schema registry.
	NeedsRuntime bool `json:"needs_runtime"`
}

// FramerCaps declares a framer's contract.
type FramerCaps struct {
	Caps
	// Terminated means Terminator returns a non-empty suffix, which the engine must emit
	// exactly once per request.
	Terminated bool `json:"terminated"`
}

// CompressorCaps declares a compressor's contract.
type CompressorCaps struct {
	Caps
	// ContentEncoding is the token this compressor sets, declared as data so the negotiation
	// need not instantiate anything.
	ContentEncoding string `json:"content_encoding"`
}
