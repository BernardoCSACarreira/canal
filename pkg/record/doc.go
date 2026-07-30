// Package record defines canal's canonical in-flight record model. It is the
// spine of the system: every other package agrees on these types and nothing
// else.
//
// Design rule R2 exists because an earlier attempt named a stage
// source_canonical_event_serializer and never defined the canonical form, so an
// HTTP DTO became the internal type by default. Here the envelope is decided
// first, is independent of every transport, and imports only the standard library
// and [github.com/BernardoCSACarreira/canal/pkg/schema].
//
// Growth discipline: new capabilities are added as FIELDS ON CORE STRUCTS, never
// as methods on connector interfaces. A struct field is a source-compatible
// addition; an interface method is not. This one rule is what lets canal reach v3
// without breaking a v1 connector.
//
// The envelope is deliberately not generic. A type parameter on [Record] would
// propagate to Source, Sink, Buffer, Transform, Codec and the registry, and would
// then have to be erased at the registry boundary — which buys nothing. Flink's
// FLIP-191 needed a whole new package because a plugin interface had type
// parameters.
//
// The four divergences from every surveyed system, all of them deliberate:
//
//  1. Stable framework-assigned in-flight identity ([RecordID]), because
//     positional identity within a batch is deprecated by its own author in the
//     one system that shipped it.
//  2. Provenance is unforgeable by construction, because the only constructor
//     stamps it ([Allocator], reachable only through [Batch]).
//  3. Conversion is never implicit. [Payload] holds no codec.
//  4. An unexported reference count on [Origin], which makes fan-out, filtering,
//     expansion and regrouping correct with zero core code paths per topology and
//     makes early settlement structurally impossible.
package record
