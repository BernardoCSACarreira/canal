package record

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Record is the envelope.
//
// Three separately-lifetimed layers plus core-owned provenance: [Payload] is one
// body with two views, [Meta] is a separately addressable namespace, [Change] is an
// optional typed facet, and the unexported origin is stamped once by an
// [Allocator].
type Record struct {
	// origin is stamped by an Allocator. There is no exported mutator, which is
	// what makes settlement identity uncorruptible by a transform.
	origin Origin

	// Dest is the routing target a transform may rewrite. It starts equal to
	// origin.Stream. Two fields for two concepts: one is identity, one is
	// destination. This is the FIX for a conflation another system had to retrofit,
	// not a violation of design rule R9 — they are genuinely different facts.
	Dest StreamName

	EventTime time.Time
	Payload   Payload
	Meta      Meta

	// Change is the optional typed change facet. Nil for a source with no change
	// semantics.
	Change *Change

	// Schema is an optional reference resolved against the pipeline's schema table.
	Schema *schema.Ref

	// handle is the source's own delivery handle for a discrete-ordering lane: a
	// queue receipt handle, a delivery tag, an ack id. Set by the source through
	// SetHandle, carried through derivations, and returned to the source in
	// Ack.Handles. Nil for prefix-ordering lanes.
	handle []byte

	// fault is set by MarkFailed and read by the engine's routing policy. It is a
	// plain error rather than a fault.Fault so that record does not import fault
	// and the dependency direction stays strictly downward.
	fault error
}

// Origin returns the record's immutable provenance.
//
// It returns a COPY. Provenance a transform could rewrite is provenance settlement
// cannot trust, so the settlement fields — Lane, Stream, Group, ID, Root, refs — have
// no mutator at all. The two IDENTITY fields a source alone can know, Key and
// Upstream, are written through [Record.SetKey] and [Record.SetUpstream], which is
// the only reason `o := r.Origin(); o.Key = k` is a mistake nobody needs to make.
func (r *Record) Origin() Origin { return r.origin }

// SetHandle attaches the source's delivery handle.
//
// It is legal only from the source that produced the record, before it returns from
// Read; the engine rejects a handle set later. For an OrderingDiscrete lane it is
// REQUIRED on every record, because the handle is the only progress vocabulary such
// a lane has.
func (r *Record) SetHandle(h []byte) { r.handle = h }

// SetKey attaches [Origin].Key: the source-derived stable identity of the thing this
// record is about, canonically encoded so a sink knowing nothing about the source can
// use it directly as an upsert or dedupe key.
//
// THE SOURCE-ONLY WINDOW, identical to [Record.SetHandle]'s: legal only from the
// source that produced the record, before that source returns from Read or ReadLanes.
// The engine rejects a key set later, and a transform that calls it gets
// fault.PermanentContract — which is why the settlement fields stayed unwritable while
// these two did not.
//
// A source declaring SourceCaps.StableKeys MUST call this on every record and MUST
// document the derivation in its registered Notes; registration lint fails a source
// that declares StableKeys with empty Notes. Everything downstream that rests on
// identity — StableKeys, SinkCaps.RequiresKey, both dedupe layers,
// Request.IdempotencyKey, Ref.Key, EffectivelyOnce — reaches the destination through
// this one method and through no other.
func (r *Record) SetKey(k []byte) { r.origin.Key = k }

// SetUpstream attaches [Origin].Upstream: the vendor's own id for this record, carried
// verbatim, which is idempotency layer one. Same source-only window as [Record.SetKey].
//
// Two methods rather than one SetIdentity(key, upstream), because a source that has a
// vendor id and no derivable canonical key — and one with the reverse — must not be
// made to pass nil for the half it does not have. A nil argument that means "leave it
// alone" and a nil argument that means "clear it" are indistinguishable, and that
// ambiguity is worth two methods.
func (r *Record) SetUpstream(u []byte) { r.origin.Upstream = u }

// Handle returns the delivery handle, or nil.
func (r *Record) Handle() []byte { return r.handle }

// MarkFailed attaches a fault and lets the record continue. The engine's configured
// routing decides whether that means a Failed edge, a drop, or a pipeline stop.
//
// Carrying the error ON the record is what makes mark-and-route error handling need
// no extra interface vocabulary.
func (r *Record) MarkFailed(err error) { r.fault = err }

// Failed reports the attached fault, if any.
func (r *Record) Failed() (error, bool) { return r.fault, r.fault != nil }

// Ref is the minimum a sink needs to report a per-record outcome: identity and
// enough provenance to build a useful message, and no payload.
type Ref struct {
	ID     RecordID   `json:"id"`
	Group  GroupID    `json:"group"`
	Lane   LaneID     `json:"lane"`
	Stream StreamName `json:"stream"`
	Key    []byte     `json:"key,omitempty"`
}

// Ref returns the identity a sink reports outcomes against. Note that it carries
// Dest rather than origin.Stream, because a sink reports on where it was asked to
// write.
func (r *Record) Ref() Ref {
	return Ref{
		ID:     r.origin.ID,
		Group:  r.origin.Group,
		Lane:   r.origin.Lane,
		Stream: r.Dest,
		Key:    r.origin.Key,
	}
}
