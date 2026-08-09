package record

import "time"

// Origin is a record's immutable provenance. It is stamped once, by an
// [Allocator], inside this package, and there is no exported mutator.
//
// This is a direct response to Kafka Connect's KIP-793 retrofit: its SMTs rewrite
// topic/partition while offset accounting needs the pre-transform coordinates, so
// originalTopic/originalKafkaPartition/originalKafkaOffset had to be bolted on
// plus prose warnings in two javadocs. Here a transform cannot corrupt settlement
// identity because it has no access to the fields settlement uses.
type Origin struct {
	Tenant   TenantID
	Pipeline PipelineID

	// Node is the source node that produced the record. It never changes, even
	// when the record is derived by a transform on another node.
	Node NodeID

	Lane   LaneID
	Stream StreamName
	Group  GroupID
	ID     RecordID

	// Key is the source-derived stable identity of the thing this record is about,
	// canonically encoded so that a sink knowing nothing about the source can use
	// it directly as an upsert or dedupe key. May be nil.
	//
	// A source with no natural upstream id MUST derive a deterministic one from
	// stable fields and document the derivation in its registered Notes (design
	// rule R5). That documentation obligation is asserted at registration: a
	// source declaring StableKeys with empty Notes panics in pkg/registry's
	// cross-check, on the author's own first go test.
	Key []byte

	// Upstream is the vendor's own id for this record, when it has one, carried
	// verbatim for the first of the three idempotency layers. Layer 1 is Upstream,
	// layer 2 is Key, layer 3 is the engine's in-flight submit guard.
	Upstream []byte

	// ReadAt is when the source produced the record.
	ReadAt time.Time

	// Parent is non-zero when this record was derived from exactly one other
	// record. Parents is non-nil when it was merged from several. Both are
	// provenance for the dead-letter route and the tap; neither is used by
	// settlement.
	Parent  RecordID
	Parents []RecordID

	// Root is the RecordID of the original admitted record this one descends from.
	// Preserved through any depth of derivation, and it is what dedupe and the tap
	// correlate on.
	Root RecordID

	// refs is how many group references this record's settlement discharges.
	//
	// A record admitted from a source has refs 1. A 1-to-N expansion produces N
	// records with refs 1 each and adds N-1 references to the group. An N-to-1
	// merge produces one record whose refs is the sum of its parents'.
	//
	// This single unexported field is why fan-out, filtering, expansion and
	// regrouping need no core code path and cannot early-settle a group.
	refs uint32
}

// Refs reports how many group references settling this record discharges.
// Exported for the ledger and for tests; there is deliberately no setter.
func (o Origin) Refs() uint32 { return o.refs }
