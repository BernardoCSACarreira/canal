package record

import "time"

// ChangeVersion is the current version of the [Change] facet's vocabulary. It is
// core-owned and bumped by canal, and it is carried on every Change so a persisted
// record written by an older binary stays readable.
const ChangeVersion uint16 = 1

// Change is the optional typed change-event facet.
//
// It exists because total genericity provably does not hold. Surveyed CDC inputs
// invent DIFFERENT op vocabularies and DIFFERENT position keys and flatten both
// into metadata strings, so every CDC-aware sink ends up special-casing every
// source — the source/sink-agnostic constraint violated by drift rather than by
// design. The omission of an operation type is the documented reason every CDC
// connector in one ecosystem reinvented the envelope incompatibly.
//
// It is OPTIONAL DATA, not a core-mandated shape, so the core still never switches
// on source type and the relational shape is never forced onto a webhook, a metrics
// scrape or a file tail.
type Change struct {
	// Version versions this facet's vocabulary. Core-owned; see [ChangeVersion].
	Version uint16 `json:"version"`

	Op Op `json:"op"`

	// Keys are the field paths forming the record key, in order. [][]string, not
	// []string, so composite and nested keys need no later breaking change.
	Keys [][]string `json:"keys,omitempty"`

	Before *Payload `json:"before,omitempty"`
	After  *Payload `json:"after,omitempty"`

	// BeforeComplete and AfterComplete answer the question a surveyed logical-
	// decoding input cannot: is this image whole? Logical decoding with an
	// unchanged out-of-line value and a replica identity that is not FULL produces
	// a partial after-image, and a sink that upserts it writes nulls over live
	// data.
	//
	// A sink that needs whole images declares RequiresCompleteImages and the
	// pipeline is REFUSED AT SUBMIT TIME against a source that cannot supply them.
	BeforeComplete Completeness `json:"before_complete"`
	AfterComplete  Completeness `json:"after_complete"`

	// TxID groups records from one upstream transaction. Opaque; the core only
	// tests equality, and a batch policy may flush on a change of it.
	TxID       string    `json:"tx_id,omitempty"`
	CommitTime time.Time `json:"commit_time,omitempty"`
}

// Clone returns a Change sharing nothing with c.
func (c *Change) Clone() *Change {
	if c == nil {
		return nil
	}
	d := *c
	if c.Before != nil {
		b := c.Before.Clone()
		d.Before = &b
	}
	if c.After != nil {
		a := c.After.Clone()
		d.After = &a
	}
	if c.Keys != nil {
		d.Keys = make([][]string, len(c.Keys))
		for i := range c.Keys {
			d.Keys[i] = append([]string(nil), c.Keys[i]...)
		}
	}
	return &d
}

// Op is the change operation. Closed set: it is a metric label.
type Op uint8

const (
	// OpUnknown is the zero value. A source emitting a Change must set Op.
	OpUnknown Op = iota
	OpInsert
	OpUpdate
	OpDelete
	OpTruncate

	// OpScanRead marks a record produced by a scan lane rather than an incremental
	// one. It is INFORMATIONAL: no core code branches on it.
	OpScanRead

	// OpUpsert means "the thing now looks like this, and whether it existed before is
	// genuinely unknown".
	//
	// It is not a synonym for OpUpdate and not a weaker OpInsert. A "recently changed"
	// REST feed, a polled table with an updated_at watermark, a document store that
	// reports state and not transitions — none of them can distinguish a first
	// appearance from a later one, and before this member existed all of them had to
	// claim OpUpdate for every record and be wrong on every insert, or claim
	// OpUnknown, which the vocabulary documents as a defect.
	//
	// A sink writing to DestUpsert treats it exactly as it treats OpInsert and
	// OpUpdate. A sink that genuinely needs the distinction — a change-audit table, a
	// CDC re-publisher — can now REFUSE it as fault.PermanentMapping instead of
	// silently recording a fiction.
	OpUpsert
)

var opNames = [...]string{
	OpUnknown:  "unknown",
	OpInsert:   "insert",
	OpUpdate:   "update",
	OpDelete:   "delete",
	OpTruncate: "truncate",
	OpScanRead: "scan_read",
	OpUpsert:   "upsert",
}

// String returns the stable snake_case token for o.
func (o Op) String() string {
	if int(o) < len(opNames) {
		return opNames[o]
	}
	return "unknown"
}

// Completeness states how much of an image is present.
type Completeness uint8

const (
	// CompletenessAbsent means the image was not supplied at all.
	CompletenessAbsent Completeness = iota
	// CompletenessPartial means some fields are present, and an absent field does
	// NOT mean null.
	CompletenessPartial
	// CompletenessComplete means every field of the governing schema is present.
	CompletenessComplete
)

var completenessNames = [...]string{
	CompletenessAbsent:   "absent",
	CompletenessPartial:  "partial",
	CompletenessComplete: "complete",
}

// String returns the stable snake_case token for c.
func (c Completeness) String() string {
	if int(c) < len(completenessNames) {
		return completenessNames[c]
	}
	return "absent"
}
