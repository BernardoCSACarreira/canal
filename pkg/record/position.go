package record

import (
	"bytes"
	"time"
)

// Position is a source's own notion of "where am I", in the only form the core
// will hold: opaque bytes it never parses, plus core-readable scalars and two
// optional facets that let the core reason about ordering WITHOUT interpreting the
// bytes.
//
// This is the type that makes an acknowledgement-based design observable, which is
// the property the surveyed ack-graph systems lack.
type Position struct {
	// Seq is assigned by the CORE, monotonically increasing within a lane, at
	// admission. The connector must not set it; the core overwrites it. Because it
	// is core-assigned, the core computes records read, records committed,
	// in-flight depth and replay-window size for any source whatsoever without
	// understanding Token.
	//
	// It is generation-local and never persisted. Deriving order from a
	// connector's opaque bytes is exactly the mistake this field exists to avoid.
	Seq uint64 `json:"seq"`

	// Token is the connector's resume payload: a binlog coordinate, an LSN, a
	// resume token, a key-range boundary, a file+byte offset, a page cursor. The
	// core never parses, compares or truncates it. It is written to and read from
	// durable storage verbatim.
	//
	// INVARIANT THE CONNECTOR UPHOLDS: given Token, the source can resume such
	// that no record at or before this position is skipped. Duplicates are
	// permitted; gaps are not.
	Token Blob `json:"token"`

	// Order, when non-nil, is an order-preserving encoding of this position: for
	// any two positions a and b from the same connector for the same lane,
	// bytes.Compare(a.Order, b.Order) has the same sign as the connector's own
	// notion of a-before-b.
	//
	// Comparability is optional DATA, not an optional METHOD. An isBefore/isAfter
	// method works in-process and cannot cross a process boundary — the exact trap
	// that forces a capability to be re-declared as a bool at every wire boundary.
	// Order crosses a wire, reaches a browser, and is comparable by a generic core
	// with bytes.Compare.
	//
	// Supplying Order unlocks: mid-lane monotonicity assertions, min/max over
	// watermarks, position-fraction progress (with Scalar), and the key-range
	// filter a future concurrent-snapshot engine needs. Not supplying it costs
	// only those things.
	//
	// Order's encoding is part of Token.Version's contract. Changing it changes the
	// total order and therefore invalidates persisted ranges, so it MUST bump
	// Token.Version.
	Order []byte `json:"order,omitempty"`

	// Scalar, when non-nil, is a monotone numeric projection used ONLY for
	// progress arithmetic — (cur-lo)/(hi-lo). It need not be exact, dense or
	// meaningful in any unit; it must be monotone with Order.
	Scalar *float64 `json:"scalar,omitempty"`

	// Label is a short human-readable rendering, authored by the connector:
	// "binlog.000042:1273", "lsn 0/1A2B3C4", "id > 'acme-991'".
	//
	// The core renders it and NEVER parses it. This is how the frontend shows a
	// meaningful position for an arbitrary connector with zero connector-specific
	// UI code: the connector supplies the string, the UI supplies the box.
	Label string `json:"label,omitempty"`

	// Safe reports whether resuming from Token is gap-free. A source that emits
	// records mid-transaction sets Safe=false on those positions and Safe=true only
	// at a transaction boundary, because resuming mid-transaction can skip a
	// partial commit.
	//
	// The ledger resolves the contiguous prefix over ALL positions but only ever
	// commits a position with Safe=true. "The committed position is the last safe
	// point at or before the resolved prefix" is therefore a CORE INVARIANT rather
	// than a per-connector convention. A connector with no such distinction sets
	// Safe:true everywhere and pays nothing.
	Safe bool `json:"safe"`

	// At is when the source observed this position. Zero means unknown, and the
	// core reports unknown rather than zero.
	At time.Time `json:"at,omitempty"`
}

// IsZero reports whether p names no position at all — the cold-start value.
func (p Position) IsZero() bool { return p.Seq == 0 && p.Token.IsZero() }

// Comparable reports whether p carries an order-preserving encoding.
func (p Position) Comparable() bool { return p.Order != nil }

// Compare returns -1, 0 or +1 and true when both positions carry Order. It returns
// (0, false) otherwise, and EVERY core call site handles false by degrading rather
// than by guessing. That discipline is what keeps a non-comparable source a
// first-class citizen.
func (p Position) Compare(q Position) (int, bool) {
	if p.Order == nil || q.Order == nil {
		return 0, false
	}
	return bytes.Compare(p.Order, q.Order), true
}

// Fraction returns o's position within [lo, hi] in [0,1], and true only when all
// three carry Scalar and hi.Scalar > lo.Scalar.
//
// On false the caller OMITS the metric series entirely rather than emitting zero:
// an unmeasurable quantity reported as 0 is a lie the UI cannot detect.
func Fraction(lo, o, hi Position) (float64, bool) {
	if lo.Scalar == nil || o.Scalar == nil || hi.Scalar == nil {
		return 0, false
	}
	l, c, h := *lo.Scalar, *o.Scalar, *hi.Scalar
	if !(h > l) {
		return 0, false
	}
	f := (c - l) / (h - l)
	switch {
	case f < 0:
		return 0, true
	case f > 1:
		return 1, true
	default:
		return f, true
	}
}
