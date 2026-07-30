package fault

import (
	"errors"
	"strings"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Fault is canal's error type. Connectors construct it; the engine reads it.
//
// The two message fields are deliberately separate: the operator sees User, the log
// carries Dev, and neither is a truncation of the other. One string cannot serve
// both audiences.
//
// The wire form of a fault is exactly its declared fields. Err is a local-only
// convenience and its wrap chain is NOT preserved across a process boundary — and
// the in-process path pays the same fidelity loss deliberately, because if only the
// remote path lost fidelity the two transports would diverge semantically.
type Fault struct {
	Class Class
	Op    Op

	Stream record.StreamName
	Lane   record.LaneID
	Node   record.NodeID

	// Record is zero when the fault is not record-scoped.
	Record record.RecordID

	// RetryAfter is honoured verbatim by the engine's backoff when non-zero. This is
	// where a Retry-After header lands, and honouring it at EVERY call site rather
	// than only on connect is the fix for the ignored-hint bug.
	RetryAfter time.Duration

	// User is shown to an operator. No stack traces, no Go types, no internal
	// identifiers. It should name the count, the component, an example, the fix and
	// the consequence.
	User string

	// Dev is for the log and the developer. Anything goes.
	Dev string

	// Err is the wrapped cause. Local-only: see the type comment.
	Err error
}

// Error implements error. It renders the class, the op and whichever message fields
// are populated, and never the stack.
func (f *Fault) Error() string {
	var b strings.Builder
	b.WriteString(f.Class.String())
	b.WriteByte('/')
	b.WriteString(f.Op.String())
	switch {
	case f.User != "":
		b.WriteString(": ")
		b.WriteString(f.User)
	case f.Dev != "":
		b.WriteString(": ")
		b.WriteString(f.Dev)
	}
	if f.Err != nil {
		b.WriteString(": ")
		b.WriteString(f.Err.Error())
	}
	return b.String()
}

// Unwrap exposes the wrapped cause to errors.Is and errors.As.
func (f *Fault) Unwrap() error { return f.Err }

// Is makes errors.Is work against the package sentinels for any
// connector-constructed fault: a *Fault matches a target *Fault when their Classes
// are equal and the target carries no other distinguishing field.
//
// Without this, errors.Is(err, fault.ErrNotConnected) silently fails for every
// connector that constructed its own NotConnected fault, which is a control-flow bug
// that looks like a data bug.
func (f *Fault) Is(target error) bool {
	t, ok := target.(*Fault)
	if !ok {
		return false
	}
	if t.Class != f.Class {
		return false
	}
	// A target carrying distinguishing detail is asking for an exact match on that
	// detail, not for a class-level match.
	if t.Op != OpUnknown && t.Op != f.Op {
		return false
	}
	if t.Lane != "" && t.Lane != f.Lane {
		return false
	}
	if t.Node != "" && t.Node != f.Node {
		return false
	}
	if t.Record != 0 && t.Record != f.Record {
		return false
	}
	return true
}

// New constructs a Fault. Connector authors use this or a Class-named helper rather
// than fmt.Errorf, and the conformance kit fails a connector whose returned errors do
// not classify.
func New(c Class, op Op, err error) *Fault {
	return &Fault{Class: c, Op: op, Err: err}
}

// Transient reports a failure the connector KNOWS did not land. See the
// retry-safety obligation.
func Transient(op Op, err error) *Fault { return New(TransientUpstream, op, err) }

// Throttle reports that the remote is rate-limiting and asked us to wait. after is the
// upstream's own hint — a Retry-After header, an SDK's backoff advice — and zero means the
// connector has none, in which case the engine's backoff schedule applies.
//
// It does NOT spend a retry attempt; see [Throttled] and [Class.Counted]. It is a named
// constructor rather than a field on Transient so that the one classification decision a
// rate-limited connector must get right is the one the constructor name asks about.
func Throttle(op Op, after time.Duration, err error) *Fault {
	f := New(Throttled, op, err)
	f.RetryAfter = after
	return f
}

// Internal reports that canal itself is temporarily unable.
func Internal(op Op, err error) *Fault { return New(TransientInternal, op, err) }

// Unknown reports that the operation MAY have been applied and the connector cannot
// tell. It is named Unknown rather than Indeterminate at the constructor because
// that is the sentence a connector author is thinking: "I do not know".
func Unknown(op Op, err error) *Fault { return New(Indeterminate, op, err) }

// Permanent reports that the remote refuses and will keep refusing.
func Permanent(op Op, err error) *Fault { return New(PermanentUpstream, op, err) }

// Mapping reports that THIS RECORD cannot be represented for this destination.
func Mapping(op Op, err error) *Fault { return New(PermanentMapping, op, err) }

// Contract reports that the two ends disagree structurally. It stops the pipeline.
func Contract(op Op, err error) *Fault { return New(PermanentContract, op, err) }

// Duplicate reports that the write was refused because it already landed. It is a
// SUCCESS for settlement.
func Duplicate(op Op, err error) *Fault { return New(DuplicateIdempotent, op, err) }

// Bug reports an impossible internal state. Always terminal, never dead-lettered as
// if it were the record's fault.
func Bug(op Op, err error) *Fault { return New(PermanentInternal, op, err) }

// ClassOf walks the error chain with errors.As and returns the OUTERMOST declared
// class, or [Unclassified].
//
// Outermost, not innermost: a wrapper that deliberately re-classifies — the engine's
// own "this transient read error has now failed four times, treat it as permanent" —
// must win over the original. Returning the innermost class makes deliberate
// re-classification by wrapping impossible.
func ClassOf(err error) Class {
	var f *Fault
	if errors.As(err, &f) {
		return f.Class
	}
	return Unclassified
}

// RetryAfterOf returns the honoured backoff hint from the outermost fault in the
// chain that carries one, and whether any did. It is a function rather than a field
// read so that a connector wrapping a rate-limit fault does not lose its
// Retry-After.
func RetryAfterOf(err error) (time.Duration, bool) {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if f, ok := e.(*Fault); ok && f.RetryAfter > 0 {
			return f.RetryAfter, true
		}
	}
	return 0, false
}

// The package sentinels. These are *Fault values so errors.Is works through
// connector wrapping, following driver.ErrBadConn's documented contract.
var (
	// ErrNotConnected asks the engine to call Open again, with backoff.
	ErrNotConnected = New(NotConnected, OpUnknown, errNotConnected)

	// ErrEndOfInput reports that this component has nothing more to produce, ever.
	ErrEndOfInput = New(EndOfInput, OpRead, errEndOfInput)

	// ErrFenced reports that this worker no longer holds the lane's lease. It revokes
	// the LANE, not the pipeline.
	ErrFenced = New(Fenced, OpPersist, errFenced)

	// ErrDeclined declines a capability FOR THIS CONFIGURATION, and it is legal only
	// from a method called during the negotiation window: Validate, Discover, and any
	// capability method the engine calls inside Build. Returning it removes the
	// capability from the resolved set BEFORE admission, with the error's message
	// becoming the capability report's Reason.
	//
	// There is deliberately no per-call "skip this optional fast path" sentinel. A
	// silent, per-call, invisible skip is precisely the degradation this design exists
	// to prevent, and a sentinel valid only on a prose allowlist that grows with every
	// optional interface is the ignored-hint bug in a new costume.
	ErrDeclined = New(PermanentContract, OpValidate, errDeclined)
)

type sentinelError string

func (s sentinelError) Error() string { return string(s) }

const (
	errNotConnected sentinelError = "component is not connected"
	errEndOfInput   sentinelError = "no more input"
	errFenced       sentinelError = "lease epoch is stale for this lane"
	errDeclined     sentinelError = "capability declined for this configuration"
)
