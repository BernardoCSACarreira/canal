package fault

// Class is the OWNERSHIP taxonomy: whose problem is this? It is the question an
// operator UI actually needs answered — my config, their system, or a blip — and
// only the connector can answer it, so the connector classifies at the point of
// raise.
//
// Class names a FACT. It does not name a behaviour. Retryability, termination and
// dead-lettering are computed by the engine from (class, component capabilities,
// configured policy). Fusing fact and behaviour into one enum is why a timed-out
// write had no representable class in the designs this one replaces.
type Class uint8

const (
	// Unclassified is the zero value and is always a defect. The engine treats it as
	// PermanentInternal, increments canal_unclassified_faults_total, and the
	// conformance kit asserts that counter stays at zero for a compliant connector.
	Unclassified Class = iota

	// TransientUpstream means the remote system is temporarily unable AND the effect
	// definitely did not land: 503, connection refused, throttle, reset before send.
	//
	// See the retry-safety obligation in retry.go. A connector may return this ONLY
	// when it knows the effect did not land.
	TransientUpstream

	// TransientInternal means canal itself is temporarily unable: buffer full, lease
	// contention, local disk pressure, store unreachable.
	TransientInternal

	// Throttled means the remote is RATE-LIMITING and has told us to come back: a 429, a
	// 503 with Retry-After, an SDK's explicit throttle signal. The effect definitely did
	// not land, exactly as with TransientUpstream.
	//
	// It is a separate member because it is FLOW CONTROL, not failure, and the difference
	// is whether it spends a retry attempt. Classified as TransientUpstream, a source
	// politely honouring a 429 for 90 seconds burned its four attempts on an upstream that
	// was working perfectly and reached TerminalStop — and RetryPolicy has no unbounded
	// option by design, so there was no configuration that made honouring a rate limit
	// safe.
	//
	// The engine's response is: wait Fault.RetryAfter (or the backoff schedule when it is
	// absent), DO NOT increment the attempt counter, and bound the total wait by
	// RetryPolicy.Backoff.MaxElapsed alone. Sustained throttling therefore surfaces as
	// ReasonSustainedBackoff on a running pipeline instead of as a dead one, which is the
	// honest description of an upstream asking us to slow down.
	//
	// It sits alongside NotConnected as the second uncounted class, and for the same
	// reason: both are the remote saying "not now", neither is the record's fault.
	Throttled

	// Indeterminate means the operation MAY have been applied and the connector
	// cannot tell. A write that timed out after the request was sent is the canonical
	// case, and before this member existed every sink had to lie: claiming
	// TransientUpstream violates the retry-safety obligation, and claiming
	// PermanentUpstream stops a healthy pipeline.
	//
	// The engine's response is computed, never guessed:
	//
	//	sink declares Idempotent          -> retry (a duplicate is absorbed)
	//	else RetryPolicy.OnIndeterminate  -> Retry | DeadLetter | Stall (default Stall)
	//
	// Stall means: settle nothing, block the lane, raise a degraded condition naming
	// the record. Failing loud on an ambiguous write is the correct default for a
	// data-movement tool.
	Indeterminate

	// PermanentUpstream means the remote system refuses and will keep refusing: 401,
	// 403, table dropped, quota exhausted, credential revoked.
	PermanentUpstream

	// PermanentInternal means canal has a bug or an impossible internal state.
	// Always terminal, always logged with a stack, never dead-lettered as if it were
	// the record's fault.
	PermanentInternal

	// PermanentMapping means THIS RECORD cannot be represented for this destination:
	// unencodable value, missing required field, type mismatch, oversized field.
	// Dead-letter the record; the pipeline is healthy.
	PermanentMapping

	// PermanentContract means the two ends disagree structurally: an unrecognised
	// Blob version, an unsupported schema change, a capability mismatch discovered at
	// runtime, a sink whose reported counts do not add up. Stop the pipeline:
	// retrying cannot help and continuing risks corruption.
	PermanentContract

	// DuplicateIdempotent means the write was refused because it already landed. This
	// is a SUCCESS for settlement and is counted separately so "we are re-delivering a
	// lot" is visible.
	DuplicateIdempotent

	// ClockSkew means a timestamp is implausible relative to the local clock.
	// Behaviour is configured (clamp, reject or pass), never silently chosen.
	ClockSkew

	// Fenced means this worker no longer holds the lease for the lane it tried to
	// write progress for. It revokes the LANE, not the pipeline: the other lanes this
	// worker validly holds keep running.
	Fenced

	// NotConnected means the component lost its connection and needs Open called
	// again before any further call. Control flow, not failure.
	NotConnected

	// EndOfInput means this component has nothing more to produce, ever. Control
	// flow, not failure; a bounded pipeline terminates cleanly this way.
	EndOfInput
)

var classNames = [...]string{
	Unclassified:        "unclassified",
	TransientUpstream:   "transient_upstream",
	TransientInternal:   "transient_internal",
	Throttled:           "throttled",
	Indeterminate:       "indeterminate",
	PermanentUpstream:   "permanent_upstream",
	PermanentInternal:   "permanent_internal",
	PermanentMapping:    "permanent_mapping",
	PermanentContract:   "permanent_contract",
	DuplicateIdempotent: "duplicate_idempotent",
	ClockSkew:           "clock_skew",
	Fenced:              "fenced",
	NotConnected:        "not_connected",
	EndOfInput:          "end_of_input",
}

// String returns the stable snake_case token for c. It is simultaneously the wire
// form, the metric label value and the i18n key suffix (design rule R9).
func (c Class) String() string {
	if int(c) < len(classNames) {
		return classNames[c]
	}
	return "unclassified"
}

// Blames reports which side owns the problem, for the UI's grouping. Closed
// three-value answer: "your config", "their system", "canal".
func (c Class) Blames() Blame {
	switch c {
	case TransientUpstream, PermanentUpstream, DuplicateIdempotent, Throttled:
		return BlameUpstream
	case PermanentMapping, PermanentContract, ClockSkew:
		return BlameConfig
	case TransientInternal, PermanentInternal, Fenced, Unclassified:
		return BlameCanal
	case Indeterminate, NotConnected, EndOfInput:
		// Indeterminate and NotConnected are observations about the remote;
		// EndOfInput is not a failure at all and is never rendered as blame.
		return BlameUpstream
	default:
		return BlameUnknown
	}
}

// Terminal reports whether retrying c can ever help. It is a pure function of the
// class, which is why behaviour is computed by the engine rather than declared by
// the connector.
func (c Class) Terminal() bool {
	switch c {
	case PermanentUpstream, PermanentInternal, PermanentMapping, PermanentContract:
		return true
	default:
		return false
	}
}

// Counted reports whether a fault of this class SPENDS a RetryPolicy attempt.
//
// The three uncounted classes are the ones that are flow control rather than failure:
// [Throttled] is the remote asking us to wait, [NotConnected] is the component asking to
// be reopened, [EndOfInput] is not a failure at all. Everything else is counted, so a
// poison record still reaches a terminal disposition in MaxAttempts and "retry forever"
// stays inexpressible.
//
// It is a method on the class, next to [Class.Terminal], because "does this consume an
// attempt" is the same kind of computed fact as "can retrying help", and leaving it to
// engine prose is what made the answer for a Retry-After-bearing read fault
// undiscoverable from the interface. An uncounted wait is bounded by
// RetryPolicy.Backoff.MaxElapsed and by nothing else.
func (c Class) Counted() bool {
	switch c {
	case Throttled, NotConnected, EndOfInput:
		return false
	default:
		return true
	}
}

// Blame is the closed three-value ownership answer a UI groups faults by.
type Blame uint8

const (
	BlameUnknown Blame = iota
	BlameConfig
	BlameUpstream
	BlameCanal
)

var blameNames = [...]string{
	BlameUnknown:  "unknown",
	BlameConfig:   "config",
	BlameUpstream: "upstream",
	BlameCanal:    "canal",
}

// String returns the stable snake_case token for b.
func (b Blame) String() string {
	if int(b) < len(blameNames) {
		return blameNames[b]
	}
	return "unknown"
}
