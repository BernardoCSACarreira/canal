// Package nocursor is a HOSTILE STRESS CONNECTOR. It is not a shipping connector and it
// talks to no real vendor. It exists to answer one question with a compiler rather than
// with prose: can canal's frozen source surface express a source that has NO monotonic
// cursor, NO ordering and NO stable pagination?
//
// THE SOURCE BEING MODELLED
//
//	GET /changed?since=<ts>&limit=<n>&offset=<k>  ->  {"items":[...], "has_more":bool}
//
//	  - items come back in NO defined order;
//	  - the only progress-ish field is updated_at at ONE-SECOND granularity, with ties
//	    everywhere (a busy second holds hundreds of items);
//	  - pagination is offset-based over a LIVE result set, so there is no stable token:
//	    the same item can appear on two pages, and an item can be missed entirely if a
//	    write reorders the underlying set between page k and page k+1;
//	  - it rate-limits, answering 429 with Retry-After.
//
// Progress therefore CANNOT be a scalar. The only sound progress value is a pair:
//
//	(W, S)  where W is a watermark second and S is the set of item ids already
//	        delivered whose updated_at lies in [W-overlap, W]
//
// Resume means "re-query updated_at >= W-overlap and drop anything in S". Duplicates are
// accepted; the seen-set is what keeps ties from turning into gaps.
//
// ============================================================================
// WHAT FITS — recorded first, because the negative findings below are only
// meaningful if the positives are honest.
// ============================================================================
//
//  1. record.Position.Token being an OPAQUE Blob carries (W, S) with no core change at
//     all. A design that had modelled progress as a uint64 offset, an LSN, or any
//     "cursor" scalar would have been unimplementable here on line one. This is the
//     single most important thing the design gets right.
//
//  2. Position.Order and Position.Scalar being OPTIONAL DATA rather than methods is
//     exactly right for this source. Two consecutive cycles can legitimately produce the
//     same W with different S, so my positions are genuinely PARTIALLY ordered; I set
//     Order=nil, declare ComparablePositions:false, and ledger.Flushable degrades to the
//     core-assigned Seq — which still cannot move a cursor backwards. An isBefore()
//     METHOD would have forced me to invent a total order that does not exist.
//
//  3. Position.Safe is the whole reason a multi-page poll cycle is expressible. A cycle
//     is my "transaction": pages 1..k-1 carry Safe:false and no commit point, page k
//     carries the real Safe:true position. ledger.Settle only promotes a Safe position to
//     pendingFlush, so the core will never commit mid-cycle. I did not have to invent
//     anything; the field was already there for exactly the analogous reason.
//
//  4. Core-assigned Position.Seq means the core computes in-flight depth, replay window
//     and read/commit counters for a source whose own progress value is an unordered set.
//     Nothing else in the surveyed field can do that.
//
//  5. Declining Discoverer, BacklogReporter and Nackable by NOT IMPLEMENTING THEM is a
//     real answer, not a shrug. "The minimum viable Discover is not implementing it"
//     holds up under pressure.
//
//  6. fault.Fault.RetryAfter exists as a FIELD on the error, so a 429 hint survives
//     wrapping. See BREAKAGE 3 for the part of this that is unfinished.
//
// ============================================================================
// BREAKAGE 1 — FATAL, COMPILER-PROVEN. A source cannot set Origin.Key.
// ============================================================================
//
// THE EXACT FAILURE. This is not an argument; it is `go build` output, produced from a
// four-line probe package in this same tree:
//
//	internal/stress/probe/probe.go:7:4: r.SetKey undefined
//	    (type *record.Record has no field or method SetKey)
//	internal/stress/probe/probe.go:8:4: r.SetUpstream undefined
//	    (type *record.Record has no field or method SetUpstream)
//	internal/stress/probe/probe.go:11:2: cannot assign to r.Origin().Key
//	    (neither addressable nor a map index expression)
//
// THE BLOCKING SIGNATURES.
//
//	func (b *record.Batch) Add() *record.Record   // no arguments
//	func (r *record.Record) Origin() Origin       // returns a VALUE
//	// and record.Record's only exported mutators are:
//	func (r *record.Record) SetHandle(h []byte)
//	func (r *record.Record) MarkFailed(err error)
//
// record.Origin.Key and record.Origin.Upstream are exported FIELDS of an unexported
// field on Record, in package record, with — as record.go:5-11 says outright — "no
// exported mutator". Add() takes no arguments, so there is no construction-time path
// either. A source therefore CANNOT populate Origin.Key. Ever.
//
// WHY THAT IS WORSE THAN A MISSING SETTER. The one syntactically legal-looking attempt
//
//	o := r.Origin()
//	o.Key = []byte(id)   // compiles. writes to a COPY. silently does nothing.
//
// compiles clean and is a no-op. So the failure mode for a connector author who does not
// read record.go closely is not a compile error — it is a source that declares
// StableKeys, passes its own tests, and ships unkeyed records.
//
// WHAT THE CORE ALREADY PROMISES ON TOP OF THIS UNREACHABLE FIELD:
//
//	connector.SourceCaps.StableKeys   "Origin.Key is populated and stable across re-reads"
//	connector.SinkCaps.RequiresKey    "every record must carry Origin.Key"
//	connector.Request.IdempotencyKey  "present when the source declares StableKeys"
//	record.Ref.Key                    the identity a sink reports outcomes against
//	registry.AddSource                PANICS on StableKeys with empty Notes — it polices
//	                                  the documentation of a derivation no source can perform
//	docs/architecture.md §22.4 item 8 "Set Origin.Key if you can"
//	docs/architecture.md §"three layers" DedupeUpstream / DedupeKey
//	docs/decisions/0002-dedupe-scope-and-store.md
//
// Every one of those is currently unreachable from a connector. EffectivelyOnce requires
// SinkCaps.Idempotent AND SourceCaps.StableKeys, so EffectivelyOnce is unreachable for
// every source in the system, and the whole negotiated-guarantee lattice above
// AtLeastOnce is dead code.
//
// WHY IT IS FATAL SPECIFICALLY FOR THIS SOURCE, not merely unfortunate. Duplicates here
// are not a crash-window artefact — they are the STEADY STATE. The feed returns the same
// item on two pages of one cycle, and every cycle deliberately re-scans an overlap
// window. My entire correctness argument is "emit duplicates freely; the sink absorbs
// them by key". With Origin.Key unsettable I must declare StableKeys:false, which:
//   - caps the negotiated guarantee at AtLeastOnce,
//   - disables engine-owned dedupe (docs/decisions/0002),
//   - strips Request.IdempotencyKey,
//   - and REFUSES the pipeline outright against any sink declaring RequiresKey —
//     i.e. against every upsert destination, which is the only kind of destination this
//     source makes sense against.
//
// So the hostile source is not merely awkward. It is reduced to append-only sinks, and
// it will duplicate into them forever, by design, with no absorption layer.
//
// THE WORKAROUND I REFUSED TO HIDE. Below, fill() writes the item id into
// Meta(NSSource,"item_id"). That is NOT a fix: nothing in the core reads Meta as an
// identity. It does not feed dedupe, it does not become Request.IdempotencyKey, it does
// not satisfy SinkCaps.RequiresKey, and it obliges every sink to grow source-specific
// knowledge of a metadata key — which is constraint #1 violated by drift, the exact
// failure the Change facet's own doc comment describes.
//
// SMALLEST FIX (purely additive; breaks nothing already written). Two methods in package
// record, next to SetHandle, carrying SetHandle's existing "legal only from the producing
// source, before Read returns" contract:
//
//	// SetKey attaches the source-derived stable identity. Legal only from the source
//	// that produced the record, before it returns from Read.
//	func (r *Record) SetKey(k []byte) { r.origin.Key = k }
//
//	// SetUpstream attaches the vendor's own id for this record.
//	func (r *Record) SetUpstream(u []byte) { r.origin.Upstream = u }
//
// Origin stays unforgeable in every way that matters: Tenant, Pipeline, Node, Lane,
// Stream, Group, ID, Root, Parent(s) and refs remain writable only by the Allocator, so
// settlement identity is still uncorruptible by a transform. Key and Upstream are not
// settlement identity — they are source-authored payload-adjacent facts, and they are the
// only two fields on Origin the SOURCE is documented as the author of.
//
// Rejected alternative: Batch.AddKeyed(key, upstream []byte) *Record. It duplicates Add,
// leaves the "identity is stamped, then mutated" question unanswered for transforms, and
// gives a transform no way to key a record it derived from an unkeyed one.
//
// ============================================================================
// BREAKAGE 2 — MAJOR. A source cannot advance a cursor without emitting a record,
//
//	and a zero-record batch permanently wedges the lane.
//
// ============================================================================
//
// A polling source's most common observation is "I looked, and nothing changed up to
// time T". That observation is pure progress: it is safe to commit immediately, because
// there is nothing in flight for it to commit past. This source cannot express it.
//
// THE THREE ROUTES AND WHY EACH IS CLOSED.
//
// (a) An empty batch carrying a Position. record.Batch.Position is a field, so this looks
//
//	available, and record.Batch's own doc blesses the shape: "EndOfLane ... may be set
//	with zero records". It WEDGES THE LANE FOREVER. Traced through the real code:
//
//	  ledger.Admit         refs = sum over b.Records of Refs() = 0, then
//	                       `if refs == 0 { refs = 1 }`      (ledger.go:205-207)
//	                       weight = b.Len() = 0, then
//	                       `if weight == 0 { weight = 1 }`  (ledger.go:209-211)
//	                       tracker.Track(pos, 1, 1) appends a node to the FIFO
//	                       l.groups[g] = &group{refs:1, records:0}
//	                       NOTHING is added to l.byRec, because there are no records
//	  ledger.Settle        every settlement is looked up as `l.byRec[o.Record]`
//	                       (ledger.go:294-297). With no records there is no key, so
//	                       this group can NEVER be released or abandoned.
//	  Tracker.advanceLocked walks the contiguous prefix from the head and stops at the
//	                       first unresolved node. Once the zero-record node reaches the
//	                       head, the prefix NEVER ADVANCES AGAIN.
//
//	Net effect: one empty batch and the lane's cursor is frozen for the lifetime of the
//	process, with the ledger reporting a healthy resolved prefix that simply stops
//	moving. This is also, note, a live defect for the DOCUMENTED zero-record EndOfLane
//	batch — which internal/example/linefile/source.go:158-161 returns today.
//
// (b) Heartbeater. `Heartbeat(ctx, lane, idle) error` — and its doc closes the door
//
//	explicitly: "It never carries a position and cannot advance a cursor". The stated
//	reason is sound for the case it names ("a heartbeat batch carrying a cursor and no
//	records would commit past unsettled records") but it does not hold for a
//	QUIESCENCE claim, where there are no unsettled records to commit past.
//
// (c) StateHandle. Get/Set/SetMany/Delete let me keep my own watermark blob per lane, so
//
//	I could ignore LaneAssignment.Cursor entirely and treat Position.Token as
//	decorative. This WORKS, and it is the workaround a real author would be pushed
//	into, and it is bad enough to be a finding rather than a solution:
//	  - two durable representations of one fact, which is design rule R1's
//	    dual-representation failure reintroduced by the connector because the core left
//	    it no choice;
//	  - the read model renders the CORE's committed Position (stale) while real progress
//	    sits in a blob the UI cannot interpret, so the operator-facing cursor lies;
//	  - my StateHandle.Set is not atomic with the core's phase-2 cursor write, so a
//	    crash between them leaves the two disagreeing.
//
// CONSEQUENCE, SHIPPED IN THIS FILE. I take neither workaround. I advance the watermark
// IN MEMORY during idle cycles and publish it on the next cycle that yields at least one
// record (see closeCycle / Read). Correct, and free while the process lives — but across
// a restart an idle feed resumes from a stale durable watermark and re-scans a widening
// window. For a "recently changed" endpoint whose own horizon is shorter than the gap,
// that re-scan can return nothing, and I cannot then distinguish "caught up" from "the
// window slid past me".
//
// SMALLEST FIX — and it is an ENGINE fix with NO interface change, which is why I prefer
// it. In ledger.Admit, stop coercing a zero-record batch to refs=1/weight=1; admit it as
// a group that is already resolved:
//
//	if b.Len() == 0 {
//	    // A zero-record batch is a position claim, not work. It has nothing in flight to
//	    // commit past, so it resolves at admission. This is also what makes the
//	    // documented zero-record EndOfLane batch terminate a bounded lane.
//	    ... promote b.Position into pendingFlush when Safe, open no group ...
//	}
//
// Breaks no connector: no existing connector can currently emit a zero-record batch
// without wedging, so there is no behaviour to preserve. Fixes the idle-advance case and
// the zero-record-EndOfLane case with one change. If instead the core wants the claim to
// be explicit, the additive form is a `record.Batch.Idle bool` — also non-breaking, since
// nothing sets it today.
//
// ============================================================================
// BREAKAGE 3 — MAJOR. Being rate-limited is not expressible as anything but a fault,
//
//	and Fault.RetryAfter has no defined semantics out of Read.
//
// ============================================================================
//
// 429 + Retry-After is this source's NORMAL OPERATING MODE, not an error. The only
// vocabulary Source.Read has for "hold off for 8 seconds" is fault.Class, and every
// member either blames somebody or is control flow:
//
//	TransientUpstream  Blames() = BlameUpstream, counted as a fault. This is the only
//	                   honest class available (the effect definitely did not land), so
//	                   routine throttling is reported as upstream failure.
//	NotConnected       wrong: the connection is fine, and it costs a full re-Open.
//	EndOfInput         wrong: terminal.
//
// Then the second half: fault.Fault.RetryAfter's doc says it is "honoured verbatim by the
// engine's backoff ... at EVERY call site rather than only on connect", and names the
// Retry-After header as its purpose. But fault.RetryPolicy has, DELIBERATELY, no
// unbounded option — "a policy that never gives up cannot be constructed" — with
// DefaultRetry.MaxAttempts = 4 and Terminal = TerminalStop. Nothing in the interface set
// or in docs/architecture.md §23 says whether a fault returned from Source.Read consumes
// an attempt against that budget. If it does, five consecutive 429s stop the pipeline,
// and this source cannot run against any real SaaS quota.
//
// So the connector author's only safe move is the one this file does NOT take: swallow the
// 429 inside Read, sleep Retry-After locally, and never surface it. That silently defeats
// the one field designed to carry it, and makes the throttle invisible to the core at
// exactly the moment an operator is asking why throughput collapsed.
//
// What I actually do: surface the fault WITH RetryAfter set, ALSO gate my own next
// request locally on s.nextAllowed so the pipeline is safe whichever way the engine
// resolves this, and Note an EventDegraded so a human can see it. Two of those three are
// belt-and-braces I should not have to write.
//
// SMALLEST FIX (prose + engine, zero interface change, breaks nothing): state on
// Source.Read that a returned fault carrying RetryAfter is honoured as a delay before the
// next Read and does NOT consume a RetryPolicy attempt — the remote told us how long to
// wait, so there is nothing to escalate. If the core wants it visible in the taxonomy
// instead, the additive form is a fault.Throttled class with Blames()=BlameUpstream and
// Terminal()=false, exempted from MaxAttempts; that adds an enum member, which is
// additive but does touch a switch in Class.Blames/Terminal.
//
// ============================================================================
// BREAKAGE 4 — MINOR. Backlog cannot say "I know the lag but not the count".
// ============================================================================
//
//	type Backlog struct {
//	    Records uint64; Bytes uint64; Exact bool; AsOf time.Time
//	    EventTimeLag *time.Duration
//	}
//
// EventTimeLag got the nil treatment, with the right justification on the field: "Nil
// when the source has no event time — never zero, which would read as 'caught up'".
// Records and Bytes did not, so 0 means both "caught up" and "no idea".
//
// This source can answer the lag exactly (now - committed watermark) and CANNOT answer
// the count at all: the endpoint reports no total, and counting would mean paginating the
// whole window, which is the rate-limited operation I am trying to conserve. Implementing
// BacklogReporter forces me to publish Records:0 — the precise lie EventTimeLag's comment
// forbids one field up. So I do not implement it, and canal loses the lag I could have
// given it. The capability is all-or-nothing over three independent facts.
//
// SMALLEST FIX: make them pointers, `Records *uint64` / `Bytes *uint64`, while the type
// still has zero implementors anywhere in the tree (verified: only registry/resolve.go
// references the interface). If pointers are unwanted, additive `RecordsKnown bool` /
// `BytesKnown bool` breaks nothing.
//
// ============================================================================
// BREAKAGE 5 — MINOR. record.Op cannot say "changed, and I cannot tell you how".
// ============================================================================
//
// record.Op is {OpUnknown, OpInsert, OpUpdate, OpDelete, OpTruncate, OpScanRead}, and
// OpUnknown is documented as a defect ("A source emitting a Change must set Op"). A
// "recently changed" feed genuinely does not say whether a row is new. I set OpUpdate,
// which is a LIE for every first appearance of an item, and a sink that maps Insert->
// INSERT / Update->UPDATE will do the wrong thing with it. My alternatives are to lie,
// or to drop the Change facet and lose delete semantics with it.
//
// SMALLEST FIX: add `OpUpsert` to the closed set. Additive at the end of the iota block
// plus one entry in opNames; every existing switch keeps compiling, and any sink that
// does not know the member falls through to whatever it does for unrecognised ops.
//
// ============================================================================
// BREAKAGE 6 — MINOR. config.Predicate cannot compare two fields, and its numeric
//
//	operators are inoperative on duration and size fields.
//
// ============================================================================
//
// This connector has exactly one cross-field rule: `overlap >= safety_lag`. If the re-scan
// window is narrower than the safety lag, there is a band of seconds nothing ever reads —
// silent data loss from a plausible-looking config. config.Spec.Lints is documented as
// "declarative cross-field rules, evaluated offline with no I/O", and the whole point of
// declarative is that the browser evaluates it inline with no round trip.
//
// It cannot express the rule. config.Predicate is
//
//	type Predicate struct { Path []string; Op PredOp; Value any; All, Any []Predicate; Not *Predicate }
//
// and Predicate.eval always treats Value as a LITERAL:
//
//	case PredGreaterThan:
//	    a, aok := asFloat(got)        // the value AT Path
//	    b, bok := asFloat(p.Value)    // a literal, never a second path
//	    return aok && bok && a > b
//
// There is no path-to-path comparand, so no relational rule between two operator-supplied
// values is writable: min<=max, low<high, window>=lag, batch.max_bytes<=request cap. Those
// are not exotic; they are most of what cross-field validation IS.
//
// SECOND, SHARPER DEFECT FOUND WHILE TRYING. Even the literal form is broken for the field
// types I need it on. A TypeDuration field's raw config value is the string "5m", and
// asFloat("5m") is strconv.ParseFloat, which fails. So PredGreaterThan/PredLessThan return
// false for every duration and every size field. And config/validate.go:216-221 reads
//
//	if l.Require.Eval(c) { continue }   // else emit the diagnostic
//
// so a `gt` lint on a duration field does not silently never fire — it fires ALWAYS,
// failing every config unconditionally. The package already owns ParseDuration and
// ParseSize; the predicate evaluator simply does not reach for them.
//
// CONSEQUENCE, SHIPPED IN THIS FILE. The rule is enforced in Validator.Validate instead,
// which is tier TWO: it needs a constructed connector and a server round trip, so the form
// cannot show it inline as the operator types. A rule that is pure arithmetic over two
// declared fields has been demoted to an I/O-capable method for want of a comparand.
//
// SMALLEST FIX, both parts additive and breaking nothing:
//   - add `ValuePath []string` to config.Predicate; when non-empty the comparand is
//     resolved from the config instead of Value, and Predicate.Paths() yields it so the
//     registry's existing cross-check still validates it. Nothing sets it today.
//   - in Predicate.eval's numeric cases, fall back to ParseDuration then ParseSize when
//     asFloat fails, comparing in nanoseconds or bytes. That is a strict widening: every
//     comparison that works today keeps working.
//
// ============================================================================
// VERDICT: requires-core-change. Breakage 1 is not survivable — this source's only
// correctness story is key-based absorption of deliberate duplicates, and the field that
// story rests on cannot be written from a connector. Breakage 2 is a cursor that freezes
// on an idle feed unless the author duplicates it into StateHandle. Everything else is
// expressible, and points 1-6 at the top are genuine wins that most of the surveyed field
// does not have.
// ============================================================================
package nocursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// cursorV1 versions this connector's progress encoding. Everything durable is
// (version, bytes); rule 3 of the Blob format contract says never reject a newer version
// unless the encoding genuinely cannot tolerate it, and this one is additive JSON, so a
// newer version is decoded best-effort rather than refused.
const cursorV1 uint32 = 1

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*src]{
		Meta: registry.Meta{
			Name:    "stress_no_cursor_feed",
			Version: "0.1.0",
			Title:   "Unordered changed-items feed (stress)",
			Summary: "Polls a REST 'recently changed' endpoint that has no cursor, no ordering and no stable pagination token.",
			Notes: "Origin.Key is NOT populated, because record.Record exposes no way to set it " +
				"(see BREAKAGE 1). The item id is written to Meta(source,item_id) instead, which " +
				"nothing in the core reads as identity. StableKeys is therefore declared false and " +
				"this source cannot reach EffectivelyOnce or run against a RequiresKey sink.",
			Support: registry.SupportCommunity,
		},

		Spec: config.NewSpec().
			Describe(
				"An unordered, tie-heavy, offset-paginated changed-items feed.",
				"Progress is a watermark second plus the set of item ids already delivered inside the "+
					"overlap window. It is not a scalar and it is not totally ordered.").
			Field(config.Field{
				Name:        "endpoint",
				Type:        config.TypeString,
				Description: "Absolute URL of the changed-items endpoint.",
				Short:       "It must accept since, limit and offset query parameters.",
				Examples:    []any{"https://api.example.com/v1/changed"},
			}).
			Field(config.Field{
				Name:        "auth_token",
				Type:        config.TypeString,
				Description: "Bearer token sent on every request.",
				Secret:      true,
			}).
			Field(config.Field{
				Name:        "stream",
				Type:        config.TypeString,
				Description: "Logical stream name announced for the records this endpoint produces.",
				Default:     "items",
				Optional:    true,
			}).
			Field(config.Field{
				Name:        "id_field",
				Type:        config.TypeString,
				Description: "Field holding the item's stable id. It is the dedupe identity for the whole design.",
				Default:     "id",
				Optional:    true,
			}).
			Field(config.Field{
				Name:        "updated_at_field",
				Type:        config.TypeString,
				Description: "Field holding the coarse change timestamp. One-second granularity is assumed.",
				Default:     "updated_at",
				Optional:    true,
			}).
			Field(config.Field{
				Name:        "page_size",
				Type:        config.TypeInt,
				Description: "Items requested per page.",
				Default:     100,
				Optional:    true,
				Min:         ptr(1.0),
				Max:         ptr(1000.0),
			}).
			Field(config.Field{
				Name:        "poll_interval",
				Type:        config.TypeDuration,
				Description: "Minimum wait between the start of one poll cycle and the next.",
				Default:     "15s",
				Optional:    true,
			}).
			Field(config.Field{
				Name:  "safety_lag",
				Type:  config.TypeDuration,
				Title: "Safety lag",
				Description: "The watermark is never advanced past (cycle start - safety_lag). This is what " +
					"converts 'an item written during pagination is missed' into 'an item is read twice'.",
				Default:  "60s",
				Optional: true,
			}).
			Field(config.Field{
				Name:  "overlap",
				Type:  config.TypeDuration,
				Title: "Re-scan overlap",
				Description: "Every cycle re-queries from (watermark - overlap). Larger is safer and slower. " +
					"It must be at least safety_lag or the two windows leave a hole.",
				Default:  "5m",
				Optional: true,
			}).
			Field(config.Field{
				Name:  "max_pinned_ids",
				Type:  config.TypeInt,
				Title: "Maximum pinned ids",
				Description: "Bound on the durable seen-set. When a cycle would exceed it the set is dropped " +
					"and resume re-delivers duplicates instead of growing the cursor without limit.",
				Default:  50000,
				Optional: true,
				Min:      ptr(0.0),
			}).
			// BREAKAGE 6 lives here, as an absence. The one cross-field rule this connector
			// needs is `overlap >= safety_lag`, and config.Spec.Lints — documented as
			// "declarative cross-field rules, evaluated offline with no I/O" — cannot express
			// it. See the BREAKAGE 6 block at the top of this file. The rule is enforced in
			// Validate instead, which requires instantiating the connector and cannot run in
			// the browser, so the form gives no inline feedback.
			Example(config.Example{
				Title: "Default polling",
				Config: map[string]any{
					"endpoint":   "https://api.example.com/v1/changed",
					"auth_token": "tok",
				},
			}),

		Caps: connector.SourceCaps{
			Caps: connector.Caps{APIVersion: connector.APIVersion},

			// Prefix, not discrete. Discrete requires a per-delivery handle the upstream
			// will accept back (a receipt, a delivery tag, an ack id) and this API has no
			// ack at all. Prefix works because Safe lets a multi-page cycle expose exactly
			// one commit point.
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Unbounded},
			LaneKinds:       []connector.LaneKind{connector.LaneKindStream},

			// One lane. The endpoint offers no partitioning key, so there is nothing to
			// divide; MaxLanes is hard-enforced at announce time, which is the right place
			// for that fact to live.
			MaxLanes: 1,

			// Nothing happens upstream when this source commits: no slot, no offset, no
			// delete. The feed keeps a window regardless, so commit ordering here is a
			// latency question and never a correctness one.
			UpstreamRetention: connector.RetentionWindow,

			// Zero means unknown, and it genuinely is: the vendor does not document how far
			// back /changed reaches. Declaring a number would let the core's restart check
			// approve a resume it cannot actually satisfy.
			ReplayWindow: 0,

			UnitAssignment: connector.UnitsStatic,

			ProducesEventTime: true,
			ProducesChange:    true,
			ProducesSchema:    false,

			// The endpoint returns whole objects, so an after-image is complete.
			CompleteImages: true,

			// Two positions can share a watermark second and differ only in their seen-set,
			// so there is no total order to encode. Order stays nil and every core call
			// site degrades, as Position.Compare's contract promises.
			ComparablePositions: false,

			// Re-querying updated_at >= W-overlap re-reads everything at or after the
			// committed position, which is what Replayable asks.
			Replayable: true,

			// FORCED FALSE BY BREAKAGE 1. This source has a perfect stable key — the item
			// id — and no way to attach it to a record.
			StableKeys: false,

			// Every cycle boundary is a legal resume point; there is no lane boundary at
			// all, the lane being unbounded.
			MidLaneResume: true,

			Heartbeats: true, // Heartbeater
			Validates:  true, // Validator
			Probes:     true, // Prober

			// Deliberately NOT declared, with reasons:
			//   Discoverable   - one stream, named in config; there is nothing to enumerate.
			//   ReportsBacklog - see BREAKAGE 4. I can answer the lag and not the count, and
			//                    the interface makes that all-or-nothing.
			//   Nackable       - Nack is keyed on a handle (discrete lanes only) or a
			//                    Position. My positions are per-BATCH, so a Position names
			//                    up to page_size items and cannot identify the one that
			//                    dead-lettered. The only per-record identity I have is the
			//                    item id, which is Origin.Key — unsettable, and also not a
			//                    field on connector.Nack. Declining is the honest answer.
		},

		New: func(_ context.Context, c *config.Config) (*src, error) {
			s := &src{
				endpoint:     config.Must[string](c, "endpoint"),
				stream:       record.StreamName(config.Must[string](c, "stream")),
				idField:      config.Must[string](c, "id_field"),
				updatedField: config.Must[string](c, "updated_at_field"),
				pageSize:     config.Must[int](c, "page_size"),
				poll:         config.Must[time.Duration](c, "poll_interval"),
				lag:          config.Must[time.Duration](c, "safety_lag"),
				overlap:      config.Must[time.Duration](c, "overlap"),
				maxPinned:    config.Must[int](c, "max_pinned_ids"),
			}
			tok, err := c.Secret("auth_token")
			if err == nil {
				s.token = tok
			}
			return s, c.Err()
		},
	})
}

func ptr[T any](v T) *T { return &v }

// ---------------------------------------------------------------------------
// The durable progress value.
// ---------------------------------------------------------------------------

// cursor is this source's progress: a watermark second plus the ids already delivered
// inside the overlap window ending at it. It is JSON so the encoding is additive, which is
// what lets rule 3 of the Blob format contract be honoured — an unknown newer field is
// ignored rather than fatal.
//
// It is NOT a scalar and it is NOT totally ordered. Two cursors can carry the same
// Watermark and different Pinned sets, and neither precedes the other.
type cursor struct {
	// Watermark is a unix SECOND. Everything with updated_at <= Watermark has been
	// delivered, except that ties inside the second are disambiguated only by Pinned.
	Watermark int64 `json:"watermark"`

	// OverlapSeconds records the overlap this cursor was written under, so a config change
	// that shrinks the overlap does not silently invalidate the Pinned set's meaning.
	OverlapSeconds int64 `json:"overlap_seconds"`

	// Pinned is the set of item ids already delivered whose updated_at lies in
	// [Watermark-OverlapSeconds, Watermark]. It is the only thing standing between
	// one-second granularity and either duplicates without end or a gap.
	Pinned []string `json:"pinned,omitempty"`

	// PinnedDropped is true when the set exceeded max_pinned_ids and was discarded.
	// Resume will then re-deliver every item in the overlap window. Recorded rather than
	// silently forgotten, because "you will see duplicates" is an operator-visible fact.
	PinnedDropped bool `json:"pinned_dropped,omitempty"`
}

func (c cursor) watermarkTime() time.Time {
	if c.Watermark == 0 {
		return time.Time{}
	}
	return time.Unix(c.Watermark, 0).UTC()
}

func (c cursor) encode() (record.Blob, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return record.Blob{}, err
	}
	// Stamped at SERIALISE time, per rule 4 of the format contract.
	return record.Blob{Version: cursorV1, Bytes: b}, nil
}

func decodeCursor(b record.Blob) (cursor, error) {
	var c cursor
	if b.IsZero() {
		return c, nil // rule 2: absent or zero version is legacy, which here means cold.
	}
	if err := json.Unmarshal(b.Bytes, &c); err != nil {
		// The encoding is additive JSON, so a decode failure is genuine corruption or a
		// foreign connector's state, not a version skew. Fail loudly naming both versions.
		return c, fmt.Errorf("cursor version %d is undecodable by build %d: %w", b.Version, cursorV1, err)
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// One poll cycle.
// ---------------------------------------------------------------------------

// cycle is the state of one logical pass over the feed. It is the unit that maps onto
// Position.Safe: no position inside a cycle is a legal resume point, because the pages are
// unordered and page 7 can hold an older updated_at than page 1.
//
// It is DISCARDED on any interruption. Offset pagination over a live unordered set has no
// stable resume point, so continuing a cycle after a 429 pause would silently skip
// whatever shifted across the offset boundary while we waited. Restarting the cycle costs
// re-reads and cannot lose anything.
type cycle struct {
	from      time.Time // the since= value this cycle was opened with
	startedAt time.Time
	offset    int

	// maxUpdated is the largest updated_at OBSERVED this cycle, including items that were
	// skipped as duplicates. Skipped items still constrain the watermark.
	maxUpdated time.Time

	// observed maps every id seen this cycle to its updated_at. It becomes the next
	// cursor's Pinned set, filtered to the new overlap window.
	observed map[string]time.Time

	// delivered is the subset actually handed to the engine this cycle, so a repeat of the
	// same item on a later page of the SAME cycle is dropped.
	delivered map[string]struct{}

	// pending holds the tail of a page that did not fit in the caller's batch.
	pending []item

	// hasMore is what the last page reported.
	hasMore bool

	// pages counts fetches, for the position label.
	pages int
}

// item is one decoded feed entry.
type item struct {
	id      string
	updated time.Time
	body    record.Map
}

// ---------------------------------------------------------------------------
// The source.
// ---------------------------------------------------------------------------

type src struct {
	// Config, immutable after New.
	endpoint     string
	token        string
	stream       record.StreamName
	idField      string
	updatedField string
	pageSize     int
	poll         time.Duration
	lag          time.Duration
	overlap      time.Duration
	maxPinned    int

	rt  connector.SourceRuntime
	log *slog.Logger
	hc  *http.Client

	// --- read goroutine only, no lock ---
	lane     record.LaneID
	cur      cursor
	curSet   map[string]struct{} // cur.Pinned as a set
	curToken record.Blob         // cur, pre-encoded, for unsafe mid-cycle positions
	cyc      *cycle
	lastPoll time.Time

	// nextAllowed is a purely local rate-limit gate. It exists because BREAKAGE 3 leaves
	// the engine's handling of a RetryAfter-bearing Read fault undefined: whatever the
	// engine does, this source will not hammer a throttled endpoint.
	nextAllowed time.Time

	// --- shared between the read goroutine and the control goroutine ---
	//
	// The contract on Source.Read promises Read never runs concurrently with itself, and
	// that Commit/Heartbeat/Backlog/Nack share ONE other goroutine. So exactly one mutex
	// is needed, and it protects exactly the fields both sides touch. Nothing else in this
	// file is locked.
	mu         sync.Mutex
	committedW time.Time
	throttled  time.Time

	// Metrics. The core owns naming and labels; a nil handle is skipped rather than
	// treated as a reason to fail Open.
	mDuplicates connector.Counter
	mThrottled  connector.Counter
	mCycles     connector.Counter
	gLag        connector.Gauge
}

// Open is idempotent: the engine calls it again, with backoff, after any method returns
// fault.ErrNotConnected. Re-announcing an existing lane with an identical LaneSpec returns
// its id, so the announce path is safe to repeat.
//
// ctx here is scoped to the OPENING. This source stores no context; every later call gets
// its own, and the HTTP client's timeout bounds anything that would otherwise outlive one.
func (s *src) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt
	s.log = rt.Log()
	if s.hc == nil {
		s.hc = &http.Client{Timeout: 30 * time.Second}
	}
	s.registerMetrics(rt.Metrics())

	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}

	switch {
	case len(as) == 0:
		// Cold start. A cold start is distinguished by Assigned returning nothing, never by
		// testing a position against nil.
		id, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
			// Derived from stable content — the endpoint URL — never from a handle, a
			// pointer or a timestamp.
			Name:        "feed:" + s.endpoint,
			Stream:      s.stream,
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       "feed",
			Label:       "changed-items feed at " + s.endpoint,
		})
		if err != nil {
			return err
		}
		s.lane = id
		s.cur = cursor{OverlapSeconds: int64(s.overlap / time.Second)}

	default:
		s.lane = as[0].ID
		c, err := decodeCursor(as[0].Cursor.Token)
		if err != nil {
			return fault.Contract(fault.OpOpen, err)
		}
		s.cur = c
		if s.cur.OverlapSeconds == 0 {
			s.cur.OverlapSeconds = int64(s.overlap / time.Second)
		}
	}

	s.curSet = make(map[string]struct{}, len(s.cur.Pinned))
	for _, id := range s.cur.Pinned {
		s.curSet[id] = struct{}{}
	}
	if s.curToken, err = s.cur.encode(); err != nil {
		return fault.Bug(fault.OpOpen, err)
	}

	// Any cycle in progress is abandoned across a reconnect. There is no stable
	// pagination token, so a half-finished cycle cannot be resumed — only restarted.
	s.cyc = nil

	s.mu.Lock()
	s.committedW = s.cur.watermarkTime()
	s.mu.Unlock()

	return nil
}

func (s *src) registerMetrics(m connector.Metrics) {
	if m == nil {
		return
	}
	if c, err := m.Counter("duplicates_skipped"); err == nil {
		s.mDuplicates = c
	}
	if c, err := m.Counter("throttled"); err == nil {
		s.mThrottled = c
	}
	if c, err := m.Counter("poll_cycles"); err == nil {
		s.mCycles = c
	}
	if g, err := m.Gauge("watermark_lag_seconds"); err == nil {
		s.gLag = g
	}
}

// Read produces at most one page's worth of records per call and NEVER returns an empty
// non-terminal batch.
//
// The empty-batch prohibition is not a style choice: ledger.Admit coerces a zero-record
// batch to refs=1 and ledger.Settle can only release a group through l.byRec, which a
// zero-record group never populates, so one empty batch freezes the lane's contiguous
// prefix forever. See BREAKAGE 2. That prohibition is also why an idle feed's DURABLE
// watermark cannot advance: the advance is real and in memory, and it has no carrier.
//
// Cancellation is drain-then-stop. When ctx is done, this returns whatever is already in
// dst — with a position attached, so the work is accounted — and reports ctx.Err() only
// once there is nothing left.
func (s *src) Read(ctx context.Context, dst *record.Batch) error {
	dst.Reset()
	dst.Lane = s.lane

	// The fence. Records already handed over settle for accounting, but this source must
	// stop producing for a lane it no longer holds.
	if s.rt.Lanes().Revoked(s.lane) {
		return fault.ErrFenced
	}

	for {
		if s.cyc == nil {
			if err := s.waitForGate(ctx); err != nil {
				return s.handoff(dst, err)
			}
			s.openCycle()
		}

		items, err := s.nextPage(ctx)
		if err != nil {
			return s.handoff(dst, s.cycleFailed(err))
		}

		full := s.emit(dst, items)

		switch {
		case full:
			// The caller's batch hit its hard cap mid-page. Hand it over with no commit
			// point; the remainder of the page is stashed on the cycle.
			s.markUnsafe(dst)
			return nil

		case s.cyc.hasMore:
			if dst.Len() > 0 {
				s.markUnsafe(dst)
				return nil
			}
			// Every item on that page was a duplicate. Keep paging rather than handing
			// back an empty batch.

		default:
			// Cycle complete. Advance the in-memory watermark unconditionally; publish it
			// only if we have a record to carry it.
			s.closeCycle()
			if dst.Len() > 0 {
				if err := s.markSafe(dst); err != nil {
					return s.handoff(dst, err)
				}
				return nil
			}
			// Nothing changed. Loop, and the gate will hold us for a poll interval.
		}

		if err := ctx.Err(); err != nil {
			return s.handoff(dst, err)
		}
	}
}

// handoff attaches a position to whatever is already in dst before returning err, because
// the engine admits the batch BEFORE handling the error and a batch of records with no
// position would be admitted with an empty token.
func (s *src) handoff(dst *record.Batch, err error) error {
	if dst.Len() > 0 {
		s.markUnsafe(dst)
	}
	return err
}

// waitForGate blocks until both the poll interval and any rate-limit pause have elapsed.
func (s *src) waitForGate(ctx context.Context) error {
	gate := s.lastPoll.Add(s.poll)
	if s.nextAllowed.After(gate) {
		gate = s.nextAllowed
	}
	d := time.Until(gate)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *src) openCycle() {
	now := time.Now().UTC()
	from := s.cur.watermarkTime()
	if from.IsZero() {
		// Cold start: read the whole window the endpoint will give us.
		from = now.Add(-s.overlap)
	} else {
		from = from.Add(-time.Duration(s.cur.OverlapSeconds) * time.Second)
	}
	s.cyc = &cycle{
		from:      from.Truncate(time.Second),
		startedAt: now,
		observed:  map[string]time.Time{},
		delivered: map[string]struct{}{},
		hasMore:   true,
	}
	s.lastPoll = now
	if s.mCycles != nil {
		s.mCycles.Add(1)
	}
}

// closeCycle folds the cycle's observations into the in-memory cursor.
//
// The new watermark is min(largest updated_at observed, cycleStart - safety_lag), floored
// at the old watermark so it can never move backwards. The lag term is the whole defence
// against the API's mid-pagination misses: an item written into a second we have already
// passed is re-read on the next cycle instead of being lost, because we never claim a
// second younger than the lag.
func (s *src) closeCycle() {
	c := s.cyc
	s.cyc = nil

	old := s.cur.watermarkTime()
	next := c.maxUpdated
	if hi := c.startedAt.Add(-s.lag); next.IsZero() || next.After(hi) {
		next = hi
	}
	if next.Before(old) {
		next = old
	}
	next = next.Truncate(time.Second)

	lo := next.Add(-s.overlap)
	pinned := make([]string, 0, len(c.observed))
	for id, at := range c.observed {
		if !at.Before(lo) {
			pinned = append(pinned, id)
		}
	}

	nc := cursor{
		Watermark:      next.Unix(),
		OverlapSeconds: int64(s.overlap / time.Second),
		Pinned:         pinned,
	}
	if s.maxPinned > 0 && len(pinned) > s.maxPinned {
		// Duplicates are permitted; gaps are not. So the set is dropped, not truncated
		// arbitrarily, and the consequence is announced.
		nc.Pinned, nc.PinnedDropped = nil, true
		s.note(connector.Event{
			Kind:     connector.EventDrift,
			Severity: fault.TransientInternal,
			Lane:     s.lane,
			Message:  "seen-set exceeded max_pinned_ids and was dropped",
			Detail: fmt.Sprintf("%d ids in the %s overlap window exceeds the %d cap; a restart will "+
				"re-deliver the window. Raise max_pinned_ids or shorten overlap.",
				len(pinned), s.overlap, s.maxPinned),
		})
	}

	s.cur = nc
	s.curSet = make(map[string]struct{}, len(nc.Pinned))
	for _, id := range nc.Pinned {
		s.curSet[id] = struct{}{}
	}
	if b, err := nc.encode(); err == nil {
		s.curToken = b
	}
}

// emit moves items into dst, skipping anything already delivered. It reports whether the
// batch filled, in which case the unconsumed tail is stashed on the cycle.
func (s *src) emit(dst *record.Batch, items []item) (full bool) {
	var skipped float64
	for i := range items {
		it := items[i]

		// Observed BEFORE the duplicate check: a skipped item still constrains the
		// watermark and still belongs in the next seen-set.
		if prev, ok := s.cyc.observed[it.id]; !ok || it.updated.After(prev) {
			s.cyc.observed[it.id] = it.updated
		}
		if it.updated.After(s.cyc.maxUpdated) {
			s.cyc.maxUpdated = it.updated
		}

		if s.alreadyDelivered(it.id) {
			skipped++
			continue
		}

		r := dst.Add()
		if r == nil {
			s.cyc.pending = items[i:]
			if skipped > 0 && s.mDuplicates != nil {
				s.mDuplicates.Add(skipped)
			}
			return true
		}
		s.fill(r, it)
		s.cyc.delivered[it.id] = struct{}{}
	}
	s.cyc.pending = nil
	if skipped > 0 && s.mDuplicates != nil {
		s.mDuplicates.Add(skipped)
	}
	return false
}

func (s *src) alreadyDelivered(id string) bool {
	if _, ok := s.cyc.delivered[id]; ok {
		return true // same item on two pages of one cycle
	}
	_, ok := s.curSet[id]
	return ok // delivered by an earlier cycle inside the overlap window
}

// fill populates one record slot. Identity and provenance are already stamped; everything
// set here is either an exported field or goes through a documented mutator.
func (s *src) fill(r *record.Record, it item) {
	r.EventTime = it.updated
	r.Payload = record.StructPayload(it.body)

	after := r.Payload.Clone()
	r.Change = &record.Change{
		Version: record.ChangeVersion,
		// BREAKAGE 5: the feed says "this changed". It does not say whether the item is
		// new. OpUpsert does not exist, and OpUnknown is documented as a defect, so this
		// is a lie for every first appearance of an item.
		Op:             record.OpUpdate,
		Keys:           [][]string{{s.idField}},
		After:          &after,
		AfterComplete:  record.CompletenessComplete,
		BeforeComplete: record.CompletenessAbsent,
		CommitTime:     it.updated,
	}

	// ------------------------------------------------------------------
	// BREAKAGE 1, at the exact line where it bites.
	//
	// What this connector needs to write, and cannot:
	//
	//     r.SetKey([]byte(it.id))       // undefined
	//     r.SetUpstream([]byte(it.id))  // undefined
	//
	// `go build` on a probe package containing those two lines:
	//
	//     r.SetKey undefined (type *record.Record has no field or method SetKey)
	//     r.SetUpstream undefined (type *record.Record has no field or method SetUpstream)
	//     cannot assign to r.Origin().Key (neither addressable nor a map index expression)
	//
	// The line below is the workaround, and it is NOT equivalent. Nothing in the core
	// reads Meta as identity: it does not feed dedupe, it does not become
	// Request.IdempotencyKey, and it does not satisfy SinkCaps.RequiresKey. It obliges
	// every sink to learn a source-specific metadata key, which is precisely the
	// per-source special-casing that constraint #1 forbids.
	// ------------------------------------------------------------------
	_ = r.Meta.Set(record.NSSource, "item_id", record.String(it.id))
	_ = r.Meta.Set(record.NSSource, "updated_at_second", record.Int(it.updated.Unix()))
}

// markUnsafe attaches a position that is NOT a resume point.
//
// The token is the last DURABLE cursor rather than an empty blob: if anything ever did
// persist this position, it would persist a conservative but valid resume point rather
// than a cold start. Safe:false means the ledger records it in the resolved prefix and
// never promotes it to pendingFlush, which is exactly the mid-transaction shape Safe was
// introduced for.
func (s *src) markUnsafe(dst *record.Batch) {
	dst.Position = record.Position{
		Token: s.curToken,
		Safe:  false,
		At:    time.Now().UTC(),
		Label: fmt.Sprintf("mid-cycle, page %d (no commit point)", s.cyc.pageNo()),
	}
}

// markSafe attaches the one commit point a cycle produces.
func (s *src) markSafe(dst *record.Batch) error {
	tok, err := s.cur.encode()
	if err != nil {
		return fault.Bug(fault.OpRead, err)
	}
	s.curToken = tok

	label := fmt.Sprintf("changed through %s, %d ids pinned",
		s.cur.watermarkTime().Format(time.RFC3339), len(s.cur.Pinned))
	if s.cur.PinnedDropped {
		label += " (set dropped; resume re-delivers the window)"
	}

	dst.Position = record.Position{
		Token: tok,
		// Order stays nil: two cursors can share a watermark second and differ only in
		// their pinned set, so there is no order-preserving encoding to supply. Scalar
		// stays nil for the same reason, and the core reports unknown rather than zero.
		Safe:  true,
		At:    time.Now().UTC(),
		Label: label,
	}
	return nil
}

func (c *cycle) pageNo() int {
	if c == nil {
		return 0
	}
	return c.pages
}

// cycleFailed classifies a page failure and discards the cycle.
func (s *src) cycleFailed(err error) error {
	s.cyc = nil

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	var he *httpError
	if !errors.As(err, &he) {
		// A transport failure before or during send. The effect definitely did not land —
		// this is a read, so there is no effect — so TransientUpstream is honest.
		return fault.Transient(fault.OpRead, err)
	}

	switch {
	case he.status == http.StatusTooManyRequests || he.status == http.StatusServiceUnavailable:
		d := he.retryAfter
		if d <= 0 {
			d = 5 * time.Second
		}
		s.nextAllowed = time.Now().Add(d)

		s.mu.Lock()
		s.throttled = s.nextAllowed
		s.mu.Unlock()

		if s.mThrottled != nil {
			s.mThrottled.Add(1)
		}
		s.note(connector.Event{
			Kind:     connector.EventDegraded,
			Severity: fault.TransientUpstream,
			Lane:     s.lane,
			Message:  "rate limited by the feed",
			Detail:   fmt.Sprintf("HTTP %d; Retry-After %s; the poll cycle was restarted", he.status, d),
		})

		// BREAKAGE 3. RetryAfter is set because that is what the field is for. Whether the
		// engine honours it as a delay or spends one of RetryPolicy.MaxAttempts on it is
		// undefined, which is why s.nextAllowed above independently gates the next request.
		f := fault.Transient(fault.OpRead, err)
		f.RetryAfter = d
		f.User = fmt.Sprintf("The feed is rate limiting canal and asked for %s before the next request.", d)
		f.Dev = fmt.Sprintf("HTTP %d from %s; cycle discarded because offset pagination has no stable resume point", he.status, s.endpoint)
		return f

	case he.status == http.StatusUnauthorized || he.status == http.StatusForbidden:
		f := fault.Permanent(fault.OpRead, err)
		f.User = "The feed rejected canal's credentials. Check auth_token."
		return f

	case he.status >= 500:
		return fault.Transient(fault.OpRead, err)

	case he.status == http.StatusNotFound:
		f := fault.Permanent(fault.OpRead, err)
		f.User = "The configured endpoint returned 404. Check endpoint."
		return f

	default:
		// A 4xx that is not auth and not throttling means this request is malformed and
		// will stay malformed: the two ends disagree structurally.
		return fault.Contract(fault.OpRead, err)
	}
}

func (s *src) note(e connector.Event) {
	if s.rt == nil {
		return
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	s.rt.Note(e)
}

// Commit is where a source tells its upstream to advance. This one has nothing to tell:
// the feed has no ack, no offset, no receipt and no slot, and the core has already
// persisted Ack.Through.Token in phase two — that token IS this source's cursor.
//
// So all that happens here is that the committed watermark is recorded for the lag gauge.
// It runs on the control goroutine, concurrently with Read, which is why it is the one
// place in this file that takes the mutex.
//
// Ack.Abandoned is deliberately ignored: this commit is non-destructive, so a
// dead-lettered record does not need to hold the watermark back. A source whose commit
// deleted messages would refuse to advance here instead.
func (s *src) Commit(_ context.Context, a connector.Ack) error {
	if a.Through.Token.IsZero() {
		return nil
	}
	c, err := decodeCursor(a.Through.Token)
	if err != nil {
		// Escalated, not swallowed: the core hands back bytes this connector authored, so
		// failing to decode them means the two ends disagree structurally.
		return fault.Contract(fault.OpCommitSource, err)
	}
	s.mu.Lock()
	s.committedW = c.watermarkTime()
	s.mu.Unlock()
	return nil
}

// Close releases resources and is safe on a never-opened source: config validation
// constructs a component and then closes it, so s.hc may be nil here.
func (s *src) Close(_ context.Context) error {
	if s.hc != nil {
		s.hc.CloseIdleConnections()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Optional interfaces.
// ---------------------------------------------------------------------------

// Heartbeat publishes the event-time lag. It runs on the control goroutine.
//
// The gauge is OMITTED, never set to zero, when there is no committed watermark: a
// stalled pipeline reporting zero lag is worse than one reporting nothing.
func (s *src) Heartbeat(_ context.Context, lane record.LaneID, idle time.Duration) error {
	if lane != s.lane {
		return nil
	}
	s.mu.Lock()
	w, th := s.committedW, s.throttled
	s.mu.Unlock()

	if s.gLag != nil && !w.IsZero() {
		s.gLag.Set(time.Since(w).Seconds())
	}
	if !th.IsZero() && time.Now().Before(th) && s.log != nil {
		s.log.Info("feed idle while rate limited",
			slog.Duration("idle", idle), slog.Time("until", th))
	}
	return nil
}

// Validate is tier two: it may do I/O and it returns EVERY diagnostic, per field.
func (s *src) Validate(ctx context.Context) config.Diagnostics {
	var d config.Diagnostics

	u, err := url.Parse(s.endpoint)
	switch {
	case err != nil:
		d = d.Errorf(config.CodeWrongType, []string{"endpoint"},
			"endpoint is not a URL: "+err.Error(), "Give an absolute https URL.")
	case u.Scheme != "https" && u.Scheme != "http":
		d = d.Errorf(config.CodeWrongType, []string{"endpoint"},
			"endpoint scheme must be http or https", "")
	case u.Host == "":
		d = d.Errorf(config.CodeMissingField, []string{"endpoint"},
			"endpoint has no host", "")
	case u.Scheme == "http":
		d = d.Warnf(config.CodeCustom, []string{"endpoint"},
			"the bearer token will be sent over plaintext http", "Use https.")
	}

	if s.token == "" {
		d = d.Errorf(config.CodeMissingField, []string{"auth_token"},
			"auth_token is required", "")
	}
	if s.overlap < s.lag {
		d = d.Errorf(config.CodeOutOfRange, []string{"overlap"},
			fmt.Sprintf("overlap (%s) is shorter than safety_lag (%s), which leaves a band of "+
				"seconds nothing ever reads", s.overlap, s.lag),
			"Set overlap to at least safety_lag.")
	}
	if s.lag < time.Second {
		d = d.Warnf(config.CodeOutOfRange, []string{"safety_lag"},
			"safety_lag below one second cannot cover this feed's one-second timestamp granularity",
			"A busy second's writes will be missed. Use at least 30s.")
	}
	if s.maxPinned == 0 {
		d = d.Warnf(config.CodeOutOfRange, []string{"max_pinned_ids"},
			"max_pinned_ids of 0 means the seen-set is unbounded",
			"The durable cursor will grow with the number of items changed per overlap window.")
	}

	if d.HasErrors() {
		return d
	}

	// Reachability is a separate fact from configuration, so it is a separate diagnostic.
	if _, err := s.fetch(ctx, time.Now().Add(-time.Minute), 0, 1); err != nil {
		var he *httpError
		switch {
		case errors.As(err, &he) && (he.status == http.StatusUnauthorized || he.status == http.StatusForbidden):
			d = d.Errorf(config.CodeAuthFailed, []string{"auth_token"},
				"the feed rejected the token", "")
		case errors.As(err, &he) && he.status == http.StatusNotFound:
			d = d.Errorf(config.CodeNotFound, []string{"endpoint"},
				"the feed returned 404", "")
		default:
			d = d.Warnf(config.CodeUnreachable, []string{"endpoint"},
				"could not reach the feed during validation: "+err.Error(),
				"Validation does not block on this; Open is retried with backoff.")
		}
	}
	return d
}

// Probe returns a LIST, because "the endpoint answered" and "the response is shaped the
// way this connector needs" are different facts.
func (s *src) Probe(ctx context.Context) connector.ProbeResults {
	if s.hc == nil {
		s.hc = &http.Client{Timeout: 10 * time.Second}
	}
	items, err := s.fetch(ctx, time.Now().Add(-time.Hour), 0, 1)
	if err != nil {
		return connector.ProbeFailed("feed reachable", err)
	}
	out := connector.ProbeOK("feed reachable")
	switch {
	case len(items) == 0:
		out = append(out, connector.ProbeResult{
			Label: "response shape",
			Err:   errors.New("the feed returned no items in the last hour, so the item shape could not be checked"),
			Error: "the feed returned no items in the last hour, so the item shape could not be checked",
		})
	default:
		out = append(out, connector.ProbeResult{Label: "response shape"})
	}
	return out
}

// ---------------------------------------------------------------------------
// The transport. Plain, and only as clever as the API forces it to be.
// ---------------------------------------------------------------------------

// httpError carries the status and the parsed Retry-After so classification happens once,
// at the point of raise, rather than by re-inspecting a response later.
type httpError struct {
	status     int
	retryAfter time.Duration
	body       string
}

func (e *httpError) Error() string {
	if e.retryAfter > 0 {
		return fmt.Sprintf("feed returned HTTP %d (retry after %s): %s", e.status, e.retryAfter, e.body)
	}
	return fmt.Sprintf("feed returned HTTP %d: %s", e.status, e.body)
}

// nextPage returns the next page of the open cycle, preferring anything stashed by a
// batch that filled mid-page.
func (s *src) nextPage(ctx context.Context) ([]item, error) {
	if p := s.cyc.pending; len(p) > 0 {
		s.cyc.pending = nil
		return p, nil
	}
	if d := time.Until(s.nextAllowed); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	items, err := s.fetch(ctx, s.cyc.from, s.cyc.offset, s.pageSize)
	if err != nil {
		return nil, err
	}
	s.cyc.pages++
	s.cyc.offset += len(items)
	// has_more is what the API says, but a short page is also the end: offset paging over
	// a shrinking result set can report has_more and then return nothing.
	s.cyc.hasMore = s.cyc.hasMore && len(items) >= s.pageSize
	return items, nil
}

type feedPage struct {
	Items   []map[string]any `json:"items"`
	HasMore bool             `json:"has_more"`
}

func (s *src) fetch(ctx context.Context, since time.Time, offset, limit int) ([]item, error) {
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("since", since.UTC().Format(time.RFC3339))
	q.Set("offset", strconv.Itoa(offset))
	q.Set("limit", strconv.Itoa(limit))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &httpError{
			status:     resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			body:       strings.TrimSpace(string(b)),
		}
	}

	var page feedPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, err
	}
	if s.cyc != nil {
		s.cyc.hasMore = page.HasMore
	}

	out := make([]item, 0, len(page.Items))
	for _, raw := range page.Items {
		it, err := s.decodeItem(raw)
		if err != nil {
			// A single malformed item is this record's problem, not the feed's, and not a
			// reason to abandon a page that cost a rate-limit token. It is counted and
			// dropped here rather than dead-lettered, because there is no record to carry
			// it: MarkFailed needs a slot, and a slot needs an id and a timestamp this
			// item does not have.
			if s.log != nil {
				s.log.Warn("dropping unparseable feed item", slog.String("error", err.Error()))
			}
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

func (s *src) decodeItem(raw map[string]any) (item, error) {
	idv, ok := raw[s.idField]
	if !ok {
		return item{}, fmt.Errorf("item has no %q field", s.idField)
	}
	id := stringOf(idv)
	if id == "" {
		return item{}, fmt.Errorf("item %q field is empty or not a scalar", s.idField)
	}
	uv, ok := raw[s.updatedField]
	if !ok {
		return item{}, fmt.Errorf("item %q has no %q field", id, s.updatedField)
	}
	at, err := parseTime(uv)
	if err != nil {
		return item{}, fmt.Errorf("item %q: %w", id, err)
	}
	m, ok := toValue(raw).(record.Map)
	if !ok {
		return item{}, fmt.Errorf("item %q did not convert to a map", id)
	}
	// Truncated because the API's granularity IS one second. Keeping sub-second precision
	// it does not have would make the watermark look finer than it is.
	return item{id: id, updated: at.UTC().Truncate(time.Second), body: m}, nil
}

func stringOf(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		return ""
	}
}

func parseTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, nil
			}
		}
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return time.Unix(n, 0), nil
		}
		return time.Time{}, fmt.Errorf("%q is not a recognised timestamp", x)
	case float64:
		return time.Unix(int64(x), 0), nil
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(n, 0), nil
	default:
		return time.Time{}, fmt.Errorf("timestamp is a %T, not a string or a number", v)
	}
}

// parseRetryAfter accepts both forms RFC 9110 permits: delta-seconds and an HTTP-date.
// Honouring only the integer form is how a Retry-After hint gets silently discarded.
func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil {
		if n < 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// toValue converts a JSON tree into the canonical record.Value closed set.
//
// Note that a nil interface becomes record.Null{}: "explicitly null" and "no value
// supplied" are different facts in record.Value, and JSON only expresses the first.
func toValue(v any) record.Value {
	switch x := v.(type) {
	case nil:
		return record.Null{}
	case bool:
		return record.Bool(x)
	case string:
		return record.String(x)
	case float64:
		// An integral float64 is reported as an Int so a downstream schema does not see
		// every id as a floating-point number.
		if x == float64(int64(x)) {
			return record.Int(int64(x))
		}
		return record.Float(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return record.Int(n)
		}
		if f, err := x.Float64(); err == nil {
			return record.Float(f)
		}
		return record.String(x.String())
	case []any:
		out := make(record.List, 0, len(x))
		for _, e := range x {
			out = append(out, toValue(e))
		}
		return out
	case map[string]any:
		out := make(record.Map, len(x))
		for k, e := range x {
			out[k] = toValue(e)
		}
		return out
	case time.Time:
		return record.Time(x)
	default:
		return record.String(fmt.Sprint(x))
	}
}

// Compile-time assertions that this really is the shape the core asked for. If any of
// these breaks, the connector is broken, not the assertion.
var (
	_ connector.Source      = (*src)(nil)
	_ connector.Heartbeater = (*src)(nil)
	_ connector.Validator   = (*src)(nil)
	_ connector.Prober      = (*src)(nil)
)
