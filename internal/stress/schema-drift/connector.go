// Package schemadrift is a HOSTILE CONNECTOR SET written against canal's real
// interfaces to find out where they break.
//
// The case: a source whose schema changes mid-stream — columns added, a column type
// widened, a column dropped, a table renamed, plus one field whose type is a union
// varying per record. Three sinks with incompatible needs: one must have DDL applied
// before the next record, one absorbs anything, one must reject and dead-letter
// incompatible records. Schema history itself must be checkpointed, because on
// restart the pipeline must know the schema AS OF THE RESUME POSITION, not the
// current one.
//
// Everything below compiles against pkg/record, pkg/schema, pkg/fault, pkg/config,
// pkg/connector and pkg/registry and imports nothing else of canal's, exactly as a
// third-party connector would.
//
// VERDICT: requires-core-change. Two of the six findings are fatal — not "awkward",
// not "verbose", but structurally unreachable. The full argument for each is in the
// numbered comment block immediately above the code that hits it. In summary:
//
//	B1 FATAL  a source has no channel to publish a schema body or an ordered
//	          schema.Change to the core. SourceRuntime has no Schemas(). There is no
//	          SchemaDeclarer. connector.Event carries only strings. Therefore
//	          SinkCaps.SchemaChanges, SchemaApplier.ApplySchemaChange,
//	          FlushSchemaChange, Checkpoint.SchemaEpoch, schema.Change and
//	          SourceCaps.ProducesSchema are all unreachable from a connector: the
//	          entire drift half of the architecture has no producer.
//
//	B2 FATAL  a sink cannot resolve a schema.Ref it did not receive at Open.
//	          SinkRuntime has no Schemas(), and schema.Change carries only From/To
//	          *Field deltas and never the resulting *schema.Schema. A sink told to
//	          CreateStream cannot learn the columns; a sink handed Request.Schema
//	          naming a ref minted after Open cannot resolve it at all.
//
//	B3 MAJOR  one lane cannot serve more than one stream. Origin.Stream is stamped
//	          from the Allocator and has no per-record setter, so a shared-log CDC
//	          source (one slot, N tables — the normal shape) must collapse every
//	          table into one origin.Stream, which collapses the dedupe scope and
//	          reintroduces the exact R5 collision the scope was designed to prevent.
//
//	B4 MAJOR  schema.ChangeKind has no RenameStream, and Opening.Streams is frozen at
//	          Open. A table rename is expressible only as DropStream+CreateStream,
//	          which Destructive() reports true, so the DEFAULT DriftLenient policy
//	          refuses a change that loses nothing; and the rename target has no
//	          DestMode and no Keys anywhere in the interface set.
//
//	B5 MAJOR  schema history has no durable home that commits atomically with the
//	          lane cursor. StateHandle is keyed on LaneID only and the source writes
//	          it in Commit (phase three), after the core's own cursor write (phase
//	          two). The only atomic channel is Position.Token, which forces an
//	          O(history) blob on every batch.
//
//	B6 MINOR  a per-record union type is expressible only as schema.TypeAny, which
//	          erases the variant set, or as Logical.Name:"union<string,int64>", the
//	          magic-name-string convention schema/doc.go explicitly condemns.
//
// What DID fit cleanly, stated so the fits are not lost among the breaks:
// Position.Token/Order/Scalar/Safe/Label carry a CDC coordinate with no strain;
// WriteResult.Failed keyed on RecordID expresses per-record DLQ exactly;
// Meta.NoteChange expresses a narrowing conversion exactly; Request.Schema.Epoch is
// enough for a sink to detect that it is behind; Record.Dest is enough to reroute a
// renamed table; and Batch.Add's pre-stamped provenance never once got in the way.
package schemadrift

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// tokenV2 versions this connector's cursor encoding. See B5: version 1 held only the
// LSN; version 2 had to grow a whole schema history because there is nowhere else to
// put one.
const tokenV2 = 2

// The two logical streams this source produces, and the rename target of the first.
const (
	streamOrders   record.StreamName = "orders"
	streamOrdersV2 record.StreamName = "orders_v2"
	streamCustomer record.StreamName = "customers"
)

// laneChangelog is the single lane. ONE lane, because a logical-replication slot has
// one position for every table it publishes. See B3 for what that costs.
const laneChangelog = "changelog"

// ---------------------------------------------------------------------------------
// B5 (MAJOR): schema history has no durable home that commits with the cursor.
//
// THE REQUIREMENT: on restart, the pipeline must know the schema as of the resume
// position. That means the history must be committed ATOMICALLY with the position it
// describes. If the cursor advances past a DDL whose history entry did not survive,
// the next record decodes against the wrong shape — the "encountered a change event
// whose schema isn't known" failure that architecture.md §14.1 rule 2 and
// engine/checkpoint.go both claim is structurally absent.
//
// WHAT THE INTERFACE OFFERS A SOURCE, exhaustively:
//
//	StateHandle.Set(ctx, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error)
//	StateHandle.SetMany(ctx, w map[record.LaneID]Write) error
//
// Both are keyed on record.LaneID and nothing else. Two consequences:
//
//  1. WRONG KEY SPACE. Schema history is per-STREAM. A lane is per-progress-domain.
//     They are not the same partition and the interface offers no stream-keyed or
//     pipeline-keyed slot. store.Key has a "schema" Space (pkg/store/key.go) and the
//     core's own schema table uses it — but StateHandle, the only store view a
//     connector can reach, exposes the lane space only. A source that keeps its
//     history in the lane blob and later splits one lane into two (a concurrent
//     snapshot, a resharded slot) has to copy the history into both and they diverge.
//
//  2. WRONG PHASE. The core's three-phase commit persists the lane cursor in phase
//     two and calls Source.Commit in phase three (architecture.md §13.1 lines
//     3175-3183). Persister.Commit — the sanctioned helper — therefore writes in
//     phase three. A crash between them leaves the CURSOR advanced and the HISTORY
//     stale. StateHandle.SetMany is atomic across lanes but not atomic with the
//     core's own phase-two write, and a connector has no handle on that write.
//
// THE ONLY ATOMIC CHANNEL IS record.Position.Token, because the core writes it into
// LaneState.Cursor inside the phase-two atom. So this connector serialises the entire
// schema history into the cursor token on EVERY BATCH. That is what encodeToken does
// below, and it is a real cost: O(streams x DDLs) bytes per batch, unbounded, on the
// hot path, re-encoded even when nothing changed. For a 500-table publication with a
// year of migrations that is megabytes per checkpoint. Flink CDC's checkpoint blowup
// (cited in schema/ref.go's own doc comment as the reason Ref exists) is exactly this
// shape of mistake, reintroduced one layer down because the connector had no
// alternative.
//
// A real connector author would not find this. They would put the history in
// StateHandle, ship it, and lose a DDL to a crash six months later.
//
// MINIMAL FIX (additive, breaks no written connector): give SourceRuntime a
// stream-scoped, phase-two-atomic slot. Either
//
//	SourceRuntime.Schemas() connector.SchemaLookup   // already exists on CodecRuntime
//
// which lets the source push the body into the core's own schema table so the core
// commits it in the atom it already writes (this is the same fix B1 needs, and it
// resolves B5 for free); or, if a source must own the encoding,
//
//	StateHandle.SetStream(ctx, s record.StreamName, b record.Blob, ifVersion uint64) (uint64, error)
//
// plus a documented guarantee that a stream write issued during Read lands in the
// next phase-two atom. The first is strictly better: it needs no new key space and it
// is the mechanism the architecture already describes.
// ---------------------------------------------------------------------------------

// histEntry is one point in a stream's schema history: the change that happened, the
// schema that resulted, and the epoch that orders it.
type histEntry struct {
	Epoch  uint64         `json:"epoch"`
	Change schema.Change  `json:"change"`
	Result *schema.Schema `json:"result"`

	// FP is schema.Fingerprint(Result), cached so a restart need not recompute it and
	// so a mismatch after an upgrade is detectable.
	FP [16]byte `json:"fp"`
}

// streamHist is one stream's ordered history. Append-only; entry i has epoch i+1.
type streamHist struct {
	Entries []histEntry `json:"entries"`
}

func (h *streamHist) epoch() uint64 {
	if len(h.Entries) == 0 {
		return 0
	}
	return h.Entries[len(h.Entries)-1].Epoch
}

func (h *streamHist) current() *schema.Schema {
	if len(h.Entries) == 0 {
		return nil
	}
	return h.Entries[len(h.Entries)-1].Result
}

// ref returns the schema.Ref for the head of this history. Note what the source is
// doing here: MINTING A REF ITSELF, with a fingerprint the core has never seen and a
// body the core cannot resolve. See B1.
func (h *streamHist) ref(stream record.StreamName) *schema.Ref {
	if len(h.Entries) == 0 {
		return nil
	}
	e := h.Entries[len(h.Entries)-1]
	return &schema.Ref{Fingerprint: e.FP, Epoch: e.Epoch, Stream: string(stream)}
}

// asOf returns the schema in force at the given epoch — the restart question. This
// method is the whole point of checkpointing history and it works fine; the problem
// is never the arithmetic, it is that the bytes have nowhere durable to live (B5) and
// nobody downstream to read them (B1, B2).
func (h *streamHist) asOf(epoch uint64) *schema.Schema {
	var out *schema.Schema
	for i := range h.Entries {
		if h.Entries[i].Epoch > epoch {
			break
		}
		out = h.Entries[i].Result
	}
	return out
}

// cursor is everything this source must remember, and it all rides in
// Position.Token because that is the only field the core commits atomically with
// progress.
type cursor struct {
	LSN uint64 `json:"lsn"`

	// Step is where in the scripted drift program the source is. A real connector
	// would not need this; a real connector's equivalent is "which DDL statements in
	// the binlog have I already applied", which is the same fact.
	Step int `json:"step"`

	// Hist is the schema history for every stream this lane carries. THIS IS THE
	// PART THAT SHOULD NOT BE HERE. See B5.
	Hist map[record.StreamName]*streamHist `json:"hist"`
}

func newCursor() *cursor {
	return &cursor{Hist: map[record.StreamName]*streamHist{}}
}

func (c *cursor) hist(s record.StreamName) *streamHist {
	if c.Hist == nil {
		c.Hist = map[record.StreamName]*streamHist{}
	}
	h, ok := c.Hist[s]
	if !ok {
		h = &streamHist{}
		c.Hist[s] = h
	}
	return h
}

func encodeToken(c *cursor) (record.Blob, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return record.Blob{}, fault.Bug(fault.OpRead, err)
	}
	// Rule four of the format contract: stamp at serialise time.
	return record.Blob{Version: tokenV2, Bytes: b}, nil
}

func decodeToken(b record.Blob) (*cursor, error) {
	if b.IsZero() {
		return newCursor(), nil
	}
	switch b.Version {
	case 1:
		// Rule two: absent or legacy version means behave as the previous version
		// did. Version 1 was a bare big-endian LSN with no history, which is
		// precisely the state a pre-drift build wrote.
		if len(b.Bytes) < 8 {
			return nil, fault.Contract(fault.OpOpen,
				fmt.Errorf("token version 1 is %d bytes, want 8", len(b.Bytes)))
		}
		c := newCursor()
		c.LSN = binary.BigEndian.Uint64(b.Bytes[:8])
		return c, nil
	case tokenV2:
		c := newCursor()
		if err := json.Unmarshal(b.Bytes, c); err != nil {
			return nil, fault.Contract(fault.OpOpen, err)
		}
		if c.Hist == nil {
			c.Hist = map[record.StreamName]*streamHist{}
		}
		return c, nil
	default:
		// Rule three says never reject a NEWER version when the encoding is
		// additive. JSON is additive, so a newer token is readable and this branch
		// only fires for a version this build genuinely cannot parse.
		if b.Version > tokenV2 {
			c := newCursor()
			if err := json.Unmarshal(b.Bytes, c); err == nil {
				if c.Hist == nil {
					c.Hist = map[record.StreamName]*streamHist{}
				}
				return c, nil
			}
		}
		return nil, fault.Contract(fault.OpOpen,
			fmt.Errorf("cursor token version %d unreadable by build %d", b.Version, tokenV2))
	}
}

// ---------------------------------------------------------------------------------
// The drift program: the five schema events the case demands, plus the union field.
// ---------------------------------------------------------------------------------

func fieldDecimal(name string, prec, scale int) schema.Field {
	return schema.Field{
		Name: name, Type: schema.TypeDecimal, Nullable: false,
		Logical: &schema.Logical{Precision: prec, Scale: scale},
	}
}

// baselineOrders is the shape before any drift.
//
// B6 lives on the `attr` field. The case says one field is a union type varying per
// record: sometimes a string, sometimes an int, sometimes a nested map. The canonical
// type set (schema/type.go) has TypeBool..TypeMap and TypeAny, and no variant member.
// The two expressible encodings are both wrong in a different way; this one picks
// TypeAny and records the intended variant set in Doc, where nothing reads it. See the
// B6 block below.
func baselineOrders() *schema.Schema {
	return &schema.Schema{
		Fields: []schema.Field{
			{Name: "id", Type: schema.TypeInt64},
			fieldDecimal("amount", 10, 2),
			{Name: "note", Type: schema.TypeString, Nullable: true},
			{
				Name: "attr", Type: schema.TypeAny, Nullable: true,
				Doc: "union<string,int64,map<string,string>> — see B6; the type set cannot say this",
			},
		},
		Keys: [][]string{{"id"}},
	}
}

func baselineCustomers() *schema.Schema {
	return &schema.Schema{
		Fields: []schema.Field{
			{Name: "id", Type: schema.TypeInt64},
			{Name: "email", Type: schema.TypeString},
		},
		Keys: [][]string{{"id"}},
	}
}

// driftStep is one entry in the scripted program. Either it emits rows or it is a DDL.
type driftStep struct {
	// Rows is how many rows this step emits, for the stream named by Stream.
	Rows   int
	Stream record.StreamName

	// DDL, when non-nil, mutates the named stream's schema and produces the
	// schema.Change describing it. Returning both is what a real CDC source has after
	// parsing a DDL statement out of the log.
	DDL func(cur *schema.Schema) (*schema.Schema, schema.Change)
}

// program is the whole hostile sequence. Read it top to bottom: this is exactly the
// case under test.
func program() []driftStep {
	return []driftStep{
		{Rows: 3, Stream: streamOrders},
		{Rows: 2, Stream: streamCustomer},

		// (1) COLUMN ADDED — additive, the one kind every system handles.
		{Stream: streamOrders, DDL: func(cur *schema.Schema) (*schema.Schema, schema.Change) {
			next := cloneSchema(cur)
			f := schema.Field{Name: "currency", Type: schema.TypeString, Nullable: true}
			next.Fields = append(next.Fields, f)
			return next, schema.Change{
				Kind: schema.AddField, Stream: string(streamOrders),
				Field: []string{"currency"}, To: &f,
			}
		}},
		{Rows: 3, Stream: streamOrders},

		// (2) COLUMN TYPE WIDENED — decimal(10,2) -> decimal(20,4). Destructive() is
		// true for AlterFieldType even though widening loses nothing, so under the
		// DEFAULT DriftLenient policy the core rewrites this into RenameField+AddField
		// and the destination keeps a dead amount_old column forever. See B4's second
		// half: Destructive() is a property of the KIND, not of the transition, and a
		// widening is indistinguishable from a narrowing at this granularity.
		{Stream: streamOrders, DDL: func(cur *schema.Schema) (*schema.Schema, schema.Change) {
			next := cloneSchema(cur)
			var from, to schema.Field
			for i := range next.Fields {
				if next.Fields[i].Name == "amount" {
					from = next.Fields[i]
					next.Fields[i] = fieldDecimal("amount", 20, 4)
					to = next.Fields[i]
				}
			}
			return next, schema.Change{
				Kind: schema.AlterFieldType, Stream: string(streamOrders),
				Field: []string{"amount"}, From: &from, To: &to,
			}
		}},
		{Rows: 2, Stream: streamOrders},

		// (3) COLUMN DROPPED.
		{Stream: streamOrders, DDL: func(cur *schema.Schema) (*schema.Schema, schema.Change) {
			next := cloneSchema(cur)
			var from schema.Field
			out := next.Fields[:0]
			for _, f := range next.Fields {
				if f.Name == "note" {
					from = f
					continue
				}
				out = append(out, f)
			}
			next.Fields = out
			return next, schema.Change{
				Kind: schema.DropField, Stream: string(streamOrders),
				Field: []string{"note"}, From: &from,
			}
		}},
		{Rows: 2, Stream: streamOrders},

		// (4) TABLE RENAMED: orders -> orders_v2. See B4.
		{Stream: streamOrders, DDL: func(cur *schema.Schema) (*schema.Schema, schema.Change) {
			next := cloneSchema(cur)
			return next, schema.Change{
				// There is no schema.RenameStream. DropStream is the only kind that
				// names the departure and it reports Destructive() == true, so under
				// every policy except an explicit DriftEvolve this change is refused —
				// for an operation that loses not one byte.
				Kind: schema.DropStream, Stream: string(streamOrders),
			}
		}},
		{Rows: 3, Stream: streamOrdersV2},
		{Rows: 2, Stream: streamCustomer},
	}
}

func cloneSchema(s *schema.Schema) *schema.Schema {
	if s == nil {
		return &schema.Schema{}
	}
	out := &schema.Schema{Open: s.Open}
	out.Fields = append(out.Fields, s.Fields...)
	for _, k := range s.Keys {
		out.Keys = append(out.Keys, append([]string(nil), k...))
	}
	return out
}

// ---------------------------------------------------------------------------------
// B1 (FATAL): a source cannot publish a schema, or a schema change, to the core.
//
// This is the finding. Everything else in this file is a cost; this one is a wall.
//
// WHAT THE CASE NEEDS: when the source parses `ALTER TABLE orders ADD currency` out of
// the log, it must hand the core (a) the resulting schema BODY, so the core's schema
// table can hold it and hand it to the sink and the codec, and (b) an ordered
// schema.Change, so the core can run the quiesce-flush-apply-checkpoint sequence
// before the next record of the new shape is admitted.
//
// WHAT THE INTERFACE OFFERS, exhaustively. I enumerated every method a source can
// reach and every field it can write:
//
//	Source.Open(ctx, rt SourceRuntime) error
//	Source.Read(ctx, dst *record.Batch) error
//	Source.Commit(ctx, a Ack) error
//	Source.Close(ctx) error
//	SourceRuntime: Context, Lanes, State, Log, Metrics, Batcher, Note, Tenant,
//	               Pipeline, Node
//	LaneCtl:       Announce, Finish, Assigned, Changes, Revoked, Budget
//	StateHandle:   Get, Set, SetMany, Delete
//	Batch:         Records, Lane, Position, EndOfLane, Add, Derive, Merge, Reset, …
//	Record:        Dest, EventTime, Payload, Meta, Change, Schema, SetHandle, MarkFailed
//	Discoverer.Discover(ctx) (Catalog, error)
//
// NOT ONE OF THEM ACCEPTS A *schema.Schema OR A schema.Change. Specifically:
//
//   - SourceRuntime has NO Schemas() SchemaLookup. CodecRuntime has exactly that
//     method, with exactly the right shape — Get(ctx, ref) and Register(ctx, s)
//     (connector/runtime.go:108-114) — and it is absent from SourceRuntime and from
//     SinkRuntime. architecture.md line 4818 even LISTS Schemas() among the reverse-
//     channel methods the runtimes provide. The code does not have it.
//
//   - There is no SchemaDeclarer, SchemaEmitter or equivalent optional interface. The
//     optional source interfaces are Discoverer, Nackable, BacklogReporter,
//     Heartbeater, StateAdopter, Validator, Prober, ChoiceProvider. None concerns
//     schema.
//
//   - SourceRuntime.Note(e Event) has EventSchemaChange in its vocabulary, which
//     reads like the intended channel. It is not one. Event is
//     {At, Kind, Severity, Stream, Lane, Message string, Detail string}. A
//     schema.Change is {Kind, Stream, Field []string, From *Field, To *Field, Epoch,
//     At} and the resulting schema is a whole *schema.Schema. Flattening those into
//     Message/Detail is byte-for-byte the mistake record/change.go's own doc comment
//     condemns: "Surveyed CDC inputs invent DIFFERENT op vocabularies and DIFFERENT
//     position keys and flatten both into metadata strings, so every CDC-aware sink
//     ends up special-casing every source". And Note is documented as "best-effort by
//     contract: it is the one thing on a runtime that cannot fail" — it returns
//     nothing. A dropped DDL is silent, unordered and unretryable. A channel that
//     cannot fail is the wrong channel for the one event that must not be lost.
//
//   - Record.Schema *schema.Ref lets the source ATTACH a reference. It does not let it
//     publish the body. A Ref is {Fingerprint [16]byte, Epoch uint64, Stream string};
//     the doc says the body "lives in the pipeline's schema table". The source can
//     call schema.Fingerprint(s) and mint a syntactically valid Ref — this file's
//     streamHist.ref does exactly that — and every consumer that tries to resolve it
//     gets nothing, because nothing ever wrote the body. That is worse than no
//     mechanism: it is a mechanism that compiles, runs, and produces dangling
//     references.
//
//   - Discoverer.Discover(ctx) (Catalog, error) is the ONLY method in the whole
//     connector surface that carries a *schema.Schema out of a source
//     (StreamDesc.Schema). It takes no runtime, no lane and no position; it is
//     documented as pre-run catalog enumeration; and it cannot be ordered against the
//     record stream, which is the entire requirement ("in-band ordered changes with a
//     schema-before-data rule", §14.1 rule 3). Polling Discover cannot say "this
//     change happened at LSN 4711, before the next record".
//
// THE CONSEQUENCE, and it is larger than this connector. The following core machinery
// has no producer and therefore cannot execute at all:
//
//	SinkCaps.SchemaChanges []schema.ChangeKind        — negotiated against nothing
//	SinkCaps.AppliesSchema / SchemaApplier            — never invoked
//	connector.FlushSchemaChange                       — never a reason
//	engine.Checkpoint.SchemaEpoch                     — never advances past zero
//	schema.Change, ChangeKind, Destructive()           — never constructed
//	spec.DriftPolicy and its five modes                — never consulted
//	SourceCaps.ProducesSchema                          — declarable, unimplementable
//	engine/negotiate.go driftKinds() + its diagnostic  — validates a dead path
//
// registry.AddSource will happily accept ProducesSchema: true, because it is
// documented as a "pure declaration with no interface behind it" (caps.go:94-98) and
// declaredPlain records it with unlocks "destination creation and drift handling".
// Nothing behind it can be reached. That is the single most misleading declaration in
// the capability set: it promises the case this file was written to exercise.
//
// MINIMAL FIX, additive, breaking no already-written connector. Two additions, both on
// interfaces the CORE implements, which connector/runtime.go:20-23 names as the
// sanctioned growth path precisely because growing them breaks nothing:
//
//	// on SourceRuntime — the same method CodecRuntime already has:
//	Schemas() SchemaLookup
//
//	// and one new method for the ORDERED half, also on SourceRuntime:
//	//
//	// Declare publishes the resulting schema and the change that produced it. It is
//	// legal ONLY from inside Read, between records, which is what makes the ordering
//	// in-band and the schema-before-data rule enforceable without a position field on
//	// schema.Change. The core registers the body, assigns the epoch (so the connector
//	// never mints one), returns the Ref for the source to stamp on subsequent
//	// records, and defers the quiesce to the batch boundary.
//	Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error)
//
// Nothing on Source, Sink, Buffer or Transform changes. No Caps field changes meaning.
// A connector that never calls Declare is unaffected. SourceCaps.ProducesSchema
// becomes an interface-backed flag (Declares/SchemaDeclarer) instead of a promise with
// no referent, which is a one-line addition to the AddSource cross-check table.
//
// A SECOND, SMALLER ADDITION worth making at the same time: schema.Change should carry
// the resulting schema, or a Ref to it, so a sink can act on a change without a second
// round trip. See B2 — that is the sink half of the same hole.
//
// PROOF THAT THIS IS THE GAP AND NOT MY MISREADING: the two interface assertions below
// compile today and are both statically unsatisfiable, because no type in pkg/connector
// has either method. When the core grows them, this connector starts working with no
// other edit. That is the test the repair should make pass.
// ---------------------------------------------------------------------------------

// proposedSchemaRuntime is the fix for B1, expressed as an interface this connector
// probes for. It is deliberately written as a structural assertion rather than as a
// wish in prose: the moment SourceRuntime grows these two methods, declareOrDegrade
// below takes the working path instead of the degraded one.
type proposedSchemaRuntime interface {
	Schemas() connector.SchemaLookup
	Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error)
}

// proposedSinkSchemaRuntime is the fix for B2, same technique.
type proposedSinkSchemaRuntime interface {
	Schemas() connector.SchemaLookup
}

// ---------------------------------------------------------------------------------
// The source.
// ---------------------------------------------------------------------------------

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*driftSource]{
		Meta: registry.Meta{
			Name:    "stress_schema_drift",
			Version: "1.0.0",
			Title:   "Schema-drift stress source",
			Summary: "A scripted CDC tail whose schema changes mid-stream.",
			Notes: "Origin.Key is the big-endian primary key prefixed by the ORIGIN stream name. " +
				"The origin-stream prefix is load-bearing and is the workaround for B3: " +
				"every record on this lane carries origin.Stream=\"changelog\", so the " +
				"engine's dedupe scope (tenant, pipeline, node, stream, layer, identity) " +
				"is identical for orders row 1 and customers row 1, and the key must " +
				"disambiguate what the scope no longer does.",
			Support: registry.SupportCommunity,
		},

		Spec: config.NewSpec().
			Field(config.Field{
				Name:        "rows_per_batch",
				Type:        config.TypeInt,
				Title:       "Rows per batch",
				Description: "How many rows one Read produces at most.",
				Default:     64,
				Optional:    true,
			}),

		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Unbounded},
			LaneKinds:       []connector.LaneKind{connector.LaneKindStream},
			MaxLanes:        1,

			// A replication slot: the upstream frees log when the source commits.
			UpstreamRetention: connector.PrunesOnCommit,
			UnitAssignment:    connector.UnitsStatic,

			Heartbeats:          true,
			ProducesEventTime:   true,
			ProducesChange:      true,
			CompleteImages:      true,
			ComparablePositions: true,
			Replayable:          true,
			StableKeys:          true,
			MidLaneResume:       true,

			// B1, REPAIRED. ProducesSchema was deliberately left FALSE, and that was the
			// finding stated as data: this source does nothing but produce schemas and could
			// not declare the capability honestly, because there was no interface behind the
			// flag. SourceRuntime.Declare is that interface, so the flag is now true and the
			// promise behind it is one the engine can keep.
			ProducesSchema: true,
		},

		New: func(_ context.Context, c *config.Config) (*driftSource, error) {
			s := &driftSource{
				rowsPerBatch: config.Must[int](c, "rows_per_batch"),
				prog:         program(),
			}
			if s.rowsPerBatch < 1 {
				s.rowsPerBatch = 64
			}
			return s, c.Err()
		},
	})
}

type driftSource struct {
	rowsPerBatch int
	prog         []driftStep

	rt   connector.SourceRuntime
	lane record.LaneID

	// mu guards cur, which Read mutates and which Commit and Heartbeat read. This is
	// the one mutex source.go's concurrency contract says a source needs: state shared
	// between the read path and the progress path.
	mu  sync.Mutex
	cur *cursor

	// rowsLeft tracks progress within the current program step.
	rowsLeft int
	nextID   int64

	// pending is the declaration this source could not make. See declareOrDegrade.
	pendingRefusal error
}

func (s *driftSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt

	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	switch {
	case len(as) == 0:
		// Cold start. ONE lane for every stream in the publication, because a slot has
		// one position. See B3.
		s.lane, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name:        laneChangelog,
			Stream:      laneChangelog,
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       "tail",
			Label:       "logical replication tail",
		})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.cur = newCursor()
		s.seedLocked()
		s.mu.Unlock()

	default:
		s.lane = as[0].ID
		// Warm start: the history comes back inside the cursor token, because that is
		// the only place it could have been written atomically with the position. B5.
		c, err := decodeToken(as[0].Cursor.Token)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.cur = c
		if len(c.Hist) == 0 {
			s.seedLocked()
		}
		s.nextID = int64(c.LSN)
		s.mu.Unlock()

		// THE RESTART QUESTION, ANSWERED CORRECTLY AND THEN THROWN AWAY. The source
		// now knows the schema as of the resume position for every stream. It has no
		// way to tell anybody. There is no SourceRuntime.Schemas().Register, so the
		// core's schema table is still empty, so the sink's Opening.Schemas — which
		// the core has ALREADY built and passed by the time Source.Open runs — cannot
		// have contained these bodies. Even with a perfect answer in hand the source
		// is mute. B1.
		s.reportHistory()
	}
	return nil
}

// seedLocked installs the baseline schemas as epoch 1 of each stream's history.
func (s *driftSource) seedLocked() {
	for stream, sch := range map[record.StreamName]*schema.Schema{
		streamOrders:   baselineOrders(),
		streamCustomer: baselineCustomers(),
	} {
		h := s.cur.hist(stream)
		if len(h.Entries) > 0 {
			continue
		}
		h.Entries = append(h.Entries, histEntry{
			Epoch:  1,
			Change: schema.Change{Kind: schema.CreateStream, Stream: string(stream), At: time.Now()},
			Result: sch,
			FP:     schema.Fingerprint(sch),
		})
	}
}

// reportHistory is the least-bad thing a source can do with a schema history it
// cannot publish: render it as human strings into the event ring, so that at least an
// operator staring at the UI can see what the pipeline does not know.
//
// This is the workaround, and it is exactly as useless as it looks. Note returns
// nothing, carries no structure, is best-effort by contract, and is read by no code
// path. A real connector author reaching this point either gives up on drift or
// forks the core.
func (s *driftSource) reportHistory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for stream, h := range s.cur.Hist {
		cur := h.current()
		if cur == nil {
			continue
		}
		blob, _ := json.Marshal(cur)
		s.rt.Note(connector.Event{
			At:      time.Now(),
			Kind:    connector.EventSchemaChange,
			Stream:  stream,
			Lane:    s.lane,
			Message: fmt.Sprintf("schema for %s restored at epoch %d, %d fields", stream, h.epoch(), len(cur.Fields)),
			Detail:  string(blob),
		})
	}
}

// declareOrDegrade is where B1 becomes executable rather than rhetorical.
//
// It tries the fix first: if the runtime it was handed satisfies
// proposedSchemaRuntime, it registers the body and declares the ordered change, and
// everything downstream works. Today no core type does, so it falls through to the
// degraded path: mint a Ref nobody can resolve, flatten the typed change into an event
// string, and record a refusal that the next Read returns as fault.PermanentContract.
//
// Returning a Contract is the honest outcome and it is worth being explicit about why.
// The case says one sink "needs DDL applied before the next record". A source that
// cannot cause the DDL to be applied has exactly two options: emit the next record
// anyway, which writes new-shape data into an old-shape table — silent corruption, the
// thing this whole architecture exists to prevent — or stop. Stopping is correct.
// So the truthful implementation of this connector against today's interfaces is a
// connector that halts at the first schema change. That is the measurement.
func (s *driftSource) declareOrDegrade(ctx context.Context, stream record.StreamName, ch schema.Change, result *schema.Schema) *schema.Ref {
	s.mu.Lock()
	h := s.cur.hist(stream)
	epoch := h.epoch() + 1
	ch.Epoch = epoch
	ch.At = time.Now()
	fp := schema.Fingerprint(result)
	h.Entries = append(h.Entries, histEntry{Epoch: epoch, Change: ch, Result: result, FP: fp})
	s.mu.Unlock()

	// THE WORKING PATH, NOW REACHED. This structural probe was written before the repair
	// existed, as an assertion rather than a wish: "the moment SourceRuntime grows these two
	// methods, declareOrDegrade takes the working path instead of the degraded one". It has.
	// SourceRuntime.Declare's signature is exactly proposedSchemaRuntime's, so this branch is
	// live and every line below it is now dead on the happy path — kept because a v1 core
	// talking to a v2 connector over the out-of-process seam can still land there.
	if pr, ok := s.rt.(proposedSchemaRuntime); ok {
		ref, err := pr.Declare(ctx, ch, result)
		if err == nil {
			return &ref
		}
		s.rt.Log().Error("schema declaration refused", "stream", stream, "epoch", epoch, "err", err)
	}

	// THE DEGRADED PATH. All three channels below are dead ends; each is here because
	// a connector author would try it, in this order, and find out.
	//
	// (a) Flatten the typed change into an Event. Unordered, undurable, unread.
	blob, _ := json.Marshal(ch)
	s.rt.Note(connector.Event{
		At:       ch.At,
		Kind:     connector.EventSchemaChange,
		Severity: fault.PermanentContract,
		Stream:   stream,
		Lane:     s.lane,
		Message: fmt.Sprintf("%s on %s at epoch %d cannot be declared: SourceRuntime has no schema channel",
			ch.Kind, stream, epoch),
		Detail: string(blob),
	})

	// (b) Mint the Ref anyway. Syntactically valid, semantically dangling: no body was
	// ever registered under this fingerprint, so CodecRuntime.Schemas().Get will not
	// find it and the sink's Opening.Schemas does not contain it.
	ref := &schema.Ref{Fingerprint: fp, Epoch: epoch, Stream: string(stream)}

	// (c) Record the refusal. Read returns it once the current batch has been handed
	// over, because source.go is explicit that a source must never discard records it
	// has already produced on an error path.
	if s.pendingRefusal == nil {
		s.pendingRefusal = fault.Contract(fault.OpRead, fmt.Errorf(
			"schema change %s on stream %q (epoch %d) cannot be published to the core: "+
				"SourceRuntime exposes no Schemas() and no Declare(), and connector.Event carries "+
				"only strings, so SchemaApplier.ApplySchemaChange can never be invoked and the next "+
				"record of the new shape would be written against the old destination shape",
			ch.Kind, stream, epoch))
	}
	return ref
}

func (s *driftSource) Read(ctx context.Context, dst *record.Batch) error {
	dst.Reset()
	dst.Lane = s.lane

	if s.rt.Lanes().Revoked(s.lane) {
		return fault.ErrFenced
	}

	for dst.Len() < s.rowsPerBatch {
		s.mu.Lock()
		step := s.cur.Step
		s.mu.Unlock()

		if step >= len(s.prog) {
			// The scripted program is finished. A tail lane is unbounded, so this is
			// "nothing available right now", not end of input. Hand over what we have.
			break
		}
		st := s.prog[step]

		// A DDL step. Declare it, then STOP THIS BATCH so that the position boundary
		// falls between the old shape and the new one. That batch boundary is the only
		// schema-before-data enforcement a source has, given B1: Batch.Position
		// attaches to a batch, and the engine's quiesce hooks off a schema change it
		// never receives.
		if st.DDL != nil {
			s.mu.Lock()
			cur := s.cur.hist(st.Stream).current()
			s.mu.Unlock()
			next, ch := st.DDL(cur)
			s.declareOrDegrade(ctx, st.Stream, ch, next)

			// A rename is DropStream+CreateStream because there is no RenameStream.
			// Emit the arrival half too, seeded with the departing shape. See B4.
			if ch.Kind == schema.DropStream && st.Stream == streamOrders {
				s.declareOrDegrade(ctx, streamOrdersV2, schema.Change{
					Kind:   schema.CreateStream,
					Stream: string(streamOrdersV2),
				}, next)
			}

			s.mu.Lock()
			s.cur.Step++
			s.rowsLeft = 0
			s.mu.Unlock()
			break
		}

		if s.rowsLeft == 0 {
			s.rowsLeft = st.Rows
		}
		r := dst.Add()
		if r == nil {
			break
		}
		s.emit(r, st.Stream)

		s.rowsLeft--
		s.mu.Lock()
		s.cur.LSN++
		if s.rowsLeft == 0 {
			s.cur.Step++
		}
		s.mu.Unlock()
	}

	// The position, including the whole schema history. B5.
	s.mu.Lock()
	tok, err := encodeToken(s.cur)
	lsn := s.cur.LSN
	s.mu.Unlock()
	if err != nil {
		return err
	}

	var ord [8]byte
	binary.BigEndian.PutUint64(ord[:], lsn)
	scalar := float64(lsn)
	dst.Position = record.Position{
		Token:  tok,
		Order:  ord[:],
		Scalar: &scalar,
		Safe:   true, // every row boundary in this synthetic log is a commit boundary
		At:     time.Now(),
		Label:  fmt.Sprintf("lsn %d", lsn),
	}

	// The engine admits what is in the batch BEFORE handling the error, so returning
	// the refusal here does not lose the records already produced.
	if s.pendingRefusal != nil {
		err := s.pendingRefusal
		s.pendingRefusal = nil
		return err
	}
	if dst.Len() == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// ---------------------------------------------------------------------------------
// B3 (MAJOR): one lane cannot serve more than one stream.
//
// A logical-replication slot, a MySQL binlog, a Kinesis shard carrying several entity
// types, an S3 prefix of mixed files: one PROGRESS DOMAIN, many LOGICAL STREAMS. That
// is the normal shape of every CDC source in existence, and this case forces it,
// because a table rename plus per-table drift only makes sense across at least two
// tables sharing one log.
//
// THE BLOCKING SIGNATURES:
//
//	record.NewAllocator(t, p, n, l, stream StreamName, firstID, firstGroup) *Allocator
//	func (b *Batch) Add() *Record        // stamps Origin{… Stream: a.stream …}
//	func (o Origin) …                    // no exported mutator, by design
//
// Origin.Stream is stamped from the Allocator, and the engine creates one Allocator
// per (lane, generation). So EVERY record on one lane has the SAME origin.Stream, and
// there is no per-record setter — correctly so, since Origin's unforgeability is the
// point. The only writable field is Record.Dest, which this connector duly uses below
// and which does route correctly: Record.Ref() reports Dest, Opening.Streams is keyed
// by stream name, and the sink writes where it is told. Routing is fine.
//
// WHAT IS NOT FINE IS EVERYTHING KEYED ON origin.Stream:
//
//  1. THE DEDUPE SCOPE. spec.DedupeConfig documents the durable key as
//     (tenant, pipeline, source-node, stream, layer, identity), and documents WHY:
//     "an earlier attempt keyed on the bare event id, so two connectors or two tenants
//     emitting 1, 2, 3 silently discarded each other's events (design rule R5)". With
//     one lane over N tables, `stream` is the constant "changelog" for all of them, so
//     orders id=1 and customers id=1 land on the SAME dedupe key and the second one is
//     silently discarded. R5's bug, reintroduced, in the mechanism built to prevent it.
//     This connector's only defence is to prefix Origin.Key with the origin stream name
//     by hand — which is why this source's registration Notes say so, and which every
//     multi-stream source author must independently rediscover.
//
//  2. Per-stream metrics and the read model. The core labels by origin.Stream, so a
//     40-table publication reports one series called "changelog".
//
//  3. fault.Fault.Stream and connector.Event.Stream. A mapping failure on a customers
//     row is reported against "changelog". An operator cannot tell which table broke.
//
// THE ALTERNATIVE IS WORSE. Announce one lane per table over one physical log, and
// each lane gets its own independent cursor over the same position space; the source
// must then re-read the whole log per lane and filter, N times, and N cursors over one
// log can commit at N different points, so the slot can only be advanced to the
// minimum — which is correct but means the interface is now fighting the upstream, the
// exact failure UnitsExternal exists to avoid one level up.
//
// MINIMAL FIX, purely additive, breaks no written connector: one method on Batch.
//
//	// AddFor lends a record slot whose Origin.Stream is stream rather than the
//	// lane's announced stream. For a source whose one progress domain carries several
//	// logical streams. Dest starts equal to it, exactly as Add does.
//	func (b *Batch) AddFor(stream record.StreamName) *Record
//
// Origin stays unforgeable — the Allocator still stamps it, inside package record, and
// there is still no exported mutator. Add() keeps its exact behaviour, so every
// existing source is untouched. LaneSpec.Stream becomes the lane's DEFAULT stream
// rather than its only one, which is a doc change, not a contract change.
// ---------------------------------------------------------------------------------

// emit fills one record for the given logical stream.
func (s *driftSource) emit(r *record.Record, stream record.StreamName) {
	s.mu.Lock()
	h := s.cur.hist(stream)
	sch := h.current()
	ref := h.ref(stream)
	lsn := s.cur.LSN
	s.mu.Unlock()

	s.nextID++
	id := s.nextID

	// B3: origin.Stream is already "changelog" and cannot be changed. Dest is the only
	// lever.
	r.Dest = stream

	var idb [8]byte
	binary.BigEndian.PutUint64(idb[:], uint64(id))

	// B7 (FATAL), REPAIRED: record.Record.SetKey is exactly the line that belonged here.
	// Stream-qualified, because a drifting source whose streams are renamed at runtime must
	// not let a renamed stream's ids collide with the old stream's in one dedupe namespace.
	r.SetKey(append(append([]byte(stream), 0), idb[:]...))

	r.EventTime = time.Now()

	row := record.Map{
		"id": record.Int(id),
	}
	if sch != nil {
		for _, f := range sch.Fields {
			switch f.Name {
			case "id":
			case "amount":
				// A widened decimal: the same logical value, re-scaled to the current
				// schema's scale. Meta.NoteChange is the right home for the fidelity
				// statement and it fits perfectly.
				scale := int32(2)
				if f.Logical != nil {
					scale = int32(f.Logical.Scale)
				}
				row["amount"] = record.Decimal{Unscaled: idb[:], Scale: scale}
			case "note":
				row["note"] = record.String(fmt.Sprintf("note for %d", id))
			case "currency":
				row["currency"] = record.String("EUR")
			case "email":
				row["email"] = record.String(fmt.Sprintf("user%d@example.test", id))
			case "attr":
				row["attr"] = s.unionValue(id)
			default:
				row[f.Name] = record.Null{}
			}
		}
	}

	r.Payload = record.StructPayload(row)
	r.Schema = ref // dangling unless B1 is fixed
	r.Change = &record.Change{
		Version:        record.ChangeVersion,
		Op:             record.OpInsert,
		Keys:           [][]string{{"id"}},
		After:          payloadPtr(row),
		BeforeComplete: record.CompletenessAbsent,
		AfterComplete:  record.CompletenessComplete,
		TxID:           fmt.Sprintf("tx-%d", lsn/4),
		CommitTime:     time.Now(),
	}

	// The one thing the interface gets exactly right about lossy shape changes: a
	// per-field, countable, operator-visible note, on the record, in a closed
	// vocabulary. Nothing about this needed working around.
	_ = r.Meta.Set(record.NSSource, "origin_stream", record.String(string(stream)))
	if stream == streamOrdersV2 {
		_ = r.Meta.Set(record.NSSource, "renamed_from", record.String(string(streamOrders)))
	}
}

// unionValue is the per-record union field: a string, an int, or a nested map,
// varying by record. See B6.
func (s *driftSource) unionValue(id int64) record.Value {
	switch id % 3 {
	case 0:
		return record.String("tag")
	case 1:
		return record.Int(id * 7)
	default:
		return record.Map{"nested": record.String("v")}
	}
}

func payloadPtr(v record.Value) *record.Payload {
	p := record.StructPayload(v)
	return &p
}

// ---------------------------------------------------------------------------------
// B7 (FATAL): Origin.Key and Origin.Upstream cannot be set by any connector.
//
// This one is not specific to schema drift. I hit it while working around B3, and it
// is the cleanest compiler-verifiable defect in the whole set: the line
//
//	r.SetKey(k)      // or  r.SetUpstream(u)
//
// does not compile, and there is no spelling of it that does.
//
// THE BLOCKING SIGNATURES, exhaustively. Package record's entire mutation surface on a
// record and a batch is:
//
//	func (r *Record) Origin() Origin              // BY VALUE — mutating it mutates a copy
//	func (r *Record) SetHandle(h []byte)
//	func (r *Record) MarkFailed(err error)
//	func (r *Record) Ref() Ref
//	func (b *Batch) Add() *Record                 // stamps Tenant, Pipeline, Node, Lane,
//	                                              // Stream, Group, ID, Root, ReadAt, refs
//	func (b *Batch) Derive(in *Record) *Record    // copies in.origin, including Key
//	func (b *Batch) Merge(parents ...*Record) *Record
//	func (b *Batch) SetSeq(uint64)
//	func (b *Batch) SetGroup(GroupID)
//
// Origin.Key and Origin.Upstream are exported FIELDS on a struct returned BY VALUE
// from an accessor, on a struct stored in an UNEXPORTED field, in a package whose doc
// says "there is no exported mutator". Add() does not set them. Derive() propagates
// whatever was never there. `o := r.Origin(); o.Key = k` compiles and does nothing —
// which is worse than not compiling, because it is the first thing an author will try
// and it will pass their unit test if they assert on `o` rather than on `r`.
//
// WHAT DEPENDS ON A FIELD NO CONNECTOR CAN WRITE:
//
//	SourceCaps.StableKeys           documented as "Origin.Key is populated and stable"
//	                                — and registry.AddSource PANICS if you declare it
//	                                with empty Notes, so the registry enforces
//	                                documentation for a field the source cannot fill
//	SinkCaps.RequiresKey            "every record must carry Origin.Key" — never
//	                                satisfiable, so negotiate.go refuses every pipeline
//	                                with such a sink, forever
//	Request.IdempotencyKey          "present when the source declares StableKeys"
//	spec.DedupeLayer DedupeKey      keys on Origin.Key — always empty
//	spec.DedupeLayer DedupeUpstream keys on Origin.Upstream — always empty
//	Guarantee EffectivelyOnce       requires StableKeys; unreachable
//	record.Ref.Key                  always nil at every sink
//	Origin doc: "A source with no natural upstream id MUST derive a deterministic one
//	from stable fields and document the derivation in its registered Notes (R5)"
//	— an obligation placed on the connector, with no method to discharge it.
//
// The one in-tree source, internal/example/linefile, declares StableKeys: false and
// its Notes say "Origin.Key is not populated: a text line has no stable identity". So
// the single worked example is the single case that does not need the missing method,
// which is exactly how a hole this size stays invisible.
//
// MINIMAL FIX, additive, breaks nothing:
//
//	// SetKey attaches the source-derived stable identity. Legal only from the source
//	// that produced the record, before it returns from Read — the same restriction
//	// SetHandle already documents and the engine already enforces.
//	func (r *Record) SetKey(k []byte)
//	func (r *Record) SetUpstream(u []byte)
//
// Both live in package record, so the unexported-origin invariant is untouched: the
// fields settlement uses (Lane, Group, ID, Root, refs) remain unreachable to a
// transform, which is Origin's whole purpose. Key and Upstream are not settlement
// identity — Origin's own doc calls Key "source-derived" and Upstream "the vendor's
// own id" — so exposing them does not weaken the property the encapsulation protects.
//
// Alternative if per-record mutation is unwanted: take them as parameters,
// `func (b *Batch) AddKeyed(key, upstream []byte) *Record`. This composes with B3's
// proposed AddFor, and both could be one variadic-option Add. I would rather have two
// plain setters: a source computes the key from the row it just decoded, which is
// after Add() has already returned.
// ---------------------------------------------------------------------------------

// Commit advances the replication slot. UpstreamRetention is PrunesOnCommit, so the
// core has already durably flushed its own record of the position before this runs —
// which is the one ordering guarantee this connector needs and gets for free.
func (s *driftSource) Commit(ctx context.Context, a Ack) error {
	if a.Abandoned > 0 {
		// A destructive commit with abandoned records: refuse to advance, per Ack's
		// documented contract that the core surfaces the number and the source chooses.
		s.rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventDegraded,
			Severity: fault.PermanentContract,
			Lane:     a.Lane,
			Message: fmt.Sprintf("refusing to advance the slot: %d of %d records reached a terminal disposition",
				a.Abandoned, a.Records),
		})
		return nil
	}
	// Nothing else to do: the position IS canal's cursor and the history rode with it.
	_ = ctx
	return nil
}

// Ack is aliased so the method set above reads at the width of the rest of the file.
type Ack = connector.Ack

// Heartbeat keeps the slot from pinning the upstream's log while the program is idle.
// Required here because UpstreamRetention is PrunesOnCommit.
func (s *driftSource) Heartbeat(_ context.Context, lane record.LaneID, idle time.Duration) error {
	s.rt.Log().Debug("heartbeat", "lane", lane, "idle", idle)
	return nil
}

func (s *driftSource) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------------
// B2 (FATAL): a sink cannot resolve a schema.Ref it did not receive at Open, and a
// schema.Change does not carry enough to act on.
//
// This is the sink half of B1 and it is independently fatal: even if the source could
// declare changes, the sink that must apply DDL still cannot.
//
// THE CASE: sink one "needs DDL applied before the next record". Concretely it must
// issue ALTER TABLE orders ADD currency VARCHAR, and after the rename, CREATE TABLE
// orders_v2 (…all columns…). What is it given?
//
//	SchemaApplier.ApplySchemaChange(ctx context.Context, ch schema.Change) error
//
//	schema.Change{Kind, Stream string, Field []string, From *Field, To *Field,
//	              Epoch uint64, At time.Time}
//
//	SinkRuntime: Context, Log, Metrics, Note, Tenant, Pipeline, Node
//
//	Opening{Restored *uint64, Schemas []schema.Entry, Streams []ConfiguredStream,
//	        Guarantee}
//
// THREE SEPARATE FAILURES:
//
//  1. CreateStream CANNOT BE APPLIED. schema.Change has no *schema.Schema and no Ref
//     to one. For AddField, From/To are enough — one column, one type. For
//     CreateStream, Field is documented as "nil for stream-level kinds" and From/To are
//     a single *Field each, so the change names a table and says nothing about its
//     columns. A sink handed CreateStream can create a table with no columns or
//     refuse. This connector refuses, loudly, in ApplySchemaChange below, and
//     registry.AddSink FORCES the contradiction into the open: it panics unless
//     AppliesSchema declares a non-empty SchemaChanges list, so the sink must claim it
//     can perform CreateStream in order to be asked, and then cannot.
//
//  2. THE SINK HAS NO SchemaLookup. CodecRuntime.Schemas() exists and is exactly the
//     needed handle. SinkRuntime does not have it. So the sink cannot resolve
//     ch.Epoch, or Request.Schema, or anything else, to a body. Its ONLY source of
//     bodies is Opening.Schemas, a slice fixed at Open. Everything minted afterwards is
//     unresolvable, permanently.
//
//  3. Request.Schema BECOMES A DANGLING POINTER MID-RUN. Request.Schema is a
//     *schema.Ref and the engine guarantees one epoch per request — genuinely useful,
//     and the sink below uses it to detect that it is behind. But a ref whose epoch is
//     past Open is a fingerprint the sink cannot look up. The correct behaviour for a
//     sink that writes typed columns is then to fail EVERY record of the new epoch,
//     because it cannot know whether the shape is compatible. That converts the DLQ
//     sink's requirement — "reject and DLQ the INCOMPATIBLE records" — into "DLQ
//     everything after the first drift". Per-record precision is lost to a missing
//     lookup, not to anything about the records.
//
// MINIMAL FIX, additive, breaks nothing:
//
//	// on SinkRuntime — the method CodecRuntime already has:
//	Schemas() SchemaLookup
//
//	// and one field on schema.Change:
//	//
//	// Result is the schema in force AFTER this change. Non-nil for every kind; it is
//	// what makes CreateStream applicable and what saves every other kind a lookup.
//	Result *Schema `json:"result,omitempty"`
//
// The schema.Change addition is a new field on a struct, which record/doc.go names as
// the one source-compatible growth move ("new capabilities are added as FIELDS ON CORE
// STRUCTS"). Nothing that constructs a Change today breaks; nothing that reads one
// today breaks. SinkRuntime is core-implemented, so adding a method breaks no sink.
//
// A THIRD ADDITION IS NEEDED FOR THE RENAME, and it belongs to B4: ApplySchemaChange
// tells the sink a stream exists but never how to write it. Opening.ConfiguredStream
// carries Mode and Keys; schema.Change carries neither, and there is no runtime
// equivalent of Opening.Streams. Smallest fix: give schema.Change an optional
//
//	Configured *connector.ConfiguredStream
//
// — except that would make package schema import package connector, which it must not.
// So the field belongs on the METHOD instead:
//
//	ApplySchemaChange(ctx context.Context, ch schema.Change, cs []ConfiguredStream) error
//
// That IS a breaking change to an optional interface, which is why the preferable
// shape is a second, additive optional interface the core prefers when present:
//
//	type SchemaApplier2 interface {
//	    ApplySchemaChange2(ctx context.Context, req SchemaChangeRequest) error
//	}
//	type SchemaChangeRequest struct {
//	    Change     schema.Change
//	    Result     *schema.Schema
//	    Configured []ConfiguredStream
//	}
//
// resolved into its own nilable field on ResolvedSink beside SchemaApp, with the
// engine preferring SchemaApplier2 when both are present. Already-written sinks
// implementing SchemaApplier keep working unchanged. Given that no sink can currently
// have been written against SchemaApplier usefully — B1 means it is never called —
// changing SchemaApplier in place is also defensible right now, and is cheaper. That
// window closes the moment a third party ships one.
// ---------------------------------------------------------------------------------

// ddlSink is the sink that must have DDL applied before the next record.
type ddlSink struct {
	rt connector.SinkRuntime

	mu sync.Mutex

	// known is the body per stream, seeded from Opening.Schemas and — in a fixed
	// world — extended by ApplySchemaChange.
	known map[record.StreamName]*schema.Schema

	// appliedEpoch is the highest schema epoch whose DDL this sink has executed, per
	// stream. Comparing it against Request.Schema.Epoch is how "DDL before the next
	// record" is enforced, and this part of the interface set works.
	appliedEpoch map[record.StreamName]uint64

	// configured is Opening.Streams, indexed. It cannot grow. See B4.
	configured map[record.StreamName]connector.ConfiguredStream

	pending int64
}

func init() {
	registry.AddSink(registry.Default, registry.SinkDef[*ddlSink]{
		Meta: registry.Meta{
			Name:    "stress_ddl_sink",
			Version: "1.0.0",
			Title:   "DDL-first sink",
			Summary: "A typed destination that must have DDL applied before the next record of a new epoch.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 1,
			Modes:          []connector.DestMode{connector.DestUpsert, connector.DestAppend},
			Idempotent:     true,
			PartialFailure: true,

			RequiresCompleteImages: true,

			// B7: RequiresKey CANNOT be declared, even though this is an upserting
			// destination that genuinely requires one, because no source can populate
			// Origin.Key and negotiate.go would refuse every pipeline built on this
			// sink. The honest declaration is unusable, so the declaration is a lie by
			// omission. That is the second-order cost of B7.
			RequiresKey: false,

			Flushes:       true,
			AppliesSchema: true,
			Prepares:      true,

			// registry.AddSink panics on AppliesSchema with an empty list, so this sink
			// is REQUIRED to claim CreateStream in order to be offered the change — and
			// per B2.1 it cannot actually perform one, because schema.Change carries no
			// column list. The registry enforces a promise the interface cannot keep.
			SchemaChanges: []schema.ChangeKind{
				schema.CreateStream, schema.AddField, schema.DropField,
				schema.RenameField, schema.AlterFieldType, schema.AlterNullability,
			},
		},
		New: func(context.Context, *config.Config) (*ddlSink, error) {
			return &ddlSink{
				known:        map[record.StreamName]*schema.Schema{},
				appliedEpoch: map[record.StreamName]uint64{},
				configured:   map[record.StreamName]connector.ConfiguredStream{},
			}, nil
		},
	})
}

func (k *ddlSink) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	k.rt = rt
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, e := range o.Schemas {
		k.known[record.StreamName(e.Ref.Stream)] = e.Schema
		if e.Ref.Epoch > k.appliedEpoch[record.StreamName(e.Ref.Stream)] {
			k.appliedEpoch[record.StreamName(e.Ref.Stream)] = e.Ref.Epoch
		}
	}
	for _, cs := range o.Streams {
		k.configured[cs.Stream] = cs
	}

	// B2.2 probe. If SinkRuntime ever grows Schemas(), this sink can resolve refs
	// minted after Open and everything below takes the precise path instead of the
	// blunt one. Statically unsatisfiable today.
	if _, ok := rt.(proposedSinkSchemaRuntime); !ok {
		rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventDegraded,
			Severity: fault.PermanentContract,
			Message: "SinkRuntime exposes no Schemas(): every schema minted after Open is " +
				"unresolvable, so this sink must refuse whole epochs rather than individual records",
		})
	}

	// Opening.Guarantee is worth asserting on, and this is one of the places the
	// interface set is exactly right: a sink requiring upsert semantics can refuse at
	// Open rather than writing wrong data.
	if o.Guarantee < connector.AtLeastOnce {
		return fault.Contract(fault.OpOpen, fmt.Errorf(
			"this destination cannot run at %s: a lost in-flight window would leave the "+
				"destination shape ahead of its data", o.Guarantee))
	}
	return nil
}

func (k *ddlSink) Prepare(_ context.Context, streams []connector.ConfiguredStream, ss []schema.Entry) error {
	// Preparer fits cleanly for the pre-flow case and does everything it claims. It
	// simply has no runtime counterpart, which is the whole of B4's second half: a
	// stream that appears mid-run never reaches here.
	k.rt.Log().Info("preparing destination", "streams", len(streams), "schemas", len(ss))
	return nil
}

// ApplySchemaChange is where B2.1 and B4 become executable.
func (k *ddlSink) ApplySchemaChange(_ context.Context, ch schema.Change) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	stream := record.StreamName(ch.Stream)

	switch ch.Kind {
	case schema.AddField, schema.AlterNullability:
		if ch.To == nil {
			return fault.Contract(fault.OpSchemaApply,
				fmt.Errorf("%s on %q carries no To field", ch.Kind, ch.Stream))
		}
		// This works. One column, one type, enough to write the DDL.
		k.applyFieldLocked(stream, ch)

	case schema.AlterFieldType, schema.RenameField, schema.DropField:
		if ch.From == nil {
			return fault.Contract(fault.OpSchemaApply,
				fmt.Errorf("%s on %q carries no From field", ch.Kind, ch.Stream))
		}
		k.applyFieldLocked(stream, ch)

	case schema.CreateStream:
		// B2.1, EXECUTABLE. The sink was told to create a table and given no columns.
		return fault.Contract(fault.OpSchemaApply, fmt.Errorf(
			"cannot apply %s for stream %q at epoch %d: schema.Change carries no *schema.Schema "+
				"and SinkRuntime exposes no Schemas() to resolve one, so this sink cannot know "+
				"the column list it is being asked to create. Add schema.Change.Result, or "+
				"SinkRuntime.Schemas()",
			ch.Kind, ch.Stream, ch.Epoch))

	case schema.DropStream:
		// B4, EXECUTABLE. This arrives as half of a rename this sink cannot recognise
		// as one, because there is no schema.RenameStream. Dropping the table would
		// destroy the data the rename was preserving.
		return fault.Contract(fault.OpSchemaApply, fmt.Errorf(
			"refusing %s for stream %q at epoch %d: schema.ChangeKind has no RenameStream, so a "+
				"rename is indistinguishable from a drop at this interface, and applying it "+
				"would delete the renamed table's data. Add schema.RenameStream",
			ch.Kind, ch.Stream, ch.Epoch))

	default:
		return fault.Contract(fault.OpSchemaApply,
			fmt.Errorf("unsupported change kind %s", ch.Kind))
	}

	if ch.Epoch > k.appliedEpoch[stream] {
		k.appliedEpoch[stream] = ch.Epoch
	}
	return nil
}

func (k *ddlSink) applyFieldLocked(stream record.StreamName, ch schema.Change) {
	cur := k.known[stream]
	if cur == nil {
		cur = &schema.Schema{}
	}
	next := cloneSchema(cur)
	switch ch.Kind {
	case schema.AddField:
		next.Fields = append(next.Fields, *ch.To)
	case schema.DropField:
		out := next.Fields[:0]
		for _, f := range next.Fields {
			if f.Name != ch.From.Name {
				out = append(out, f)
			}
		}
		next.Fields = out
	default:
		for i := range next.Fields {
			if next.Fields[i].Name == ch.From.Name && ch.To != nil {
				next.Fields[i] = *ch.To
			}
		}
	}
	k.known[stream] = next
}

// Write enforces schema-before-data from the sink side. This part of the interface set
// works and is worth naming as a fit: Request.Schema.Epoch plus a locally tracked
// applied epoch is exactly enough for a sink to refuse data it is not shaped for,
// without the sink ever seeing a position or a cursor.
func (k *ddlSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.AllWritten(0), nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// Every record in a request shares a Dest, because the engine batches per stream —
	// but the sink cannot rely on that from the interface alone, so it checks.
	stream := req.Records[0].Stream
	for i := range req.Records {
		if req.Records[i].Stream != stream {
			return connector.WriteResult{}, fault.Bug(fault.OpWrite,
				fmt.Errorf("request mixes streams %q and %q", stream, req.Records[i].Stream))
		}
	}

	if req.Schema == nil {
		return connector.WriteResult{}, fault.Contract(fault.OpWrite, fmt.Errorf(
			"stream %q: no schema on the request, and this destination writes typed columns", stream))
	}
	if req.Schema.Epoch > k.appliedEpoch[stream] {
		// THE CASE'S CENTRAL REQUIREMENT, FAILING. The records are of a newer shape
		// than the DDL this sink has executed. Because of B1 the change was never
		// declared, so ApplySchemaChange was never called, so appliedEpoch never
		// advanced, so this branch is taken on the first record after the first drift
		// and the pipeline stops. That is the correct behaviour and the wrong outcome.
		return connector.WriteResult{}, fault.Contract(fault.OpWrite, fmt.Errorf(
			"stream %q: records are at schema epoch %d but this destination has DDL applied only "+
				"through epoch %d; writing them would put new-shape data into an old-shape table",
			stream, req.Schema.Epoch, k.appliedEpoch[stream]))
	}

	if _, ok := k.configured[stream]; !ok {
		// B4, EXECUTABLE from the write side. A stream that appeared after Open — the
		// rename target — has no ConfiguredStream, so this sink does not know whether
		// to append or upsert it, or on which keys.
		return connector.WriteResult{}, fault.Contract(fault.OpWrite, fmt.Errorf(
			"stream %q was not in Opening.Streams and there is no runtime channel that carries a "+
				"connector.ConfiguredStream, so this sink knows neither the DestMode nor the Keys "+
				"for it; a stream that appears mid-run is unwritable",
			stream))
	}

	k.pending += int64(req.Count)
	// Flusher is declared, so the core settles on Flush and not here. Returning clean
	// from Write does NOT claim durability for a Flusher sink, and that split is one of
	// the cleanest things in the interface set: weakening durability required
	// implementing a visible interface rather than being sloppy in prose.
	return connector.WriteResult{Written: int64(req.Count), Bytes: int64(len(req.Body))}, nil
}

func (k *ddlSink) Flush(_ context.Context, reason connector.FlushReason) (connector.WriteResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := k.pending
	k.pending = 0
	if reason == connector.FlushSchemaChange {
		// The quiesce. This hook is right, and it is dead: FlushSchemaChange can only
		// be produced by a core that received a schema.Change, which per B1 it cannot.
		k.rt.Log().Info("quiescing for a schema change", "records", n)
	}
	return connector.WriteResult{Written: n}, nil
}

func (k *ddlSink) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------------
// absorbSink: the sink that can absorb anything.
//
// THIS ONE FITS CLEANLY AND IT MATTERS THAT IT DOES. A schemaless destination is three
// methods, one Caps struct, no optional interfaces, and it is COMPLETELY UNAFFECTED by
// every finding in this file. It never asks for a schema, so B2 cannot touch it; it
// declares no AppliesSchema, so the engine never quiesces on its behalf; it needs no
// key, so B7 does not reach it. The declarative-capability design is doing exactly what
// it claims here: the easy case pays nothing for the hard case's machinery.
//
// The one wrinkle is not a defect: under a pipeline-wide spec.DriftPolicy of
// DriftLenient, this sink's records get an AlterFieldType rewritten into
// RenameField+AddField on behalf of a DIFFERENT sink in the same fan-out, so a
// destination that would have absorbed the widened column happily receives both the old
// and the new one forever. spec.Spec documents Retry and WhenFull as node-overridable
// and Drift as pipeline-wide, so there is no per-node escape. It is a real sharp edge
// and the smallest fix is a per-sink-node drift override in the stage-standard field
// set; it is not worth a core change on its own, and three pipelines is a legitimate
// answer.
// ---------------------------------------------------------------------------------

type absorbSink struct {
	rt connector.SinkRuntime
	n  int64
}

func init() {
	registry.AddSink(registry.Default, registry.SinkDef[*absorbSink]{
		Meta: registry.Meta{
			Name:    "stress_absorb_sink",
			Version: "1.0.0",
			Title:   "Absorb-anything sink",
			Summary: "A schemaless destination that accepts any shape.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 4,
			Modes:          []connector.DestMode{connector.DestAppend, connector.DestUpsert, connector.DestOverwrite},
			Idempotent:     true,
			PartialFailure: false,
		},
		New: func(context.Context, *config.Config) (*absorbSink, error) { return &absorbSink{}, nil },
	})
}

func (k *absorbSink) Open(_ context.Context, rt connector.SinkRuntime, _ connector.Opening) error {
	k.rt = rt
	return nil
}

func (k *absorbSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	k.n += int64(req.Count)
	return connector.WriteResult{Written: int64(req.Count), Bytes: int64(len(req.Body))}, nil
}

func (k *absorbSink) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------------
// B4 (MAJOR): a table rename is not expressible, and a stream that appears mid-run is
// not writable.
//
// THE VOCABULARY. schema.ChangeKind is CreateStream, AddField, DropField, RenameField,
// AlterFieldType, AlterNullability, AlterKeys, TruncateStream, DropStream. There is
// RenameField for a column and NOTHING for a table. The only encoding of
// `ALTER TABLE orders RENAME TO orders_v2` is DropStream(orders) +
// CreateStream(orders_v2), and:
//
//	func (k ChangeKind) Destructive() bool {
//	    switch k {
//	    case DropField, AlterFieldType, TruncateStream, DropStream:
//	        return true
//
// so the departure half is destructive. spec.DriftPolicy.Permits returns false for a
// destructive kind under DriftLenient (the DEFAULT), DriftTryEvolve, DriftIgnore and
// DriftFail. Only an explicit DriftEvolve permits it. So the default policy refuses a
// change that loses not one byte of data, and the policy that permits it also permits
// genuine table drops. An operator who wants renames to work must opt into destructive
// drops — the exact coupling §14.1 rule 5 says must be opt-in and separate.
//
// Worse, a sink that DOES receive DropStream cannot tell a rename from a drop, so the
// correct implementation is to refuse (ddlSink.ApplySchemaChange does). A rename is
// therefore refused under every policy: refused by the policy under four of five modes,
// and refused by any careful sink under the fifth.
//
// THE SECOND HALF. Even granting the rename, the arrival stream has no write
// configuration. connector.ConfiguredStream{Stream, Mode, Keys} is the operator's
// choice and reaches a sink ONLY through Opening.Streams and Preparer.Prepare, both
// pre-flow. There is no runtime channel for one. A sink handed a record whose Dest is
// orders_v2 does not know whether to append or upsert it, and has no keys — so it must
// refuse, which ddlSink.Write does. The same gap blocks the ordinary case of a NEW table
// appearing in a publication, which is far more common than a rename.
//
// MINIMAL FIX, additive on both counts:
//
//	// package schema, one new member APPENDED to the iota block so no existing
//	// constant's value changes:
//	RenameStream                       // after DropStream
//	// and it is NOT listed in Destructive(), because it is not.
//	// Change.From/To are already *Field and unusable here, so the stream names ride on
//	// Change.Stream (the departure) plus a new
//	ToStream string `json:"to_stream,omitempty"`
//
//	// package connector, on Opening's runtime counterpart — see B2's SchemaApplier2:
//	SchemaChangeRequest.Configured []ConfiguredStream
//
// Appending an enum member is safe here BECAUSE the vocabulary is versioned by
// Caps.APIVersion, which caps.go and architecture.md line 5841 both name as the
// versioning mechanism for exactly these closed iota enums. A sink built against the
// older APIVersion simply never declares RenameStream in SchemaChanges and the drift
// policy's kinds-subset check refuses the pipeline at submit time with a diagnostic —
// which is the designed behaviour, not a break.
//
// WHAT DOES WORK, and it is worth recording: rerouting the DATA for a renamed table
// needs nothing new. Record.Dest is writable by the source, Record.Ref() reports Dest,
// and the engine batches per Dest. The rename's data path is free. It is only the DDL
// and the write configuration that have no expression.
// ---------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------
// B6 (MINOR): a per-record union type has no honest expression.
//
// The case requires one field whose type varies per record: string, int64, or a nested
// map. schema.Type is a CLOSED set — Bool, Int64, Uint64, Float64, String, Bytes,
// Timestamp, Date, Time, Decimal, Struct, List, Map, Any — with no variant or union
// member, and schema.Logical carries Precision, Scale, TimeUnit, TimeZone and Name.
//
// The two expressible encodings:
//
//  1. TypeAny. Documented as "the explicit escape hatch for a source that genuinely
//     cannot report a type… a sink may refuse it". It works, and it is what this
//     connector uses. What it loses is the VARIANT SET: "string or int64" and "anything
//     at all" become the same declaration, so a sink cannot validate a record against
//     the permitted arms, and negotiate cannot refuse at submit time a sink that could
//     have handled string|int64 but not map. Rejection moves from Build to runtime,
//     which is the one thing the negotiation design exists to prevent.
//
//  2. TypeAny plus Logical.Name: "union<string,int64,map>". This is precisely the
//     "magic name string plus a parameters map" convention schema/doc.go names as
//     Kafka Connect's "documented source of silent disagreement between bad connectors
//     and bad converters". The interface's own doc forbids the only encoding that
//     carries the information.
//
// There is also a throughput cliff behind the alternative nobody should take: modelling
// each variant as its own schema gives each record a distinct fingerprint, and
// Request.Schema plus "the engine never mixes epochs in one request" then caps every
// request at one record.
//
// MINIMAL FIX, additive, breaks nothing, and it needs no new Type member:
//
//	// on schema.Field:
//	// Variants, when non-empty, declares the permitted alternatives for a TypeAny
//	// field. A sink may refuse a variant set it cannot represent; a record's actual
//	// arm is identified by its record.Value Kind.
//	Variants []Field `json:"variants,omitempty"`
//
// Type stays closed and stays a metric label (TypeAny is still the label). Fingerprint
// gains one clause in normaliseField. A source that does not set Variants is unchanged,
// and a sink that ignores them behaves exactly as today. This is the same move Logical
// already makes for parameterised detail, applied to alternation.
//
// RATED MINOR because TypeAny genuinely works — the data flows, the DLQ sink below
// rejects the arms it cannot take, and nothing is silently corrupted. What is lost is
// submit-time refusal, which is a real loss of a real property, but not a wall.
// ---------------------------------------------------------------------------------

// dlqSink must reject and dead-letter incompatible records. It is a StructuredSink
// because per-record type inspection is the whole job.
type dlqSink struct {
	rt connector.SinkRuntime

	mu sync.Mutex

	// accept is the per-stream set of field name -> permitted value kinds, derived
	// from Opening.Schemas. It cannot be extended at runtime: see B2.2.
	accept map[record.StreamName]map[string]map[record.Kind]bool

	// openEpoch is the highest epoch this sink has a body for. Anything above it is
	// unresolvable.
	openEpoch map[record.StreamName]uint64
}

func init() {
	registry.AddSink(registry.Default, registry.SinkDef[*dlqSink]{
		Meta: registry.Meta{
			Name:    "stress_dlq_sink",
			Version: "1.0.0",
			Title:   "Reject-and-DLQ sink",
			Summary: "A strict destination that dead-letters records it cannot represent.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency: 2,
			Modes:          []connector.DestMode{connector.DestAppend},
			Idempotent:     false,

			// The capability that makes per-record rejection expressible at all, and it
			// is exactly right: without it the engine would have to guess.
			PartialFailure: true,
			Structured:     true,
			Partitions:     true,
		},
		New: func(context.Context, *config.Config) (*dlqSink, error) {
			return &dlqSink{
				accept:    map[record.StreamName]map[string]map[record.Kind]bool{},
				openEpoch: map[record.StreamName]uint64{},
			}, nil
		},
	})
}

// AcceptsStructured satisfies connector.StructuredSink. Its return value is documented
// as never read by the core; the method exists so the cross-check has a target.
func (k *dlqSink) AcceptsStructured() bool { return true }

// Partition batches by destination stream, so one request never mixes tables. Two lines,
// no batching code in the sink, and it works exactly as advertised.
func (k *dlqSink) Partition(r *record.Record) (string, error) { return string(r.Dest), nil }

func (k *dlqSink) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	k.rt = rt
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, e := range o.Schemas {
		s := record.StreamName(e.Ref.Stream)
		if e.Ref.Epoch > k.openEpoch[s] {
			k.openEpoch[s] = e.Ref.Epoch
		}
		if e.Schema == nil {
			continue
		}
		fields := map[string]map[record.Kind]bool{}
		for _, f := range e.Schema.Fields {
			fields[f.Name] = permittedKinds(f)
		}
		k.accept[s] = fields
	}
	return nil
}

// permittedKinds maps a declared schema field onto the record.Value kinds this
// destination will accept for it.
//
// B6 is visible in the TypeAny arm: the schema said "anything", so the strictest thing
// this sink can do is accept the three arms it happens to know about from the field's
// Doc string — which is to say, from a comment. With schema.Field.Variants the arm set
// would be data and this function would be exact.
func permittedKinds(f schema.Field) map[record.Kind]bool {
	set := func(ks ...record.Kind) map[record.Kind]bool {
		m := map[record.Kind]bool{record.KindNull: f.Nullable}
		for _, k := range ks {
			m[k] = true
		}
		return m
	}
	switch f.Type {
	case schema.TypeBool:
		return set(record.KindBool)
	case schema.TypeInt64:
		return set(record.KindInt)
	case schema.TypeUint64:
		return set(record.KindUint)
	case schema.TypeFloat64:
		return set(record.KindFloat)
	case schema.TypeString:
		return set(record.KindString)
	case schema.TypeBytes:
		return set(record.KindBytes)
	case schema.TypeTimestamp, schema.TypeDate, schema.TypeTime:
		return set(record.KindTime)
	case schema.TypeDecimal:
		return set(record.KindDecimal, record.KindInt)
	case schema.TypeStruct, schema.TypeMap:
		return set(record.KindMap)
	case schema.TypeList:
		return set(record.KindList)
	case schema.TypeAny:
		// B6: no variant set to consult, so "any" must mean any. This destination
		// cannot represent a nested map, so it will reject that arm at runtime —
		// something a submit-time check could have caught if Variants existed.
		return set(record.KindBool, record.KindInt, record.KindUint, record.KindFloat,
			record.KindString, record.KindBytes, record.KindTime, record.KindDecimal,
			record.KindList, record.KindMap)
	default:
		return set()
	}
}

// Write is the per-record rejection path, and it is where B2.3 costs precision.
func (k *dlqSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.AllWritten(0), nil
	}
	if req.Rows == nil {
		return connector.WriteResult{}, fault.Bug(fault.OpWrite,
			fmt.Errorf("StructuredSink was handed a request with no Rows"))
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	stream := req.Records[0].Stream

	// B2.3, EXECUTABLE. The request names a schema epoch this sink has no body for. It
	// cannot resolve the ref — SinkRuntime has no Schemas() — so it cannot tell a
	// compatible record from an incompatible one and must reject the whole request. The
	// case asked for "reject and DLQ the INCOMPATIBLE records"; the interface set can
	// only deliver "DLQ everything after the first drift".
	if req.Schema != nil && req.Schema.Epoch > k.openEpoch[stream] {
		failed := make([]fault.RecordFault, 0, req.Count)
		for i := range req.Records {
			failed = append(failed, fault.RecordFault{
				Record: req.Records[i].ID,
				Class:  fault.PermanentMapping,
				Op:     fault.OpWrite,
				User: fmt.Sprintf(
					"stream %s is at schema epoch %d and this destination only has a schema for "+
						"epoch %d; it cannot check this record's shape",
					stream, req.Schema.Epoch, k.openEpoch[stream]),
				Dev: "SinkRuntime exposes no Schemas(), so a schema.Ref minted after Open is " +
					"unresolvable and whole-epoch rejection is the only safe behaviour (B2.3)",
			})
		}
		// Written must equal Count - len(Failed): zero. Reconcile is mandatory and
		// checked, and getting it right is trivial — this is a fit, not a break.
		return connector.WriteResult{Written: 0}, fault.Mapping(fault.OpWrite,
			fmt.Errorf("stream %s: unresolvable schema epoch %d", stream, req.Schema.Epoch))
	}

	fields := k.accept[stream]
	var failed []fault.RecordFault
	var written int64

	for i, r := range req.Rows {
		if r == nil {
			continue
		}
		ref := r.Ref()
		if i < len(req.Records) {
			ref = req.Records[i]
		}
		if bad, why := k.incompatible(fields, r); bad {
			failed = append(failed, fault.RecordFault{
				Record: ref.ID,
				Class:  fault.PermanentMapping,
				Op:     fault.OpWrite,
				User:   fmt.Sprintf("record for %s cannot be represented: %s", ref.Stream, why),
				Dev:    why,
			})
			continue
		}
		written++
	}

	res := connector.WriteResult{Written: written}
	res.Failed = failed
	if ok, want := res.Reconcile(req.Count); !ok {
		return res, fault.Bug(fault.OpWrite,
			fmt.Errorf("reconciliation: Written=%d want=%d", res.Written, want))
	}
	if len(failed) > 0 {
		// (res, err) with Failed non-empty: everything not named is durable, and err is
		// the headline. All four quadrants of the Write contract are usable and none of
		// them is ambiguous. Another fit.
		return res, fault.Mapping(fault.OpWrite,
			fmt.Errorf("%d of %d records could not be represented", len(failed), req.Count))
	}
	return res, nil
}

// incompatible checks one record against the shape this destination accepts.
func (k *dlqSink) incompatible(fields map[string]map[record.Kind]bool, r *record.Record) (bool, string) {
	if fields == nil {
		return true, "no schema for this stream at Open, and no way to resolve one since"
	}
	v, ok := r.Payload.Structured()
	if !ok {
		return true, "no structured view on the payload"
	}
	m, ok := v.(record.Map)
	if !ok {
		return true, "payload is " + v.Kind().String() + ", not a map"
	}
	for name, val := range m {
		allowed, known := fields[name]
		if !known {
			// A column added after Open. The schema might be Open (undeclared fields
			// permitted) but this sink cannot see the current schema to find out, so it
			// rejects. B2.2 again.
			return true, "field " + name + " is not in the schema this destination was opened with"
		}
		kind := record.KindNull
		if val != nil {
			kind = val.Kind()
		}
		if !allowed[kind] {
			return true, "field " + name + " is " + kind.String() + ", which this destination cannot store"
		}
		// The nested-map arm of the union field: representable in record.Value, not
		// representable at this destination, rejected per record. This is exactly what
		// the case asked for and the mechanism delivers it — the only loss is that it
		// could not have been refused at submit time. See B6.
		if name == "attr" && kind == record.KindMap {
			return true, "field attr carries a nested map, which this destination cannot flatten"
		}
	}
	return false, ""
}

func (k *dlqSink) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------------
// Compile-time assertions. These are the receipt: every interface this file claims to
// implement, checked by the compiler rather than asserted in prose.
// ---------------------------------------------------------------------------------

var (
	_ connector.Source         = (*driftSource)(nil)
	_ connector.Heartbeater    = (*driftSource)(nil)
	_ connector.Sink           = (*ddlSink)(nil)
	_ connector.Flusher        = (*ddlSink)(nil)
	_ connector.SchemaApplier  = (*ddlSink)(nil)
	_ connector.Preparer       = (*ddlSink)(nil)
	_ connector.Sink           = (*absorbSink)(nil)
	_ connector.Sink           = (*dlqSink)(nil)
	_ connector.StructuredSink = (*dlqSink)(nil)
	_ connector.Partitioner    = (*dlqSink)(nil)
)

// The two assertions that DO NOT compile, kept as commented code because they are the
// finding. Uncomment either one after the corresponding fix lands and this file starts
// doing what its package comment says it does.
//
//	var _ = func(r *record.Record) { r.SetKey([]byte("k")) }                       // B7
//	var _ = func(b *record.Batch) { b.AddFor(streamOrders) }                       // B3
//	var _ = func(rt connector.SourceRuntime) { _ = rt.Schemas() }                  // B1
//	var _ = func(rt connector.SinkRuntime) { _ = rt.Schemas() }                    // B2
//	var _ = schema.Change{Result: baselineOrders()}                                // B2
//	var _ = schema.RenameStream                                                    // B4
//	var _ = schema.Field{Variants: []schema.Field{}}                               // B6

// ---------------------------------------------------------------------------------
// COMPILER EVIDENCE. Every one of the seven missing pieces was probed in a throwaway
// package against this repo at commit-time. Verbatim output of `go build`:
//
//	B7  r.SetKey undefined (type *record.Record has no field or method SetKey)
//	B7  r.SetUpstream undefined (type *record.Record has no field or method SetUpstream)
//	B3  b.AddFor undefined (type *record.Batch has no field or method AddFor)
//	B1  rt.Schemas undefined (type connector.SourceRuntime has no field or method Schemas)
//	B2  rt.Schemas undefined (type connector.SinkRuntime has no field or method Schemas)
//	B2  unknown field Result in struct literal of type schema.Change
//	B4  undefined: schema.RenameStream
//	B6  unknown field Variants in struct literal of type schema.Field
//
// AND THE ONE THAT COMPILES, which is the worst result in the set:
//
//	func P(r *record.Record) { o := r.Origin(); o.Key = []byte("k"); _ = o }   // builds
//
// That is B7's trap. The obvious way to set a key compiles, runs, and has no effect,
// because Origin() returns by value. An author who unit-tests `o.Key` rather than
// `r.Origin().Key` gets a green test and an empty key in production, and every
// downstream consumer of Key — dedupe, IdempotencyKey, RequiresKey, upsert identity —
// degrades silently. A missing method is a compile error; a value-returning accessor
// over exported mutable-looking fields is a silent one. If only one thing in this file
// gets fixed, fix that.
// ---------------------------------------------------------------------------------
