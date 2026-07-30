// Package multistream is a HOSTILE connector written against canal's real interfaces.
//
// The case it models is the one that breaks most connector frameworks: a single upstream that is
// really N independent streams — 900 of them — discovered at runtime, each with its own cursor,
// its own schema and its own snapshot state, where streams appear and disappear while the
// pipeline runs, and where the operator selects a subset with a per-stream sync mode
// (full_refresh / incremental / append_dedup).
//
// It is a real implementation, not a sketch: the fake upstream at the bottom of this file has
// per-stream sessions, cursors, deletes, event times and a stream set that churns, and every
// method body threads the state it would have to thread against a real API.
//
// It compiles. `go build ./internal/stress/multi-stream-source/` is clean. That is deliberate:
// the interesting findings are NOT compile failures except where marked, and the two that ARE
// compile failures are quoted verbatim below with the exact `go vet` output.
//
// READ THE BREAKAGES BLOCK BELOW BEFORE CHANGING ANYTHING IN pkg/. Each entry states the exact
// blocking signature, why the interface cannot express the case, what this file had to do
// instead, and the smallest additive fix. Sites in the code are tagged // B1: … // B9: so the
// argument and the workaround can be read together.
package multistream

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// ============================================================================================
//
//                                      B R E A K A G E S
//
// ============================================================================================
//
// ---------------------------------------------------------------------------------------------
// B1 — FATAL. One Read call cannot serve more than one lane, so a 900-lane source cannot be
//       written. The batch's identity stamper is bound to ONE lane and ONE stream and there is
//       no way to obtain, name, or retarget the stamper for another lane.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	connector.Source.Read(ctx context.Context, dst *record.Batch) error
//	record.NewBatch(a *Allocator, capHint int) *Batch      // b.alloc is unexported, immutable
//	func (b *record.Batch) Add() *Record                   // stamps Origin.Lane/Stream from b.alloc
//	// record.Batch has NO Alloc() accessor and NO Rebind(*Allocator).
//
// `Batch.Add` stamps `Origin.Lane`, `Origin.Stream` and `Dest` from the batch's unexported
// `*Allocator`, which `NewBatch` fixes at construction. `Batch.Lane` is a SEPARATE exported
// field the source is told to set ("Read fills dst and sets dst.Lane"). Those two facts cannot
// both be honoured by a multi-lane source. Measured, not argued — internal/stress/keyprobe_
// scratch_test.go prints:
//
//	batch.Lane="lane-b" origin.Lane="lane-a" origin.Stream="orders" dest="orders"
//
// so setting `dst.Lane` moves settlement (ledger.Admit keys on `b.Lane`) while leaving every
// record's provenance and stream naming behind. The two disagree SILENTLY.
//
// The two readings of the contract are each fatal, and the architecture document asserts both:
//
//   (a) "The engine creates one Allocator per (lane, generation) and hands the source a Batch
//       that already carries it" (§4.8) means the ENGINE picks the lane and the source reads
//       `dst.Lane` on entry. Then a source that must "block until at least one record is
//       available" blocks on the ONE lane the engine chose. With 900 lanes of which 890 are
//       idle, the read goroutine — "Read is never called concurrently with itself" — spends
//       essentially all of its time blocked on idle streams while the busy ones starve. There
//       is no readiness hint anywhere on LaneCtl (Budget() is an allowance, not readiness), so
//       the engine cannot choose well either. The only escape is to return an empty batch with
//       a nil error, which the contract does not sanction and which turns the engine's read
//       loop into a 900-way busy poll.
//
//   (b) "`Read` serves the eight scan lanes round-robin up to `Parallelism`" (§22(b) step 5)
//       means the SOURCE picks the lane. It cannot: `Add()` will stamp whatever lane and stream
//       the engine's allocator holds, so 899 of 900 lanes' records get a false `Origin.Stream`
//       (breaking per-stream dedupe scope, which is keyed on the stream, and per-stream read-
//       model attribution) and a false `Origin.Lane` (breaking dead-letter provenance and
//       `record.Ref.Lane`) while the ledger settles them under the lane the source wrote into
//       `dst.Lane`.
//
// WHAT THIS FILE DOES: it implements (a) and handles (b) defensively — `pick` uses `dst.Lane`
// when the engine chose a lane this instance holds, and otherwise retargets to a ready lane,
// counts the retarget on a metric and Notes it once. That is not a workaround, it is
// instrumentation of the gap: the retarget path knowingly emits records with false provenance
// because the alternative is emitting nothing at all.
//
// MEASURED, by TestDrive in this package, driving 860 assigned lanes through the single-batch
// node loop the architecture document describes:
//
//	reads=1333 records=65250 lanes served=432
//	65100/65250 records carry a false Origin.Lane and a false Origin.Stream
//
// 99.77%. Only the 150 records belonging to the one lane the allocator was built for are
// correctly stamped.
//
// THE TRAP THIS INTERFACE LEAVES OPEN, which a real connector author WILL find and which is
// worse than the gap: `record.NewAllocator` and `record.NewBatch` are EXPORTED, `Batch.Records`
// is an EXPORTED field, and `connector.Batcher.Flush` demonstrates the move
// (`dst.Records = append(dst.Records, b.pending...)`). So a source can mint its own per-lane
// allocators — it has Tenant(), Pipeline(), Node() from the runtime and the LaneID from
// Assigned — stamp records correctly for all 900 lanes, and splice them into `dst.Records`
// bypassing `Add`. It compiles, it looks right, and it silently corrupts settlement: the forged
// allocator restarts `RecordID` from its own `firstID`, and `ledger.Admit` does
// `l.byRec[r.Origin().ID] = b.Group()`. Colliding ids remap records to the wrong settlement
// group, a group resolves early, and the cursor commits past unwritten data. §4.8's claim that
// "a source therefore cannot forge provenance" is false as written; only prose stops it.
//
// SMALLEST FIX — canal's own documented growth mechanism, "new optional interfaces plus a
// SourceCaps field", so `Source` stays frozen and NOT ONE existing connector changes:
//
//	// MultiLaneSource is implemented by a source that holds many lanes at once and can
//	// produce for whichever of them has data. The engine hands it one batch per lane it
//	// holds, each already stamped for that lane, and admits every non-empty batch when
//	// ReadLanes returns. A source implementing it is never called through Read.
//	//
//	// Blocking rule: ReadLanes blocks until at least one record is available on ANY of
//	// the batches, which is the promise Read cannot keep for a source holding 900 lanes.
//	type MultiLaneSource interface {
//	    ReadLanes(ctx context.Context, dst []*record.Batch) error
//	}
//
//	// on SourceCaps, interface-backed exactly like Discoverable and Heartbeats:
//	MultiLane bool `json:"multi_lane"`  // MultiLaneSource
//
// It is additive (registration already cross-checks declared-flag-versus-interface and PANICS
// only on over-declaration), it costs the engine one branch in the source node loop, and it is
// seam-clean: a slice of batches is data, so a remote adapter fills local per-lane batches and
// ships them back on return exactly as it does for `dst`. `Parallelism` finally means something
// enforceable — it is the length of the slice.
//
// The alternative shape, `LaneCtl.Batch(lane record.LaneID) *record.Batch`, works too and needs
// no caps flag, but LaneCtl is the reverse channel and handing a mutable core-owned buffer
// across it is a worse fit for the out-of-process seam than passing a slice down.
//
// EITHER WAY, ALSO SEAL THE TRAP, or the forged-allocator path stays available and silently
// corrupts settlement: unexport `Batch.Records` behind an accessor plus an `Append` that checks
// provenance, or have `Ledger.Admit` reject a batch containing a record whose `Origin.Lane` is
// not `b.Lane`. The second is three lines and turns 99.77% silent corruption into a loud
// PermanentInternal on the first test run.
//
// ---------------------------------------------------------------------------------------------
// B2 — FATAL, AND A REAL COMPILER ERROR. A source cannot set Origin.Key or Origin.Upstream, so
//       no source can populate the identity every keyed mechanism in canal depends on.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	func (r *record.Record) Origin() Origin        // VALUE receiver, returns a copy
//	type record.Record struct { origin Origin; … } // unexported, no mutator
//	// record.Record has SetHandle and MarkFailed. It has no SetKey and no SetUpstream.
//
// Measured, verbatim, from a probe compiled in this package:
//
//	vet: keyprobe_test.go:13:2: cannot assign to r.Origin().Key
//	     (neither addressable nor a map index expression)
//	vet: keyprobe_test.go:13:4: r.SetKey undefined
//	     (type *record.Record has no field or method SetKey)
//
// This is not a niche gap. The design tells the author to do it — connector-author rule 8, "Set
// `Origin.Key` if you can" — and four core mechanisms are keyed on it:
//
//	SourceCaps.StableKeys        "means Origin.Key is populated"        -> undeclarable
//	spec.DedupeConfig{Layer: DedupeKey}      keys on Origin.Key         -> unreachable
//	spec.DedupeConfig{Layer: DedupeUpstream} keys on Origin.Upstream    -> unreachable
//	SinkCaps.RequiresKey         "every record must carry Origin.Key"   -> refuses every source
//	Request.IdempotencyKey       "present when the source declares StableKeys" -> never present
//	Guarantee EffectivelyOnce    requires StableKeys                    -> unreachable
//
// For THIS case it is the whole of the hostile requirement's third sync mode: `append_dedup` IS
// `DedupeConfig{Layer: DedupeKey}`, and it cannot be honoured by any source, so this file
// declares `StableKeys: false`, degrades append_dedup to append, and Notes it at Open. An upsert
// destination fed by 900 tables cannot upsert either.
//
// A source CAN write the key into `Meta.Set(record.NSSource, "key", …)`, and this file does, but
// nothing in the core reads that: it is provenance, not identity. That workaround is a decoy.
//
// SMALLEST FIX (additive, breaks nothing — `SetHandle` already sets the precedent, with the same
// "legal only from the source that produced the record, before it returns from Read" rule):
//
//	func (r *Record) SetKey(k []byte)
//	func (r *Record) SetUpstream(u []byte)
//
// Rejected alternative: derive Key in the core from `Change.Keys` plus the structured payload.
// It cannot work for a bytes-payload source (no structured view to project) and `Change` is
// optional, so it would leave the majority of sources exactly where they are now.
//
// ---------------------------------------------------------------------------------------------
// B3 — FATAL. A source cannot read the operator's per-stream selection. `spec.Spec.Streams` is
//       the core's own per-stream config and `SourceRuntime` does not expose it.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURE — the whole interface, by omission:
//
//	type SourceRuntime interface {
//	    Context() context.Context
//	    Lanes() LaneCtl
//	    State() StateHandle
//	    Log() *slog.Logger
//	    Metrics() Metrics
//	    Batcher(config.BatchPolicy) *Batcher
//	    Note(Event)
//	    Tenant() record.TenantID
//	    Pipeline() record.PipelineID
//	    Node() record.NodeID
//	}   // no Streams(), no Spec(), no per-stream anything
//
// `spec.StreamConfig{Stream, Read []LaneKind, Write DestMode, Keys, Dedupe}` is described as
// "the operator's per-stream choice, validated against the source's discovered catalog", and
// `engine/negotiate.go` validates `Read` against `SourceCaps.LaneKinds` and `Write` against
// `SinkCaps.Modes`. The SINK is told (`Opening.Streams []ConfiguredStream`). The SOURCE, which
// is the component that has to decide which of 900 streams to announce lanes for, which lane
// kinds to announce for each, and which candidate key the operator picked, is told nothing.
//
// WHAT THIS FILE DOES: it re-declares the entire selection in its own config spec — the
// `streams` array field below, with `name`, `sync_mode`, `cursor_field` and `key_fields`. That
// is a second representation of one entity, which is design rule R1's named failure mode, and
// the two representations drift in a way neither side can detect: an operator who selects
// stream "orders" in the connector's `streams` array but not in `spec.Streams` gets a source
// happily announcing lanes for it, records flowing, and a sink whose `Opening.Streams` never
// mentioned it, so it writes with the zero-value `DestAppend` into a destination the operator
// configured for upsert. Nothing validates the pair, because the core does not know the
// connector's copy exists.
//
// It also cannot honour `spec.StreamConfig.Keys` (the operator's choice among
// `StreamDesc.Keys`), so the key derivation is guessed from the connector's own duplicate
// field — on top of B2, which means it cannot be applied anyway.
//
// SMALLEST FIX (additive, breaks nothing; `connector.ConfiguredStream` already exists and
// `spec` imports `connector`, so the type must live in `connector` to avoid a cycle):
//
//	// on connector.ConfiguredStream, additively:
//	Read []LaneKind `json:"read,omitempty"`
//
//	// on SourceRuntime — the documented growth path, "adding a method here does not break
//	// a single connector, because the core implements it":
//	Streams() []ConfiguredStream
//
// Then delete the `streams` array from every multi-stream connector's own spec and let the core
// validate one representation.
//
// ---------------------------------------------------------------------------------------------
// B4 — FATAL. A stream that disappears and comes back either STOPS THE PIPELINE or is silently
//       unreadable forever, because a lane row cannot be inspected, revived, or re-announced
//       with a legitimately changed construction payload.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	Announce(ctx context.Context, spec LaneSpec) (record.LaneID, error)
//	// "idempotent on LaneSpec.Name: re-announcing an existing lane with an identical Spec
//	//  returns its id; with a DIFFERENT Spec it returns a fault.PermanentContract"
//	Assigned(ctx context.Context) ([]LaneAssignment, error)
//	// "A source never receives a finished lane from LaneCtl.Assigned" — and Assigned also
//	//  excludes GATED lanes, so a tail lane behind a scan group is invisible too.
//	// LaneAssignment.Finished exists — "the field exists for the read model" — and no LaneCtl
//	//  method ever returns a finished row.
//
// Streams appear and disappear; that is the hostile requirement, and it is routine in the real
// world (a table dropped and recreated, a per-day collection rolling over, a permission blip).
// When a stream disappears this source must call `Finish`, or its lane sits forever with a
// rising `LaneStatus.CheckpointAge` (B9). When it comes back there are exactly two outcomes and
// both are defects:
//
//   - Re-announce the same Name with the SAME payload: `Announce` returns the FINISHED lane's
//     id with a nil error, `Assigned` never returns it, and the source produces nothing for that
//     stream FOREVER with no error anywhere. Silent data loss dressed as success.
//   - Re-announce the same Name with a payload that legitimately differs — a tail lane's
//     construction blob carries the change-log position captured at announce time, so it MUST
//     differ — and `Announce` returns fault.PermanentContract, which stops the whole pipeline.
//     Measured, by TestReappear in this package, on one disappear/reappear cycle of 40 streams
//     out of 900:
//
//	permanent_contract/open: lane "tail:t_0860" already exists with a different
//	construction payload
//
// One vanishing table takes down the other 899 streams.
//
// A SECOND, INDEPENDENT HOLE IN THE SAME METHOD SET: a gated tail lane is not in `Assigned`
// either, so when its stream disappears the source cannot retire it — it has no id for it — and
// when the scan lane is retired the GATE OPENS and the core hands out a lane for a stream that
// no longer exists. The only defence is to remember every id `Announce` returned, in memory,
// which a restart loses (B6). This file does exactly that and the comment on `announcedLanes`
// says so.
//
// The same blindness makes the cold/warm discriminator ambiguous for EVERY source, not just this
// one: `Assigned` returning empty means either "never announced" or "every lane I ever had is
// finished", and the shipped linefile source silently re-reads its whole file in the second case.
//
// SMALLEST FIX (additive, breaks nothing; `LaneAssignment.Finished` already exists for exactly
// this purpose):
//
//	// Table returns every lane row this source has announced, finished and gated ones
//	// included, so a source can tell a new stream from a retired one and can retire a lane
//	// it does not hold.
//	Table(ctx context.Context) ([]LaneAssignment, error)
//
// That alone fixes the silent-loss branch and the gated-lane branch. The PermanentContract
// branch needs one more sentence of contract, and it should be a SEPARATE method rather than a
// weakening of Announce, so that the accidental-rewrite protection Announce exists for is kept:
//
//	// Reannounce replaces a FINISHED lane's construction payload and revives it, keeping
//	// its id and its persisted cursor. Refused for a lane that is not finished.
//	Reannounce(ctx context.Context, spec LaneSpec) (record.LaneID, error)
//
// WHAT THIS FILE DOES INSTEAD: it suffixes an incarnation number into the tail lane's name
// (`tail:orders:g3`) so a name is never reused, and hand-carries the previous incarnation's
// cursor from memory. Measured cost, by TestChurn: lane rows go 1800 -> 1840 over one churn
// cycle and 80 rows are finished-forever, i.e. one permanent extra row per stream per
// disappearance (B7), and a restart mid-outage loses the carried cursor, which silently
// re-starts the tail at the CURRENT log position — a gap exactly the width of the outage.
//
// ---------------------------------------------------------------------------------------------
// B5 — MAJOR. `Announce` is one durable write per lane and there is no batch form, so opening
//       this source is 1800 sequential fsync-round-trips inside a retried Open.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURE:
//
//	Announce(ctx context.Context, spec LaneSpec) (record.LaneID, error)
//	// "The core persists the row — atomically, together with any state write in flight —
//	//  BEFORE returning."
//
// 900 streams in incremental mode is 1800 lanes (a scan lane and a gated tail lane each), so
// cold-start Open performs 1800 serialised durable writes. At 5 ms per local fsync that is 9
// seconds; against a Postgres state store at 20 ms it is 36 seconds, inside a method the engine
// retries with backoff on any failure, and a failure at lane 1500 re-walks all 1500 on the
// retry. `StateHandle` already proves the core can write many keys atomically —
// `SetMany(ctx, map[record.LaneID]Write)` — so the capability exists and only the lane-
// announcement path lacks it.
//
// SMALLEST FIX (additive, breaks nothing):
//
//	// AnnounceMany persists every spec in ONE durable write, all or nothing, and is
//	// idempotent per Name exactly as Announce is.
//	AnnounceMany(ctx context.Context, specs []LaneSpec) ([]record.LaneID, error)
//
// ---------------------------------------------------------------------------------------------
// B6 — MAJOR. There is no connector-scoped durable state. `StateHandle` is keyed by LaneID
//       only, so state that is not per-lane has nowhere to live.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	Get(ctx context.Context, lane record.LaneID) (record.Blob, uint64, error)
//	Set(ctx context.Context, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error)
//	SetMany(ctx context.Context, w map[record.LaneID]Write) error
//	Delete(ctx context.Context, lane record.LaneID) error
//	// every method takes a lane; there is no node-scoped slot.
//
// This source needs three durable facts that are not per-lane:
//
//	1. the full_refresh pass counter per stream, so the next pass can announce a lane name
//	   that is not already finished (B4);
//	2. the last discovered stream set, so "stream vanished" can be told from "one list call
//	   failed" without re-listing 900 streams at every Open;
//	3. the stream -> lane-name index, so a rename upstream does not silently re-snapshot.
//
// Every one of them must exist BEFORE the first lane exists, and the only durable slot is keyed
// by a lane. The workaround is to announce a lane that carries no data purely to own a state
// key — which this file does NOT do, because that lane is visible in the read model as a lane
// that never commits, never finishes, and reports a monotonically rising checkpoint age, i.e.
// it manufactures exactly the alert this design says is its primary signal. Instead this file
// keeps the pass counter in the LANE SPEC of the previous pass, which is only readable through
// `Assigned` and therefore invisible once that lane finishes — so the counter is lost precisely
// when it is needed, and the code below has to fall back to a clock-derived pass id, which
// makes a resumed full_refresh unresumable. That is the awkwardness, stated plainly.
//
// SMALLEST FIX (additive, breaks nothing — `StateHandle` is core-implemented, so growing it
// affects fakes only, and the store's Key already has a `Space`/`Parts` shape that can carry it):
//
//	// GetNode and SetNode address a slot owned by this NODE rather than by a lane, for
//	// state a source needs before any lane exists.
//	GetNode(ctx context.Context) (record.Blob, uint64, error)
//	SetNode(ctx context.Context, b record.Blob, ifVersion uint64) (uint64, error)
//
// ---------------------------------------------------------------------------------------------
// B7 — MAJOR. Nothing can retire a lane ROW, so the durable lane table grows with the
//       historical union of every stream that ever existed.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	Finish(ctx context.Context, id record.LaneID) error   // marks Finished + FinishedAt; row stays
//	StateHandle.Delete(ctx, lane)                         // deletes the STATE, not the lane row
//	// engine.Checkpoint.Lanes map[record.LaneID]LaneState — written by Announce, never pruned by
//	// any connector-reachable call. `DELETE /v1/pipelines/{id}/offsets/{lane}` is an OPERATOR
//	// action, per ADR 0011.
//
// The hostile requirement is explicit that checkpoint state "must not grow unboundedly". Per
// COMMIT it does not — ADR 0011 is right that a phase-two write touches only advancing lanes.
// Per LIFETIME it does: a source whose 900 streams churn (temp tables, per-day collections,
// per-tenant collections) accumulates a finished lane row for every stream that ever existed,
// forever, and the rows cannot be dropped because "finished" is what stops a re-scan. After a
// year of daily collections that is ~330k rows in `Checkpoint.Lanes`, read in full at every
// Open and listed in full in `PipelineStatus.Lanes`. Note that B4's only workaround — a pass
// suffix per full_refresh run — makes this strictly worse: one new permanent row per stream per
// pass.
//
// SMALLEST FIX (additive, breaks nothing):
//
//	// Forget drops a finished lane's row and its state in one durable write. It is
//	// refused for a lane that is not finished, or that has unsettled groups.
//	Forget(ctx context.Context, id record.LaneID) error
//
// ---------------------------------------------------------------------------------------------
// B8 — MAJOR. A source cannot register a schema, so per-stream schema evolution — the thing 900
//       independent streams guarantees you will see — cannot reach the core at all.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	type record.Record struct { … Schema *schema.Ref … }   // a REF; the body lives in the table
//	type CodecRuntime interface { … Schemas() SchemaLookup }  // codecs can Register
//	type SourceRuntime interface { … }                        // sources cannot: no Schemas()
//	func (rt) Note(e Event)  // Event carries Kind/Stream/Lane/Message/Detail — strings only,
//	                         // no schema.Change, no *schema.Schema
//
// `schema.Ref{Fingerprint, Epoch, Stream}` is constructible (`schema.Fingerprint` is exported)
// but the EPOCH is core-assigned and the schema BODY must be in the pipeline's table or the
// reference does not resolve — which is the failure mode the schema table exists to prevent. The
// only population path is `Discoverer.Discover` -> `Catalog.Streams[i].Schema` -> the persisted
// catalog, i.e. submit time. A column added to table 412 while the pipeline runs is seen by this
// source, in order, in the change log, and it has no way to say so except a free-text Event, so
// `DriftEvolve` and `SchemaApplier.ApplySchemaChange` can never be driven by the component that
// actually observes the DDL. §14.1's "schema changes are ordered in-band events with a
// schema-before-data rule" is unimplementable from a source: there is no control record type and
// no source-side registration.
//
// WHAT THIS FILE DOES: it emits `Record.Schema = nil` for every one of 900 streams and posts a
// free-text EventSchemaChange. A destination therefore cannot create or alter 900 tables.
//
// SMALLEST FIX (additive, breaks nothing — the interface and its implementation both already
// exist for codecs, and `Register` already assigns the epoch):
//
//	// on SourceRuntime:
//	Schemas() SchemaLookup
//
// ---------------------------------------------------------------------------------------------
// B9 — MINOR. 890 idle-but-healthy streams each report a rising checkpoint age, and the read
//       model has no way to distinguish idle from stalled, nor any bound on its lane list.
// ---------------------------------------------------------------------------------------------
//
// BLOCKING SIGNATURES:
//
//	Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error
//	// "Heartbeat … never carries a position and cannot advance a cursor"
//	telemetry.PipelineStatus.Lanes []LaneStatus       // unpaginated, unaggregated
//	telemetry.LaneStatus.CheckpointAge *float64       // "the primary alert signal"
//
// A stream with nothing new never commits, so its checkpoint age rises without bound while it is
// perfectly healthy, and `CheckpointAge` is the design's primary alert signal. At 900 streams
// that is not an edge case, it is the steady state: most streams are quiet most of the time. An
// operator can cross-reference `Backlog` (0 records) and `OldestPendingAge` (nil) per lane, but
// nothing in the model says "idle", and 1800 `LaneStatus` rows are re-serialised into every
// status document and every SSE frame.
//
// The UI requirement itself is MET with no core change: `LaneStatus.Stream` plus connector-
// authored `Label`/`Position.Label` give per-stream progress, and `config.Field.Choices` plus
// the array field give per-stream config, with no connector knowledge in the core. Only the
// scale of the document and the idle/stalled ambiguity are unaddressed.
//
// SMALLEST FIX: telemetry-only, no connector interface changes. Either add
// `LaneStatus.IdleSince *time.Time` set by the engine when a heartbeat succeeded with nothing
// pending, or aggregate `PipelineStatus.Lanes` per stream with a drill-down. Neither touches a
// frozen interface.
//
// ============================================================================================

// Blob versions. Everything durable this connector authors is (version, bytes), and a version
// this build cannot read fails LOUDLY with both numbers named.
const (
	// laneSpecV1 versions the write-once lane construction payload.
	laneSpecV1 uint32 = 1
	// cursorV1 versions the write-many cursor payload.
	cursorV1 uint32 = 1
)

// Read-loop shape. maxBatch is a courtesy: record.Batch has its own hard cap and Add returns
// nil at it.
const (
	maxBatch = 500

	// idleWait is how long Read waits for the ONE lane the engine chose before returning an
	// empty batch. See B1: a source holding 900 lanes has no legal way to wait on all of them,
	// and blocking here for longer starves every other lane.
	idleWait = 250 * time.Millisecond

	// sweepEvery is how often the read path reconciles the discovered stream set against the
	// announced lane set. It runs on the read goroutine because LaneCtl declares no
	// concurrency contract (see the note on reconcile).
	sweepEvery = 30 * time.Second
)

// syncMode is the operator's per-stream source-side choice.
//
// It duplicates spec.StreamConfig.Read, which the source cannot see (B3).
type syncMode uint8

const (
	modeFullRefresh syncMode = iota + 1
	modeIncremental
	modeAppendDedup
)

func parseMode(s string) (syncMode, error) {
	switch s {
	case "full_refresh":
		return modeFullRefresh, nil
	case "incremental":
		return modeIncremental, nil
	case "append_dedup":
		return modeAppendDedup, nil
	default:
		return 0, fmt.Errorf("unknown sync mode %q", s)
	}
}

func (m syncMode) String() string {
	switch m {
	case modeFullRefresh:
		return "full_refresh"
	case modeIncremental:
		return "incremental"
	case modeAppendDedup:
		return "append_dedup"
	default:
		return "unknown"
	}
}

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*Source]{
		Meta: registry.Meta{
			Name:    "multi_stream_probe",
			Version: "0.1.0",
			Title:   "Multi-stream probe (stress)",
			Summary: "A synthetic source that is 900 independent streams discovered at runtime.",
			Notes: "Origin.Key is NOT populated: record.Record exposes no SetKey, so no source " +
				"can populate it (see B2). The natural key is written to Meta under the source " +
				"namespace instead, where nothing in the core reads it. StableKeys is therefore " +
				"declared false and append_dedup degrades to append.",
			Support: registry.SupportCommunity,
		},

		Spec: config.NewSpec().
			Describe("A source that is many streams",
				"Discovers streams at runtime and reads a selected subset, one lane per stream per phase.").
			Field(config.Field{
				Name:        "endpoint",
				Type:        config.TypeString,
				Title:       "Endpoint",
				Description: "Base address of the upstream that owns the streams.",
				Examples:    []any{"https://api.example.invalid/v1"},
			}).
			Field(config.Field{
				Name:        "token",
				Type:        config.TypeString,
				Title:       "API token",
				Description: "Credential used for every call. Redacted everywhere by the core.",
				Secret:      true,
			}).
			Field(config.Field{
				Name:        "discover_interval",
				Type:        config.TypeDuration,
				Title:       "Discovery interval",
				Description: "How often to re-list streams while running, so appearances and disappearances are noticed.",
				Default:     "1m",
				Optional:    true,
			}).
			Field(config.Field{
				Name:        "page_size",
				Type:        config.TypeInt,
				Title:       "Page size",
				Description: "How many records to request per upstream call, per stream.",
				Default:     500,
				Optional:    true,
			}).
			// B3: this whole field is a SECOND representation of spec.Spec.Streams, which the
			// core owns, validates against the discovered catalog and against the sink's declared
			// modes, and does not show to the source. Nothing reconciles the two.
			Field(config.Field{
				Name:        "streams",
				Type:        config.TypeArray,
				Title:       "Streams",
				Description: "Which discovered streams to read, and how. Duplicates the pipeline's own per-stream selection because a source cannot read it.",
				Item: &config.Field{
					Name:        "stream",
					Type:        config.TypeObject,
					Title:       "Stream",
					Description: "One selected stream and its source-side sync mode.",
					Fields: []config.Field{{
						Name:        "name",
						Type:        config.TypeString,
						Title:       "Stream name",
						Description: "Name as reported by discovery.",
						// This is how a specialised picker is built with no core knowledge that
						// streams exist: a named hook the frontend resolves over HTTP.
						Choices: "streams",
					}, {
						Name:        "sync_mode",
						Type:        config.TypeEnum,
						Title:       "Sync mode",
						Description: "How this stream is read.",
						Default:     "incremental",
						Optional:    true,
						Enum: []config.EnumValue{
							{Value: "full_refresh", Title: "Full refresh", Description: "Re-read the whole stream every pass."},
							{Value: "incremental", Title: "Incremental", Description: "Snapshot once, then tail the change log."},
							{Value: "append_dedup", Title: "Append + dedupe", Description: "Append everything and let the engine drop duplicates by key."},
						},
					}, {
						Name:        "cursor_field",
						Type:        config.TypeString,
						Title:       "Cursor field",
						Description: "Field the incremental cursor advances over. Empty means the stream's default.",
						Default:     "",
						Optional:    true,
					}, {
						Name:        "key_fields",
						Type:        config.TypeArray,
						Title:       "Key fields",
						Description: "Fields forming this stream's identity. Duplicates the operator's choice among the catalog's candidate keys.",
						Optional:    true,
						Item: &config.Field{
							Name:        "field",
							Type:        config.TypeString,
							Title:       "Field",
							Description: "One key field path segment set.",
						},
					}},
				},
			}),

		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Bounded, connector.Unbounded},
			LaneKinds: []connector.LaneKind{
				connector.LaneKindScan,
				connector.LaneKindStream,
			},

			// Two lanes per stream (scan + gated tail), and the upstream tolerates a few
			// thousand streams. A hard number rather than 0/unlimited so that a runaway
			// discovery fails at announce instead of announcing a million rows.
			MaxLanes: 4000,

			UpstreamRetention: connector.RetentionWindow,
			ReplayWindow:      72 * time.Hour,
			UnitAssignment:    connector.UnitsDynamic,

			Discoverable:   true,
			ReportsBacklog: true,
			Heartbeats:     true,
			Validates:      true,
			Choices:        true,

			ProducesEventTime: true,
			ProducesChange:    true,

			// B8: false. The upstream reports a schema per stream and it evolves at runtime, but
			// a source has no way to register one, so no ref can be attached to a record.
			ProducesSchema: false,

			CompleteImages:      true,
			ComparablePositions: true,
			Replayable:          true,
			MidLaneResume:       true,

			// B2: NOT declarable. record.Record has no SetKey, so Origin.Key cannot be
			// populated by any source. Declaring true here would be a lie the registry cannot
			// catch, and it would make the engine emit Request.IdempotencyKey over records with
			// no key.
			StableKeys: false,
		},

		New: func(_ context.Context, c *config.Config) (*Source, error) {
			s := &Source{
				endpoint:  config.Must[string](c, "endpoint"),
				poll:      config.Must[time.Duration](c, "discover_interval"),
				page:      config.Must[int](c, "page_size"),
				selected:  map[record.StreamName]*choice{},
				lanes:     map[record.LaneID]*laneState{},
				order:     nil,
				found:     map[record.StreamName]apiStream{},
				retired:   map[record.StreamName]retiredTail{},
				announced: map[record.StreamName]announcedLanes{},
			}
			tok, err := c.Secret("token")
			if err != nil {
				return nil, err
			}
			s.token = tok

			items, err := c.List("streams")
			if err != nil {
				return nil, err
			}
			for _, it := range items {
				name := record.StreamName(config.Must[string](it, "name"))
				modeStr := "incremental"
				if it.Has("sync_mode") {
					modeStr = config.Must[string](it, "sync_mode")
				}
				mode, err := parseMode(modeStr)
				if err != nil {
					return nil, fault.Contract(fault.OpValidate, err)
				}
				ch := &choice{name: name, mode: mode}
				if it.Has("cursor_field") {
					ch.cursorField = config.Must[string](it, "cursor_field")
				}
				if it.Has("key_fields") {
					ch.keyFields = config.Must[[]string](it, "key_fields")
				}
				s.selected[name] = ch
				s.order = append(s.order, name)
			}
			if err := itemErrs(items); err != nil {
				return nil, err
			}
			return s, c.Err()
		},
	})
}

// itemErrs collects accessor errors accumulated on the per-item sub-configs, which do not
// propagate to the parent Config.
func itemErrs(items []*config.Config) error {
	for _, it := range items {
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

// choice is one selected stream, as the operator configured it.
type choice struct {
	name        record.StreamName
	mode        syncMode
	cursorField string
	keyFields   []string
}

// laneSpecPayload is the write-once construction state persisted with a lane row and handed
// back verbatim at restart. It is the only place a lane's stream, mode and pass survive.
type laneSpecPayload struct {
	Stream      string   `json:"stream"`
	Mode        string   `json:"mode"`
	Kind        string   `json:"kind"`
	CursorField string   `json:"cursor_field,omitempty"`
	KeyFields   []string `json:"key_fields,omitempty"`

	// Pass distinguishes one full_refresh run from the next, because a finished lane name can
	// never be reused (B4).
	Pass uint64 `json:"pass,omitempty"`

	// Gen distinguishes one INCARNATION of a tail lane from the next, for a stream that
	// disappeared and came back. See B4: without it, re-announcing the same name with a fresh
	// change-log position returns fault.PermanentContract and stops the pipeline.
	Gen uint64 `json:"gen,omitempty"`

	// From is the change-log position captured BEFORE the scan started, so the tail lane
	// resumes at a point no scan row can precede.
	From string `json:"from,omitempty"`
}

// cursorPayload is the write-many progress state carried in Position.Token.
//
// It is per LANE, which is the shape canal wants and the reason the "checkpoint state is a map"
// problem does not become one blob of 900 entries: the map is the core's lane table, one row per
// stream, and a phase-two write touches only the rows that advanced. That part of the design
// holds up under this case; see B7 for the part that does not.
type cursorPayload struct {
	V      uint32 `json:"v"`
	Cursor string `json:"cursor,omitempty"`
	Pass   uint64 `json:"pass,omitempty"`
	Rows   uint64 `json:"rows,omitempty"`
}

// laneState is everything this source knows about one lane it holds.
type laneState struct {
	id     record.LaneID
	name   string
	stream record.StreamName
	kind   connector.LaneKind
	mode   syncMode

	cursorField string
	keyFields   []string
	pass        uint64
	gen         uint64

	// cursor is the upstream position this lane will read from next.
	cursor string
	rows   uint64

	// weight is the estimate announced with the lane, for progress reporting.
	weight uint64

	// scanDone is set when a bounded lane has produced its final batch, so a second Read does
	// not re-announce EndOfLane.
	scanDone bool

	// acked is the last cursor this source told the upstream about, so Commit is idempotent.
	acked string
}

// retiredTail is what this source remembers about a tail lane it had to retire.
type retiredTail struct {
	gen    uint64
	cursor string
}

// announcedLanes is the ids this instance announced for one stream.
//
// It exists because `Assigned` returns only lanes this worker HOLDS and is UNGATED, so a tail
// lane waiting behind its scan group is invisible: not in Assigned, not reachable through any
// LaneCtl method, and therefore impossible to retire when its stream disappears. The ids have to
// be remembered from the Announce return value — in memory, because B6 leaves nowhere durable to
// keep them, so a restart loses the ability to retire a gated lane at all.
type announcedLanes struct {
	scan record.LaneID
	tail record.LaneID
	gen  uint64
}

// Source is the connector.
type Source struct {
	// Immutable after New.
	endpoint string
	token    string
	poll     time.Duration
	page     int
	selected map[record.StreamName]*choice
	order    []record.StreamName

	rt  connector.SourceRuntime
	api *api

	// persist is the source-shaped second write: the upstream needs telling where we are, and
	// the Persister is safe for concurrent use so no mutex covers it.
	persist *connector.Persister

	// changes is LaneCtl's assignment-change signal, re-taken on every refresh.
	changes <-chan struct{}

	// mu guards everything below: the read goroutine fills batches while the control goroutine
	// runs Commit, Heartbeat and Backlog. One mutex, exactly as the Read contract predicts.
	mu    sync.Mutex
	lanes map[record.LaneID]*laneState
	ring  []record.LaneID // round-robin order over held lanes
	next  int

	// found is the last discovery result, used to notice appearances and disappearances.
	found map[record.StreamName]apiStream

	// retired remembers, PER PROCESS ONLY, the incarnation number and last cursor of a tail
	// lane whose stream disappeared, so a reappearance can announce a fresh lane name (B4)
	// that resumes where the old one stopped.
	//
	// B6 is why this is in memory: there is no connector-scoped durable slot to keep it in, and
	// it cannot live in a lane's state because the lane it describes is finished and therefore
	// invisible (B4). A restart between a disappearance and a reappearance loses it, and the
	// stream then resumes from the CURRENT change-log position — a silent gap covering exactly
	// the outage — or needs a full re-scan. Neither is expressible as a choice here.
	retired map[record.StreamName]retiredTail

	// announced maps a stream to the lane ids this instance announced for it. See
	// announcedLanes for why remembering them is not optional.
	announced map[record.StreamName]announcedLanes

	lastSweep time.Time
	opened    bool

	// Instrumentation for B1: how often Read had to emit records for a lane other than the one
	// the batch's allocator was stamped for.
	retargets  connector.Counter
	liveStream connector.Gauge
	notedB1    bool
	notedB2    bool
}

// Open connects, restores every assigned lane from its own persisted bytes, and announces lanes
// for selected streams that do not have them yet.
//
// It is idempotent: the engine calls it again with backoff after any method returns
// fault.ErrNotConnected, and reconnecting must not re-announce or lose a cursor.
func (s *Source) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt
	if s.api == nil {
		s.api = newAPI(s.endpoint, s.token)
	}
	if err := s.api.connect(ctx); err != nil {
		return fault.Transient(fault.OpOpen, err)
	}
	if s.persist == nil {
		s.persist = connector.AutoPersist(rt)
	}
	if s.retargets == nil {
		// The core owns metric naming and the label set; a connector can only ask.
		s.retargets, _ = rt.Metrics().Counter("lane_retargets")
		s.liveStream, _ = rt.Metrics().Gauge("live_streams")
	}
	s.changes = rt.Lanes().Changes()

	// B2, reported once per Open so an operator sees it in the events ring rather than only in
	// a capability table.
	if !s.notedB2 {
		s.notedB2 = true
		for _, ch := range s.selected {
			if ch.mode == modeAppendDedup {
				rt.Note(connector.Event{
					At:       time.Now(),
					Kind:     connector.EventDegraded,
					Severity: fault.PermanentContract,
					Stream:   ch.name,
					Message:  "append_dedup degraded to append: this source cannot populate Origin.Key",
					Detail:   "record.Record exposes no SetKey, so the engine's keyed dedupe has nothing to key on",
				})
			}
		}
	}

	// ---- restore -------------------------------------------------------------------------
	//
	// Warm start and cold start are two separate paths and the discriminator is DATA — whether
	// Assigned returned anything — never a nil position. Note the ambiguity B4 leaves here:
	// an empty result also means "every lane I ever had is finished".
	as, err := s.reload(ctx)
	if err != nil {
		return err
	}
	cold := len(as) == 0

	// ---- discover and announce -----------------------------------------------------------
	if err := s.sweep(ctx); err != nil {
		return err
	}

	// Announcing does not assign: the planner decides where a lane runs, so the lane state
	// this instance will actually read has to be read back. On a cold start that is the whole
	// set; on a warm one it picks up whatever the sweep just added.
	if cold || s.pending() {
		if _, err := s.reload(ctx); err != nil {
			return err
		}
	}
	s.opened = true
	return nil
}

// reload reads the assigned set, reconstructs every lane from its Spec plus Cursor, drops lanes
// this instance no longer holds, and primes the source-shaped cursor cache.
//
// It is the same code path for a cold start, a warm restart and a rebalance, which is what makes
// horizontal scaling free: distribution is restart with a different subset.
func (s *Source) reload(ctx context.Context) ([]connector.LaneAssignment, error) {
	as, err := s.rt.Lanes().Assigned(ctx)
	if err != nil {
		return nil, err
	}
	held := make(map[record.LaneID]bool, len(as))

	s.mu.Lock()
	for i := range as {
		held[as[i].ID] = true
		if _, ok := s.lanes[as[i].ID]; ok {
			continue
		}
		ls, err := s.restore(as[i])
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.lanes[as[i].ID] = ls
	}
	for id := range s.lanes {
		if !held[id] {
			delete(s.lanes, id)
		}
	}
	s.rebuildRing()
	s.mu.Unlock()

	// Prime the Persister's cache for every lane before the first Read, so a Commit's CAS
	// version is the stored one and not zero.
	for i := range as {
		if _, _, err := s.persist.Load(ctx, as[i].ID); err != nil {
			return nil, err
		}
	}
	return as, nil
}

// pending reports whether any selected, present stream has no lane state on this instance yet.
func (s *Source) pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	have := make(map[record.StreamName]bool, len(s.lanes))
	for _, ls := range s.lanes {
		have[ls.stream] = true
	}
	for name := range s.found {
		if s.selected[name] != nil && !have[name] {
			return true
		}
	}
	return false
}

// restore reconstructs a lane from the two blobs the core handed back: the write-once Spec and
// the write-many Cursor. Both are bytes this connector authored, returned verbatim.
func (s *Source) restore(a connector.LaneAssignment) (*laneState, error) {
	var sp laneSpecPayload
	switch blob := a.Spec.Spec; {
	case blob.IsZero():
		return nil, fault.Bug(fault.OpOpen,
			fmt.Errorf("lane %q was announced with no construction payload", a.ID))
	case blob.Version == laneSpecV1:
		if err := json.Unmarshal(blob.Bytes, &sp); err != nil {
			return nil, fault.Contract(fault.OpOpen, fmt.Errorf("lane %q spec: %w", a.ID, err))
		}
	default:
		// Rule three of the format contract says never reject a NEWER version when the format
		// is additive — and this one is, so a newer payload is decoded on a best-effort basis
		// and the unknown fields are ignored.
		if err := json.Unmarshal(blob.Bytes, &sp); err != nil {
			return nil, fault.Contract(fault.OpOpen,
				fmt.Errorf("lane %q spec version %d unreadable by build %d: %w",
					a.ID, blob.Version, laneSpecV1, err))
		}
	}

	mode, err := parseMode(sp.Mode)
	if err != nil {
		return nil, fault.Contract(fault.OpOpen, fmt.Errorf("lane %q: %w", a.ID, err))
	}

	ls := &laneState{
		id:          a.ID,
		name:        a.Spec.Name,
		stream:      record.StreamName(sp.Stream),
		kind:        a.Spec.Kind,
		mode:        mode,
		cursorField: sp.CursorField,
		keyFields:   sp.KeyFields,
		pass:        sp.Pass,
		gen:         sp.Gen,
		cursor:      sp.From,
		weight:      a.Spec.Weight,
	}

	// The cursor overrides the spec's starting point when there is one. Absent is "no progress
	// yet", never "start from now".
	switch tok := a.Cursor.Token; {
	case tok.IsZero():
	case tok.Version == cursorV1:
		var cp cursorPayload
		if err := json.Unmarshal(tok.Bytes, &cp); err != nil {
			return nil, fault.Contract(fault.OpOpen, fmt.Errorf("lane %q cursor: %w", a.ID, err))
		}
		ls.cursor, ls.rows = cp.Cursor, cp.Rows
		if cp.Pass != 0 {
			ls.pass = cp.Pass
		}
	default:
		var cp cursorPayload
		if err := json.Unmarshal(tok.Bytes, &cp); err != nil {
			return nil, fault.Contract(fault.OpOpen,
				fmt.Errorf("lane %q cursor version %d unreadable by build %d: %w",
					a.ID, tok.Version, cursorV1, err))
		}
		ls.cursor, ls.rows = cp.Cursor, cp.Rows
	}
	ls.acked = ls.cursor
	return ls, nil
}

// sweep re-lists the upstream, announces lanes for streams that need them, and retires lanes
// whose stream has gone.
//
// It runs on whichever goroutine called it — Open's, or the read goroutine's — and never
// concurrently with itself, because LaneCtl declares NO concurrency contract. That silence is a
// smaller gap than the numbered ones but it is a gap: a source that wants to announce from a
// background discovery goroutine has to guess whether that is legal.
func (s *Source) sweep(ctx context.Context) error {
	found, err := s.api.list(ctx)
	if err != nil {
		return fault.Transient(fault.OpDiscover, err)
	}

	live := make(map[record.StreamName]apiStream, len(found))
	for _, st := range found {
		live[record.StreamName(st.Name)] = st
	}
	if s.liveStream != nil {
		s.liveStream.Set(float64(len(live)))
	}

	// ---- appearances ---------------------------------------------------------------------
	//
	// B5: one Announce per lane, each a durable write that must complete before the next. For
	// 900 selected streams in incremental mode that is 1800 serialised round-trips, here, in a
	// method the engine retries.
	var announced int
	for _, name := range s.order {
		ch := s.selected[name]
		st, ok := live[name]
		if !ok {
			continue // selected but not present upstream; Validate reports it
		}
		if s.hasLanesFor(name) {
			continue
		}
		if err := s.announceFor(ctx, ch, st); err != nil {
			return err
		}
		announced++
		if err := ctx.Err(); err != nil {
			// 1800 serialised durable writes is long enough that a shutdown can land in the
			// middle of them. Announced rows are durable, so stopping here is safe and the next
			// Open continues.
			return err
		}
	}

	// A lane that was just announced is not yet in this instance's lane map — announcing is not
	// assignment — and a stream that vanishes before its lanes are loaded would otherwise never
	// be retired.
	if announced > 0 && s.opened {
		if _, err := s.reload(ctx); err != nil {
			return err
		}
	}

	// ---- disappearances ------------------------------------------------------------------
	//
	// This iterates over what this instance ANNOUNCED, not over what it holds. A tail lane
	// gated behind its scan group is not returned by Assigned, so it is not in s.lanes, so
	// iterating the lane map would leave every gated tail of a vanished stream un-retired
	// forever — and then the gate opens when the scan is retired and the core hands out a lane
	// for a stream that no longer exists.
	s.mu.Lock()
	type dying struct {
		stream record.StreamName
		an     announcedLanes
	}
	var gone []dying
	for name, an := range s.announced {
		if _, ok := live[name]; !ok {
			gone = append(gone, dying{stream: name, an: an})
		}
	}
	s.mu.Unlock()

	for _, d := range gone {
		for _, id := range []record.LaneID{d.an.scan, d.an.tail} {
			if id == "" {
				continue
			}
			// Finish is a REQUEST: the core does not consider the lane finished until every
			// group admitted for it has settled and that fact is durable.
			if err := s.rt.Lanes().Finish(ctx, id); err != nil {
				return err
			}
		}
		s.rt.Note(connector.Event{
			At:      time.Now(),
			Kind:    connector.EventLaneFinished,
			Stream:  d.stream,
			Lane:    d.an.tail,
			Message: "stream disappeared upstream; retiring its lanes",
		})

		// Carry what the next incarnation will need. The cursor is only available when the tail
		// was actually assigned to this instance; a gated tail never read anything, so the next
		// incarnation starts from a fresh log position.
		s.mu.Lock()
		carry := retiredTail{gen: d.an.gen}
		if ls, ok := s.lanes[d.an.tail]; ok {
			carry.cursor = ls.cursor
		}
		s.retired[d.stream] = carry
		delete(s.announced, d.stream)
		delete(s.lanes, d.an.scan)
		delete(s.lanes, d.an.tail)
		s.rebuildRing()
		s.mu.Unlock()

		// B7: all of that retires the lanes' READING. The durable rows stay forever, and there
		// is no connector-reachable call that drops them, so the lane table grows with the
		// historical union of every stream that has ever existed — plus one extra row per
		// incarnation, because B4 forces a new lane name every time a stream comes back.
		// StateHandle.Delete would drop the state blob and orphan the row, which is worse.
	}

	s.mu.Lock()
	s.found = live
	s.lastSweep = time.Now()
	s.mu.Unlock()
	return nil
}

// hasLanesFor reports whether this instance already holds a lane for a stream.
//
// It answers only for lanes this worker HOLDS. It cannot answer "does a lane for this stream
// exist anywhere", because LaneCtl exposes no lane table (B4): a lane held by another worker,
// gated behind a scan, or finished is invisible here. Re-announcing is safe for the first two
// (Announce is idempotent on Name) and silently useless for the third.
func (s *Source) hasLanesFor(name record.StreamName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.announced[name]; ok {
		return true
	}
	for _, ls := range s.lanes {
		if ls.stream == name {
			return true
		}
	}
	return false
}

// announceFor announces the lane set one stream's sync mode needs.
func (s *Source) announceFor(ctx context.Context, ch *choice, st apiStream) error {
	lanes := s.rt.Lanes()
	cursorField := ch.cursorField
	if cursorField == "" {
		cursorField = st.DefaultCursor
	}

	switch ch.mode {
	case modeFullRefresh:
		// B4 + B6: a full_refresh pass must not reuse a finished lane's name, and there is
		// nowhere durable to keep the pass counter before a lane exists, so the pass id is
		// derived from the wall clock. The consequence is stated rather than hidden: a
		// full_refresh interrupted mid-pass resumes as a NEW pass on the next Open — every
		// restart restarts the scan — because the previous pass's lane name is unguessable
		// once it is no longer assigned.
		pass := uint64(time.Now().UTC().Truncate(time.Hour).Unix())
		name := fmt.Sprintf("scan:%s:p%d", st.Name, pass)
		id, err := lanes.Announce(ctx, connector.LaneSpec{
			Name:        name,
			Stream:      record.StreamName(st.Name),
			Kind:        connector.LaneKindScan,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Bounded,
			Group:       record.LaneGroup("scan:" + st.Name),
			Spec: mustBlob(laneSpecV1, laneSpecPayload{
				Stream: st.Name, Mode: ch.mode.String(), Kind: "scan",
				CursorField: cursorField, KeyFields: ch.keyFields, Pass: pass,
			}),
			Weight: st.Rows,
			Label:  fmt.Sprintf("full refresh of %s (pass %d)", st.Name, pass),
		})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.announced[record.StreamName(st.Name)] = announcedLanes{scan: id, gen: pass}
		s.mu.Unlock()
		return nil

	case modeIncremental, modeAppendDedup:
		// Capture the change-log position FIRST, so no change between the capture and the scan
		// can be missed, then announce the tail BEFORE the scan lanes, gated on the scan group.
		// The gate is core-enforced data, not a connector convention, which is the one thing
		// that makes this handoff work in a cluster.
		from, err := s.api.logPosition(ctx, st.Name)
		if err != nil {
			return fault.Transient(fault.OpOpen, err)
		}

		// B4, worked around at a cost. A tail lane's construction payload legitimately differs
		// between incarnations (it carries the change-log position captured at announce time),
		// and `Announce` returns fault.PermanentContract — WHICH STOPS THE PIPELINE — when a
		// name already exists with a different payload. Measured: a stream that disappears and
		// comes back fails the whole pipeline with
		//
		//	permanent_contract/open: lane "tail:t_0860" already exists with a different
		//	construction payload
		//
		// so the name must carry an incarnation number. That means a permanent new lane row per
		// disappearance (compounding B7), and it means the previous incarnation's cursor has to
		// be carried across by hand — from memory, because there is nowhere durable to keep it
		// (B6) and the old lane is invisible once finished (B4).
		var gen uint64
		s.mu.Lock()
		if r, ok := s.retired[record.StreamName(st.Name)]; ok {
			gen = r.gen + 1
			if r.cursor != "" {
				from = r.cursor
			}
			delete(s.retired, record.StreamName(st.Name))
		}
		s.mu.Unlock()

		scanGroup := record.LaneGroup("scan:" + st.Name)
		tailID, err := lanes.Announce(ctx, connector.LaneSpec{
			Name:        fmt.Sprintf("tail:%s:g%d", st.Name, gen),
			Stream:      record.StreamName(st.Name),
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       record.LaneGroup("tail:" + st.Name),
			StartAfter:  []record.LaneGroup{scanGroup},
			Spec: mustBlob(laneSpecV1, laneSpecPayload{
				Stream: st.Name, Mode: ch.mode.String(), Kind: "stream",
				CursorField: cursorField, KeyFields: ch.keyFields, From: from, Gen: gen,
			}),
			Label: fmt.Sprintf("%s change log from %s", st.Name, shorten(from)),
		})
		if err != nil {
			return err
		}

		scanID, err := lanes.Announce(ctx, connector.LaneSpec{
			Name:        "scan:" + st.Name,
			Stream:      record.StreamName(st.Name),
			Kind:        connector.LaneKindScan,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Bounded,
			Group:       scanGroup,
			Spec: mustBlob(laneSpecV1, laneSpecPayload{
				Stream: st.Name, Mode: ch.mode.String(), Kind: "scan",
				CursorField: cursorField, KeyFields: ch.keyFields,
			}),
			Weight: st.Rows,
			Label:  fmt.Sprintf("initial scan of %s", st.Name),
			// A scan wants a deeper in-flight window than a tail.
			Budget: 4000,
		})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.announced[record.StreamName(st.Name)] = announcedLanes{scan: scanID, tail: tailID, gen: gen}
		s.mu.Unlock()
		return nil

	default:
		return fault.Contract(fault.OpOpen, fmt.Errorf("stream %q has no sync mode", st.Name))
	}
}

// Read fills dst for exactly ONE lane.
//
// B1 IS ENTIRELY VISIBLE IN THIS METHOD. It cannot serve the 900 lanes it holds, because
// dst.Add() stamps identity from the batch's unexported allocator and there is no way to obtain
// or retarget the allocator for another lane. So:
//
//   - if dst.Lane names a lane this instance holds, that lane is served — the engine chose, and
//     the records' provenance is correct;
//   - otherwise a ready lane is chosen here, dst.Lane is rewritten, and every record in the
//     batch gets a FALSE Origin.Lane and a FALSE Origin.Stream while the ledger settles them
//     under the rewritten lane. That path is counted and Noted, because it is a correctness
//     defect this source cannot avoid;
//   - either way, if the chosen lane has nothing available, this returns an empty batch and a
//     nil error after idleWait rather than blocking. Blocking is what the contract asks for and
//     it is unimplementable here: one blocked lane starves the other 899, and Read is never
//     called concurrently with itself.
func (s *Source) Read(ctx context.Context, dst *record.Batch) error {
	if err := s.maybeSweep(ctx); err != nil {
		return err
	}
	s.refreshAssignment(ctx)

	ls, retargeted := s.pick(dst.Lane)
	if ls == nil {
		// Nothing assigned yet, or every lane is gated behind a scan. Not end of input: more
		// lanes will be announced.
		return s.idle(ctx, dst)
	}
	if retargeted {
		if s.retargets != nil {
			s.retargets.Add(1)
		}
		if !s.notedB1 {
			s.notedB1 = true
			s.rt.Note(connector.Event{
				At:       time.Now(),
				Kind:     connector.EventDegraded,
				Severity: fault.PermanentInternal,
				Lane:     ls.id,
				Stream:   ls.stream,
				Message:  "emitting records for a lane the batch was not stamped for",
				Detail: "record.Batch.Add stamps Origin.Lane and Origin.Stream from the batch's " +
					"allocator, and a source holding many lanes has no way to obtain the batch " +
					"for another one; provenance will disagree with settlement",
			})
		}
	}

	dst.Reset()
	dst.Lane = ls.id

	// Everything mutable on a laneState is shared between this read goroutine and the control
	// goroutine that runs Commit, Backlog and Heartbeat, so it is read and written under the one
	// mutex the Read contract predicts. kind, stream and id are immutable after restore.
	s.mu.Lock()
	cursor, scanDone := ls.cursor, ls.scanDone
	s.mu.Unlock()

	if scanDone {
		return s.idle(ctx, dst)
	}

	rows, next, done, err := s.api.fetch(ctx, string(ls.stream), ls.kind == connector.LaneKindScan,
		cursor, s.page)
	if err != nil {
		if isDropped(err) {
			// The stream vanished mid-read. Retire the lane rather than failing the pipeline:
			// 899 other streams are healthy.
			return s.retire(ctx, ls, dst)
		}
		if isDisconnected(err) {
			return fault.ErrNotConnected
		}
		return fault.Transient(fault.OpRead, err)
	}

	if len(rows) == 0 {
		if done && ls.kind == connector.LaneKindScan {
			s.mu.Lock()
			ls.scanDone = true
			s.mu.Unlock()
			dst.EndOfLane = true
			return nil // an empty final batch still carries EndOfLane
		}
		return s.idle(ctx, dst)
	}

	for i := range rows {
		if dst.Len() >= maxBatch {
			next = rows[i].Cursor
			done = false
			break
		}
		r := dst.Add()
		if r == nil {
			// The engine's hard cap. Stop and let the rest come next call; the cursor we report
			// must match what we actually emitted.
			next = rows[i].Cursor
			done = false
			break
		}
		s.emit(r, ls, &rows[i])
	}

	s.mu.Lock()
	ls.cursor = next
	ls.rows += uint64(dst.Len())
	total := ls.rows
	if done && ls.kind == connector.LaneKindScan {
		ls.scanDone = true
	}
	s.mu.Unlock()

	tok, err := blobOf(cursorV1, cursorPayload{
		V: cursorV1, Cursor: next, Pass: ls.pass, Rows: total,
	})
	if err != nil {
		return fault.Bug(fault.OpRead, err)
	}
	scalar := float64(total)
	dst.Position = record.Position{
		Token: tok,
		// A lexicographic cursor IS an order-preserving encoding for this upstream, which is
		// what unlocks mid-lane monotonicity assertions and position-fraction progress.
		Order:  []byte(next),
		Scalar: &scalar,
		// Every upstream page boundary is a legal resume point. A source that emitted
		// mid-transaction would set Safe false until the boundary.
		Safe:  true,
		At:    time.Now(),
		Label: fmt.Sprintf("%s @ %s", ls.stream, shorten(next)),
	}
	if done && ls.kind == connector.LaneKindScan {
		dst.EndOfLane = true
	}
	return nil
}

// emit fills one lent record slot.
func (s *Source) emit(r *record.Record, ls *laneState, row *apiRow) {
	r.EventTime = row.At

	// B1: Dest is settable, so ROUTING survives a retargeted batch even though Origin.Stream
	// does not. That asymmetry is exactly why the retarget path is a defect and not a fix: the
	// sink writes to the right place while every core mechanism keyed on Origin.Stream — dedupe
	// scope, per-stream attribution — sees the batch allocator's stream.
	r.Dest = ls.stream

	r.Payload = record.StructPayload(row.Fields)

	op := record.OpInsert
	switch {
	case ls.kind == connector.LaneKindScan:
		op = record.OpScanRead
	case row.Deleted:
		op = record.OpDelete
	case row.Updated:
		op = record.OpUpdate
	}

	keys := make([][]string, 0, len(ls.keyFields))
	for _, k := range ls.keyFields {
		keys = append(keys, strings.Split(k, "."))
	}
	after := r.Payload
	r.Change = &record.Change{
		Version:       record.ChangeVersion,
		Op:            op,
		Keys:          keys,
		After:         &after,
		AfterComplete: record.CompletenessComplete,
		TxID:          row.TxID,
		CommitTime:    row.At,
	}

	// B2, REPAIRED. record.Record.SetKey is the identity the core actually reads: Ref.Key,
	// both dedupe layers, Request.IdempotencyKey and every upsert destination. The stream is
	// part of the key because 900 streams share one dedupe namespace only if the key is
	// stream-qualified — design rule R5's collision, avoided at the source.
	r.SetKey(append(append([]byte(ls.stream), 0x1F), row.Key...))
	if row.Cursor != "" {
		// The upstream's own per-row cursor IS its event id here, which is idempotency layer
		// one. It is not the lane cursor: that is a per-stream watermark and lives on Position.
		r.SetUpstream([]byte(row.Cursor))
	}
	_ = r.Meta.Set(record.NSSource, "stream", record.String(string(ls.stream)))

	// B8: no schema ref. The upstream reports a schema per stream and it evolves at runtime,
	// and a source cannot register one, so a destination cannot create 900 tables.
	r.Schema = nil
}

// idle returns an empty batch after a bounded wait. See B1 on why blocking is not an option.
func (s *Source) idle(ctx context.Context, dst *record.Batch) error {
	t := time.NewTimer(idleWait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		// Cancellation means DRAIN, not abort: there is nothing buffered here, so the drain is
		// already complete and ctx.Err() is correct.
		return ctx.Err()
	case <-s.changes:
		s.refreshAssignment(ctx)
		return nil
	case <-t.C:
		return nil
	}
}

// retire finishes a lane whose stream has gone while it was being read.
func (s *Source) retire(ctx context.Context, ls *laneState, dst *record.Batch) error {
	if err := s.rt.Lanes().Finish(ctx, ls.id); err != nil {
		return err
	}
	s.rt.Note(connector.Event{
		At:      time.Now(),
		Kind:    connector.EventLaneFinished,
		Stream:  ls.stream,
		Lane:    ls.id,
		Message: "stream dropped mid-read; retiring its lane",
	})
	s.forget(ls)
	if ls.kind == connector.LaneKindScan {
		dst.EndOfLane = true
	}
	return nil
}

// forget drops a retired lane from this instance's map and remembers what the next incarnation
// of it will need: the incarnation number and the cursor to resume from.
func (s *Source) forget(ls *laneState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ls.kind == connector.LaneKindStream {
		s.retired[ls.stream] = retiredTail{gen: ls.gen, cursor: ls.cursor}
	}
	delete(s.lanes, ls.id)
	s.rebuildRing()
}

// pick resolves which lane this Read will serve.
//
// want is dst.Lane on entry, which is the lane the engine's allocator stamps for. When this
// instance holds it, it is served and provenance is correct. Otherwise a ready lane is chosen
// round-robin and the caller is told it retargeted.
func (s *Source) pick(want record.LaneID) (ls *laneState, retargeted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if want != "" {
		if l, ok := s.lanes[want]; ok && !s.rt.Lanes().Revoked(want) {
			return l, false
		}
	}
	for i := 0; i < len(s.ring); i++ {
		id := s.ring[s.next%len(s.ring)]
		s.next++
		l, ok := s.lanes[id]
		if !ok || s.rt.Lanes().Revoked(id) || l.scanDone {
			continue
		}
		return l, want != "" && want != id
	}
	return nil, false
}

// maybeSweep re-reconciles the stream set periodically, on the read goroutine.
func (s *Source) maybeSweep(ctx context.Context) error {
	s.mu.Lock()
	due := s.opened && time.Since(s.lastSweep) > maxDur(sweepEvery, s.poll)
	s.mu.Unlock()
	if !due {
		return nil
	}
	if err := s.sweep(ctx); err != nil {
		// A failed discovery is not a failed pipeline: keep reading the lanes we hold.
		s.rt.Log().Warn("stream discovery failed", "err", err)
		s.mu.Lock()
		s.lastSweep = time.Now()
		s.mu.Unlock()
	}
	return nil
}

// refreshAssignment re-reads the assigned set when the core signalled a change.
//
// Distribution is restart with a different subset, so this is the same code path as a warm
// Open: read Assigned, reconstruct from Spec plus Cursor.
func (s *Source) refreshAssignment(ctx context.Context) {
	select {
	case <-s.changes:
	default:
		return
	}
	s.changes = s.rt.Lanes().Changes()
	if _, err := s.reload(ctx); err != nil {
		s.rt.Log().Warn("assignment refresh failed", "err", err)
	}
}

func (s *Source) rebuildRing() {
	s.ring = s.ring[:0]
	for id := range s.lanes {
		s.ring = append(s.ring, id)
	}
	sort.Slice(s.ring, func(i, j int) bool { return s.ring[i] < s.ring[j] })
	if len(s.ring) > 0 {
		s.next %= len(s.ring)
	}
}

// Commit tells the upstream where this lane has got to.
//
// It runs on the control goroutine, concurrently with Read, which is why every read of laneState
// here takes the mutex. The core has already persisted its own cursor before this is called.
func (s *Source) Commit(ctx context.Context, a connector.Ack) error {
	s.mu.Lock()
	ls, ok := s.lanes[a.Lane]
	s.mu.Unlock()
	if !ok {
		// Commit for a lane we no longer hold. The core never delivers one for a revoked lane,
		// so this is a lane retired between the ack and here: nothing to advance.
		return nil
	}

	// Decode the position the core resolved. It is our own bytes, handed back.
	if a.Through.Token.IsZero() {
		return nil
	}
	var cp cursorPayload
	if err := json.Unmarshal(a.Through.Token.Bytes, &cp); err != nil {
		return fault.Contract(fault.OpCommitSource, fmt.Errorf("lane %q ack cursor: %w", a.Lane, err))
	}

	// This upstream keeps a retention window regardless of what we acknowledge, so advancing is
	// a latency question and not a correctness one, and Abandoned > 0 is safe to advance past. A
	// destructive upstream — one that prunes on commit — would refuse here instead, which the
	// core surfaces and never decides on the source's behalf.
	if cp.Cursor != "" && cp.Cursor != ls.acked {
		if err := s.api.ack(ctx, string(ls.stream), cp.Cursor); err != nil {
			if isDisconnected(err) {
				return fault.ErrNotConnected
			}
			// Escalated, not logged and dropped: canal's own cursor is already durable, so this
			// is a degraded upstream and not lost progress.
			return fault.Transient(fault.OpCommitSource, err)
		}
		s.mu.Lock()
		ls.acked = cp.Cursor
		s.mu.Unlock()
	}

	// The source-shaped second write, CAS- and epoch-fenced by the Persister.
	if err := s.persist.Commit(ctx, a); err != nil {
		return err
	}

	if a.LaneFinished {
		s.mu.Lock()
		delete(s.lanes, a.Lane)
		s.rebuildRing()
		s.mu.Unlock()
		s.api.close(string(ls.stream))
		// B7 again, at the one moment a connector could act on it: the lane is finished and
		// settled and durable, its row will never be needed again for a full_refresh pass that
		// has already rolled over, and there is no call that drops it.
	}
	return nil
}

// Close releases every per-stream session. Safe on a never-opened source, because the core calls
// it after a failed Open and after config validation.
func (s *Source) Close(ctx context.Context) error {
	if s.api == nil {
		return nil
	}
	return s.api.shutdown(ctx)
}

// ---- optional interfaces --------------------------------------------------------------------

// Discover enumerates every stream the upstream has. This is what populates the stream picker
// and makes drift a diff against a stored catalog.
func (s *Source) Discover(ctx context.Context) (connector.Catalog, error) {
	streams, err := s.api.list(ctx)
	if err != nil {
		return connector.Catalog{}, fault.Transient(fault.OpDiscover, err)
	}
	cat := connector.Catalog{At: time.Now(), Streams: make([]connector.StreamDesc, 0, len(streams))}
	for _, st := range streams {
		keys := make([][]string, 0, len(st.Keys))
		for _, k := range st.Keys {
			keys = append(keys, strings.Split(k, "."))
		}
		cat.Streams = append(cat.Streams, connector.StreamDesc{
			Name:   record.StreamName(st.Name),
			Schema: schemaOf(st),
			Keys:   keys,
			// The operator chooses among candidates; this upstream does not dictate.
			KeysFixed: false,
			Supports: []connector.LaneKind{
				connector.LaneKindScan,
				connector.LaneKindStream,
			},
			EstimatedRecords: st.Rows,
			Label:            st.Label,
		})
	}
	// A partial catalog says so rather than quietly under-reporting.
	cat.Truncated = s.api.listTruncated
	return cat, nil
}

// Choices backs the streams[].name field's picker. A named hook, not a callback, so it crosses a
// process boundary and so the core needs no knowledge that streams exist.
func (s *Source) Choices(ctx context.Context, hook string, partial *config.Config) ([]config.EnumValue, error) {
	if hook != "streams" {
		return nil, fault.Contract(fault.OpValidate, fmt.Errorf("unknown choice hook %q", hook))
	}
	if s.api == nil {
		s.api = newAPI(config.Must[string](partial, "endpoint"), "")
	}
	streams, err := s.api.list(ctx)
	if err != nil {
		return nil, fault.Transient(fault.OpValidate, err)
	}
	out := make([]config.EnumValue, 0, len(streams))
	for _, st := range streams {
		out = append(out, config.EnumValue{
			Value:       st.Name,
			Title:       st.Label,
			Description: fmt.Sprintf("about %d records", st.Rows),
		})
	}
	return out, nil
}

// Validate is tier two: it may do I/O and it returns ALL the diagnostics, each anchored to a
// field path.
func (s *Source) Validate(ctx context.Context) config.Diagnostics {
	var d config.Diagnostics
	streams, err := s.api.list(ctx)
	if err != nil {
		return d.Errorf(config.CodeUnreachable, []string{"endpoint"},
			"cannot list streams at this endpoint",
			"check the endpoint and the token; every other check needs this call to succeed")
	}
	live := map[string]bool{}
	for _, st := range streams {
		live[st.Name] = true
	}
	i := 0
	for _, name := range s.order {
		path := []string{"streams", fmt.Sprint(i), "name"}
		i++
		if !live[string(name)] {
			d = d.Errorf(config.CodeUnknownComponent, path,
				fmt.Sprintf("stream %q does not exist upstream", name),
				"pick one from the discovered catalog, or remove it")
		}
		if s.selected[name].mode == modeAppendDedup {
			// Not a warning about the operator's choice: a statement that the mode cannot be
			// honoured. See B2.
			d = d.Errorf(config.CodeCapability, []string{"streams", fmt.Sprint(i - 1), "sync_mode"},
				"append_dedup cannot be honoured: this source cannot populate Origin.Key",
				"record.Record exposes no SetKey, so the engine's keyed dedupe has nothing to key on")
		}
	}
	return d
}

// Backlog answers "how much is left" for one lane.
func (s *Source) Backlog(ctx context.Context, lane record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	ls, ok := s.lanes[lane]
	s.mu.Unlock()
	if !ok {
		return connector.Backlog{}, nil
	}
	n, exact, lag, err := s.api.remaining(ctx, string(ls.stream), ls.cursor)
	if err != nil {
		return connector.Backlog{}, fault.Transient(fault.OpRead, err)
	}
	b := connector.Backlog{Records: connector.Count(n), Exact: exact, AsOf: time.Now()}
	if lag > 0 {
		// Nil rather than zero when there is no event time: zero reads as "caught up".
		b.EventTimeLag = &lag
	}
	return b, nil
}

// Heartbeat keeps a quiet lane's upstream session alive so it does not pin retention.
//
// It cannot advance a cursor, by contract, which is what makes B9 unavoidable: a healthy quiet
// stream's checkpoint age rises forever.
func (s *Source) Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error {
	s.mu.Lock()
	ls, ok := s.lanes[lane]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	if err := s.api.keepalive(ctx, string(ls.stream)); err != nil {
		if isDisconnected(err) {
			return fault.ErrNotConnected
		}
		return fault.Transient(fault.OpRead, err)
	}
	return nil
}

// ---- small helpers --------------------------------------------------------------------------

func mustBlob(v uint32, p laneSpecPayload) record.Blob {
	b, err := json.Marshal(p)
	if err != nil {
		// A construction payload that cannot serialise is a programming error, and there is no
		// error return on the announce path's argument list. Encoding a struct of strings
		// cannot fail, so this is unreachable rather than swallowed.
		return record.Blob{Version: v}
	}
	return record.Blob{Version: v, Bytes: b}
}

func blobOf(v uint32, p cursorPayload) (record.Blob, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return record.Blob{}, err
	}
	return record.Blob{Version: v, Bytes: b}, nil
}

func shorten(s string) string {
	if len(s) <= 24 {
		return s
	}
	return s[:21] + "..."
}

func maxDur(a, b time.Duration) time.Duration {
	if b > a {
		return b
	}
	return a
}

func schemaOf(st apiStream) *schema.Schema {
	if len(st.Fields) == 0 {
		// Nil when not knowable without reading, rather than an empty schema.
		return nil
	}
	out := &schema.Schema{Open: true}
	for _, f := range st.Fields {
		out.Fields = append(out.Fields, schema.Field{
			Name:     f,
			Type:     schema.TypeString,
			Nullable: true,
		})
	}
	for _, k := range st.Keys {
		out.Keys = append(out.Keys, strings.Split(k, "."))
	}
	return out
}

// ---- the fake upstream ----------------------------------------------------------------------
//
// Deliberately real enough to make the threading honest: per-stream sessions, per-stream
// cursors, a stream set that churns, deletes, event times and a truncated list.

type apiStream struct {
	Name          string
	Label         string
	Rows          uint64
	Keys          []string
	Fields        []string
	DefaultCursor string
}

type apiRow struct {
	Key     string
	Cursor  string
	At      time.Time
	TxID    string
	Deleted bool
	Updated bool
	Fields  record.Map
}

type apiError struct {
	msg          string
	dropped      bool
	disconnected bool
}

func (e *apiError) Error() string { return e.msg }

func isDropped(err error) bool {
	e, ok := err.(*apiError)
	return ok && e.dropped
}

func isDisconnected(err error) bool {
	e, ok := err.(*apiError)
	return ok && e.disconnected
}

// api is the synthetic upstream. 900 streams, 40 of which come and go.
type api struct {
	endpoint string
	token    string

	mu       sync.Mutex
	sessions map[string]uint64
	acked    map[string]string
	epoch    int

	listTruncated bool
	connected     bool
}

const (
	streamCount   = 900
	volatileCount = 40
)

func newAPI(endpoint, token string) *api {
	return &api{
		endpoint: endpoint,
		token:    token,
		sessions: map[string]uint64{},
		acked:    map[string]string{},
	}
}

func (a *api) connect(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.connected = true
	return nil
}

func (a *api) list(ctx context.Context) ([]apiStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.epoch++
	epoch := a.epoch
	a.mu.Unlock()

	out := make([]apiStream, 0, streamCount)
	for i := 0; i < streamCount; i++ {
		// The last volatileCount streams exist only on even epochs: appearances and
		// disappearances while running.
		if i >= streamCount-volatileCount && epoch%2 == 1 {
			continue
		}
		name := fmt.Sprintf("t_%04d", i)
		out = append(out, apiStream{
			Name:          name,
			Label:         "table " + name,
			Rows:          uint64(1000 + i*7),
			Keys:          []string{"id"},
			Fields:        []string{"id", "payload", "updated_at"},
			DefaultCursor: "updated_at",
		})
	}
	return out, nil
}

func (a *api) logPosition(ctx context.Context, stream string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return fmt.Sprintf("log:%s:%020d", stream, time.Now().UnixNano()), nil
}

// fetch returns up to limit rows for one stream from cursor.
func (a *api) fetch(ctx context.Context, stream string, scan bool, cursor string, limit int) (rows []apiRow, next string, done bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, cursor, false, err
	}
	a.mu.Lock()
	if !a.connected {
		a.mu.Unlock()
		return nil, cursor, false, &apiError{msg: "not connected", disconnected: true}
	}
	seq := a.sessions[stream]
	a.mu.Unlock()

	if limit <= 0 {
		limit = 1
	}
	// A scan is bounded: three pages and it is finished. A tail is unbounded and quiet most of
	// the time, which is the whole point of the 900-stream case.
	pages := uint64(3)
	if scan && seq >= pages {
		return nil, cursor, true, nil
	}
	if !scan && seq%7 != 0 {
		a.mu.Lock()
		a.sessions[stream] = seq + 1
		a.mu.Unlock()
		return nil, cursor, false, nil
	}

	n := limit
	if scan {
		n = limit / 2
		if n == 0 {
			n = 1
		}
	}
	now := time.Now()
	rows = make([]apiRow, 0, n)
	for i := 0; i < n; i++ {
		id := seq*uint64(limit) + uint64(i)
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], id)
		rows = append(rows, apiRow{
			Key:     fmt.Sprintf("%s/%d", stream, id),
			Cursor:  fmt.Sprintf("%s:%020d", stream, id),
			At:      now.Add(-time.Duration(n-i) * time.Millisecond),
			TxID:    fmt.Sprintf("tx-%d", seq),
			Deleted: !scan && id%97 == 0,
			Updated: !scan && id%3 == 0,
			Fields: record.Map{
				"id":         record.String(fmt.Sprintf("%d", id)),
				"payload":    record.Bytes(key[:]),
				"updated_at": record.Time(now),
			},
		})
	}
	a.mu.Lock()
	a.sessions[stream] = seq + 1
	a.mu.Unlock()

	next = rows[len(rows)-1].Cursor
	done = scan && seq+1 >= pages
	return rows, next, done, nil
}

func (a *api) ack(ctx context.Context, stream, cursor string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.connected {
		return &apiError{msg: "not connected", disconnected: true}
	}
	a.acked[stream] = cursor
	return nil
}

func (a *api) keepalive(ctx context.Context, stream string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.connected {
		return &apiError{msg: "not connected", disconnected: true}
	}
	return nil
}

func (a *api) remaining(ctx context.Context, stream, cursor string) (uint64, bool, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, 0, err
	}
	a.mu.Lock()
	seq := a.sessions[stream]
	a.mu.Unlock()
	return 3000 - min64(seq*500, 3000), false, 250 * time.Millisecond, nil
}

func (a *api) close(stream string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, stream)
}

func (a *api) shutdown(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.connected = false
	a.sessions = map[string]uint64{}
	return nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// Compile-time assertions that this connector satisfies every interface it declares. The
// registry cross-checks the same thing at init and PANICS on a declared capability with no
// interface behind it, but a static assertion fails at build time instead of at first import.
var (
	_ connector.Source          = (*Source)(nil)
	_ connector.Discoverer      = (*Source)(nil)
	_ connector.BacklogReporter = (*Source)(nil)
	_ connector.Heartbeater     = (*Source)(nil)
	_ connector.Validator       = (*Source)(nil)
	_ connector.ChoiceProvider  = (*Source)(nil)
)
