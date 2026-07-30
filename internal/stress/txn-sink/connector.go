// Package txnsink is a HOSTILE STRESS CONNECTOR, not a shipping connector.
//
// It implements the worst two-phase-commit sink the design has to survive:
//
//   - Exactly-once by TRUE two-phase commit. Data is staged as files; publication is a
//     single atomic metadata commit against a table pointer.
//   - The metadata commit CAN TIME OUT AFTER SUCCEEDING SERVER-SIDE. Recovery must be
//     able to decide whether a prepared transaction landed, and must never double-commit.
//   - A MINIMUM-FILE-SIZE economics constraint (128 MiB by default). One staged file must
//     therefore span MANY source reads, MANY engine requests and MANY checkpoints, and
//     must survive restart mid-accumulation.
//
// It is written against canal's real interfaces and it compiles. The findings are argued
// in full in the FINDINGS block below; each one names the exact signature that blocks it.
//
// ============================================================================
// FINDINGS
// ============================================================================
//
// VERDICT: requires-core-change. Six findings. Two are documentation-only, three are
// additive, and one (BREAKAGE 1) is a genuine hole in the recovery path that this hostile
// case walks straight into and cannot be worked around honestly. NONE of the fixes breaks
// an already-written connector.
//
// The implementation compiles, vets clean, passes -race, and registers without a panic or
// a capability warning. connector_test.go drives the full hostile sequence: accumulate
// below the minimum across checkpoints, restart mid-accumulation, seal, meet a commit that
// lands but reports indeterminate, resolve it, and survive re-presentation of the same
// committable without double-committing.
//
// WHAT WORKS, AND IT IS THE MAJORITY. Stated first, because a false positive here costs
// real design churn and because these are non-obvious wins:
//
//	(W1) The three-phase ordering is exactly right for a commit that can time out after
//	     succeeding. Committer.Commit is called ONLY after the committable is durable in
//	     the checkpoint, and Committable.Handle is connector-authored bytes. That means a
//	     DETERMINISTIC transaction id can be derived from durable state (see txnIDFor),
//	     so "did my prepared transaction land?" is answerable after any crash, at any
//	     point, without a side ledger. Debezium-style two-store divergence is structurally
//	     impossible here. This is the single most valuable property of the design for this
//	     connector and it was not obvious until the implementation was written.
//
//	(W2) The subsuming contract plus DispositionRetryLater expresses min-file-size
//	     accumulation CLEANLY, and better than WriterState does. Every PrepareCommit mints
//	     a committable covering everything written so far to a partition; while the file is
//	     under the minimum, Commit answers RetryLater; the next checkpoint mints a
//	     longer-span committable that subsumes it. The covered records are therefore NEVER
//	     uncovered by a persisted committable even though the cursor advanced past them, so
//	     "canal reported delivered, destination never saw it" cannot happen for the
//	     accumulation window. DispositionAborted is also exactly the right value for a
//	     subsumed committable, because its documented meaning is "as if never triggered;
//	     artifacts retained" rather than "discard".
//
//	(W3) fault.Indeterminate + SinkCaps.Idempotent + Request.IdempotencyKey compose
//	     correctly. Nothing had to be faked on the Write path.
//
//	(W4) Committable.Lanes []LaneID (plural) is load-bearing and correct: one staged file
//	     for a Partitioner sink genuinely covers records from many lanes.
//
// ---------------------------------------------------------------------------
// BREAKAGE 1 (FATAL) — AbortStale has no per-item return channel, so the ONE
// question this hostile case is built around cannot be answered on the recovery path.
//
//	BLOCKING SIGNATURE:
//	    AbortStale(ctx context.Context, cs []Committable) error
//
// Compare its sibling:
//
//	Commit(ctx context.Context, cs []Committable) ([]CommitOutcome, error)
//
// Commit gets five closed dispositions, a per-item fault, and DispositionAlreadyCommitted
// specifically so a sink can say "this one landed, I discovered it rather than performed
// it". AbortStale gets a single naked error for the whole batch.
//
// AbortStale is called in exactly two situations, and BOTH are situations in which this
// sink's answer is per-item and is not "aborted":
//
//	(a) a recovered committable this sink "no longer recognises". For a warehouse whose
//	    commit can time out after succeeding, "I do not recognise this staged file"
//	    is indistinguishable from "this staged file was already published by the
//	    attempt that timed out". The correct answer is per item: LANDED (nothing to
//	    abort, and the records ARE durable) or NOT LANDED (abort and reclaim) or STILL
//	    IN DOUBT (do nothing, ask again).
//
//	(b) Committable.Expires has passed. For a staging sink the correct response to
//	    expiry is NOT "abort" — it is "publish it anyway, undersized, and take the
//	    economics hit", because the records it covers are already behind a committed
//	    cursor. Aborting them is silent data loss with the cursor past them. There is
//	    no disposition to express "I converted your expiry into a commit", and no way
//	    to say "item 3 of 4 is still in doubt, keep it pending".
//
// Returning nil is a lie that loses data (the core drops the committable from the
// pending set and the file is orphaned while canal has reported those records written).
// Returning an error is untargeted: the core cannot tell which of the four items it
// concerns, whether the rest were resolved, or whether to retry, stop or degrade — the
// core's behaviour on a non-nil AbortStale is not specified anywhere in the interface or
// in docs/architecture.md.
//
// This is not theoretical. Every timed-out commit in this connector sits in the pending
// set across multiple checkpoints; Expires is minted at PrepareCommit; so an in-doubt
// committable reaching its expiry is the NORMAL failure mode, not a corner.
//
// WORKAROUND ACTUALLY TAKEN IN THIS FILE (and it is ugly enough to be a finding, not a
// clean fit): abortStale resolves each item against the warehouse and, for anything not
// landed, ATTEMPTS THE COMMIT FROM INSIDE AbortStale — turning the abort path into a
// second commit path, which directly contradicts the method's documented contract — and
// then, when even that is indeterminate, returns a batch-level fault.Indeterminate that
// the core has no defined handling for. Separately, PrepareCommit sets Expires 30 days
// out purely to keep the core from ever calling AbortStale for reason (b). Neutering a
// core safety mechanism with a magic constant is the exact "silent skip with a warning
// ratio" the design says it exists to prevent.
//
//	SMALLEST FIX (additive, breaks nothing):
//	    type StaleResolver interface {
//	        ResolveStale(ctx context.Context, cs []Committable) ([]CommitOutcome, error)
//	    }
//	preferred by the core over AbortStale when present, reusing the existing five
//	dispositions verbatim: AlreadyCommitted = it landed, records durable, drop it;
//	Committed = I published it now; Aborted = reclaimed, records NOT durable, do not
//	advance; RetryLater = still in doubt, keep it pending; DeadLetter = route the covered
//	records. No new vocabulary, no change to Committer, no already-written connector
//	affected. The non-additive alternative — changing AbortStale's return type — is
//	cleaner and there are zero Committer implementations in tree to break, but it is a
//	signature change to a declared interface and so is the second choice.
//
// ---------------------------------------------------------------------------
// BREAKAGE 2 (MAJOR) — the recovery order omits RestoreState, and the
// PrepareCommit/SnapshotState order within one checkpoint is unstated. A
// Committer+WriterState sink can double-commit for either reason.
//
//	BLOCKING SIGNATURES:
//	    WriterState.RestoreState(ctx context.Context, bs []record.Blob) error
//	    WriterState.SnapshotState(ctx context.Context, id uint64) ([]record.Blob, error)
//
// The normative recovery order in Committer's doc comment and in docs/architecture.md
// §8.1 is:
//
//	Open -> AbortStale -> Commit(recovered) -> first Write
//
// RestoreState does not appear in it. That is not a documentation nit for this connector,
// it is the difference between correct and corrupt:
//
//   - AbortStale's own stated job is to discard committables "this sink no longer
//     recognises". Recognition IS restored writer state. If AbortStale runs before
//     RestoreState the sink recognises nothing, so a literal implementation aborts every
//     recovered committable — including ones whose commit landed.
//   - Symmetrically, within one checkpoint: if the core calls SnapshotState BEFORE
//     PrepareCommit, then a PrepareCommit that seals an accumulating file produces a
//     durable record whose WriterState says "upload U is open, resume appending at part
//     8" and whose Committables says "upload U is sealed, publish it". On recovery the
//     sink both republishes U and keeps appending to it. That is the double-commit the
//     case forbids, produced entirely by call order the interface never fixes.
//
// WORKAROUND TAKEN: every recovery entry point funnels through adoptLocked, a
// last-writer-wins merge keyed on upload id that prefers the sealed/longer view, and the
// sink is written to be order-insensitive across {RestoreState, AbortStale, Commit}. It
// works, it costs ~40 lines, and no connector author writing their first sink will think
// to do it — they will implement the documented order and ship the bug.
//
//	SMALLEST FIX (documentation only, no signature change, breaks nothing):
//	state normatively, in Committer's and WriterState's doc comments,
//	    recovery: Open -> RestoreState -> AbortStale -> Commit(recovered) -> first Write
//	    checkpoint: Flush(FlushCheckpoint) -> PrepareCommit(id) -> SnapshotState(id)
//	                -> persist ONE record -> Commit(...)
//	and that both PrepareCommit and SnapshotState are called for EVERY checkpoint id when
//	both interfaces are present. The relative order is the whole content of the fix.
//
// ---------------------------------------------------------------------------
// BREAKAGE 3 (MAJOR) — PrepareCommit takes a bare uint64, so a staging sink cannot
// learn that THIS checkpoint is the last one. Getting the reason code requires declaring
// Flusher, which relocates the operator-visible acknowledgement point.
//
//	BLOCKING SIGNATURE:
//	    PrepareCommit(ctx context.Context, id uint64) ([]Committable, error)
//
// A minimum-file-size sink must keep a file open across checkpoints, and must therefore
// be told when it MUST stop waiting: end of a bounded pipeline, or a graceful drain.
// Flush carries exactly that datum (FlushReason). PrepareCommit does not carry it, and
// cannot grow one, because it takes scalars rather than a struct — while the design's
// stated growth doctrine is the opposite (Opening's own doc comment: "A struct rather
// than parameters, so adding a field later is not a breaking change... This is the growth
// mechanism"). SnapshotState(ctx, id uint64) has the identical defect.
//
// The only way to obtain the reason is to declare Flusher purely as a signal carrier —
// which this file does. The cost is not cosmetic: Flusher's doc comment says "the core
// does not settle records on Write for a Flusher sink; it settles them on the Flush that
// covers them", and "the negotiated ack point is disclosed to the operator". This sink's
// writes ARE durable on return (each request becomes an uploaded part), so declaring
// Flushes=true tells the operator something weaker than the truth about where settlement
// happens, in exchange for a two-bit enum. A capability declaration that has to be lied
// about to obtain an unrelated signal is a capability declaration that has stopped
// meaning what it says.
//
//	SMALLEST FIX (additive, breaks nothing):
//	    type CommitPoint struct {
//	        ID       uint64
//	        Reason   FlushReason
//	        Deadline time.Time
//	    }
//	    type PointCommitter interface {
//	        PrepareCommitAt(ctx context.Context, cp CommitPoint) ([]Committable, error)
//	    }
//	preferred over PrepareCommit when present. Cleaner still, and viable today because
//	there are no Committer implementations in tree: change PrepareCommit and SnapshotState
//	to take CommitPoint directly, restoring the struct-argument doctrine on the one
//	surface that violates it.
//
// ---------------------------------------------------------------------------
// BREAKAGE 4 (MAJOR) — the concurrency contract is enumerated on the frozen Sink and
// never extended to the optional interfaces, so the only safe reading forces
// MaxConcurrency: 1.
//
//	BLOCKING TEXT, in Sink.Write's doc comment:
//	    "CONCURRENCY: up to SinkCaps.MaxConcurrency Write calls may be in flight on one
//	     Sink. Open, Flush and Close never run concurrently with Write."
//
// The list is exhaustive and names three methods. PrepareCommit, Commit, AbortStale,
// SnapshotState, RestoreState and ApplySchemaChange are not in it, and no other document
// covers them. PrepareCommit seals the very buffer Write appends to; SnapshotState reads
// the part list Write mutates. A connector author who assumes exclusion corrupts data if
// the core ever overlaps them; one who assumes overlap must serialise everything behind
// one mutex, which is what this file does, and which makes MaxConcurrency > 1 pointless.
// For a warehouse whose metadata commit takes seconds, holding that mutex across Commit
// stalls all ingest for the duration — so the ambiguity costs real throughput, not just
// clarity.
//
//	SMALLEST FIX (documentation only, breaks nothing): extend the sentence to
//	"Open, Flush, Close, PrepareCommit, Commit, AbortStale, SnapshotState, RestoreState
//	and ApplySchemaChange never run concurrently with Write or with each other, EXCEPT
//	Commit, which MAY overlap Write" — and if Commit may overlap, say so, because that is
//	the one a warehouse sink desperately needs and cannot assume.
//
// ---------------------------------------------------------------------------
// BREAKAGE 5 (MINOR) — Committable persists RecordID, which the record model says is
// never persisted; so DispositionDeadLetter's blast-radius promise is void on exactly
// the path where a 2PC failure matters.
//
//	BLOCKING SIGNATURE:
//	    type Committable struct { ...; FirstRec record.RecordID; LastRec record.RecordID }
//
// record.RecordID's own doc comment: "It is NOT durable and never appears in persisted
// state. Durable identity is [Origin].Key plus the lane cursor." Committable is persisted
// (engine.Checkpoint.Committables), and carries two of them. Disposition's doc comment
// promises "[DispositionDeadLetter] routes the covered records and does NOT advance the
// prefix past them".
//
// After a restart neither half holds. The RecordIDs belong to a dead generation and name
// nothing in the current ledger, so the covered records cannot be routed; and the prefix
// was already advanced past them by the checkpoint that persisted the committable — that
// is the protocol's own ordering. So for a recovered committable, which is the only kind
// this hostile case cares about, DispositionDeadLetter is unimplementable as documented,
// and this file never returns it for a recovered item.
//
//	SMALLEST FIX (additive field, breaks nothing): add
//	    Cursors map[record.LaneID]record.Position `json:"cursors,omitempty"`
//	to Committable, populated by the engine (not the connector) at PrepareCommit from the
//	lane cursors being persisted in the same record. Then dead-lettering a recovered
//	committable can name durable coordinates, and the docs can keep FirstRec/LastRec as
//	the explicitly same-generation, human-facing provenance they actually are.
//
// ---------------------------------------------------------------------------
// BREAKAGE 6 (MINOR) — Committable has no Attempt counter, so a sink cannot tell a
// first commit attempt from a re-presentation, and DispositionAlreadyCommitted becomes
// either a lie or a wasted round trip.
//
//	BLOCKING SIGNATURE:
//	    type Committable struct { Checkpoint uint64; Handle record.Blob; ... }   // no Attempt
//	CONTRAST, in the same package:
//	    type Request struct { ...; Attempt int }   // "1 on first delivery, incrementing on
//	    retry. A sink may switch to a safer, slower path on a late attempt."
//
// Request got exactly this field for exactly this reason. Committable did not. The
// checkpoint id is no substitute: a re-presentation after a lost confirmation carries the
// SAME id as the attempt that was lost, so nothing in the value distinguishes them.
//
// The consequences are not cosmetic. DispositionAlreadyCommitted's documented purpose is
// that the duplicate rate is visible — "a spike after restart is expected; a sustained one
// is a symptom". A sink with no attempt counter must either report Committed for a
// re-presentation (destroying that signal, and reporting a publication it did not perform)
// or probe the destination before every commit (a round trip on the happy path). THIS BUG
// WAS PRESENT IN THIS FILE UNTIL A SCENARIO TEST CAUGHT IT — see
// connector_test.go/TestHostileFlow, which asserts already_committed on re-presentation
// and got committed.
//
// WORKAROUND TAKEN: a process-local sealedHere set, cleared as soon as a commit is
// attempted, standing in for the missing counter. It is correct but it is per-connector
// bookkeeping for a fact the engine already knows.
//
//	SMALLEST FIX (additive field, breaks nothing): add
//	    Attempt int `json:"attempt"`
//	to Committable, engine-maintained, 1 on first presentation to Commit and incrementing
//	on every re-presentation including across a restart (it is persisted with the
//	committable, so it survives). Mirrors Request.Attempt exactly, including the "switch to
//	a safer, slower path on a late attempt" reading, which for this sink means "probe
//	before committing".
//
// ============================================================================
package txnsink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

func init() {
	registry.AddSink(registry.Default, registry.SinkDef[*sink]{
		Meta: registry.Meta{
			Name:    "stress_txn_warehouse",
			Version: "0.0.1-stress",
			Title:   "Two-phase-commit warehouse (stress)",
			Summary: "Stages files, publishes with one atomic metadata commit. Hostile stress connector.",
			Notes: "Stress connector. Declares Flushes only to receive FlushReason; its writes are " +
				"durable on return. See BREAKAGE 3.",
			Support: registry.SupportCommunity,
		},
		Spec: spec(),
		Caps: connector.SinkCaps{
			Caps: connector.Caps{APIVersion: connector.APIVersion},

			// FORCED TO 1 BY BREAKAGE 4. This sink can genuinely absorb parallel part
			// uploads; it cannot prove that PrepareCommit will not seal the buffer a
			// concurrent Write is appending to, so it serialises everything.
			MaxConcurrency: 1,

			// A part upload caps out well below the file minimum. The minimum itself is
			// NOT expressible here and correctly so: SinkCaps names maxima only, and a
			// batcher honouring a minimum would trade unbounded latency for it. The sink
			// accumulates instead. Not a breakage.
			MaxRequestBytes:   64 << 20,
			MaxRequestRecords: 500_000,

			Idempotent:     true,
			PartialFailure: false,

			Modes: []connector.DestMode{connector.DestAppend, connector.DestOverwrite},

			Flushes:    true, // Flusher   — signal carrier only, see BREAKAGE 3
			Partitions: true, // Partitioner
			Commits:    true, // Committer
			KeepsState: true, // WriterState

			// BREAKAGE 1, REPAIRED: connector.StaleResolver. The core prefers it over
			// AbortStale whenever it is present, so this sink's per-item answer — landed,
			// reclaimable, or still in doubt — is now expressible instead of being smuggled
			// through the abort path.
			ResolvesStale: true, // StaleResolver
		},
		New: newSink,
	})
}

func spec() *config.Spec {
	return config.NewSpec().
		Describe(
			"Stage-then-commit warehouse sink.",
			"Writes staged files to object storage and publishes them with a single atomic "+
				"metadata commit against the destination table.",
		).
		Field(config.Field{
			Name:        "table",
			Title:       "Destination table",
			Description: "Fully qualified destination table the metadata commit targets.",
			Type:        config.TypeString,
			Examples:    []any{"analytics.public.events"},
		}).
		Field(config.Field{
			Name:        "stage_uri",
			Title:       "Staging location",
			Description: "Object-store prefix staged files are uploaded to before they are published.",
			Type:        config.TypeString,
			Examples:    []any{"s3://warehouse-stage/events"},
		}).
		Field(config.Field{
			Name:  "min_file_bytes",
			Title: "Minimum staged file size",
			Description: "A staged file is not sealed until it reaches this size, so one file spans " +
				"many reads and many checkpoints. Small files cost far more to query than they cost to write.",
			Type:     config.TypeSize,
			Default:  "128MiB",
			Optional: true,
		}).
		Field(config.Field{
			Name:  "max_staging_age",
			Title: "Maximum staging age",
			Description: "An undersized file is sealed anyway once it has been open this long, so " +
				"visibility latency is bounded rather than unbounded.",
			Type:     config.TypeDuration,
			Default:  "15m",
			Optional: true,
		}).
		Field(config.Field{
			Name:        "part_bytes",
			Title:       "Upload part size",
			Description: "Buffered bytes are uploaded as a durable part once they reach this size.",
			Type:        config.TypeSize,
			Default:     "8MiB",
			Optional:    true,
			Advanced:    true,
		}).
		Field(config.Field{
			Name:  "commit_timeout",
			Title: "Metadata commit timeout",
			Description: "How long to wait for the atomic metadata commit. A timeout here is " +
				"INDETERMINATE: the commit may have succeeded server-side.",
			Type:     config.TypeDuration,
			Default:  "60s",
			Optional: true,
			Advanced: true,
		}).
		Example(config.Example{
			Title: "Publish 128 MiB files into a warehouse table",
			Config: map[string]any{
				"table":     "analytics.public.events",
				"stage_uri": "s3://warehouse-stage/events",
			},
		})
}

// ---------------------------------------------------------------------------
// The destination, as an interface so the connector logic above it is real
// ---------------------------------------------------------------------------

// warehouse is the vendor client, reduced to the five calls a stage-then-commit
// destination actually offers. Only Commit is atomic, and only Commit can lie about
// having failed.
type warehouse interface {
	// StartUpload opens a multipart upload and returns its id.
	StartUpload(ctx context.Context, uri string) (string, error)
	// UploadPart makes body durable server-side under (uploadID, number).
	UploadPart(ctx context.Context, uploadID string, number int, body []byte) (etag string, err error)
	// CompleteUpload seals the upload and returns the staged file's URI. The file is
	// durable and INVISIBLE to readers of the table until a metadata commit names it.
	CompleteUpload(ctx context.Context, uploadID string, parts []partRef) (uri string, err error)
	// AbortUpload reclaims an unsealed upload's storage.
	AbortUpload(ctx context.Context, uploadID string) error

	// Commit is the single atomic metadata commit: one table version, all files or none.
	// txnID makes it idempotent server-side WHEN the server sees it — which is exactly
	// what a timeout does not tell you.
	//
	// It may return errIndeterminate AFTER HAVING SUCCEEDED.
	Commit(ctx context.Context, table, txnID string, files []string) error

	// Resolve answers, per staged file URI, whether the table already references it.
	// This is the recovery oracle: without it, exactly-once against this destination is
	// not achievable by ANY interface design.
	Resolve(ctx context.Context, table string, files []string) (map[string]bool, error)
}

var (
	errIndeterminate = errors.New("warehouse: commit outcome unknown (timeout after send)")
	errPermanent     = errors.New("warehouse: request refused and will keep being refused")
)

type partRef struct {
	Number int    `json:"n"`
	ETag   string `json:"etag"`
}

// ---------------------------------------------------------------------------
// Durable payloads. Both are record.Blob bodies, so both obey the four-part format
// contract: additive-only, zero version means legacy, never reject a newer version
// blindly, stamp at serialise time.
// ---------------------------------------------------------------------------

const (
	handleVersion uint32 = 1
	stateVersion  uint32 = 1
)

// handleV1 is Committable.Handle's body: everything needed to (a) resume appending to an
// unsealed upload, (b) seal it, (c) publish it, and (d) ASK THE WAREHOUSE WHETHER IT WAS
// ALREADY PUBLISHED. (d) is the whole game, and it works because Handle is
// connector-authored and is durable before Commit is ever called.
type handleV1 struct {
	Partition string    `json:"partition"`
	Stream    string    `json:"stream"`
	UploadID  string    `json:"upload_id"`
	URI       string    `json:"uri"`
	Parts     []partRef `json:"parts"`
	Bytes     int64     `json:"bytes"`
	Records   int64     `json:"records"`
	Sealed    bool      `json:"sealed"`
	StagedURI string    `json:"staged_uri,omitempty"`
	OpenedAt  time.Time `json:"opened_at"`
}

func encodeHandle(h handleV1) (record.Blob, error) {
	b, err := json.Marshal(h)
	if err != nil {
		return record.Blob{}, err
	}
	// Version is stamped at serialise time, not at construction time.
	return record.Blob{Version: handleVersion, Bytes: b}, nil
}

func decodeHandle(b record.Blob) (handleV1, error) {
	var h handleV1
	if b.IsZero() {
		return h, errors.New("empty committable handle")
	}
	// Rule 3: never reject a newer version unless the encoding cannot tolerate it. JSON
	// with additive-only fields always can, so no version gate here at all.
	if err := json.Unmarshal(b.Bytes, &h); err != nil {
		return h, err
	}
	if h.UploadID == "" {
		return h, errors.New("committable handle names no upload")
	}
	return h, nil
}

type stateV1 struct {
	Checkpoint uint64     `json:"checkpoint"`
	Uploads    []handleV1 `json:"uploads"`
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

type upload struct {
	partition string
	stream    record.StreamName
	uploadID  string
	uri       string

	parts []partRef
	buf   []byte

	uploaded int64 // bytes made durable as parts
	records  int64
	firstRec record.RecordID
	lastRec  record.RecordID
	lanes    map[record.LaneID]struct{}
	openedAt time.Time

	sealed    bool
	stagedURI string
}

func (u *upload) size() int64 { return u.uploaded + int64(len(u.buf)) }

func (u *upload) handle() handleV1 {
	return handleV1{
		Partition: u.partition,
		Stream:    string(u.stream),
		UploadID:  u.uploadID,
		URI:       u.uri,
		Parts:     slices.Clone(u.parts),
		Bytes:     u.size(),
		Records:   u.records,
		Sealed:    u.sealed,
		StagedURI: u.stagedURI,
		OpenedAt:  u.openedAt,
	}
}

func (u *upload) laneList() []record.LaneID {
	out := make([]record.LaneID, 0, len(u.lanes))
	for l := range u.lanes {
		out = append(out, l)
	}
	slices.Sort(out)
	return out
}

type sink struct {
	table    string
	stageURI string
	minBytes int64
	partSize int64
	maxAge   time.Duration
	commitTO time.Duration

	wh warehouse
	rt connector.SinkRuntime

	// ONE mutex over everything, forced by BREAKAGE 4.
	mu sync.Mutex

	// open holds one accumulating upload per engine partition key. Unsealed uploads and
	// sealed-but-unpublished ones both live here; sealed ones are removed when Commit or
	// AbortStale resolves them.
	open map[string]*upload

	// sinceFlush counts records admitted since the last successful Flush, because
	// Flush's WriteResult must reconcile the same way Write's does.
	sinceFlush int64

	// finalize is set by Flush(FlushEndOfInput|FlushDrain) and read by the NEXT
	// PrepareCommit. It exists only because PrepareCommit has no reason parameter
	// (BREAKAGE 3).
	finalize bool

	// restored records the checkpoint id Open was resumed from, for events and logs.
	restored *uint64

	// sealedHere names uploads THIS PROCESS sealed and has not yet attempted to publish.
	// It is the only way to tell a first commit attempt from a re-presentation, because
	// Committable carries no attempt counter (BREAKAGE 6). Its absence means "probe the
	// destination before committing", which keeps DispositionAlreadyCommitted honest and
	// costs a round trip only on the recovery path.
	sealedHere map[string]bool

	seq int // upload-name counter, for the stub client

	stagedBytes   connector.Gauge
	stagedFiles   connector.Gauge
	inDoubtFiles  connector.Gauge
	oldestOpenAge connector.Gauge
}

func newSink(_ context.Context, cfg *config.Config) (*sink, error) {
	k := &sink{open: map[string]*upload{}, sealedHere: map[string]bool{}}

	k.table = config.Must[string](cfg, "table")
	k.stageURI = config.Must[string](cfg, "stage_uri")
	k.maxAge = config.Must[time.Duration](cfg, "max_staging_age")
	k.commitTO = config.Must[time.Duration](cfg, "commit_timeout")

	// A TypeSize field is validated but not normalised, so it is read as the operator
	// wrote it and parsed here.
	var err error
	if k.minBytes, err = config.ParseSize(config.Must[string](cfg, "min_file_bytes")); err != nil {
		return nil, err
	}
	if k.partSize, err = config.ParseSize(config.Must[string](cfg, "part_bytes")); err != nil {
		return nil, err
	}
	if err := cfg.Err(); err != nil {
		return nil, err
	}

	// New must not do I/O; the real client is constructed here, connected in Open.
	k.wh = &stubWarehouse{}
	return k, nil
}

// ---------------------------------------------------------------------------
// Sink — the three frozen methods
// ---------------------------------------------------------------------------

func (k *sink) Open(_ context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.rt = rt
	k.restored = o.Restored

	// A sink MAY assert on the negotiated tier. Refusing loudly beats writing wrong data.
	if o.Guarantee != connector.ExactlyOnce {
		k.rt.Note(connector.Event{
			At:       time.Now(),
			Kind:     connector.EventDowngrade,
			Severity: fault.PermanentContract,
			Message:  "two-phase warehouse sink is running below exactly-once",
			Detail: fmt.Sprintf("negotiated %s; staged files will still be published atomically "+
				"but duplicate publication is possible", o.Guarantee),
		})
	}
	for _, s := range o.Streams {
		if s.Mode == connector.DestUpsert || s.Mode == connector.DestDelete {
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"stream %q is configured for %s; this sink appends staged files and cannot express "+
					"row-level upsert or delete", s.Stream, s.Mode))
		}
	}

	m := rt.Metrics()
	var err error
	if k.stagedBytes, err = m.Gauge("staged_bytes", "partition"); err != nil {
		return fault.Internal(fault.OpOpen, err)
	}
	if k.stagedFiles, err = m.Gauge("staged_files"); err != nil {
		return fault.Internal(fault.OpOpen, err)
	}
	if k.inDoubtFiles, err = m.Gauge("in_doubt_files"); err != nil {
		return fault.Internal(fault.OpOpen, err)
	}
	// Bounding visibility latency is this connector's own job: nothing in the interface
	// bounds how long a sink may hold records the core has already committed past, and
	// the only disclosure channel is a gauge the connector registers itself.
	if k.oldestOpenAge, err = m.Gauge("oldest_open_file_age_seconds"); err != nil {
		return fault.Internal(fault.OpOpen, err)
	}
	return nil
}

// Write appends one request to the accumulating file for its partition.
//
// Each request's bytes become part of an upload part, and a part is DURABLE SERVER-SIDE
// once uploaded — which is why this method can honestly return AllWritten. What it must
// NOT claim is visibility: the records are not readable at the destination until a
// metadata commit names the sealed file. That gap is covered by a committable in every
// checkpoint (see PrepareCommit), never by an uncovered window.
func (k *sink) Write(ctx context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.WriteResult{}, nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	u, err := k.uploadForLocked(ctx, req)
	if err != nil {
		return connector.WriteResult{}, err
	}

	// The engine does not retain the Request past this return, and neither may we. The
	// body is copied; the Refs are reduced to the two record ids and the lane set the
	// committable will need MANY checkpoints from now.
	u.buf = append(u.buf, req.Body...)
	for i := range req.Records {
		r := req.Records[i]
		if u.records == 0 && i == 0 && u.firstRec == 0 {
			u.firstRec = r.ID
		}
		if r.ID < u.firstRec || u.firstRec == 0 {
			u.firstRec = r.ID
		}
		if r.ID > u.lastRec {
			u.lastRec = r.ID
		}
		if r.Lane != "" {
			u.lanes[r.Lane] = struct{}{}
		}
	}
	u.records += int64(req.Count)

	for int64(len(u.buf)) >= k.partSize {
		n := k.partSize
		if err := k.uploadPartLocked(ctx, u, u.buf[:n]); err != nil {
			// The part upload either landed or it did not; the client tells us which, and
			// we never launder an unknown into a Transient.
			return connector.WriteResult{}, err
		}
		u.buf = append(u.buf[:0], u.buf[n:]...)
	}

	k.sinceFlush += int64(req.Count)
	k.observeLocked()
	return connector.AllWritten(req.Count), nil
}

func (k *sink) Close(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.wh == nil {
		return nil
	}

	// Close CANNOT mint a committable and CANNOT safely publish: a commit issued here is
	// covered by no checkpoint, so if it timed out nothing durable would record that a
	// transaction is in doubt. So Close only makes the buffered tail durable as a part
	// and leaves everything for the next Open's recovery to resolve. This is why
	// FlushEndOfInput/FlushDrain (and therefore Flusher, and therefore BREAKAGE 3) are
	// load-bearing rather than a nicety: they are the last point at which sealing is
	// still covered by a checkpoint.
	var first error
	for _, u := range k.open {
		if u.sealed || len(u.buf) == 0 {
			continue
		}
		if err := k.uploadPartLocked(ctx, u, u.buf); err != nil && first == nil {
			first = err
			continue
		}
		u.buf = u.buf[:0]
	}
	return first
}

// ---------------------------------------------------------------------------
// Flusher — declared for the reason code, not for the ack point. See BREAKAGE 3.
// ---------------------------------------------------------------------------

func (k *sink) Flush(ctx context.Context, reason connector.FlushReason) (connector.WriteResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	switch reason {
	case connector.FlushEndOfInput, connector.FlushDrain:
		// The ONLY channel through which this sink can learn that no further checkpoint
		// will arrive, and therefore that an undersized file must be sealed anyway.
		k.finalize = true
	}

	for _, u := range k.open {
		if u.sealed || len(u.buf) == 0 {
			continue
		}
		if err := k.uploadPartLocked(ctx, u, u.buf); err != nil {
			return connector.WriteResult{}, err
		}
		u.buf = u.buf[:0]
	}

	n := k.sinceFlush
	k.sinceFlush = 0
	k.observeLocked()
	return connector.WriteResult{Written: n}, nil
}

// ---------------------------------------------------------------------------
// Partitioner — one accumulating file per destination stream.
// ---------------------------------------------------------------------------

func (k *sink) Partition(r *record.Record) (string, error) {
	if r == nil {
		return "", fault.Bug(fault.OpWrite, errors.New("nil record handed to Partition"))
	}
	return string(r.Dest), nil
}

// ---------------------------------------------------------------------------
// Committer
// ---------------------------------------------------------------------------

// PrepareCommit mints one committable per accumulating partition, covering EVERYTHING
// written to that partition since its last published file.
//
// It mints one even when the file is far below the minimum size, and that is the design's
// own subsuming contract doing exactly what it promises: the covered records are never
// left uncovered by a persisted committable even though the cursor has advanced past
// them, and the next checkpoint's committable subsumes this one with a longer span.
// Without that property, min-file-size accumulation would mean "canal says delivered,
// destination cannot see it, and nothing durable knows".
func (k *sink) PrepareCommit(ctx context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
	id := p.ID
	k.mu.Lock()
	defer k.mu.Unlock()

	out := make([]connector.Committable, 0, len(k.open))
	for _, u := range k.open {
		if u.records == 0 && !u.sealed {
			continue
		}
		if !u.sealed {
			if len(u.buf) > 0 {
				if err := k.uploadPartLocked(ctx, u, u.buf); err != nil {
					return nil, err
				}
				u.buf = u.buf[:0]
			}
			if k.shouldSealLocked(u) {
				uri, err := k.wh.CompleteUpload(ctx, u.uploadID, u.parts)
				if err != nil {
					return nil, classify(fault.OpPrepare, err)
				}
				u.sealed, u.stagedURI = true, uri
				if k.sealedHere == nil {
					k.sealedHere = map[string]bool{}
				}
				k.sealedHere[u.uploadID] = true
			}
		}

		h, err := encodeHandle(u.handle())
		if err != nil {
			return nil, fault.Bug(fault.OpPrepare, err)
		}
		out = append(out, connector.Committable{
			Checkpoint: id,
			Handle:     h,
			Lanes:      u.laneList(),
			FirstRec:   u.firstRec,
			LastRec:    u.lastRec,
			Records:    u.records,

			// THIRTY DAYS, AND THIS IS A WORKAROUND, NOT A POLICY. Expires is the core's
			// only escalation for a staged artifact sitting unpublished, and its escalation
			// is AbortStale — which for this sink is the one thing that must not happen to
			// an in-doubt or an undersized file (BREAKAGE 1). Until AbortStale can be
			// answered per item, the safe value is one the core will never reach, and the
			// real bound is enforced privately by max_staging_age in shouldSealLocked.
			Expires: time.Now().Add(30 * 24 * time.Hour),
		})
	}
	k.finalize = false
	k.observeLocked()
	return out, nil
}

// Commit publishes every SEALED committable in cs with ONE atomic metadata commit, and
// answers RetryLater for the ones still accumulating.
//
// The transaction id is derived deterministically from the sorted set of staged file URIs
// (txnIDFor). Every input to that derivation is already durable in the checkpoint before
// this method is ever called, so a retry after ANY crash reproduces the same id — which
// is what makes "did my prepared transaction land?" answerable at all.
func (k *sink) Commit(ctx context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	out := make([]connector.CommitOutcome, 0, len(cs))

	// Fold the pending set down to one entry per upload, highest checkpoint wins. The
	// core may legitimately hand us several committables for the same file: a re-minted
	// longer-span one plus the shorter one it subsumes.
	best := map[string]entry{}
	var order []string
	for _, c := range cs {
		h, err := decodeHandle(c.Handle)
		if err != nil {
			// An unreadable handle is a structural disagreement, not a record problem.
			out = append(out, connector.CommitOutcome{
				Handle:      c.Handle,
				Disposition: connector.DispositionAborted,
				Fault:       fault.Contract(fault.OpCommitSink, err),
			})
			continue
		}
		k.adoptLocked(h)
		prev, seen := best[h.UploadID]
		if !seen {
			order = append(order, h.UploadID)
			best[h.UploadID] = entry{c, h}
			continue
		}
		if c.Checkpoint >= prev.c.Checkpoint {
			// The superseded one is Aborted, whose documented meaning is precisely "as if
			// never triggered, artifacts retained" — not "discard".
			out = append(out, subsumed(prev.c))
			best[h.UploadID] = entry{c, h}
		} else {
			out = append(out, subsumed(c))
		}
	}

	var files []string
	var winners []entry
	var suspect []entry
	for _, id := range order {
		e := best[id]
		if e.h.Sealed && !k.sealedHere[e.h.UploadID] {
			// Either this process did not seal it (a recovered committable) or it has
			// already been through one commit attempt whose outcome may have been lost.
			// Either way the destination — not this process — is the authority on whether
			// it already landed, and answering Committed without asking would destroy the
			// duplicate-rate signal that DispositionAlreadyCommitted exists to carry.
			suspect = append(suspect, e)
			continue
		}
		if !e.h.Sealed {
			out = append(out, connector.CommitOutcome{
				Handle:      e.c.Handle,
				Disposition: connector.DispositionRetryLater,
				Fault: &fault.Fault{
					Class: fault.TransientInternal,
					Op:    fault.OpCommitSink,
					User: fmt.Sprintf("accumulating %s of the %s minimum file size for %q; "+
						"it will be published when it is full or after %s",
						bytesHuman(e.h.Bytes), bytesHuman(k.minBytes), e.h.Stream, k.maxAge),
					Dev: "upload " + e.h.UploadID + " is unsealed",
				},
			})
			continue
		}
		files = append(files, e.h.StagedURI)
		winners = append(winners, e)
	}

	// Resolve the suspects before touching the table. Anything already referenced is
	// durable and done; anything not is folded into this attempt's commit set, so a lost
	// confirmation costs one probe rather than a second table version.
	if len(suspect) > 0 {
		known, unknown, ferr := k.probeLocked(ctx, suspect)
		out = append(out, known...)
		if ferr != nil {
			for _, e := range unknown {
				out = append(out, connector.CommitOutcome{
					Handle:      e.c.Handle,
					Disposition: connector.DispositionRetryLater,
					Fault:       fault.Unknown(fault.OpCommitSink, ferr),
				})
			}
		} else {
			for _, e := range unknown {
				files = append(files, e.h.StagedURI)
				winners = append(winners, e)
			}
		}
	}

	if len(winners) == 0 {
		k.observeLocked()
		return out, nil
	}

	// From here on every winner has had a commit ATTEMPTED, so none of them may be
	// treated as fresh again.
	for _, e := range winners {
		delete(k.sealedHere, e.h.UploadID)
	}
	slices.Sort(files)
	txn := txnIDFor(k.table, files)

	cctx, cancel := context.WithTimeout(ctx, k.commitTO)
	err := k.wh.Commit(cctx, k.table, txn, files)
	cancel()

	switch {
	case err == nil:
		for _, e := range winners {
			delete(k.open, e.h.Partition)
			out = append(out, connector.CommitOutcome{Handle: e.c.Handle, Disposition: connector.DispositionCommitted})
		}

	case errors.Is(err, errPermanent):
		// A permanent refusal on a SAME-GENERATION committable can be dead-lettered,
		// because FirstRec/LastRec still name live records. For a RECOVERED committable it
		// cannot (BREAKAGE 5), and recovered items are indistinguishable here — Committable
		// carries no "which generation minted me" — so this path stays conservative and
		// never claims to have routed records it cannot name.
		for _, e := range winners {
			out = append(out, connector.CommitOutcome{
				Handle:      e.c.Handle,
				Disposition: connector.DispositionRetryLater,
				Fault:       fault.Permanent(fault.OpCommitSink, err),
			})
		}
		k.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventDegraded, Severity: fault.PermanentUpstream,
			Message: "metadata commit refused; staged files retained",
			Detail:  fmt.Sprintf("%d file(s) staged for %s, txn %s: %v", len(files), k.table, txn, err),
		})

	default:
		// THE HOSTILE CASE. The commit may have succeeded server-side. Ask the warehouse
		// which files the table now references, per file rather than per transaction,
		// because the retry set may differ from this attempt's set.
		out = append(out, k.resolveLocked(ctx, winners, err)...)
	}

	k.observeLocked()
	return out, nil
}

// ResolveStale is BREAKAGE 1's repair, and it is the method this connector needed all along.
//
// The two situations that bring the core here are (a) a committable the sink no longer
// recognises and (b) an expired committable, and in both the honest answer for a warehouse
// whose commit can time out AFTER SUCCEEDING is per item and is usually not "aborted". This
// implementation says exactly what it knows, per committable, using only the five
// dispositions that already existed:
//
//	unreadable handle          -> Aborted            (names no artifact; nothing to lose)
//	sealed and landed          -> AlreadyCommitted   (a previous attempt finished it)
//	sealed, not landed, committed here -> Committed  (published on this pass)
//	unsealed, sealed and published now -> Committed
//	unsealed, unsealable       -> DeadLetter         (records named; prefix must NOT advance)
//	warehouse unreachable      -> RetryLater         (in doubt; keep it pending)
//
// The old body could express none of that. It resolved against the warehouse and then
// ATTEMPTED THE COMMIT FROM INSIDE THE ABORT PATH, because the alternatives were silently
// orphaning the file (return nil) or failing the whole batch when three of four items were
// resolvable. AbortStale is kept below, delegating, so a core that has not learned about
// StaleResolver still gets the best answer this sink can give through one error.
func (k *sink) ResolveStale(ctx context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	outs := make([]connector.CommitOutcome, 0, len(cs))
	emit := func(c connector.Committable, d connector.Disposition, f *fault.Fault) {
		outs = append(outs, connector.CommitOutcome{Handle: c.Handle, Disposition: d, Fault: f})
	}

	type pending struct {
		c connector.Committable
		h handleV1
	}
	var sealed []pending

	for _, c := range cs {
		h, err := decodeHandle(c.Handle)
		if err != nil {
			// An unreadable handle names no artifact: nothing to reclaim, nothing to publish.
			// This one really is abortable, and now it can be said about this one alone.
			emit(c, connector.DispositionAborted, nil)
			continue
		}
		k.adoptLocked(h)
		if h.Sealed {
			sealed = append(sealed, pending{c: c, h: h})
			continue
		}

		// An unsealed upload was never published, so reclaiming its storage would be safe —
		// except that its records are behind a committed cursor, so dropping them is data
		// loss. Seal and publish if possible; DEAD-LETTER, not abort, if not, which is the
		// disposition that routes the covered records and refuses to advance the prefix.
		uri, err := k.wh.CompleteUpload(ctx, h.UploadID, h.Parts)
		if err != nil {
			if aerr := k.wh.AbortUpload(ctx, h.UploadID); aerr != nil {
				emit(c, connector.DispositionRetryLater, classify(fault.OpCommitSink, aerr))
				continue
			}
			delete(k.open, h.Partition)
			emit(c, connector.DispositionDeadLetter, fault.Contract(fault.OpCommitSink, fmt.Errorf(
				"upload %s held %d record(s) that could not be published: %w", h.UploadID, h.Records, err)))
			continue
		}
		h.Sealed, h.StagedURI = true, uri
		k.adoptLocked(h)
		sealed = append(sealed, pending{c: c, h: h})
	}

	if len(sealed) == 0 {
		return outs, nil
	}

	files := make([]string, 0, len(sealed))
	for _, pd := range sealed {
		files = append(files, pd.h.StagedURI)
	}
	slices.Sort(files)

	landed, err := k.wh.Resolve(ctx, k.table, files)
	if err != nil {
		// STILL IN DOUBT, and now that is a sentence. RetryLater keeps them pending rather
		// than forcing a choice between losing them and duplicating them.
		f := fault.Unknown(fault.OpCommitSink, fmt.Errorf(
			"cannot determine whether %d staged file(s) were published: %w", len(files), err))
		for _, pd := range sealed {
			emit(pd.c, connector.DispositionRetryLater, f)
		}
		return outs, nil
	}

	var missing []string
	var missingPend []pending
	for _, pd := range sealed {
		if landed[pd.h.StagedURI] {
			delete(k.open, pd.h.Partition)
			delete(k.sealedHere, pd.h.UploadID)
			emit(pd.c, connector.DispositionAlreadyCommitted, nil)
			continue
		}
		missing = append(missing, pd.h.StagedURI)
		missingPend = append(missingPend, pd)
	}
	if len(missing) == 0 {
		return outs, nil
	}

	slices.Sort(missing)
	cctx, cancel := context.WithTimeout(ctx, k.commitTO)
	err = k.wh.Commit(cctx, k.table, txnIDFor(k.table, missing), missing)
	cancel()
	if err != nil {
		// The commit itself may have landed after timing out, which is the whole reason this
		// interface exists. RetryLater plus Indeterminate is the truth.
		f := classify(fault.OpCommitSink, err)
		for _, pd := range missingPend {
			emit(pd.c, connector.DispositionRetryLater, f)
		}
		return outs, nil
	}
	for _, pd := range missingPend {
		delete(k.open, pd.h.Partition)
		delete(k.sealedHere, pd.h.UploadID)
		emit(pd.c, connector.DispositionCommitted, nil)
	}
	return outs, nil
}

// AbortStale remains, delegating to ResolveStale and collapsing its per-item answer into the
// one error this signature allows. It is what a core that does not know about StaleResolver
// gets, and it is strictly worse: RetryLater and DeadLetter both become "the batch failed".
func (k *sink) AbortStale(ctx context.Context, cs []connector.Committable) error {
	outs, err := k.ResolveStale(ctx, cs)
	if err != nil {
		return err
	}
	for _, o := range outs {
		switch o.Disposition {
		case connector.DispositionRetryLater, connector.DispositionDeadLetter:
			if o.Fault != nil {
				return o.Fault
			}
			return fault.Unknown(fault.OpCommitSink, fmt.Errorf(
				"a staged artifact is %s and this signature cannot say so per item", o.Disposition))
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// WriterState
// ---------------------------------------------------------------------------

// SnapshotState records every accumulating upload so a restart resumes the same file
// rather than starting a new, undersized one.
//
// It is deliberately IDEMPOTENT AND ORDER-INSENSITIVE with respect to PrepareCommit: it
// snapshots sealed uploads too, marked sealed. If the core calls it before PrepareCommit,
// the snapshot says "open"; if after, "sealed"; adoptLocked reconciles either against the
// recovered committables. That reconciliation exists solely because the order is unstated
// (BREAKAGE 2) and a first-time connector author would not write it.
func (k *sink) SnapshotState(_ context.Context, id uint64) ([]record.Blob, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	st := stateV1{Checkpoint: id, Uploads: make([]handleV1, 0, len(k.open))}
	for _, u := range k.open {
		if u.records == 0 && !u.sealed {
			continue
		}
		st.Uploads = append(st.Uploads, u.handle())
	}
	slices.SortFunc(st.Uploads, func(a, b handleV1) int {
		if a.UploadID < b.UploadID {
			return -1
		}
		if a.UploadID > b.UploadID {
			return 1
		}
		return 0
	})
	b, err := json.Marshal(st)
	if err != nil {
		return nil, fault.Bug(fault.OpPersist, err)
	}
	return []record.Blob{{Version: stateVersion, Bytes: b}}, nil
}

func (k *sink) RestoreState(_ context.Context, bs []record.Blob) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, b := range bs {
		if b.IsZero() {
			continue
		}
		var st stateV1
		if err := json.Unmarshal(b.Bytes, &st); err != nil {
			// A structural disagreement about our own state format, named with the version.
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"writer state version %d is unreadable by format version %d: %w",
				b.Version, stateVersion, err))
		}
		for _, h := range st.Uploads {
			k.adoptLocked(h)
		}
	}
	k.observeLocked()
	return nil
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

// adoptLocked merges one durable handle into the live upload table, last-and-widest wins.
//
// THIS FUNCTION IS BREAKAGE 2's WORKAROUND. Every recovery entry point — RestoreState,
// AbortStale, Commit — funnels through it, so the sink behaves identically whichever
// order the core chooses, and a handle that says "sealed" can never be overwritten by an
// older one that says "open" (which is the double-commit).
func (k *sink) adoptLocked(h handleV1) {
	cur, ok := k.open[h.Partition]
	if ok {
		switch {
		case cur.uploadID != h.UploadID:
			// Two different uploads claim one partition. Keep the sealed one if either is
			// sealed, else the one with more durable bytes; the loser is orphaned storage
			// that the warehouse's own lifecycle policy reclaims, and there is no interface
			// channel through which to report it.
			if cur.sealed || (!h.Sealed && cur.size() >= h.Bytes) {
				return
			}
		case cur.sealed && !h.Sealed:
			return
		case cur.sealed == h.Sealed && cur.size() >= h.Bytes:
			return
		}
	}

	u := &upload{
		partition: h.Partition,
		stream:    record.StreamName(h.Stream),
		uploadID:  h.UploadID,
		uri:       h.URI,
		parts:     slices.Clone(h.Parts),
		uploaded:  h.Bytes,
		records:   h.Records,
		lanes:     map[record.LaneID]struct{}{},
		openedAt:  h.OpenedAt,
		sealed:    h.Sealed,
		stagedURI: h.StagedURI,
	}
	if u.openedAt.IsZero() {
		u.openedAt = time.Now()
	}
	if ok {
		// Lane and record-range provenance from this generation is strictly better than
		// the persisted RecordIDs, which name a dead generation (BREAKAGE 5).
		for l := range cur.lanes {
			u.lanes[l] = struct{}{}
		}
		u.firstRec, u.lastRec = cur.firstRec, cur.lastRec
	}
	k.open[h.Partition] = u
}

// entry pairs a committable with its decoded handle. Package-scoped because both Commit
// and resolveLocked deal in it.
type entry struct {
	c connector.Committable
	h handleV1
}

func (k *sink) uploadForLocked(ctx context.Context, req *connector.Request) (*upload, error) {
	p := req.Partition
	if u, ok := k.open[p]; ok && !u.sealed {
		return u, nil
	}
	if u, ok := k.open[p]; ok && u.sealed {
		// A sealed file is waiting for its metadata commit; a new one accumulates beside
		// it. One accumulating upload per partition is all this table can hold, so a
		// sealed-but-unpublished file blocks. Bounded by Commit resolving it, and the
		// alternative — an unbounded set of pending files per partition — is what the
		// pending-set size limits exist to avoid.
		return nil, fault.Internal(fault.OpWrite, fmt.Errorf(
			"partition %q has a sealed file awaiting its metadata commit", p))
	}

	stream := record.DefaultStream
	if len(req.Records) > 0 {
		stream = req.Records[0].Stream
	}
	k.seq++
	uri := fmt.Sprintf("%s/%s/part-%06d", k.stageURI, p, k.seq)
	id, err := k.wh.StartUpload(ctx, uri)
	if err != nil {
		return nil, classify(fault.OpWrite, err)
	}
	u := &upload{
		partition: p,
		stream:    stream,
		uploadID:  id,
		uri:       uri,
		lanes:     map[record.LaneID]struct{}{},
		openedAt:  time.Now(),
	}
	k.open[p] = u
	return u, nil
}

func (k *sink) uploadPartLocked(ctx context.Context, u *upload, body []byte) error {
	n := len(u.parts) + 1
	etag, err := k.wh.UploadPart(ctx, u.uploadID, n, body)
	if err != nil {
		return classify(fault.OpWrite, err)
	}
	u.parts = append(u.parts, partRef{Number: n, ETag: etag})
	u.uploaded += int64(len(body))
	return nil
}

func (k *sink) shouldSealLocked(u *upload) bool {
	switch {
	case u.size() >= k.minBytes:
		return true
	case k.finalize:
		return true
	case k.maxAge > 0 && time.Since(u.openedAt) >= k.maxAge:
		// Private bound on visibility latency. The core bounds a COMMITTABLE's staging
		// time through Expires; it does not bound this, and every sink of this shape must
		// re-invent the bound with its own knob and its own bugs.
		return true
	default:
		return false
	}
}

// probeLocked asks the destination which of es already landed. The ones that did become
// DispositionAlreadyCommitted outcomes; the rest are returned for this attempt's commit
// set. On a probe failure nothing is assumed either way.
func (k *sink) probeLocked(ctx context.Context, es []entry) (known []connector.CommitOutcome, unknown []entry, err error) {
	files := make([]string, 0, len(es))
	for _, e := range es {
		files = append(files, e.h.StagedURI)
	}
	slices.Sort(files)

	landed, err := k.wh.Resolve(ctx, k.table, files)
	if err != nil {
		return nil, es, err
	}
	for _, e := range es {
		if landed[e.h.StagedURI] {
			delete(k.open, e.h.Partition)
			delete(k.sealedHere, e.h.UploadID)
			known = append(known, connector.CommitOutcome{
				Handle:      e.c.Handle,
				Disposition: connector.DispositionAlreadyCommitted,
			})
			continue
		}
		unknown = append(unknown, e)
	}
	return known, unknown, nil
}

// resolveLocked answers the timed-out-commit question per file and turns it into
// dispositions. This is the method that proves the hostile case IS satisfiable on the
// Commit path — and its absence on the AbortStale path is BREAKAGE 1.
func (k *sink) resolveLocked(ctx context.Context, winners []entry, cause error) []connector.CommitOutcome {
	files := make([]string, 0, len(winners))
	for _, e := range winners {
		files = append(files, e.h.StagedURI)
	}
	slices.Sort(files)

	landed, rerr := k.wh.Resolve(ctx, k.table, files)
	out := make([]connector.CommitOutcome, 0, len(winners))
	if rerr != nil {
		for _, e := range winners {
			out = append(out, connector.CommitOutcome{
				Handle:      e.c.Handle,
				Disposition: connector.DispositionRetryLater,
				Fault: fault.Unknown(fault.OpCommitSink, fmt.Errorf(
					"commit outcome unknown and unresolvable: %w (resolve: %v)", cause, rerr)),
			})
		}
		k.inDoubtFiles.Set(float64(len(winners)))
		return out
	}

	var doubt int
	for _, e := range winners {
		if landed[e.h.StagedURI] {
			delete(k.open, e.h.Partition)
			// The commit DID land; the timeout lied. This is a success for settlement and
			// is reported separately so the rate is visible.
			out = append(out, connector.CommitOutcome{
				Handle:      e.c.Handle,
				Disposition: connector.DispositionAlreadyCommitted,
			})
			continue
		}
		doubt++
		out = append(out, connector.CommitOutcome{
			Handle:      e.c.Handle,
			Disposition: connector.DispositionRetryLater,
			Fault:       fault.Unknown(fault.OpCommitSink, cause),
		})
	}
	k.inDoubtFiles.Set(float64(doubt))
	return out
}

func (k *sink) observeLocked() {
	if k.stagedFiles == nil {
		return
	}
	var sealed int
	var oldest time.Time
	for p, u := range k.open {
		k.stagedBytes.Set(float64(u.size()), p)
		if u.sealed {
			sealed++
		}
		if oldest.IsZero() || u.openedAt.Before(oldest) {
			oldest = u.openedAt
		}
	}
	k.stagedFiles.Set(float64(sealed))
	if !oldest.IsZero() {
		k.oldestOpenAge.Set(time.Since(oldest).Seconds())
	}
	// A gauge whose value is unmeasurable is OMITTED, never set to zero — so when there
	// is no open file, oldest_open_file_age_seconds is simply not updated.
}

func subsumed(c connector.Committable) connector.CommitOutcome {
	return connector.CommitOutcome{
		Handle:      c.Handle,
		Disposition: connector.DispositionAborted,
		Fault: &fault.Fault{
			Class: fault.TransientInternal,
			Op:    fault.OpCommitSink,
			User:  "subsumed by a later checkpoint covering a longer span; the staged file is retained",
			Dev:   fmt.Sprintf("committable from checkpoint %d subsumed", c.Checkpoint),
		},
	}
}

// txnIDFor derives the warehouse transaction id from durable inputs only. Determinism is
// the entire idempotency mechanism: the same set of staged files always produces the same
// id, so a retry after a timeout — or after a crash, in a new process, in a new
// generation — presents the id the server may already have seen.
func txnIDFor(table string, files []string) string {
	h := sha256.New()
	h.Write([]byte(table))
	h.Write([]byte{0})
	for _, f := range files {
		h.Write([]byte(f))
		h.Write([]byte{0})
	}
	return "canal-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// classify maps client errors onto canal's ownership taxonomy. An unknown outcome is
// NEVER laundered into TransientUpstream: that would breach the retry-safety obligation
// and is the lie fault.Indeterminate exists to make unnecessary.
func classify(op fault.Op, err error) *fault.Fault {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errIndeterminate), errors.Is(err, context.DeadlineExceeded):
		return fault.Unknown(op, err)
	case errors.Is(err, errPermanent):
		return fault.Permanent(op, err)
	default:
		return fault.Transient(op, err)
	}
}

func bytesHuman(n int64) string {
	const u = 1024
	if n < u {
		return fmt.Sprintf("%dB", n)
	}
	f, suf := float64(n), []string{"KiB", "MiB", "GiB", "TiB"}
	i := -1
	for f >= u && i < len(suf)-1 {
		f /= u
		i++
	}
	return fmt.Sprintf("%.1f%s", f, suf[i])
}

// ---------------------------------------------------------------------------
// A stub client, so the connector logic above is real and compiles standalone.
// A shipping connector would hold the vendor SDK here.
// ---------------------------------------------------------------------------

type stubWarehouse struct {
	mu        sync.Mutex
	n         int
	published map[string]bool
}

func (s *stubWarehouse) StartUpload(_ context.Context, uri string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("upload-%d-%s", s.n, hash8(uri)), nil
}

func (s *stubWarehouse) UploadPart(_ context.Context, uploadID string, number int, body []byte) (string, error) {
	return hash8(fmt.Sprintf("%s/%d/%d", uploadID, number, len(body))), nil
}

func (s *stubWarehouse) CompleteUpload(_ context.Context, uploadID string, parts []partRef) (string, error) {
	return fmt.Sprintf("staged://%s/%d", uploadID, len(parts)), nil
}

func (s *stubWarehouse) AbortUpload(context.Context, string) error { return nil }

func (s *stubWarehouse) Commit(_ context.Context, _, _ string, files []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.published == nil {
		s.published = map[string]bool{}
	}
	for _, f := range files {
		s.published[f] = true
	}
	return nil
}

func (s *stubWarehouse) Resolve(_ context.Context, _ string, files []string) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(files))
	for _, f := range files {
		out[f] = s.published[f]
	}
	return out, nil
}

func hash8(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4])
}

// Compile-time proof that every declared capability is actually implemented, so a
// registration panic is impossible for this connector.
var (
	_ connector.Sink        = (*sink)(nil)
	_ connector.Flusher     = (*sink)(nil)
	_ connector.Partitioner = (*sink)(nil)
	_ connector.Committer   = (*sink)(nil)
	_ connector.WriterState = (*sink)(nil)

	// BREAKAGE 1's repair, asserted at compile time: the per-item stale resolution this
	// connector needed and could not express.
	_ connector.StaleResolver = (*sink)(nil)
)
