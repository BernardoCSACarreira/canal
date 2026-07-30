// Package parallelsnapshot is a HOSTILE connector written against canal's real
// interfaces to find out where they break.
//
// # The case
//
// A source whose initial scan is 4 TB across 900 tables and must run with 32-way
// parallelism, chunked by primary-key range, while the incremental change stream runs
// CONCURRENTLY (Flink-CDC / DBLog style incremental snapshot with per-chunk watermarks
// and changelog backfill). It must survive a crash at 60% and resume without rescanning
// finished chunks and without losing changes that arrived to already-scanned chunks.
// Chunk boundaries are only discoverable at runtime.
//
// The upstream is stubbed (see [Upstream] and [stubUpstream]) because the finding is
// about canal's seam, not about SQL. Everything that touches connector, record, fault,
// config and registry is real, complete, and compiles.
//
// # Verdict
//
// requires-core-change. Seven distinct breakages, three of them fatal, two of them
// provable by the Go compiler rather than by argument. The full argument for each is at
// the site where it bites; this is the index.
//
//	B1 FATAL   a source cannot populate Origin.Key. There is no setter, the field is
//	           reachable only through a value copy, and `r.Origin().Key = k` does not
//	           compile. SourceCaps.StableKeys is therefore unsatisfiable by ANY source,
//	           and with it go EffectivelyOnce, engine dedupe, SinkCaps.RequiresKey and
//	           every upsert destination. For this case that is not a nicety: snapshot
//	           rows and changelog events converge at the sink BY KEY or not at all.
//	           -> see "BREAKAGE B1" at (*Source).materialiseChunkRow
//
//	B2 FATAL   a source cannot produce records for more than one lane. Batch.Add stamps
//	           Origin.Lane and Origin.Stream from the *Allocator the batch was built
//	           with; Batch.Lane is a separate settable field; Read takes one batch and no
//	           lane argument. Setting dst.Lane to any other lane silently mislabels every
//	           record. 32-way parallel emission across 900 streams has no expression.
//	           -> see "BREAKAGE B2" at (*Source).Read
//
//	B3 FATAL   a prefix lane's position cannot advance without emitting a record, so a
//	           filtering tail lane pins the upstream's changelog for the whole 4 TB scan,
//	           and ADR 0008's "a chunk-planning lane is a lane, with a cursor" cannot
//	           checkpoint at all — a planning lane emits no records.
//	           -> see "BREAKAGE B3" at (*Source).stageTailAdvance
//
//	B4 MAJOR   a connector cannot read the lane table. LaneCtl.Assigned returns only the
//	           unfinished lanes THIS worker leases, and there is no All. The tail's
//	           emit/drop predicate needs every chunk's range and HIGH watermark
//	           cluster-wide, so the concurrent watermark protocol is unimplementable in
//	           the connector — which is exactly where ADR 0008 puts it.
//	           -> see "BREAKAGE B4" at (*Source).worldView
//
//	B5 MAJOR   Announce is one durable round-trip per lane and MaxLanes is a
//	           registration-time constant, so a source whose lane count is data-dependent
//	           (here 10^4..10^5 chunks) must declare MaxLanes: 0 and forfeit the whole
//	           protection, then pay 10^5 serialised durable writes to plan.
//	           -> see "BREAKAGE B5" at (*Source).planTable
//
//	B6 MAJOR   the `shadow` transform ADR 0008 prescribes as the remedy for the
//	           concurrent case cannot be written: TransformRuntime has no Lanes() and no
//	           State(), so it can neither know how many scan lanes exist (to self-retire)
//	           nor persist its seen-key set.
//	           -> see "BREAKAGE B6" at the bottom of this file
//
//	B7 MINOR   MidLaneResume, Replayable and CompleteImages are per-SOURCE declarations
//	           of per-LANE properties. Here the chunk lanes are all-or-nothing and the
//	           tail is mid-lane resumable; only one value can be declared, and the
//	           submit-time refusal it drives is then evaluated against the wrong lane.
//	           -> see "BREAKAGE B7" at the caps block
//
// What FITS CLEANLY, stated because a false positive costs more than a missed one:
// LaneSpec's split of write-once Spec from write-many Cursor is exactly right and is the
// single most valuable thing in the interface set for this case — a chunk's key range and
// its scan progress have genuinely different lifetimes and nothing forced them together.
// Announce being durable before it returns removes a whole class of lost-plan bug.
// Position.Order plus Position.Scalar over a runtime-discovered key range is enough for
// per-chunk progress with no core knowledge of keys. Lane revocation as a fence, and
// Commit never being delivered for a revoked lane, is correct and is what makes a
// cluster-wide parallel scan safe at all. None of those needed a workaround.
package parallelsnapshot

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// Encoding versions. Everything durable is (version, bytes) per the four-part Blob
// contract, and Order's encoding is part of the token's version contract.
const (
	specV1  uint32 = 1 // LaneSpec.Spec
	tokenV1 uint32 = 1 // Position.Token
)

// Lane groups. Deliberately NOT used with StartAfter: this case requires the tail to run
// concurrently with the scan, which is the one thing lane gating cannot express.
const (
	groupScan record.LaneGroup = "scan"
	groupPlan record.LaneGroup = "plan"
	groupTail record.LaneGroup = "tail"
)

const (
	laneTail       = "tail"
	lanePlanPrefix = "plan/"
	laneChunkFmt   = "chunk/%s/%d"
)

// chunk phases. This is the connector's OWN state machine inside its own opaque token —
// the core never sees it, which is correct and is not a finding.
const (
	phaseLow  uint8 = 0 // low watermark not yet captured
	phaseScan uint8 = 1 // low captured, range read in progress
	phaseHigh uint8 = 2 // range read, high captured, backfill merged, rows staged
	phaseDone uint8 = 3 // every staged row handed over
)

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*Source]{
		Meta: registry.Meta{
			Name:    "stress_parallel_snapshot",
			Version: "0.1.0",
			Title:   "Parallel incremental snapshot (stress case)",
			Summary: "4 TB / 900 tables, 32-way PK-range chunked scan concurrent with the change stream.",
			Notes: "Origin.Key is the canonical encoding of the table's declared primary key: the " +
				"literal bytes of the single-column pk for a simple key, or the 0x1F-joined column " +
				"values in declaration order for a composite one. It is stable across re-reads because " +
				"it is derived from the row's own identity and never from a page cursor or a log " +
				"position, so a chunk re-read after a crash produces byte-identical keys. Origin.Upstream " +
				"carries the change-stream log position for a tail record and is empty for a scan record, " +
				"because a scan read has no upstream event id. REPAIRED: BREAKAGE B1's fix is " +
				"record.Record.SetKey; this connector now declares StableKeys and populates it.",
			Support: registry.SupportCommunity,
		},

		Spec: config.NewSpec().
			Field(config.Field{
				Name:        "dsn",
				Type:        config.TypeString,
				Description: "Connection string for the source database.",
				Short:       "Where to connect.",
				Secret:      true,
			}).
			Field(config.Field{
				Name:        "tables",
				Type:        config.TypeArray,
				Description: "Tables to move. Empty means every table the source reports.",
				Short:       "Leave empty for all 900.",
				Optional:    true,
				Default:     []any{},
				Item: &config.Field{
					Name:        "table",
					Type:        config.TypeString,
					Description: "One fully-qualified table name.",
				},
			}).
			Field(config.Field{
				Name:        "chunk_rows",
				Type:        config.TypeInt,
				Description: "Target rows per primary-key chunk. One chunk is one lane and is read atomically.",
				Short:       "Rows per chunk.",
				Default:     100000,
				Optional:    true,
			}).
			Field(config.Field{
				Name:        "scan_parallelism",
				Type:        config.TypeInt,
				Description: "How many chunks this worker reads at once.",
				Short:       "Concurrent chunk readers.",
				Default:     32,
				Optional:    true,
			}).
			Field(config.Field{
				Name: "honour_batch_lane",
				Type: config.TypeBool,
				Description: "When true, Read only ever fills the lane the engine's batch was allocated " +
					"for, which is correct but serialises 32 chunk readers behind one lane per call. " +
					"When false, Read fills whichever lane has data, which is the only way to reach the " +
					"required throughput and which mislabels Origin.Lane and Origin.Stream on every " +
					"record. This knob exists to make BREAKAGE B2 executable rather than rhetorical: " +
					"both settings are wrong and the interface offers no third.",
				Short:    "Correct-but-serialised (true) or parallel-but-mislabelled (false).",
				Default:  true,
				Optional: true,
				Advanced: true,
			}),

		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Bounded, connector.Unbounded},
			LaneKinds: []connector.LaneKind{
				connector.LaneKindScan, connector.LaneKindStream, connector.LaneKindBackfill,
			},

			// BREAKAGE B5, first half. See planTable for the argument.
			MaxLanes: 0,

			// BREAKAGE B2, REPAIRED: connector.LaneReader plus ReadConcurrency is how 32-way
			// concurrent emission across 900 streams became expressible. The engine owns one
			// allocator and one batch per held lane and partitions them into at most this
			// many disjoint groups.
			ReadsLanes:      true,
			ReadConcurrency: 32,

			// A replication slot / binlog that is freed when we confirm a position.
			UpstreamRetention: connector.PrunesOnCommit,
			UnitAssignment:    connector.UnitsDynamic,

			Discoverable:   true,
			ReportsBacklog: true,
			Heartbeats:     true,

			ProducesEventTime: true,
			ProducesChange:    true,
			CompleteImages:    true,

			ComparablePositions: true,
			Replayable:          true,

			// -----------------------------------------------------------------
			// BREAKAGE B7 — MINOR. SourceCaps declares per-SOURCE what are
			// per-LANE properties, and this connector's two lane kinds disagree.
			//
			// Blocking signature:
			//
			//	SourceCaps.MidLaneResume bool  // also Replayable, CompleteImages
			// -----------------------------------------------------------------
			//
			// A DBLog chunk is atomic: the low watermark is captured, the whole range is
			// read, the high watermark is captured, and the changelog window is merged
			// before ANY row is handed over. There is no legal resume point inside a
			// chunk — resuming mid-chunk leaves two consistency positions under one
			// watermark, which is the documented Flink CDC scar. So for a chunk lane the
			// honest value is FALSE.
			//
			// The tail lane is unbounded. For it, MidLaneResume=false means "commit only
			// at end of lane", and an unbounded lane has no end, so false means never
			// commit. For the tail the only possible value is TRUE.
			//
			// One field, two lanes, opposite answers. The union has to be declared, and
			// the submit-time refusal this field drives — "!MidLaneResume => no
			// ack-shortening buffer" — is then evaluated against the lane that did not
			// need the protection. The same argument applies to Replayable and
			// CompleteImages for a source mixing a full-image scan with a partial-image
			// changelog.
			//
			// SMALLEST FIX: an optional per-lane override on LaneSpec, e.g.
			// `MidLaneResume *bool` (nil = inherit the source's declaration). Adding a
			// field to LaneSpec is additive; its zero value is the current behaviour, so
			// no written connector changes. This is the least important of the seven and
			// is worth doing only alongside B2's fix, which touches the same struct.
			MidLaneResume: true,

			// BREAKAGE B1, REPAIRED. This connector has perfect, stable,
			// canonically-encodable primary keys for all 900 tables, and record.Record.SetKey
			// now lets it say so. Before the repair, declaring true here would have passed
			// registration — the only check is that Notes is non-empty — and been a lie that
			// no test in the repository could catch.
			StableKeys: true,
		},

		New: func(_ context.Context, c *config.Config) (*Source, error) {
			s := &Source{
				dsn:         config.Must[string](c, "dsn"),
				tables:      config.Must[[]string](c, "tables"),
				chunkRows:   config.Must[int](c, "chunk_rows"),
				parallelism: config.Must[int](c, "scan_parallelism"),
				honourLane:  config.Must[bool](c, "honour_batch_lane"),
				up:          stubUpstream{},
			}
			return s, c.Err()
		},
	})
}

// ---------------------------------------------------------------------------
// The upstream, stubbed.
// ---------------------------------------------------------------------------

// Upstream is the narrow view of the source database this connector needs. It is an
// interface so the canal-facing logic below is real code with a real dependency rather
// than a sketch.
type Upstream interface {
	// LogPosition returns the current changelog position: the watermark primitive.
	LogPosition(ctx context.Context) (uint64, error)

	// Tables lists every movable table.
	Tables(ctx context.Context) ([]string, error)

	// NextBoundary returns the exclusive upper key of the next chunk of at most n rows
	// starting at (exclusive) lower bound after. ok is false when the table is exhausted.
	// THIS is why chunk boundaries are only knowable at runtime.
	NextBoundary(ctx context.Context, table, after string, n int) (hi string, ok bool, err error)

	// ScanRange reads the whole half-open key range in one consistent read.
	ScanRange(ctx context.Context, table, lo, hi string) ([]Row, error)

	// ReadLog returns changelog events in (from, to], oldest first.
	ReadLog(ctx context.Context, from, to uint64, limit int) ([]Event, error)
}

// Row is one snapshot row.
type Row struct {
	Key    string
	Fields map[string]string
	At     time.Time
}

// Event is one changelog event.
type Event struct {
	Pos    uint64
	Table  string
	Key    string
	Op     record.Op
	Fields map[string]string
	At     time.Time
	TxID   string
	TxEnd  bool // true on the last event of an upstream transaction: a safe resume point
}

// stubUpstream stands in for the driver. Every method is a well-typed no-op, so this
// package builds and its canal-facing logic is exercised by proof_test.go.
type stubUpstream struct{}

func (stubUpstream) LogPosition(context.Context) (uint64, error) { return 0, nil }
func (stubUpstream) Tables(context.Context) ([]string, error)    { return nil, nil }

func (stubUpstream) NextBoundary(context.Context, string, string, int) (string, bool, error) {
	return "", false, nil
}
func (stubUpstream) ScanRange(context.Context, string, string, string) ([]Row, error) {
	return nil, nil
}
func (stubUpstream) ReadLog(context.Context, uint64, uint64, int) ([]Event, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Durable encodings. Two lifetimes, two types, per LaneSpec's own contract.
// ---------------------------------------------------------------------------

// chunkSpec is a chunk lane's WRITE-ONCE construction payload: LaneSpec.Spec.
type chunkSpec struct {
	Table string `json:"table"`
	Lo    string `json:"lo"`  // inclusive; "" is unbounded below
	Hi    string `json:"hi"`  // exclusive; "" is unbounded above
	Ord   uint64 `json:"ord"` // ordinal within the table, for the label only
	Est   uint64 `json:"est"`
}

// chunkToken is a chunk lane's WRITE-MANY progress payload: Position.Token.
type chunkToken struct {
	Phase uint8  `json:"phase"`
	Low   uint64 `json:"low"`  // changelog position captured BEFORE the range read
	High  uint64 `json:"high"` // changelog position captured AFTER the range read
	Rows  uint64 `json:"rows"`
}

// tailSpec is the tail lane's write-once payload: where the changelog reading starts.
type tailSpec struct {
	From uint64 `json:"from"`
}

// tailToken is the tail lane's progress.
type tailToken struct {
	At uint64 `json:"at"`
}

// planSpec / planToken belong to a per-table chunk-planning lane. ADR 0008 names exactly
// this construct — "the chunker's own resume cursor: a chunk-planning lane is a lane,
// with a cursor" — as the reason no core change is needed. See BREAKAGE B3.
type planSpec struct {
	Table string `json:"table"`
}

type planToken struct {
	NextLo string `json:"next_lo"`
	Chunks uint64 `json:"chunks"`
	Done   bool   `json:"done"`
}

func mustBlob(v uint32, x any) record.Blob {
	b, err := json.Marshal(x)
	if err != nil {
		// Marshalling a fixed local struct cannot fail; a panic here is a bug in this
		// connector and the engine's sandbox turns it into fault.PermanentInternal.
		panic("parallelsnapshot: unmarshallable durable payload: " + err.Error())
	}
	return record.Blob{Version: v, Bytes: b}
}

func decodeBlob[T any](b record.Blob, want uint32) (T, bool, error) {
	var out T
	if b.IsZero() {
		return out, false, nil
	}
	if b.Version > want {
		// Rule 3 of the format contract: a newer version is refused only when the
		// encoding genuinely cannot tolerate it. JSON is additive, so a newer version is
		// read, not refused.
		if err := json.Unmarshal(b.Bytes, &out); err != nil {
			return out, false, fault.Contract(fault.OpOpen,
				fmt.Errorf("state written by version %d is unreadable by version %d: %w", b.Version, want, err))
		}
		return out, true, nil
	}
	if err := json.Unmarshal(b.Bytes, &out); err != nil {
		return out, false, fault.Contract(fault.OpOpen, err)
	}
	return out, true, nil
}

// orderOfKey is the order-preserving encoding of a primary key. For a string key the key
// itself is already order-preserving under bytes.Compare, which is what Position.Order
// promises. Its encoding is part of tokenV1's contract.
func orderOfKey(k string) []byte { return []byte(k) }

// orderOfLogPos is the order-preserving encoding of a changelog position.
func orderOfLogPos(p uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], p)
	return b[:]
}

// ---------------------------------------------------------------------------
// The source.
// ---------------------------------------------------------------------------

// Source is the connector. One mutex guards everything shared between the read
// goroutine, the control goroutine and this connector's own workers — the contract
// permits Read to run concurrently with Commit, Heartbeat and Backlog, and this
// connector adds parallelism/2 more goroutines of its own.
type Source struct {
	dsn         string
	tables      []string
	chunkRows   int
	parallelism int
	honourLane  bool

	up  Upstream
	rt  connector.SourceRuntime
	log *slog.Logger

	staged  connector.Counter
	skipped connector.Counter
	mislab  connector.Counter

	mu      sync.Mutex
	started bool
	chunks  map[record.LaneID]*chunkLane
	plans   map[record.LaneID]*planLane
	tail    *tailLane
	rr      []record.LaneID // round-robin cursor over lanes with staged work
	wake    chan struct{}   // closed and replaced whenever work is staged

	stop context.CancelFunc
	wg   sync.WaitGroup
}

type chunkLane struct {
	id    record.LaneID
	spec  chunkSpec
	tok   chunkToken
	rows  []Row // read, deduped against the changelog window, waiting for Read
	sent  uint64
	claim bool // a worker owns this lane right now
	fin   bool // EndOfLane has been handed to the engine
}

type planLane struct {
	id   record.LaneID
	spec planSpec
	tok  planToken
}

type tailLane struct {
	id     record.LaneID
	spec   tailSpec
	tok    tailToken
	events []Event
	// advance is a position the tail must publish with NO records, because every event
	// up to it was filtered. See BREAKAGE B3.
	advance    uint64
	hasAdvance bool
}

// Open reads the assigned lanes, reconstructs every lane from (Spec, Cursor), announces
// what is missing, and starts this connector's own workers.
//
// It is idempotent: the engine calls it again with backoff after any method returns
// fault.ErrNotConnected.
func (s *Source) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.mu.Lock()
	first := !s.started
	s.started = true
	s.rt = rt
	s.log = rt.Log()
	if s.chunks == nil {
		s.chunks = map[record.LaneID]*chunkLane{}
		s.plans = map[record.LaneID]*planLane{}
		s.wake = make(chan struct{})
	}
	s.mu.Unlock()

	if first {
		s.staged, _ = rt.Metrics().Counter("staged_rows")
		s.skipped, _ = rt.Metrics().Counter("filtered_events")
		s.mislab, _ = rt.Metrics().Counter("mislabelled_records")
	}

	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}

	// Rebuild from what the core handed back. The discriminator is data — whether
	// Assigned returned anything — never a nil position test.
	for _, a := range as {
		switch {
		case a.Spec.Name == laneTail:
			sp, _, err := decodeBlob[tailSpec](a.Spec.Spec, specV1)
			if err != nil {
				return err
			}
			tk, ok, err := decodeBlob[tailToken](a.Cursor.Token, tokenV1)
			if err != nil {
				return err
			}
			if !ok {
				tk.At = sp.From // no progress yet: start where the lane was planned
			}
			s.mu.Lock()
			s.tail = &tailLane{id: a.ID, spec: sp, tok: tk}
			s.mu.Unlock()

		case strings.HasPrefix(a.Spec.Name, lanePlanPrefix):
			sp, _, err := decodeBlob[planSpec](a.Spec.Spec, specV1)
			if err != nil {
				return err
			}
			tk, _, err := decodeBlob[planToken](a.Cursor.Token, tokenV1)
			if err != nil {
				return err
			}
			s.mu.Lock()
			s.plans[a.ID] = &planLane{id: a.ID, spec: sp, tok: tk}
			s.mu.Unlock()

		default:
			sp, _, err := decodeBlob[chunkSpec](a.Spec.Spec, specV1)
			if err != nil {
				return err
			}
			tk, _, err := decodeBlob[chunkToken](a.Cursor.Token, tokenV1)
			if err != nil {
				return err
			}
			s.mu.Lock()
			s.chunks[a.ID] = &chunkLane{id: a.ID, spec: sp, tok: tk}
			s.mu.Unlock()
		}
	}

	if s.tail == nil {
		// Cold start for the tail. Capture the changelog position FIRST and announce it
		// durably before any chunk is planned, so a crash before the first chunk row
		// moves still resumes the changelog from before the scan began.
		//
		// NOTE the deliberate absence of StartAfter. Gating the tail behind the scan
		// group — the whole of ADR 0008 — is precisely what this case forbids: 4 TB of
		// scan means the changelog would have to be retained for the entire scan, which
		// is the problem incremental snapshot exists to solve. So this connector opts out
		// of the core's only handoff mechanism and owes the correctness itself.
		low, err := s.up.LogPosition(ctx)
		if err != nil {
			return fault.Transient(fault.OpOpen, err)
		}
		sp := tailSpec{From: low}
		id, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name:        laneTail,
			Stream:      record.DefaultStream,
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       groupTail,
			Spec:        mustBlob(specV1, sp),
			Label:       fmt.Sprintf("changelog from %d", low),
			Budget:      4096,
		})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.tail = &tailLane{id: id, spec: sp, tok: tailToken{At: low}}
		s.mu.Unlock()
	}

	if first {
		wctx, cancel := context.WithCancel(rt.Context())
		s.stop = cancel
		s.wg.Add(1)
		go s.planLoop(wctx)
		s.wg.Add(1)
		go s.tailLoop(wctx)
		for i := 0; i < s.parallelism; i++ {
			s.wg.Add(1)
			go s.chunkLoop(wctx)
		}
	}
	return nil
}

// Read is the funnel, and the funnel is BREAKAGE B2.
//
// ---------------------------------------------------------------------------
// BREAKAGE B2 — FATAL. A source cannot produce records for more than one lane.
//
// Blocking signatures, all three needed to see it:
//
//	func (s Source) Read(ctx context.Context, dst *record.Batch) error
//	func record.NewBatch(a *record.Allocator, capHint int) *record.Batch   // b.Lane = a.lane
//	func (b *record.Batch) Add() *record.Record                            // origin.Lane = a.lane
//	                                                                       // origin.Stream = a.stream
//
// record.Batch.Lane is an exported, settable field. record.Record.origin is not: Add
// stamps origin.Lane and origin.Stream from the *Allocator the batch was constructed
// with, and there is no second constructor. So for one Read call, the lane and the stream
// of every record it produces are fixed before the call begins, by the engine, and the
// only lane-shaped thing the source can influence — dst.Lane — does not affect them.
//
// Proven, not argued. proof_test.go in this package executes:
//
//	a := record.NewAllocator("default", "p", "n", "lane-a", "orders", 1, 1)
//	b := record.NewBatch(a, 4); b.Lane = "lane-b"; r := b.Add()
//	// b.Lane == "lane-b"  but  r.Origin().Lane == "lane-a", r.Origin().Stream == "orders"
//
// The ledger settles on b.Lane, so the mislabelling is not immediately fatal to
// accounting — it is worse than that, it is silent. Origin.Lane is what record.Ref
// carries, so every per-record outcome, every dead-letter row and every fault this
// connector's records produce is attributed to the wrong lane and the wrong stream. With
// 900 tables, 899/900 records get the wrong Origin.Stream, and Origin.Stream — not Dest —
// is the identity half of that pair, the one schema attribution and dedupe scope key on.
//
// So there are exactly two implementable behaviours and this connector implements both,
// under honour_batch_lane, because a knob whose two settings are "correct but cannot meet
// the requirement" and "meets the requirement but corrupts provenance" is the finding
// rendered as code:
//
//	honour_batch_lane=true   Fill only dst.Lane. Provenance is correct. But Read must
//	                         BLOCK until that lane has data ("It blocks until at least one
//	                         record is available"), and with 32 chunk readers and skewed
//	                         900-table data the other 31 lanes starve behind one lane's
//	                         I/O. The engine cannot fix this by calling Read more often:
//	                         there is one read goroutine per source node ("A source node
//	                         runs two goroutines and only two"), so calls are serial.
//	honour_batch_lane=false  Fill whichever lane has data. Throughput is reached.
//	                         Origin.Lane and Origin.Stream are wrong on every record whose
//	                         lane is not the allocator's, counted here as
//	                         canal_connector_source_mislabelled_records.
//
// Note also spec.Spec.Parallelism, "the max number of lanes one worker reads
// concurrently", and the §21b walkthrough, "Read serves the eight scan lanes
// round-robin". Both describe behaviour the interface cannot express: round-robin over
// lanes from one batch is exactly the mislabelling case.
//
// SMALLEST FIX — additive optional interface, breaks no written connector:
//
//	// source_optional.go
//	type LaneReader interface {
//	    // ReadLane fills dst for exactly lane. The engine owns one batch and one
//	    // Allocator per assigned lane, so Origin.Lane and Origin.Stream are correct by
//	    // construction, and it may call ReadLane for different lanes CONCURRENTLY, up to
//	    // SourceCaps.MaxReadConcurrency.
//	    ReadLane(ctx context.Context, lane record.LaneID, dst *record.Batch) error
//	}
//	// caps.go
//	ReadsLanes         bool `json:"reads_lanes"`           // LaneReader
//	MaxReadConcurrency int  `json:"max_read_concurrency"`  // 0 = 1
//
// Source stays frozen at four methods; Read remains the whole story for the
// ninety-percent source; the engine type-asserts once in registry.ResolveSource exactly
// as it already does for eight other optional interfaces. Every connector already
// written keeps working unchanged, because the engine only takes the new path when
// ReadsLanes is declared. It also retires the ambiguity about who sets Batch.Lane: for a
// LaneReader the answer is nobody, the allocator already knows.
//
// A smaller-looking fix that does NOT work, recorded so it is not proposed later: adding
// `SetLane`/`SetStream` to record.Record. That reopens the provenance hole Origin exists
// to close, and it cannot fix the concurrency half — one batch is still one buffer filled
// by one goroutine.
// ---------------------------------------------------------------------------
func (s *Source) Read(ctx context.Context, dst *record.Batch) error {
	for {
		want := dst.Lane
		dst.Reset()

		n, lane, eol, pos, done := s.drainInto(dst, want)
		if n > 0 || eol || pos {
			dst.Lane = lane
			return nil
		}
		if done {
			// Every lane this worker holds is finished and no more will be announced.
			return fault.ErrEndOfInput
		}

		s.mu.Lock()
		w := s.wake
		s.mu.Unlock()

		select {
		case <-w:
		case <-ctx.Done():
			// CANCELLATION MEANS DRAIN. One more pass so nothing already produced is
			// discarded, then report.
			n, lane, eol, pos, _ = s.drainInto(dst, want)
			if n > 0 || eol || pos {
				dst.Lane = lane
				return nil
			}
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			// The tail may have advanced its filtered position with no records.
		}
	}
}

// drainInto materialises at most one lane's worth of staged work into dst. It reports how
// many records it wrote, which lane, whether it set EndOfLane, whether it set a position,
// and whether this worker has nothing left forever.
// ReadLanes is BREAKAGE B2's repair, and it is what the 32-way parallelism requirement
// always wanted.
//
// Before it, Read was handed ONE batch bound to ONE allocator, so this source had exactly
// two settings and both were wrong: honour the batch's lane and serialise 32 chunk readers
// behind whichever lane the engine happened to allocate for, or fill it anyway and
// mislabel every record's Origin.Lane and Origin.Stream — measured at 33350 of 33500.
//
// Now the engine hands one pre-bound batch per held lane. Every record is stamped by the
// allocator for the lane it actually came from, the scheduler in pickLocked stops existing
// (the engine's partitioner does that job), and dst[i].Position is that lane's own cursor.
// The whole honour_batch_lane dilemma and its mislabelling counter are dead code kept only
// so the regression test can still exercise the old paths.
//
// ReadConcurrency is declared 32 to match parallelism, so the engine makes up to 32
// concurrent calls over disjoint lane sets. Each call touches only its own batches, so this
// source needs no locking beyond the one mutex it already holds for its upstream state.
func (s *Source) ReadLanes(ctx context.Context, dst []*record.Batch) error {
	for {
		var filled bool
		for _, b := range dst {
			if b == nil {
				continue
			}
			b.Reset()
			// drainInto with want == b.Lane and honourLane forced: a batch bound to a lane
			// is filled from that lane or not at all. That is no longer a compromise —
			// there is a batch for every other lane too.
			n, _, eol, pos, _ := s.drainLane(b, b.Lane)
			if n > 0 || eol || pos {
				filled = true
			}
		}
		if filled {
			return nil
		}

		s.mu.Lock()
		exhausted := s.exhaustedLocked()
		w := s.wake
		s.mu.Unlock()
		if exhausted {
			return fault.ErrEndOfInput
		}

		select {
		case <-w:
		case <-ctx.Done():
			// CANCELLATION MEANS DRAIN, per batch: one more pass so nothing already
			// produced is discarded.
			for _, b := range dst {
				if b == nil {
					continue
				}
				if n, _, eol, pos, _ := s.drainLane(b, b.Lane); n > 0 || eol || pos {
					filled = true
				}
			}
			if filled {
				return nil
			}
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			// The tail may have advanced its filtered position with no records.
		}
	}
}

// drainLane fills exactly the requested lane, never another. It is drainInto with the
// honour-the-batch's-lane rule unconditional, which is the only rule that makes sense once
// there is a batch per lane.
func (s *Source) drainLane(dst *record.Batch, lane record.LaneID) (n int, served record.LaneID, eol, pos, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lane == "" || !s.hasWorkLocked(lane) {
		return 0, "", false, false, s.exhaustedLocked()
	}
	if s.tail != nil && lane == s.tail.id {
		return s.drainTailLocked(dst)
	}
	return s.drainChunkLocked(dst, lane)
}

func (s *Source) drainInto(dst *record.Batch, want record.LaneID) (n int, lane record.LaneID, eol, pos, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pick := s.pickLocked(want)
	if pick == "" {
		return 0, "", false, false, s.exhaustedLocked()
	}
	if s.honourLane && want != "" && pick != want {
		// The correct-but-serialised branch: refuse to fill a lane the batch was not
		// allocated for, and block instead.
		return 0, "", false, false, false
	}
	if pick != want && want != "" && s.mislab != nil {
		s.mislab.Add(1)
	}

	if s.tail != nil && pick == s.tail.id {
		return s.drainTailLocked(dst)
	}
	return s.drainChunkLocked(dst, pick)
}

// pickLocked chooses a lane to serve: the requested one if it has work, else round-robin
// over everything that does. This scheduler is the entire content of the 32-way
// parallelism requirement, and it exists in the connector only because the interface has
// nowhere else to put it.
func (s *Source) pickLocked(want record.LaneID) record.LaneID {
	if want != "" && s.hasWorkLocked(want) {
		return want
	}
	if len(s.rr) == 0 {
		s.rr = s.rr[:0]
		if s.tail != nil {
			s.rr = append(s.rr, s.tail.id)
		}
		for id := range s.chunks {
			s.rr = append(s.rr, id)
		}
	}
	for i := 0; i < len(s.rr); i++ {
		id := s.rr[0]
		s.rr = append(s.rr[1:], id)
		if s.hasWorkLocked(id) {
			return id
		}
	}
	return ""
}

func (s *Source) hasWorkLocked(id record.LaneID) bool {
	if s.tail != nil && id == s.tail.id {
		return len(s.tail.events) > 0 || s.tail.hasAdvance
	}
	c, ok := s.chunks[id]
	if !ok {
		return false
	}
	if c.fin {
		return false
	}
	return len(c.rows) > 0 || (c.tok.Phase == phaseHigh && len(c.rows) == 0)
}

func (s *Source) exhaustedLocked() bool {
	if s.tail != nil {
		return false // an unbounded lane never finishes
	}
	for _, c := range s.chunks {
		if !c.fin {
			return false
		}
	}
	for _, p := range s.plans {
		if !p.tok.Done {
			return false
		}
	}
	return true
}

func (s *Source) drainChunkLocked(dst *record.Batch, id record.LaneID) (int, record.LaneID, bool, bool, bool) {
	c := s.chunks[id]
	if c == nil {
		return 0, "", false, false, false
	}

	n := 0
	for len(c.rows) > 0 {
		r := dst.Add()
		if r == nil {
			break // hard cap; the rest goes out on the next Read
		}
		s.materialiseChunkRow(r, c, c.rows[0])
		c.rows = c.rows[1:]
		c.sent++
		n++
	}
	if s.staged != nil && n > 0 {
		s.staged.Add(float64(n))
	}

	if len(c.rows) > 0 || c.tok.Phase != phaseHigh {
		// A chunk is ATOMIC: no position until every row of it has been handed over.
		// Intermediate batches carry the zero Position, which the ledger tracks and never
		// flushes because Safe is false. That is exactly the all-or-nothing semantics a
		// DBLog chunk needs, and it works with no core involvement. Not a finding.
		return n, id, false, false, false
	}

	// Last batch of the chunk. Now, and only now, a position.
	c.tok.Phase = phaseDone
	c.tok.Rows = c.sent
	scalar := float64(c.sent)
	dst.Position = record.Position{
		Token:  mustBlob(tokenV1, c.tok),
		Order:  orderOfKey(c.spec.Hi),
		Scalar: &scalar,
		Safe:   true,
		At:     time.Now(),
		Label: fmt.Sprintf("%s chunk %d complete at high=%d",
			c.spec.Table, c.spec.Ord, c.tok.High),
	}
	dst.EndOfLane = true
	c.fin = true
	return n, id, true, true, false
}

// materialiseChunkRow turns one snapshot row into one record, and is BREAKAGE B1.
//
// ---------------------------------------------------------------------------
// BREAKAGE B1 — FATAL, and proven by the compiler.
//
// Blocking signatures:
//
//	func (r *record.Record) Origin() record.Origin   // returns a COPY
//	type record.Origin struct { ...; Key []byte; Upstream []byte; ... }
//	func (b *record.Batch) Add() *record.Record      // stamps origin; never a key
//	func record.NewAllocator(t, p, n, l, stream, firstID, firstGroup) *Allocator
//
// There is NO way for a source to populate Origin.Key. Record.origin is unexported;
// Origin() hands back a value copy; Add() stamps Tenant, Pipeline, Node, Lane, Stream,
// Group, ID, Root, ReadAt and refs and nothing else; the Allocator takes no key and could
// not, because it is per-lane while a key is per-record. The one exported setter for an
// unexported field on Record is SetHandle, for a discrete lane's delivery handle, so the
// pattern and its documented "legal only from the source that produced the record, before
// it returns from Read" rule already exist — Key was simply not given one.
//
// The natural line does not compile:
//
//	r.Origin().Key = []byte(row.Key)
//	// cannot assign to r.Origin().Key (neither addressable nor a map index expression)
//
// (That is the verbatim compiler message, from go vet on this repository.)
//
// What that costs, in the core's own terms:
//
//   - SourceCaps.StableKeys is documented as "Origin.Key is populated and stable across
//     re-reads". No source can satisfy it. Registration checks only that Notes is
//     non-empty, so declaring it is an undetectable lie.
//   - SinkCaps.RequiresKey is refused against a source that does not declare StableKeys,
//     so no keyed destination can ever be fed.
//   - EffectivelyOnce requires SinkCaps.Idempotent AND SourceCaps.StableKeys, so the
//     strongest tier reachable without a Committer sink is unreachable for every source.
//   - Engine dedupe and Request.IdempotencyKey both key on it.
//   - record.Ref.Key, the thing a sink reports per-record outcomes with, is always nil.
//   - ADR 0008's `shadow` transform "drops an OpScanRead record whose Origin.Key has
//     already been seen" — it would have nothing to compare.
//
// For THIS case it is not a lost optimisation, it is the whole design. A 4 TB snapshot
// running concurrently with a changelog converges only because both sides carry the same
// primary key and the destination upserts on it. Without Origin.Key the pipeline can only
// be pointed at an append-only sink, and an append-only sink receiving both a stale
// snapshot row and its newer changelog event stores both, with no way to order them —
// which is the silent-loss outcome the whole architecture is organised against.
//
// The workaround below is what a real connector author would be forced into, and it is
// not good enough to call this "fits awkwardly": the key is put in Meta under the source
// namespace, where it is invisible to every core check listed above. No sink can find it
// without connector-specific knowledge, which violates constraint #1 more thoroughly than
// anything else in this file.
//
// SMALLEST FIX — two methods in package record, additive, breaks nothing:
//
//	// SetKey attaches the source-derived stable identity, canonically encoded. Legal
//	// only from the source that produced the record, before it returns from Read; the
//	// engine rejects a key set later. Exactly SetHandle's rule.
//	func (r *Record) SetKey(k []byte) { r.origin.Key = k }
//
//	// SetUpstream attaches the vendor's own id for this record: idempotency layer 1.
//	func (r *Record) SetUpstream(k []byte) { r.origin.Upstream = k }
//
// Provenance is not weakened: Lane, Stream, Group, ID and Root — everything settlement
// uses — stay unwritable, and Key is not one of them. Transforms must NOT get these
// setters; the source-only rule is the same one SetHandle already carries and the same
// place enforces it.
// ---------------------------------------------------------------------------
func (s *Source) materialiseChunkRow(r *record.Record, c *chunkLane, row Row) {
	r.EventTime = row.At

	// Dest is settable, so routing works: 900 tables reach 900 destinations.
	r.Dest = record.StreamName(c.spec.Table)

	m := record.Map{}
	for k, v := range row.Fields {
		m[k] = record.String(v)
	}
	r.Payload = record.StructPayload(m)

	after := r.Payload.Clone()
	r.Change = &record.Change{
		Version:       record.ChangeVersion,
		Op:            record.OpScanRead,
		Keys:          [][]string{{"id"}},
		After:         &after,
		AfterComplete: record.CompletenessComplete,
	}

	// BREAKAGE B1, REPAIRED. The key the core actually reads: Ref.Key carries it to every
	// per-record outcome, the dedupe layers key on it, Request.IdempotencyKey is derived
	// from it and every upsert destination uses it directly. A scan record has no upstream
	// event id, so Upstream is deliberately left nil rather than filled with the chunk's
	// position — which would be an id for the READ and not for the row.
	r.SetKey([]byte(row.Key))
}

func (s *Source) drainTailLocked(dst *record.Batch) (int, record.LaneID, bool, bool, bool) {
	t := s.tail
	n := 0
	var last Event
	for len(t.events) > 0 {
		r := dst.Add()
		if r == nil {
			break
		}
		e := t.events[0]
		t.events = t.events[1:]
		last = e
		n++

		r.EventTime = e.At
		r.Dest = record.StreamName(e.Table)
		m := record.Map{}
		for k, v := range e.Fields {
			m[k] = record.String(v)
		}
		r.Payload = record.StructPayload(m)
		after := r.Payload.Clone()
		r.Change = &record.Change{
			Version:       record.ChangeVersion,
			Op:            e.Op,
			Keys:          [][]string{{"id"}},
			After:         &after,
			AfterComplete: record.CompletenessComplete,
			TxID:          e.TxID,
		}
		// BREAKAGE B1, REPAIRED, on the tail side. A tail record has both identities: the
		// row's stable key, and the change stream's own event id for idempotency layer one.
		r.SetKey([]byte(e.Key))
		r.SetUpstream(orderOfLogPos(e.Pos))
	}

	switch {
	case n > 0:
		t.tok.At = last.Pos
		dst.Position = s.tailPositionLocked(last.Pos, last.TxEnd, last.At)
		return n, t.id, false, true, false

	case t.hasAdvance:
		// BREAKAGE B3's site: a position with no records.
		t.tok.At = t.advance
		t.hasAdvance = false
		dst.Position = s.tailPositionLocked(t.advance, true, time.Now())
		return 0, t.id, false, true, false
	}
	return 0, "", false, false, false
}

func (s *Source) tailPositionLocked(at uint64, safe bool, when time.Time) record.Position {
	scalar := float64(at)
	return record.Position{
		Token:  mustBlob(tokenV1, tailToken{At: at}),
		Order:  orderOfLogPos(at),
		Scalar: &scalar,
		Safe:   safe, // only a transaction boundary is a gap-free resume point
		At:     when,
		Label:  fmt.Sprintf("changelog %d", at),
	}
}

// Commit is where a PrunesOnCommit upstream is told it may recycle. The core has already
// durably flushed its own record of the position — the three-phase ordering — so
// advancing the slot here cannot open a gap.
//
// It runs on the control goroutine, concurrent with Read, which is why every field it
// touches is under the same mutex.
func (s *Source) Commit(ctx context.Context, a connector.Ack) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tail != nil && a.Lane == s.tail.id {
		tk, ok, err := decodeBlob[tailToken](a.Through.Token, tokenV1)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		// A destructive/pruning advance with abandoned records in the window: refuse to
		// move the slot, so the changelog is still there for another attempt. The core
		// surfaces the number and the source chooses.
		if a.Abandoned > 0 {
			s.rt.Note(connector.Event{
				At: time.Now(), Kind: connector.EventNote, Severity: fault.PermanentMapping,
				Lane:    a.Lane,
				Message: "changelog slot not advanced: records were abandoned in this window",
				Detail:  fmt.Sprintf("abandoned=%d of %d", a.Abandoned, a.Records),
			})
			return nil
		}
		return s.confirmLocked(ctx, tk.At)
	}

	if c, ok := s.chunks[a.Lane]; ok && a.LaneFinished {
		// The chunk is durably finished. Its HIGH watermark is now the fact the tail
		// needs — and cannot reach. See BREAKAGE B4.
		if err := s.rt.Lanes().Finish(ctx, c.id); err != nil {
			return err
		}
		delete(s.chunks, c.id)
	}
	return nil
}

func (s *Source) confirmLocked(ctx context.Context, at uint64) error {
	// The real connector confirms the replication slot here. The stub cannot, and the
	// ctx is threaded so a timeout is honoured.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_ = at
	return nil
}

// Close is called exactly once, always, including after a failed Open and including when
// Open was never called — config validation constructs a source and then closes it. It
// receives a fresh context carrying the shutdown grace period.
func (s *Source) Close(ctx context.Context) error {
	s.mu.Lock()
	stop := s.stop
	s.stop = nil
	s.mu.Unlock()

	if stop == nil {
		return nil
	}
	stop()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fault.Internal(fault.OpClose, fmt.Errorf("chunk readers did not stop within the grace period"))
	}
}

// ---------------------------------------------------------------------------
// The planner: chunk boundaries discovered at runtime.
// ---------------------------------------------------------------------------

func (s *Source) planLoop(ctx context.Context) {
	defer s.wg.Done()

	tables := s.tables
	if len(tables) == 0 {
		t, err := s.up.Tables(ctx)
		if err != nil {
			return
		}
		tables = t
	}

	for _, tbl := range tables {
		if ctx.Err() != nil {
			return
		}
		if err := s.planTable(ctx, tbl); err != nil {
			s.log.Error("chunk planning failed", "table", tbl, "err", err)
			return
		}
	}
}

// planTable discovers this table's chunk boundaries and announces one lane per chunk.
//
// ---------------------------------------------------------------------------
// BREAKAGE B5 — MAJOR. Lane announcement does not scale to a data-dependent lane count.
//
// Blocking signatures:
//
//	Announce(ctx context.Context, spec LaneSpec) (record.LaneID, error)  // one lane, one
//	                                                                    // durable write
//	SourceCaps.MaxLanes int  // registration-time constant, hard-enforced per Announce
//
// Two distinct problems, one site.
//
// (a) Throughput. "The core persists the row — atomically, together with any state write
// in flight — BEFORE returning." 4 TB across 900 tables at 100k rows per chunk is
// O(10^4)-O(10^5) chunks, so planning costs that many serialised durable transactions
// before the scan is fully planned. There is no batch form. Flink CDC's second documented
// scar is precisely that its finished-chunk set outgrew one message; canal's shape has the
// same size problem at the other end of the protocol.
//
// (b) MaxLanes is declared on the CONNECTOR TYPE at registration, but the lane count here
// is a function of the operator's config (chunk_rows) and of runtime data (table sizes).
// The connector cannot know it at init. So it must declare MaxLanes: 0, "unlimited", and
// the field's entire purpose — "an advisory task cap is an eight-year bug waiting to
// threaten cluster stability" — is forfeited by exactly the connector that most needs a
// bound. The submit-time check `Parallelism <= SourceCaps.MaxLanes` becomes vacuous too.
//
// SMALLEST FIX. For (a), one additive method on LaneCtl — which is INJECTED and
// implemented by the core, so growing it cannot break a connector:
//
//	// AnnounceMany persists every spec in ONE atomic write, returning ids in order.
//	// Idempotent per spec exactly as Announce is.
//	AnnounceMany(ctx context.Context, specs []LaneSpec) ([]record.LaneID, error)
//
// For (b), let the effective cap come from config rather than from the type: keep
// SourceCaps.MaxLanes as the ceiling a connector can never exceed, and add an optional
// per-run bound the core computes from the spec (a `lane_limit` stage-standard field), so
// "unlimited connector, 40000 lanes for this pipeline" is expressible. Neither change
// touches a required interface; both are opt-in.
// ---------------------------------------------------------------------------
func (s *Source) planTable(ctx context.Context, tbl string) error {
	name := lanePlanPrefix + tbl

	// The planning lane. ADR 0008: "the chunker's own resume cursor — a chunk-planning
	// lane is a lane, with a cursor". It is announced with Boundedness Bounded and
	// LaneKindBackfill because it is not a full read of state and it does end.
	planID, err := s.rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name:        name,
		Stream:      record.StreamName(tbl),
		Kind:        connector.LaneKindBackfill,
		Ordering:    connector.OrderingPrefix,
		Boundedness: connector.Bounded,
		Group:       groupPlan,
		Spec:        mustBlob(specV1, planSpec{Table: tbl}),
		Label:       "chunk planner for " + tbl,
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	p := s.plans[planID]
	if p == nil {
		p = &planLane{id: planID, spec: planSpec{Table: tbl}}
		s.plans[planID] = p
	}
	lo := p.tok.NextLo
	ord := p.tok.Chunks
	done := p.tok.Done
	s.mu.Unlock()

	if done {
		return nil
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		hi, ok, err := s.up.NextBoundary(ctx, tbl, lo, s.chunkRows)
		if err != nil {
			return fault.Transient(fault.OpRead, err)
		}
		if !ok {
			s.mu.Lock()
			p.tok.Done = true
			s.mu.Unlock()
			// BREAKAGE B3 again: this lane must now publish "planning finished" durably,
			// and it has no records with which to do it.
			return nil
		}

		spec := chunkSpec{Table: tbl, Lo: lo, Hi: hi, Ord: ord, Est: uint64(s.chunkRows)}
		id, err := s.rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name:        fmt.Sprintf(laneChunkFmt, tbl, ord),
			Stream:      record.StreamName(tbl),
			Kind:        connector.LaneKindScan,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Bounded,
			Group:       groupScan,
			Spec:        mustBlob(specV1, spec),
			Weight:      uint64(s.chunkRows),
			Label:       fmt.Sprintf("%s chunk %d: pk in [%q,%q)", tbl, ord, lo, hi),
			Budget:      s.chunkRows,
		})
		if err != nil {
			return err
		}

		s.mu.Lock()
		if _, exists := s.chunks[id]; !exists {
			s.chunks[id] = &chunkLane{id: id, spec: spec}
		}
		ord++
		lo = hi
		p.tok.NextLo = lo
		p.tok.Chunks = ord
		s.rr = s.rr[:0]
		s.mu.Unlock()
		s.notify()
	}
}

// ---------------------------------------------------------------------------
// The chunk readers: 32 of them, and the DBLog watermark protocol.
// ---------------------------------------------------------------------------

func (s *Source) chunkLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		c := s.claimChunk()
		if c == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		if err := s.readChunk(ctx, c); err != nil {
			s.log.Error("chunk read failed", "lane", string(c.id), "err", err)
		}
		s.release(c)
		s.notify()
	}
}

func (s *Source) claimChunk() *chunkLane {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.chunks {
		if c.claim || c.fin || c.tok.Phase >= phaseHigh {
			continue
		}
		// Revocation is the fence: stop producing for a lane that is no longer ours.
		if s.rt.Lanes().Revoked(c.id) {
			continue
		}
		c.claim = true
		return c
	}
	return nil
}

func (s *Source) release(c *chunkLane) {
	s.mu.Lock()
	c.claim = false
	s.mu.Unlock()
}

// readChunk is the DBLog / Flink-CDC incremental-snapshot protocol for one chunk:
// low watermark, consistent range read, high watermark, changelog backfill merge.
//
// Every row whose key changed inside (low, high] is DROPPED from the snapshot result,
// because the tail will deliver the newer value for it. That is what makes the chunk's
// output correct as of `high` without stopping the world.
func (s *Source) readChunk(ctx context.Context, c *chunkLane) error {
	low, err := s.up.LogPosition(ctx)
	if err != nil {
		return fault.Transient(fault.OpRead, err)
	}

	rows, err := s.up.ScanRange(ctx, c.spec.Table, c.spec.Lo, c.spec.Hi)
	if err != nil {
		return fault.Transient(fault.OpRead, err)
	}

	high, err := s.up.LogPosition(ctx)
	if err != nil {
		return fault.Transient(fault.OpRead, err)
	}

	// BREAKAGE B4's second half, in passing: this is a SECOND changelog reader, opened
	// per chunk, because there is no way to declare that this chunk lane and the tail
	// lane must be co-located on one worker. Flink CDC does the same thing, so it is not
	// a defect — but 32 concurrent replication connections plus the tail is a resource
	// cost the operator cannot see and the core cannot bound, and the shared-stream
	// variant of DBLog is simply not expressible. LaneSpec.Group is equality-only for
	// ordering and StartAfter is a gate; neither is affinity.
	events, err := s.up.ReadLog(ctx, low, high, 1<<16)
	if err != nil {
		return fault.Transient(fault.OpRead, err)
	}
	changed := map[string]struct{}{}
	for _, e := range events {
		if e.Table != c.spec.Table {
			continue
		}
		if !inRange(e.Key, c.spec.Lo, c.spec.Hi) {
			continue
		}
		changed[e.Key] = struct{}{}
	}
	kept := rows[:0]
	for _, r := range rows {
		if _, hit := changed[r.Key]; hit {
			continue
		}
		kept = append(kept, r)
	}

	s.mu.Lock()
	c.tok.Phase = phaseHigh
	c.tok.Low = low
	c.tok.High = high
	c.rows = kept
	s.mu.Unlock()
	return nil
}

func inRange(k, lo, hi string) bool {
	if lo != "" && k < lo {
		return false
	}
	if hi != "" && k >= hi {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// The tail: concurrent changelog reading, with the per-chunk watermark filter.
// ---------------------------------------------------------------------------

func (s *Source) tailLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		if s.tail == nil {
			s.mu.Unlock()
			return
		}
		from := s.tail.tok.At
		id := s.tail.id
		s.mu.Unlock()

		if s.rt.Lanes().Revoked(id) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		events, err := s.up.ReadLog(ctx, from, ^uint64(0), 4096)
		if err != nil {
			s.log.Error("changelog read failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if len(events) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		world := s.worldView(ctx)
		var emit []Event
		var highestFiltered uint64
		for _, e := range events {
			switch world.verdict(e) {
			case verdictEmit:
				emit = append(emit, e)
			case verdictDrop:
				highestFiltered = e.Pos
				if s.skipped != nil {
					s.skipped.Add(1)
				}
			case verdictWait:
				// The chunk covering this key has not published its HIGH watermark yet,
				// so whether this event must be emitted is not yet decidable. Stop here
				// and retry: this is the only safe answer, and it is why B4's fix must
				// also let the tail stall rather than guess.
				highestFiltered = 0
				goto publish
			}
		}
	publish:
		s.mu.Lock()
		s.tail.events = append(s.tail.events, emit...)
		if len(emit) == 0 && highestFiltered > s.tail.tok.At {
			s.tail.advance = highestFiltered
			s.tail.hasAdvance = true
		}
		s.mu.Unlock()
		s.notify()
	}
}

type verdict uint8

const (
	verdictEmit verdict = iota
	verdictDrop
	verdictWait
)

// world is the tail's view of every chunk in the pipeline: the emit/drop predicate's
// entire input.
type world struct {
	// byTable maps a table to its chunks, each with the HIGH watermark at which its
	// snapshot became consistent. A chunk with high == 0 has not reached its high
	// watermark yet.
	byTable map[string][]worldChunk

	// planned reports whether this table's chunk set is fully enumerated. An event for a
	// key in a not-yet-enumerated range must be dropped, because the chunk that will
	// cover it has not been created and will read the newer value anyway.
	planned map[string]bool

	// complete is false when this view is known to be partial, which is the normal case.
	// See BREAKAGE B4.
	complete bool
}

type worldChunk struct {
	lo, hi string
	high   uint64
}

func (w world) verdict(e Event) verdict {
	if !w.complete {
		// BREAKAGE B4's consequence at runtime: with a partial view the tail cannot
		// distinguish "no chunk covers this key" from "a chunk on another worker covers
		// this key and I cannot see it". The only non-lossy answer is to wait forever,
		// which is a stalled pipeline; the only live answer is to guess, which loses
		// updates. This connector chooses the safe one and therefore does not work.
		return verdictWait
	}
	for _, c := range w.byTable[e.Table] {
		if !inRange(e.Key, c.lo, c.hi) {
			continue
		}
		switch {
		case c.high == 0:
			return verdictWait // chunk in flight; its window will cover this event
		case e.Pos > c.high:
			return verdictEmit // after the chunk's consistent read: ours to deliver
		default:
			return verdictDrop // inside the chunk's window: the chunk's backfill has it
		}
	}
	if w.planned[e.Table] {
		return verdictEmit // no chunk covers this key and the table is fully planned
	}
	return verdictDrop // the chunk that will cover it has not been planned yet
}

// worldView assembles what the tail needs, and is BREAKAGE B4.
//
// ---------------------------------------------------------------------------
// BREAKAGE B4 — MAJOR (fatal for the concurrent protocol, which is this case).
// A connector cannot read the lane table.
//
// Blocking signature:
//
//	Assigned(ctx context.Context) ([]LaneAssignment, error)
//	// "In one-process mode that is every announced, unfinished, ungated lane. In a
//	//  cluster it is the subset this worker holds a lease on."
//
// LaneCtl has Announce, Finish, Assigned, Changes, Revoked and Budget. There is no All.
// So the holder of the tail lane can see:
//
//   - the chunk lanes IT leases and has not finished, and
//   - nothing else.
//
// It cannot see chunks leased by the other 31 workers, chunks not yet leased, or any
// FINISHED chunk — and a finished chunk is exactly the case where the tail MUST emit.
// LaneAssignment.Finished exists and is documented "the field exists for the read model",
// which is the admission: the data is there and the connector is not allowed to have it.
//
// StateHandle.Get(ctx, lane) is not epoch-fenced and would happily return another lane's
// blob, but the connector cannot obtain the LaneIDs: LaneID is "derived deterministically
// from (tenant, pipeline, node, LaneSpec.Name)" by a derivation that is not exported, and
// the Names themselves are discovered at runtime by planners on OTHER workers.
//
// ADR 0008's table claims "a paged finished-chunk set — lane rows are already paged by
// reference from the store". True of the core; false of the connector, which cannot reach
// `store` at all and must not. The ADR then puts the concurrent case in the connector:
// "a connector wanting Flink-CDC-grade behaviour writes its own key-range planning
// against a helper library rather than getting it from the core". Those two sentences are
// jointly unsatisfiable, and this function is where they meet. The claim "adding it later
// changes no connector interface" holds only for a core-side watermark engine; for a
// connector-side one — the v1 story — it does not.
//
// Note also what is NOT the fix: LaneSpec.StartAfter. Gating the tail behind the scan
// group makes the tail correct and makes this case impossible, since the premise is
// concurrency across a 4 TB scan.
//
// SMALLEST FIX — one additive method on LaneCtl. LaneCtl is injected and implemented by
// the core, so adding a method to it breaks NO written connector; the doc comment on
// SourceRuntime already names this as the growth path.
//
//	// All returns every lane of this node in this pipeline generation — finished ones
//	// included — with its Spec and its last durably committed Cursor, ordered by id and
//	// paged from after. It is a read of the durable lane table, so a connector needing
//	// cross-lane facts does not need cross-worker messaging.
//	All(ctx context.Context, after record.LaneID, limit int) ([]LaneAssignment, error)
//
// That single method closes this breakage completely: the chunk ranges are in
// LaneSpec.Spec, the HIGH watermarks are in the connector's own Cursor.Token, the
// finished set is LaneAssignment.Finished, and the enumeration frontier is the planning
// lanes' cursors. Staleness is safe because the verdict function above already stalls on
// "not decidable yet" rather than guessing.
// ---------------------------------------------------------------------------
func (s *Source) worldView(ctx context.Context) world {
	w := world{
		byTable: map[string][]worldChunk{},
		planned: map[string]bool{},
	}

	// This is everything the interface permits: the lanes this worker holds, unfinished.
	as, err := s.rt.Lanes().Assigned(ctx)
	if err != nil {
		return w
	}
	for _, a := range as {
		if a.Spec.Name == laneTail {
			continue
		}
		if strings.HasPrefix(a.Spec.Name, lanePlanPrefix) {
			sp, _, err := decodeBlob[planSpec](a.Spec.Spec, specV1)
			if err != nil {
				continue
			}
			tk, _, err := decodeBlob[planToken](a.Cursor.Token, tokenV1)
			if err != nil {
				continue
			}
			w.planned[sp.Table] = tk.Done
			continue
		}
		sp, _, err := decodeBlob[chunkSpec](a.Spec.Spec, specV1)
		if err != nil {
			continue
		}
		tk, _, err := decodeBlob[chunkToken](a.Cursor.Token, tokenV1)
		if err != nil {
			continue
		}
		w.byTable[sp.Table] = append(w.byTable[sp.Table], worldChunk{lo: sp.Lo, hi: sp.Hi, high: tk.High})
	}

	// THE LINE THAT DOES NOT EXIST. With it, w.complete would be true and this connector
	// would be correct:
	//
	//	for after := record.LaneID(""); ; {
	//	    page, err := s.rt.Lanes().All(ctx, after, 1000)
	//	    if err != nil { return w }
	//	    if len(page) == 0 { break }
	//	    for _, a := range page { /* same decode as above, finished lanes included */ }
	//	    after = page[len(page)-1].ID
	//	}
	//	w.complete = true
	//
	// Without it the view is partial by construction and every verdict degrades to
	// verdictWait, which is a pipeline that never emits a changelog event during the
	// scan. That is not a workaround an author would ship; it is the breakage.
	w.complete = false
	return w
}

// stageTailAdvance is where a filtered window must move the cursor with no records, and
// is BREAKAGE B3.
//
// ---------------------------------------------------------------------------
// BREAKAGE B3 — FATAL. A prefix lane's position cannot advance without emitting a record.
//
// Blocking signatures:
//
//	func (s Source) Read(ctx context.Context, dst *record.Batch) error
//	// "It blocks until at least one record is available"
//	type Heartbeater interface { Heartbeat(ctx, lane, idle time.Duration) error }
//	// "Heartbeat is a method on the control goroutine, NOT a batch. It never carries a
//	//  position and cannot advance a cursor"
//
// and the core-side mechanism that makes the alternative impossible, internal/ledger:
//
//	weight := uint64(b.Len()); if weight == 0 { weight = 1 }
//	var refs uint32; for _, r := range b.Records { refs += r.Origin().Refs() }
//	if refs == 0 { refs = 1 }
//	// Ledger.Settle is keyed on record ids, and an empty batch has none.
//
// So a batch with a Position and zero records IS admitted, IS tracked as one outstanding
// node with one outstanding reference, and can NEVER be settled, because settlement
// arrives per record id and there are no record ids. The lane's contiguous prefix stops
// at that node permanently and its cursor freezes for the life of the run. record.Batch
// models the shape — Position is a batch field and EndOfLane "may be set with zero
// records" — and the ledger cannot resolve it.
//
// Two places in this case need it, and both are load-bearing:
//
//  1. The tail. During the snapshot the verdict function above drops nearly every event,
//     because most keys belong to chunks that are unfinished or unplanned. The changelog
//     position must still advance, or a PrunesOnCommit upstream retains its log for the
//     whole 4 TB scan and the disk fills. That is the exact failure Heartbeater was added
//     for, and Heartbeater is explicitly documented as unable to carry a position. The
//     stated reason — "a heartbeat batch carrying a cursor and no records would commit past
//     unsettled records" — is a correct objection to committing IMMEDIATELY and not to
//     admitting an ORDERED position: a position admitted in sequence with refs 0 resolves
//     at once but only ADVANCES the prefix when every node before it has settled, which is
//     precisely the required semantics.
//
//  2. The planning lane. ADR 0008 offers "the chunker's own resume cursor — a chunk-
//     planning lane is a lane, with a cursor" as the reason no core change is needed for a
//     connector-side incremental snapshot. A planning lane produces NO records: its whole
//     output is Announce calls. Its cursor can therefore never advance, so the enumeration
//     frontier across 900 tables is never durable, so a crash at 60% re-enumerates every
//     table from the start. The named remedy does not work for the named purpose.
//
// SMALLEST FIX — no interface change at all, and no connector breakage whatsoever. Make an
// empty positioned batch resolve at admission, in order:
//
//	// internal/ledger.Admit
//	if b.Len() == 0 && !b.EndOfLane {
//	    // A position-only advance. It takes its ordered place in the tracker and
//	    // resolves immediately, so it can never advance the prefix past an unsettled
//	    // group that precedes it, and it cannot be settled later because it has no
//	    // records. This is the ONLY way a lane that filters everything can move.
//	    ticket, err := tracker.TrackResolved(ctx, pos)   // weight 0, resolved on arrival
//	    ...
//	}
//
// with Tracker gaining TrackResolved (or Track honouring refs == 0 instead of raising it
// to 1). Then document on Source.Read that returning a batch with a Position, no records
// and no error is legal and means "everything up to here was filtered". Every connector
// already written is unaffected: none of them emits an empty positioned batch today,
// because today it hangs.
// ---------------------------------------------------------------------------
func (s *Source) stageTailAdvance(to uint64) {
	s.mu.Lock()
	if s.tail != nil && to > s.tail.tok.At {
		s.tail.advance = to
		s.tail.hasAdvance = true
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Source) notify() {
	s.mu.Lock()
	if s.wake != nil {
		close(s.wake)
		s.wake = make(chan struct{})
	}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Optional interfaces. These fit cleanly and are recorded as such.
// ---------------------------------------------------------------------------

// Discover enumerates the 900 tables. Every field the UI needs for a per-table scan
// choice is expressible, including "this stream cannot be scanned", and none of it
// required core knowledge that tables exist. No finding.
func (s *Source) Discover(ctx context.Context) (connector.Catalog, error) {
	tables, err := s.up.Tables(ctx)
	if err != nil {
		return connector.Catalog{}, fault.Transient(fault.OpDiscover, err)
	}
	cat := connector.Catalog{At: time.Now(), Streams: make([]connector.StreamDesc, 0, len(tables))}
	for _, t := range tables {
		cat.Streams = append(cat.Streams, connector.StreamDesc{
			Name:      record.StreamName(t),
			Keys:      [][]string{{"id"}},
			KeysFixed: true,
			Supports:  []connector.LaneKind{connector.LaneKindScan, connector.LaneKindStream},
			Label:     t,
		})
	}
	return cat, nil
}

// Backlog answers "how much is left" per lane. For a chunk lane the answer is exact and
// cheap; for the tail it is an estimate. The Exact flag distinguishing them, rather than
// a label, is right. No finding.
func (s *Source) Backlog(ctx context.Context, lane record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.chunks[lane]; ok {
		return connector.Backlog{
			Records: connector.Count(uint64(len(c.rows))),
			Exact:   c.tok.Phase >= phaseHigh, // exact only once the range has been read
			AsOf:    time.Now(),
		}, nil
	}
	if s.tail != nil && lane == s.tail.id {
		head, err := s.up.LogPosition(ctx)
		if err != nil {
			return connector.Backlog{}, fault.Transient(fault.OpRead, err)
		}
		var lag time.Duration
		return connector.Backlog{
			Records:      connector.Count(head - s.tail.tok.At),
			Exact:        false,
			AsOf:         time.Now(),
			EventTimeLag: &lag,
		}, nil
	}
	return connector.Backlog{AsOf: time.Now()}, nil
}

// Heartbeat is called when a lane has been idle. For the tail it is the only thing
// holding the replication slot while the scan runs — and it cannot advance the cursor,
// which is BREAKAGE B3. What it CAN do is nudge the upstream's own keepalive, so the
// interface is not useless, merely insufficient.
func (s *Source) Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error {
	s.mu.Lock()
	isTail := s.tail != nil && lane == s.tail.id
	at := uint64(0)
	if isTail {
		at = s.tail.tok.At
	}
	s.mu.Unlock()
	if !isTail {
		return nil
	}

	head, err := s.up.LogPosition(ctx)
	if err != nil {
		return fault.Transient(fault.OpRead, err)
	}
	if head > at {
		// We know the changelog has moved and that everything in between was filtered.
		// The ONLY way to record that is a position-only batch, which currently wedges
		// the lane. See BREAKAGE B3.
		s.stageTailAdvance(head)
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventNote, Lane: lane,
			Message: "changelog advanced with every event filtered",
			Detail:  fmt.Sprintf("idle=%s from=%d to=%d", idle, at, head),
		})
	}
	return nil
}

// Compile-time proof that this type satisfies what it declares.
var (
	_ connector.Source          = (*Source)(nil)
	_ connector.Discoverer      = (*Source)(nil)
	_ connector.BacklogReporter = (*Source)(nil)
	_ connector.Heartbeater     = (*Source)(nil)
)

// ---------------------------------------------------------------------------
// BREAKAGE B6 — MAJOR. The prescribed remedy for this case cannot be written.
//
// Blocking signatures:
//
//	type TransformRuntime interface {
//	    Context() context.Context
//	    Log() *slog.Logger
//	    Metrics() Metrics
//	    Note(e Event)
//	    Node() record.NodeID
//	}
//	Apply(ctx context.Context, in *record.Batch, from int, out *record.Batch) (int, error)
//
// ADR 0008 routes the concurrent scan-and-stream case to two things that are "not core
// machinery": the `canal/scan` library, and the `shadow` transform, which "drops an
// OpScanRead record whose Origin.Key has already been seen from a stream lane in this run,
// and self-retires when the last scan lane closes". Neither half is writable.
//
//   - Origin.Key is always nil (BREAKAGE B1), so there is nothing to key the seen-set on.
//     Meta is deep-copied per derivation and readable, so a connector-specific fallback
//     exists — for a transform that knows this connector's namespace and key name, which
//     is source-shaped knowledge in a core-provided component: constraint #1 violated.
//   - "self-retires when the last scan lane closes" needs to know HOW MANY scan lanes
//     exist. in.EndOfLane and in.Lane let a transform observe closures it sees, but
//     TransformRuntime has no Lanes(), so it cannot know the denominator, and in a cluster
//     the chunk lanes on the other 31 workers never traverse this instance at all. In this
//     case the denominator is discovered at runtime and is O(10^5).
//   - The seen-key set is per-run and in memory. TransformRuntime has no State(), so it
//     cannot be persisted; StatefulTransform's SnapshotState/RestoreState exist but are
//     driven by the engine's checkpoint id, not by a lane-scoped store, and the ADR
//     already accepts the loss. The unacknowledged half is the retirement.
//
// SMALLEST FIX. If B4's LaneCtl.All lands, expose the same read to a transform:
//
//	type TransformRuntime interface {
//	    // ... existing methods ...
//	    // Lanes returns a READ-ONLY view of the pipeline's lane table. A transform
//	    // cannot announce or finish; it can only observe.
//	    Lanes() LaneView   // All(ctx, after, limit) ([]LaneAssignment, error)
//	}
//
// Adding a method to TransformRuntime breaks no transform, for the same reason it breaks
// no source: the core implements it. Combined with SetKey from B1, `shadow` becomes
// writable and generic. Without either, ADR 0008's own escape hatch for this case is
// closed, and that is what makes the concurrent story a gap rather than a documented
// limitation.
// ---------------------------------------------------------------------------
