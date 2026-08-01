package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// PAGING AND ROLLUP ARE PURE FUNCTIONS OVER THE LANE LIST, and they are tested as such here.
//
// The end-to-end wiring is asserted against a real pipeline and the real HTTP handler elsewhere,
// which is R8's rule. What lives here is the algebra: the cases that matter are a list of 29,000
// lanes, a lane that finishes between two pages and a stream where one lane cannot answer, and
// standing up a fixture for each of those would test the fixture rather than the arithmetic.

func lane(id, stream string, opts ...func(*telemetry.LaneStatus)) telemetry.LaneStatus {
	l := telemetry.LaneStatus{ID: record.LaneID(id), Stream: stream}
	for _, o := range opts {
		o(&l)
	}
	return l
}

func age(secs float64) func(*telemetry.LaneStatus) {
	return func(l *telemetry.LaneStatus) { l.CheckpointAge = &secs }
}

func backlog(n uint64, exact bool, at time.Time) func(*telemetry.LaneStatus) {
	return func(l *telemetry.LaneStatus) {
		v := n
		l.Backlog = &telemetry.Backlog{Records: &v, Exact: exact, AsOf: at}
	}
}

func ids(ls []telemetry.LaneStatus) []string {
	out := make([]string, 0, len(ls))
	for _, l := range ls {
		out = append(out, string(l.ID))
	}
	return out
}

// EVERY LANE EXACTLY ONCE, ACROSS EVERY PAGE. A pager that skips or repeats is worse than one that
// truncates, because truncation is announced and a gap is not.
func TestPagingVisitsEveryLaneExactlyOnce(t *testing.T) {
	const total, size = 47, 10
	all := make([]telemetry.LaneStatus, 0, total)
	for i := 0; i < total; i++ {
		all = append(all, lane(fmt.Sprintf("lane-%03d", i), "orders"))
	}

	seen := map[string]int{}
	q := telemetry.StatusQuery{LaneLimit: ptr(size)}
	pages := 0
	for {
		page, cursor, truncated := pageLanes(all, q)
		pages++
		if pages > total {
			t.Fatal("paging did not terminate")
		}
		for _, id := range ids(page) {
			seen[id]++
		}
		if !truncated {
			if cursor != "" {
				t.Errorf("the last page carried cursor %q; empty is what ends the walk", cursor)
			}
			break
		}
		if cursor == "" {
			t.Fatal("a truncated page carried no cursor, so the rest is unreachable")
		}
		q.LaneCursor = cursor
	}

	if pages != 5 {
		t.Errorf("%d pages for %d lanes at %d per page, want 5", pages, total, size)
	}
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("lane-%03d", i)
		switch seen[id] {
		case 1:
		case 0:
			t.Errorf("%s was never returned; a gap is a lane an operator cannot see at all", id)
		default:
			t.Errorf("%s was returned %d times", id, seen[id])
		}
	}
}

// KEYSET, NOT OFFSET, and this is the case that distinguishes them. Lanes disappear between reads —
// a finished scan chunk is removed from the live set — and an offset pager skips a row every time
// the list shrinks behind the reader.
func TestPagingDoesNotSkipWhenTheListShrinksBehindTheReader(t *testing.T) {
	all := []telemetry.LaneStatus{
		lane("a", "s"), lane("b", "s"), lane("c", "s"), lane("d", "s"), lane("e", "s"),
	}
	first, cursor, truncated := pageLanes(all, telemetry.StatusQuery{LaneLimit: ptr(2)})
	if !truncated || cursor != "b" {
		t.Fatalf("first page is %v cursor=%q truncated=%v", ids(first), cursor, truncated)
	}

	// "a" finishes and leaves the live set before the next page is asked for. An offset of 2 would
	// now return d and e, skipping c entirely.
	shrunk := []telemetry.LaneStatus{lane("b", "s"), lane("c", "s"), lane("d", "s"), lane("e", "s")}
	second, _, _ := pageLanes(shrunk, telemetry.StatusQuery{LaneLimit: ptr(2), LaneCursor: cursor})
	if got := ids(second); len(got) != 2 || got[0] != "c" {
		t.Errorf("second page is %v, want [c d]: a keyset cursor resumes AFTER b regardless of what "+
			"happened before it", got)
	}
}

// A HEALTH BANNER WANTS NO LANES AT ALL. It renders the phase, the conditions and the rollup, and
// downloading 29,000 lanes to render none of them is the cost the limit exists to remove.
func TestAZeroLimitReturnsNoLanesAndStillCountsThem(t *testing.T) {
	all := []telemetry.LaneStatus{lane("a", "s"), lane("b", "s"), lane("c", "s")}
	page, _, truncated := pageLanes(all, telemetry.StatusQuery{LaneLimit: ptr(0)})
	if len(page) != 0 {
		t.Errorf("a zero limit returned %d lanes", len(page))
	}
	if !truncated {
		t.Error("a zero limit over three lanes did not report the list as cut")
	}
	// Nil is a different request: the producer's default page, not none.
	def, _, _ := pageLanes(all, telemetry.StatusQuery{})
	if len(def) != 3 {
		t.Errorf("the default query returned %d lanes, want all three; nil and 0 are different asks",
			len(def))
	}
}

// A ZERO-LIMIT RESPONSE HAS CONSUMED NOTHING, so its cursor is empty and that is not an end marker.
//
// The two fields have to agree on one reading: LanesTruncated answers "is there more", and the
// cursor is a continuation token whose empty value means "start from the beginning" — which is also
// what a first request sends. Reading the cursor as the terminator instead would make a banner
// response look like an empty pipeline, and the field's own doc comment said exactly that until this
// case was written down.
func TestAZeroLimitCursorIsAStartAndNotAnEnd(t *testing.T) {
	all := []telemetry.LaneStatus{lane("a", "s"), lane("b", "s"), lane("c", "s")}

	_, cursor, truncated := pageLanes(all, telemetry.StatusQuery{LaneLimit: ptr(0)})
	if !truncated {
		t.Fatal("a zero-limit response over three lanes reported nothing more to fetch")
	}
	if cursor != "" {
		t.Errorf("cursor is %q; a caller that consumed no lanes has nothing to continue after", cursor)
	}

	// Feeding that cursor back must return the FIRST page, not nothing.
	page, _, _ := pageLanes(all, telemetry.StatusQuery{LaneCursor: cursor, LaneLimit: ptr(2)})
	if got := ids(page); len(got) != 2 || got[0] != "a" {
		t.Errorf("continuing from a zero-limit response gave %v, want the first page [a b]", got)
	}
}

func TestTheStreamFilterIsTheDrillDown(t *testing.T) {
	all := []telemetry.LaneStatus{
		lane("a", "orders"), lane("b", "customers"), lane("c", "orders"),
	}
	page, _, _ := pageLanes(all, telemetry.StatusQuery{Stream: "orders"})
	if got := ids(page); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("filtering to orders gave %v, want [a c]", got)
	}
	if page, _, _ := pageLanes(all, telemetry.StatusQuery{Stream: "nothing"}); len(page) != 0 {
		t.Errorf("an unknown stream matched %d lanes", len(page))
	}
}

// --- the rollup ---------------------------------------------------------------------------------

// THE ALERT ON A STREAM IS ITS WORST LANE. An average hides one stuck chunk behind thirty-one
// healthy ones, which for a 32-way snapshot is precisely the failure the rollup exists to surface.
func TestTheRollupTakesTheWorstLaneAndNotTheAverage(t *testing.T) {
	all := []telemetry.LaneStatus{
		lane("a", "orders", age(1)), lane("b", "orders", age(2)), lane("c", "orders", age(3600)),
	}
	got := rollUpStreams(all)
	if len(got) != 1 {
		t.Fatalf("%d streams, want 1", len(got))
	}
	if got[0].MaxCheckpointAge == nil || *got[0].MaxCheckpointAge != 3600 {
		t.Errorf("max checkpoint age is %v, want the stuck lane's 3600 and not the mean of 1201",
			got[0].MaxCheckpointAge)
	}
	if got[0].Lanes != 3 {
		t.Errorf("the rollup counted %d lanes, want 3", got[0].Lanes)
	}
}

// A SUM OVER SOME OF THE LANES IS NOT A BACKLOG. One lane that cannot answer makes the stream's
// total unknown, because a partial sum reads as a SMALL backlog rather than an absent one — and of
// the two mistakes that is the dangerous one.
func TestABacklogIsSummedOnlyWhenEveryLaneCanAnswer(t *testing.T) {
	at := time.Unix(1000, 0)
	older := time.Unix(500, 0)

	full := rollUpStreams([]telemetry.LaneStatus{
		lane("a", "orders", backlog(10, true, at)),
		lane("b", "orders", backlog(5, true, older)),
	})
	if full[0].Backlog == nil || full[0].Backlog.Records == nil || *full[0].Backlog.Records != 15 {
		t.Fatalf("backlog is %+v, want a sum of 15", full[0].Backlog)
	}
	// AS FRESH AS ITS STALEST TERM. Reporting the newest AsOf would make a sum containing a
	// ten-minute-old reading look like it was taken just now.
	if !full[0].Backlog.AsOf.Equal(older) {
		t.Errorf("the summed AsOf is %v, want the oldest contributing reading %v",
			full[0].Backlog.AsOf, older)
	}

	partial := rollUpStreams([]telemetry.LaneStatus{
		lane("a", "orders", backlog(10, true, at)),
		lane("b", "orders"), // this source cannot answer
	})
	if partial[0].Backlog != nil {
		t.Errorf("a stream where one lane cannot answer reported a backlog of %+v; "+
			"a partial sum reads as a small backlog rather than an unknown one", partial[0].Backlog)
	}

	// An estimate anywhere makes the total an estimate. An exact count and a guess must not render
	// identically.
	mixed := rollUpStreams([]telemetry.LaneStatus{
		lane("a", "orders", backlog(10, true, at)),
		lane("b", "orders", backlog(5, false, at)),
	})
	if mixed[0].Backlog == nil || mixed[0].Backlog.Exact {
		t.Errorf("a sum containing an estimate is reported exact: %+v", mixed[0].Backlog)
	}
}

func TestTheRollupSeparatesStreamsAndIsOrdered(t *testing.T) {
	got := rollUpStreams([]telemetry.LaneStatus{
		lane("c", "orders"), lane("a", "customers"), lane("b", "orders"),
	})
	if len(got) != 2 {
		t.Fatalf("%d streams, want 2", len(got))
	}
	if got[0].Stream != "customers" || got[1].Stream != "orders" {
		t.Errorf("streams came out %s, %s; the order must be stable or no consumer can diff two reads",
			got[0].Stream, got[1].Stream)
	}
	if got[1].Lanes != 2 {
		t.Errorf("orders rolled up %d lanes, want 2", got[1].Lanes)
	}
}

func TestAnEmptyPipelineRollsUpToNothing(t *testing.T) {
	if got := rollUpStreams(nil); got != nil {
		t.Errorf("an empty lane list produced %d streams", len(got))
	}
}

func ptr[T any](v T) *T { return &v }
