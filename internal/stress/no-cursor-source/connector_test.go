package nocursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// ---------------------------------------------------------------------------
// Fakes. Their existence is itself a finding in the POSITIVE direction: because
// SourceRuntime and LaneCtl are INTERFACES, a third-party connector is unit-testable
// against ~90 lines of fake with no engine, no ledger and no store. A concrete struct with
// unexported state would have made every test below impossible.
// ---------------------------------------------------------------------------

// The fakes are connectortest's, EMBEDDED rather than hand-written.
//
// The original versions of these two types were 60 lines of method-by-method
// implementation, and their existence was recorded here as a finding in the positive
// direction: because SourceRuntime and LaneCtl are interfaces, a third-party connector is
// unit-testable with no engine, no ledger and no store. That half was right. The half that
// was wrong is that growing either interface — canal's declared free growth path — broke
// every one of those hand-written fakes at once. pkg/connectortest is the fix, and this file
// is now four lines of fake instead of sixty.
type fakeLanes struct {
	connectortest.LaneCtl
	revoked bool
}

func (f *fakeLanes) Revoked(record.LaneID) bool { return f.revoked }

type fakeRT struct {
	connectortest.SourceRuntime
	lanes *fakeLanes
}

func (r *fakeRT) Lanes() connector.LaneCtl { return r.lanes }

func newBatch(lane record.LaneID, cap int) *record.Batch {
	a := record.NewAllocator(record.DefaultTenant, "p", "n", lane, "items", 1, 1)
	return record.NewBatch(a, cap)
}

// ---------------------------------------------------------------------------
// A feed that behaves exactly as badly as the hostile case says.
// ---------------------------------------------------------------------------

type feedItem struct {
	ID      string `json:"id"`
	Updated int64  `json:"updated_at"`
	Body    string `json:"body"`
}

type badFeed struct {
	// pages is returned in order, one per request, cycling back to the last page.
	pages   [][]feedItem
	hasMore []bool
	calls   int
	status  int    // if non-zero, returned once
	retryHi string // Retry-After header for that status
}

func (f *badFeed) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.calls++
		if f.status != 0 {
			st := f.status
			f.status = 0
			if f.retryHi != "" {
				w.Header().Set("Retry-After", f.retryHi)
			}
			w.WriteHeader(st)
			_, _ = w.Write([]byte(`{"error":"slow down"}`))
			return
		}
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		idx := 0
		if lim > 0 {
			idx = off / lim
		}
		if idx >= len(f.pages) {
			idx = len(f.pages) - 1
		}
		out := struct {
			Items   []feedItem `json:"items"`
			HasMore bool       `json:"has_more"`
		}{Items: f.pages[idx], HasMore: f.hasMore[idx]}
		_ = json.NewEncoder(w).Encode(out)
	}
}

func build(t *testing.T, endpoint string, over map[string]any) *src {
	t.Helper()
	e, ok := registry.Default.Source("stress_no_cursor_feed")
	if !ok {
		t.Fatal("source is not registered")
	}
	raw := map[string]any{
		"endpoint":       endpoint,
		"auth_token":     "tok",
		"page_size":      2,
		"poll_interval":  "0s",
		"safety_lag":     "60s",
		"overlap":        "10m",
		"max_pinned_ids": 100,
	}
	for k, v := range over {
		raw[k] = v
	}
	cfg, diags := e.Spec.Validate(raw)
	if diags.HasErrors() {
		t.Fatalf("config does not validate: %v", diags)
	}
	s, err := e.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cs, ok := s.(*src)
	if !ok {
		t.Fatalf("registry returned %T", s)
	}
	return cs
}

// TestColdStartPagesAndDedupes is the core claim: a multi-page cycle over an unordered,
// tie-heavy, duplicate-emitting feed produces exactly one commit point, at the end.
func TestColdStartPagesAndDedupes(t *testing.T) {
	sec := time.Now().Add(-5 * time.Minute).Unix()
	feed := &badFeed{
		pages: [][]feedItem{
			// Page 1 and page 2 SHARE item "b" — the same item on two pages of one cycle —
			// and every item ties on the same updated_at second.
			{{ID: "a", Updated: sec, Body: "1"}, {ID: "b", Updated: sec, Body: "2"}},
			{{ID: "b", Updated: sec, Body: "2"}, {ID: "c", Updated: sec, Body: "3"}},
			{},
		},
		hasMore: []bool{true, true, false},
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	s := build(t, srv.URL, nil)
	rt := &fakeRT{lanes: &fakeLanes{}}
	ctx := context.Background()
	if err := s.Open(ctx, rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close(ctx) }()

	b := newBatch(s.lane, 500)

	// Read 1: page 1, cycle continues -> unsafe position, no commit point.
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	if b.Len() != 2 {
		t.Fatalf("Read 1 produced %d records, want 2", b.Len())
	}
	if b.Position.Safe {
		t.Fatal("Read 1 published a SAFE position mid-cycle; the core would commit inside a cycle")
	}

	// Read 2: page 2. "b" repeats and must be dropped; "c" is new.
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read 2: %v", err)
	}
	if b.Len() != 1 || b.Records[0].Meta.Len() == 0 {
		t.Fatalf("Read 2 produced %d records, want 1 (only the new id)", b.Len())
	}
	gotID, _ := b.Records[0].Meta.Get(record.NSSource, "item_id")
	if gotID != record.String("c") {
		t.Fatalf("Read 2 delivered %v, want c", gotID)
	}
	if b.Position.Safe {
		t.Fatal("Read 2 published a SAFE position while has_more was still true")
	}

	// Read 3: the empty short page closes the cycle. It yields no records, so the source
	// must NOT hand back an empty batch (it would wedge the ledger); it keeps polling. With
	// poll_interval 0 it will spin, so bound it and expect the drain path.
	dctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	err := s.Read(dctx, b)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read 3 returned %v, want DeadlineExceeded from the drain path", err)
	}
	if b.Len() != 0 {
		t.Fatalf("Read 3 handed back %d records; the cycle closed with nothing new", b.Len())
	}

	// The in-memory watermark advanced even though nothing could carry it, which is the
	// mitigation described in BREAKAGE 2.
	if s.cur.Watermark == 0 {
		t.Fatal("the in-memory watermark did not advance across a completed cycle")
	}
	if len(s.cur.Pinned) != 3 {
		t.Fatalf("pinned %d ids, want 3 (all three tie inside the overlap window)", len(s.cur.Pinned))
	}
}

// TestSafePositionAppearsWithRecords checks the one shape the ledger can actually commit.
func TestSafePositionAppearsWithRecords(t *testing.T) {
	sec := time.Now().Add(-5 * time.Minute).Unix()
	feed := &badFeed{
		// One short page: the cycle opens and closes in one fetch, with a record to carry
		// the position.
		pages:   [][]feedItem{{{ID: "a", Updated: sec}}},
		hasMore: []bool{false},
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	s := build(t, srv.URL, nil)
	rt := &fakeRT{lanes: &fakeLanes{}}
	ctx := context.Background()
	if err := s.Open(ctx, rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	b := newBatch(s.lane, 500)
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("got %d records, want 1", b.Len())
	}
	if !b.Position.Safe {
		t.Fatal("a completed cycle carrying a record must publish Safe:true")
	}
	if b.Position.Comparable() {
		t.Fatal("this source must NOT claim comparable positions; ties make them partially ordered")
	}
	if b.Position.Label == "" {
		t.Fatal("no operator-visible position label")
	}

	c, err := decodeCursor(b.Position.Token)
	if err != nil {
		t.Fatalf("the core cannot round-trip this token: %v", err)
	}
	if c.Watermark == 0 || len(c.Pinned) != 1 {
		t.Fatalf("cursor is %+v, want a watermark and one pinned id", c)
	}

	// Commit is what the core calls after phase two. It must accept its own bytes back.
	if err := s.Commit(ctx, connector.Ack{Lane: s.lane, Epoch: 1, Through: b.Position, Records: 1}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := s.Heartbeat(ctx, s.lane, time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

// TestWarmStartSkipsPinnedIDs is the restart claim: given only the token, the source
// re-reads the overlap window and re-delivers nothing.
func TestWarmStartSkipsPinnedIDs(t *testing.T) {
	sec := time.Now().Add(-5 * time.Minute).Unix()
	feed := &badFeed{
		pages:   [][]feedItem{{{ID: "a", Updated: sec}}},
		hasMore: []bool{false},
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	// Cycle one, cold.
	s1 := build(t, srv.URL, nil)
	rt1 := &fakeRT{lanes: &fakeLanes{}}
	if err := s1.Open(context.Background(), rt1); err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	b := newBatch(s1.lane, 500)
	if err := s1.Read(context.Background(), b); err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	token := b.Position.Token
	lane := s1.lane
	_ = s1.Close(context.Background())

	// Cycle two: a brand-new process handed nothing but the token.
	s2 := build(t, srv.URL, nil)
	warm := &fakeLanes{}
	warm.Rows = []connector.LaneAssignment{{
		ID:     lane,
		Spec:   connector.LaneSpec{Name: "feed:" + srv.URL, Stream: "items"},
		Cursor: record.Position{Token: token, Safe: true},
		Epoch:  2,
	}}
	rt2 := &fakeRT{lanes: warm}
	if err := s2.Open(context.Background(), rt2); err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer func() { _ = s2.Close(context.Background()) }()
	if len(s2.curSet) != 1 {
		t.Fatalf("warm start restored %d pinned ids, want 1", len(s2.curSet))
	}
	dctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := s2.Read(dctx, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read after restart returned %v; it should have found nothing new and drained", err)
	}
	if b.Len() != 0 {
		t.Fatalf("restart re-delivered %d records that the pinned set should have absorbed", b.Len())
	}
}

// TestThrottleCarriesRetryAfter is BREAKAGE 3 as an executable observation: the hint IS
// carried, and the class is a fault, because there is nothing else to be.
func TestThrottleCarriesRetryAfter(t *testing.T) {
	feed := &badFeed{
		pages:   [][]feedItem{{}},
		hasMore: []bool{false},
		status:  http.StatusTooManyRequests,
		retryHi: "7",
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	s := build(t, srv.URL, nil)
	rt := &fakeRT{lanes: &fakeLanes{}}
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	b := newBatch(s.lane, 500)
	err := s.Read(context.Background(), b)
	if err == nil {
		t.Fatal("a 429 produced no error")
	}
	if got := fault.ClassOf(err); got != fault.TransientUpstream {
		t.Fatalf("429 classified as %s; TransientUpstream is the only honest choice available", got)
	}
	d, ok := fault.RetryAfterOf(err)
	if !ok || d != 7*time.Second {
		t.Fatalf("Retry-After survived as (%s, %v), want (7s, true)", d, ok)
	}
	if s.nextAllowed.IsZero() {
		t.Fatal("the local rate-limit gate was not armed")
	}
	if s.cyc != nil {
		t.Fatal("the cycle must be discarded: offset pagination has no stable resume point")
	}
	var sawDegraded bool
	for _, e := range rt.Events {
		if e.Kind == connector.EventDegraded {
			sawDegraded = true
		}
	}
	if !sawDegraded {
		t.Fatal("the throttle was not surfaced as an operator-visible event")
	}
}

// TestBatchCapStashesRemainder checks that Batch.Add returning nil at the hard cap does not
// lose the tail of a page.
func TestBatchCapStashesRemainder(t *testing.T) {
	sec := time.Now().Add(-5 * time.Minute).Unix()
	feed := &badFeed{
		pages: [][]feedItem{
			{{ID: "a", Updated: sec}, {ID: "b", Updated: sec}},
			{},
		},
		hasMore: []bool{true, false},
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	s := build(t, srv.URL, nil)
	rt := &fakeRT{lanes: &fakeLanes{}}
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	b := newBatch(s.lane, 1) // hard cap of ONE record
	if err := s.Read(context.Background(), b); err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("got %d, want 1 at the hard cap", b.Len())
	}
	if len(s.cyc.pending) != 1 {
		t.Fatalf("the un-emitted tail was dropped: pending is %d", len(s.cyc.pending))
	}
	if err := s.Read(context.Background(), b); err != nil {
		t.Fatalf("Read 2: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("Read 2 got %d, want the stashed record", b.Len())
	}
	id, _ := b.Records[0].Meta.Get(record.NSSource, "item_id")
	if id != record.String("b") {
		t.Fatalf("Read 2 delivered %v, want the stashed b", id)
	}
}

// TestNoKeyIsSettable is BREAKAGE 1 as a runtime assertion. The two lines that would fix it
// do not compile, so this asserts the CONSEQUENCE: every record this source emits is
// unkeyed, and no sink can dedupe it.
func TestNoKeyIsSettable(t *testing.T) {
	sec := time.Now().Add(-5 * time.Minute).Unix()
	feed := &badFeed{
		pages:   [][]feedItem{{{ID: "a", Updated: sec}}},
		hasMore: []bool{false},
	}
	srv := httptest.NewServer(feed.handler())
	defer srv.Close()

	s := build(t, srv.URL, nil)
	rt := &fakeRT{lanes: &fakeLanes{}}
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	b := newBatch(s.lane, 10)
	if err := s.Read(context.Background(), b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	r := b.Records[0]
	if r.Origin().Key != nil {
		t.Fatal("Origin.Key is populated -- BREAKAGE 1 has been fixed; delete this test")
	}
	if r.Ref().Key != nil {
		t.Fatal("Ref.Key is populated -- BREAKAGE 1 has been fixed; delete this test")
	}
	if r.Origin().Upstream != nil {
		t.Fatal("Origin.Upstream is populated -- BREAKAGE 1 has been fixed; delete this test")
	}
	// The item id exists, perfectly stable, one method call away from being useful.
	id, ok := r.Meta.Get(record.NSSource, "item_id")
	if !ok || id != record.String("a") {
		t.Fatalf("even the workaround failed: %v %v", id, ok)
	}
	e, _ := registry.Default.Source("stress_no_cursor_feed")
	if e.Caps.StableKeys {
		t.Fatal("StableKeys must stay false while Origin.Key is unsettable")
	}
	fmt.Fprintf(nopWriter{}, "")
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
