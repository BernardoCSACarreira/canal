package pushsource

// This test exists to prove that the claims in connector.go's comment blocks are
// executable, not rhetorical. It builds a fake SourceRuntime (which the interface
// promises is possible, and it is), drives a real HTTP push through Read and Commit,
// and asserts the handler gets its 204 only after the record settled.
//
// It also pins the two breakages that are observable at runtime rather than at compile
// time: TestOriginKeyIsUnreachable (BREAKAGE 1) and TestNoPromptRefusal (BREAKAGE 4).

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// ---------------------------------------------------------------------------------
// Fakes. SourceRuntime being an interface rather than a struct is what makes this
// possible, and it is one of the design's better calls.
// ---------------------------------------------------------------------------------

// SourceRuntime being an interface rather than a struct is what makes this possible, and it
// is one of the design's better calls. What was NOT a good call was leaving every connector
// to hand-write the fake: these two types were 60 lines, and growing SourceRuntime or LaneCtl
// broke them and four other packages at once. pkg/connectortest supplies the base now, and a
// test overrides only what it asserts on.
type fakeLaneCtl struct {
	connectortest.LaneCtl
}

type fakeRuntime struct {
	connectortest.SourceRuntime
	lanes *fakeLaneCtl
}

func (f *fakeRuntime) Lanes() connector.LaneCtl { return f.lanes }

func newSource(t *testing.T, overrides map[string]any) (*Source, *fakeRuntime, func()) {
	t.Helper()
	spec := Spec()
	raw := map[string]any{"listen": freePort(t), "path": "/ingest", "ack_timeout": "2s"}
	for k, v := range overrides {
		raw[k] = v
	}
	cfg, d := spec.Validate(raw)
	if d.HasErrors() {
		t.Fatalf("config did not validate: %s", d.Error())
	}
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &fakeRuntime{lanes: &fakeLaneCtl{}}
	rt.Ctx = ctx
	if err := s.Open(context.Background(), rt); err != nil {
		cancel()
		t.Fatalf("Open: %v", err)
	}
	return s, rt, func() {
		cancel()
		_ = s.Close(context.Background())
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newTestBatch builds the batch the engine would hand to Read.
func newTestBatch(lane record.LaneID, capHint int) *record.Batch {
	a := record.NewAllocator(record.DefaultTenant, "p", "n", lane, record.DefaultStream, 1, 1)
	return record.NewBatch(a, capHint)
}

// ---------------------------------------------------------------------------------
// The happy path: a synchronous ack that means what it says.
// ---------------------------------------------------------------------------------

func TestSynchronousAckAfterSettlement(t *testing.T) {
	s, _, done := newSource(t, nil)
	defer done()

	type resp struct {
		code int
		at   time.Time
	}
	got := make(chan resp, 1)
	go func() {
		r, err := http.Post("http://"+s.addr+"/ingest", "application/json",
			bytes.NewReader([]byte(`{"hello":"world"}`)))
		if err != nil {
			got <- resp{code: -1}
			return
		}
		_ = r.Body.Close()
		got <- resp{code: r.StatusCode, at: time.Now()}
	}()

	lane := record.DeriveLaneID(record.DefaultTenant, "p", "n", "ingress")
	b := newTestBatch(lane, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("expected 1 record, got %d", b.Len())
	}
	if b.Lane != lane {
		t.Fatalf("lane not set: %q", b.Lane)
	}
	// A discrete lane leaves Position zero, and that is correct.
	if !b.Position.IsZero() {
		t.Fatalf("discrete lane should carry no position, got %+v", b.Position)
	}
	h := b.Records[0].Handle()
	if len(h) == 0 {
		t.Fatal("no delivery handle: the whole ack path depends on it")
	}

	// The handler must still be blocked: nothing has settled.
	select {
	case r := <-got:
		t.Fatalf("handler returned %d before settlement — a 2xx for undurable data", r.code)
	case <-time.After(150 * time.Millisecond):
	}

	// Now do what the ledger does after the sink reports durable.
	if err := s.Commit(context.Background(), connector.Ack{
		Lane: lane, Epoch: 1, Handles: [][]byte{h}, Records: 1,
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	select {
	case r := <-got:
		if r.code != http.StatusNoContent {
			t.Fatalf("expected 204 after settlement, got %d", r.code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never woke after Commit")
	}
}

// ---------------------------------------------------------------------------------
// BREAKAGE 1's regression test: the key the source computes reaches the core.
// ---------------------------------------------------------------------------------

func TestOriginKeyReachesTheCore(t *testing.T) {
	s, _, done := newSource(t, nil)
	defer done()

	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+s.addr+"/ingest",
			bytes.NewReader([]byte(`{"a":1}`)))
		req.Header.Set("Idempotency-Key", "peer-supplied-1234")
		req.Header.Set("Content-Type", "application/json")
		r, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = r.Body.Close()
		}
	}()

	lane := record.DeriveLaneID(record.DefaultTenant, "p", "n", "ingress")
	b := newTestBatch(lane, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	r := b.Records[0]

	// The peer supplied the idempotency key, and it is now in the field the core reads.
	const want = "peer-supplied-1234"
	if got := string(r.Origin().Key); got != want {
		t.Fatalf("Origin.Key is %q, want %q", got, want)
	}
	if got := string(r.Origin().Upstream); got != want {
		t.Fatalf("Origin.Upstream is %q, want %q", got, want)
	}
	// record.Ref is the identity a sink reports outcomes against and the engine dedupes on.
	// This is the assertion that matters: a setter writing a field nothing reads would pass
	// the two above and fail here.
	if got := string(r.Ref().Key); got != want {
		t.Fatalf("Ref.Key is %q, want %q: the key does not reach the sink-facing identity", got, want)
	}
	// Meta still carries it, now as human-facing provenance rather than as the only copy.
	v, ok := r.Meta.Get(record.NSSource, "idempotency_key")
	if !ok {
		t.Fatal("provenance metadata missing")
	}
	if string(v.(record.String)) != want {
		t.Fatalf("provenance metadata wrong: %v", v)
	}

	// The settlement half of Origin stays a copy: mutating it must still be a no-op, because
	// the two identity setters must not have bought a writable Origin.Lane.
	o := r.Origin()
	o.Lane = "somewhere-else"
	if r.Origin().Lane == "somewhere-else" {
		t.Fatal("mutating the Origin copy reached the record; settlement identity became writable")
	}

	// And the capability the key unlocks is now declarable without lying.
	if !Caps().StableKeys {
		t.Error("StableKeys is not declared, but the key is populated and stable across peer retries")
	}
	if !Caps().RedeliversUnacked {
		t.Error("RedeliversUnacked is not declared, so this source is still clamped to at-most-once " +
			"and would ack its peer before durability")
	}

	_ = s.Commit(context.Background(), connector.Ack{
		Lane: lane, Handles: [][]byte{r.Handle()}, Records: 1,
	})
}

// ---------------------------------------------------------------------------------
// BREAKAGE 4, at runtime: the only refusal available costs the peer its whole deadline.
// ---------------------------------------------------------------------------------

func TestNoPromptRefusal(t *testing.T) {
	// ack_timeout is the peer's budget. Nothing ever calls Read, which is exactly what
	// a source observes when Ledger.Admit is blocking on a full lane budget.
	s, _, done := newSource(t, map[string]any{"ack_timeout": "600ms", "retry_after": "1s"})
	defer done()

	start := time.Now()
	r, err := http.Post("http://"+s.addr+"/ingest", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = r.Body.Close() }()
	elapsed := time.Since(start)

	if r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when the pipeline never admits, got %d", r.StatusCode)
	}
	if r.Header.Get("Retry-After") == "" {
		t.Fatal("a refusal without Retry-After is a dropped connection with extra steps")
	}
	// THE FINDING: the refusal was available at t=0 and took the full deadline, because
	// a source cannot observe admission pressure. See BREAKAGE 4.
	if elapsed < 500*time.Millisecond {
		t.Fatalf("refusal was prompt (%v) — BREAKAGE 4 may be wrong", elapsed)
	}
	t.Logf("BREAKAGE 4: refusal took %v; the answer was knowable at t=0", elapsed)
}

// ---------------------------------------------------------------------------------
// Terminal failure closes the loop, via Nackable — which is mandatory here and
// documented as niche.
// ---------------------------------------------------------------------------------

func TestNackAnswersTheHandler(t *testing.T) {
	s, _, done := newSource(t, nil)
	defer done()

	got := make(chan int, 1)
	go func() {
		r, err := http.Post("http://"+s.addr+"/ingest", "application/json", bytes.NewReader([]byte(`bad`)))
		if err != nil {
			got <- -1
			return
		}
		_ = r.Body.Close()
		got <- r.StatusCode
	}()

	lane := record.DeriveLaneID(record.DefaultTenant, "p", "n", "ingress")
	b := newTestBatch(lane, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read: %v", err)
	}
	h := b.Records[0].Handle()

	if err := s.Nack(context.Background(), lane, []connector.Nack{{
		Handle: h, Class: fault.PermanentMapping, Reason: "unencodable", Attempts: 1,
	}}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	select {
	case code := <-got:
		if code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 for a terminally failed record, got %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never woke after Nack")
	}
}

// Close must answer every socket it is holding, or a graceful shutdown looks like a
// crash to every peer.
func TestCloseShedsInFlight(t *testing.T) {
	s, _, _ := newSource(t, nil)

	got := make(chan int, 1)
	go func() {
		r, err := http.Post("http://"+s.addr+"/ingest", "application/json", bytes.NewReader([]byte(`{}`)))
		if err != nil {
			got <- -1
			return
		}
		_ = r.Body.Close()
		got <- r.StatusCode
	}()

	lane := record.DeriveLaneID(record.DefaultTenant, "p", "n", "ingress")
	b := newTestBatch(lane, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Read(ctx, b); err != nil {
		t.Fatalf("Read: %v", err)
	}

	shutdown, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	go func() { _ = s.Close(shutdown) }()

	select {
	case code := <-got:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 on shutdown, got %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not answer the in-flight handler")
	}
}

// Registration must not panic and must produce no descriptor warnings. This is the
// "implement, register, done" claim, tested.
func TestRegistersCleanly(t *testing.T) {
	r := registry.New()
	Register(r)
	e, ok := r.Source(Name)
	if !ok {
		t.Fatal("not registered")
	}
	if len(e.Descriptor.Warnings) > 0 {
		t.Fatalf("descriptor warnings: %v", e.Descriptor.Warnings)
	}
	for _, c := range e.Descriptor.Capabilities {
		if !c.Present && c.Reason == "" {
			t.Errorf("capability %q is absent with no reason", c.Name)
		}
	}
}
