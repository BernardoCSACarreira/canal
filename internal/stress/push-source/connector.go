// Package pushsource is a HOSTILE connector written to stress canal's source
// interfaces. It is not a product connector and is not meant to ship.
//
// # THE CASE
//
// An inbound push endpoint canal does not control the timing of: an HTTP webhook
// receiver (and, structurally identically, a server-streaming gRPC ingest where the
// remote peer is the client). Properties that make it hostile:
//
//   - Nothing to poll. There is no upstream cursor, no offset, no LSN, no resume
//     token. `Read` cannot "fetch"; it can only wait for something that already
//     happened.
//   - The peer wants a SYNCHRONOUS ack. A 2xx must mean "durable in canal's
//     downstream", so the HTTP handler cannot return until the record has settled.
//   - The peer applies its OWN retry after 5s of no ack. So the connector has a hard
//     per-record settlement deadline it did not choose and cannot negotiate.
//   - Backpressure must be REFUSABLE. When the pipeline is saturated the correct
//     answer to the peer is 429/503 + Retry-After, promptly, BEFORE the record enters
//     the pipeline — not a 200 for undurable data and not a five-second hang.
//   - Redelivery is driven by the PEER, not by canal seeking backwards.
//   - In a cluster the load balancer, not canal's planner, decides which worker a
//     request lands on. Work assignment is external in a sense `UnitsExternal` names
//     but the lane vocabulary cannot express.
//
// # WHAT WORKS, AND IT IS MORE THAN I EXPECTED
//
// The discrete-ordering / delivery-handle path is a genuinely good fit and carries the
// whole design:
//
//	handler enqueues arrival ──> Read drains it, r.SetHandle(h), pending[h] = arrival
//	                        ──> engine admits, graph runs, sink writes, group settles
//	                        ──> Ledger.emitDiscrete ──> Source.Commit(Ack{Handles})
//	                        ──> pending[h].done <- settled ──> handler writes 204
//
// `Ordering: OrderingDiscrete` + `Record.SetHandle` + `Ack.Handles` + `Nackable` is
// exactly the vocabulary a synchronous-ack push source needs, and no other surveyed
// framework has it as a first-class concept. `Source` is four methods and I needed
// four methods. `SourceRuntime.Context()` is exactly the connection-lifetime context
// an `http.Server` needs. `StateHandle` is correctly untouched — this source has no
// durable state at all, and the interface let me have none.
//
// Five things do not work. They are argued at length at the bottom of this file, in
// BREAKAGE 1..5, in severity order. Summary:
//
//	1 FATAL   record.Origin.Key is unsettable by a source. No exported mutator exists.
//	          The idempotency key is the ONLY defence against peer-retry duplicates and
//	          it cannot be attached. This is a hard compile error, reproduced below.
//	2 FATAL   SourceCaps.Replayable=false forces AtMostOnce, which settles on hand-over
//	          and therefore acks the peer before the data is durable. A truthful
//	          declaration produces the exact failure the design forbids.
//	3 MAJOR   No worker/instance identity on SourceRuntime, so N replicas behind one
//	          load balancer cannot announce N lanes. UnitsExternal names the concept and
//	          nothing implements it.
//	4 MAJOR   No way to observe admission pressure. LaneCtl has Budget() but not
//	          headroom/blocked. A prompt 429 is unreachable; the only refusal available
//	          is "wait out the peer's 5s deadline, then refuse".
//	5 MINOR   Ack.Abandoned is a COUNT while Ack.Handles is a LIST, and the ledger drops
//	          a partially-abandoned group's good handles entirely. Handlers hang.
//
// A sixth item is a documentation defect rather than an interface one and is noted at
// BREAKAGE 6: architecture.md claims "no connector-owned goroutine is sanctioned" and
// files the risk as Moot. A push source cannot exist without one.
package pushsource

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// Name is the registry key.
const Name = "http_push"

// maxBody caps a single pushed body. A push source must bound what a stranger can
// send it; nothing in the core can do this on its behalf.
const maxBody = 4 << 20

// verdict is what the pipeline eventually decided about one pushed message. It is the
// vocabulary the HTTP handler renders into a status code.
type verdict uint8

const (
	// verdictSettled means the record reached a sink and the group settled. 2xx.
	verdictSettled verdict = iota + 1

	// verdictRejected means the record reached a TERMINAL disposition — dead-lettered
	// or dropped. The data is not coming back, so a retry is pointless: 422.
	verdictRejected

	// verdictShed means the record never made it into the pipeline, or its fate is
	// unknown and canal is going away. 503 + Retry-After: the peer must resend.
	verdictShed
)

// arrival is one in-flight push. It is the connector's own settlement unit, existing
// only because the HTTP handler goroutine and the engine's read goroutine are
// different goroutines and the handler must be woken by name.
type arrival struct {
	// handle is the synthetic delivery handle. It is what makes this whole design
	// work: it is the source's own vocabulary, the core carries it verbatim on the
	// record, and Ack.Handles hands it back. Nothing else in the interface set could
	// have woken this specific HTTP handler.
	handle []byte

	// idem is the peer's idempotency key, or a content hash when the peer supplied
	// none. THIS IS THE VALUE THAT CANNOT BE ATTACHED TO THE RECORD. See BREAKAGE 1.
	idem []byte

	body []byte
	hdrs [][2]string

	at       time.Time
	deadline time.Time

	// done is buffered so resolve never blocks on a handler that has already given up.
	done chan verdict

	// once guards double-resolution: Commit and Nack and Close can all race for the
	// same arrival, and the core makes no promise that they cannot.
	once sync.Once
}

func (a *arrival) resolve(v verdict) {
	a.once.Do(func() { a.done <- v })
}

// Source is the push source.
//
// CONCURRENCY. The core promises Read is never concurrent with itself and that
// Commit/Nack/Backlog/Heartbeat share one separate control goroutine. That promise is
// worth exactly nothing here, because the third participant is N HTTP handler
// goroutines the core has never heard of. So this source needs the mutex the core's
// contract says it should not need, and it needs it around a map, which is the thing
// the contract's "a source needs at most one mutex" sentence is written to avoid.
// That is not a breakage — it is the honest cost of owning a server — but it is worth
// recording that the stated concurrency budget assumes a pull source.
type Source struct {
	// Config, immutable after New.
	addr       string
	path       string
	ackTimeout time.Duration
	maxPending int
	laneName   string
	stream     record.StreamName
	retryAfter time.Duration

	rt connector.SourceRuntime

	// arrivals is an UNBUFFERED rendezvous, and that is deliberate: see BREAKAGE 4.
	// A buffered channel here would be a second, invisible, unmeasured in-flight bound
	// competing with the lane budget, and the design explicitly has exactly one such
	// concept. Unbuffered means a handler that has been picked up by Read is, by
	// definition, on its way into the ledger; a handler that has not been picked up has
	// definitely not been admitted and can be refused truthfully.
	arrivals chan *arrival

	// serveErr carries the http.Server's terminal error into Read's select. Without
	// this, a listener that dies leaves Read blocked forever on a channel nobody will
	// ever write to, and the engine never learns the source is dead.
	serveErr chan error

	mu      sync.Mutex
	lane    record.LaneID
	laneOK  bool
	pending map[string]*arrival
	srv     *http.Server
	closed  bool

	// waiting counts handlers that hold a body and have not yet been picked up by
	// Read. It is the connector's own admission gate, and it exists only because the
	// core will not tell it whether admission would block. See BREAKAGE 4.
	waiting atomic.Int64

	cShed    connector.Counter
	cSettled connector.Counter
	cOrphan  connector.Counter
	gWaiting connector.Gauge
	hLatency connector.Histogram
}

// Compile-time assertion that the hostile connector satisfies everything it claims.
var (
	_ connector.Source          = (*Source)(nil)
	_ connector.Nackable        = (*Source)(nil)
	_ connector.BacklogReporter = (*Source)(nil)
	_ connector.Validator       = (*Source)(nil)
	_ connector.Prober          = (*Source)(nil)
)

// New builds the source from pre-validated config and does NO I/O, per the contract.
// Binding the listener is I/O and belongs in Open.
func New(ctx context.Context, cfg *config.Config) (*Source, error) {
	s := &Source{
		addr:       config.Must[string](cfg, "listen"),
		path:       config.Must[string](cfg, "path"),
		ackTimeout: config.Must[time.Duration](cfg, "ack_timeout"),
		maxPending: config.Must[int](cfg, "max_waiting"),
		laneName:   config.Must[string](cfg, "lane_name"),
		stream:     record.StreamName(config.Must[string](cfg, "stream")),
		retryAfter: config.Must[time.Duration](cfg, "retry_after"),
		arrivals:   make(chan *arrival),
		serveErr:   make(chan error, 1),
		pending:    map[string]*arrival{},
	}
	return s, cfg.Err()
}

// Open announces the lane and starts the listener. It is idempotent: a second call
// re-announces (Announce is idempotent on the name) and leaves a running server
// alone.
func (s *Source) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.mu.Lock()
	s.rt = rt
	running := s.srv != nil
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return fault.Contract(fault.OpOpen, errors.New("push source reopened after Close"))
	}

	// BREAKAGE 3 lives on this line. laneName is a config string, so every replica of
	// this pipeline announces the SAME lane name, derives the SAME LaneID, and exactly
	// one of them will hold the lease. The others bind their port and 503 everything.
	id, err := rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name:        s.laneName,
		Stream:      s.stream,
		Kind:        connector.LaneKindStream,
		Ordering:    connector.OrderingDiscrete,
		Boundedness: connector.Unbounded,
		Group:       record.LaneGroup("ingress"),
		Label:       "inbound push endpoint " + s.addr + s.path,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.lane, s.laneOK = id, true
	s.mu.Unlock()

	if err := s.initMetrics(rt); err != nil {
		return err
	}
	if running {
		return nil
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		// A port already in use is worth retrying — the previous generation may still
		// be shutting down — so this is transient, not permanent.
		return &fault.Fault{
			Class: fault.TransientUpstream, Op: fault.OpOpen,
			User: "could not bind " + s.addr,
			Dev:  "net.Listen failed", Err: err, RetryAfter: time.Second,
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.path, s.serveHTTP)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// The server's own lifetime is the COMPONENT's, not this call's: ctx here may
		// be cancelled the instant Open returns. This is the one contract note in
		// Source.Open that saved me a bug.
		BaseContext: func(net.Listener) context.Context { return rt.Context() },
	}
	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()

	// BREAKAGE 6: a connector-owned goroutine, which the architecture says is
	// unsanctioned and files as Moot.
	go func() { s.serveErr <- srv.Serve(ln) }()
	rt.Note(connector.Event{
		At: time.Now(), Kind: connector.EventNote, Lane: id,
		Message: "push endpoint listening", Detail: s.addr + s.path,
	})
	return nil
}

func (s *Source) initMetrics(rt connector.SourceRuntime) error {
	m := rt.Metrics()
	var err error
	if s.cShed, err = m.Counter("shed_total", "reason"); err != nil {
		return err
	}
	if s.cSettled, err = m.Counter("acked_total", "verdict"); err != nil {
		return err
	}
	if s.cOrphan, err = m.Counter("orphaned_total"); err != nil {
		return err
	}
	if s.gWaiting, err = m.Gauge("waiting_handlers"); err != nil {
		return err
	}
	s.hLatency, err = m.Histogram("ack_latency_seconds",
		[]float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 10})
	return err
}

// serveHTTP is the inbound handler. It runs on a goroutine the core does not know
// exists.
func (s *Source) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "unreadable or oversized body", http.StatusBadRequest)
		return
	}

	now := time.Now()
	a := &arrival{
		body:     body,
		at:       now,
		deadline: now.Add(s.ackTimeout),
		done:     make(chan verdict, 1),
		hdrs:     captureHeaders(r),
	}
	a.idem = idempotencyKey(r, body)
	a.handle = newHandle(a.idem, now)

	// ---- the connector's own admission gate, which should not have to exist -------
	n := s.waiting.Add(1)
	defer func() {
		s.gWaiting.Set(float64(s.waiting.Add(-1)))
	}()
	s.gWaiting.Set(float64(n))
	if n > int64(s.maxPending) {
		s.refuse(w, "ingress_full")
		return
	}

	ctx := r.Context()
	handoff := time.NewTimer(time.Until(a.deadline))
	defer handoff.Stop()

	// Phase one: hand the arrival to Read. Until this succeeds the record is
	// PROVABLY not admitted, so refusing is truthful and costs the peer nothing but a
	// retry.
	select {
	case s.arrivals <- a:
	case <-ctx.Done():
		return
	case <-handoff.C:
		// The pipeline never came for it. This is the only backpressure signal
		// available, and it costs the peer its entire deadline. See BREAKAGE 4.
		s.refuse(w, "not_admitted_before_deadline")
		return
	}

	// Phase two: wait for settlement. From here on the record IS in canal and a
	// refusal means a duplicate on the peer's retry.
	settle := time.NewTimer(time.Until(a.deadline))
	defer settle.Stop()
	select {
	case v := <-a.done:
		s.hLatency.Observe(time.Since(a.at).Seconds())
		switch v {
		case verdictSettled:
			s.cSettled.Add(1, "settled")
			w.WriteHeader(http.StatusNoContent)
		case verdictRejected:
			s.cSettled.Add(1, "rejected")
			http.Error(w, "record was rejected downstream and will not be retried",
				http.StatusUnprocessableEntity)
		default:
			s.cSettled.Add(1, "shed")
			s.refuse(w, "shutting_down")
		}
	case <-ctx.Done():
		// The peer hung up. The record is still in flight; it will settle and its
		// handle will arrive at Commit with nobody waiting.
		s.forget(a, "peer_gone")
	case <-settle.C:
		// DUPLICATE WINDOW. The record may still land. The peer will resend. The only
		// thing that could deduplicate it is Origin.Key, which cannot be set.
		// See BREAKAGE 1.
		s.forget(a, "settle_deadline")
		s.refuse(w, "settle_deadline")
	}
}

func (s *Source) refuse(w http.ResponseWriter, reason string) {
	s.cShed.Add(1, reason)
	w.Header().Set("Retry-After", strconv.Itoa(int(s.retryAfter.Seconds())))
	// 429 when the refusal is ours (we are full); 503 when it is the pipeline's.
	code := http.StatusServiceUnavailable
	if reason == "ingress_full" {
		code = http.StatusTooManyRequests
	}
	http.Error(w, reason, code)
}

// forget drops an arrival from the pending map because nobody is waiting for it any
// more. The record itself is untouched and will still settle.
func (s *Source) forget(a *arrival, reason string) {
	s.mu.Lock()
	delete(s.pending, string(a.handle))
	s.mu.Unlock()
	a.resolve(verdictShed)
	s.cOrphan.Add(1)
	if s.rt != nil {
		s.rt.Log().Warn("push arrival orphaned",
			"reason", reason, "handle", hex.EncodeToString(a.handle))
	}
}

// Read blocks for the first arrival then drains without blocking.
//
// It never "fetches". There is nothing to fetch. The whole method is a select.
func (s *Source) Read(ctx context.Context, dst *record.Batch) error {
	s.mu.Lock()
	lane, laneOK := s.lane, s.laneOK
	s.mu.Unlock()
	if !laneOK {
		return fault.ErrNotConnected
	}
	if s.rt != nil && s.rt.Lanes().Revoked(lane) {
		// Another worker holds this lane. Every handler must be refused, because a
		// revoked lane's acks are never delivered and a 2xx would be a lie.
		s.shedAllWaiting()
		return fault.ErrNotConnected
	}
	dst.Lane = lane
	// No dst.Position: a discrete lane's progress vocabulary is per-record handles,
	// and this is one of the places the interface set is exactly right.

	select {
	case a := <-s.arrivals:
		if !s.stamp(dst, a) {
			return nil
		}
	case err := <-s.serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fault.Transient(fault.OpRead, err)
		}
		// The server closed cleanly and no further request can ever arrive.
		return fault.ErrEndOfInput
	case <-ctx.Done():
		// Cancellation means DRAIN, and there is nothing buffered to drain: an
		// un-picked-up handler has not been admitted and is refused, not dropped.
		return ctx.Err()
	}

	for dst.Len() < dst.Cap() {
		select {
		case a := <-s.arrivals:
			if !s.stamp(dst, a) {
				return nil
			}
		default:
			return nil
		}
	}
	return nil
}

// stamp turns one arrival into one record. It returns false when the batch is full.
func (s *Source) stamp(dst *record.Batch, a *arrival) bool {
	r := dst.Add()
	if r == nil {
		// The batch filled between the length check and here. The arrival was already
		// taken off the rendezvous and cannot be put back, so it must be refused.
		a.resolve(verdictShed)
		s.cShed.Add(1, "batch_full")
		return false
	}
	r.EventTime = a.at
	r.Payload = record.BytesPayload(a.body)
	r.SetHandle(a.handle)

	// ---------------------------------------------------------------------------
	// BREAKAGE 1, at the exact line where it bites.
	//
	// What a push source MUST write here and cannot:
	//
	//	r.origin.Key      = a.idem   // the peer's idempotency key
	//	r.origin.Upstream = a.idem   // the vendor's own id, layer 1 of 3
	//
	// Both fields exist on record.Origin, both are documented as source-supplied,
	// and neither has an exported mutator. Reproduced compiler output is in the
	// BREAKAGE 1 block at the bottom of this file.
	//
	// REPAIRED. record.Record.SetKey and SetUpstream are the two lines that belonged
	// here. The idempotency key a retrying push peer supplies is now attachable, so a
	// peer that re-sends the same body with the same key collapses at the destination
	// instead of landing twice — which is the entire reason a push protocol carries one.
	// ---------------------------------------------------------------------------
	r.SetKey(a.idem)
	r.SetUpstream(a.idem)
	_ = r.Meta.Set(record.NSSource, "idempotency_key", record.String(string(a.idem)))
	_ = r.Meta.Set(record.NSSource, "received_at", record.Time(a.at))
	for _, h := range a.hdrs {
		_ = r.Meta.Set(record.NSSource, "header."+h[0], record.String(h[1]))
	}

	s.mu.Lock()
	s.pending[string(a.handle)] = a
	s.mu.Unlock()
	return true
}

// Commit wakes the HTTP handlers whose records have settled.
//
// For this source "commit" means: write 204 to a socket a stranger is holding open.
// The core's refusal to define what Commit means is what makes that legal, and it is
// the single best decision in the source interface.
func (s *Source) Commit(ctx context.Context, a connector.Ack) error {
	s.mu.Lock()
	lane, laneOK := s.lane, s.laneOK
	s.mu.Unlock()
	if !laneOK || a.Lane != lane {
		return nil
	}

	for _, h := range a.Handles {
		key := string(h)
		s.mu.Lock()
		arr, ok := s.pending[key]
		if ok {
			delete(s.pending, key)
		}
		s.mu.Unlock()
		if ok {
			arr.resolve(verdictSettled)
		}
		// !ok is normal: the handler already timed out and forgot it. The record still
		// landed; the peer will send it again; nothing deduplicates it.
	}

	// BREAKAGE 5. Abandoned is a COUNT. When it is non-zero and Handles is empty, the
	// ledger has told us "n records of some group ended terminally" and has silently
	// discarded that group's SUCCESSFUL handles too. Those handlers will hang until
	// their deadline. There is no field on Ack that could tell us which they were, so
	// the best available response is to say so out loud.
	if a.Abandoned > 0 && len(a.Handles) == 0 {
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventDegraded, Lane: a.Lane,
			Severity: fault.PermanentContract,
			Message:  "settled handles were withheld for a partially abandoned group",
			Detail: fmt.Sprintf(
				"%d records covered, %d abandoned, 0 handles returned; that many push handlers will now time out",
				a.Records, a.Abandoned),
		})
	}
	return nil
}

// Nack is how the terminally-failed records get their handlers answered. It is
// documented as a niche capability that "most sources do not want"; for a
// synchronous-ack push source it is MANDATORY, and nothing in the caps cross-check
// says so.
func (s *Source) Nack(ctx context.Context, lane record.LaneID, ns []connector.Nack) error {
	for i := range ns {
		if ns[i].Handle == nil {
			continue
		}
		key := string(ns[i].Handle)
		s.mu.Lock()
		arr, ok := s.pending[key]
		if ok {
			delete(s.pending, key)
		}
		s.mu.Unlock()
		if !ok {
			continue
		}
		// A permanent mapping failure will fail identically on retry, so tell the peer
		// not to bother. Anything else is worth resending.
		if ns[i].Class.Terminal() {
			arr.resolve(verdictRejected)
		} else {
			arr.resolve(verdictShed)
		}
	}
	return nil
}

// Backlog reports how many pushes are outstanding. For a push source this is the only
// meaningful queue depth, and it is exact.
func (s *Source) Backlog(ctx context.Context, lane record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	inPipeline := len(s.pending)
	s.mu.Unlock()
	return connector.Backlog{
		Records: connector.Count(uint64(inPipeline) + uint64(s.waiting.Load())),
		Exact:   true,
		AsOf:    time.Now(),
		// EventTimeLag is deliberately nil, not zero: this source's records are by
		// definition current, so "lag" is not a quantity it has. Zero would read as
		// "caught up", which is a different and unearned claim.
	}, nil
}

// Validate is tier two: it checks the address is bindable-shaped without binding.
func (s *Source) Validate(ctx context.Context) config.Diagnostics {
	var d config.Diagnostics
	if _, _, err := net.SplitHostPort(s.addr); err != nil {
		d = d.Errorf(config.CodeWrongType, []string{"listen"},
			"listen must be host:port, for example \":8080\"", err.Error())
	}
	if len(s.path) == 0 || s.path[0] != '/' {
		d = d.Errorf(config.CodeInvalidPattern, []string{"path"},
			"path must begin with a slash", "for example /ingest")
	}
	if s.ackTimeout < 500*time.Millisecond {
		d = d.Warnf(config.CodeOutOfRange, []string{"ack_timeout"},
			"an ack timeout under 500ms will refuse almost every push",
			"the peer's own retry deadline is the number to match")
	}
	return d
}

// Probe reports the two facts that are actually different: is the port ours, and is
// the pipeline draining us.
func (s *Source) Probe(ctx context.Context) connector.ProbeResults {
	out := connector.ProbeResults{}
	s.mu.Lock()
	running := s.srv != nil
	inPipeline := len(s.pending)
	s.mu.Unlock()

	if running {
		out = append(out, connector.ProbeResult{Label: "listener bound"})
	} else {
		out = append(out, connector.ProbeFailed("listener bound",
			errors.New("no listener; Open has not succeeded"))...)
	}
	if w := s.waiting.Load(); w > int64(s.maxPending)/2 {
		out = append(out, connector.ProbeFailed("ingress not saturated",
			fmt.Errorf("%d handlers waiting for admission (cap %d)", w, s.maxPending))...)
	} else {
		out = append(out, connector.ProbeResult{Label: "ingress not saturated"})
	}
	out = append(out, connector.ProbeResult{
		Label: "in-flight pushes: " + strconv.Itoa(inPipeline)})
	return out
}

// Close stops the listener and answers every socket still being held.
//
// This is the method that makes a push source viable at all: it gets a FRESH context
// carrying the shutdown grace, so the in-flight handlers can be told 503 rather than
// having their connections reset. Getting the cancelled read context here would have
// made a clean shutdown impossible.
func (s *Source) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	srv := s.srv
	s.srv = nil
	pend := s.pending
	s.pending = map[string]*arrival{}
	s.mu.Unlock()

	var err error
	if srv != nil {
		err = srv.Shutdown(ctx)
	}
	// Anything still in flight is unknowable now. Shed it: a 503 makes the peer resend,
	// and resending is correct because we never claimed durability.
	for _, a := range pend {
		a.resolve(verdictShed)
	}
	s.shedAllWaiting()
	if err != nil && !errors.Is(err, context.Canceled) {
		return fault.New(fault.TransientInternal, fault.OpClose, err)
	}
	return nil
}

// shedAllWaiting drains the rendezvous, refusing whatever is on it.
func (s *Source) shedAllWaiting() {
	for {
		select {
		case a := <-s.arrivals:
			a.resolve(verdictShed)
			s.cShed.Add(1, "revoked_or_closing")
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------------

// idempotencyKey is the peer's own id when it gave one, else a content hash. This is
// exactly the "derive a deterministic id from stable fields and document the
// derivation" obligation that SourceCaps.StableKeys + Meta.Notes describe — and it is
// unusable, because there is nowhere to put the result. See BREAKAGE 1.
func idempotencyKey(r *http.Request, body []byte) []byte {
	for _, h := range []string{"Idempotency-Key", "X-Request-Id", "X-Event-Id", "X-Delivery-Id"} {
		if v := r.Header.Get(h); v != "" {
			return []byte(v)
		}
	}
	sum := sha256.Sum256(body)
	return []byte("sha256:" + hex.EncodeToString(sum[:]))
}

// newHandle mints the delivery handle. It must be unique per DELIVERY, not per
// message: two retries of the same message are two handlers to wake, so the handle
// carries a monotonic counter and is deliberately NOT the idempotency key.
func newHandle(idem []byte, at time.Time) []byte {
	n := handleSeq.Add(1)
	h := make([]byte, 0, len(idem)+24)
	h = append(h, idem...)
	h = append(h, '#')
	h = strconv.AppendInt(h, at.UnixNano(), 36)
	h = append(h, '.')
	h = strconv.AppendUint(h, n, 36)
	return h
}

var handleSeq atomic.Uint64

func captureHeaders(r *http.Request) [][2]string {
	keep := []string{"Content-Type", "User-Agent", "X-Signature", "X-Event-Type"}
	out := make([][2]string, 0, len(keep))
	for _, k := range keep {
		if v := r.Header.Get(k); v != "" {
			out = append(out, [2]string{k, v})
		}
	}
	return out
}

// ---------------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------------

// Spec is the config declaration.
func Spec() *config.Spec {
	return config.NewSpec().
		Describe("Inbound HTTP push endpoint",
			"Receives records pushed by a remote peer and answers 2xx only once the record is durable downstream.").
		Field(config.Field{
			Name: "listen", Title: "Listen address", Type: config.TypeString,
			Description: "host:port to bind the ingress listener to.",
			Default:     ":8099", Examples: []any{":8099", "127.0.0.1:9000"},
		}).
		Field(config.Field{
			Name: "path", Title: "Path", Type: config.TypeString,
			Description: "HTTP path the peer posts to.",
			Default:     "/ingest",
		}).
		Field(config.Field{
			Name: "stream", Title: "Stream name", Type: config.TypeString,
			Description: "Logical stream every pushed record is attributed to.",
			Default:     string(record.DefaultStream),
		}).
		Field(config.Field{
			Name: "lane_name", Title: "Lane name", Type: config.TypeString,
			Description: "Stable lane name for this endpoint. BREAKAGE 3: in a multi-replica " +
				"deployment every replica announces this same name, so exactly one replica " +
				"holds the lane and the rest can only refuse traffic.",
			Default: "ingress",
		}).
		Field(config.Field{
			Name: "ack_timeout", Title: "Ack timeout", Type: config.TypeDuration,
			Description: "How long a handler waits for settlement before refusing. Must be " +
				"under the peer's own retry deadline.",
			Default: "4s",
		}).
		Field(config.Field{
			Name: "retry_after", Title: "Retry-After", Type: config.TypeDuration,
			Description: "Value rendered into the Retry-After header on a refusal.",
			Default:     "1s", Advanced: true,
		}).
		Field(config.Field{
			Name: "max_waiting", Title: "Max waiting handlers", Type: config.TypeInt,
			Description: "Refuse with 429 once this many handlers are waiting for admission. " +
				"BREAKAGE 4: this is a second in-flight bound that duplicates the lane budget, " +
				"and it exists only because the core will not report admission headroom.",
			Default: 512, Advanced: true,
		}).
		Example(config.Example{
			Title:  "Webhook receiver",
			Config: map[string]any{"listen": ":8099", "path": "/ingest", "ack_timeout": "4s"},
		})
}

// Caps is the declared capability set.
func Caps() connector.SourceCaps {
	return connector.SourceCaps{
		Caps:            connector.Caps{APIVersion: connector.APIVersion},
		DefaultOrdering: connector.OrderingDiscrete,
		Boundedness:     []connector.Boundedness{connector.Unbounded},
		LaneKinds:       []connector.LaneKind{connector.LaneKindStream},
		MaxLanes:        1,

		// The peer holds each message and resends it for as long as it is unacked, so
		// commit ordering is a latency question and not a correctness one. This is the
		// closest of the four Retention values and it is a good fit.
		UpstreamRetention: connector.RetentionWindow,

		// UnitsExternal is the RIGHT answer — the load balancer assigns work — and it
		// is unimplementable. See BREAKAGE 3.
		UnitAssignment: connector.UnitsExternal,

		Nackable:       true,
		ReportsBacklog: true,
		Validates:      true,
		Probes:         true,

		ProducesEventTime: true,

		// BREAKAGE 1, REPAIRED. This source computes a perfectly good stable key in
		// idempotencyKey() and can now attach it, so declaring StableKeys is honest and the
		// engine's dedupe and SinkCaps.RequiresKey both act on something real.
		StableKeys: true,

		// BREAKAGE 2, REPAIRED. Replayable stays FALSE, because it is true: this source has
		// no cursor and nothing to rewind to. What changed is that false no longer implies
		// at-most-once.
		Replayable: false,

		// RedeliversUnacked is the promise that makes at-least-once truthful here: this source
		// does not answer its peer until the core has settled the records, and the peer
		// re-sends on any other answer. The peer's retry IS the replay mechanism.
		//
		// It is the difference between an ingress that acks on hand-over — the one thing a
		// push protocol must never do — and one that acks on durability. The promise is
		// enforced by ackOne, which blocks on settlement, and by the deadline path, which
		// answers 503 rather than 202 when settlement does not arrive.
		RedeliversUnacked: true,

		ComparablePositions: false,
		MidLaneResume:       false,
		CompleteImages:      false,
	}
}

// Register adds the source to a registry. This is the whole "implement, register,
// done" claim, and for everything except the five breakages it holds: no core file is
// touched, no switch statement is edited, and the core knows nothing about HTTP.
func Register(r *registry.Registry) {
	registry.AddSource(r, registry.SourceDef[*Source]{
		Meta: registry.Meta{
			Name:    Name,
			Version: "0.0.1-stress",
			Title:   "HTTP push (stress)",
			Summary: "Hostile connector: inbound push with a synchronous ack and a 5s peer deadline.",
			Notes: "Origin.Key is the peer's Idempotency-Key or X-Request-Id header, falling " +
				"back to sha256 of the request body when the peer supplies neither. It is stable " +
				"across re-sends because a retrying peer reuses its own key, which is the entire " +
				"purpose of the header; the sha256 fallback is stable because it is a pure " +
				"function of the bytes. Origin.Upstream carries the same value, since for a push " +
				"protocol the peer's id IS the vendor id. Historical note, kept as the " +
				"derivation's provenance: the key was formerly only reachable as " +
				"Meta[source/idempotency_key], which no sink and no dedupe path reads.",
			Support: registry.SupportCommunity,
		},
		Spec: Spec(),
		Caps: Caps(),
		New:  New,
	})
}

/*
=====================================================================================
BREAKAGE 1 — FATAL — record.Origin.Key and record.Origin.Upstream are unsettable
=====================================================================================

BLOCKING SIGNATURE

	// pkg/record/record.go
	type Record struct {
	    origin Origin      // unexported, no exported mutator
	    ...
	}
	func (r *Record) Origin() Origin  { return r.origin }   // returns a COPY
	func (r *Record) SetHandle(h []byte)                     // the only identity setter

	// pkg/record/origin.go
	type Origin struct {
	    Key      []byte   // "the source-derived stable identity"
	    Upstream []byte   // "the vendor's own id ... layer 1 of the three idempotency layers"
	    ...
	}

REPRODUCED COMPILER OUTPUT. This is not a theory. I wrote the two lines an author
would actually write, ran `go build ./internal/stress/push-source/`, and got:

	# github.com/BernardoCSACarreira/canal/internal/stress/push-source
	proof_breakage1.go:6:4: r.origin undefined (type *record.Record has no field or
	    method origin, but does have method Origin)
	proof_breakage1.go:7:4: r.SetKey undefined (type *record.Record has no field or
	    method SetKey)

The third shape an author reaches for is worse, because it COMPILES:

	o := r.Origin()
	o.Key = a.idem       // mutates a copy. Silently does nothing. No diagnostic.

That path is pinned by TestOriginKeyIsUnreachable in connector_test.go, which asserts
Origin().Key, Origin().Upstream and Ref().Key are all nil after a push that carried
`Idempotency-Key: peer-supplied-1234`, and that assigning through the Origin copy does
not reach the record. A connector author who writes those two lines and does not write
that test ships a source they believe has stable keys and does not.

WHY IT IS FATAL RATHER THAN ANNOYING

This is not a missing convenience. It is the difference between correct and incorrect
for this entire class of source.

A push peer with a 5s deadline WILL resend. Not occasionally — on every GC pause, every
sink hiccup, every rolling restart, every time the settle deadline fires in
serveHTTP above. Duplicate delivery is the STEADY STATE of a push source, not an edge
case. The design has a complete, well-argued answer to it: Origin.Key feeds the
engine's dedupe, feeds Request.IdempotencyKey, and feeds a sink's upsert. Three layers,
documented in Origin.Upstream's own comment. A push source can reach NONE of them.

It gets worse in two directions:

 1. SinkCaps.RequiresKey is refused against a source that does not declare StableKeys
    (internal/engine/negotiate.go:125). So this source cannot be connected to any
    upsert-shaped sink AT ALL — which is the only sink shape that would have absorbed
    the duplicates it is guaranteed to produce. The one mitigation is structurally
    denied to the one source that needs it most.

 2. The obligation is asserted in the wrong place. AddSource panics if StableKeys is
    declared with empty Notes, so the interface is confident enough about
    source-supplied keys to POLICE the documentation of the derivation — while
    providing no way to perform it. I wrote idempotencyKey() to the letter of design
    rule R5 and then had nowhere to put the return value.

Note that this is NOT the Kafka-Connect KIP-793 problem the Origin comment is defending
against. That problem is a TRANSFORM corrupting settlement identity after the fact. The
producing source stamping its own record before Read returns is a different actor at a
different time, and SetHandle already establishes the precedent for exactly that window
("legal only from the source that produced the record, before it returns from Read").

SMALLEST FIX — additive, breaks nothing

	// pkg/record/record.go, alongside SetHandle, with the same window rule.
	//
	// SetIdentity attaches the source-derived stable key and the vendor's own id.
	// Legal only from the source that produced the record, before it returns from
	// Read; the engine rejects a later write exactly as it does for SetHandle.
	func (r *Record) SetIdentity(key, upstream []byte) {
	    r.origin.Key, r.origin.Upstream = key, upstream
	}

Two methods (SetKey/SetUpstream) would also do. Either way:

  - No existing connector breaks: nothing today calls a method that does not exist.
  - No transform gains power: the method is on Record, and the engine's existing
    "handle set after Read" rejection is the same enforcement point.
  - Batch.Derive already copies origin wholesale, so derived records inherit the key
    with zero further work.
  - The alternative — a Key field on a per-record struct passed to Add — would change
    Batch.Add's signature and IS a breaking change. Prefer the method.

=====================================================================================
BREAKAGE 2 — FATAL — Replayable=false forces AtMostOnce, which acks before durability
=====================================================================================

BLOCKING SIGNATURE

	// pkg/connector/caps.go
	// Replayable means the source can re-read from a committed position. False means a
	// lost in-flight window is lost data, and the core refuses AtLeastOnce.
	Replayable bool `json:"replayable"`

	// internal/engine/negotiate.go:53
	if !c.Replayable {
	    effective = effective.Min(connector.AtMostOnce)
	    ...
	}

	// pkg/connector/guarantee.go
	// AtMostOnce settles on hand-over.

THE ARGUMENT

Replayable asks "can canal seek backwards?". For a push source the answer is a flat no
and always will be: there is no position, no cursor, nothing to seek. So the truthful
declaration is false — which is what Caps() above declares.

The consequence is that the negotiator pins the pipeline to AtMostOnce, and AtMostOnce
"settles on hand-over". Settling on hand-over means the group resolves as soon as the
source's batch is admitted, which means emitDiscrete fires immediately, which means
Source.Commit is called with the handles before the sink has written anything, which
means serveHTTP writes 204 for data that is not durable.

That is R4's original violation, restated in architecture.md §18.2 as the one thing
that must never happen: "The one thing that never happens is a 202 for data that is not
yet durable." Declaring the truth about this source produces exactly that outcome, and
the operator is never warned, because from the negotiator's point of view everything is
consistent.

The category error is that Replayable conflates two different properties:

	A. canal can re-read from a committed position.        (seek)
	B. anything canal does not acknowledge WILL be redelivered.  (redelivery)

At-least-once needs A OR B. A pull source has A. A push source with a synchronous ack
has B, and B is arguably the stronger property — the peer's retry loop is a real,
externally-durable replay mechanism, whereas A depends on an upstream retention window
that ReplayWindow already admits may be unknown.

Only A is representable. Note that UpstreamRetention: RetentionWindow already states B
in prose ("the upstream keeps data for a bounded time regardless") — the fact is
already in the caps struct, just not in the field the negotiator reads.

SMALLEST FIX — additive, breaks nothing

Option 1 (preferred, one field + one condition):

	// pkg/connector/caps.go
	// RedeliversUnacked means the UPSTREAM resends anything this source does not
	// acknowledge, so a lost in-flight window is re-delivered rather than lost. It is
	// the push-shaped alternative to Replayable: at-least-once requires either.
	RedeliversUnacked bool `json:"redelivers_unacked"`

	// internal/engine/negotiate.go:53
	if !c.Replayable && !c.RedeliversUnacked {

Option 2 (zero new fields): treat OrderingDiscrete lanes as satisfying the same
condition, since handle-based settlement means an unsettled handle is by construction
never acknowledged. Cheaper, but it couples a guarantee to an ordering mode, which is
the kind of implicit coupling the rest of this design is careful to avoid. Prefer
option 1.

Either way no already-written connector changes: a source that declares Replayable
today keeps its exact behaviour, and the new field defaults to false.

=====================================================================================
BREAKAGE 3 — MAJOR — no instance identity, so UnitsExternal is unimplementable
=====================================================================================

BLOCKING SIGNATURE

	// pkg/connector/runtime.go
	type SourceRuntime interface {
	    ...
	    Tenant() record.TenantID
	    Pipeline() record.PipelineID
	    Node() record.NodeID
	    // and nothing else identity-shaped
	}

	// pkg/connector/lane.go
	// Name ... record.LaneID is derived from (tenant, pipeline, node, Name)
	// It MUST be derived from stable content properties, not from an ephemeral handle.

	// pkg/connector/caps.go
	// UnitsExternal means units exist but SOMEONE ELSE assigns them. canal announces one
	// lane per source instance and does not attempt to place work.

THE ARGUMENT

Deploy this at "enterprise scale, horizontal, k8s, multi-worker". Three replicas sit
behind one Service. The load balancer decides which replica receives a given POST.
canal's planner has no say and cannot have one.

Each replica calls Open, each announces a lane. LaneID = f(tenant, pipeline, node,
Name), and Name is the same string in all three (it comes from config; there is nothing
else it could come from). So all three derive the SAME LaneID, and the lease machinery
correctly grants it to exactly ONE replica. The other two bind their port, call
Lanes().Revoked(lane) == true, and can do nothing but 503 every request the load
balancer sends them. Two thirds of the fleet is a black hole.

There is no escape inside the current interface:

  - rt.Node() is the pipeline GRAPH vertex id, identical on every replica by design.
  - There is no rt.Worker() / rt.Instance(). store.WorkerID exists in the core
    (pkg/store/coordinator.go:11) and internal/engine/build.go:31 already holds one —
    the value exists, it is simply not on the runtime.
  - Minting a name from os.Hostname() or a UUID directly violates LaneSpec.Name's
    stated contract ("not from an ephemeral handle") and would accrete a dead,
    unfinishable lane row per pod restart forever: the lane is Unbounded, so there is
    no EndOfLane, and Finish() can only be called by the worker that no longer exists.

UnitsExternal's own comment says "canal announces one lane per source instance", which
is the exactly correct design — but Announce is called BY THE SOURCE with a name the
SOURCE picks, so the sentence describes a behaviour no source can produce.

SMALLEST FIX — additive, breaks nothing

	// pkg/connector/runtime.go, on SourceRuntime (core-implemented, so growing it is free)
	//
	// Instance identifies this worker process. It is stable for the process's lifetime
	// and is the only legal ingredient of a LaneSpec.Name for a UnitsExternal source,
	// whose lanes are per-instance by construction.
	Instance() record.InstanceID

plus: for UnitsExternal sources the core garbage-collects an instance lane when its
lease expires and no worker reclaims it, so pod churn does not accrete lane rows.

Better still, and still additive: for UnitsExternal, have the CORE pre-announce the
single instance lane and let the source get it from Lanes().Assigned() without calling
Announce at all. That makes the cap's own sentence literally true and removes the
naming decision from the connector entirely. Nothing written today is affected, because
nothing today declares UnitsExternal.

=====================================================================================
BREAKAGE 4 — MAJOR — admission pressure is unobservable, so a prompt 429 is impossible
=====================================================================================

BLOCKING SIGNATURE

	// pkg/connector/source.go
	Read(ctx context.Context, dst *record.Batch) error

	// pkg/connector/lanectl.go
	// Budget reports the current in-flight allowance in records. It is informational.
	Budget(id record.LaneID) int

THE ARGUMENT

architecture.md §18.2 states the requirement precisely and then does not deliver it:

	"A push source (an HTTP ingress, a socket receiver) must be able to refuse, and the
	 refusal is a typed fault, not a dropped connection. Ledger.Admit returns
	 fault.New(TransientInternal, OpBuffer, …) with RetryAfter set … The built-in HTTP
	 source maps that to 503 plus a Retry-After header."

Ledger.Admit is called by the ENGINE, after Read has returned. Its error goes to the
engine. Read's signature has no out-parameter for it, SourceRuntime has no callback for
it, and there is no Admit-shaped method anywhere on the connector surface. The sentence
describes a data flow that does not exist: the source can never be "handed the returned
fault", because it is not on the call path.

What a push source can actually observe about backpressure: that Read is not called
again. That is it. Which means the only refusal it can construct is a TIMEOUT — see
serveHTTP's `handoff` timer above. The peer's entire 5-second budget is consumed
before it is told to retry, when the correct answer was available at millisecond zero.
Under sustained saturation every single request costs 5s of held connection, and a
webhook sender's connection pool exhausts long before its retry queue does. Load
shedding that takes as long as success is not load shedding.

MEASURED, in TestNoPromptRefusal:

	BREAKAGE 4: refusal took 601.657708ms; the answer was knowable at t=0

with ack_timeout set to 600ms. The refusal is correct — 503 with a Retry-After — and
it arrives one full deadline late, every time, because a timeout is the only signal
the interface offers.

LaneCtl.Budget does not close the gap:

  - Its documented meaning is the "allowance", and in the ledger the only matching
    field is laneState.budget, the CONFIGURED cap (LaneStats separates
    InFlightBudget=configured from InFlight=actual, so the distinction is understood
    in core and simply not exposed on LaneCtl).
  - Even read as remaining headroom, it is a polled int with no edge signal, so it
    cannot be selected on and races with every concurrent handler.
  - It is per-lane and this source has one lane, so it cannot even be used as a
    per-stream hint.

So the connector is forced to build its own bound — `max_waiting` in Spec() above —
which is precisely the second in-flight accounting concept that §18.1 point 3 exists to
prevent ("It replaces the five overlapping mechanisms Benthos accumulated … with one
operator-set number"). The operator now has two numbers that must be tuned against each
other, and only one of them appears in the read model.

SMALLEST FIX — additive, breaks nothing

LaneCtl is INJECTED, not implemented by connectors, so adding a method to it cannot
break a single connector:

	// pkg/connector/lanectl.go
	//
	// Headroom reports how many more records may be admitted for this lane before
	// admission blocks, and whether it is blocked right now. A push source refuses
	// promptly on blocked; a pull source ignores it.
	//
	// Ready is closed and replaced when a blocked lane regains headroom, so a push
	// source can wait on it instead of polling.
	Headroom(id record.LaneID) (records int, blocked bool)
	Ready(id record.LaneID) <-chan struct{}

Headroom alone fixes the prompt-429 case (two lines in serveHTTP). Ready additionally
lets a handler park politely inside its deadline rather than spin, and matches the
Changes() idiom already on LaneCtl. Delete max_waiting from the spec when it lands.

=====================================================================================
BREAKAGE 5 — MINOR — Ack.Abandoned is a count while Ack.Handles is a list
=====================================================================================

BLOCKING SIGNATURE

	// pkg/connector/source.go
	type Ack struct {
	    Handles   [][]byte `json:"handles,omitempty"`   // a LIST
	    Abandoned uint64   `json:"abandoned"`           // a COUNT
	}

	// internal/ledger/ledger.go:333
	if st.ordering == connector.OrderingDiscrete && g.abandoned == 0 {
	    st.settledHandles = append(st.settledHandles, g.handles...)
	}

THE ARGUMENT

Take a batch of ten pushes. Nine land; one is unencodable for the destination and is
dead-lettered. The group settles with abandoned == 1, so the guard above appends NOTHING
and all ten handles are discarded. The Ack that eventually arrives carries
Records: 10, Abandoned: 1, Handles: nil.

The one abandoned record does come back through Nackable with its handle, so it is
answerable. The nine that SUCCEEDED are in neither Ack.Handles nor Nack. Their handlers
sit in serveHTTP's `settle` select until the deadline, return 503, and the peer resends
nine records that are already durable at the sink — with no Origin.Key to deduplicate
them (BREAKAGE 1 compounds this). One poison record in a batch of ten produces nine
duplicates, per batch, indefinitely.

The ledger line is arguably a plain bug and could be fixed there alone (append the
handles of the records that were NOT abandoned). But the interface is complicit: with
Abandoned as a scalar there is no way for a source to learn WHICH deliveries failed
except by also implementing Nackable — and Nackable's own doc-comment says "Most
sources do not want it", nothing in AddSource's cross-check requires it for a discrete
source, and a connector author following the documentation will not implement it. The
first author of a queue-shaped source will hit this, and the symptom (a slow trickle of
duplicates under partial failure) is close to undiagnosable from the outside.

SMALLEST FIX — additive, breaks nothing

Ack is a plain serialisable struct with no methods, so a new field breaks no connector
and no wire form:

	// AbandonedHandles are the discrete-lane deliveries that reached a terminal
	// disposition. Handles and AbandonedHandles together account for every delivery
	// this ack covers, so a source needs no second interface to close the loop.
	AbandonedHandles [][]byte `json:"abandoned_handles,omitempty"`

and in the ledger, partition the group's handles by disposition instead of dropping
them all. Additionally: make AddSource's cross-check require Nackable when
DefaultOrdering is OrderingDiscrete, or soften Nackable's "most sources do not want it"
to say that a discrete source does.

=====================================================================================
BREAKAGE 6 — documentation, not interface — connector-owned goroutines
=====================================================================================

architecture.md line 5820 dismisses the risk of "connector-spawned goroutines with no
bound, no cancellation handle and no error path" as:

	"Moot. No plugin interface hands out a goroutine spawner, and no connector-owned
	 goroutine is sanctioned. Read blocks and Announce is synchronous."

It is not moot for any push source, and push sources are a named goal of this project
(architecture.md:715 and :1628 both cite a webhook as a first-class case). This file
spawns one goroutine per http.Server plus one per connection, and there is no
alternative: an inbound listener cannot be driven from inside Read.

The mechanics do work out, and they work out because of two good decisions:
rt.Context() gives the server a component-lifetime context, and Close gets a fresh
context with the shutdown grace. The one thing I had to invent is the serveErr channel
in Read's select — without it a dead listener leaves Read blocked forever on arrivals
and the engine never learns anything is wrong.

No interface change is needed. The risk register entry should stop saying "Moot" and
instead state the obligation: a source that owns a goroutine must route its terminal
error into Read's select and must stop it in Close. That is a documentable rule, and it
belongs in the connector-author guide next to the Read cancellation rule, which is the
other thing nobody gets right by accident.

=====================================================================================
WHAT I EXPECTED TO BREAK AND DID NOT — recorded so nobody "fixes" these
=====================================================================================

 1. No position. A discrete lane genuinely needs no Position, Read genuinely may leave
    dst.Position zero, and StateHandle is genuinely untouched. Three interfaces that
    could each have forced a fake cursor on me and none did.

 2. Commit's meaning. "WHAT THIS MEANS IS ENTIRELY THE SOURCE'S DECISION" turns out to
    cover "write 204 to a socket a stranger is holding open". The core's refusal to
    define Commit is what makes a synchronous-ack push source possible at all.

 3. Discoverer. A webhook has no catalog, and not implementing the interface is the
    documented answer. Correct.

 4. Boundedness / LaneKind. Unbounded + stream, no phase enum, no pipeline.type field
    to lie to. Nothing needed.

 5. Retention. RetentionWindow describes a retrying peer accurately. Four values and
    the right one was there.

 6. Close-after-failed-Open. Called exactly once, always, even if Open never ran —
    which is exactly what a connector holding a listener needs, and it is stated on the
    method.

 7. Read's drain-not-abort cancellation rule. Fiddly, but stated so explicitly that I
    implemented it correctly on the first attempt.

 8. Metrics. Core-owned names and a fixed label set were no obstacle; five series
    covered everything this source has to say.

 9. Registration. AddSource accepted this connector with no panic and no descriptor
    warnings on the first attempt: the caps cross-check, the spec lint and the example
    validation all passed. The declare-and-cross-check machinery is not in the way.

10. SourceRuntime as an interface. connector_test.go fakes the whole thing in about
    sixty lines, including a LaneCtl. The doc-comment's claim that this is why it is an
    interface and not a struct is correct, and it is what made every assertion in that
    file possible.

VERDICT: fits-awkwardly on the mechanics, requires-core-change on correctness.

The shape of the source interface is right for a push source — I did not have to fake a
cursor, fake a position, invent a poll loop, or lie about boundedness, and the
handle/Ack/Nack triangle is a better fit than anything in the surveyed field. But
BREAKAGE 1 and BREAKAGE 2 are not ergonomics: with them unfixed, this connector is
guaranteed to emit duplicates it computed the key for and cannot attach, at a guarantee
tier that acks the peer before the data is durable. Both fixes are additive and neither
touches a frozen interface.
=====================================================================================
*/
