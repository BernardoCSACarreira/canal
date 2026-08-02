package engine

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// Deps is the deployment assembly: the four store interfaces plus the process's own identity and
// logging.
//
// Swapping these four is the ENTIRE difference between a laptop and a cluster. The connector-facing API
// is byte-identical in both modes, and a test builds the same spec against both assemblies and asserts
// identical negotiation output.
type Deps struct {
	Config      store.ConfigStore
	State       store.StateStore
	Coordinator store.Coordinator
	Status      store.StatusStore

	Worker store.WorkerID
	Log    *slog.Logger

	// Metrics is where this process accumulates its series. A nil registry instruments nothing and
	// is safe on every path — see obs — which is what lets a test build a pipeline without one.
	Metrics *metrics.Registry

	// Version is this canal build's version, recorded in every checkpoint header so an operator can see
	// what wrote the state they are looking at.
	Version string

	// FlushInterval and FlushRecords bound how far phase three lags phase one. Phase three CANNOT run
	// before phase two's write is flushed, so a pruning upstream's retention release is delayed by up to
	// one interval. That cost is named, bounded and tunable rather than hidden.
	FlushInterval time.Duration
	FlushRecords  int

	// GracePeriod is how long Close and the final drain are given. A drain that does not complete inside
	// it is reported as a DRAIN-TIMEOUT, which is a different event from a completed drain because it
	// means records may replay.
	GracePeriod time.Duration

	// ControlInterval is the cadence of each source's control goroutine: how often a quiet lane is
	// heartbeated, a backlog re-polled and queued nacks delivered. It is ALSO the idleness threshold,
	// because one number is enough — a lane that produced nothing since the last tick is quiet, and
	// the heartbeat carries the real elapsed duration rather than the interval.
	//
	// It is not FlushInterval and must not be derived from it. That one bounds how much data replays
	// after a crash; this one bounds how long an upstream pins its own retention with nothing to
	// acknowledge. The default is deliberately short of the ten-second-ish window a logical-decoding
	// upstream expects before it considers a subscriber gone.
	ControlInterval time.Duration

	// ConfigInterval is how often the config watch re-reads the stored revision, and it is a THIRD
	// cadence rather than a reuse of either above because it is the only one that is not on a data
	// path. Nothing waits for it: it bounds how stale the answer to "did my config take effect" may
	// be, and its cost is one config-store read per pipeline per tick. Borrowing ControlInterval's
	// five seconds would put a deployment of four hundred pipelines at eighty config reads a second
	// to answer a question nobody asks that often.
	//
	// It is the reconcile timer store.ConfigStore.Watch names when it says a watch is a convenience
	// and never a correctness dependency, so it applies whether or not the store offers one.
	ConfigInterval time.Duration

	// StatusInterval is how often this worker publishes its read model to store.StatusStore, and it
	// is a FOURTH cadence because it is the one an aggregator's staleness threshold is a function of:
	// PipelineStatus.StaleAfterSeconds answers "past what age does a worker's last report stop
	// counting", and that number is meaningless unless somebody knows how often reports arrive.
	//
	// Nothing waits for it and nothing on the data path is affected by it — a report that fails is
	// dropped rather than retried, because the next one carries a fresher document than the one that
	// failed.
	StatusInterval time.Duration
}

// withDefaults fills the values an operator did not set, and records where each came from so the
// negotiation can label them (design rule R10: scaffolding is labelled as such).
func (d Deps) withDefaults() (Deps, []telemetry.DefaultNote) {
	var notes []telemetry.DefaultNote
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Worker == "" {
		d.Worker = "single"
		notes = append(notes, telemetry.DefaultNote{Path: []string{"worker"}, Value: "single", From: "core default"})
	}
	if d.FlushInterval <= 0 {
		d.FlushInterval = time.Second
		notes = append(notes, telemetry.DefaultNote{Path: []string{"state", "flush_interval"}, Value: "1s", From: "core default"})
	}
	if d.FlushRecords <= 0 {
		d.FlushRecords = 10000
		notes = append(notes, telemetry.DefaultNote{Path: []string{"state", "flush_records"}, Value: 10000, From: "core default"})
	}
	if d.GracePeriod <= 0 {
		d.GracePeriod = 30 * time.Second
		notes = append(notes, telemetry.DefaultNote{Path: []string{"shutdown", "grace_period"}, Value: "30s", From: "core default"})
	}
	if d.ControlInterval <= 0 {
		d.ControlInterval = 5 * time.Second
		notes = append(notes, telemetry.DefaultNote{Path: []string{"source", "control_interval"}, Value: "5s", From: "core default"})
	}
	if d.ConfigInterval <= 0 {
		d.ConfigInterval = 30 * time.Second
		notes = append(notes, telemetry.DefaultNote{Path: []string{"config", "config_interval"}, Value: "30s", From: "core default"})
	}
	if d.StatusInterval <= 0 {
		d.StatusInterval = 5 * time.Second
		notes = append(notes, telemetry.DefaultNote{Path: []string{"status", "status_interval"}, Value: "5s", From: "core default"})
	}
	return d, notes
}

// Pipeline is a built, not-yet-running pipeline.
//
// Build resolves and validates; Run executes. Separating them is what lets the submit screen show a
// negotiated guarantee before anything has connected to anything.
type Pipeline struct {
	spec spec.Spec
	deps Deps

	sources map[record.NodeID]*registry.ResolvedSource
	sinks   map[record.NodeID]*registry.ResolvedSink

	// codecs holds the encode/frame/compress chain of every byte sink, resolved at build time. A
	// structured sink maps to nil.
	codecs map[record.NodeID]*codecChain

	// configs is each node's VALIDATED config, kept so the runtime can hand it back through
	// connector.Runtime.Config and so the engine can read the stage-standard fields.
	//
	// Build validated it, defaulted it and handed it to New, and then dropped it — which left
	// baseRuntime.cfg nil for every connector in existence, so Runtime.Config returned (nil, nil) to
	// anyone who asked. It also left the per-node when_full offered on every config form with no
	// possible reader.
	configs map[record.NodeID]*config.Config

	ledger *ledger.Ledger

	// obs is this pipeline's instrument set, registered once at build.
	obs *obs

	// negotiated is kept so the read model can serve what the operator GOT rather than what they asked
	// for, for the pipeline's whole life.
	negotiated telemetry.Negotiated

	// active is the current or MOST RECENT execution, and it is deliberately never cleared. A bounded
	// pipeline that completed still has to be able to say what it did; clearing the pointer at the end
	// of Run would make a finished pipeline report the same empty document as one that never started.
	active atomic.Pointer[runner]

	// inflight counts the plugin calls the core has outstanding on each node, so that Close never
	// enters a component that a call sandbox abandoned is still inside. See sandbox.go.
	inflight inflight

	// config is the last observation of the control plane's stored revision, or nil when this worker
	// has no store.ConfigStore or has not looked yet. See config.go.
	//
	// It lives on the Pipeline rather than the runner because Status does, and a document read after
	// Run returned still has to report the last generation this process knew about.
	config atomic.Pointer[configView]

	// version is the read model's monotonic revision, and it is both the ETag and the SSE cursor.
	//
	// It counts MATERIALISATIONS rather than changes, and that is honest rather than lazy: the
	// document carries live ages, so it genuinely differs on every read and a content hash would
	// change just as often. The number is a cursor a consumer can order and resume from; it is not a
	// cache key, and nothing here pretends otherwise.
	version atomic.Uint64
}

// Negotiated returns the resolved, honest delivery contract.
func (p *Pipeline) Negotiated() telemetry.Negotiated { return p.negotiated }

// Spec returns the specification this pipeline was built from.
func (p *Pipeline) Spec() spec.Spec { return p.spec }

// Build assembles a pipeline.
//
// Capability negotiation happens here, as a PURE FUNCTION OF CONFIG, before anything starts. Nothing in
// Build does I/O except constructing components — whose New is contractually forbidden from doing any —
// and the optional connector.Validator tier, which is called separately and whose absence never blocks a
// build.
//
// An impossible pipeline is REFUSED HERE, at submit time, with per-field diagnostics. Every diagnostic is
// returned together: a form that surfaces one error at a time is a form operators fight.
//
// On an error the returned pipeline is nil and every component Build constructed has already been
// closed — Close is called exactly once, always, including after a failed Open and including when Open
// was never called at all.
func Build(ctx context.Context, r *registry.Registry, s spec.Spec, d Deps) (*Pipeline, telemetry.Negotiated, config.Diagnostics) {
	var diags config.Diagnostics
	deps, defaults := d.withDefaults()

	if r == nil {
		diags = diags.Errorf(config.CodeUnknownComponent, nil, "no component registry was supplied", "")
		return nil, telemetry.Negotiated{}, diags
	}
	if deps.State == nil {
		diags = diags.Errorf(config.CodeGuarantee, nil,
			"no state store was supplied, so no delivery guarantee can be honoured",
			"canal cannot promise durability without a durable place to record progress")
		return nil, telemetry.Negotiated{}, diags
	}

	diags = validateGraph(r, &s, diags)

	res := resolved{
		sources: map[record.NodeID]*registry.ResolvedSource{},
		sinks:   map[record.NodeID]*registry.ResolvedSink{},
		buffers: map[record.NodeID]connector.BufferCaps{},
		codecs:  map[record.NodeID]*codecChain{},
		configs: map[record.NodeID]*config.Config{},
	}
	var built []closer

	fail := func() (*Pipeline, telemetry.Negotiated, config.Diagnostics) {
		closeAll(ctx, deps, built, nil)
		return nil, telemetry.Negotiated{}, diags
	}

	for i := range s.Graph {
		n := &s.Graph[i]
		switch n.Kind {
		case registry.KindSource:
			e, ok := r.Source(n.Name)
			if !ok {
				continue // already diagnosed by validateGraph
			}
			cfg, nd := e.Spec.Validate(n.Config)
			diags = anchor(diags, n.ID, nd)
			if nd.HasErrors() {
				continue
			}
			src, err := e.New(ctx, cfg)
			if err != nil {
				diags = append(diags, nodeDiag(n.ID, config.CodeCustom,
					fmt.Sprintf("source %q could not be constructed: %v", n.Name, err), ""))
				continue
			}
			built = append(built, closer{id: n.ID, close: src.Close})
			rs, err := registry.ResolveSource(n.Name, src, e.Caps)
			if err != nil {
				diags = append(diags, nodeDiag(n.ID, config.CodeCapability, err.Error(), ""))
				continue
			}
			res.sources[n.ID] = rs
			res.configs[n.ID] = cfg

		case registry.KindSink:
			e, ok := r.Sink(n.Name)
			if !ok {
				continue
			}
			cfg, nd := e.Spec.Validate(n.Config)
			diags = anchor(diags, n.ID, nd)
			if nd.HasErrors() {
				continue
			}
			sk, err := e.New(ctx, cfg)
			if err != nil {
				diags = append(diags, nodeDiag(n.ID, config.CodeCustom,
					fmt.Sprintf("sink %q could not be constructed: %v", n.Name, err), ""))
				continue
			}
			built = append(built, closer{id: n.ID, close: sk.Close})
			rk, err := registry.ResolveSink(n.Name, sk, e.Caps)
			if err != nil {
				diags = append(diags, nodeDiag(n.ID, config.CodeCapability, err.Error(), ""))
				continue
			}
			res.sinks[n.ID] = rk
			res.configs[n.ID] = cfg

			// A codec node attached to a structured sink would be a double encoding, so the field is not
			// even offered. Its presence in the config therefore means the operator edited around the
			// spec, which is a diagnostic rather than a silent double-encode.
			if rk.Caps.Structured && cfg.Has(config.FieldCodec) {
				diags = append(diags, nodeDiag(n.ID, config.CodeCapability,
					fmt.Sprintf("sink %q accepts structured records, so it must not be given a codec", n.Name),
					"remove the codec block; a structured sink is handed records, not bytes"))
			}

			chain, made, notes, cd := resolveCodec(ctx, r, n.ID, cfg, rk)
			built = append(built, made...)
			defaults = append(defaults, notes...)
			diags = append(diags, cd...)
			if cd.HasErrors() {
				continue
			}
			res.codecs[n.ID] = chain

		case registry.KindBuffer:
			e, ok := r.Buffer(n.Name)
			if !ok {
				continue
			}
			if _, nd := e.Spec.Validate(n.Config); nd.HasErrors() {
				diags = anchor(diags, n.ID, nd)
				continue
			}
			res.buffers[n.ID] = e.Caps

		case registry.KindTransform:
			e, ok := r.Transform(n.Name)
			if !ok {
				continue
			}
			if _, nd := e.Spec.Validate(n.Config); nd.HasErrors() {
				diags = anchor(diags, n.ID, nd)
			}

		default:
			// A kind this loop does not handle must be REFUSED, not skipped.
			//
			// Without this arm the switch silently ignored five of the registry's nine kinds. An
			// encoder, decoder, framer, deframer or compressor placed in the graph passed
			// validateGraph — the component is registered, so `r.Has` is satisfied — and then fell
			// through here, so its config was never validated and it was never constructed. The
			// pipeline built, negotiated a delivery tier and reported success for a graph containing
			// a node the engine had quietly dropped.
			//
			// Codecs are not graph nodes: ADR 0022 makes them stage-standard FIELDS on the node that
			// needs them, which is why nothing here resolves one. That is a deliberate design
			// choice, so saying it out loud is the whole fix.
			//
			// The arm is also the guard against the next kind added to registry.Kind without a case
			// here. Silently dropping a node is the worst available behaviour; this makes it a
			// diagnostic instead.
			diags = append(diags, nodeDiag(n.ID, config.CodeGraphInvalid,
				fmt.Sprintf("a %s cannot be a graph node", n.Kind),
				"codecs are configured as fields on the node that needs them, not as nodes of their own; see ADR 0022"))
		}
	}

	if len(res.sources) == 0 {
		diags = diags.Errorf(config.CodeGraphInvalid, nil, "the pipeline has no usable source node", "")
	}
	if len(res.sinks) == 0 {
		diags = diags.Errorf(config.CodeGraphInvalid, nil, "the pipeline has no usable sink node", "")
	}
	if diags.HasErrors() {
		return fail()
	}

	neg, diags := negotiate(&s, res, deps.State.Capabilities(), diags)
	neg.Defaults = append(neg.Defaults, defaults...)
	if diags.HasErrors() {
		return fail()
	}

	// A CONTRACT THIS BUILD CANNOT EXECUTE IS A WARNING HERE AND A REFUSAL AT RUN. See [Executable]
	// for the rule and for why it is not an error at this point.
	if err := Executable(neg); err != nil {
		diags = diags.Warnf(config.CodeGuarantee, nil, err.Error(),
			"the pipeline will be refused when it is run; use a sink that is durable on Write, or lower the requested guarantee")
	}

	// The wire format each sink will produce is disclosed alongside the guarantee, because it is a
	// thing the operator GOT rather than asked for: an unnamed encoder defaults to json, which
	// base64-encodes a byte payload. Sorted, because iterating a map here is how DurabilityEdge
	// once became nondeterministic.
	for _, id := range slices.Sorted(maps.Keys(res.codecs)) {
		neg.Why = append(neg.Why, fmt.Sprintf("sink %s writes %s", id, res.codecs[id].describe()))
	}

	budget := s.LaneBudget
	if budget <= 0 {
		budget = defaultLaneBudget
		neg.Defaults = append(neg.Defaults,
			telemetry.DefaultNote{Path: []string{config.FieldLaneBudget}, Value: budget, From: "core default"})
		neg.ReplayBudget = budget
	}
	// The EFFECTIVE budget is written back, so everything downstream reads one number instead of
	// re-deriving the default. The flush trigger got this wrong by reading the spec's zero and
	// capping at FlushRecords=10000, which a budget of 1000 makes unreachable — so the trigger never
	// fired and a Flusher pipeline ran at budget-per-tick until its drain timed out. p.Spec() now
	// reports what is in force rather than what was submitted, which is the more useful answer and
	// is already disclosed as a default note.
	s.LaneBudget = budget

	ob, err := newObs(deps.Metrics, s)
	if err != nil {
		// Every name and label in newObs is a constant from telemetry's closed sets, so a failure
		// here means one of those sets was edited without this engine. That is a build-time mistake
		// and refusing is the only way it gets noticed.
		diags = append(diags, config.Diagnostic{
			Severity: config.SeverityError, Code: config.CodeCustom,
			Message: "the engine could not register its own metrics: " + err.Error(),
		})
		return fail()
	}

	p := &Pipeline{
		spec:    s,
		deps:    deps,
		obs:     ob,
		sources: res.sources,
		sinks:   res.sinks,
		codecs:  res.codecs,
		configs: res.configs,
		ledger: ledger.New(ledger.Config{
			Tenant:        s.Tenant,
			Pipeline:      s.ID,
			DefaultBudget: budget,
			GroupTTL:      10 * time.Minute,
		}),
		negotiated: neg,
	}
	return p, neg, diags
}

// defaultLaneBudget is the in-flight bound a spec that names none gets.
const defaultLaneBudget = 1000

// Executable reports whether THIS BUILD can honour a negotiated contract.
//
// WHY THIS EXISTS. Negotiation is a pure function of COMPONENT capabilities: it works out what the
// source, the sink and the store can promise between them, and it never asks whether the engine can
// drive the answer. That gap is not theoretical. A source declaring StableKeys and durable
// retention, paired with a sink implementing Committer, negotiates exactly_once with
// ack_point=commit — and nothing in internal/engine has ever called Committer. Such a pipeline would
// settle every record on Write returning cleanly, advance the source past it, and leave the sink
// holding staged data that is never committed: data loss, under a promise of exactly-once, which is
// the one failure the whole negotiation exists to prevent.
//
// WHY IT IS NOT A BUILD ERROR. Build answers "is this pipeline coherent"; this answers "can the
// binary in front of you run it". They are different questions with different lifetimes — the second
// changes as the engine grows, the first does not — and conflating them would make Build unusable as
// the negotiation entry point that internal/stress uses it as. So Build WARNS, [Pipeline.Run]
// refuses before opening anything, and `canal check` exits non-zero. Nothing can move a record under
// a contract this build cannot keep.
//
// SCOPE (R10). This engine settles at three points: Sink.Write returning cleanly, Flusher.Flush
// making an accepted batch durable, and the two-phase commit publishing a committable. TokenSink is
// resolved by the registry, reported by the negotiation, and called by nothing, so ack point
// "token" is still a promise this build cannot keep. When it is implemented this function has
// nothing left to refuse and should be deleted rather than left as a check that never fires.
func Executable(n telemetry.Negotiated) error {
	for _, id := range slices.Sorted(maps.Keys(n.Nodes)) {
		c := n.Nodes[id]
		switch c.AckPoint {
		case "write", "flush", "commit":
			// "write" settles when Write returns; "flush" holds until Flusher.Flush makes the batch
			// durable; "commit" holds until the two-phase commit publishes it. All three are
			// implemented — see durability.go and commit.go.
			continue
		}
		return fmt.Errorf(
			"engine: sink %s earns its acknowledgement at %q and the negotiated guarantee is %s, "+
				"but this build settles only on Write returning cleanly and never calls %s",
			id, c.AckPoint, c.Guarantee, ifaceFor(c.AckPoint))
	}
	return nil
}

// ifaceFor names the Go interface behind an ack point, so a refusal is a task list rather than a
// category. It is the same discipline as config.Diagnostic.Iface.
func ifaceFor(ackPoint string) string {
	switch ackPoint {
	case "flush":
		return "connector.Flusher"
	case "commit":
		return "connector.Committer"
	case "token":
		return "connector.TokenSink"
	default:
		return "that interface"
	}
}

// closer pairs a node id with the Close it owes, so a failed build closes exactly what it constructed and
// nothing else.
type closer struct {
	id    record.NodeID
	close func(context.Context) error
}

// closeAll closes components in reverse construction order.
//
// out is the caller's outstanding-call table, or nil when there cannot be any: Build's failure path
// passes nil because nothing it does goes through sandbox — it constructs and validates, and neither
// is a plugin call the core can abandon.
func closeAll(ctx context.Context, d Deps, cs []closer, out *inflight) {
	// Close receives a FRESH context carrying the grace period, never the cancelled build context.
	base := context.WithoutCancel(ctx)
	for i := len(cs) - 1; i >= 0; i-- {
		cctx, cancel := context.WithTimeout(base, d.GracePeriod)

		// SETTLING SPENDS THE COMPONENT'S OWN GRACE PERIOD, not a second one beside it. A node's whole
		// shutdown budget is GracePeriod; waiting for an abandoned call to unwind comes out of it and
		// leaves the remainder for Close, so the worst case per component is what it always was.
		//
		// The wait almost always succeeds in microseconds, because an abandoned call is one whose
		// context was already cancelled and a connector that respects context is already unwinding.
		// It is the connector that does NOT that this exists for, and for that one the answer is to
		// LEAK THE COMPONENT: a file handle held open until the process exits is a far smaller harm
		// than a second goroutine inside a connector that is demonstrably not expecting one.
		if out != nil && !out.settle(cctx, cs[i].id) {
			d.Log.Error("not closing a component that still has an abandoned call inside it; "+
				"its resources are leaked until this process exits",
				"node", cs[i].id, "grace_period", d.GracePeriod)
			cancel()
			continue
		}

		if err := cs[i].close(cctx); err != nil {
			d.Log.Warn("closing a component after a failed build returned an error",
				"node", cs[i].id, "error", err)
		}
		cancel()
	}
}

// anchor attaches a node id to diagnostics produced by a component's own spec validation, so a form can
// render them against the right node in a multi-node graph.
func anchor(into config.Diagnostics, id record.NodeID, from config.Diagnostics) config.Diagnostics {
	for i := range from {
		from[i].Node = id
		into = append(into, from[i])
	}
	return into
}

// The shapes Run implements, kept here because they are the contract rather than the code:
//
//   - One goroutine per node with one select, over bounded channels of *record.Batch with capacity two.
//     A SOURCE node runs exactly two: the read goroutine (Open, Read, Close) and the control goroutine
//     (Commit, Heartbeat, Backlog, Nack, assignment refresh). That split is what makes Source.Read's
//     concurrency contract satisfiable at all: promising that Commit never runs concurrently with a
//     blocking Read is unsatisfiable, because an idle tail source would then never commit.
//
//   - Every call into a connector, codec, transform or buffer goes through [sandbox], so a panic becomes
//     a classified fault and a hang is abandoned rather than wedging the host.
//
//   - The commit pump reads [ledger.Ledger.Acks] on a DEDICATED per-source goroutine, never inline from
//     the persister, because a slow connector would otherwise block the process-wide flush cycle. See
//     the three phases in this package's doc comment, and do not reorder phase three above the flush.
//
//   - Shutdown is: stop reading; drain, with acknowledgements STILL being delivered throughout, because a
//     graceful stop must not throw away a commit that is one millisecond from safe; one final checkpoint
//     carrying the reconciliation pair; Close in reverse graph order with a fresh grace-period context;
//     report [ledger.Ledger.Leaks] and, if the drain timed out, NAME the unsettled groups in the final
//     log line and status document as a DRAIN-TIMEOUT, which is a different event from DRAINED because it
//     means records may replay; then close the ledger.
//
//   - The planner runs a pure Reconcile(plan, catalog, lanes, now) on a thirty-second timer as well as on
//     events, so no control path depends on message delivery.

// Close releases everything the pipeline holds. It is safe to call on a pipeline that never ran, and it is
// the caller's obligation whether Run succeeded or not.
func (p *Pipeline) Close(ctx context.Context) error {
	var cs []closer
	for id, s := range p.sources {
		cs = append(cs, closer{id: id, close: s.Source.Close})
	}
	for id, k := range p.sinks {
		cs = append(cs, closer{id: id, close: k.Sink.Close})
	}
	closeAll(ctx, p.deps, cs, &p.inflight)
	return p.ledger.Close()
}
