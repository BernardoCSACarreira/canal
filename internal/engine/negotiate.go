package engine

import (
	"fmt"
	"slices"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// resolved is the set of components one Build resolved, in graph order.
type resolved struct {
	sources map[record.NodeID]*registry.ResolvedSource
	sinks   map[record.NodeID]*registry.ResolvedSink
	buffers map[record.NodeID]connector.BufferCaps
}

// negotiate computes the honest delivery tier and every reason for it, and appends a diagnostic for
// every impossible combination.
//
// Capability negotiation is a PURE FUNCTION OF CONFIG, evaluated before anything starts. An impossible
// pipeline is refused HERE, at submit time, with per-field diagnostics — not at three in the morning with
// a quieter delivery guarantee. That is what turns "an acknowledgement means durable" from prose into a
// type.
//
// Every row of the refusal table below prevents a specific, documented failure in the surveyed field, and
// all rows are evaluated so that every diagnostic is returned together.
func negotiate(s *spec.Spec, res resolved, sc store.StoreCaps, d config.Diagnostics) (telemetry.Negotiated, config.Diagnostics) {
	out := telemetry.Negotiated{
		Guarantee:      s.Guarantee,
		ReplayBudget:   s.LaneBudget,
		Nodes:          map[record.NodeID]telemetry.NodeContract{},
		Downgrades:     s.Downgrades,
		DurabilityEdge: "",
	}

	// ---- the deployment's own stores are part of what is validated -------------------
	//
	// The store bounds EVERY branch, including one whose spec.Node.Guarantee asks for more, so a
	// per-node override can never route around this refusal.
	storeCeiling := connector.ExactlyOnce
	for g := connector.ExactlyOnce; ; g-- {
		if ok, _ := sc.Supports(g); ok {
			storeCeiling = g
			break
		}
		if g == connector.AtMostOnce {
			storeCeiling = connector.AtMostOnce
			break
		}
	}
	if ok, why := sc.Supports(s.Guarantee); !ok {
		d = d.Errorf(config.CodeGuarantee, nil, why,
			"choose a weaker tier, or configure a state store that can support this one")
	}

	// sourceCeiling is the strongest tier the SOURCE half of this pipeline can truthfully support. It
	// is folded separately from the sink half so that a per-branch tier is computable at all.
	sourceCeiling := connector.ExactlyOnce

	for id, src := range res.sources {
		c := src.Caps
		out.Why = append(out.Why, fmt.Sprintf("source %s: upstream retention is %s", src.Name, c.UpstreamRetention))

		// A lost in-flight window is lost data unless the source can re-read it OR its peer will
		// re-send it. The second disjunct is the push source: it has no cursor to rewind to and its
		// peer's own retry IS the replay mechanism, so clamping it to at-most-once forced it to
		// acknowledge that peer before the data was durable.
		switch {
		case c.Replayable, c.RedeliversUnacked:
			if !c.Replayable {
				out.Why = append(out.Why, fmt.Sprintf(
					"source %s cannot re-read a committed position, but declares that its peer redelivers anything unacknowledged",
					src.Name))
			}
		default:
			sourceCeiling = sourceCeiling.Min(connector.AtMostOnce)
			out.Why = append(out.Why, fmt.Sprintf("source %s cannot be replayed, so nothing above at-most-once is truthful", src.Name))
			if s.Guarantee > connector.AtMostOnce {
				d = append(d, capDiag(id, fmt.Sprintf("source %q declares neither Replayable nor RedeliversUnacked, so a lost in-flight window is lost data", src.Name),
					"request at_most_once explicitly, use a source that can re-read from a committed position, or declare RedeliversUnacked if the peer retries", ""))
			}
		}

		// EffectivelyOnce and above need a key for duplicates to collapse onto.
		if !c.StableKeys {
			sourceCeiling = sourceCeiling.Min(connector.AtLeastOnce)
			out.Why = append(out.Why, fmt.Sprintf("source %s does not declare StableKeys, so duplicates have nothing to collapse onto", src.Name))
		}

		// A source whose upstream discards on commit needs canal's own record to exist first, so the
		// deployment must have a durable state store. This is the ack-before-persist defect, refused.
		if c.UpstreamRetention == connector.PrunesOnCommit && !sc.FlushIsDurable {
			d = append(d, capDiag(id,
				fmt.Sprintf("source %q prunes its upstream on commit, but the configured state store does not guarantee durability on return", src.Name),
				"configure a durable state store; without one, a crash between the commit and the write is an unrecoverable gap", ""))
		}

		// A gated lane behind a long scan is not being read, so only a heartbeat can hold its slot.
		if c.UpstreamRetention == connector.PrunesOnCommit && wantsGatedStream(s) && !c.Heartbeats {
			d = append(d, capDiag(id,
				fmt.Sprintf("source %q prunes on commit and this pipeline gates a stream lane behind a scan, but the source cannot heartbeat", src.Name),
				"the upstream would pin its retention for the whole scan; implement connector.Heartbeater or drop the initial scan",
				"connector.Heartbeater"))
		}

		if s.Parallelism > 0 && c.MaxLanes > 0 && s.Parallelism > c.MaxLanes {
			d = append(d, capDiag(id,
				fmt.Sprintf("parallelism %d exceeds what source %q supports (%d lanes)", s.Parallelism, src.Name, c.MaxLanes),
				"lower parallelism; the lane cap is enforced at announce time, not advisory", ""))
		}

		// Stream choices are resolved PER NODE. Cross-multiplying them against every source is what
		// refused two merging sources whose lane kinds legitimately differ — and record.DefaultStream
		// makes that name collision the DEFAULT case for two single-stream sources.
		for _, want := range s.StreamsFor(id) {
			for _, k := range want.Read {
				if !hasLaneKind(c.LaneKinds, k) {
					d = append(d, capDiag(id,
						fmt.Sprintf("stream %q asks for a %s lane at node %q, which source %q cannot produce", want.Stream, k, id, src.Name),
						"remove that read mode for this stream, or scope the entry to the node that can produce it", ""))
				}
			}
		}
	}

	for id, sk := range res.sinks {
		c := sk.Caps

		// PER-BRANCH TIER. One value folded over every sink cannot describe a fan-out to an
		// exactly-once warehouse plus three at-least-once branches: requesting the strong tier is a
		// hard error on the weak branches, and requesting the weak one mis-reports the strong branch to
		// the operator who asked for it.
		requested := s.Guarantee
		if n, ok := s.Node(id); ok && n.Guarantee != nil {
			requested = *n.Guarantee
		}
		branch := sourceCeiling.Min(c.MaxGuarantee()).Min(storeCeiling)

		if !c.Idempotent {
			out.Why = append(out.Why, fmt.Sprintf("sink %s is not idempotent", sk.Name))
		}
		if sk.Committer == nil && sk.Token == nil {
			out.Why = append(out.Why, fmt.Sprintf("sink %s implements neither Committer nor TokenSink", sk.Name))
			if requested == connector.ExactlyOnce {
				d = append(d, capDiag(id,
					fmt.Sprintf("exactly-once was requested for node %q but sink %q implements neither Committer nor TokenSink", id, sk.Name),
					"implement one of them, request effectively_once with an idempotent sink and stable keys, or set this node's own guarantee",
					"connector.Committer"))
			}
		}
		if requested == connector.EffectivelyOnce && !c.Idempotent {
			d = append(d, capDiag(id,
				fmt.Sprintf("effectively-once was requested for node %q but sink %q does not declare Idempotent", id, sk.Name),
				"duplicates would land at the destination with nothing to absorb them", ""))
		}
		if c.RequiresCompleteImages && !everySourceDeclares(res, func(sc connector.SourceCaps) bool { return sc.CompleteImages }) {
			d = append(d, capDiag(id,
				fmt.Sprintf("sink %q requires complete images, and a source in this pipeline may emit partial after-images", sk.Name),
				"an upsert of a partial image writes nulls over live data; use a source that declares CompleteImages", ""))
		}
		if c.RequiresKey && !everySourceDeclares(res, func(sc connector.SourceCaps) bool { return sc.StableKeys }) {
			d = append(d, capDiag(id,
				fmt.Sprintf("sink %q requires a record key, and a source in this pipeline does not declare StableKeys", sk.Name),
				"a keyed destination fed unkeyed records cannot upsert", ""))
		}
		for _, want := range s.StreamsFor(id) {
			if !hasMode(c.Modes, want.Write) {
				d = append(d, capDiag(id,
					fmt.Sprintf("stream %q is configured to %s at node %q, which sink %q does not support", want.Stream, want.Write, id, sk.Name),
					"choose a mode the sink lists, or scope a per-node entry for this branch", ""))
			}
			if want.Dedupe != nil && want.Dedupe.Window == 0 {
				d = d.Errorf(config.CodeGuarantee, []string{"streams", string(want.Stream), "dedupe", "window"},
					"a dedupe window is required and has no default",
					"a process-lifetime cache described as a retention window matches neither semantics")
			}
		}

		// A GAP IN A SINK'S DECLARED SchemaChanges IS A WARNING, NOT A REFUSAL. No drift policy makes
		// a sink capability a precondition — every one of them is defined relative to what the sink
		// supports — so refusing here punished the sink that declared honestly and rewarded the one
		// that withheld AppliesSchema entirely. See spec.DriftPolicy.Permits.
		if sk.SchemaApp != nil {
			for _, k := range driftKinds(s) {
				if !hasChangeKind(c.SchemaChanges, k) {
					d = append(d, config.Diagnostic{
						Node:     id,
						Severity: config.SeverityWarning,
						Code:     config.CodeCapability,
						Message: fmt.Sprintf("the %s drift policy may apply %s, which sink %q does not declare",
							s.Drift, k, sk.Name),
						Hint: "such a change will raise a drift event and a degraded condition, and the old shape will keep being written",
					})
				}
			}
		}

		if branch != requested {
			out.Why = append(out.Why, fmt.Sprintf("node %s: min(requested=%s, achievable=%s)", id, requested, branch))
		}
		out.Nodes[id] = telemetry.NodeContract{
			Guarantee: requested.Min(branch),
			// AckPoint and DurabilityEdge are PER NODE. Assigning two single-valued strings inside a
			// map-range loop made the disclosed answer depend on Go's map iteration order: 200 identical
			// Builds produced four durability_edge values and two ack_point values for one graph.
			AckPoint:       sk.AckPoint(),
			DurabilityEdge: "sink:" + string(id),
			BestEffort:     nodeIsBestEffort(s, id),
		}
	}

	// ---- a buffer may shorten the ack chain only inside its own durability domain ----
	for id, bc := range res.buffers {
		if bc.Durability == connector.DurabilityNone {
			continue
		}
		if bc.Durability < connector.DurabilityCluster {
			out.Why = append(out.Why, fmt.Sprintf(
				"buffer %s is %s-durable, which is narrower than a lane's assignment domain, so it does not shorten the acknowledgement chain",
				id, bc.Durability))
		}
	}

	// ---- retry policy -------------------------------------------------------------
	if err := s.Retry.Validate(); err != nil {
		d = d.Errorf(config.CodeOutOfRange, []string{config.FieldRetry}, err.Error(),
			"there is deliberately no unbounded retry: a poison record must be able to reach a terminal disposition")
	}
	if s.Retry.Terminal == fault.TerminalDeadLetter {
		if !hasFailedEdge(s) {
			d = d.Errorf(config.CodeGraphInvalid, []string{config.FieldRetry, "terminal"},
				"the retry policy dead-letters, but no node takes a failed edge",
				"add a sink node whose input edge selects failed records, or choose a different terminal")
		}
	}

	// ---- the pipeline-wide summary is the WEAKEST branch, named -----------------------
	//
	// It is derived from the per-node contracts rather than folded independently, so the headline and
	// the detail cannot disagree. A pipeline with one exactly-once branch and one at-least-once branch
	// reports at_least_once here and both truths in Nodes, which is the only pair of statements that
	// is honest at once.
	//
	// The old fold clamped `effective` to AtLeastOnce for every Replayable source, so
	// Negotiated.Guarantee could NEVER report effectively_once or exactly_once for any pipeline
	// whatsoever: Build accepted the request and then under-reported it for the pipeline's whole life.
	// Replayability bounds a source at AtLeastOnce only when it is the ONLY evidence available; a
	// source with stable keys reaches EffectivelyOnce and a Committer sink reaches ExactlyOnce, which
	// is what the guarantee ladder says.
	out.Guarantee = connector.ExactlyOnce
	if len(out.Nodes) == 0 {
		out.Guarantee = connector.AtMostOnce
	}
	weakest := record.NodeID("")
	for id, nc := range out.Nodes {
		if nc.BestEffort {
			// A best-effort branch bears no progress, so it cannot lower the pipeline's guarantee. It
			// would otherwise drag every honest fan-out down to its weakest deliberately-lossy leg.
			continue
		}
		if nc.Guarantee < out.Guarantee {
			out.Guarantee, weakest = nc.Guarantee, id
		}
		if nc.DurabilityEdge != "" && (out.DurabilityEdge == "" || nc.Guarantee <= out.Guarantee) {
			out.DurabilityEdge = nc.DurabilityEdge
			out.AckPoint = nc.AckPoint
		}
	}
	if weakest != "" {
		out.Why = append(out.Why, fmt.Sprintf("pipeline guarantee is the weakest progress-bearing branch: node %s at %s", weakest, out.Guarantee))
	}
	if out.AckPoint == "" {
		out.AckPoint = "write"
	}

	// ---- an operator-signed waiver is the ONLY way to run below the request -----------
	for _, w := range out.Downgrades {
		out.Why = append(out.Why, fmt.Sprintf(
			"operator-signed waiver by %s: requested %s, running %s (%s)", w.AcknowledgedBy, w.Requested, w.Effective, w.Reason))
	}
	d = applyWaivers(s, d)
	return out, d
}

// applyWaivers downgrades a guarantee or capability ERROR to a warning when a waiver in the spec
// covers the node it is anchored to.
//
// It exists because ADR 0024 names a waiver as the only sanctioned way to run below the requested
// tier, and there was nowhere to write one: spec had no field, negotiate read none, and
// Negotiated.Downgrades was therefore always nil. The mechanism the guarantee ADR rests on was
// unreachable, leaving only refuse-outright or degrade-silently.
//
// A waiver never invents a capability and never changes what the engine does; it changes whether an
// honest refusal blocks submission, and it is recorded with actor, time and reason for the pipeline's
// whole life. A waiver matching nothing becomes its own warning, so a stale one is visible rather
// than load-bearing.
// waiverCovers reports whether one operator-signed waiver actually covers one refusal.
//
// This predicate is the whole of the fix for the audit's fatal R4 finding. The old matcher tested
// only three things — that the waiver was signed, that it gave a reason, and that its Node was
// either empty or equal — so an empty Node acted as a WILDCARD and any single signed waiver
// downgraded every capability and guarantee error on every node in the pipeline, including ADR
// 0006's ack-before-persist guard. Signing off "the queue sink cannot upsert" silently also signed
// off "this pipeline will lose data".
//
// Three conditions now hold, and the first two are what close that hole:
//
//  1. SCOPE IS EXACT. A waiver anchored to a node covers only that node; a pipeline-scoped waiver
//     (empty Node) covers only pipeline-scoped refusals. There is no wildcard.
//  2. IT MUST NAME WHAT IT WAIVES. Missing lists capability tokens (ADR 0024: "capability NAMES,
//     never iota ids"). A capability refusal carries Iface, the Go interface that would fix it, so
//     the waiver has to name that interface. A waiver cannot cover a refusal it does not mention.
//  3. IT MUST BE SIGNED AND EXPLAINED, as before.
//
// A waiver is a TIER decision — ADR 0024 exists so an operator with an append-only sink can run at
// a lower tier knowingly. It is not a data-loss decision, and the refusals that mean "this
// configuration loses records" are not on the ladder it can move you down.
func waiverCovers(w telemetry.Downgrade, dg config.Diagnostic) bool {
	if w.AcknowledgedBy == "" || w.Reason == "" {
		return false // an unsigned or unexplained waiver waives nothing
	}
	if len(w.Missing) == 0 {
		return false // and neither does one that does not say what it is waiving
	}
	if w.Node != dg.Node {
		return false // exact scope: no wildcard, in either direction
	}

	// A capability refusal names the interface that would fix it. The waiver must name it too.
	if dg.Code == config.CodeCapability {
		if dg.Iface == "" {
			// No machine-readable identity to match against, so this refusal is not waivable. It
			// fails closed deliberately: the alternative is matching on prose, and a waiver that
			// matches on a message string stops matching the day the message is reworded.
			return false
		}
		return slices.Contains(w.Missing, dg.Iface)
	}

	// A guarantee refusal is the tier itself. The waiver must be the one for this move, which means
	// naming both ends of it.
	return w.Requested != "" && w.Effective != ""
}

func applyWaivers(s *spec.Spec, d config.Diagnostics) config.Diagnostics {
	if len(s.Downgrades) == 0 {
		return d
	}
	used := make([]bool, len(s.Downgrades))
	for i := range d {
		if d[i].Severity != config.SeverityError {
			continue
		}
		if d[i].Code != config.CodeCapability && d[i].Code != config.CodeGuarantee {
			continue
		}
		for j, w := range s.Downgrades {
			if !waiverCovers(w, d[i]) {
				continue
			}
			d[i].Severity = config.SeverityWarning
			d[i].Hint = "running under an operator-signed waiver: " + w.Reason + " — " + d[i].Hint
			used[j] = true
			break
		}
	}
	for j, w := range s.Downgrades {
		switch {
		case len(w.Missing) == 0:
			d = append(d, config.Diagnostic{
				Node: w.Node, Severity: config.SeverityError, Code: config.CodeGuarantee,
				Message: "a downgrade waiver must name the capabilities whose absence it is waiving",
				Hint:    "list them in Missing, as their stable tokens; a waiver that names nothing cannot be checked against anything and would silence every refusal on the node",
			})
		case w.AcknowledgedBy == "" || w.Reason == "":
			d = append(d, config.Diagnostic{
				Node: w.Node, Severity: config.SeverityError, Code: config.CodeGuarantee,
				Message: "a downgrade waiver must name who acknowledged it and why",
				Hint:    "the core can never mint a waiver itself; an anonymous one is a silent degradation with extra steps",
			})
		case !used[j]:
			d = append(d, config.Diagnostic{
				Node: w.Node, Severity: config.SeverityWarning, Code: config.CodeGuarantee,
				Message: fmt.Sprintf("the waiver for %s -> %s covers no refusal in this pipeline", w.Requested, w.Effective),
				Hint:    "remove it; a waiver that waives nothing outlives the reason it was signed for",
			})
		}
	}
	return d
}

// nodeIsBestEffort reports whether EVERY inbound edge of a node is declared best-effort, which is what
// makes the node itself non-progress-bearing.
func nodeIsBestEffort(s *spec.Spec, id record.NodeID) bool {
	n, ok := s.Node(id)
	if !ok || len(n.Inputs) == 0 {
		return false
	}
	for _, e := range n.Inputs {
		if !e.BestEffort {
			return false
		}
	}
	return true
}

func capDiag(id record.NodeID, msg, hint, iface string) config.Diagnostic {
	return config.Diagnostic{
		Node:     id,
		Severity: config.SeverityError,
		Code:     config.CodeCapability,
		Message:  msg,
		Hint:     hint,
		Iface:    iface,
	}
}

func wantsGatedStream(s *spec.Spec) bool {
	// A pipeline gates a stream lane whenever an operator asked for both a scan and a stream of one
	// stream: the scan group must finish before the tail may be read.
	for _, st := range s.Streams {
		var scan, stream bool
		for _, k := range st.Read {
			switch k {
			case connector.LaneKindScan:
				scan = true
			case connector.LaneKindStream:
				stream = true
			}
		}
		if scan && stream {
			return true
		}
	}
	return false
}

func everySourceDeclares(res resolved, pred func(connector.SourceCaps) bool) bool {
	if len(res.sources) == 0 {
		return false
	}
	for _, src := range res.sources {
		if !pred(src.Caps) {
			return false
		}
	}
	return true
}

func hasLaneKind(ks []connector.LaneKind, k connector.LaneKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

func hasMode(ms []connector.DestMode, m connector.DestMode) bool {
	for _, x := range ms {
		if x == m {
			return true
		}
	}
	return false
}

// driftKinds is the set of schema change kinds the configured drift policy may ask a sink to perform.
//
// It is derived from the policy rather than from the data, because the check must happen at submit time:
// "a schema change nobody can apply" must be a diagnostic now, not a stopped pipeline at three in the
// morning.
func driftKinds(s *spec.Spec) []schema.ChangeKind {
	all := []schema.ChangeKind{
		schema.CreateStream, schema.AddField, schema.DropField, schema.RenameField,
		schema.AlterFieldType, schema.AlterNullability, schema.AlterKeys,
		schema.TruncateStream, schema.DropStream,
	}
	var out []schema.ChangeKind
	for _, k := range all {
		if s.Drift.Permits(k) {
			out = append(out, k)
		}
	}
	return out
}

func hasChangeKind(ks []schema.ChangeKind, k schema.ChangeKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

func hasFailedEdge(s *spec.Spec) bool {
	for i := range s.Graph {
		for _, e := range s.Graph[i].Inputs {
			if e.Select.CarriesFailed() {
				return true
			}
		}
	}
	return false
}
