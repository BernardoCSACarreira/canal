package engine

import (
	"fmt"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
)

// validateGraph reports every structural problem in a topology, all at once, each anchored to a node id.
//
// It asks the REGISTRY whether a kind produces or consumes rather than switching on a name, which is why
// adding a node kind is data and not a contract change (design rule R1).
func validateGraph(r *registry.Registry, s *spec.Spec, d config.Diagnostics) config.Diagnostics {
	if len(s.Graph) == 0 {
		return append(d, config.Diagnostic{
			Severity: config.SeverityError, Code: config.CodeGraphInvalid,
			Message: "the pipeline has no nodes",
			Hint:    "a pipeline needs at least one source node and one sink node",
		})
	}

	seen := map[record.NodeID]int{}
	for i := range s.Graph {
		n := &s.Graph[i]
		if n.ID == "" {
			d = append(d, config.Diagnostic{
				Severity: config.SeverityError, Code: config.CodeGraphInvalid,
				Message: fmt.Sprintf("the node at position %d has no id", i),
				Hint:    "every node needs a stable id; it is the metric label and the edge endpoint",
			})
			continue
		}
		if _, dup := seen[n.ID]; dup {
			d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
				fmt.Sprintf("duplicate node id %q", n.ID),
				"node ids must be unique within a pipeline"))
		}
		seen[n.ID] = i
	}

	for i := range s.Graph {
		n := &s.Graph[i]

		if !r.Has(n.Kind, n.Name) {
			d = append(d, nodeDiag(n.ID, config.CodeUnknownComponent,
				fmt.Sprintf("no %s named %q is registered", n.Kind, n.Name),
				"check the spelling, or check that the connector package is imported into this build"))
		}

		for _, e := range n.Inputs {
			if _, ok := seen[e.From]; !ok {
				d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
					fmt.Sprintf("input edge names node %q, which does not exist", e.From), ""))
			}
			if e.From == n.ID {
				d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
					"a node cannot be its own input", ""))
			}
		}

		switch {
		case len(n.Inputs) == 0 && !n.Kind.Produces():
			d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
				fmt.Sprintf("node %q is a %s with no inputs", n.ID, n.Kind),
				"only a source node may have no inputs"))
		case len(n.Inputs) > 0 && n.Kind.Produces():
			d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
				fmt.Sprintf("source node %q has inputs", n.ID),
				"a source node reads from its upstream system, not from another node"))
		}

		if n.Kind == registry.KindBuffer && len(n.Inputs) != 1 {
			d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
				fmt.Sprintf("buffer node %q has %d inputs", n.ID, len(n.Inputs)),
				"a buffer has exactly one input; put a transform in front of it to merge streams"))
		}

		if n.Kind == registry.KindTransform && len(n.Inputs) > 1 {
			e, ok := r.Transform(n.Name)
			if ok && !e.Caps.Regroups {
				d = append(d, nodeDiag(n.ID, config.CodeCapability,
					fmt.Sprintf("transform %q has %d inputs but does not declare that it regroups", n.ID, len(n.Inputs)),
					"a transform merging several inputs must use Batch.Merge and declare TransformCaps.Regroups"))
			}
		}

		for _, e := range n.Inputs {
			if !e.Select.CarriesFailed() {
				continue
			}
			from, ok := s.Node(e.From)
			if !ok {
				continue
			}
			if !canFailPerRecord(r, from) {
				d = append(d, nodeDiag(n.ID, config.CodeGraphInvalid,
					fmt.Sprintf("node %q takes a failed edge from %q, which cannot fail per record", n.ID, e.From),
					"a failed edge is only meaningful from a node that classifies faults per record"))
			}
		}
	}

	for _, t := range s.Terminals() {
		n, ok := s.Node(t)
		if !ok || n.Kind.Consumes() {
			continue
		}
		d = append(d, nodeDiag(t, config.CodeGraphInvalid,
			fmt.Sprintf("node %q is terminal but is a %s", t, n.Kind),
			"every path must end at a sink; a terminal transform silently discards its output"))
	}

	d = checkAcyclic(s, d)
	d = checkReachable(s, d)
	return d
}

// canFailPerRecord reports whether a node kind can produce a per-record fault at all.
//
// A failed edge from a node that can only fail wholesale would be an edge nothing ever traverses, which is
// worse than an error: it looks like a dead-letter route and is not one.
func canFailPerRecord(r *registry.Registry, n *spec.Node) bool {
	switch n.Kind {
	case registry.KindSource, registry.KindTransform, registry.KindEncoder, registry.KindDecoder:
		return true
	case registry.KindSink:
		e, ok := r.Sink(n.Name)
		return ok && e.Caps.PartialFailure
	default:
		return false
	}
}

func checkAcyclic(s *spec.Spec, d config.Diagnostics) config.Diagnostics {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	colour := map[record.NodeID]int{}
	inputs := map[record.NodeID][]record.NodeID{}
	for i := range s.Graph {
		for _, e := range s.Graph[i].Inputs {
			inputs[s.Graph[i].ID] = append(inputs[s.Graph[i].ID], e.From)
		}
	}

	var visit func(record.NodeID) bool
	visit = func(id record.NodeID) bool {
		switch colour[id] {
		case grey:
			return false
		case black:
			return true
		}
		colour[id] = grey
		for _, up := range inputs[id] {
			if !visit(up) {
				return false
			}
		}
		colour[id] = black
		return true
	}

	for i := range s.Graph {
		if !visit(s.Graph[i].ID) {
			return append(d, nodeDiag(s.Graph[i].ID, config.CodeGraphInvalid,
				fmt.Sprintf("the graph contains a cycle through node %q", s.Graph[i].ID),
				"a pipeline is a directed acyclic graph; a cycle can never drain"))
		}
	}
	return d
}

func checkReachable(s *spec.Spec, d config.Diagnostics) config.Diagnostics {
	reachable := map[record.NodeID]bool{}
	// Walk backwards from every terminal: a node that no terminal depends on is doing nothing.
	inputs := map[record.NodeID][]record.NodeID{}
	for i := range s.Graph {
		for _, e := range s.Graph[i].Inputs {
			inputs[s.Graph[i].ID] = append(inputs[s.Graph[i].ID], e.From)
		}
	}
	var walk func(record.NodeID)
	walk = func(id record.NodeID) {
		if reachable[id] {
			return
		}
		reachable[id] = true
		for _, up := range inputs[id] {
			walk(up)
		}
	}
	for _, t := range s.Terminals() {
		walk(t)
	}
	for i := range s.Graph {
		if !reachable[s.Graph[i].ID] {
			d = append(d, nodeDiag(s.Graph[i].ID, config.CodeUnreachable,
				fmt.Sprintf("node %q is unreachable: no sink depends on it", s.Graph[i].ID),
				"connect it to a sink, or remove it; an unreachable node is not silently ignored"))
		}
	}
	return d
}

func nodeDiag(id record.NodeID, code config.Code, msg, hint string) config.Diagnostic {
	return config.Diagnostic{
		Node:     id,
		Severity: config.SeverityError,
		Code:     code,
		Message:  msg,
		Hint:     hint,
	}
}
