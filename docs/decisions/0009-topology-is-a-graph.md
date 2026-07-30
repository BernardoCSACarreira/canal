# 0009 — Topology is a graph of nodes with selector edges

**Status:** accepted, normative. Satisfies R1.

## Context

R1 exists because the abandoned attempt froze an eight-stage pipeline into its OpenAPI schema as `stages` with
`minItems: 8, maxItems: 8` and `ordinal` constrained `1..8`, so adding a transform, a router or a second
buffer tier was a **breaking contract change** — and because buffers were modelled *twice*, as stages 3/5/7
and again as "segments" keyed by `followsStageOrdinal` 2/4/6, one entity with two identifiers and a `kind`
enum with exactly one permitted value.

The proposal this architecture is rooted in reintroduced the same failure in milder form: a fixed seven-named-
slot `Spec{Source, Decode, Transforms, Buffer, Encode, Sink, DLQ}` that the config store persisted and the API
accepted, so a new stage kind was a nine-site edit plus a persisted-format change. Its reviewer called that a
major R1 violation.

Its answer for variety was Benthos's: a fixed outer shape plus components that *contain* other components,
resolved from config — `broker`, `switch`, `fallback`, `retry`. Its reviewer found that **fatal**: nothing in
the design let a component obtain its children, so the entire topology story was unbuildable, and the two
composite config extractors that would have enabled it created a `config`↔`connector` import cycle. Benthos
itself punches the same hole with an undocumented `Unwrap()` type assertion and five `XUnwrapper() any` back
doors.

## Decision

**A pipeline is a graph, and the graph is data.**

```go
type Spec struct {
    Graph []Node
    // ... policy
}

type Node struct {
    ID     record.NodeID
    Kind   registry.Kind   // a STRING, validated against the registry
    Name   string
    Label  string
    Config map[string]any
    Inputs []Edge
}

type Edge struct {
    From   record.NodeID
    Select EdgeSelect // Main | Failed | All
}
```

**One mechanism, and it covers everything the wrappers existed for:**

| Wanted | Expressed as |
|---|---|
| fan-out | two sink nodes naming the same input |
| fan-in | one node naming two inputs |
| dead-letter route | an edge with `Select: EdgeFailed` |
| fallback | a sink node whose only input is another sink node's `EdgeFailed` edge |
| retry | a stage-standard `retry` config field on the node, enforced by the engine |
| buffer tiers | two buffer nodes, the first configured `when_full: overflow` |
| routing / switch | a `route` transform node with `EdgeMain` outputs, or per-node predicates |

**`EdgeFailed` works for sources too**, which Kafka Connect's sink-only `ErrantRecordReporter` does not.

**`registry.Kind` and the graph's validation are open.** A new node kind is a registry entry and a config
document, not a schema change and not a coordinated frontend edit. The validator checks membership, so an
unknown kind is a diagnostic anchored to the node.

**Component-valued config fields survive only for non-topological policy** — never for stages. That is what
keeps `config` free of any dependency on `connector` and therefore free of the cycle.

**Graph validation, all diagnostics at once, anchored to a node id:** unique ids; every `Edge.From` exists;
acyclic; a node with no inputs is a producer kind; a terminal node is a consumer kind; a buffer has exactly one
input; a transform has one input unless it declares `Regroups`; no `EdgeFailed` from a kind that cannot fail
per-record; and the graph is connected, because an unreachable node is a diagnostic rather than something
silently doing nothing.

## Alternatives rejected

- **A fixed stage list in the contract.** Rejected: it is R1's originating defect.
- **A fixed outer shape plus recursive component composition (Benthos).** Rejected on four counts, and this is
  the closest call in the architecture. (a) It needs a child-resolution accessor that `config` cannot have
  without importing `connector`, which is the cycle. (b) A nested branch has no node id, so per-branch metrics,
  per-branch settlement attribution and per-branch faults are invisible — the reviewer's exact complaint that
  "no label or field can name a nested branch". (c) It is a *second* representation of composition alongside
  the outer shape, which is R9. (d) It requires a sanctioned "I am a stage, not a leaf" declaration, which
  Benthos never shipped and replaced with an undocumented assertion and five back doors. What is lost is
  genuine: deeply nested YAML is sometimes more readable than an edge list, and a wrapper can be authored
  out-of-tree while an edge kind cannot.
- **A general DAG with shuffles and barriers.** Rejected — see ADR 0010.
- **1-to-1 transforms only (Kafka Connect's SMTs).** Rejected: it is why that transform ecosystem is stunted
  and why conditional application needed its own KIP, with one predicate per transform and no boolean
  combinators.
- **Closed `Kind` iota.** Rejected: a closed enum used as a persisted discriminator and a metric label makes
  adding a component kind a contract change one level up from the stage list — R1 in a new place.

## Consequences

- Positive: fan-out, fan-in, dead-lettering, fallback and buffer chaining all exist with **zero core
  special-casing** and every branch has a node id that is already a metric label. One representation per
  entity. New node kinds are data.
- **Negative, accepted:** wrapper components (`retry`, `fallback`, `broker`) do not exist as third-party
  extension points, so a novel *composition* behaviour needs either a new edge selector (a core change) or a
  transform. Judged the right trade: the three compositions anyone actually wants are expressible, and the
  invisible-branch problem is worse than the extensibility loss.
- **Negative, accepted:** a graph is more verbose in YAML than nesting for the simple linear case. Mitigated by
  `canal run --source X --sink Y` generating the graph, and by `Inputs` defaulting to "the previous node" when
  a document lists nodes in order.
- **Negative, accepted:** losing compile-time exhaustiveness on `Kind`. Mitigated by registry validation and a
  golden-file test pinning every rendered vocabulary.
