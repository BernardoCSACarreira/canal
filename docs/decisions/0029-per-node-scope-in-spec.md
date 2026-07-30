# 0029 — Guarantee, destination mode, cadence and writer state are per NODE, not per pipeline

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

ADR 0009 made topology data: fan-out is two nodes naming one input, fan-in is one node naming two inputs, and
no other vocabulary is needed. That was right about the *graph* and wrong about everything hung off it. Five
facts that vary per branch were single-valued per pipeline, and the fan-out/fan-in hostile case rated all
five FATAL:

1. **`spec.StreamConfig.Write` — one destination mode per stream**, checked against every sink in the
   per-sink loop. A fan-out to an upserting warehouse plus an append-only queue, a metrics feed and a
   dead-letter sink is refused at `Build`: at most one of four can match.
2. **`spec.Spec.Guarantee` — one requested tier per pipeline**, folded with `Min` over every sink and handed
   identically to all four through `Opening.Guarantee`. Requesting exactly-once for the warehouse is a hard
   error on each of the other three; requesting at-least-once so the graph builds mis-reports the warehouse,
   which really is exactly-once, to the operator who asked for it.
3. **`engine.Checkpoint.WriterState []record.Blob` — one unkeyed slice**, while `TransformState` on the same
   struct is `map[record.NodeID][]record.Blob`. Two `WriterState` sinks in one graph are handed each other's
   blobs at `RestoreState`. The lucky case is a loud contract fault; the unlucky case is two nodes running
   the same connector, where the blobs decode perfectly and each sink adopts the other's open uploads.
4. **`spec.StreamConfig.Read` cross-multiplied against every source**, so a read mode wanted from one source
   is validated against the other — and `record.DefaultStream` makes that name collision the *default* case
   for two single-stream sources merging.
5. **`engine.Deps.FlushInterval` — one durability cadence per deployment**, driving both `Flusher.Flush` and
   `Committer.PrepareCommit`. A 30-second-batch warehouse and a 1-millisecond queue in one graph have two
   correct cadences and one field.

A sixth, related: **no way to declare a branch non-progress-bearing.** Every branch contributed a settlement
reference, so a deliberately best-effort metrics feed shedding load held the prefix of the warehouse's
source — and `Ack.Abandoned` is one `uint64`, so a source with a DESTRUCTIVE commit could not tell a
by-design shed from a real dead-letter and had to refuse to advance on either.

## Decision

**One scoping field, four per-branch overrides, and one edge property.**

- **`spec.StreamConfig.Node record.NodeID`**, empty meaning every node. Node-scoped entries win over the
  unscoped entry for the same stream. `Spec.StreamFor(node, stream)` and `Spec.StreamsFor(node)` are the
  only resolvers, so "specific beats general" has exactly one implementation. This alone fixes (1) and (4)
  and turns negotiate's cross product into a lookup.
- **`spec.Node.Guarantee *connector.Guarantee`**, nil meaning the pipeline's request. The negotiated tier is
  still a fold of `Min`, per branch, over that branch's own factors. A branch may never exceed the
  deployment store's ceiling, so an override cannot route around the store-capability refusal.
- **`engine.Checkpoint.WriterState map[record.NodeID][]record.Blob`**, matching `TransformState`.
- **`spec.Node.Cadence *Cadence{Interval, Records}`**, nil meaning the deployment's. Zero on a bound means
  the deployment's value for that bound, never "no bound" — a node disabling both would hold data forever.
- **`spec.Edge.BestEffort bool`**: the engine adds no settlement reference for that branch, so a record lost
  there never holds the source's cursor and never appears in `Ack.Abandoned`. Paired with
  **`Ack.AbandonedBy map[record.NodeID]uint64`**, so the abandonments that *are* reported are attributable.

**`telemetry.Negotiated.Nodes map[record.NodeID]NodeContract`** reports the per-branch answer, and
`Negotiated.Guarantee` becomes the weakest *progress-bearing* branch, derived from `Nodes` so the headline
and the detail cannot disagree.

## Why `BestEffort` is on the EDGE and not on the sink

A sink cannot know whether the operator considers it load-bearing. The same metrics sink is best-effort in
one graph and the entire point of another. Progress-bearing-ness is a property of the *operator's intent for
this branch*, which is exactly what an edge is. Putting it on `SinkCaps` would have let a connector author
decide, on the user's behalf, that losing their data is acceptable.

Faults on a best-effort branch are still counted, still classified and still raise `EventDegraded`.
Unobserved is not the same as unimportant.

## Why not a sub-pipeline per branch

The alternative was to model each fan-out leg as its own pipeline sharing a source. It fails on the thing the
whole design is built around: one source, one lane set, one cursor. Two pipelines reading one source either
duplicate the read or need a coordination protocol between pipelines, and neither exists. The graph is
already the right representation; it just needed its leaves to be able to differ.

## Consequences

- Five fatal and four major breakages close with one scoping field, three optional overrides and one edge
  bool. No required interface changes; no connector changes.
- Every valid spec stays valid: every new field's zero value is the previous behaviour.
- `negotiate` becomes per-sink rather than pipeline-folded, which is also what fixed three separate
  reporting bugs (architecture §30.3).
- `spec` now imports `telemetry` for `Downgrade`. That is the correct direction — `telemetry` owns the JSON
  contract for the read model and imports neither `spec` nor `registry` — and it avoids two Go spellings for
  one operator-signed waiver, which R9 forbids.
