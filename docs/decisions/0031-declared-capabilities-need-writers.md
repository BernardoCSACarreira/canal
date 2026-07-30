# 0031 — Every declared capability must have a reachable writer and an end-to-end conformance test

**Status:** accepted, normative. This is the generalisation the hostile-connector pass produced, and it
constrains every future capability.

## Context

Eight adversarial connectors produced 23 fatal breakages. They fell into recognisable families (architecture
§30.1), but one shape recurred across families and is worth naming as a rule, because it is the most
expensive defect shape an interface set can contain and it is invisible to every form of review that is not
"someone tried".

**A declared capability with no reachable writer.**

The canonical instance: `SourceCaps.StableKeys`. It existed. It was documented, in detail, with a rationale
citing design rule R5. It was cross-checked at registration — registration *lint failed* a source that
declared it with empty `Notes`, enforcing documentation of the key derivation. It was read by three separate
negotiation rules. `SinkCaps.RequiresKey` was refused against a source that did not declare it.
`Request.IdempotencyKey` was documented as "present when the source declares StableKeys". `record.Ref.Key`
carried it to every per-record outcome. Both dedupe layers keyed on it. `EffectivelyOnce` required it.

**No connector could ever set it.** `record.Origin.Key` was a field of an unexported struct with no mutator,
and `Origin()` returned a copy. Seven declared capabilities rested on a value with no writer, and the whole
edifice type-checked, documented itself, negotiated correctly, and appeared in the read model.

The second instance is the schema-drift subsystem. The core tracked changes into the checkpoint atomically
with cursors, quiesced affected streams before applying a change, negotiated `SinkCaps.SchemaChanges`
against a five-mode `DriftPolicy`, and exposed drift as a first-class `EventDrift`. There was no interface
through which a source could report that a column had appeared. **A complete consumer with no producer.**

The third is smaller and the same: `SourceCaps.MidLaneResume`, `Heartbeater`'s role in holding a gated tail
lane, and `LaneSpec.StartAfter`'s cluster-wide gate were all present and correct, while the tail lane's
starting position — the high watermark taken when the scan finished — had nowhere to be written, because
`LaneSpec.Spec` is write-once and authored before the gate opens.

## Decision

**Two obligations, both on the conformance kit (ADR 0023), both enforced in CI.**

**1. Reachability.** For every field in every `Caps` struct and every optional interface, the conformance kit
must contain a connector that *declares it* and a test that *observes its effect end to end*. Not a unit test
of the field's presence — an end-to-end observation: the record reaches the sink with the key set, the
dedupe drops the duplicate, the schema change reaches `ApplySchemaChange`, the throttle survives more than
`MaxAttempts` returns.

A capability nothing exercises is a capability nothing validates.

**2. Producer-consumer symmetry.** Any subsystem the core *consumes* must have a declared, tested producer
before the consuming code merges. Concretely: no `Caps` field, `spec` field, negotiation rule, checkpoint
field, metric, condition or read-model field may reference a value unless there is a named interface method
through which a connector supplies it, and a conformance test that supplies it.

These are stated as rules on the *kit* rather than as review guidance because review demonstrably does not
catch this. The capability set had been reviewed repeatedly. It took eight people writing real code against
it, and the compiler, to find seven capabilities resting on nothing.

## The cheaper heuristic, for when a full test is not yet written

Three questions, answerable in one minute, that would have caught every instance above:

1. **Who writes this?** Name the exported method. If the answer is "the source sets `Origin.Key`", check that
   a setter exists.
2. **Who reads it, and would they notice if it were always the zero value?** `StableKeys` with a nil key
   produces a nil `IdempotencyKey`, a dedupe that deduplicates nothing, and an upsert with no key — all of
   which fail *silently and correctly-looking*.
3. **Is the producer on the same side of the boundary as the declaration?** A source-declared capability
   needs a source-side writer. `ProducesSchema` was declarable by a source that had no channel to produce a
   schema.

## Consequences

- ADR 0023's conformance kit grows a per-capability matrix and a CI check that every `Caps` field appears in
  it. A new capability with no kit entry does not merge.
- `pkg/connectortest` exists partly to make this affordable: the kit's connectors need runtimes, and
  hand-written fakes were what made five stress packages break on every runtime addition.
- The audit is retrospective as well as prospective: every capability present at the time of this ADR has
  been checked for a reachable writer, and the ones that lacked one are the subject of ADRs 0025 through
  0030.
