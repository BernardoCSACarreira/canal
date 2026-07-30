# 0005 — The canonical record model

**Status:** accepted, normative. Closes `design-rules.md` open decision 5 and satisfies R2.

## Context

The abandoned attempt named a stage `source_canonical_event_serializer` and **never defined what the canonical
form was**. No document specified it and no code implemented it; stages 4, 6, 7 and 8 had zero
implementation. The only record model that existed was a wire-level HTTP DTO, which became the de facto
internal type by default.

R2 therefore requires the envelope to be decided **first**, independently of any transport.

Three facts from the prior art constrain the answer:

- **A domain-free envelope does not hold.** Benthos's MySQL CDC input invents
  `MessageOperation{read,insert,update,delete,snapshot_complete,xid}` and flattens it into a metadata string;
  its Postgres input uses a *different* key set and a *different* op vocabulary. There is no cross-source
  contract, so every CDC-aware sink special-cases every source — constraint #1 violated by drift. Kafka
  Connect's deliberate omission of an operation type is the documented reason every CDC connector reinvented
  the envelope incompatibly, and Airbyte and Singer independently reinvented magic prefixed columns for the
  same missing fields.
- **A typed CDC envelope in core forces a relational shape** onto a webhook, a metrics scrape and a file tail.
- **Identity must be framework-assigned and provenance must be unforgeable.** Kafka Connect's SMTs rewrite
  topic and partition while offset accounting needs the pre-transform coordinates, so
  `originalTopic`/`originalKafkaPartition`/`originalKafkaOffset` had to be retrofitted (KIP-793) plus prose
  warnings in two javadocs.

## Decision

```
Record
├─ Payload   one body, two views (bytes / structured). Conversion is NEVER implicit.
├─ Meta      a separately addressable namespace, reserved with a check. Not sent to a sink by default.
├─ Change    OPTIONAL typed facet: Op, Keys, Before, After, two Completeness fields, TxID, CommitTime
└─ origin    UNEXPORTED provenance, stamped once by record.Allocator. No mutator exists.
```

**Eight load-bearing choices.**

1. **Generic envelope plus an optional typed facet.** The generic path is unchanged and the core never
   switches on source type; sources that can populate op/key/before/after do, and sinks that want them read a
   nil-checkable pointer. Nothing is relationally shaped because the facet is *optional data*, not a subtype.
2. **`Completeness` on **both** images.** Postgres logical decoding with an unchanged TOAST value and a
   `REPLICA IDENTITY` that is not `FULL` produces a partial after-image, and a sink that upserts it writes
   nulls over live data. A sink declares `RequiresCompleteImages` and the pipeline is refused at submit time
   against a source that cannot supply them. This is the only defence in the surveyed field against that
   corruption class.
3. **Provenance is unforgeable by construction.** `record.Allocator` lives in package `record` and is the only
   thing that stamps an `Origin`; `Batch.Add`/`Derive`/`Merge` are the only constructors and a connector never
   holds an `Allocator`. There is no second emitter — one proposal exported one and its reviewer found it
   fatal, because any connector could then emit records colliding on id zero.
4. **`Origin.refs` — one unexported counter — makes every topology correct with no core code path.**
   `Derive` yields refs 1 and adds a group reference; a filter releases one; `Merge` sums its parents'. Fan-out,
   filtering, 1→N expansion and N→M regrouping all settle correctly, and early settlement is structurally
   impossible.
5. **Conversion is never implicit and `Payload` holds no codec.** `Bytes() ([]byte, bool)` and
   `Structured() (Value, bool)` are pure accessors. Three of the four reviewed proposals specified a payload
   whose `Bytes()` materialises "using the pipeline's configured encoder", which requires either global mutable
   codec state or an import cycle and makes a record untestable in isolation. The engine's decode and encode
   nodes do the conversion.
6. **`Meta` is a slice of triples, not a map, and it is deep-copied on every derivation.** A map inside a
   value that gets copied on fan-out is shared mutable state, and its failure is a concurrent map write — an
   unrecoverable process-wide fatal error. A typical record carries zero to five entries, so a slice is also
   smaller and faster.
7. **`Value` is a sealed sum with a closed member set and no stream member.** Sealed so a third party cannot
   widen it (which would be a checkpoint break and a codec break at once). No lazily-read nested stream,
   because a record must be encodable, bufferable, dead-letterable and wire-shippable at every instant — a
   stream member makes all four impossible and two proposals shipped one.
8. **`Position` is opaque bytes plus core-readable scalars plus two optional facets.** `Order` (an
   order-preserving byte encoding) and `Scalar` (a monotone numeric projection) make comparability *data*
   rather than a *method*, which is the only form that crosses a process boundary or reaches a browser.
   `Label` is a connector-authored display string the core renders and never parses. `Safe` marks a legal
   resume point, so transaction-boundary correctness is a core invariant instead of per-connector lore.

**Namespaces are reserved with a check, not a convention** (`canal` read-only, `source`, `sink`, `user`, plus
an unlisted secrets compartment). Conduit shipped six source-shaped keys into a connector-agnostic spec
namespace precisely because nothing enforced it.

**`nil` and `Null{}` are different facts** and canal never collapses them. **The empty string does not delete
a metadata key**, because "present but empty" must be representable.

## Alternatives rejected

- **A domain-free envelope with CDC in metadata conventions.** Rejected on the measured evidence above.
- **A typed CDC envelope as the core record.** Rejected: constraint #1.
- **A generic `Facet` registry with third-party facets.** Rejected: one proposal sealed its facet interface
  with an unexported method while advertising third-party facets, which its reviewer found fatal. There is
  exactly one facet, it is a plain struct field, and third-party extension is `Meta`.
- **A type parameter on `Record`.** Rejected: it propagates to `Source`, `Sink`, `Buffer`, `Transform`,
  `Codec` and the registry and must then be erased at the registry boundary. FLIP-191 needed a whole new
  package because a plugin interface had type parameters.
- **`Batch.Records []Record` (values).** Rejected: measured at ~500 bytes per record, copied per range
  iteration, and it is how a reference field ends up shared between fan-out branches.
- **A `Copy()` that preserves `RecordID` for fan-out.** Rejected as fatal: two branches settling becomes
  indistinguishable from one double-settling.
- **An untyped JSON object as the only payload option.** Rejected: Airbyte deferred the type problem to
  dbt-based normalisation, which became the worst part of the product and was eventually deleted.
- **A comparator method on `Position`.** Rejected: it works in-process and cannot cross a boundary.

## Consequences

- Positive: the envelope is decided first and no transport can become it; a transform cannot corrupt
  settlement identity; every topology is correct with no core branch; the core can report progress for an
  arbitrary connector without parsing anything.
- **Negative, accepted:** `Origin` is roughly 150 bytes per record. Mitigated by the `Allocator` holding the
  fields that are constant per lane, so only the varying ones are per-record.
- **Negative, accepted:** `Value` boxes structured fields on the heap. Mitigated by bytes being first-class:
  a pass-through pipeline never materialises a structured view.
- **Negative, accepted:** a sink author must handle "the facet is absent" and "the image is partial". That is
  the point — the alternative is a silent hole.
- **Negative, accepted:** `Order` puts a real burden on a connector author who wants comparability, and a
  wrong implementation fails subtly. Mitigated by it being optional, by nothing deriving it automatically, and
  by a conformance case that asserts it agrees with emission order over a generated sequence including the
  `"9" > "10"` case.
