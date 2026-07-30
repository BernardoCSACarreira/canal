# 0025 — Record identity is writable by the source; settlement identity is not

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

`record.Origin` was designed to be unforgeable. The reasoning was sound and is preserved: Kafka Connect's
KIP-793 retrofit exists because its SMTs rewrite topic and partition while offset accounting needs the
pre-transform coordinates, so `originalTopic`, `originalKafkaPartition` and `originalKafkaOffset` had to be
bolted on plus prose warnings in two javadocs. canal's answer was that a transform has no access to the
fields settlement uses, because `Origin` is unexported, stamped once by an `Allocator`, and `Origin()`
returns a value copy.

The design then applied that rule to the whole struct, including two fields that are not settlement
identity at all:

- `Origin.Key` — the source-derived stable identity of the thing a record is about, canonically encoded.
- `Origin.Upstream` — the vendor's own id, carried verbatim, idempotency layer one.

**Six of eight hostile connectors were blocked on this, all six FATALLY, and one of them proved it with a
compiler error in its own test file.** `r.Origin().Key = k` does not compile (`Origin()` returns a value);
`o := r.Origin(); o.Key = k` compiles and silently no-ops, which is worse.

The consequence is the part worth recording. Seven declared, documented, negotiated capabilities rested on
a field with no writer:

| Capability | What it required |
|---|---|
| `SourceCaps.StableKeys` | `Origin.Key` populated and stable across re-reads |
| `SinkCaps.RequiresKey` | every record carries `Origin.Key` |
| `spec.DedupeKey` | dedupe layer two keys on `Origin.Key` |
| `spec.DedupeUpstream` | dedupe layer one keys on `Origin.Upstream` |
| `Request.IdempotencyKey` | derived by the engine, "present when the source declares StableKeys" |
| `record.Ref.Key` | the identity a sink reports outcomes against |
| `connector.EffectivelyOnce` | requires stable keys so duplicates collapse |

Registration lint *enforced* that a source declaring `StableKeys` documents its key derivation in its
`Notes`. It enforced documentation of a thing no source could do.

## Decision

**`(*record.Record).SetKey(k []byte)` and `(*record.Record).SetUpstream(u []byte)`, in the spine, under
`SetHandle`'s existing contract.**

They are legal only from the source that produced the record, before that source returns from `Read` or
`ReadLanes`. The engine rejects a key set later; a transform calling either gets
`fault.PermanentContract`. That is not a new rule — it is `SetHandle`'s rule, with the same enforcement
point.

**The settlement fields stay unwritable.** `Lane`, `Stream`, `Group`, `ID`, `Root`, `Parent`, `Parents`
and `refs` have no mutator and never will. `Origin()` still returns a copy. The line
`internal/stress/parallel-snapshot/proof_test.go` draws is exact: it reflects over `*record.Record` and
fails if `SetLane`, `SetGroup`, `SetRefs`, `SetID`, `SetOrigin` or `SetStream` ever appears.

**Two methods, not one `SetIdentity(key, upstream)`.** One of the eight proposed the combined form. A
source with a vendor id and no derivable canonical key — and one with the reverse — must not be made to
pass `nil` for the half it does not have, because a `nil` meaning "leave it alone" and a `nil` meaning
"clear it" are indistinguishable. That ambiguity is worth two methods.

**`Origin.Stream` is NOT given a setter.** Per-record stream targeting is real (a shared log interleaving
many tables under one cursor) and it is served by `(*record.Batch).AddFor(stream)` — a parameter at the one
moment identity comes into existence, not a post-hoc mutation. The distinction is the whole decision: a
source may *choose* identity while creating a record; nobody may *change* it afterwards.

## Alternatives rejected

**Carry the key in `Meta` under a reserved namespace.** Three connectors tried this as a workaround and
all three documented why it fails: `Ref.Key`, `Request.IdempotencyKey` and the dedupe layers read
`Origin.Key`, so the core never sees it. Every capability above would need re-plumbing to a magic metadata
name — precisely the convention `schema/doc.go` condemns.

**Pass the key to `Add`**, as `Add(key []byte)`. It breaks every existing `Add()` call, it forces a source
with no key to pass `nil` on every record, and it cannot express "I learn the key after parsing the body",
which is the normal order for a JSON or Avro payload.

**Have the engine derive the key from the schema's declared `Keys`.** Attractive and wrong: it requires the
core to parse the payload, which means the core needs a codec, which is the dependency inversion §9 exists
to prevent. It also cannot work for a source whose key is not a payload field — a queue message whose
identity is a header, a file whose identity is a content hash.

## Consequences

- The seven capabilities above become reachable for the first time.
- A source declaring `StableKeys` and never calling `SetKey` is now a *conformance-kit failure* rather than
  an impossibility, which is the honest place for that error.
- ADR 0031 generalises the lesson: a declared capability with no reachable writer must fail the
  conformance kit.
