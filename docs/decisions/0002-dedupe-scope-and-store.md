# 0002 — Dedupe key scope, backing store, and commit-after-write ordering

**Status:** accepted, normative. Closes `design-rules.md` open decision 2.

## Context

R5 records two distinct bugs in one code path of the abandoned attempt, plus a third that made the
observable behaviour match neither documented semantics:

1. the seen-set was keyed on the bare event `id`, so two connectors or two tenants emitting `1, 2, 3` would
   silently discard each other's events;
2. the id was recorded as seen **before** the append ran, so any failure in between made the event
   permanently unresubmittable — the mandated retry with the same id returns "duplicate" and the event is
   lost behind a healthy-looking `202`;
3. the set was a 50,000-entry process-lifetime FIFO while the product documentation described deduplication
   over a "platform retention window".

The prior art offers no usable primitive. Benthos's `Cache.Add` documents that *"it is okay for caches to
return nil on duplicates if it isn't possible to implement"* — the one primitive that could underpin
exactly-once is explicitly permitted to be unreliable.

One of the reviewed proposals placed dedupe in a transform, and its own reviewer found that unimplementable:
a transform returns immediately and has no channel through which to observe settlement, so it *cannot* mark a
key seen after the write.

## Decision

**1. Dedupe is a property of a sink node, configured per stream, implemented by the engine.** Not a
transform, not a connector concern. The engine owns settlement, so only the engine can order the mark
correctly.

**2. The key scope is fixed and not configurable below it:**

```
(tenant, pipeline, source-node, stream, layer, identity)
```

`layer` is `upstream` (the vendor's own id, `Origin.Upstream`) or `key` (canal's canonical identity,
`Origin.Key`). Nothing is ever keyed on `RecordID`, which is generation-local by construction and never
persisted.

**3. `DedupeConfig.Window` is required and has no default.** A window is what makes "duplicate" mean
anything, and its absence is bug 3 above. `Build` refuses a dedupe config with a zero window.

**4. The ordering, which is the whole point:**

```
a  look up the key
b  present -> drop, count canal_records_deduped_total{layer}, settle Delivered
c  absent  -> write to the sink
d  ONLY after the sink confirms durability, and ONLY in the same atomic durable
   write that advances the lane cursor, record the key as seen
```

A failure anywhere between (b) and (d) leaves the key **unseen** and the record resubmittable. "Duplicate"
means "already durably stored", never "present in a RAM cache that may have evicted it".

**5. The backing store is `store.StateStore` under the reserved key space
`dedupe/<tenant>/<pipeline>/<node>/<stream>/<layer>/`, with strict per-key CAS.** No fifth store interface
is introduced. Using the same store is what makes step (d)'s atomicity with the cursor possible at all.

**6. Trimming is part of the same write.** Entries older than `Window` are deleted in the write that
advances the cursor, so the store cannot grow without bound and the trim cannot diverge from the progress it
protects.

**7. The store is injected, never module-global.** The abandoned attempt's TypeScript edge had a
module-scope set that leaked across every app instance and forced tests to randomise ids to pass.

**8. Three layers of idempotency, each with one home.** Layer 1 is `Origin.Upstream`, layer 2 is
`Origin.Key`, layer 3 is the engine-derived `Request.IdempotencyKey`. A source with no natural upstream id
**must** derive a deterministic layer-2 key from stable fields and document the derivation in its registered
`Notes`; declaring `StableKeys` with empty `Notes` fails registration lint.

## Alternatives rejected

- **A cache interface.** Rejected on the quoted evidence: a primitive permitted to be unreliable cannot
  underpin a correctness property.
- **A dedicated `DedupeStore` interface.** Rejected because it would be a fifth store interface (violating
  the four-interface rule of ADR 0021) and, worse, a *separate* durability domain — which would make step
  (d)'s atomicity with the cursor impossible and reintroduce the two-store divergence class of failure.
- **Dedupe as a transform.** Rejected as unimplementable: no settlement channel.
- **Dedupe at the source.** Rejected: a source cannot know whether the record was written, and it would make
  every source reimplement the same store.
- **Keying on `RecordID`.** Rejected: it is generation-local, so run 2's ids collide with run 1's and a new
  poison record is dropped as a duplicate — a defect one proposal shipped.
- **A default window.** Rejected: any default is a lie about the operator's retention semantics, and the
  absence of a window is exactly R5's third bug.
- **Bloom filters or another probabilistic structure.** Rejected for v1: a false positive is silent data
  loss, and the failure is unattributable.

## Consequences

- Positive: R5 is closed structurally rather than by discipline; "duplicate" has one meaning; the dedupe set
  is bounded and its trim is atomic with the progress it guards.
- **Negative, accepted:** dedupe costs one store read per record on the write path for streams that enable
  it. Mitigated by it being opt-in per stream and by an in-memory read-through cache in front of the store
  whose misses are the only correctness-relevant path.
- **Negative, accepted:** the dedupe set shares a durability domain and a write path with the checkpoint, so
  a very large window makes each phase-two write larger. Measured, and the window is the operator's knob.
- **Negative, accepted:** a source that cannot supply either identity layer cannot use dedupe at all. The
  capability report says so with a reason, and `EffectivelyOnce` is refused for it.
