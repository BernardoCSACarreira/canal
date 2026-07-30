# canal — the decision space: Conduit addendum

**Status: draft, analytical. Not normative.** Per `docs/design-rules.md` R12 this document is draft and nothing
in it is citable as a MUST. It is an **addendum to `docs/research/_decision-space.md`** and must be read
alongside it: that document enumerated canal's fourteen architectural forks from nine dossiers while the
ConduitIO dossier was blocked by a network outage, and it said so — its evidence table rates `conduit.md` as
"None", and Part 4 open question 1 instructs "re-run that research before freezing interfaces." That research
now exists (`docs/research/conduit.md`, verified against pinned commits). This addendum records **only what
changes because of it**: entries are tagged `[CONFIRMS]`, `[CONTRADICTS]` or `[NEW]`, each names the decision id
it touches, and nothing from the parent document is restated. Conduit is the closest existing system to canal —
Go, source/sink, opaque positions, snapshot-then-CDC, a generated config spec driving a UI, one binary, and an
in-process plugin boundary that a gRPC subprocess already satisfies unchanged — so where it diverges from the
consensus the parent document recorded, the divergence is treated as evidence rather than noise. Section
references in the form §N point into `docs/research/conduit.md`.

---

## 0. Meta: what the addendum does to the evidence base

**[NEW] — the evidence table.** `conduit.md` moves from weight *None* to weight **High**: every signature, field
name, metric name and constant in it was read from source at pinned SHAs, and its appendix enumerates what was
*not* read. It is also the only dossier in the set whose subject's own **postmortems, ADRs and design documents**
were read. That gives it a category of evidence no other dossier has: implementation-level regret, dated, with
the fix and the fix's own follow-on bug both visible. Where this addendum reports a trap, it is usually because
Conduit wrote the trap down itself.

**[NEW] — read the SHA, not the project name.** Several of the most load-bearing findings below (the
ack-before-persist sev-0, the deferred-ack delivery goroutine, the partition-claims RFC, the two-engine ADR) are
from 2026-07 and are **not** in the widely-cited `v0.14`-era Conduit. Any future cross-check of this addendum
must pin commits the same way. Also: the record model moved out of the engine repo into `conduit-commons/opencdc`
— an older analysis that puts `Record` in `conduit/pkg/record` is describing a dead layout.

**[NEW] — two live engines.** Conduit's tree contains `pkg/lifecycle` (v1, default, record-at-a-time node
graph) and `pkg/lifecycle-poc` (v2, opt-in batch "funnel"). Several entries below cite v2's internal types as the
*better* shape while noting v1 is what actually ships. Where an entry does not say which, it is v1.

**[CONFIRMS] — Part 4 open question 1 is now closed.** The three decisions it named as most likely to change
(D3, D9, D13) did change, though not in the direction it guessed: D3 and D9 gain mechanisms, and D13's
recursive-composition recommendation gets no corroboration at all from Conduit (§ D13 below).

---

## 1. D1 — the record / envelope model

**[CONTRADICTS] the premise behind option (c).** D1 rejected option (b) — a typed CDC envelope in core — on the
grounds that it "forces relational/document shape onto sources that have neither (an HTTP webhook, a metrics
scrape, a file tail)" and "risks the exact 'Mongo-shaped core' constraint #1 prohibits." Conduit chose (b),
almost exactly: `opencdc.Record` has five public fields — `Position`, `Operation`, `Metadata`, `Key`,
`Payload Change{Before, After}` — with `Operation` and the before/after `Change` **mandatory and top-level, not
an optional facet** (§2). And the feared outcome did not occur. That same envelope is used by `file`, `s3`,
`generator` and `log` connectors which have no CDC semantics whatsoever; they emit `OperationCreate` with only
`After` populated and nothing in the core notices. Four years of production use across CDC and non-CDC sources is
the strongest available evidence that a mandatory four-value operation plus an optional-by-nilability
before/after pair is **not** source-shaped.

This does not overturn D1's recommendation, but it changes its cost model. The real price Conduit pays is not
Mongo-shapedness, it is **unenforced invariants**: nothing prevents `OperationDelete` with a populated `After`, or
`OperationCreate` with a populated `Before`. The de-facto enforcement is a family of SDK constructors
(`Util.Source.NewRecordCreate/Update/Delete/Snapshot`) that populate the right combination, and a connector that
builds the struct literally bypasses all of it (§2.2). D1's `(facet, ok)` accessor buys exactly the enforcement
Conduit lacks — so the fork is now **"mandatory fields with constructor-only enforcement" vs "optional facet with
accessor-enforced presence"**, which is a narrower and more decidable question than the one D1 posed.

**[NEW] two forward-compatibility details to adopt regardless of which side of that fork canal lands on.**
`Operation` is `iota + 1`, so the zero value is invalid-and-detectable rather than silently meaning "create"
(§2.1) — the same trick D1 needs for any closed enum on the envelope. And `UnmarshalText` **deliberately accepts
unknown numeric operations** (`Operation(7)`) rather than erroring, so a record written by a newer producer
survives an older consumer. That is the record-model half of the position-versioning contract in §3.2, and D1 has
no equivalent rule.

**[CONFIRMS] the payload sum type, and shows the exact cost of the missing conversion helper.** D1's
"one payload, two views" plus D1's instruction to "expose one explicit, well-named conversion helper" are both
vindicated. Conduit's `Data` is a **sealed interface with exactly two implementations** closed by an unexported
`isData()` marker — `RawData []byte` and `StructuredData map[string]any` — and consumers type-switch (§2.3). That
is Go's approximation of a sum type and it is the right shape. But Conduit then ships **no conversion at all**:
there is no `AsStructured()`, nobody converts automatically, and a destination needing structure must type-assert
and fail or parse the bytes itself. The measured consequence is a family of three `unwrap` processors
(`pkg/plugin/processor/builtin/impl/unwrap/{opencdc,debezium,kafka_connect}.go`) whose only job is to un-nest a
record that arrived as bytes containing an encoded record (§13.2j). The dossier's own verdict — "each one is a
place where the envelope failed to be self-describing" — makes D1's conversion helper a *requirement*, not a
nicety.

**[NEW] `StructuredData.Clone()` is a trap in miniature.** It recurses into nested `map[string]any` values only;
slices and custom structs are copied by reference (§2.3). A record model documented to be used as a value, with a
`Clone()` that is shallow for some value shapes and deep for others, is a mutation-aliasing bug generator. canal's
D1 must state its clone depth per layer explicitly and test it.

**[NEW] `RawData.MarshalJSON` threads a `ctx` into marshalling** to select base64-vs-string output
(`JSONMarshalOptions.RawDataAsString`), with a hand-rolled encoder because "this is in the hot path" (§2.3). A
`ctx` in `MarshalJSON` is a smell; the presentation choice belongs to the codec (D2), not to the record's
marshaller.

**[CONFIRMS, third independent occurrence] Part 3 trap 1 — provenance must be structurally immutable.** The
parent document called two occurrences (Connect's `original*` retrofit; the general argument) conclusive. Conduit
is a third, and it is the cleanest: `Position` is the **first public field on `Record`**, mutable by any
processor, and v2's `Batch` therefore has to snapshot positions at construction with the comment "Store positions
separately, as we need the original positions when acking records in the source, in case a processor tries to
change the position" (§9.3). A defensive copy inside the engine is what you build when identity is not
structurally protected. D1's "position does not go on the record's public surface" and D13 item 5 are both
confirmed by the workaround Conduit needed.

**[CONFIRMS + sharpens] the two-namespace metadata split, and the enforcement gap.** Conduit's `Metadata` is
`map[string]string` with typed accessor pairs (`GetReadAt() (time.Time, error)`, `SetPayloadSchemaVersion(int)`)
and two reserved namespaces: `opencdc.*` (spec-level, portable) and `conduit.*` (engine-level, provenance and
DLQ) (§2.4). That corroborates Vector's split, which D1 adopted on one unverified dossier. Two additions:

- **The namespaces are not enforced.** A connector can write `conduit.dlq.nack.error` and nothing stops it. D1
  and R9 assume reserved prefixes; Conduit shows that unenforced reservation is a convention, and the engine
  "smuggles data through a connector-owned envelope" through it.
- **`opencdc.file.*` is constraint #1 violated in the wild.** Six reserved keys about *file chunking* —
  `file.name`, `file.size`, `file.hash`, `file.chunked`, `file.chunk.index`, `file.chunk.count` — sit in the
  connector-agnostic spec namespace (§2.4, §13.2g). This is precisely the "source-shaped assumption leaking into
  core" that canal's constraint #1 forbids, and it is the highest-value single datapoint in the dossier for why
  the namespace rule needs teeth. The same organisation got it *right* later in a different repo with
  `postgres.snapshot.resumed` (§4.5) — connector-namespaced, not spec-namespaced. **The rule canal should write
  down: a key enters the spec namespace only if a fully generic source could populate it.**

**[NEW] no timestamp on the envelope.** There is no time field; `opencdc.createdAt` and `opencdc.readAt` live in
metadata as strings, so `GetReadAt()` is **fallible for every record** (§2.4). D1 does not decide whether time is
a typed field or metadata. Conduit's choice makes the most-read temporal value a parse-and-error path on the hot
path; canal should decide deliberately rather than inherit it.

**[CONFIRMS] `opencdc.collection` as the generic "which entity" key.** One string key naming the
table/topic/bucket, in the spec namespace, is the right call and is what schema-subject naming keys off (§2.4,
§5.2). D8's "derive schema subjects from a generic collection key" now has a shipped implementation behind it
rather than an inference.

---

## 2. D2 — is serialisation a connector concern or a separate stage?

**[CONTRADICTS by counterexample, which strengthens the recommendation.]** D2 recommended (a)+(b) — codec,
framer and compression as separately registered pipeline stages, connectors doing transport only. Conduit does
(c): destinations receive `[]opencdc.Record` and each one marshals for itself. **There is no codec stage in
Conduit at all.** The two documented consequences are exactly the ones D2 predicted: the `unwrap` processor
family (§13.2j, above) and `Record.serializer` (below). A system that skipped the codec stage grew per-connector
un-nesting processors and a mutable codec field on the record. That is the negative datapoint D2's argument was
missing.

**[NEW] `Record.serializer` is the concrete failure mode of codec-on-the-record, and it is worse than D2
imagined.** It is a **private, mutable, non-thread-safe field on a value type documented to be used as a value**;
`SetSerializer` mutates the receiver; `Clone()` does not copy it; and `Bytes()` falls back **silently to a
different output format** if the serializer errors (§2.5). Four independent defects in one field. The dossier's
verdict — "put codec selection in the pipeline configuration, never on the record" — is D2's recommendation
restated from the inside. Add it to Part 3 as a named trap.

**[NEW] Conduit has no framing concept whatsoever.** D2's split of encoding from framing rests on Vector
(unverified) corroborated structurally by Benthos's scanner stage. Conduit neither confirms nor denies it: there
is nothing to compare. Record honestly as absence of evidence — the framer half of D2 remains supported by one
verified dossier plus one unverified one.

**[NEW] a one-implementation pluggability layer, labelled as such.** `conduit-commons/schema` has a `Serde`
interface (`Marshal`/`Unmarshal`/`String`), a `SerdeFactory` with both a schema-parsing and a
type-inference constructor, a `KnownSerdeFactories map[Type]SerdeFactory` and an `ErrUnsupportedType` — and
`Type` has **exactly one member: Avro** (§5.1). The abstraction is present and unexercised. This is mild but real
evidence for keeping D2's registry machinery thin until the second codec exists, and for D2's own note that the
codec registry "is a second instance of D-registry machinery, which validates that machinery early" — validating
machinery is worth doing; shipping a factory map with one entry and calling it pluggable is not.

**[NEW] serde caching is a hard performance requirement, not an optimisation.** Serdes are cached
process-globally keyed by **Rabin fingerprint** of the schema bytes, with `AutoCleanInterval(1h)` and
`MaxAge(4h)`, "because a per-record registry lookup would be fatal" (§5.1, §5.3). D2 and D8 both assume a
schema-aware codec; neither states that the codec must be resolvable from a cheap content hash on the hot path.
It must.

---

## 3. D3 — checkpoint ownership and representation

**[CONFIRMS, and supplies the best available wording for the rule.]** D3's "the core never requires positions to
be comparable" is Conduit's central checkpoint decision, enforced and documented. `Position` is `[]byte`; the
**only** things the engine ever does with one are `bytes.Equal` (matching an ack to its record), store it, hand it
back to `Open`, and `string(p)` for log lines and message ids (§3.1). It never parses, orders, or
compares-for-greater-than. And when the engine genuinely needed ordering over positions, it minted its own
counter rather than imputing structure to the bytes:

> `nextAckSeq` is a purely engine-internal, monotonically increasing counter assigned to each `Ack` call — NOT
> derived from the opaque connector `Position` bytes, which `Source` cannot generically parse or compare (a
> `Position`'s structure, e.g. per-partition offsets, is entirely connector-defined).

That is D3's rule, shipped, with the reason stated at the enforcement site. canal should copy the *comment* as
well as the mechanism.

**[NEW] — this closes `design-rules.md` open decision 9 (checkpoint format compatibility across binary
upgrades), which D3 raised and did not answer.** D3 specifies `VersionedBlob{Version, Bytes}` and a
`Serialiser[T]` handed "the version it was written with", but never states the *policy* on an unrecognised
version. Conduit's Postgres connector states a complete four-part contract (§3.2):

1. **Additive-only JSON** with every new field `omitempty`, so a position written by a newer connector stays
   structurally readable by an older one (which ignores unknown keys).
2. **A `version` field where absent/0 means legacy**, and legacy must be treated as "none of the newer fields
   were recorded — behave exactly as the previous version did" until a later event naturally populates them.
3. **Never reject a position whose version is greater than the current one.** Quoted rationale: "the format is
   additive-only, so a newer position stays structurally readable, and rejecting it would break the 'readable by
   N+1 versions' rule." This is the rule that makes a binary **downgrade** survivable, and it is the part D3
   omits entirely.
4. **Stamp the version at serialize time, not construction time**, so a parsed legacy position is silently
   upgraded on first write (`p.Version = CurrentPositionVersion` inside `ToSDKPosition()`).

Backed at engine level by a stated invariant: *"Position serialization changes require a versioned migration path
and an upgrade test."* D3 should adopt all four plus the test requirement.

**[CONTRADICTS] the two-level requirement — and the contradiction is informative in both directions.** D3
declares two-level checkpointing "not optional": a shared position for the one log, plus independently
committable per-stream positions, because "a model with only a global blob cannot express independent per-stream
progress." Conduit has **one composite blob per connector** and nothing else. Postgres encodes into it a mode tag
(`Type ∈ {TypeInitial, TypeSnapshot, TypeCDC}`), a **per-table map** of `{last_read, snapshot_end}`, the CDC
`LastLSN`, and a `snapshot_low_watermark_lsn` — a whole state machine inside a single `[]byte` (§3.1).

Two things follow, and they pull opposite ways:

- **Traps 4 and 5 are independent, and Conduit proves it.** The parent document treats "snapshot has no state"
  (trap 4) and "smuggling phase into the opaque checkpoint" (trap 5) as companions, because Benthos and Airbyte
  committed both together. Conduit commits trap 5 fully and **dodges trap 4 completely**: its snapshot is
  resumable per table, from a per-table `last_read`, with no side state and no second store (§4.5). So
  resumability does not require the core to understand the checkpoint. It requires only that the connector write
  cumulative progress into the blob on every record.
- **The price is exactly what D3 predicted, and it is paid in full.** Because the engine cannot read the blob, it
  cannot report phase, cannot report snapshot progress, cannot compute lag, and cannot re-parallelise. Conduit's
  metrics surface (§11.1) has no phase, no progress and no record count. D3's typed header is therefore confirmed
  as the *minimum* the core needs to serve canal's frontend goal — but the two-level decomposition is confirmed
  as an **availability/observability** requirement, not a correctness one. That is a weaker justification than
  D3 currently claims, and stating it accurately matters because D3 spends real complexity on it.

**[CONFIRMS] one store, one write, atomic by the DB's transaction — with a caveat D3 did not consider.** The
position is **not** a separate offset store: it is a field inside the serialised connector-instance record
(`SourceState{Position}`), written with the rest of the instance in one `db.NewTransaction` → `tx.Commit()`, over
a pluggable `database.DB` with `badger`/`sqlite`/`postgres`/`inmemory` backends (§3.3). Destinations have their
own `DestinationState{Positions map[string]Position}`. This is D3's consequence ("a real checkpoint store
interface with atomic multi-key writes") satisfied by the cheapest possible means. The caveat: **checkpoint and
configuration then share one durability domain and one record**, so a config write and a position write contend,
and a corrupt instance record loses both. D3 should state whether that coupling is intended. Also note
`Instance.State` is typed `any` and type-asserted at every use (`s.Instance.State.(SourceState)`) — a smell canal
should not copy when it has generics.

**[NEW] the persister is a debouncing batcher, and its ack-ordering algebra is the contiguous-prefix resolver in
miniature.** `Persist()` enqueues into an in-memory batch that flushes on `DefaultPersisterDelayThreshold = 1s`
**or** `DefaultPersisterBundleCountThreshold = 10000` items, whichever first, inside one transaction, with
callbacks fired in goroutines after commit and tracked by a `WaitGroup` (§3.4). D3's "double-buffered flush"
recommendation gains a second, simpler precedent. More valuable is the invariant that makes it safe:
`durableAckSeq` only ever advances and `pendingAcks` drains **from the head up to it**, so out-of-order flush
confirmations are safe no-ops rather than double-sends or gaps — and this works *because* `SourceState.Position`
is cumulative and monotonic, so whichever flush lands covers every earlier sequence number. That is a 20-line
version of the ~200-line resolver D3 says canal must own, valid only under a cumulative-position model. Worth
recording as the degenerate case: **if positions are cumulative, the prefix resolver collapses to a monotonic
counter comparison.**

**[NEW] what one-position-per-connector actually forbids.** Restart is: read `SourceState.Position`, pass it to
`Open`. No log replay, no WAL of in-flight records, and **no "replay from checkpoint N−3" facility of any kind** —
the engine keeps exactly one position per connector (§3.5). Anything read-but-not-durably-acked is re-read from
upstream. D3's checkpoint carries a monotonic `CheckpointID` and a `Committables map[CheckpointID][]...`, which
implies retained history; Conduit shows that the simplest model is viable and names precisely what it costs.
canal should decide whether checkpoint *history* is a requirement, because D3 currently implies it without
arguing for it.

---

## 4. D4 — commit timing, and the sev-0 the decision space has no entry for

This is the single most important section of the addendum. D4's model of commit timing is **two-phase**: the sink
confirms durability, and the checkpoint advances. Conduit shipped that model, and it was a **confirmed sev-0
data-loss mechanism** that survived to 2026.

**[NEW] the ack-before-persist sev-0 (§3.4, §13.2c).** Until 2026-07, `Source.Ack` sent the ack to the connector
plugin and *then* enqueued the durable position write. From the postmortem:

> For a plugin whose upstream **prunes** data once it is told to commit (Postgres logical replication slots:
> acking advances `confirmed_flush_lsn`, which frees WAL for recycling), a crash inside that window causes
> Conduit to resume, on restart, from a durably-persisted position that is now _behind_ data the upstream has
> already discarded — a structural, unrecoverable gap.

D4 governs the *downstream* half of the handshake ("the checkpoint advances only when the sink has confirmed
durability for every record preceding it"). This bug is in the **upstream** half: telling the source it may
release data before canal's own record of that position is durable. The parent document treats "advance the
checkpoint" and "tell the source" as one action; they are two, and the ordering between them is a correctness
property. **The commit protocol is three-phase, not two:**

1. sink confirms durability → 2. canal's position write is confirmed **durably flushed** → 3. only then is the
   upstream told it may advance/prune.

Conduit's fix ("Approach A") does exactly this, and the enforcement-site comment is worth transplanting verbatim:
*"a plugin's own upstream commit — e.g. a Postgres replication slot's `confirmed_flush_lsn` advance, which frees
WAL for recycling — must never be triggered before Conduit's own crash-recoverable record of that position exists
on disk... Do not reintroduce a synchronous `stream.Send` here without re-reading that design doc."* The stated
cost is the whole trade: "the plugin only learns about it once it's true, which delays a pruning upstream's
WAL/log retention release by up to one debounce interval — bounded, tunable."

**[NEW] the mechanism the fix needed, which D4 should design in rather than retrofit.** The deferred upstream ack
is handed to a **dedicated per-source goroutine** (`deliverDeferredAcks`) rather than sent inline from the
persister callback, "because a slow plugin would otherwise block the **process-wide** flush cycle" — with a
bounded, explicit retry policy (`DefaultDeferredAckMaxRetries = 12`, backoff 10 ms → 500 ms cap) and a
`tearingDown` flag to suppress escalation during shutdown (§3.4). D4's recommendation list has nothing about
isolating the upstream-notification path from the flush path. It must.

**[NEW] — the reason the sev-0 survived is a D9 requirement, and it is the most transplantable lesson in the
dossier.** The guarantee depends on a property of the **upstream** that the interface does not express. Postgres
replication slots prune on commit; Kafka, MySQL binlog and Mongo change streams are retention-based and do not.
**The same engine code is a data-loss bug for one class of source and completely benign for the other**, and
nothing in `Source` distinguishes them — the postmortem had to classify each connector by reading its source
code. D9's declared-capability struct has nothing about ack semantics. It needs something like
`AckDrivesIrreversibleUpstreamCommit bool` (or a three-valued `UpstreamRetention ∈ {prunes-on-commit,
retention-window, unbounded}`), because:

> if a guarantee depends on connector semantics, the interface must let the connector declare those semantics.

That single sentence generalises past this bug and is arguably the best one-line statement of D9's purpose in the
entire evidence base.

**[CONFIRMS] D4 item 4 — "acknowledge before durable requires the sink to *lie about a return value*."**
Conduit's destination side is exactly this: `Write(ctx, records) (n int, err error)` **is** the ack; the SDK
adapter derives acks from the return and there is no `Ack` method on `Destination` at all (§1.2, §9.3). The
asymmetry with `Source` (which does have `Ack`) is deliberate. R4-as-a-type works.

**[NEW] position-driven drain on shutdown — a mechanism D4 and D14 both lack.** The source's `Stop` returns the
`LastPosition` it emitted; the engine passes that to the destination's `Stop`; the destination **blocks until it
has seen exactly that position**, then flushes (`lastPosition.Watch(ctx, func(v) bool { return bytes.Equal(v,
req.LastPosition) })`) (§6.2). Deterministic drain without counting records, using only `bytes.Equal` on opaque
bytes — so it composes with D3's opacity rule. Both waits are bounded by a one-minute `stopTimeout` (worst case
two minutes), which is a package var carrying `TODO make the timeout configurable`. Steal the mechanism; make the
timeout config from the first commit, and make *drained* vs *drain-timeout* a distinct event as D14 already
requires.

**[NEW] the ack-ordering semaphore and the broken-latch.** Acks reach the source in read order even though they
complete out of order downstream, via a **ticket taken at enqueue time (in pipeline order) and acquired at ack
time** — a semaphore, not a channel. And a single ack failure **latches the node broken** (`fail bool`, "at that
point we completely stop processing acks/nacks"), because after one out-of-order ack the position guarantee is
void (§8.3). "That latch is a good pattern: it converts a subtle correctness violation into a loud stop." D4 and
D12 both need this and neither names it.

**[CONFIRMS] the fail-loud default.** Conduit's DLQ circuit breaker defaults to `WindowNackThreshold: 0` with
`WindowSize: 1` — **the first nack stops the pipeline** — and the default "DLQ destination" is a log line
(§9.4). The dossier's judgement, which matches R4 and D11 item 4: "that is the right default for a data tool:
fail loudly rather than silently discard."

---

## 5. D5 — the connector/task split and how work is partitioned

**[NEW] a third option D5 does not enumerate: reserve the vocabulary, build nothing.** D5's option list is
(a) one-shot planning, (b) enumerator+reader with splits, (c) no split concept. Conduit chose (c) **deliberately,
with a written ADR** — and then shipped the *wire vocabulary* for partitioning years before any consumer exists.
The procedural insight from `20260704-single-node-engine.md` is the part canal needs: *"The connector protocol
must carry a partition-claim concept earlier than the scheduler needs it, to avoid a later breaking change."* The
resulting RFC (`20260723-partition-claims-protocol.md`) defines only four things: what a partitionable unit is per
connector kind, how a source *declares* its units additively, how a *future* scheduler would consume the
declaration, and "the failure modes this seam must survive with **zero scheduler code running**" (§12.3).

This speaks directly to D5's strongest claim — that "adding a partition concept later is a **breaking interface
change**, so it belongs in v1 even if the first coordinator is 'one worker, all splits'." Conduit agrees with the
diagnosis and offers a cheaper treatment: reserve the vocabulary in v1, defer the machinery indefinitely. That is a
materially lower-cost path to the same non-breaking future, and D5 should cost it explicitly before committing to a
full enumerator/reader implementation in v1.

**[NEW] `MODE_EXTERNALLY_MANAGED` is a gap in D5, not a detail.** The capability enum is
`MODE_UNSPECIFIED | MODE_STATIC | MODE_DYNAMIC | MODE_EXTERNALLY_MANAGED`, mapped per connector kind: Kafka
consumer-group source → topic-partition, externally managed; Postgres CDC → table, static; File/S3 → object-prefix
shard, dynamic; `generator` → none, unspecified (§12.3). D5's model assumes canal's enumerator always plans. For a
source whose assignment belongs to a broker that already solves it, **an enumerator that plans is actively wrong** —
the dossier calls this "the humility slot... Without that mode, a partition-aware engine would fight the broker."
D5 has no way to express "units exist, but someone else assigns them," and every real deployment that includes a
Kafka-family source needs it.

**[NEW] `MODE_STATIC` vs `MODE_DYNAMIC` is the re-enumeration licence.** Static means units are fixed at
`Configure` time; dynamic means they can change at runtime and the engine must re-poll. D5 discusses dynamic
work-stealing and `SplitsAssignment` being "always incremental" but never makes *whether the unit set may change*
a declared property. It should: it is the difference between a one-shot plan and a reconciliation loop, and it is
knowable at config time.

**[CONFIRMS] the four properties of the seam are exactly D9's pattern applied to D5.** Zero value =
today's behaviour (`MODE_UNSPECIFIED = 0`, and empty `claimed_units` = "claim everything", preserving
single-instance behaviour for every unspecified connector *and* for any engine build predating the RFC).
**Per-unit position is the same opaque-bytes contract, just scoped smaller** — the position model scales down from
connector to unit with no new concept, which is a genuinely strong argument for D3's opacity rule. And the Go-side
declaration is an **optional interface, type-asserted, never a required method**:

```go
// A Source that does not implement it is treated as MODE_UNSPECIFIED automatically
// -- zero behavior change, zero required migration.
type PartitionAwareSource interface {
    Source
    DeclareClaimableUnits(context.Context) ([]PartitionUnit, error)
}
```

with the type assertion happening host-side once in `NewSourcePlugin`, and the RFC naming Go's own precedent
(`io.ReaderFrom`, `sql.Scanner`). This is D9's rule shipped and is the best-documented instance of it anywhere in
the evidence base — see §D9 below.

**[CONFIRMS] `epoch` as the fencing token, with an honesty caveat D5 and D14 should copy.**
`PartitionClaim{unit_id, position, epoch uint64}` reserves the fencing field, and the RFC explicitly flags `epoch`
as **not yet decided** — committing to the field's shape while refusing to commit to a fencing implementation.
"Reserving a field you might not use is cheap; renumbering is not." The RFC is equally honest that reserving a
shape is not proving a design: *"invariant 4 (ordering) holds today for one reason only: the seam is inert until a
consumer exists, not because this RFC has proven a multi-instance implementation correct."* D5's recommendation
list contains eight concrete mechanisms (pull-based assignment, `AddSplitsBack`, lease TTL ~30 s, reassignment
delay ~120 s, `(workerID, epoch)`) presented as settled; this is the discipline for admitting which of them are
reserved-but-unproven.

**[CONTRADICTS] "no split concept ⇒ structurally unable to scale or resume a stateful source."** D5 argues that
every system omitting splits (Benthos, Vector, Airbyte) is structurally unable to scale a stateful source, and
Chain B in Part 2 concludes "choose 'no split concept' and you get Vector." Conduit has no split concept and
nonetheless ships a **parallel, resumable** Postgres snapshot: one `FetchWorker` per table, all fanning into one
channel under a tomb, with per-table resume from the cumulative position map (§4.5). Parallelism is **per-table,
not intra-table** — no key-range sharding of one large table. So the accurate claim is narrower than D5's:

- *Resumable* snapshot does **not** require splits (Conduit).
- *Parallel* snapshot does **not** require splits, up to the granularity of the source's natural object list.
- *Intra-object* parallelism, cross-worker assignment, engine-visible progress and re-parallelisation-at-restart
  **do** require them.

D5's conclusion survives, but the three unverified dossiers it leaned on (Vector, Airbyte, plus Benthos) conflated
"no splits" with "no resumability," and Conduit separates them. Restating D5's justification on the narrower
ground makes it more defensible, not less.

---

## 6. D6 — how is snapshot modelled, and how does it hand off?

**[CONTRADICTS] — Conduit is a fifth option, and it is the closest system's answer.** D6's options are
(a) connector-internal keyed on "is there a checkpoint?", (b) per-stream mode as configuration, (c) named phases
with a coordinator, (d) boundedness-of-splits, (e) reduction annotations making duplicates harmless. Conduit's
answer, in the dossier's own one line: **"snapshot is not a separate pipeline, a separate connector, or a separate
interface — it is an `Operation` value and a mode tag inside the connector's opaque position. The engine knows
nothing about it."** (§4.1).

What the core knows is exactly one thing: `OperationSnapshot` exists as a peer of create/update/delete. There is
**no `Snapshotter` interface, no `SupportsSnapshot()` capability bit, and no snapshot phase in the pipeline
lifecycle.** The dossier calls it "the single highest-value modelling decision in Conduit for canal" and the
reason it works is stated precisely: "a source that has no snapshot concept simply never emits
`OperationSnapshot`, and no core code path changes."

This is close to option (a), with one difference that makes it categorically better and that D6's critique of (a)
misses. D6 rejected (a) partly because Benthos's operation vocabulary is a per-source metadata string with **no
cross-source contract**, so every CDC-aware sink special-cases every source. Conduit's `snapshot` is a **member of
the shared envelope's closed enum**, so a generic sink routes on it without knowing which source produced it —
`Util.Destination.Route(...)` takes a `handleSnapshot` callback alongside the other three (§4.1). Option (a)'s
fatal flaw was the *un-contracted* vocabulary, not the *connector-internal* mechanism. Conduit fixes the first and
keeps the second, and gets snapshot-then-CDC for **zero core surface area**.

The cost is precisely what D6 predicted and it is not small: the engine cannot report phase or snapshot progress,
because both live in bytes it may not read (see §D3 above, and §11 below where the metric set has neither). So the
fork D6 must actually decide is now sharp and narrow:

> Does canal's frontend goal justify a phase concept in the core, given that correctness demonstrably does not
> require one?

D6's recommendation 3 (phase as a small closed field in the checkpoint header, consumed **only** by progress,
status and metrics, never control flow) is the answer that gets both, and Conduit is the counterfactual that
proves the two halves are separable. Keep it — but note that D6's recommendations 1 and 2 (splits-as-mechanism,
per-stream mode pair) are now paying for observability rather than correctness, and price them accordingly.

**[CONFIRMS] Part 0 item 12 — snapshot records use the same envelope, batcher, ack path and sinks.** Conduit is
another instance. Nothing in the data path distinguishes a snapshot record except the `Operation` value.

**[NEW + sharpens] D6 item 5 — capturing the stream position at snapshot start is not sufficient; the
subscription must be *created* first and *started* last.** This is the most operationally valuable new detail in
the dossier for D6. Postgres's `CombinedIterator.Open` initialises the **CDC iterator first**, before the
snapshot, with the comment "This creates (or, on a resume, reuses) the replication slot, so the slot's consistent
point is only known afterwards" — and then does **not** start the subscriber until handoff (§4.3). Three
properties fall out of that ordering, all of which D6 item 5 needs:

- Creating the slot pins a WAL position **and** yields an exported transaction snapshot id (`TXSnapshotID()`),
  which the snapshot workers read under — so the snapshot is point-in-time consistent *with* the CDC start point,
  and nothing between the two can be lost.
- Starting the subscriber only at handoff guarantees the slot's `confirmed_flush_lsn` **cannot advance during the
  snapshot** — i.e. the upstream cannot prune the very log range the handoff depends on.
- The switch itself is driven by a **sentinel error inside the read call** (`snapshot.ErrIteratorDone`),
  transparently to both the SDK and the engine, with `useCDCIterator` tearing down the snapshot iterator,
  re-seeding the CDC handler's carry-forward fields, then starting the subscriber (§4.4).

D6 item 5's formulation — "`stream_from` = position captured at snapshot start" — is a read-only observation.
Conduit's is a *durable reservation*: create the thing that retains the log, then read the snapshot under a pin
derived from it. For any source that can prune, the read-only version is insufficient.

**[NEW] the mode tag makes the "never snapshot again" decision free.** Mode selection happens at `Open` from the
position: `willSnapshot := conf.WithSnapshot && pos.Type != position.TypeCDC` (§4.2). Resuming a pipeline whose
position says `TypeCDC` skips the snapshot **entirely, forever, with no extra state anywhere**. D6's phase field
in the header achieves the same thing; the point worth recording is that this is a one-line predicate over two
values (a config flag and a position tag) and canal should not need more.

**[CONFIRMS] D6 item 4 / trap 4 — every phase is checkpointable, and it costs almost nothing.** Every snapshot
record carries the **full cumulative** snapshot progress across all tables plus the CDC watermark; restart
mid-snapshot resumes each table from its own `last_read`; no side state, no second store (§4.5). A 500M-row
snapshot does not restart from zero. This is the concrete refutation of "snapshot has no state" as an economy, from
a system that has no phase model at all — so canal cannot use "we have no phase model yet" as an excuse.

**[NEW] fidelity caveats belong on the record, in a connector-namespaced key.** A *resumed* snapshot cannot
re-acquire the exported transaction snapshot, so its point-in-time consistency degrades — and the connector makes
that **observable on every affected record** rather than silent:

```go
// MetadataSnapshotResumed lets a downstream consumer distinguish a resumed-snapshot record — read
// without the transaction-snapshot pin, whose point-in-time consistency is guaranteed only by CDC
// replay reconciliation, not by snapshot isolation — from a first-run snapshot record.
const MetadataSnapshotResumed = "postgres.snapshot.resumed"
```

Two things to take (§4.5). First, the pattern: **when a connector cannot deliver its normal guarantee, it says so
per record instead of degrading quietly.** That is `design-rules.md`'s honesty principle applied to the data path
rather than the UI, and neither D1 nor D6 nor D11 currently has a place for it. Second, the naming: it is
`postgres.*`, not `opencdc.*` — the correct side of the namespace rule that the same organisation got wrong with
`opencdc.file.*` (§D1 above). A generic-envelope caveat key would have been a constraint #1 violation; a
connector-namespaced one is not.

**[NEW] the snapshot completion gate couples the phase transition to ack liveness — and that coupling caused a
deadlock.** The snapshot iterator will not declare itself done until **every emitted record has been acked**
(`i.acks.Wait(ctx)`, a `WaitGroup` incremented per emitted record and decremented in `Ack`), and CDC does not
start until the snapshot declares done (§4.4). Combined with §3.4's "ack only after durable persist," the boundary
ack becomes a **hard liveness dependency**, and a dropped deferred ack becomes "a permanent handoff deadlock +
silent post-snapshot CDC loss" rather than a benign no-op.

D6 recommendation 6 — "**only start the incremental stream after a complete checkpoint following snapshot
completion**" — is *exactly this coupling*, adopted verbatim from Flink CDC and currently stated as a one-line
rule. Conduit is the warning about what implementing it costs. canal must make the gate wait on **durable
persistence of the position** rather than on a boundary-ack round trip, and must give the wait a timeout and an
escalation path, or it will reproduce the deadlock. See the traps section.

**[NEW] no heartbeat / idle-position-advance mechanism exists in Conduit.** D6 item 10 asks for a way to advance
the durable position with no records emitted, so an idle stream's stored position never falls out of the upstream
retention window, and notes Benthos ships it as a per-connector hack. Conduit has nothing in the engine; the
nearest thing is the deferred-ack fix's stated cost ("delays a pruning upstream's retention release by up to one
debounce interval"), which is the same tension from the other end. Record as absence of evidence: D6 item 10
remains supported by one unverified precedent only.

---

## 7. D7 — is snapshot chunking / parallelism a core concept?

**[CONTRADICTS, and the contradiction should change D7's default rather than reverse it.]** D7 recommends the
full eight-step Flink CDC recipe — key-space chunking with unbounded end chunks, the per-chunk
`LOW`/`HIGH`/`END` Offset Signal Algorithm, a checkpointed splitter cursor, `FinishedSnapshotSplitInfo`
accumulation, a self-retiring stream-phase filter, and a pure `filterOutdatedSplitInfos` reconciliation — as "the
single largest piece of core machinery canal will build." Conduit ships **none of it**. There is no watermark
protocol, no dedup filter, no key-range sharding; chunking is `FetchSize` pagination *within* a table and
parallelism is one worker *per* table (§4.5).

And its production Postgres snapshot→CDC source is nonetheless correct, because it uses a **different consistency
family**: pin an exported transaction snapshot and read under it (§4.3). The watermark-and-reconcile algorithm
exists for sources that **cannot** pin a consistent read snapshot. That distinction is missing from D7 entirely,
and it reframes the decision:

| Family | Requirement on the source | What the core must own |
|---|---|---|
| **Pin-a-snapshot** | an exported/consistent read snapshot spanning the whole scan | almost nothing — order the CDC-create before the scan (§D6) |
| **Watermark-and-reconcile** | comparable positions, splittable key space, replay-from-position | the whole eight-step engine |

So D7's engine should be an **opt-in capability for sources in the second family**, not the default snapshot path
— which is consistent with D7's own gating on `SupportsChunkedSnapshot` / `SupportsReplay` / `PositionComparer`,
but changes what happens when a source declares *none* of them. D7 currently says such a source "does a single
unchunked, unresumable scan and says so." Conduit shows that a source with a pin instead gets a **chunked,
parallel, resumable, stream-concurrent** scan with no core machinery at all. The capability set needs a fourth
member — something like `SupportsConsistentReadSnapshot` — and the unchunked-unresumable fallback should be the
third case, not the second.

**[CONFIRMS] the degradation is real and must be surfaced.** The pin-a-snapshot family's weakness is exactly
where Conduit is honest: a resumed snapshot loses the pin (§4.5, and §D6 above). So the two families are not
ranked — the pin is cheaper and stronger on the first run and *weaker on resume*, where it depends on CDC replay
reconciliation for correctness. D7's recommendation that chunked-snapshot output must be **keyed upserts, not
appends**, checked at submit time, applies to the pin family's resume path too, and for the same reason.

---

## 8. D8 — schema propagation and drift

**[CONTRADICTS] there is no `Discover` at all, and the alternative is a sixth option.** D8 makes
`Discover(ctx, config) (Catalog, error)` a **required** source method and builds the stream picker, drift-as-diff
and per-stream mode selection on top of it. Conduit has no discovery operation, no catalog and no stream picker
data. Its answer is: **infer the schema from the data, per record, in SDK middleware; register it with a schema
service; put only `subject` + `version` in the record's metadata** (§5.1–5.2). `SourceWithSchemaExtraction` is
**on by default** for both payload and key, and the schema itself never travels on the record — it travels by
reference.

As an option for D8 this is *(f) infer-and-register-by-reference*, and it has one large advantage the option list
does not currently contain: **zero connector obligation.** It works unchanged for a webhook receiver, a socket
tail or a metrics scrape — which is exactly the population D8's Part 4 open question 8 worries about ("what is the
minimum viable `Discover` for a source with no catalog? Making it required is a real tax on webhook/socket/metrics
sources"). Conduit answers that question from the other side: the tax can be **zero** if you give up the catalog.
What you give up with it is everything D8 wanted the catalog for — the picker, the diff, the declared
`source_defined_cursor`/`primary_key` constraints, and per-stream mode validation. That is a real fork and D8
should state it as one rather than treating `Discover` as obviously required.

**[CONFIRMS] D8's "derive schema subjects from a generic collection key," with the shipped mechanism and a second
prefix canal will need.** The subject is a config string, optionally prefixed by `opencdc.collection` — from the
field doc: *"If the record metadata contains the field `opencdc.collection` it is prepended to the subject name
and separated with a dot"* — **so a multi-table source naturally gets one subject per table without the
middleware knowing what a table is** (§5.2). That is the generic mechanism D8 inferred, verified. And there is a
second prefix layer: `SourceWithSchemaContext` produces `"<ctx>:<subject>"` so **two pipelines can use the same
subject names without collision.** D8 has no notion of subject-namespace-per-pipeline and will need one the first
time two pipelines share a registry.

**[CONFIRMS] the capability is optional and the metadata contract is single.** A connector that already knows its
schema attaches it directly (`AttachKeySchemaToRecord` / `AttachPayloadSchemaToRecord`, gated on
`WithAvroSchema`); one that does not gets inference. **Two paths, same four metadata keys** (§5.2). This is D9's
"behaviour optional, fact declarative" applied to schema, and it is the shape D8 should adopt whichever way the
`Discover` fork goes.

**[NEW — the most important D8 finding] the drift policy does not exist, and the gap is documented as an
invariant.** Conduit's own `CLAUDE.md` states as data-integrity invariant 6:

> **Schema handling never silently mangles data.** Unknown fields, type mismatches, and drift follow the
> configured policy (halt/DLQ/evolve) — never silent coercion or truncation.

The dossier could not find that knob implemented anywhere it read. Extraction computes a schema per record and
calls `CreateSchema`; the registry assigns a new version if the bytes differ; the record points at whichever
version it got; records with different shapes flow on. **No halt/DLQ/coerce switch, no compatibility mode, no
`SchemaEvolutionPolicy`** (§5.4). The dossier's own conclusion is D8 recommendation 4 reached independently:

> the policy must be a first-class pipeline config with three named outcomes, and it must exist **before the first
> schema-carrying connector ships**, because retrofitting it changes record semantics.

That is confirmation of D8's five-mode drift policy from a system that skipped it and wrote down the regret, and
it is the strongest single argument for not deferring D8 item 4 to a later phase. Two caveats: the dossier flags
that it did not read `pkg/schemaregistry/*` in full, so a compatibility-mode setting could exist there; and
Conduit's aspiration names *three* outcomes (halt/DLQ/evolve) against Flink CDC's five
(`exception|evolve|try_evolve|lenient|ignore`), so the vocabulary size is still canal's to decide.

**[NEW] an invariant that names an unimplemented knob is R12 in a new form.** Invariant 6 sits in a normative
document, cited by the project's own enforcement convention, describing a policy that does not exist. The
enforcement convention is otherwise excellent (§9.1 and §12 below); this is the one place it lies. For canal:
**a normative invariant may only reference mechanisms that exist**, or it is draft — R12 applied to invariants and
not just to documents.

**[NEW — a whole missing seam] the schema service reaches the connector over a *reverse* channel, and D9/D14 must
identify that seam up front.** Out-of-process connectors do not lose schema registration: Conduit serves gRPC
**to** the connector. The subprocess receives a deliberately empty environment
(`cmd.Env = make([]string, 0)`) plus `EnvConduitConnectorUtilitiesGRPCTarget` (the engine's connector-utilities
address) and `EnvConduitConnectorToken` (a **per-connector** auth token), and the SDK swaps its package-level
`Service` for a gRPC client at init (§5.3, §10.3). The builtin path assigns the *same object* the engine uses
internally.

The parent document models the plugin boundary as one-directional throughout: the core calls the connector, and
the Appendix's wire-shippability check tests only that direction. It has no entry for **host services the
connector calls back into** — and schema registration is only the first; secrets resolution, metrics, structured
logging and the D11 error-reporting path are all candidates. The dossier's warning is explicit: *"Any canal design
with a subprocess boundary needs this reverse seam identified up front, because retrofitting host services into a
one-way boundary is painful."* Add it to D9 and to the Appendix as a second interface direction, with
authentication per connector instance from the first commit.

**[NEW] the injection mechanism is a package-level mutable `var Service`.** Global mutable state, "fine for a
one-connector-per-process subprocess, questionable in-process" (§5.3). `design-rules.md`'s "ideas worth carrying
forward" already names dependency injection over module-scope global state as the right choice (from the abandoned
attempt's dedupe store). Conduit is a second occurrence in a system that has both a subprocess mode where it is
harmless and an in-process mode where it is not — which is exactly canal's situation under constraint #3. Inject
host services through the connector's context, never a package var.

---

## 9. D9 — required vs optional surface, and the capability-upgrade mechanism

D9 is the second-highest-leverage fork in the parent document and it is where Conduit changes the most.

**[CONTRADICTS the framing question itself] — Go *does* have a default-method mechanism, and it is
`mustEmbedUnimplementedX()`.** D9 opens with "How does canal add a capability in v3 without breaking every
connector written for v1 — **given Go has no default methods**?" and its option (a) critique says "**Go has no
`default` escape hatch at all**, so (a) is strictly worse in Go than in Java." Conduit has the escape hatch. Every
core interface carries an **unexported, unimplementable-outside-the-package** marker method:

```go
type Source interface {
    Config() SourceConfig
    Open(context.Context, opencdc.Position) error
    Read(context.Context) (opencdc.Record, error)
    ReadN(context.Context, int) ([]opencdc.Record, error)
    Ack(context.Context, opencdc.Position) error
    Teardown(context.Context) error
    LifecycleOnCreated(ctx context.Context, config config.Config) error
    LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error
    LifecycleOnDeleted(ctx context.Context, config config.Config) error

    mustEmbedUnimplementedSource()
}
```

You cannot satisfy `Source` without embedding `sdk.UnimplementedSource`, and that embed supplies a default for
every method. So Conduit can add a **tenth** method and every existing connector still compiles, inheriting the
new default (§1.1, §13.1 item 2). It is the gRPC-generated-service trick applied to a hand-written interface, and
it is a compile-time forcing function rather than a convention.

This materially changes D9. Its absolute rule — "**Never add a method to a required interface**" — was derived
from Java/Scala precedent (Connect's `pluginMetrics()` forcing `catch (NoSuchMethodError)` into the official
javadoc; Flink's five `default`-throwing methods and three Sink API rewrites) on the premise that Go is strictly
worse. It is not. The rule can be relaxed to: **never add a method to a required interface that lacks a safe
default** — and with forced embedding, "safe default" becomes a compile-time-guaranteed property rather than a
hope. Conduit's `Source` has nine methods and the smallest legal source implements four (`Config`, `Open`, one of
`Read`/`ReadN`, `Teardown`); `Ack` is explicitly optional and its default returns `ErrUnimplemented`, which the
adapter swallows (§1.1). One method's default **panics** rather than erroring — `Config()`, the one thing a
connector genuinely must supply.

Two costs to record honestly. The interface is no longer implementable by a bare struct, so mocks and test doubles
must embed too. And a nine-method interface with five optional members has a *documentation* problem that the type
system does not solve: nothing tells an author which four matter.

**[NEW, and it is the sharpest distinction in the dossier] forced-embed defaults are safe for optional *methods*;
sentinel-error probing is unsafe for optional *capabilities*. Conduit does both, and only one works.** The same
`ErrUnimplemented` that safely marks an absent optional method is also used for **runtime capability
negotiation**: `runRead` calls `ReadN` first and, on `errors.Is(err, ErrUnimplemented)`, permanently falls back to
a `Read` shim (§8.1). That negotiation costs a wasted call, conflates "I don't implement batching" with "my batch
read happened to return `ErrUnimplemented` from a dependency," and caused a real, filed bug — SDK #248: a source
implementing `ReadN` **bypassed the encoding middleware's `Read` override and records went out unencoded**
(§13.2a).

Meanwhile the *same project* got the same problem right later with `PartitionAwareSource`: an optional interface,
type-asserted once at construction, absence meaning default behaviour "in perpetuity, zero required migration"
(§12.3). **Two attempts at optional-capability declaration in one repo; one works.** That is unusually clean
evidence for D9's recommendation and for Part 3 trap 11, and it gives canal a rule with a bright line:

> A missing *method* may be signalled by a default that returns a sentinel. A missing *capability* must never be
> discovered by calling something and inspecting the error.

**[CONFIRMS] trap 11 and D9's core rule — capability = data, behaviour = interface.** Conduit's partition
declaration is **both**: a Go optional interface host-side, and a proto `PartitioningCapability` message on the
wire. The type assertion never crosses the boundary; the declaration does (§12.3). This is exactly D9's split,
shipped, and it is the existence proof the parent document wanted.

**[NEW — a structural addition D9 and the Appendix are missing] there are *three* interface layers, not one.**
The dossier calls this "the single most valuable thing in this dossier for canal" (§1, §10.1, §13.1 item 1):

| Layer | Who implements | Shape |
|---|---|---|
| **Author-facing** (`conduit-connector-sdk`) | connector authors | `Source` / `Destination` — plain method calls, ergonomic |
| **Boundary** (`conduit-connector-protocol/pconnector`) | the in-proc SDK adapter **and** the gRPC client | `SourcePlugin` / `DestinationPlugin` — request/response structs + a bidi stream |
| **Engine-facing** (`conduit/pkg/plugin/connector`) | builtin adapter and standalone adapter | `Dispenser` + `SourcePlugin` + `NewStream()` |

**The author never sees the boundary layer; the engine never sees the author layer.** That indirection is what
lets in-process and subprocess connectors be interchangeable. The parent document's D9 and its Appendix assume
**one** interface set that must simultaneously be ergonomic for authors and wire-shippable — and then has to
compromise, which is why the Appendix's `AckFunc` closure needs a paragraph explaining how it survives a wire.
Conduit shows the two requirements belong to different types. canal's constraint #4 ("adding a source means
implement the interface, register it, done") is about the *author* layer; constraint #3 (a future gRPC
implementation satisfying the same interface) is about the *boundary* layer. Conflating them makes both worse.

**[CONFIRMS] the Appendix's wire-shippability checklist, with one addition and one deliberate cost.** Every
boundary method is `(ctx, RequestStruct) → (ResponseStruct, error)` **even where the request is empty**
(`type SourceStopRequest struct{}`), because that is exactly the shape protobuf generates, so the same Go
interface is satisfiable by a gRPC client with a mechanical `toproto`/`fromproto` pair and **no signature change**
— and adding a field to a request struct is non-breaking for every implementation (§1.3). The addition D9 should
adopt: **put a version constant next to the boundary types while there is still only one version** (§10.4). The
deliberate cost: `DestinationRunResponseAck.Error` is a **`string`, not an `error`**, and the in-process path pays
that fidelity loss too, so both transports behave identically (§1.3, §10.2b).

**[NEW] the entire in-process/out-of-process seam is one method: `NewStream()`.** The engine's only requirement
over the boundary interface is a transport factory — builtin returns an in-memory channel stream, standalone
returns a gRPC stream (§1.4). Everything else is shared. For canal's constraint #3 this is the concrete target
shape: one factory method, asserted nowhere in the data path.

**[NEW] making the in-process path deliberately *worse* is both a semantics guarantee and a correctness win.**
In-process gets **no** zero-copy advantage: every request and response is deep-cloned on every hop, enforced by a
type constraint rather than a convention (§10.2c):

```go
type inMemoryStreamClient[REQ cloner[REQ], RES any] inMemoryStream[REQ, RES]
type cloner[T any] interface { Clone() T }
```

Two reasons this matters for canal. It prevents the two transports from semantically diverging — the stated
purpose. And it prevents a builtin connector from **mutating a record the engine still holds**, which would be
impossible over gRPC and must therefore be impossible in-process too. This is also the dossier's exemplar of
constraint #5's intent: **generics used to enforce a semantic requirement, not for their own sake.** The parent
document's D9 warns against generics on plugin interfaces and permits them on concrete helpers; `cloner[T]` as a
*constraint on a transport type* is a third, legitimate category worth naming.

**[NEW] the in-memory stream emulates gRPC error semantics, including the client/server asymmetry.** Server-side
`Recv()` returns bare `io.EOF` on close; client-side returns the **reason** the stream was closed (§10.2d). The
engine depends on this: `isClosedSourceStream(err)` treats `io.EOF` from `Source.Ack` as "the source already
stopped, suppress this derived error so it can't mask the real one." A `Client()`/`Server()` split on the stream
interface, with the permitted caller documented on the interface itself and a runtime type check in the adapter,
is what lets one type serve both roles. canal's boundary needs this decided rather than discovered: **which side
sees a bare EOF and which side sees a reason.**

**[NEW] one remaining asymmetry, documented and accepted.** `Run` is **asynchronous in the builtin adapter** (it
spawns a goroutine and returns nil immediately, closing the stream with the run error when the plugin's `Run`
returns), whereas over gRPC `Run` establishes a stream and returns. Same observable contract, different mechanics
(§10.2). A useful precedent: perfect mechanical symmetry is not required, only *observable* equivalence — and the
one place it diverges should be written down.

**[CONTRADICTS] Part 0 item 4 and canal's constraint #3 wording: Conduit's registration is a compile-time map,
not `init()`-time self-registration, and it chose that on purpose.** Part 0 lists "registration is an init-time
registry keyed by name" as a near-axiom. Conduit's built-in registry is a literal map keyed by **module import
path**:

```go
var DefaultBuiltinConnectors = map[string]sdk.Connector{
    "github.com/conduitio/conduit-connector-postgres": postgres.Connector,
    ...
}
```

The module-path key is load-bearing: `Registry.loadPlugins` reads `debug.ReadBuildInfo()` and **overwrites the
connector's self-declared version with the resolved Go module version**, so a built-in connector cannot lie about
its version (§1.5, §13.1 item 46). `NewRegistry` takes the map as a parameter, so an embedding application can
pass its own — that is Conduit's answer to "add a connector without forking." The dossier states the trade-off
without spin: this is **one core edit per built-in connector** (a map entry plus an import), buying build-time
verification that the plugin exists and a trustworthy version string; an `init()` registry removes the edit and
loses both.

This is a genuine tension with canal's constraint #4 ("zero core edits") and it deserves a decision rather than an
assumption, because the two goals are reconcilable: **init-time self-registration, plus version resolution from
`debug.ReadBuildInfo()` keyed by the registering package's own module path.** That gets version integrity without
the map. Note also that under `init()`-registration the blank import is *itself* a core edit unless connectors
live outside the repo — so canal's "zero core edits" claim only literally holds for out-of-tree connectors, and
the parent document should say so.

**[CONFIRMS] the connector descriptor is data and the constructors are factories.** One connector *package*
exports one value carrying up to three constructors — `NewSpecification`, `NewSource`, `NewDestination`, with a
source-only connector leaving `NewDestination` nil (§1.5). These are **factories, not instances**: the engine calls
`conn.NewSource()` per connector instance, so instance state is naturally per-instance. And `Specification`
carries `SourceParams` and `DestinationParams` **separately** on one spec, with `Specify` taking an empty request
and callable **before** `Configure`, so the engine builds its catalogue at registry-load time without
instantiating anything (§7.1). That is D10 item 13 ("publish a cached connector descriptor... without
instantiating anything") satisfied structurally.

**[NEW] Conduit has no declared-capability struct and no registration-time cross-check.** D9's enforcement detail
— "declaring `Chunkable: true` without implementing `SupportsChunkedSnapshot` is a registration-time panic" — has
no precedent here, because Conduit barely has declared capabilities at all. Neutral, but worth stating: the
cross-check is canal's invention and will not have prior art to copy.

---

## 10. D10 — config self-description for the UI

D10's recommendation is confirmed almost line for line, by the only system in the evidence base that ships a
generated spec, an embedded UI and a public complaint list about what the spec lacks. This section is therefore
mostly confirmation with mechanisms attached — and one security finding that is the strongest argument in the
dossier for not deferring D10 item 3.

**[CONFIRMS] the whole pipeline: struct tags → generated spec → embedded YAML → engine *and* UI read the spec,
never the struct.** The shipped form (§7.5):

```go
type SourceWithBatch struct {
    // Maximum size of batch before it gets read from the source.
    BatchSize *int `json:"sdk.batch.size" default:"0" validate:"gt=-1"`
}
```

with `//go:generate specgen`, `//go:embed connector.yaml`, and `sdk.YAMLSpecification(specs, version)` wiring it
into the connector value at compile time. Three details D10 needs and does not have:

- **The Go doc comment above the field becomes `Parameter.Description`.** That is the detail that makes the whole
  approach work: "documentation cannot drift from the spec because it *is* the spec." D10 item 1 asserts a single
  source of truth; this is the mechanism that makes docs part of it rather than a fourth artifact.
- **One `json:` tag names the wire key, the spec key, and the struct field.** One tag, three jobs (§7.4). Decoding
  is `mapstructure` with `TagName: "json"`.
- **The generated YAML is round-tripped and merged**, so hand-written prose (`summary`, `description`, `author`)
  survives regeneration while parameters are recomputed (§7.5). D10's codegen has no story for
  human-authored-and-machine-authored content sharing one file; this is it.

**[CONFIRMS] `./connector spec` — the spec is printable with no RPC involved.** `Serve` intercepts `argv[1]`
before the plugin handshake and prints the YAML spec (or the version) to stdout and exits (§7.5). "Cheap, and
exactly what a registry indexer or a doc generator wants." D10 item 13's cached descriptor gains a second,
transport-free acquisition path.

**[CONFIRMS] validations as serialisable objects, and the closed set is small enough to reimplement in a
browser.** `Validation` is an interface with `Type() ValidationType`, `Value() string` and
`Validate(string) error`, marshalling to `{"type": ..., "value": ...}` — six kinds: `required`, `greater-than`,
`less-than`, `inclusion`, `exclusion`, `regex` (§7.3). So each validation is simultaneously **executable in Go**
and **a two-string tuple on the wire**, and all six are trivially reimplementable client-side. That is D10 item 6's
"small closed predicate set a UI can interpret," verified, and it is also a datapoint for Part 4 open question 9:
**Conduit ships a usable config UI with no expression language**, using six serialisable validations plus a
Go-only cross-field escape hatch and accepting the round-trip (§7.4). The escape hatch is `Validatable`
(`Validate(context.Context) error`), embedded in both `SourceConfig` and `DestinationConfig` so *every* connector
config is validatable, with `DefaultSourceMiddleware.Validate` walking its own fields by reflection and joining
every embedded `Validatable`'s errors. "Conduit accepts that split rather than inventing an expression language."

**[NEW] two delimiter bugs in the wire form of validations, both avoidable.** `ValidationInclusion.Value()` is
`strings.Join(v.List, ",")`, so **a list member containing a comma is unrepresentable** (§7.3). And the regex is a
Go RE2 pattern serialised as its source string, so a JS `RegExp` accepts most but not all patterns — "a real,
small incompatibility." canal's fix for both is the same: make the wire form of a validation a **typed value, not
a string** (a real list for inclusion/exclusion; and either restrict to a JS-compatible regex subset or ship the
pattern with a declared flavour so the UI can decline to evaluate it).

**[CONFIRMS] the fixed config pipeline, with the exact order and one rule canal would otherwise find as a bug.**
`sanitize (trim keys and values) → apply defaults → validate against spec → decode into struct → custom
Validate(ctx)`, with `Config.Validate` rejecting unknown keys (`ErrUnrecognizedParameter`), validating types by
parse-attempt, and **accumulating errors via `errors.Join` rather than short-circuiting** so a UI gets every
problem at once (§7.4). D10 item 8 confirmed. The rule worth transplanting explicitly: **an empty value for a
non-required parameter is valid and skips all other validations**, so an optional field left blank does not trip a
`greater-than`. That is a one-line behaviour whose absence produces a form that cannot be submitted.

**[CONFIRMS, and this is the strongest single argument in the dossier for D10 item 3/11] one missing boolean in a
spec type became a security bug class.** `Parameter` is four fields — `Default string`, `Description string`,
`Type ParameterType`, `Validations []Validation` — and there is **no `Sensitive`/`Secret` flag** (§7.2). The
documented consequences, in order:

1. At the plugin boundary, secrets must be assumed everywhere: *"`in.Config` routinely carries secrets (DB urls
   with embedded passwords, SASL credentials, access keys). There is no per-parameter sensitivity metadata yet, so
   `log.RedactAll` redacts every value"* (§6.6) — a sledgehammer at the log site.
2. A separate redaction pass at the API boundary (`pkg/http/api/toproto/redact.go`), applied **per call site**.
3. conduit#2640: *"GetDLQ returns connector Settings unredacted — potential secret exposure via API"* — one call
   site missed (§11.3, §13.2h).

D10 item 11 already says "declared `secret: true` in the connector's own spec; core owns redaction." Conduit is the
cost of not having it: redaction becomes a per-call-site discipline, and discipline fails. This also validates R8's
principle (drift prevented structurally, not by discipline) applied to a security property.

**[CONFIRMS] the rest of D10 item 3's addition list, now empirically grounded rather than inferred.** The
dossier's verdict on whether the spec can drive a UI with no per-connector frontend code: **"Yes for rendering and
single-field validation; no for good UX."** What you get: name, type (six: string, int, float, bool, file,
duration), default, description, required-ness (as a validation, not a field) and five more validations. What you
do **not** get (§7.6): `sensitive`, field **order**, **grouping**/sections, display **labels**, **conditional
visibility** ("show `ssl.ca` only if `ssl.enabled`"), **units**, and an `Enum` field (enums are
`ValidationInclusion`). Also `Default` is a **string regardless of type**, parsed later — trivially JSON/proto-able
at the cost of type safety in the spec itself. D10's list of things to add before the first connector ships is
exactly this list, and the dossier's reason is the decisive one: *"adding them later means editing every
connector's tags."*

**[NEW] the exact mechanism by which field order is lost, which D10 should design around.** The `connector.yaml`
model's `Parameters` is a **slice** — because YAML order is meaningful for humans — while the runtime
`config.Parameters` is a **map**, and `ToConfig()` converts slice→map. **Ordering information is destroyed at that
boundary, and that is why a Conduit UI has no field order** (§7.5). The lesson generalises past ordering: any
presentation metadata that exists in the authored form and not in the runtime form is unavailable to the UI. canal
should define **one** parameter type carried end to end, ordered, rather than an authored form and a runtime form
that differ.

**[CONTRADICTS] D10 item 4's "address by variadic path segments, never a dotted string" — the wildcard case needs
a pattern language.** Conduit's config is `map[string]string` end to end, structure recovered from **dotted keys**
(`Config.breakUp()` splits on `.` into nested maps before decoding), and — the part that matters — **wildcards are
supported in parameter *names***: `collection.*.format` matches `collection.foo.format` (§7.2). "That is how
per-collection configuration is expressed without the core knowing what a collection is — genuinely useful, and
worth stealing for canal's multi-collection story."

This is a real tension. D10's variadic-segment addressing is right for *values*; it does not obviously give you a
way to declare a parameter whose name is a **pattern** over unknown entity ids, which is what per-collection
configuration requires and what the core needs in order to default and validate keys it has never seen. canal must
decide how a pattern parameter is expressed in a segment-addressed spec — segments with a wildcard segment type is
the obvious answer, but it needs stating, because "one parameter declaration matching N runtime keys" is a
different concept from "a nested object."

**[CONFIRMS] a reserved namespace for framework knobs.** Every framework-provided setting is `sdk.*` —
`sdk.batch.size`, `sdk.batch.delay`, `sdk.schema.extract.payload.enabled`, `sdk.rate.perSecond`, `sdk.rate.burst`
— so there is no collision with connector-owned keys and every framework knob is uniformly discoverable in the
generated spec (§13.1 item 14). D10/R9 confirmed; canal's equivalent is `canal.*`. Note the contrast with the
metadata namespaces (§D1), which are reserved by convention only and were violated — a reserved *config* namespace
is enforceable at spec-generation time in a way a reserved *metadata* namespace is not.

---

## 11. D11 — error and retry classification

**[CONFIRMS by absence, and removes the hope of a template]** — Conduit has **excellent operator-facing error
taxonomy and effectively no connector-facing one** (§6.5, §13.2i). The entire connector-facing vocabulary is two
sentinels:

- `ErrBackoffRetry` — "there is no record to fetch right now, try again later"
- `ErrUnimplemented` — "this optional method is absent"

Any other error terminates the read loop (`default: return fmt.Errorf("read plugin error: %w", err)`). The engine
has `cerrors.FatalError`/`IsFatalError`, but it is **engine-internal only** and no connector can produce one. The
dossier's summary is D11's problem statement restated from the inside:

> a connector cannot say "this write failed because the row is malformed (never retry, DLQ it)" versus "this write
> failed because the network blipped (retry me)". Everything is one `error`, and the engine's only response is
> nack→DLQ or die.

So the closest prior art to canal has exactly the gap D11 identifies, which confirms the gap is real and
structural rather than an artefact of the surveyed systems' ages — and it means **canal's failure-mode taxonomy has
no shipped precedent to copy.** D11's class set (`transient-upstream | transient-internal | permanent-upstream |
permanent-mapping | permanent-contract | duplicate-idempotent-success | clock-skew`, plus `not-connected` and
`end-of-input`) is canal's invention, assembled from Benthos's proto and Airbyte's `failure_type` recall. Build it
knowing that.

**[NEW] `ErrBackoffRetry` overloads "nothing to read" with "retry me," and that overload is a modelling error
canal should not inherit.** It is "a long-poll idiom, not a failure classification" (§6.5a). Two consequences: a
source cannot distinguish an idle upstream from a transient failure, and the framework's backoff is applied to
both. canal's D6 item 10 (heartbeat / idle-position-advance) and D11's retry classes must be **distinct
vocabularies** — "I have nothing right now, and my position is still valid" is an *advance-the-checkpoint* signal,
not a *back off* signal.

**[NEW] the backoff parameters are hardcoded with a four-year-old TODO, and so are the shutdown timeouts.** The
SDK's read-loop backoff is `Backoff{Factor: 2, Min: 100ms, Max: 5s}` with `// TODO make backoff params
configurable` pointing at conduit#184; the one-minute stop/teardown timeout is a package var with
`TODO make the timeout configurable` (conduit#183) (§6.2, §6.5a). D11 item 4 makes retry policy
`{maxAttempts, backoff, terminal disposition}` and D10 item 5 makes it a **reusable composite config field**;
Conduit is the evidence that if these are not config from the first commit they stay hardcoded for years, because
making them configurable later touches every connector's spec.

**[NEW — worth stealing outright] the operator-facing half is genuinely good, and it has an enforcement
mechanism D11 lacks.** `conduiterr` errors at the API boundary carry a stable machine-readable `reason`, a gRPC
category, and optionally `configPath`, `suggestion` and `fix` (§6.5c):

```go
err := conduiterr.Wrap(conduiterr.CodeConnectorPluginNotFound, "...", plugin.ErrPluginNotFound)
err.Suggestion = "run `conduit connectors list` to see available built-in connectors"
// Invariant: errors.Is(err, plugin.ErrPluginNotFound) still holds — the
// sentinel is wrapped, and the ConduitError adds the machine-actionable code.
```

Three properties to transplant. The underlying sentinel is **deliberately preserved**, so `errors.Is` identity
survives the wrapping — which is what lets internal code branch while the wire gets a code. There is a **contract
test (`codes_contract_test.go`) that fails the build if a status reaches the wire without a code.** And every
un-migrated path falls back to reason `internal.unknown` rather than emitting a codeless status, so the migration
is safe by default. D11 item 9 (split error text by audience) and D10 item 8 (per-field diagnostics) both want
this shape; the build-failing contract test is the R8 mechanism that keeps it true.

**[CONFIRMS] D11 item 6 — per-record error routing must work for sources too, and Conduit's does.** The DLQ is
pipeline-level: a nack from any node routes, and the nack metadata carries the **failing node id**
(`conduit.dlq.nack.node.id`) alongside the cause (§9.4). That is better than Connect's sink-only
`ErrantRecordReporter`. But the cause is a **string** twice over — `DestinationRunResponseAck.Error string` at the
boundary, and `Reason.Error()` into DLQ metadata — and `pkg/connector/destination.go` re-inflates it into a fresh
`error` with **no `errors.Is` identity** (§9.3). So D11's insistence that the class be **typed at the boundary**,
not merely in the engine, is confirmed by the exact place the type dies.

**[CONFIRMS] partial batch failure is prefix-only with one shared error string — and this is the precise answer to
R7.** `Destination.Write(ctx, r []Record) (n int, err error)` is `io.Writer`'s contract: records `[0,n)`
succeeded, record `n` failed, `[n,len)` were unattempted. There is **no way to express "records 3 and 7 failed"**
(§1.2, §9.3). The engine converts it to acks by acking the prefix and nacking the suffix — **every nacked record
carrying the same error, including the ones never attempted**:

```go
ackErr := w.ackFn(batch[:n], nil, w.stream)   // ack the prefix
ackErr = w.ackFn(batch[n:], err, w.stream)    // nack the suffix, ALL with the same error
```

"A record that would have succeeded is dead-lettered with someone else's error message." R7 ("if the design says
retry the failures, the contract must name them") is violated by the *shape of the return type*, not by an
implementation gap — which is exactly R7's point.

**[CONFIRMS] contract violations are detected and made loud, in the fail-safe direction.** If a connector reports
`n == len(batch)` **and** a non-nil error, the SDK rewrites `n = 0` and nacks the whole batch with "this is
probably a bug in the connector"; if `n < len(batch)` with a nil error, it synthesises an error naming the counts
(§9.3). Two lessons: **detect connector contract violations at the boundary** rather than trusting the return, and
**resolve ambiguity toward re-delivery, never toward silent success.**

**[NEW — the right shape exists in v2, in the wrong layer] per-record outcome flags including `Retry`.** v2's
internal `Batch` carries `recordStatuses []RecordStatus` over four flags — `RecordFlagAck`, `RecordFlagNack`,
`RecordFlagRetry`, `RecordFlagFilter` — with per-record errors and a `tainted` flag meaning "this batch must be
split into runs of same-status records before it can proceed," plus `Ack(i, j...)`, `Nack(i, errs...)`,
`Retry(i, j...)`, `Filter(i, j...)`, `SetRecords(i, recs)` and `SplitRecord(i, recs)` (§9.3). **`Retry` as a
per-record outcome is a concept v1 does not have at all.** This is closer to canal's needs than Flink's
`CommitRequest` because it is a batch-shaped Go type rather than a per-item callback object.

The dossier's warning is the transplantable part: **the boundary type still flattens to prefix-`(n, err)`, so the
fidelity dies at the wire regardless.** Per-record outcomes must be in the *boundary* struct — `[]RecordOutcome`
with a typed class per record — or the engine's richer internal model is decoration. That is a direct requirement
on D9's Appendix, and it is the one place where the Appendix's current `WriteBatch(ctx, b Batch) error` is
provably insufficient: a single `error` return cannot carry per-record outcomes at all.

**[NEW] the DLQ is a destination connector, not a store — and that decision has a frontend consequence canal must
decide before it ships a DLQ.** The mechanism first (§9.4): `DLQHandler` is three methods (`Open`/`Write`/`Close`);
the default implementation wraps a normal destination **synchronously**, waiting for the ack before returning, so
the source is acked only after the DLQ write is confirmed; a fixed-size **ring buffer of ack/nack outcomes** is the
circuit breaker; exceeding the nack threshold is a `FatalError`; and if the DLQ write itself fails the node
**latches broken** ("write to the DLQ failed, this message is essentially lost — we need to stop processing more
nacks and stop the pipeline"). All of that is worth copying and all of it confirms D4/D11's ordering.

Two costs. The DLQ record is **re-wrapped, not carried**: the original becomes
`StructuredData(msg.Record.Map())` nested under `payload.after`, with the cause and failing-node id in metadata —
and *this nesting is why the `unwrap` processors exist* (§9.4, §13.2j). canal should carry the original record
plus a failure facet, not nest it. And then, from conduit#2639:

> The engine keeps no queryable copy of dead-lettered records — the DLQ is a destination connector, not a store —
> so v0.18 ships a **config-only DLQ view** [...] Dead-lettered record *content* is not queryable in-product, and
> the panel says so plainly.

canal's end-state goal includes a frontend showing what failed and why. Conduit's answer is "look in whatever
destination you configured," and the bounded queryable DLQ store is explicitly deferred Tier-1 data-path work
needing its own ADR. **The parent document has no entry for this at all**: D11 item 6 specifies rich provenance
*on* the DLQ record but never asks where the record lives or whether the UI can read it. That is a new open
question, and the dossier's judgement is that retrofitting the store is expensive enough that it must be decided
now.

---

## 12. D12 — backpressure and batching ownership

**[CONFIRMS — with the hard number the parent document was missing]** D12's batch-first stance and Part 3 trap 13
(deep buffering) both rested on argument plus Flink's queue capacity of 2. Conduit measured the alternative, in Go,
on the same architecture canal might otherwise pick. From `20260704-pipeline-architecture-v2.md` (§8.4):

| Metric (per 100k records) | v1 (record-at-a-time node graph) | v2 (1000-record batches) |
|---|---|---|
| Allocations | ~6,905,000 | ~1,104,000 |
| Memory | ~250 MB | ~76 MB |
| Per record | ~69 allocs / ~2497 B | ~11 allocs / ~764 B |

**69 allocations and ~2.5 KB per record, with no-op connectors and no real I/O.** The ADR attributes it to
batching amortising "the per-record channel hops, function calls, and acking that v1 pays on every single record
across four nodes," and the dossier's verdict is that "the cost is structural, not a missing optimisation." This is
the strongest available quantitative confirmation that **batch is the primitive and single-record is n=1** — and it
is a ~6.3× allocation penalty for getting it backwards, in a system that cannot now fix it without a second engine
(see traps).

**[CONFIRMS by counterexample] D12 item 1 and item 5 — a rendezvous-channel pipeline has no rejection outcome and
cannot grow one.** Every inter-node channel in v1 is **unbuffered** (`make(chan *Message)`), and `Pub()` **panics**
if you connect a node to more than one output. Backpressure is pure blocking rendezvous propagated hop by hop from
the destination back to the source's `stream.Recv()` and from there into the connector's `Read`, because the source
plugin's `stream.Send` blocks. The in-memory plugin stream is likewise unbuffered (§8.2). There is no queue depth
to tune, no high-water mark, no drop policy, no `Nack`-on-full path. The dossier states the consequences plainly:

- **No rejection outcome exists.** A source cannot be told "I'm full, come back later"; it is simply blocked.
  "canal's R6 (an expressible rejection outcome) has no analogue here and **cannot be retrofitted onto a
  rendezvous channel without changing the node contract**."
- **Zero in-flight buffering means zero pipelining**, which is why v2 exists.
- **Fan-out is explicitly not backpressure-isolating**: `fanout.go` exists, and one slow destination in a fan-out
  blocks the source for all of them.

R6 and D12 item 5 (`when_full ∈ {block | reject | overflow}` in the type) are therefore confirmed as **v1
requirements**, not refinements — the cheap default forecloses them. And the fan-out finding is direct evidence for
D13 item 7: canal's end-state includes fan-out, so per-branch accounting and the duplicate-vs-block trade must be
named at design time.

**[NEW, second independent occurrence] the ack-tracking structure is the one thing nobody bounds.** Conduit's
destination acker holds an **unbounded `deque.Deque[*Message]`** of in-flight messages awaiting acks — "the one
genuinely unbounded structure in the data path, bounded in practice only by how far ahead `DestinationNode` can
get, which unbuffered channels keep small" (§8.2). Connect's `SubmittedRecords` per-partition deques are unbounded
in exactly the same way, with a javadoc warning about exactly the pathology. **Two independent systems, both of
which bounded everything except the structure tracking outstanding acks.** Add to Part 3: the in-flight ack map is
the default blind spot, and it is the structure whose growth is *caused* by the slow path you were trying to
survive.

**[NEW] the default batch size is 1, and batching is opt-in middleware.** `runRead` calls `readFn(ctx, 1)` — "the
default is to read 1 record, the batch middleware can override it" (§8.1). Combined with the §8.4 numbers, this is
what "single-record is the default and batch is the opt-in" costs. canal's inversion (batch primitive, n=1
degenerate) should be stated at the interface, not left to a middleware default.

**[CONFIRMS] D12 item 8's inverted-control batcher shape, with two traps in the implementation.** Conduit's
destination side has two strategies behind one interface — `writeStrategy{Write, Flush, SetStream}` — chosen at
configure time from `sdk.batch.size`/`sdk.batch.delay` (§8.5). `writeStrategySingle` writes through and acks
immediately; `writeStrategyBatch` accumulates. The two traps:

- **The batch strategy reports the previous batch's error on a *later* call**, by polling a results channel
  non-blockingly (`case result := <-w.batcher.Results(): if result.Err != nil { return "last batch write failed" }`).
  So an error surfaces one call late, and **the batch that failed has already had its acks computed.**
- **It stashes a `context.Context` inside a queued item** (`writeBatchItem{ctx, records}`, with a
  `//nolint:containedctx`) and uses "the last record's context as the write context." A queued context "is a
  landmine: it can be cancelled while the batch is still pending."

D12's batcher recommendation should carry both as explicit non-goals: errors are reported against the batch that
produced them, and a batch's context is derived from the flush, never captured from a record.

**[NEW] batch middleware was a bug farm, and one fix was a log warning where a validation belonged.** Beyond SDK
#248 (§D9), the same repo carries #275 *"Fix issue with closing channel in SourceWithBatch"* — a
`panic: send on closed channel` in production — and #309 *"Warn if one of `sdk.batch.size` or `sdk.batch.delay` is
set to 0"*, whose fix is a **log line** (§13.2a):

```go
if *s.config.BatchSize > 0 && *s.config.BatchDelay == 0 {
    Logger(ctx).Warn().Msg("sdk.batch.size is set but sdk.batch.delay is not, this might result in the connector waiting indefinitely for the batch to be filled")
}
```

A config combination that can wedge a pipeline indefinitely is a **validation failure**, not a warning — and
Conduit's own validation machinery (§7.3–7.4) is expressive enough to have caught it if the constraint were
cross-field. This confirms D10 item 6's cross-field `Validate(ctx)` hook is load-bearing rather than a nicety, and
D12 item 11's "lint the deadlock Benthos merely documents."

**[NEW] rate limiting is destination-side only; record size is bounded only at the wire.** `sdk.rate.perSecond` and
`sdk.rate.burst` exist as destination middleware with no source-side equivalent, and the only size bound in the
system is `grpc.MaxRecvMsgSize(MaxReceiveRecordSize)` at the gRPC boundary, configured by env var (§8.3). So an
**in-process** connector can emit an arbitrarily large record and nothing notices — the two transports diverge on
exactly the property §10.2 works hardest to keep identical. For canal: **a record-size limit belongs in the
boundary contract, enforced on both transports**, not in the gRPC options.

---

## 13. D13 — pipeline topology, and whether transforms are core

**[CONTRADICTS — Conduit gives D13's recursive-composition recommendation no support at all.]** D13 recommends
(b)+(e): a fixed outer shape with *all* topology variety coming from components that contain other components
resolved from config (Benthos's `broker`/`switch`/`fallback`/`retry`). Conduit's v1 pipeline is a plain node chain
— `SourceNode → SourceAckerNode → [ProcessorNodes] → DestinationNode → DestinationAckerNode`, wired by
`Pub()`/`Sub()` with a panic if a node is connected to more than one output — and fan-out is a **special-cased node
type** (`fanout.go`), not an ordinary registered component containing children (§8.2). There is no
component-valued config field, no `retry` sink wrapping a child sink, no `switch`. And v2, the intended successor,
"currently supports only 1 source and 1 destination per pipeline."

So D13's recommendation rests on Benthos alone. Conduit is not a counter-argument — it simply never attempted
composition, and its fan-out consequently has the backpressure-coupling defect noted under D12. One partial
corroboration: **the DLQ *is* done as a composable component** — an ordinary destination connector behind a
three-method interface, configured per pipeline (§9.4) — and that is the one place Conduit gets
composition-instead-of-core-feature right. Weak support, honestly labelled.

**[CONFIRMS] D13 item 4's return vocabulary, as a sum type rather than a slice-of-batches.** Conduit's processor
SDK returns a `ProcessedRecord` sum type — `SingleRecord` / `MultiRecord` / `FilterRecord` / `ErrorRecord{Error}`,
closed by an unexported marker method (§13.1, steal-list item 24). That gives 1→1, 1→N, 1→0 and per-record failure
in **one return type**, with the set closed at the package boundary — arguably better than Benthos's
`([]MessageBatch, error)` because the compiler enumerates the outcomes. It is also the same sealed-interface
technique as `Data` (§D1), which suggests canal should adopt it as a general idiom for closed vocabularies rather
than a one-off.

**[NEW] `Process` must be idempotent by contract, because it may re-run after a restart.** The dossier states this
as a requirement on the processor contract (steal-list item 24). D13 has no idempotency requirement on transforms
anywhere, and under at-least-once delivery with restart-from-checkpoint it needs one: a transform with side effects
(an enrichment write, a counter increment, an external lookup that mutates) is re-executed for every record
re-read after a crash. This belongs at the transform interface's doc comment, next to the ordering-scope statement
D13 item 9 already requires.

**[NEW] processors ride the connector registry — one distribution mechanism for two plugin kinds.** From
`20260727-processors-ride-connector-registry.md`: processors were folded into the connector registry rather than
getting their own (§10.5). The dossier's advice is procedural: "canal should decide this once, early." D13 makes
transforms a first-class core stage and D10 makes the connector descriptor the UI's input, but neither says whether
transforms share the connector registry, the connector descriptor shape, the connector spec generator, or the
connector versioning scheme. Four separate registries is R9 sprawl; one registry with a `kind` discriminator is the
cheap answer.

**[NEW — and it is a warning about canal's own constraint #3 future] the WASM path forked the interface, then was
walked back.** Connectors have two transports over one protocol (builtin, standalone-subprocess). **Processors
additionally have a WASM path that does not share the connector protocol at all** — it is a separate host-module
ABI over wazero (§10.5). Then: `20260722-wasm-component-model-deferred.md` defers the WASM Component Model, and a
55 KB design document (`20260726-wasm-host-egress-capability.md`) exists because a sandboxed processor that needs
network access requires a whole capability system — there is a `pkg/plugin/processor/egress/` package with
`ipguard.go`, `policy.go`, `secret.go`. The dossier's lesson is the transplantable one:

> a **sandbox** boundary is not just a **serialization** boundary; every capability the plugin needs has to be
> re-granted explicitly, and that cost is large enough to be its own design doc.

canal's constraint #3 commits to designing for a future out-of-process boundary. This says the cost is
*categorically* different depending on which kind: a **subprocess** boundary needs serialisation plus the reverse
host-services seam (§D8); a **sandbox** boundary needs both of those plus an explicit capability grant for every
host resource the plugin touches — network, filesystem, secrets, clock. canal should state which of the two it is
designing for, because "out-of-process later" silently means the cheaper one and the expensive one is the
fashionable one.

---

## 14. D14 — the standalone ↔ distributed seam

**[CONFIRMS — the strongest strategic confirmation in the dossier, from the closest system, in writing.]** canal
wants an enterprise multi-worker deployment and a standalone single binary from one codebase. Conduit's answer,
from `20260704-single-node-engine.md` (§12.1), is that those are **the same binary with nothing added** and the
difference is entirely outside the process:

> The Conduit engine is single-node by design. It will never grow cluster-membership protocols, leader election,
> gossip, or consensus (Raft/etcd). Distribution — running many pipelines across many instances, or one hot
> pipeline across several — is a scheduling concern solved a layer above the engine (Kubernetes operator / control
> plane), not inside the engine.

The reasoning is an independent arrival at Part 3 traps 6, 7 and 8: *"Data-integration systems that put
distribution inside the engine pay for it forever. Kafka Connect's rebalance protocol is the canonical example:
worker-cluster membership and task rebalancing are a persistent source of operational pain, stop-the-world pauses,
and subtle correctness bugs."* And the ADR carries an enforcement clause: *"No membership, no leader election, no
gossip, no consensus in the engine. Well-meaning clustering PRs are rejected and pointed at this ADR. If a design
doc starts growing engine-level rebalancing, that is a stop-and-flag signal."*

Three-level scale-out, all outside the engine: **(1) fleet sharding** — a scheduler bin-packs pipelines onto
instances, "who runs what" state lives in the control plane (Postgres- or Kubernetes-lease-backed), **no engine
change at all**, with static assignment (`conduit run --pipelines <dir>` + Helm) as the pre-scheduler version;
**(2) hot-pipeline parallelism via partition claims** (§D5); **(3) HA = active/passive, resume-from-checkpoint** —
"correct by construction given the data-integrity invariants (crash-safe positions, atomic checkpoints,
at-least-once). No consensus and no warm standbys in v1." The accepted consequence is stated: static
pipeline-to-instance assignment "must cover users well before the operator exists."

**[CONTRADICTS] D14's `Runtime` struct — `Coordinator` may not belong inside the engine at all.** D14 recommends
four interfaces (`ConfigStore`, `CheckpointStore`, `Coordinator`, `StatusAggregator`) with `singleNode{}` as the
standalone `Coordinator` — "always leader, all assignments local, leases no-ops" — and the rule "if a fifth
interface appears, the abstraction is wrong." Conduit's ADR is a direct challenge: *membership, leader election and
assignment leases are not engine concerns even as an interface with a no-op implementation.* The difference is not
cosmetic. An engine that has a coordination *concept* has the surface the ADR predicts will be grown; an engine
whose only coordination-adjacent obligation is **carrying a fencing token on the checkpoint write** has no such
surface.

D14's central insights survive intact and are *strengthened* by this: "the leader only plans," "the data plane
keeps running and keeps checkpointing with the entire control plane down," "the assignment lease is the fencing
token and leadership is never trusted for correctness." Conduit makes the same claim more strongly — HA correctness
"reduces to the data-integrity invariants rather than a bespoke consensus implementation we would have to prove
correct." So the open question D14 should now answer explicitly is: **does canal ship three interfaces plus a
generation/lease parameter on every checkpoint write, with all coordination above the binary — or four interfaces
with a no-op?** The dossier's evidence favours the first, and the parent document's own Chain B (splits as the
lease subject) is what makes the second tempting.

**[CONFIRMS] the single-binary story, and adds the distinction between a shared store and a distributed engine.**
`cmd/conduit` embeds the engine, the built-in connectors, the processors, the HTTP+gRPC API and a `go:embed`ed
React UI (§12.4). One state store, four backends selected by `db.type` (`badger`, `sqlite`, `postgres`,
`inmemory`), one `NewTransaction(ctx, update bool)` interface, with positions, connector instances, pipelines and
processors all living in it. `inmemory` + `--pipelines <dir>` is local dev; `sqlite`/`badger` is single-node prod;
and — the insight D14 does not state — **`postgres` is what lets several instances share a control-plane store
*without* the engine becoming distributed.** D14's "Postgres first, etcd as the conformance target that stops the
interface acquiring Postgres-isms" is confirmed, with the addition that *sharing a store is not the same commitment
as distributing an engine* and the two should be decided separately.

**[CONFIRMS] D14's "ship exactly one snapshot/checkpoint format."** There is no format matrix — one store, one
transaction API, no aligned/unaligned/canonical/native distinction to tax operators with.

**[NEW] a scope-defence sentence canal should adopt verbatim.** `20260704-local-state-only.md` (§12.4):

> State is a local embedded KV store, checkpointed atomically with the pipeline. Recovery is
> resume-from-checkpoint [...] **Explicitly out of scope:** distributed snapshots, pluggable/tunable state
> backends, event-time watermarks, large distributed joins, late-data reprocessing frameworks.
>
> The temptation is to keep adding "just one more" stateful feature until Conduit is a worse Flink.

That confirms D4 item 7 and D13 item 8 (do not build barrier-based distributed snapshots) and is the model for
canal's own "what we will never build" ADR — which D14's recommendation list asks for and which
`design-rules.md` R12 already requires live in `docs/decisions/` from the first commit.

**[NEW] within-node recovery: the attempt counter decays over a sliding window.** `ErrRecoveryCfg{MinDelay,
MaxDelay, BackoffFactor, MaxRetries, MaxRetriesWindow}` with `InfiniteRetriesErrRecovery = -1`, a default delay
progression of 1s → 2s → 4s → … → 10m capped, and the detail that matters (§12.5):

```go
time.AfterFunc(duration+s.errRecoveryCfg.MaxRetriesWindow, func() {
    rp.recoveryAttempts.Add(-1) // Decrement the number of attempts after delay.
})
```

**A pipeline that flaps once a day never exhausts its retries; one that flaps in a tight loop does.** Exhaustion is
a `FatalError`. And backoff state (`backoff`, `recoveryAttempts`) is deliberately carried across restarts of the
same pipeline, so a restart does not reset the flap detector. D11 item 4 (bounded retry with a named terminal
disposition) and D14's status model both need this and neither specifies it: a flat `maxAttempts` either kills a
pipeline with a daily blip or never kills a livelocked one.

**[NEW] out-of-process mechanics canal will inherit if it ever ships them.** `hashicorp/go-plugin` over gRPC,
subprocess per connector instance, a magic-cookie handshake, and **discovery by directory scan + execute**: the
registry reads `pluginDir`, runs each file, and calls `Specify` to learn what it is — with a helpful error naming
the two likely causes ("check if the file is a valid connector plugin binary and if you have permissions for
running it") (§10.3). Two things to note. Configuration reaches the subprocess as **environment variables only**,
with `cmd.Env = make([]string, 0)` — a deliberately empty environment plus five named vars — and the same
`PluginConfig` struct is passed **by value** to builtin plugins, so both modes read config from the same type.
And an honest TODO on the blueprint records a supply-chain gap: *"store hash of plugin binary and compare before
running the binary to ensure someone can't switch the plugin after we registered it."* If canal ever ships
directory-scan discovery, pin the hash at registration.

**[NEW] protocol versioning is honest and expensive: 24 files for one bump.** `go-plugin`'s `VersionedPlugins`
lets the host support N protocol versions simultaneously; the plugin declares one; the highest common version wins
at handshake; a mismatch is a **handshake failure at startup, before any data moves** (§10.4). The cost is
`pconnector/v1/{client,server,toproto,fromproto}` plus `v2/{...}` × three plugin kinds — fully duplicated
conversion code. Inside a major version, compatibility is maintained by proto discipline, stated in the
partition-claims RFC: *"All changes below are new fields (fresh field numbers) on existing messages, one new
optional RPC, and new top-level messages. **Nothing existing is renamed, renumbered, or removed**."* The
transplantable rule for canal's boundary types: **only-additive within a version, a version constant from the
first commit, and a startup-time handshake failure rather than a runtime surprise.**

---

## 15. Observability and the frontend goal (D14's status/metrics half)

This is where Conduit's evidence is most directly useful to canal's frontend goal, because Conduit ships an
embedded UI and a public issue list about what the engine cannot tell it.

**[CONFIRMS, decisively] the honesty rule — "up" and "moving" are different facts, and Conduit cannot tell them
apart.** `StatusRunning` means "the pipeline's goroutines are alive." A source that is connected and returning
`ErrBackoffRetry` forever, or blocked on an unbuffered channel behind a wedged destination, is `Running` (§11.2).
Worse, the obvious workaround fails: `conduit_pipeline_execution_duration_seconds` only records records that
*completed*, so **a fully-stalled pipeline emits no samples at all — the histogram is silent rather than
alarming.** That is the sharpest statement in the whole evidence base of the failure `design-rules.md` names ("a
metrics UI that cannot distinguish *the endpoint answered* from *your data arrived* is actively misleading"), and
it is direct support for D14's requirement that **`Connected: True` must never be able to imply
`Progressing: True`**, asserted by a fixture test.

**[CONFIRMS + NEW] there is no record-count metric at all, and neither counts nor liveness can be reconstructed
later.** Not one (§11.1). Throughput must be derived from `conduit_connector_bytes_count` — a byte histogram's
implicit `_count` — which conflates "records observed" with "records sized." The docs' own worked example of adding
a metric is literally "count messages per pipeline," i.e. **the documentation teaches you to add the metric that
should have existed.** The prescription: ship monotonic `records_in`/`records_out` counters **and a
`last_record_at` timestamp per connector instance from day one**, because "neither can be reconstructed later from
histograms." D14 mandates `canal_checkpoint_age_seconds` as the primary health metric and forbids a metric called
`lag`; add the per-instance counters and timestamp to that same non-negotiable list.

**[CONFIRMS] one shared id space across topology nodes, metric labels and the live tap.** `labelComponentID` is
the connector/processor **instance** id and — per the constant's own comment — "uses the same ID space as the
topology nodes and as the existing `conduit_inspector_sessions` / `conduit_inspector_dropped_records_total`
metrics... so per-node dashboards can join across all of these metrics on this label" (§11.1). The dossier's
verdict: "Deciding that id space once, and using it everywhere, is the whole trick." That is D14's `component_id`
recommendation with the shipped mechanism and the exact reason.

**[NEW] adding a label to an existing metric resets the series, and the docs say so.** From `docs/metrics.md`:
*"Because a new label starts a new Prometheus series, upgrading Conduit resets the counters/histograms these three
metrics contribute to."* An operator-visible breaking change dressed as an additive one (§11.1). D14 already says
"over-provision labels immediately"; this is the concrete consequence of not doing so, and it applies to
`component_id` specifically.

**[NEW] metric naming drift inside one repo, with a comment apologising for fixing it.** `pipeline_recovering_count`
lacks both the `conduit_` prefix and the `_total` suffix, while a newer counter carries a comment explaining that
it "is a new metric with no consumer yet, so the name is fixed correctly now rather than matching the repo's older,
suffix-less counters" (§11.1). **A metric name is a public API.** D14's "metric naming convention decided in the
first commit" is confirmed by a project that had to choose between consistency and correctness after the fact.

**[NEW] a metric-definition honesty issue canal should pre-empt.** Bytes are measured on the **JSON representation**
of the record payload and key, not on the wire encoding (§11.1) — so `conduit_connector_bytes` is a proxy for
record size, not network volume. Under D2 (a pluggable codec) the gap widens: a record's byte size depends on which
codec is configured. Any size metric must name the representation it measures in its help text, per FLIP-33's
"documented as an explicit subtraction" discipline that D14 already adopts for lag.

**[CONFIRMS + NEW] the live per-component record inspector, and why it needs a drop counter from the start.**
`Instance.Inspect(ctx) *inspector.Session` is a per-component live tap on the actual records flowing through a
connector or processor (§11.3) — D14's "tap-any-edge live event sampler," shipped. Sessions are buffered and
**drop on full** rather than applying backpressure, which is correct (an observability tap must never wedge the
data path) — but it shipped **silently**, and `conduit_inspector_dropped_records_total` had to be added later
(#2628, "Inspector drops-on-full silently today"). "An observability tap that can silently lie about completeness
is worse than one that refuses; metering the drops is the minimum fix." Ship the counter with the tap.

**[CONFIRMS, partially] the status model, and the two things it is missing.** `Status ∈ {Running, SystemStopped,
UserStopped, Degraded, Recovering}`, with `Instance.Error string` carrying the cause and
`pipeline_recovering_count` counting flaps (§11.2). Two of D14's discriminations are validated as genuinely useful:
`SystemStopped` vs `UserStopped` ("the engine stopped this" vs "an operator stopped this") and `Degraded` vs
`Recovering` ("failed and giving up" vs "failed and retrying with backoff"). Also good: `conduit_pipelines{status}`
(fleet histogram — "how many pipelines are degraded") and `conduit_pipeline_status{pipeline_name}` (per-pipeline
state) are **different questions and both exist.**

What is missing confirms D14 twice. There is **no `Completed`** — the second system after Connect to lack a
terminal success state, which matters for canal's bounded/batch pipelines. And the rich **connector-level** state
machine (13 states, §6.1) **is not exposed to the API at all**, so an operator sees pipeline status and cannot see
which connector is stuck in `Starting`. That is `design-rules.md` open decision 7 (a connector state machine with
a last-error surface **and the API fields exposing it**) — Conduit has the machine and not the fields.

**[NEW] a UI need forced an engine field addition — exactly what D14's `Conditions` shape prevents.** Conduit's
fleet UI epic (#2624) lists as engine work: *"**P3** `stopReason`/`stopped_by` additive field on pipeline state →
gates fleet-view 'operator-stopped vs engine-restarted' accuracy"* (§11.2). A five-value enum could not answer a
question the UI needed, so the engine grew a field. D14's `Phase` + `Conditions[]{Type, Status, Reason, Message,
LastTransitionTime, ObservedGeneration}` absorbs that class of addition without a schema change, and this is the
concrete instance of the problem it solves.

**[CONFIRMS] structured, coded errors are what let a UI render a fix rather than a stack trace** — see §D11 for
the `conduiterr` shape and the build-failing contract test. Also: config values are redacted at the API boundary
(`toproto/redact.go`) with the known hole from §D10.

---

## 16. Lifecycle and cancellation — four contract gaps in the parent document's Appendix

The parent document's Appendix specifies `Connect(ctx) / ReadBatch(ctx) / Close(ctx)` and Part 0 item 7 requires
`context.Context` on every method that can block. Conduit shows that those two statements leave four load-bearing
contract questions unanswered, all of which produce connector bugs when left implicit. None of these touch a
decision id directly; they are additions to Part 0 and to the Appendix.

**[NEW] the lifecycle is an explicit guarded state machine, not implicit ordering.** Every boundary method wraps
its work in `state.DoWithLock(ctx, DoWithLockOptions{ExpectedStates, StateBefore, StateAfter,
WaitForExpectedState}, fn)` over 13 observed states (§6.1). `Stop` uses `WaitForExpectedState: true` — it *waits*
for the connector to reach a stoppable state — and accepts several (`Running`, `Stopping`, `TornDown`, `Errored`),
logging a warning and no-oping if not actually running. `Teardown` accepts **any** state (`ExpectedStates: nil`).
The value for canal is not the 13 states; it is that **the legal call sequences are data checked at runtime rather
than prose in a doc comment**, so an out-of-order call from the engine or a re-entrant call from a plugin is a loud
error instead of a corrupt connector.

**[NEW] `Configure → Teardown` with no `Open` is a legal, expected sequence — and `Teardown` must run after a
failed `Open`.** From the SDK: *"teardown can be called without 'open' or 'read' being called previously, e.g. when
Conduit is validating a connector configuration, it will call 'configure' and then 'teardown'"* (§6.3). The engine
guarantees the second half with a `defer` that tears the plugin down and nils it if `Open` returned an error. The
parent document's Appendix never says `Close` may be called without a successful `Connect`, and every connector
author will assume it cannot. **State it in the interface's doc comment**: `Close` must be safe on a
never-connected connector, and the core must call it after a failed `Connect`.

**[NEW] two cancellable scopes per connector, and a `Detach` helper to build them.** `Open`'s context must
**outlive** the `Open` call — connections opened there are used later — but must be cancellable when the connector
is asked to stop. Conduit's solution (§6.4): `ccontext.Detach(ctx)` produces a context that **keeps values and
drops cancellation and deadline**, then wraps it in a fresh `WithCancel`, and a goroutine propagates the caller's
cancellation *only for the duration of the call*:

```go
ctxOpen := ccontext.Detach(ctx)
ctxOpen, a.openCancel = context.WithCancel(ctxOpen)
go func() {
    select {
    case <-ctx.Done():  a.openCancel()   // cancelled during Open -> abort Open
    case <-startDone:                    // Open finished first -> leave the context open
    }
}()
```

So a source has **`openCtx` (connection lifetime)** and **`readCtx` (read loop)**, cancelled in that order by
`Stop`. Part 0 item 7's "context on every blocking method" does not address the lifetime problem it creates: a
`Connect(ctx)` whose ctx dies when `Connect` returns leaves the connector holding connections tied to a dead
context. canal must decide, at the interface, **which context governs the resource and which governs the call**,
and provide the `Detach` helper rather than letting each connector invent one.

**[NEW] cancellation in the read path means "drain, then stop," not "abort" — and it must be stated as prose.**
Conduit's documented `Read` contract (§6.4):

> If Read receives a cancelled context or the context is cancelled while Read is running it must stop retrieving
> new records from the source system and start returning records that have already been buffered. If there are no
> buffered records left Read must return the context error to signal a graceful stop. If Read returns
> ErrBackoffRetry while the context is cancelled it will also signal that there are no records left and Read won't
> be called again.

The dossier's note is the important one: "Almost nobody gets this right by accident, which is why it needs to be
stated as prose in the interface." canal's `ReadBatch(ctx)` needs the same paragraph, and it interacts with D12:
"drain then stop" is what makes a bounded in-flight window safe to shut down without losing the buffered tail.

**[NEW] panic *and hang* containment for in-process plugins — a whole missing axis given constraint #3.** Builtin
plugin calls run inside `runSandbox[REQ, RES]`, which executes the call in a goroutine with `recover()` **and
selects on `ctx.Done()`** (§6.6):

```go
select {
case <-ctx.Done():
    logger.Error(ctx).Msgf("context cancelled while waiting for builtin connector plugin to respond to %q, detaching from plugin", name)
    return emptyRes, ctx.Err()
case v := <-c: ...
}
```

This "is the in-process substitute for process isolation, and it does more than catch panics: the `select` on
`ctx.Done()` gives the host the ability to **abandon** a hung connector call and keep running — the other thing a
subprocess boundary gives you for free. The goroutine leaks until the call returns, which is the honest cost."

The parent document has **nothing** on panic containment or hang abandonment, and under constraint #3 it is not
optional: a subprocess gives both for free, so an in-process boundary that gives neither is not *behaviourally* the
same boundary. Two adapters that differ on "a connector panic kills the host" and "a wedged connector wedges the
pipeline" are not interchangeable no matter how identical their signatures. Add to D9: **the in-process adapter must
emulate the failure isolation of the out-of-process one, and the emulation cost (a goroutine per call, plus leak on
hang) is part of the design.**

---

## 17. The seven data-integrity invariants, and the in-code citation convention

**[NEW — the single most transplantable artifact in the dossier, and it has no counterpart in
`design-rules.md`.]** Conduit's `CLAUDE.md` states seven data-integrity invariants as a normative set (§9.1),
paraphrased here by subject rather than quoted in full: (1) never acknowledge upstream before durable downstream
handling, with ack propagation end-to-end and no intermediate early-ack for throughput; (2) positions are monotonic
and crash-safe, and serialization changes require a versioned migration path **and an upgrade test**; (3)
at-least-once is the floor — any path that could drop a record without delivering it or routing it to a DLQ is a
data-loss bug, *including error, shutdown and rebalance paths*; (4) ordering guarantees are per-source-partition
and documented, and a change that could reorder within a partition key requires a design doc and sign-off; (5)
state and checkpoint writes are atomic, torn writes impossible, and **every state feature ships with a
kill-mid-write recovery test**; (6) schema handling never silently mangles data; (7) shutdown is graceful by
default and `kill -9` at any instant is recoverable — "and we test exactly that."

Two things matter more than the list.

**The enforcement convention.** The instruction is: *"Where code upholds one of these, say so at the enforcement
site: `// Invariant 1: ack only after destination confirms durable write`."* And the dossier verified it is real —
`grep 'Invariant [0-9]'` across `pkg/connector/source.go`, `pkg/lifecycle/stream/source_acker.go` and
`conduit-connector-postgres/source/logrepl/combined.go` returns comments that cite the invariant **and the design
doc** at the exact line that upholds it. That is how a correctness property survives a refactor by someone who was
not there, and it is R8's principle ("drift is prevented structurally, not by discipline") applied to *invariants*
rather than to *tests*. `design-rules.md` R1–R13 are excellent and have **no in-code citation convention at all**;
adding one costs a sentence and a lint.

**The failure mode.** Invariant 6 names a `halt/DLQ/evolve` policy that does not exist in the code (§D8). So the
convention also demonstrates its own risk: an invariant list is a normative document, and a normative document that
references a mechanism which does not exist is R12 violated in a new place. The rule to add: **an invariant may
only reference mechanisms that exist; an aspirational one is labelled as a goal, not an invariant.**

Note also that several invariants are *test* requirements, not code requirements — "an upgrade test" (2), "a
kill-mid-write recovery test" (5), "we test exactly that" (7). That is the bridge to the next section.

---

## 18. Conformance and crash testing — an axis the parent document does not have

**[NEW] the parent document contains no testing or conformance strategy anywhere**, and Conduit's is its most
directly reusable engineering artifact for constraint #4 and for R10.

**A framework-owned conformance suite where the connector author implements a driver, not tests.**
`conduit-connector-sdk/acceptance_testing.go` (39 KB) exposes `AcceptanceTest(t *testing.T, driver
AcceptanceTestDriver)`, called from a one-function test in every connector (§13.3). The author implements the
*driver* — `SourceConfig`/`DestinationConfig` (documented as "a valid config for a source connector, reading from
the same location as the destination will write to"), `GenerateRecord(t, op)`, `WriteToSource`,
`ReadFromDestination`, `BeforeTest`/`AfterTest`, `ReadTimeout`/`WriteTimeout`, and `GoleakOptions` — with
`ConfigurableAcceptanceTestDriver` as a ready-made implementation for the common case. Four properties worth
copying exactly:

- **The tests cover contract points a connector author would never think to test**: `TestSpecifier_Exists`,
  `TestSource_Configure_RequiredParams`, `TestSource_Read_Timeout`, `TestDestination_Write_Success`, and so on.
- **`TestSource_Open_ResumeAtPositionSnapshot` *and* `TestSource_Open_ResumeAtPositionCDC` are separate cases**,
  because resuming mid-snapshot and resuming mid-stream are different code paths (§4). canal's D6/D7 make that
  distinction structural, so its conformance suite needs both from the start.
- **`GenerateRecord` produces mixed `RawData` and `StructuredData` in key and payload by default**, so a connector
  that silently assumes one representation fails conformance rather than production. That is the enforcement
  mechanism for D1's sealed-`Data` sum type.
- **Goroutine-leak checking is on by default** and a connector must *justify* an exemption via `GoleakOptions`.

The dossier's framing is the one that matters for canal's primary success criterion: "That is how you make
'implement the interface, register it, done' actually mean 'done'." Constraint #4 is a claim about what adding a
connector costs; a framework-owned conformance suite is the only thing that makes the claim checkable.

**[NEW] the conformance suite deliberately does *not* cover crash safety, and a second suite does.** That gap is
filled at the **engine** level by `tests/chaos` (`harness.go`, `child.go`, `upstream.go`) — SIGKILL/SIGTERM
mid-batch and mid-checkpoint, asserting the seven invariants on recovery — and it is a **graduation criterion** for
the v2 engine (§13.3). "The two suites are complementary and canal needs both: a per-connector conformance suite
the framework owns, and an engine-level kill-test suite."

And the datapoint that should settle any argument about sequencing: the ack-before-persist sev-0 (§D4) *"was found
by the first test ever written to look for it"*, and the postmortem notes "the bug predates this repo having any
chaos-testing infrastructure at all." R3 already makes "a checkpoint that survives `kill -9`" the first milestone;
this says the *test* is the milestone, not the checkpoint, because the checkpoint looked correct for years.

---

## 19. Traps — what Conduit tried that went badly

Ordered by value to canal. Each is a candidate addition to the parent document's Part 3.

**T1. Ack-before-persist: a sev-0 data-loss mechanism that survived years in a mature system, because the property
it depended on was invisible to the interface.** (§3.4, §13.2c; see §D4.) The engine told the connector "this
position is committed" *before* its own durable write landed. Benign for retention-based upstreams, unrecoverable
data loss for prune-on-commit ones — and nothing in `Source` distinguished them, so the postmortem had to classify
each connector by reading its source. **The trap is not the ordering bug; it is shipping a guarantee whose validity
depends on a connector semantic the interface cannot express.** Generalised: for every guarantee canal states,
ask which connector-side property it assumes, and make that property a declared capability.

**T2. The fix for T1 created a deadlock six days later, on the same seam.** (§4.4, §13.2d.) Deferring the upstream
ack until durable-persist collided with a snapshot iterator that gates its own completion on *every emitted record
being acked* — so a dropped deferred ack became "a permanent handoff deadlock + silent post-snapshot CDC loss."
The repair needed a per-source delivery goroutine, a bounded retry policy, a `tearingDown` flag to suppress
escalation during teardown, and ~200 lines of comments explaining lock ordering. **Two postmortems, six days
apart, on one seam. That is what retrofitting an ordering guarantee into an existing ack path costs** — and canal's
D6 recommendation 6 is the same coupling, so it must be designed with a durable-persistence gate (not an
ack round-trip), a timeout, and an escalation path.

**T3. `Read` *and* `ReadN` on one interface, negotiated by calling one and inspecting the error.** (§8.1, §13.2a;
see §D9.) Three filed SDK bugs, one of them a silent correctness failure (records emitted unencoded because
preferring `ReadN` bypassed a middleware's `Read` override), one a production `panic: send on closed channel`, and
one "fixed" with a log warning for a config combination that can wedge a pipeline indefinitely. Plus a ~6×
allocation penalty in the engine the batching was retrofitted into. **Batch is the primitive; single-record is
n=1. Never ship both.**

**T4. Two pipeline engines in the tree for years, the second introduced with no ADR, no completion criteria and no
committed benchmark.** (§13.2b.) The ADR that finally made it a decision is blunt: v2 "left it an open question
that silently taxes every data-path change: the metrics fix, the SIGTERM graceful-shutdown work, and the force-stop
ack-safety follow-up each have to be reasoned about in both implementations." v2 is still incomplete ("currently
supports only 1 source and 1 destination per pipeline") and still not the default, while v1's measured cost is ~69
allocations per record. This is R10 (scaffolding is labelled and tested against what it stands in for) and R12
(normative or draft, pick one) as a *process* trap: **a parallel implementation without completion criteria is a
permanent tax on every change to the thing it duplicates.**

**T5. Codec state as a private mutable field on the record value.** (§2.5, §13.2f; see §D2.) Non-thread-safe,
not copied by `Clone()`, error path silently falls back to a *different output format*. Four defects in one field.
**Codec selection is pipeline configuration.**

**T6. A source-shaped concern in the connector-agnostic envelope: `opencdc.file.*`.** (§2.4, §13.2g; see §D1.) Six
reserved metadata keys about *file chunking* in the shared spec namespace — constraint #1 violated in a shipped
spec, by the organisation that got the same question right elsewhere with `postgres.snapshot.resumed`. And the
namespaces are **unenforced**, so nothing prevented it. **Reserve namespaces with a check, not a convention.**

**T7. One missing boolean in a config-spec type became a security bug class.** (§7.2, §11.3, §13.2h; see §D10.) No
`Sensitive` flag on `Parameter` ⇒ redact-everything at the log site, a second redaction pass at the API boundary,
and then conduit#2640 — connector settings returned **unredacted** from one endpoint that was missed. Redaction
became a per-call-site discipline, and discipline failed.

**T8. Partial batch failure that cannot name the failures.** (§1.2, §9.3, §13.2e; see §D11.) `(n int, err error)`
is prefix-only; every nacked record — including the unattempted ones — carries the **same** error string; and the
boundary type degrades `error` to `string`, so the one real error loses its type and wrap chain. v2 has the right
per-record shape internally and the wire flattens it anyway. **Per-record outcomes must live in the boundary type
or they do not exist.**

**T9. The `unwrap` processor family is an admission that the envelope was not self-describing.** (§2.3, §9.4,
§13.2j.) Three processors whose only job is to un-nest a record that arrived as bytes containing an encoded record
— caused by `Data` never auto-converting, by the DLQ re-wrapping the original under `payload.after`, and by records
crossing format boundaries. **Ship one explicit conversion helper, and carry the original record in a DLQ envelope
rather than nesting it.**

**T10. An observability tap that silently dropped records, and a metric set with no record count.** (§11.1,
§11.2, §11.3, §13.2k–l.) The inspector dropped on full without saying so until a counter was added years later;
there is no records-in/records-out metric at all, so `StatusRunning` cannot be distinguished from "data is moving,"
and a fully stalled pipeline emits no histogram samples rather than alarming. **A tap that can lie about
completeness is worse than none; "up" and "moving" cannot be reconstructed from histograms later.**

**T11. A sandbox boundary treated as a serialization boundary.** (§10.5, §13.2p.) The WASM processor path forked
the plugin interface entirely (a separate wazero host-module ABI), then the Component Model was deferred by ADR,
and a 55 KB design doc plus an `egress` package with `ipguard.go`/`policy.go`/`secret.go` exists because a
sandboxed plugin that needs the network needs a whole capability-granting system. **Decide whether canal's future
out-of-process boundary is a subprocess or a sandbox; the costs are not comparable.**

**T12. A race documented in 2023 and still open, mitigated by a hand-rolled goroutine.** (§13.2n.) conduit#859,
"Possible race condition in destination" — a `DestinationNode` write racing a `DestinationAckerNode` teardown after
a nack **when no DLQ is configured** — with the mitigation living as a comment-annotated goroutine inside
`destination_acker.go`'s teardown. The lesson for canal: the nack path *with no handler configured* is the
least-tested path in every one of these systems and is where the deadlocks live. Exercise "nack with no DLQ" in the
chaos suite explicitly.

**T13. Protocol versioning is honest and costs 24 duplicated files per bump.** (§10.4, §13.2o.) Worth paying, and
worth knowing the price before designing a boundary that will be versioned. Reduce it by making the boundary
additive-only within a version, so bumps are rare.

**T14. Doc drift inside the tool whose purpose is preventing doc drift.** (§13.2q.) `specgen`'s own README shows a
one-argument `sdk.YAMLSpecification(specs)`; the real signature takes two. Small, and pointed: **generated
artifacts do not immunise their own hand-written documentation.**

---

## 20. What this addendum opens and closes

**Closed.**

- **Part 4 open question 1** ("Conduit is unread and it is the closest prior art that exists... re-run that
  research before freezing interfaces"). Done. D3's opacity rule is confirmed with better wording than the parent
  document had; D9 changes materially (§D9); D13 gains no corroboration (§D13).
- **`design-rules.md` open decision 9** (checkpoint state format compatibility across binary upgrades). Answered
  with a complete four-part contract plus a test requirement (§D3).
- **Whether an `Unimplemented*` embed makes Go interfaces additive-safe** (Part 4 question 1's sub-question).
  Yes, via an unexported marker method that makes the embed compulsory — and it changes D9's "never add a method
  to a required interface" from an absolute to a conditional (§D9).
- **What the gRPC boundary costs in typed-error fidelity** (the other sub-question). `error` degrades to `string`
  at the boundary, and Conduit pays that cost **in-process too**, deliberately, so the transports cannot diverge
  (§D9).

**Newly open, or open in a sharper form.**

1. **Does the `Coordinator` interface belong inside canal's `Runtime` at all?** (§D14.) Three interfaces plus a
   fencing token on every checkpoint write, with all coordination above the binary, versus D14's four with a
   `singleNode{}` no-op. Conduit's ADR argues the concept itself is the surface that gets grown.
2. **Is `Discover` required, or is infer-and-register-by-reference sufficient?** (§D8.) Conduit answers Part 4
   question 8 from the other side — the tax on catalog-less sources can be zero if you give up the catalog, the
   picker, drift-as-diff and per-stream mode validation. A real fork, not a detail.
3. **Does canal need a bounded, queryable DLQ store, and can that be deferred?** (§D11.) Conduit's UI can show DLQ
   *config* and nothing else; the store is explicitly deferred Tier-1 data-path work. canal's frontend goal says
   "what failed and why." Decide before the DLQ ships.
4. **Which snapshot-consistency family is canal's default?** (§D7.) Pin-a-consistent-read-snapshot (cheap, no core
   machinery, degrades on resume) versus watermark-and-reconcile (the eight-step engine, for sources that cannot
   pin). D7 assumes the second is the default; Conduit ships the first.
5. **Is canal's future out-of-process boundary a subprocess or a sandbox?** (§D13, T11.) The serialisation cost is
   shared; the capability-granting cost is not.
6. **Does the boundary need a second direction — host services the connector calls back into?** (§D8.) Schema
   registration forced Conduit to serve gRPC *to* its connectors, authenticated per instance. Secrets, metrics and
   error reporting are the obvious next three. Retrofitting a reverse seam into a one-way boundary is expensive.
7. **How is a *pattern* parameter expressed in a segment-addressed config spec?** (§D10.) `collection.*.format` is
   genuinely useful and is a string-keyed feature; D10's variadic path segments have no wildcard concept.
8. **Do canal's built-in connectors self-register at `init()`, and if so how is version integrity preserved?**
   (§D9.) Conduit trades one core edit per connector for `debug.ReadBuildInfo()`-resolved versions a connector
   cannot fake. The reconciliation — init-registration plus build-info version resolution — needs to be verified,
   not assumed.
9. **Does canal keep checkpoint *history*?** (§D3.) Conduit keeps exactly one position per connector and names
   what that forbids. The parent document's `Committables map[CheckpointID][]...` implies retained history without
   arguing for it.
10. **Do checkpoint and configuration share a durability domain?** (§D3.) Conduit stores the position *inside* the
    connector-instance record — one write, atomic, no separate offset store — and thereby couples the two.

**Unchanged by this addendum.** D2's framer/encoder split (Conduit has no framing concept — absence of evidence),
D6 item 10's heartbeat (no engine-level precedent), D13's recursive composition (no corroboration), and every
recommendation resting on Flink CDC's verified chunking internals, which Conduit neither implements nor contradicts.

