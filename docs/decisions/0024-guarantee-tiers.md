# 0024 — Guarantee tiers are which interfaces you implement, negotiated at submit time

**Status:** accepted, normative.

## Context

R4 says an acknowledgement means durable, and that delivery semantics are a property of the implementation and
not of the prose describing it. The question is how a pipeline's end-to-end guarantee is determined, expressed and
enforced.

The prior art's failure here has a name: **silent capability degradation.** Vector's acknowledgement negotiation
(`can_acknowledge` × `acknowledgements`) degrades to best-effort **with no error**, even though every input to the
decision is known at config time. Airbyte's protocol cannot express "durable", so a destination that buffers in
memory and yields state eagerly produces exactly R4's catastrophe and the protocol cannot say so. Kafka
Connect's `exactlyOnceSupport(config)` returns **null** meaning "assume unsupported" — a tri-state capability
encoded as a nullable enum.

Flink's answer is the good one: **the tier is which interfaces the connector implements.** And one reviewed
proposal added the honest counterpart: a `Negotiated` value returned from a pure `Build`, rendered on the submit
screen, so an operator sees what they got and why rather than what they asked for.

Two reviewed proposals were also found to name something exactly-once that was not: one had `EffectivelyOnce`
resting entirely on a sink returning field names, with the engine holding no dedupe state and no dedupe store
interface anywhere.

## Decision

**Four tiers, and the tier IS which interfaces the connector implements:**

| Sink implements | Tier | What the core does |
|---|---|---|
| `Sink` | `AtLeastOnce` | advance the prefix on a clean `WriteResult` |
| `+ Flusher` | `AtLeastOnce`, ack moved to `Flush` | settle on the flush that covers the request; `AckPoint` discloses it |
| `+ WriterState` | `AtLeastOnce`, resumable in-progress work | restore writer state at `Open` |
| `+ Committer` | `ExactlyOnce` via 2PC | subsuming-contract ready/pending sets inside the checkpoint |
| `+ TokenSink` | `ExactlyOnce`, one durability domain | recover canal's token from the destination |

`EffectivelyOnce` sits between: `AtLeastOnce` plus `SinkCaps.Idempotent` plus `SourceCaps.StableKeys`, so
duplicates are absorbed at the destination — **and it is backed by engine-owned keyed dedupe with a durable
scoped store** (ADR 0002), not by a sink returning field names.

`AtMostOnce` exists as an **explicitly chosen** downgrade (settle on hand-over), never as a silent degradation.

**The negotiated guarantee is `min(source, sink, buffer, requested)`, computed by a pure function of config before
anything starts.** `engine.Build` does no I/O beyond the optional `Validator` tier, and it returns:

```go
type Negotiated struct {
    Guarantee      spec.Guarantee
    Why            []string      // one sentence per factor
    DurabilityEdge string        // "sink:s3" or "buffer:wal" — where an ack is earned
    AckPoint       string        // "write" | "flush" | "commit" | "token"
    ReplayBudget   int
    Defaults       []DefaultNote // R10: every core-supplied value labelled with its origin
    Downgrades     []Downgrade
}
```

**An impossible pipeline is REFUSED at submit time**, with per-field diagnostics, each naming the Go interface
that would fix it. The full refusal table is in architecture.md §15.3; every row is paired with the prior-art
failure it prevents.

**Silent degradation is impossible, and the escape hatch is a signed waiver.** If the requested tier exceeds what
the components support, the pipeline is refused. The operator may instead record a `Downgrade`:

```go
type Downgrade struct {
    Requested, Effective string
    Missing              []string  // capability NAMES, never iota ids
    Node                 record.NodeID
    AcknowledgedBy       string
    AcknowledgedAt       time.Time
    Reason               string
}
```

It is **operator-signed, durable, config-declared**, the core can never mint one itself, and it raises
`Condition{Degraded, True}` **for the pipeline's whole life**. The operator says "yes, I know, run it anyway",
once, visibly, and the UI never stops saying so.

**The Checkpoint Subsuming Contract is implemented verbatim for the 2PC tier**, including *abort is not
discard*: notifications are not guaranteed to arrive; ids strictly increase and a higher id subsumes every lower
one; an implementation must behave as if a non-confirmed checkpoint never happened; abort means "the next
successful checkpoint covers a longer span".

**No terminal outcome silently discards data.** The five `Disposition` values include `DeadLetter`, which routes
the covered records and does **not** advance the prefix past them. Flink's `signalFailedWithKnownReason` is
documented as "only logs the error, discards the committable and continues", and canal does not copy it.

**Committables expire.** `Committable.Expires` plus `AbortStale` plus a `Degraded` condition on expiry, because
silent skipping after a transaction timeout is not honest treatment.

**Duplicates on restart are the default at `AtLeastOnce`, written down next to the code that causes them.** Flink
CDC's in-source comments — "this behaviour downgrades the delivery guarantee to at-least-once", "only the
intersection of both is exactly-once" — are the standard, and `canal_lane_replay_records` is the live number.

## Alternatives rejected

- **A config enum declaring the guarantee, with the runtime doing its best.** Rejected: it is the silent
  degradation, and it makes R4 prose again.
- **A boolean or tri-state capability method** (`exactlyOnceSupport(config)` returning null). Rejected: a
  nullable enum for a tri-state, and it forces `Build` to instantiate a connector to negotiate.
- **`EffectivelyOnce` implemented by the sink alone.** Rejected: the engine must own the dedupe state, or the tier
  is a claim rather than a mechanism.
- **Refusing without any waiver mechanism.** Rejected: an operator with an append-only sink and an at-least-once
  reality needs to be able to run, and forcing them to lie in config (by requesting a lower tier and forgetting
  why) is worse than a signed, permanently-visible waiver.
- **Letting the core mint a downgrade automatically with a warning.** Rejected: a warning is what Vector emits,
  and it is invisible three weeks later. A signature and a permanent `Degraded` condition are not.
- **Naming the ack-point choice a `bool` on the sink** (`Durable bool` on a write result). Rejected as a major
  defect: it lets a buffered sink advance the prefix for records still in RAM, and a bool is not checkable.
  `Flusher`'s *presence* is checkable.
- **Omitting exactly-once entirely** (as the rooted proposal did, offering three tiers and declining to name
  anything exactly-once). Genuinely tempting for a small core, and rejected because the pending set already lives
  in a durable record written atomically, so 2PC is a bounded addition rather than a new subsystem — and because
  the difference between "canal can never promise exactly-once" and "canal can, when the sink stages writes" is a
  goal-level difference.

## Consequences

- Positive: sinks stay trivial at tier 1 and the machinery appears only for connectors that opt in; R4 becomes a
  type obligation; degradation is either refused or signed; the operator sees the derivation, not a badge.
- **Negative, accepted:** the 2PC tier is real core machinery — a pending set in the durable record, an ordering
  contract, expiry, `AbortStale`. Bounded, opt-in, and invisible to a tier-1 sink.
- **Negative, accepted:** `Negotiated` is a new thing operators must read on the submit screen. That is the point.
- **Negative, accepted:** a signed `Downgrade` can be signed thoughtlessly. Mitigated by the permanent `Degraded`
  condition and by the actor being recorded and audited.
- **Negative, accepted:** `min(...)` means one weak component caps the pipeline, which will surprise someone whose
  source is exactly-once-capable and whose sink is not. The `Why` list names exactly which factor bound it.
