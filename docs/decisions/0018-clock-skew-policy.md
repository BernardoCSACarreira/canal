# 0018 — Clock-skew policy: clamp or reject, configured per pipeline

**Status:** accepted, normative. Closes `design-rules.md` open decision 8.

## Context

The failure taxonomy carried forward names `clock-skew` as one of its classes, and the open decision asks:
clamp or reject, and where is it configured?

The question is real because canal's read model exposes event-time lag, and event-time lag is a subtraction
between a source-supplied timestamp and a local clock. A source whose clock runs ten minutes fast produces
negative lag; a source with a nonsense epoch (year 1970, year 2106, a millisecond value parsed as seconds)
produces lag in decades. Either poisons every graph and every alert built on the series, and neither is a
record-level correctness problem.

The prior art offers nothing verified here, which is itself informative: no surveyed system has a clock-skew
policy, and several ship an event-time lag metric anyway.

Two things make silently clamping wrong on its own: a clamped timestamp is a *modified record*, and a pipeline
that quietly rewrites data has violated the honesty rule the whole read model is built on. Two things make
rejecting wrong as a default: a mis-parsed timestamp is a mapping bug the operator wants to see once, not a
reason to dead-letter a million records at 3am.

## Decision

**Configured per pipeline, in `spec.Spec.Clock`:**

```go
type ClockPolicy struct {
    MaxSkew   time.Duration  // how far a source timestamp may LEAD the local clock. 0 disables the check.
    Behaviour ClockBehaviour
}

type ClockBehaviour uint8
const (
    ClockClamp  ClockBehaviour = iota // DEFAULT: clamp to now, Meta.NoteChange the field, count it
    ClockReject                       // fault.ClockSkew; the record routes on the node's Failed edge
    ClockPass                         // accept verbatim; count it
)
```

**Default `ClockClamp` with `MaxSkew` five minutes** — and clamping is *never silent*:

1. the field is clamped to `now`;
2. `Meta.NoteChange(FieldChange{Path: "event_time", Kind: FieldRounded, Detail: "clamped, skew 14m22s"})` is
   recorded on the record, so the modification travels with the data;
3. `canal_faults_total{class="clock_skew"}` increments;
4. the first occurrence per lane per generation emits an `Event` with the observed skew, so it appears in the
   read model's recent-events ring rather than only in a counter.

**The check applies to `Record.EventTime` and to `Change.CommitTime`, and to nothing else.** It does **not**
apply to `Position.At`, because that is canal's own observation timestamp taken from canal's clock, and it does
not apply to any payload field, because canal does not know which payload fields are timestamps and guessing
would be a Mongo-shaped assumption in the core.

**Only leading skew is checked.** A timestamp *behind* the local clock is indistinguishable from legitimate
backlog — that is the entire premise of event-time lag — so there is no lower bound and no "too old" rule.

**`ClockReject` routes on the failed edge**, so it obeys ADR 0012: a rejected record is dead-lettered with its
full provenance, never dropped.

**Where it is enforced:** in the engine, immediately after admission, before any transform. One site, so a
transform cannot reintroduce skew and a codec never sees an unclamped value.

**Event-time lag is omitted, not negative.** If a lane's most recent event time exceeds `now`, the
`eventTimeLagSeconds` series is **omitted for that sample** rather than emitted as a negative number. A negative
lag is not a measurement.

## Alternatives rejected

- **Clamp silently.** Rejected: it is a pipeline rewriting data without saying so, which is what
  `Meta.NoteChange` exists to prevent.
- **Reject as the default.** Rejected: a mis-parsed timestamp would dead-letter an entire stream, which is a
  much larger blast radius than a wrong graph.
- **No policy; emit whatever the source said.** Rejected: it poisons every event-time series and every alert
  built on one, and `ClockPass` exists for the operator who genuinely wants it.
- **Per-connector configuration.** Rejected: skew is a property of a *deployment's* clocks, not of a connector,
  and per-connector placement would mean N places to fix one problem. A source that genuinely needs its own
  policy is a node with its own `clock` stage-standard field — deliberately not shipped in v1.
- **Per-stream configuration.** Rejected: same reason, one level finer.
- **NTP-style clock-offset estimation between canal and the source.** Rejected: it is a distributed-clocks
  research problem, and canal has no channel to measure a source system's clock.
- **Checking a "too old" lower bound.** Rejected: indistinguishable from backlog.
- **Clamping payload fields.** Rejected: constraint #1.

## Consequences

- Positive: event-time series cannot be poisoned by one bad source; every clamp is attributable to a field, a
  lane and a count; the choice is the operator's and is never made silently; the enforcement is one site.
- **Negative, accepted:** the default modifies data. Mitigated by the modification being recorded on the record,
  counted, and surfaced as an event, and by `ClockPass` existing.
- **Negative, accepted:** `MaxSkew` is a guess, and five minutes will be wrong for someone. It is one config
  field with a documented meaning.
- **Negative, accepted:** an omitted lag sample looks like a gap in a dashboard rather than a warning. Mitigated
  by the `clock_skew` fault counter being the thing to alert on, which is deliberate: omitting an unmeasurable
  series and alerting on the reason is the discipline the whole metric set follows.
