# 0019 — Phase plus conditions, and the honesty invariant

**Status:** accepted, normative. Closes `design-rules.md` open decision 7.

## Context

Open decision 7 asks for a connector state machine — `healthy → degraded → paused → terminal` — with a
last-error surface and the API fields exposing it.

A single enum provably cannot describe the state operators actually care about: *running, healthy connection,
forty minutes behind, sink returning 429s.* A product of enums is a combinatorial nightmare. Kafka Connect's
status API structurally cannot answer "did my config change take effect?", and it has no terminal `Completed`,
so a finished batch job looks identical to a stalled stream.

The abandoned attempt got one thing exactly right and it must be preserved: **honesty as a structural property
of the UI.** Its spec required, in fixed order, what works, what does not, any pilot qualifier and the next
action, and it *forbade* implying a latency or availability commitment from a probe returning 200 — enforced with
machine-readable attributes on every badge so the copy could be asserted in tests.

Conduit's metric surface is the counter-example: no phase, no progress, no records-in/records-out at all, so
"running" cannot be distinguished from "data is moving", and a fully stalled pipeline emits no histogram samples
rather than alarming.

## Decision

**Kubernetes-shaped: one `Phase` plus orthogonal `Condition`s.**

```
Phase ∈ { pending, starting, running, paused, draining, stopped, failed, completed }

ConditionType ∈ { configured, assigned, source_connected, sink_connected,
                  progressing, caught_up, backpressured, degraded, spec_applied }
Status ∈ { unknown, true, false }
```

`Condition{Type, Status, Reason, Message, LastTransitionTime, ObservedGeneration}`. Nine types times three
statuses is 27 bounded series per pipeline, so conditions are legitimately metrics as well as read-model fields
— which makes "my config change silently did not apply" an *alert* rather than a mystery.

**`Completed` exists** and is the terminal success of a bounded pipeline.

**The legacy vocabulary maps on with no second enum and no cross-map function (R9):**

| Asked-for state | Is |
|---|---|
| `healthy` | `PhaseRunning` with no `Degraded: True` |
| `degraded` | `PhaseRunning` with `Degraded: True`, a closed `Reason`, and `LastFault` populated |
| `paused` | `PhasePaused` — operator-initiated, or auto-paused after sustained backoff |
| `terminal` | `PhaseFailed` |

**THE HONESTY INVARIANT, and it is asserted by a test:**

> `source_connected: True` **must never be able to imply** `progressing: True`.

`Progressing` is computed from the **durable cursor** advancing — not from a connection, not from records read,
not from a successful write. A fixture in which both connections are healthy and the durable cursor has not moved
for an hour must render as unhealthy. A metrics UI that cannot distinguish *the endpoint answered* from *your
data arrived* is actively misleading, and this separation is the machine-readable form of that rule.

**`ObservedGeneration`** on the document and on every condition answers "did my change take effect?".

**`Draining` carries `StoppingSince` and `DrainDeadline`, and *drained* versus *drain-timeout* are distinct
events**, because the second means records may replay.

**The last-error surface is `LastFault{Class, Blame, Op, Node, Lane, User, Dev, At, Attempts}`.** `User` names the
count, the component, an example, the fix and the consequence; `Dev` is for the log. `Blame` is the three-value
projection so the UI can group by "your config / their system / canal".

**`Complete: false` plus `Missing: [worker-3]`** when the aggregator has not heard from every worker. A status
document that silently omits a worker is the same lie as a health check returning 200 for a broken pipeline.

**Every unknown is a nil pointer, and a pinned fixture in which every optional field is absent asserts the UI
renders no zeros.** So "the connector cannot tell you the lag" never displays as "the lag is zero".

**`canal_checkpoint_age_seconds` is the primary alert signal**: always available, unfakeable, and one alert
catches every stall mode.

**The document is k8s-operator-ready without building an operator:** `Phase` + `Conditions` +
`ObservedGeneration` maps onto a CRD status subresource with no redesign.

## Alternatives rejected

- **A single connector state enum**, as asked for literally. Rejected: it cannot express the four-fact state
  above, and every system that shipped one grew a parallel "reason" string that became the real state.
- **A product of enums** (`health × phase × assignment`). Rejected: combinatorial, and every combination needs a
  rendering.
- **Per-connector status shapes.** Rejected: it is per-connector frontend code, which is the thing the
  architecture exists to avoid.
- **Deriving `progressing` from records written or from a successful write.** Rejected: that is exactly the
  conflation the honesty invariant forbids — a sink can accept records forever while the cursor never advances,
  if settlement is blocked.
- **Conditions as free-form strings.** Rejected: they are metric labels and i18n keys, so the type set must be
  closed. `Reason` is a closed vocabulary for the same reason; `Message` is free text.
- **Omitting `Completed`.** Rejected: Kafka Connect lacks it and a finished batch job is indistinguishable from
  a stall.
- **Histogram-only observability.** Rejected on Conduit's evidence: "up" and "moving" cannot be reconstructed
  from histograms later, and a stalled pipeline emitting no samples fails silently.

## Consequences

- Positive: the operator-facing state is expressive and bounded; "did my change apply" is answerable; the
  honesty rule is a test rather than a style guide; the document maps onto a CRD.
- **Negative, accepted:** nine condition types is more surface than one enum, and the UI must render a list
  rather than a badge. Mitigated by `Phase` being the badge and conditions being the detail.
- **Negative, accepted:** computing `Progressing` from the durable cursor means a healthy idle pipeline with no
  new data reads as `Progressing: False`. Resolved by `CaughtUp: True` being the accompanying condition, and by
  `Heartbeater` advancing the cursor on an idle lane — which is one of the reasons heartbeats exist.
- **Negative, accepted:** 27 condition series per pipeline is real cardinality at high pipeline counts. Bounded
  and closed, and the alternative is an unalertable status document.
