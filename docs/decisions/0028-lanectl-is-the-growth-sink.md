# 0028 — `LaneCtl` is the growth sink for lane-table needs, and `LaneView` is its read-only half

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

Six of eight hostile connectors needed something from the lane table that `LaneCtl` did not offer, and the
six needs were genuinely different:

| Need | Case | Severity |
|---|---|---|
| enumerate every lane cluster-wide, with ranges and finish state | 32-way chunked scan's watermark protocol | fatal |
| see finished and gated rows, and re-announce a returned stream | 900 churning streams | fatal |
| announce N lanes in one durable write | 900-stream cold start (1800 serialised fsyncs); 32-way chunk plan | major ×2 |
| install a lane's initial cursor after the lane exists | tail lane seeded from the scan's high watermark | major |
| drop a finished lane's row | churn (measured 1800 → 1840 rows in one cycle) | major |
| observe admission pressure before handing a batch over | push source, 601ms to produce a knowable 503 | major |

A seventh case — ADR 0008's prescribed `shadow` transform — needed the *read* half from a
`TransformRuntime`, which has no `Lanes()` at all.

The tempting shape was six new optional interfaces on `Source`, each with a `SourceCaps` flag, following
ADR 0013's growth mechanism. That would have been the wrong reading of ADR 0013.

## Decision

**Grow `LaneCtl`. It is core-implemented and injected, so growth there costs connector authors nothing.**

Added:

- **`LaneView`**, a one-method read-only interface (`Table(ctx, LaneQuery)`), embedded in `LaneCtl` and
  returned from `TransformRuntime.Lanes()`.
- **`LaneCtl.AnnounceMany`** — N lanes, one durable write, all-or-nothing.
- **`LaneCtl.Seed`** — install a lane's initial cursor; `PermanentContract` if it already has one.
- **`LaneCtl.Forget`** — drop a *finished* lane's row and state; `PermanentContract` if it is not finished.
- **`LaneCtl.Admission`** — headroom, blocked flag, and a `Ready` channel to select on.
- **`LaneQuery`** — pagination plus stream, kind, group, finished and gated filters.

Two rules changed rather than gaining methods:

- **`Announce` with a changed `Spec`** is `PermanentContract` when the lane has a durable cursor and is not
  finished, and **accepted** when the lane is finished or cursorless. A stream that disappeared and came
  back, a chunk re-planned before anything was read, and a full-refresh pass re-deriving its range are all
  the second case, and refusing them stopped the pipeline for a re-announcement that was correct. The
  acceptance emits `EventLaneAnnounced` with both `Spec` versions, so the rewrite is visible.
- **`MaxLanes` governs GROWTH, not RECOVERY.** It is hard-enforced for a NEW lane and never fails a
  re-announcement of a lane that already exists durably. Enforcing a per-binary constant against a shared
  durable lane plan means a rollback to a narrower binary cannot read the plan it is holding, which turns a
  rollback into an outage.

## Why not six optional interfaces on `Source`

ADR 0013 names **two** unlimited growth channels, and both are things *the core implements or owns*: a
runtime interface, and a field on a request struct. `LaneCtl` is the first kind — it is handed to the
connector through `SourceRuntime.Lanes()` and no connector implements it. Adding a method there breaks
nothing, requires no `Caps` flag, requires no registration cross-check, and requires no type assertion at
the one assertion site.

An optional interface on `Source` is the right mechanism when the *connector* must supply behaviour. Every
one of the six needs above is the connector *asking the core a question*. Six optional interfaces would have
meant six `SourceCaps` flags, six `ResolvedSource` fields, six cross-checks and six nil-checks in the engine,
to express "the source would like to read the table it already writes to".

The cost is real and named: `LaneCtl` goes from six methods to eleven, and the future out-of-process adapter
must forward eleven. That adapter is written **once, by canal**, and ADR 0015's whole point is that the
in-process and remote paths must be byte-identical — which they are, because every added method takes and
returns plain serialisable values, except `Admission.Ready`, which follows the precedent `Changes()` already
set.

## The `TransformRuntime.Lanes()` half

A transform must be able to *see* the plan and must not be able to *change* it. ADR 0008's `shadow`
transform cannot self-retire without knowing how many scan lanes exist and whether they have finished, and
it must not be able to announce one. `LaneView` is that line, drawn as a type rather than as prose.

Embedding `LaneView` in `LaneCtl` is not the forbidden nesting: that rule (ADR 0013, and `WriterState` not
being embedded in `Sink`) is about *component* interfaces, where embedding forces every implementer to
satisfy the union. Here nothing outside the core implements either, so the union costs nobody anything and
buys one spelling for one concept.

## Consequences

- Two fatal and five major breakages close inside one interface that no connector implements.
- `LaneAssignment` gains `FinishedAt`, `GatedOn` and `Worker`, because `Table` returning rows a source cannot
  interpret would be a half-fix.
- The engine owes a paginated durable lane-table query, a bulk announce transaction, a row delete, and an
  admission-pressure snapshot with an edge. All four are cheaper in the core than the workarounds the six
  connectors were writing.
