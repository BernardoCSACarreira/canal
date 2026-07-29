# Design rules

**Status:** normative. These constrain every architectural decision in canal.

**Provenance:** an earlier, abandoned attempt at this idea reached four services, a board-approved topology,
two RFCs and an OpenAPI contract before being discarded. It was not a failure of effort — it was a failure of
*sequencing*: it specified the operator-facing surface in enormous detail while the data path terminated in an
in-memory slice that nothing ever read. That codebase and its twelve design documents were surveyed before
deletion, and every rule below is a specific, observed consequence. They are stated as rules rather than
history because that is how they are meant to be used.

---

## R1. Topology is data, never schema

The abandoned attempt froze an eight-stage pipeline — `source`, `source_connector`, `source_event_buffer`,
`source_canonical_event_serializer`, `event_buffer`, `sink_event_serializer`, `sink_event_buffer`,
`sink_connector` — into its OpenAPI schema as `stages` with `minItems: 8, maxItems: 8`, and `ordinal`
constrained to `1..8`.

Adding a stage — a transform, a router, a second buffer tier — was therefore a **breaking contract change**.
Worse, the buffers were modelled *twice*: as stages 3/5/7, and again as "segments" keyed by
`followsStageOrdinal` 2/4/6 — one entity with two identifiers, and an enum (`kind`) with exactly one
permitted value.

**Rule:** a pipeline is a graph the engine composes; the API describes whatever stages exist. One
representation per entity. A fixed stage count is a design smell.

## R2. The canonical record model is decided first

Stage 4 was named `source_canonical_event_serializer`. **No document ever defined what the canonical form
was**, and no code implemented it. Stages 4, 6, 7 and 8 had zero implementation. The only record model that
existed was a wire-level HTTP DTO, which became the de facto internal type by default.

**Rule:** the internal record/envelope type is the spine of the system. It is decided first, it is independent
of any transport, and no transport DTO is ever allowed to become it by omission.

## R3. One end-to-end path before any breadth

Nothing consumed the event buffer. `ReadAfter` was called **only from tests**. The shipping data path was:
validate → dedupe → append to a mutex-guarded in-process slice → return `202`. No sink, no consumer, no
checkpoint in the binary.

Meanwhile the operator UI had a 792-line three-step wizard, a tier badge system with frozen copy, and an
accessibility-audited health panel.

**Rule:** the first milestone is one record moving from a real source to a real sink, durably, with a
checkpoint that survives `kill -9`. UI follows something true to display.

## R4. An acknowledgement means durable

The connector RFC instructed adapters to advance their upstream checkpoint on receiving `202`. That `202` was
emitted after appending to an **unbounded, unsynced, in-memory slice**, in a process with no signal handling
and no graceful shutdown. Restart meant silent total loss of everything buffered — *after* upstream had
committed its position past that data.

This was the most dangerous gap in the whole design: the documented advance rule and the actual durability of
the thing being acknowledged were unrelated.

**Rule:** if a stage cannot promise durability, it must not return a success the sender is told to checkpoint
on. Delivery semantics are a property of the implementation, not of the prose describing it.

## R5. Dedupe keys are scoped, and committed after the write

Two distinct bugs in one code path:

- The seen-set was keyed on the bare event `id`. Two connectors — or two tenants — emitting `1`, `2`, `3`, or
  a vendor offset, would silently discard each other's events as duplicates. The key must carry tenant and
  source.
- The id was recorded as seen **before** the append ran. Any failure in between made the event permanently
  unresubmittable: the mandated retry with the same id returns "duplicate", and the event is lost behind a
  healthy-looking `202`.

The set was also a 50,000-entry process-lifetime FIFO while the product docs described deduplication over a
"platform retention window". After a restart the same retry would instead be accepted and re-appended — so
the observable behaviour matched neither documented semantics.

**Rule:** dedupe state is scoped, durable, and committed after the data it protects. "Duplicate" must mean
"already durably stored", not "present in a RAM cache that may have evicted it".

## R6. A buffer without a rejection path is not a buffer

Buffer was defined as where records wait "(backpressure, batching, retries)". Its `Append` could never fail,
had no depth cap, no spill to disk, and — in the shipping implementation — no trim-after-commit and no
metrics, 2 of the 4 operations its own RFC mandated. The only compliant implementation lived in the service
scheduled for deletion. Two mutually incompatible buffer abstractions coexisted in one package: a
non-destructive cursor buffer and a destructive queue, with the demo pipeline exercising the one that
validated none of the real semantics.

**Rule:** bounded by construction. One buffer interface. Backpressure is an expressible outcome — block or
reject — not unbounded growth to OOM.

## R7. If the design says "retry the failures", the contract must name them

Adapters were required to retry "only the failed subset" of a partially accepted batch, and to checkpoint only
when the response "indicates no unresolved failures". The response schema was
`{accepted: int, duplicateIds: string[]}` — **no per-event error array existed**, and no partial-failure code
path existed in any implementation. Both rules were unimplementable as written.

**Rule:** write the failure shape at the same time as the success shape.

## R8. Conformance is asserted against real responses

CI validated the spec and never a response. Three implementations hand-mirrored the same assertions in three
idioms. Consequence: a nil slice was marshalled directly, so `duplicateIds` serialized as **`null`** against a
`required: array` field — and the test asserted `len(...) != 0`, which passes for `null`. The break was
invisible to CI. Byte-vs-UTF-16 length validation, body-size caps (4 MiB vs 1 MiB) and timestamp parsing
strictness diverged identically. The contract version string was hard-coded in five places with nothing
checking they agreed.

**Rule:** tests assert real responses against the schema. Shared constants are generated from one source.
Drift is prevented structurally, not by discipline.

## R9. One concept, one vocabulary

Three tiers acquired **four** string vocabularies — `1|2|3`, `tier-1|tier-2|tier-3`,
`tier1_synthetic|tier2_real|tier3_byo`, and `ingestion_path.tier_N_*` — plus a fifth commercial enum, glued by
hand-written cross-maps. The *two-namespace* split (implementation lane vs commercial SKU) was genuinely
justified and is worth keeping; the extra two layers were accidental.

**Rule:** one wire enum plus one i18n key namespace. A function mapping between two representations of the
same concept is evidence of a modelling error.

## R10. Scaffolding is labelled, and tested against what it stands in for

The setup wizard filtered adapters by tier. The only tier-2 catalog entry was a **sink** placeholder — so an
operator on the "tier-2 managed *source* connector" lane could only select a sink. The entire RFC that lane
implemented was about the source side. Deterministic scaffolding had silently become the product.

**Rule:** scaffolding is labelled as such and is exercised by a test asserting it matches the thing it stands
in for — or it does not ship.

## R11. One server-side runtime

The abandoned attempt ran a Go data plane, a Python control plane, a TypeScript scaffold that still owned the
only complete buffer and checkpoint implementations, and a React UI. Four runtimes produced: three `/health`
endpoints returning three different service names under a contract declaring one; error shapes that agreed in
Go and not in Python; and a path-based routing split that existed only in the frontend dev proxy, with no
documented production ingress.

**Rule:** one Go binary for everything server-side. TypeScript exists only in the browser. A second
server-side runtime requires a decision record in `docs/decisions/`.

## R12. Normative or draft — pick one

Both RFCs were `Status: draft` while the architecture document cited them with "MUST conform". A diagram file
declared the TypeScript edge canonical in one section while three others declared Go canonical. Normative
documents cited source files inside the service being deleted. A README linked a design doc that did not
exist. No ADR directory existed, despite policy requiring ADRs for exceptions.

**Rule:** documents are normative or draft, never both. ADRs live in `docs/decisions/` from the first commit.

## R13. State that implies persistence is persistent

The control plane stored operator bindings in an in-memory dict while returning UUIDs and
`createdAt`/`updatedAt` timestamps. Read-model stores handed back live mutable records. Nothing had
authentication, authorization or tenant scoping; "tenancy" was realized as "single OS user".

**Rule:** if the API shape implies durability, the store is durable. Return values, not pointers into state.
Tenancy is decided before the first multi-tenant field, not after.

---

## Ideas worth carrying forward

Not everything from that attempt should be discarded. These were sound:

- **Buffer and checkpoint are distinct concerns**, comparable on the same axes: ordering, retention, replay,
  tenancy, HA class. That matrix is a good way to weigh a local dev store against Kafka-class or cloud
  providers, and it correctly kept remote drivers as *conformance targets* rather than shipped dependencies —
  which stopped connectors baking in Kafka-only assumptions at the buffer edge.
- **The failure-mode taxonomy**: transient-upstream, transient-internal, permanent-upstream,
  permanent-mapping, permanent-contract, duplicate-idempotent-success, clock-skew — each with a prescribed
  behaviour (back off and retain progress / stop and go terminal / dead-letter without poison-retrying) and a
  distinct operator signal. This is a better error model than most shipping frameworks have, and should become
  real Go error classification.
- **Three layers of idempotency** — upstream vendor id → canonical record id → in-flight submit guard — plus
  the rule that a source with no upstream id must derive a **deterministic** one from stable fields and
  document the derivation.
- **Backoff as a policy table**: full-jitter exponential everywhere, honour `Retry-After` on rate limits,
  explicitly *no blind retry* on permanent errors, and sustained backoff must transition the connector to a
  visible `degraded`/`paused` state carrying a clear last-error string.
- **Honesty as a structural property of the UI.** The spec required, in fixed order: what works, what does
  not, any pilot qualifier, and the next action — and forbade implying a latency or availability commitment
  from a probe returning 200. It enforced this with machine-readable attributes on every badge so the copy
  could be asserted in tests. A metrics UI that cannot distinguish *the endpoint answered* from *your data
  arrived* is actively misleading, and this was the right mechanism against it.
- **Deterministic read-model fixtures** pinned in tests, so the shape the UI consumes cannot drift silently.
- **Dependency injection of the dedupe store** rather than module-scope global state — which the TypeScript
  edge had, leaking the set across every app instance and forcing tests to randomize ids to pass.

---

## Decisions the architecture must close

These were left open and should be answered rather than deferred again:

1. Buffer durability model — WAL vs segment-per-partition, and whether checkpoint state shares that WAL for
   crash consistency or stays independent.
2. Dedupe key scope and backing store, satisfying both durability and tenancy.
3. Backpressure signalling, including what a sender is told when the buffer is full.
4. The per-event partial-failure response shape (blocks R7).
5. The canonical record model (blocks R2).
6. Tenancy and authentication at every ingress.
7. A connector state machine — `healthy → degraded → paused → terminal` — with a last-error surface, and the
   API fields exposing it.
8. Clock-skew policy: clamp or reject, and where it is configured.
9. Checkpoint state format compatibility across binary upgrades.
