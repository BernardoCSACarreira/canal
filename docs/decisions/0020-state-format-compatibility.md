# 0020 — State format compatibility across binary upgrades

**Status:** accepted, normative. Closes `design-rules.md` open decision 9.

## Context

Open decision 9 asks what happens to persisted state across a binary upgrade. Every reviewed proposal specified
`(version, bytes)` and a serialiser handed "the version it was written with", and **none stated the policy on an
unrecognised version.** That gap is the whole decision: `(version, bytes)` without a policy is a version number
nobody knows what to do with.

Conduit's Postgres connector states a complete four-part contract, and its engine backs it with an invariant
("position serialization changes require a versioned migration path and an upgrade test"). One part of that
contract is the piece every proposal omitted, and it is the one that makes a **downgrade** survivable.

The prior art also supplies the failure to avoid: a connector that changed its cursor encoding and fed v1 bytes
to a v2 parser on upgrade, resuming at a wrong position — silently. One proposal gave a migration hook to exactly
one of its four blob kinds and none to the others, which its reviewer flagged as a major defect.

## Decision

**The contract applies at two levels and is identical at both:** canal's own `Checkpoint` envelope
(`Header.Format`) and every connector-authored `record.Blob.Version` inside it — lane specs, cursors,
committables, writer state, transform state. There is no blob kind without the contract.

**1. Additive-only encoding.** Every new field is `omitempty`, so state written by a newer build stays
structurally readable by an older one, which ignores unknown keys. The envelope is JSON; a connector may use
anything inside its own blob provided it obeys rules 2–4.

**2. Absent or zero version means legacy**, and legacy is treated as *"none of the newer fields were recorded —
behave exactly as the previous version did"* until a later event naturally populates them. Never "reject", and
never "assume the default is correct".

**3. NEVER reject state whose version is greater than the current one.** The format is additive-only, so a newer
record stays structurally readable, and rejecting it breaks the readable-by-N+1 rule that makes a binary
**downgrade** survivable. A field the reader does not understand is ignored **and reported** as
`Condition{Degraded, True, reason: state_written_by_newer_build}`, so the operator knows a rollback is running on
forward state.

**4. Stamp the version at serialise time, not at construction time**, so a parsed legacy record is silently
upgraded on its first write.

**5. Every change to a persisted format ships with an upgrade test.** Write with build N, read with N+1, write
with N+1, read with N, and assert a lossless round trip of everything both understand. **A format change with no
upgrade test does not merge.** This is invariant 2 of the seven, cited at the enforcement site.

**6. There is exactly one snapshot format**, always self-contained and relocatable. Flink's
canonical/native × aligned/unaligned matrix is an operator tax whose sharp edges ("aligned checkpoints are not
relocatable", "you cannot upgrade minor versions from an unaligned checkpoint") are avoidable by never creating
the matrix.

**7. The one legal way to break compatibility is loud.** A connector may declare its own blob version
*unreadable* by returning `fault.Contract` from its decode, with a message naming both versions. The operator's
remedy is to reset the lane through the offsets API. A fixed-width encoding — the trivial file source's
big-endian byte offset — is a legitimate case: a different version genuinely is unreadable and failing loudly is
correct. What is forbidden is failing *quietly*.

**8. A connector may declare which other connectors' state it adopts** via `StateAdopter.AdoptsStateOf()
[]string`, so a rewrite or a rename is a declaration rather than an operator runbook.

**9. The checkpoint header records which builds wrote the state** — `CanalVersion` plus a per-node connector
version map — so an operator can see what to roll back to. Built-in connector versions are resolved from
`debug.ReadBuildInfo()` keyed by the registering package's module path, so a built-in connector cannot lie about
its version.

**10. At restart, the cursor's age is checked against `SourceCaps.ReplayWindow` when declared**, refusing with
"source guarantees 6h; this cursor is 9h old" rather than silently starting a lossy stream. Format compatibility
and *upstream* retention are separate questions and both are checked.

## Alternatives rejected

- **`(version, bytes)` with no stated policy.** Rejected: it is the gap.
- **Rejecting a newer version.** The most natural instinct, and rejected: it makes every rollback a data
  emergency. Additive-only encoding is precisely what makes accepting it safe.
- **Rejecting an older version and requiring an explicit migration.** Rejected: it makes every upgrade a
  migration, and rule 2 plus rule 4 achieves the same result silently and correctly.
- **A migration hook per blob kind** (`MigrateState(ctx, blob, from) (Blob, error)`). Rejected as unnecessary:
  additive-only encoding plus legacy-means-previous-behaviour plus stamp-on-write *is* the migration, performed
  lazily and without a hook to forget to call. One proposal shipped such a hook for one blob kind out of four,
  which is the shape of the mistake.
- **Multiple formats, or a savepoint format distinct from a checkpoint format.** Rejected: never create the
  matrix.
- **A global schema registry for state blobs.** Rejected: the whole point of opacity is that the core does not
  interpret them, and Airbyte's uninterpreted `stream_state` is what let it add an entire new phase model with no
  wire change.
- **Silent best-effort parsing of an unknown blob.** Rejected: that is the connector-that-resumes-at-the-wrong-
  position failure.

## Consequences

- Positive: upgrade *and* downgrade both survive; a forward-state rollback is visible rather than silent; there is
  one format; the policy is identical for canal's own state and for every connector's, so nobody has to look it up
  twice.
- **Negative, accepted:** additive-only means a field can never be removed or repurposed, so a state format
  accretes. Removal requires a version bump with a declared-unreadable decode and an operator reset — which is
  loud and rare, and is the correct price.
- **Negative, accepted:** the upgrade-test requirement slows down state-format changes. Intentional.
- **Negative, accepted:** accepting a newer version means an older build may run with fields it ignores, so it can
  behave subtly differently from the build that wrote them. Mitigated by the `Degraded` condition naming exactly
  that situation, which is strictly better than a refusal to start.
