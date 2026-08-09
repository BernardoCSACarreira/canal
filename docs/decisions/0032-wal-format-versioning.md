# 0032 — The WAL container is versioned, and a build reads one step back

**Status:** proposed. Merging this document to `main` is its acceptance, after which it is
normative. Drafted with both options argued so the decision is a review, not a blank page; the
recommendation is the Decision below.

## Context

[`pkg/store/wal`](../../pkg/store/wal) is the durable `StateStore` of the standalone deployment:
lane rows, cursors and every checkpoint live in one append-only file with an 8-byte magic, a 2-byte
format version, and CRC32C-framed batches ([format.go](../../pkg/store/wal/format.go)). The payload
is a REDO log in a positional binary layout — op, count, then key/value/version/epoch in order — a
torn tail is truncated rather than refused, and a compaction pass already rewrites the whole live
set when garbage crosses a ratio.

`Open` refuses any format version but its own, in both directions
([wal.go](../../pkg/store/wal/wal.go)). The refusal itself is defensible and its reason is now
written where it happens: a positional reader that meets a field it cannot parse cannot skip it
either, so every byte after it is garbage it would apply as a redo record. [ADR
0020](0020-state-format-compatibility.md)'s ignore-unknown-keys mechanism does not transfer —
it works because 0020's envelope is JSON, and 0020 governs the payloads carried INSIDE this store's
values, never the store's own container. Its rule 3 ("never reject state whose version is greater")
is scoped to artifacts a reader can carry opaquely; a container the decoder cannot parse carries
nothing.

What no document decided is what the refusal implies: **a format bump strands every existing log.**
Refusing older versions rules out read-forward migration; refusing newer ones makes every rollback
after an upgrade a cold start. That stayed abstract exactly until it did not: a delete's epoch is
not persisted — the frame carries the key alone — so a delete that raised a key's fence floor above
every prior write for that key does not survive a restart. That is a real, pinned gap in the
revocation fence (the STILL-TO-DO note in
[internal/engine/runtime.go](../../internal/engine/runtime.go) and the comment on the delete path
in wal.go both name it), and fixing it needs a new frame shape. The first format change is
therefore already queued, which forecloses the premise that the format will simply never change.

This is the roadmap's B1, and it gates M1's store work: a durable `Coordinator` next to this store
implies schema-evolution competence somewhere (completeness audit G11), and this store is where the
pattern gets set.

## Decision

Six rules — ADR 0020's shape, applied at the container level where its JSON mechanism cannot reach.

1. **The file-header version governs the container**: the framing, the CRC, the payload's
   positional layout, and which fields a write or delete record carries. The bytes inside a `value`
   remain ADR 0020's artifacts under ADR 0020's rules. One version per file; frames within a file
   are uniform.

2. **A build reads exactly two versions — its own N and N−1 — and writes only N.** The window is
   deliberately one step wide: two decoders is a bounded, testable surface on the durability path;
   an unbounded set is an archaeology project.

3. **Migration is by rewrite, at Open.** Opening a log replays the whole of it into memory
   regardless of version, so reading N−1 costs only the second decoder; the store then compacts —
   eagerly at Open for a version behind, or at the next ordinary compaction — and the rewrite emits
   format N. A log touched by build N converges to N, so stepping N−1 → N → N+1 works build by
   build with no external tooling.

4. **Outside the window, refuse loudly and specifically.** Older than N−1: name the version found,
   the versions this binary reads, and the way forward — open the log once with each intermediate
   build. Newer than N: the container analogue of 0020's `state_written_by_newer_build` — name it,
   so a rollback conversation starts from facts rather than from a corrupt-looking error.

5. **Every bump ships three things in one change**: the N−1 reader; an upgrade test that writes a
   log with the N−1 writer fixture and opens it with N, asserting the semantics survived (0020's
   mandatory-upgrade-test discipline); and fuzz coverage over both decoders — the completeness
   audit already names the WAL reader as an unowned fuzz target.

6. **The first exercise is version 2: a delete record carries its epoch.** A V1 log reads under
   legacy semantics — a delete with no recorded epoch raises no durable floor, which is exactly
   what a V1 log meant when it was written (absent-means-legacy, 0020's own posture). This closes
   the pinned floor-survival gap, and it proves rules 2–5 the day they exist rather than leaving
   them declared and unexercised — ADR 0031's rule, applied to a policy instead of a capability.

## Alternatives rejected

- **One frozen version — strictness as the price of a redo log.** The strongest case for it:
  version dispatch on the durability path is real complexity, one decoder is one correctness
  argument, and folding the delete epoch in NOW, before any deployment exists, would start the
  freeze from a complete format. Rejected because its premise — no further pressure after this one
  — was falsified once before the policy could even be written; because M1 puts rolling upgrades
  and a second store next to this one; and because its cost lands on whoever operates canal later:
  "a format bump is a cold start" is a sentence written today and paid by someone else. What
  survives of it is the one-step window in rule 2 — strictness bounded, not abandoned.

- **Self-describing frames — skip what you cannot parse.** Tagged fields per record, so any reader
  skips unknown ones at any version distance. Rejected: it buys arbitrary-distance compatibility
  this store does not need (rule 3 converges every log within one open), at the price of making the
  redo path tolerant of half-understood records — and "apply what you understood" is precisely the
  wrong disposition for a redo log, where the field you skipped can be the epoch that fences a
  write.

- **Sniffing — decode-by-try with fallback.** A CRC-valid frame that parses two ways is a format
  design failure, and "it parsed" is not evidence of "it meant this". The header states the version
  so that nothing is ever inferred from payload bytes.

- **Migration by external tool only.** Keeps the binary single-version; a `canal-migrate` rewrites
  logs offline. Rejected as the primary path: it reintroduces the operational cliff at every
  upgrade, while rule 3 performs the same rewrite in-process with the file lock already held.
  Nothing forbids building the tool later for multi-step jumps; rule 4's refusal message is where
  it would be named.

## Consequences

- The delete-epoch fix moves from "blocked on an undecided policy" to ordinary work: format version
  2 under rule 6. The STILL-TO-DO note in `runtime.go` and the two-ways-forward paragraph in
  `wal.go` resolve with that change, not with this document; until it lands, `Open`'s behaviour is
  unchanged, because V1 is both N and the only version there is.
- Two decoders, one upgrade test and two fuzz targets become the standing price of every bump —
  deliberately, so bumps stay rare and argued rather than casual.
- A downgrade after an upgrade that already rewrote the log is a loud, named refusal instead of a
  silent cold start — rule 4 exists for exactly that conversation.
- The roadmap's B1 closes, and M1's store-schema question (audit G11) inherits the pattern: a
  version marker, a one-step window, migrate-on-open. The Postgres store's DDL migrations still
  need their own decision, but not their own philosophy.
