# 0001 — Buffer durability is a domain, and the checkpoint does not share the WAL

**Status:** accepted, normative. Closes `design-rules.md` open decision 1.

## Context

R6 exists because the abandoned attempt's buffer had an `Append` that could never fail, no depth cap, no
spill, no trim-after-commit and no metrics, and because two mutually incompatible buffer abstractions — a
non-destructive cursor buffer and a destructive queue — coexisted in one package while the demo pipeline
exercised the one that validated none of the real semantics.

Two questions were left open: WAL versus segment-per-partition, and whether checkpoint state shares that
WAL for crash consistency.

A third question turned out to matter more, and it was raised as a **fatal** defect against the design this
architecture is rooted in: a buffer that declares itself "durable" shortens the acknowledgement chain, so
the source is told its data is safe once the buffer accepted it. But a WAL in one pod's data directory is
durable against *process* loss and worthless against *node* loss or lane reassignment — while the commit it
authorises is global. A `kill -9` conformance test proves process durability and says nothing about the
case that loses data.

## Decision

**1. Durability is a domain, not a boolean.**

```go
type Durability uint8
const (
    DurabilityNone    Durability = iota // memory
    DurabilityProcess                   // survives a crash of this process
    DurabilityNode                      // survives a process crash; lost with the node
    DurabilityCluster                   // survives node loss; readable by another worker
)
```

A buffer may shorten the acknowledgement chain **only if its domain is at least as wide as the lane's
assignment domain**, and the check runs at submit time with a diagnostic naming both. In `canal run` lanes
never move, so `Node` suffices. In `canal serve` only `Cluster` suffices.

**2. No shipped buffer declares `DurabilityCluster`.** A cluster-durable buffer is a distributed log and
canal is not reimplementing one. The consequence — in coordinated mode the durability edge is always the
sink — is disclosed in `Negotiated.DurabilityEdge`, not hidden.

**3. The core, not the buffer, performs the settlement.** A buffer cannot lie its way into early
acknowledgement because it does not settle anything; it returns `Accepted{Records, Refused}` and the engine
decides.

**4. Segments per lane, not one shared WAL.** `<DataDir>/<pipeline>/<lane>/<segment>.log`, each record
length-prefixed with a CRC32C, segments capped by size and age, plus a small `INDEX` recording the highest
fully-durable group per segment. Per-lane segmentation makes `Trim(through GroupID)` an unlink rather than a
compaction, and makes a lane's buffer directory the natural unit if reassignment is ever wanted.

**5. `Get` is non-destructive; `Trim(through)` reclaims.** One model, not two.

**6. The checkpoint does NOT share the buffer's WAL.** Crash consistency comes from **explicit ordering**
instead: for an ack-shortening buffer, `Put` must be durable before the group is settled, and the phase-two
checkpoint write is strictly downstream of that settlement. The checkpoint therefore can never reference
data the buffer does not hold.

**7. `CanDecode(version) bool` is checked before decoding a segment**, so an upgrade that changed the
segment format fails loudly at startup rather than misparsing bytes.

## Alternatives rejected

- **A shared WAL for buffer and checkpoint.** Rejected because the two have different durability domains — a
  buffer is worker-affine by construction while the checkpoint must be reassignable — and because it would
  make the commit protocol's correctness depend on which buffer is configured, so swapping the buffer would
  silently change the guarantee.
- **`Durable bool`.** Rejected: it is the fatal defect above. A bool cannot express the difference between
  the case that is safe and the case that loses data, and the test that would catch the difference
  (`kill -9`) passes for both.
- **A single WAL for all lanes.** Rejected: `Trim` becomes a compaction, a slow lane pins every other lane's
  disk, and the buffer stops being per-lane in exactly the way everything else is per-lane (R9).
- **A destructive `Pop`.** Rejected: a popped-but-unsettled batch is unrecoverable after a crash with the
  cursor already committed, which is R6's two-incompatible-models defect inside one interface.
- **Letting the buffer settle.** Rejected: it re-creates the R4 violation with an extra indirection.

## Consequences

- Positive: unbounded growth is inexpressible; the ack boundary is a checked property rather than a claim;
  the trim path is an unlink; a format change fails loudly.
- **Negative, accepted:** in coordinated mode no shipped buffer may shorten the ack chain, so a sustained
  sink outage applies backpressure to the source rather than being absorbed on disk. Mitigation: a
  node-durable buffer is still usable as a *non*-ack-shortening buffer, which absorbs bursts without moving
  the guarantee.
- **Negative, accepted:** a lane whose worker holds a non-empty node-durable buffer is not reassigned until
  that buffer drains, or the operator forces it and accepts re-delivery. This is stated at submit time
  rather than discovered.
- **Negative, accepted:** four durability values are more vocabulary than a bool. Justified by the fatal
  defect a bool caused.
