# 0023 — Two framework-owned test suites, and the seven invariants

**Status:** accepted, normative.

## Context

R8 exists because CI in the abandoned attempt validated the *spec* and never a *response*: three implementations
hand-mirrored the same assertions in three idioms, a nil slice marshalled to `null` against a `required: array`
field, and the test asserted `len(...) != 0` — which passes for `null`. The break was invisible to CI. Byte-vs-
UTF-16 length validation, body-size caps and timestamp strictness diverged identically, and the contract version
string was hard-coded in five places with nothing checking they agreed.

Constraint #4 is a *claim about what adding a connector costs*. A claim is not checkable without a test that a
connector author can run.

The decision space contains no testing strategy anywhere. Its addendum supplies two, and the datapoint that
settles the sequencing argument: Conduit's ack-before-persist severity-zero data-loss bug *was found by the first
test ever written to look for it*, and the postmortem notes the bug predated the repository having any
chaos-testing infrastructure at all. **The checkpoint looked correct for years.**

## Decision

**Two suites, both owned by the framework, both mandatory.**

### 1. `connector/conformance` — the author implements a driver, not tests

```go
func TestConformance(t *testing.T) {
    conformance.Run(t, conformance.Driver{
        SourceConfig:   map[string]any{"path": "testdata/in.jsonl"},
        SinkConfig:     map[string]any{},
        Generate:       func(t *testing.T, op record.Op) *record.Record { … },
        WriteUpstream:  func(t *testing.T, rs []*record.Record) { … },
        ReadDownstream: func(t *testing.T) []*record.Record { … },
    })
}
```

Four properties are copied deliberately:

- **The cases cover contract points an author would never think to test**: `Close` after a failed `Open`, `Close`
  without `Open`, drain-on-cancel, `EndOfInput`, position monotonicity, the two message audiences, the
  `Written + len(Failed) == Count` reconciliation.
- **Mid-scan resume and mid-stream resume are SEPARATE cases**, because they are different code paths. The lane
  model makes that distinction structural, so the suite must have both from the start.
- **`Generate` produces mixed byte and structured payloads by default**, so a connector silently assuming one
  representation fails conformance rather than production. That is the enforcement mechanism for the sealed
  `Value` sum and for the never-implicit conversion rule.
- **Goroutine-leak checking is on by default** and an exemption must be *justified* in the driver.

Additional canal-specific cases the evidence demands: `Registration_CapsMatchMethods`;
`Spec_ExamplesValidate` (a stale example fails CI — R10); `Spec_PathsExist` (every path a constructor reads is
declared, so a mistyped path cannot silently yield a zero in production); `Faults_AllClassified` (the
unclassified counter stays zero); `Faults_RetrySafety` (an injected post-send timeout must be reported
`Indeterminate`, not `Transient`); `Resume_AcrossFormatVersion` (write N, read N+1, write, read N);
`Read_NoRecordOutsideAdd`.

### 2. `tests/chaos` — an engine-level kill suite, and a graduation criterion

`SIGKILL` and `SIGTERM` at every interesting instant: mid-batch, mid-flush, **between phase two and phase
three**, mid-schema-change, mid-lane-handoff, during a lease expiry, and **with a nack and no dead-letter route
configured** — which is the least-tested path in every surveyed system and where the deadlocks live. It asserts
the seven invariants on recovery.

**It is a graduation criterion, not a nice-to-have.** Implementation step 10 is `canal run` end to end *with the
chaos test that kills it mid-flush*, and nothing below that step starts until it is green.

### The seven data-integrity invariants

Normative, and each references only mechanisms that exist:

1. Never tell an upstream it may advance before the data is durable downstream **and** canal's own record of the
   position is durably flushed. No intermediate early acknowledgement for throughput, ever, except through a
   buffer whose durability domain is validated and disclosed.
2. Positions are monotonic and crash-safe; a serialisation change requires a versioned migration path **and an
   upgrade test**.
3. At-least-once is the floor: any path that can drop a record without delivering it or routing it on a failed
   edge is a data-loss bug — *including* the error, shutdown, revocation and reassignment paths.
4. Ordering is per-lane and documented at the knob; a change that could reorder within a lane requires a decision
   record.
5. State and checkpoint writes are atomic; torn writes are impossible; every state feature ships with a
   kill-mid-write recovery test.
6. Schema handling never silently mangles data: a lossy conversion is a `NoteChange`, a counted metric and a
   visible event, or it is a refusal.
7. Shutdown is graceful by default and `kill -9` at any instant is recoverable, and we test exactly that.

**An invariant may only reference mechanisms that exist.** An aspirational one is labelled a goal, not an
invariant — Conduit's own list names a `halt/DLQ/evolve` policy absent from its code, which is R12 violated in a
new place.

### The in-code citation convention

Where code upholds an invariant or a design rule, it says so at the enforcement site:

```go
// Invariant 1: the source is told to advance only after StateStore.Set has
// flushed. See docs/decisions/0006-three-phase-commit.md. Do not reorder.
// R6: bounded by construction.
```

`grep -rn 'Invariant [0-9]'` must return a comment at every site that upholds one, and a CI check asserts the
count does not decrease. That is how a correctness property survives a refactor by someone who was not there, and
it is R8's principle — drift is prevented structurally, not by discipline — applied to *invariants* rather than to
tests.

### Three more structural drift preventers

- **`TestDependencyDirection`** walks the module graph and fails on any edge not in the declared table, plus a `go
  vet` analyser asserting no connector package imports `engine`, `ledger`, `store`, `telemetry` or `spec`.
- **A wire-shippability generator** walks the `connector` package and fails if any exported method's parameters or
  returns are not JSON-round-trippable — running from commit one, before any RPC exists.
- **Golden-file fixtures** for `/metrics` output, for `PipelineStatus` with every optional field absent (asserting
  the UI renders no zeros), and for every persisted struct's JSON.

## Alternatives rejected

- **Per-connector hand-written tests.** Rejected: it is the three-idioms failure that made R8 necessary, and it
  makes constraint #4 unfalsifiable.
- **Conformance as documentation.** Rejected: prose does not fail CI.
- **One combined suite.** Rejected: a per-connector suite must run in a connector's own module with no engine
  fixtures, and a kill suite must own the process. Two suites, two owners.
- **Chaos testing as a later milestone.** Rejected on the single most decisive datapoint in the evidence base: the
  checkpoint looked correct for years and the first test that looked for the bug found it. R3 makes a `kill -9`-
  surviving checkpoint the first milestone; this makes the *test* the milestone.
- **Property-based testing as the primary mechanism.** Rejected as the primary: the failures that matter are
  ordering and crash-timing failures, which need injected kill points rather than generated inputs. Used inside
  `Tracker`'s own tests, where it fits.

## Consequences

- Positive: "implement the interface, register it, done" becomes checkable; the classes of bug that survived years
  in mature systems are tested for on day one; drift is prevented by build failures rather than by review.
- **Negative, accepted:** a connector author must write a driver — roughly thirty lines plus an upstream/downstream
  helper. That is the price of the claim being true.
- **Negative, accepted:** the chaos suite is slow and needs process control, so it runs in a separate CI job and not
  on every push.
- **Negative, accepted:** the in-code citation convention can rot into decoration. Mitigated by the non-decreasing
  count check, and accepted as a weaker guarantee than a test.
- **Negative, accepted:** conformance will initially fail for connectors that are "working". That is the point.
