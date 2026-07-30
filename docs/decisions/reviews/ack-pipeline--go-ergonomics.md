# Review: `ack-pipeline` — lens: Go ergonomics and implementability

**Reviewer stance:** hostile expert. The question is not "is the thesis attractive" but "would a competent Go
engineer enjoy writing a connector against this, and does the code as written compile and run".

**Proposal under review:** `docs/decisions/proposals/ack-pipeline.md` (3,222 lines), read in full.

**Score: 6 / 10 on this lens.**

---

## Why 6

The taste is genuinely good and it is Go, not transliterated Java. Interface sizes are right (`Source` 4
methods, `Sink` 3, `Transform` 3, optional interfaces 1–2 each). `context.Context` is on every blocking
method from the first commit, with the component-lifetime/call-lifetime distinction named explicitly and a
`rt.Context()` escape for the former — the thing Benthos cannot fix without a V5. Shutdown is a stated
three-phase protocol in which acks keep flowing during the drain, which is the correct answer and one almost
nobody ships. `Encoder.Encode(dst []byte, r) ([]byte, error)` is the append idiom. `Deframer.Split` *is*
`bufio.SplitFunc`. The registry is a value type with `Clone`/`With`/`Without` from day one. Constructors
return concrete structs and accept interfaces. `Batcher` as pure policy with no goroutine is the right
inversion. The single most valuable ergonomic property in the whole document is that a sink is
`Write(ctx, *Request) error` with **no** progress awareness at all: a sink author cannot get checkpointing
wrong because they are never shown a checkpoint. That is worth more than any amount of interface elegance
elsewhere.

Against that: the document opens with "Every type below is real Go, `go 1.23`", and it is not. I found one
contradiction in the *required* `Source` contract that cannot be satisfied by any implementation, two places
where the core is documented to write fields it cannot reach across a package boundary, one core mechanism
whose signature uses a `func` value as an identity (impossible in Go), at least three import cycles in the
declared nine-package layout, a broken `errors.Is` sentinel contract, an ambiguous three-method state machine
on the most-used sink error path, and a settlement key that is provably ambiguous under the document's own
fan-out story. None of these are aesthetic. Each one is the sort of thing that surfaces on day two of
implementation and forces an interface change — which is precisely the cost the proposal exists to avoid.

The score is 6 rather than 4 because almost every break has a known, mechanical repair (an `internal/`
package, a handle type instead of a `func`, moving `engine.Spec` into its own package) and the *shape* those
repairs land on is still this shape. It is 6 rather than 8 because defect 1 is not mechanical: it invalidates
the load-bearing promise "sources need no locking", and fixing it changes the required `Source` interface.

---

## Defects

### D1 (fatal) — `Read` blocks indefinitely, yet `Commit` may not run concurrently with it. No source can satisfy both.

At fault: `Source.Read` ("blocks until at least one record is available, the connection drops, or ctx is
done"), `Source.Commit` ("never called concurrently with itself, and never concurrently with `Read`. Sources
need no locking between them"), and `Heartbeater.Heartbeat`.

The two contracts are jointly unsatisfiable. Consider the shape the engine is forced into:

- **One goroutine** doing `Read → Admit → drain Acks() → Commit`. The moment the source is caught up on a
  tail lane, `Read` parks — correctly, per its own contract — waiting for the next change. While it is
  parked, no ack is delivered and `Commit` is not called. So the durable position stops advancing exactly
  when the pipeline is idle, which is the one situation where advancing matters: an idle Postgres logical
  replication slot pins WAL and fills the disk. The proposal *knows* this failure and ships `Heartbeater`
  for it — but `Heartbeat` has to be called on the same goroutine that is parked inside `Read`, so the
  remedy is unreachable by the same argument.
- **Two goroutines**, one reading and one pumping acks into `Commit`. Now `Read` and `Commit` *are*
  concurrent, and the documented guarantee is false. Every source that mutates shared state in both (a
  cursor, a `*sql.Conn`, an in-flight map, the `Persister`) must lock, which is the opposite of what the
  contract promises and the opposite of what the doc's own example does (`linefile` mutates `s.off` in
  `Read` and `s.p` in `Commit` with no mutex).
- **Two goroutines with a mutex** held across `Read`. Same as case one: the parked reader holds the lock and
  `Commit` still cannot run.

Consequence: either commits stall on idle streams (data-loss adjacent: the replay window grows without
bound in *time*, and upstream retention is not the operator's to control), or every non-trivial source
carries locks the contract says it does not need, and the first author who believes the contract ships a
data race. `driver.Conn`, cited as precedent, gets away with the same guarantee only because
`database/sql` never has a second thing it must call on a parked connection.

The repair is not cosmetic. Either `Read` acquires a "return promptly with zero records" contract (and then
`Batch.Len() == 0, err == nil` needs to be legal and the engine needs a poll interval), or `Commit` and
`Heartbeat` are explicitly concurrent-with-`Read` and the contract says "your source must be safe for one
reader plus one committer", or acks are pushed through a channel the source selects on inside its own loop
(which is a different `Source` interface). All three change the frozen four-method surface.

### D2 (fatal) — `record.Record.origin` and `record.Batch.group` are unexported and documented to be written by package `ledger`. Go has no friend packages.

At fault: `Record.origin Origin` ("set once by the core; no exported mutator exists"), `Batch.group GroupID`
("set by the core at admission"), and `Ledger.Admit` ("assigns `Position.Seq`, assigns a `GroupID`, assigns a
`RecordID` **and `Origin` to every record**").

`ledger` is a different package from `record`. It cannot write `r.origin` or `b.group`. There is no
mechanism in Go by which it could. So the immutability story — which is presented as the structural fix for
Kafka Connect's KIP-793 retrofit and is one of the document's proudest claims — is unimplementable as
specified, along with the entire admission step of the ledger.

The consequences of each available repair are real and should be priced before the synthesiser adopts this:

- **Exported setter** (`func (r *Record) SetOrigin(Origin)`): a transform can now corrupt settlement
  identity, which is the exact property the design claims to have made structurally impossible. Guarding it
  with "panics if already set" makes it a runtime check, not a structural one.
- **Move admission into `record`**: `record` then owns `GroupID`/`RecordID` allocation and the batch-to-group
  mapping, i.e. the ledger's state moves into the envelope package. `record` currently imports stdlib only,
  and that constraint is load-bearing for R2.
- **`internal/` shared package**: works, and is what I would do — `canal/internal/admit` holding the
  concrete structs, with `record` and `ledger` both above it. But it inverts the declared layering diagram
  in §10, and it means the record model's fields live in a package connector authors cannot see, which
  changes what godoc shows them.

None of these is fatal by itself; the fact that the proposal does not notice that its central data structure
cannot be populated by its central algorithm is what makes this fatal. It is the same class of error as D3
and D4 below, and the cluster is what earns the rating.

### D3 (major) — `Payload.Bytes()` is documented to materialise via "the pipeline's configured encoder", from a struct that holds no reference to one, in a package that cannot import the encoder's package.

At fault: `Payload{b []byte; v Value; has uint8}` with
`Bytes() ([]byte, error)` — "materialising it from the structured view if necessary using the pipeline's
configured encoder" — and `StructuredMut()` — "copying first if the current view is shared".

Three fields, none of them a codec handle and none of them a sharing bit. `Encoder` lives in `connector`,
which imports `record`; `record` therefore cannot name it. So `Bytes()` on a structured-only payload either
requires hidden package-level global state (the exact thing R13 and the decision space's Vector critique
call out — Vector's global mutable `log_schema` is a named regret), or a per-`Payload` pointer that the
struct does not have, or the documented behaviour is a lie and it returns an error.

`StructuredMut`'s copy-on-write is broken the same way: with no ownership/shared bit, `has uint8` (bit 0
bytes valid, bit 1 structured valid) cannot answer "is this view still owned upstream". So either every
`StructuredMut` deep-copies (a `map[string]Value` walk per record per transform — the hot path this design
otherwise guards carefully) or it silently hands out a shared `Map` and a transform mutates a record another
branch is still reading. That second failure is a data race with no compiler help, in the accessor whose
whole purpose is to be the safe one.

Consequence for a connector author: the two most-called methods on the payload have documented semantics
that no implementation can provide, so their real behaviour will be discovered empirically, per canal
version. `Structured()`'s "mutating the returned Value is a contract violation" is unenforceable for the
same reason: `Map` and `List` are reference types and the accessor hands out the live one.

### D4 (major) — `Record.Copy()` deliberately preserves `RecordID`, while `Ledger.Settle` is keyed on `RecordID` alone. Fan-out settlement is ambiguous and resolves early.

At fault: `Copy()` — "a deep copy carrying the same `Origin` and the same `RecordID`: it is the same record,
materialised twice (for fan-out to two sinks)" — against `Ledger.Settle(id record.RecordID, d Disposition,
f *fault.Fault)` and `Ledger.Fanout(g record.GroupID, n int)`.

The broker sink in §8.1 fans out to N children. `Fanout(g, n)` raises the group's outstanding count by n−1;
each child eventually calls `Settle(id, Delivered, nil)` with **the same `RecordID`**, because `Copy`
preserved it. The ledger receives two indistinguishable `Settle(7, Delivered)` calls and has no way to tell
"branch A settled, branch B settled" from "branch A settled twice". Which means:

- A double-settle bug anywhere — a retry path that settles and then routes to DLQ which settles again; a
  sink whose `BatchError` names a record as both failed-then-retried-successfully and, via the
  `Succeeded` path, already landed (see D7) — silently decrements the group refcount to zero while a branch
  is still in flight. The group resolves, the prefix advances, `Ack.Through` is emitted, the source commits
  a position covering data that has not been written. That is exactly the R4 catastrophe the proposal is
  built to make unreachable, arrived at through the API rather than through prose.
- `Settle` cannot be made idempotent, because idempotency and fan-out counting want opposite things from the
  same key.

Note that the fix already exists in the same file: `Derive()` mints a fresh `RecordID` with `Parent` set and
keeps the group. Fan-out should use it, and `Copy` should not exist as a settlement-visible operation. That
the document has both mechanisms and chose the ambiguous one for fan-out is a modelling slip, not a typo:
`Ref` (what a sink reports outcomes with) also carries only `{ID, Group, Lane, Stream, Key}`, so a sink in a
fan-out cannot disambiguate either.

### D5 (major) — At least three import cycles in the declared nine-package layout.

At fault: §9 `store.ConfigStore.Get(ctx, id) (engine.Spec, uint64, error)` and `Put(ctx, s engine.Spec, ...)`
against §8 `engine.Deps{Config store.ConfigStore, State store.StateStore, Coordinator store.Coordinator,
Status store.StatusSink}` and §10's "engine … imports: all of the above + store".

- **`engine` ↔ `store`.** `store` names `engine.Spec`; `engine` names four `store` interfaces. Does not
  compile. The repair — hoist `Spec`/`ComponentRef`/`CodecRef`/`Guarantee`/`DriftPolicy` into a `spec`
  package — is fine, but it moves the pipeline vocabulary out of `engine`, which changes the §10 diagram
  the proposal presents as the proof that a connector can only reach five packages.
- **`config` ↔ `connector`.** §3.2 declares, on `*config.Config`, `Batching(path ...string) (BatchPolicy,
  error)` and `Codec(path ...string) (CodecChain, error)`. `BatchPolicy` is defined in `connector` (§4.14,
  with a `FlushOn *config.Predicate` field) and `CodecChain` is defined in `connector` (§4.9). `connector`
  imports `config`. So either this is a cycle, or both types are duplicated in `config` — and then there are
  two `BatchPolicy`s and two `CodecChain`s to keep in step, which is R9's "a function mapping between two
  representations of the same concept".
- **`config` ↔ `registry`.** `config.Field.ComponentKinds []Kind` — there is no `Kind` in package `config`.
  If it means `registry.Kind`, that is a cycle (`registry` imports `config`); if it is a third `Kind` enum,
  it is a third vocabulary for component kinds.

These matter beyond the compiler: §10's closing claim — "A connector package imports exactly five canal
packages … and the dependency test enforces it" — is the proposal's headline extensibility guarantee, and it
is asserted over a graph that has not been checked for acyclicity.

### D6 (major) — `Tracker.Abandon(r Resolve[P])` identifies a pending node by a `func` value. Not implementable.

At fault: `type Resolve[P any] func() (P, bool)` returned from `Track`, and
`func (t *Tracker[P]) Abandon(r Resolve[P])`.

Go func values are not comparable (except to nil) and cannot be map keys. `Abandon` receives a closure and
has no way to recover the node the closure captured. Calling it is not an option: invoking it *is* resolution
as delivered, per its own documentation. So the method cannot be written.

Consequence: mechanism (5) of §5.3 — "Head-of-line blocking cannot become a livelock … on exhaustion the
engine calls `Tracker.Abandon`, the prefix advances past 5, and the source resumes" — is the load-bearing
answer to the hard question, and it does not compile. The repair is trivial and *improves* the API: return a
handle, `type Pending[P any] struct{...}` with `func (p *Pending[P]) Resolve() (P, bool)` and
`func (p *Pending[P]) Abandon()`. That is more idiomatic than a returned closure anyway, and it removes the
one remaining closure from a design whose thesis is "keeps the ack graph and deletes the closure" — which,
as written, is a rhetorical claim rather than a structural one: the closure is not deleted, it is moved
inside the core and stored in a map keyed by `GroupID`.

### D7 (major) — `BatchError`'s `headline` / `Fail` / `Succeeded` triple has no defined meaning for records named by neither.

At fault: `NewBatchError(headline *Fault)` ("the fault that applies to every record for which `Fail` was not
called"), `Fail(id, f)` ("calling `Fail` at least once switches the batch to 'everything not named here
succeeded'"), and `Succeeded(id)` ("explicitly marks a record as landed even though the batch as a whole
errored").

Enumerate the states a sink author can produce:

| construction | records named by `Fail` | records named by `Succeeded` | records named by neither |
|---|---|---|---|
| headline only | — | — | fail with headline |
| headline + `Fail` | fail | — | **succeed** |
| headline + `Succeeded` | — | succeed | fail with headline |
| headline + both | fail | succeed | **undefined** |

The fourth row is the one a real bulk-API sink writes. An Elasticsearch-shaped `_bulk` response gives you
per-item statuses: you loop, you call `Fail` for the errors and `Succeeded` for the ok items because that is
what the response literally told you, and now the semantics of any record you happened not to visit — a
frame the encoder emitted for a record the response omitted — are undefined. Row 2 and row 3 also disagree
about what the headline is *for*: in row 2 it applies to nobody, so the constructor demands an argument that
is then ignored.

Consequence: this is design rule R7 ("write the failure shape at the same time as the success shape")
satisfied in form and failed in substance, on the single most-used error path in the system, with a
data-loss failure mode (a record silently classified as succeeded settles as `Delivered`, the prefix
advances, and the source commits past it). The fix is one field: an explicit `Default Disposition` on the
`BatchError`, or drop `Succeeded` and make `Fail` the only per-record verb.

### D8 (major) — `fault`'s sentinels do not work with `errors.Is`, and `ClassOf`'s "innermost" rule inverts re-classification.

At fault: `ErrNotConnected = New(NotConnected, OpUnknown, errNotConnected)`, `ErrEndOfInput`, `ErrSkip`, with
the comment "These wrap into `Fault` so `errors.Is` works through connector wrapping", against
`func (f *Fault) Error() string` / `func (f *Fault) Unwrap() error` — and no `Is(error) bool` method
anywhere.

`Fault` is a pointer type with only `Error`/`Unwrap`. `errors.Is(err, fault.ErrNotConnected)` therefore
succeeds only when the *same pointer* is in the chain, or when the connector wrapped the unexported
`errNotConnected` value specifically. But the whole point of the taxonomy is that a connector constructs its
own faults with useful messages: `fault.New(fault.NotConnected, fault.OpRead, err)` — and
`errors.Is(that, fault.ErrNotConnected)` is **false**. Since `Source.Read`'s contract is "returns
`fault.ErrNotConnected` to request a reconnect", a source that classifies correctly but constructs its own
value never gets reconnected. It will instead be treated as… whatever the engine's fallback is.

The doc simultaneously mandates the other idiom — "Every engine call site uses `ClassOf`; nothing uses `==`
or a type switch" — so there are two discrimination mechanisms for one taxonomy (R9), and only one of them
works. `ClassOf` should be the only one, and the three sentinels should be documented as *examples*, not as
values to compare against.

Separately, `ClassOf` "walks the error chain with `errors.As` and returns the **innermost** declared class".
`errors.As` returns the first (outermost) match; "innermost" requires a deliberate custom walk that keeps
going past a match to prefer the *least* recently applied classification. That inverts the normal Go meaning
of wrapping: a connector that catches a transient socket error and deliberately re-raises it as
`PermanentContract` ("this token codec is from the future, the reset was a symptom") gets `TransientUpstream`
back, and the engine poison-retries. The outermost class is the intended one; if not, say why in the doc,
because every Go reader will assume the opposite.

Third, smaller: `ErrSkip` is classified `TransientInternal`, which `Class.Retryable()` reports as `true`.
A control-flow sentinel that reads as a retryable fault to any code path that forgets to check for it
specifically is a landmine — an infinite retry loop dressed as backoff. `NotConnected` and `EndOfInput` are
likewise members of the *fault* class enum while the doc calls them "Control flow, not failure", which makes
`canal_records_failed_total{class="end_of_input"}` an expressible series.

### D9 (major) — one `Position` per `Batch` means a discrete-ordering (queue) source cannot settle individual messages.

At fault: `Batch.Position` ("position AFTER the last record in `Records`"), `Record` having no position field,
`Ordering = OrderingDiscrete` ("the queue case: an SQS receipt handle, an AMQP delivery tag, a Pub/Sub ack
id"), and `Ack.Discrete []record.Position` ("exactly the positions that settled").

A group is a batch, and a batch has exactly one `Position`. For a prefix lane that is right and is one of the
best ideas in the document. For a discrete lane it is structurally wrong: the position *is* the per-message
receipt handle, so there must be one per record. An SQS source that receives ten messages in one API call
has two options:

1. Emit them as one batch. Then there is one `Position`, and when three of the ten records fail, `Commit`
   receives a single settled position and the source cannot know which seven to `DeleteMessage`. It must
   delete all ten (data loss, the three failures become invisible) or none (redelivery of seven).
2. Emit ten batches of one record. Since `Read` fills one caller-owned `dst` per call, that is ten `Read`
   calls with nine messages buffered in connector state, ten `GroupID`s, ten ledger entries, ten `Ack`s and
   ten `Commit` calls — hence ten single-message `DeleteMessage` requests instead of one
   `DeleteMessageBatch`. A 10× increase in upstream API calls and in ledger bookkeeping, plus the exact
   internal-buffer-and-index bookkeeping the design claims to have abolished.

Consequence: the second of the two declared orderings — half the "one declared enum, two core-owned
strategies, zero connector algorithms" claim in §5.3(6) — is served badly enough that every queue connector
will write a worse version of what a per-record position would have given for free. The repair is small
(`Record` carries an optional `Position`, or `Batch.Positions []Position` parallel to `Records` for discrete
lanes) but it touches the envelope.

### D10 (major) — per-record `Settle` and per-record `Origin` stamping are the hot path, with no batch variant and no pooling.

At fault: `Ledger.Settle(id record.RecordID, d Disposition, f *fault.Fault)` (no ctx, no error, no batch
form), `Ledger.Admit` ("assigns … a `RecordID` and `Origin` to every record"), `Origin` (4 string fields +
2 `time.Time` + 3 integer ids ≈ 120 bytes), and `Batch` ("a caller-owned, reusable buffer … the engine
allocates one per stage and passes the same pointer every iteration").

- Walkthrough (c) is explicit: 488 successful records produce **488 `Settle` calls**. Each one is a lookup
  in a `map[RecordID]` under the ledger's lock (it must be: `Settle` is called from sink goroutines up to
  `MaxConcurrency`). At a modest 100k records/s that is 100k lock acquisitions plus 100k map inserts at
  admission and 100k deletes at settlement. The batch form is free to add — `Request.Records []record.Ref`
  is already the slice you would pass — and its absence means the common case (the whole request landed) is
  the expensive case.
- `Batch` reuse buys almost nothing, because `Batch.Records` is `[]*Record` and the walkthrough allocates
  `&record.Record{...}` per line. Reuse recycles a slice header; the ~250-byte `Record`, the cloned payload
  and (for structured payloads) every boxed `Value` are per-record garbage. There is no `record.Pool`, no
  `Batch.Grow`, no documented reuse of `*Record`, and `Reset()` is undocumented as to whether it releases
  the records. With a budget of 1000 per lane and nine lanes, that is 9,000 live fat structs plus their
  payloads, churned continuously.
- `Origin` copies four string headers and two timestamps per record that are constant per batch
  (`Pipeline`, `Source`, `Lane`, `Stream`, and `ReadAt` unless the source really wants per-record read
  times). ~120 bytes of pure bookkeeping written per record, plus a `time.Now()` per record.

None of this is fatal; all of it is the kind of thing that is very hard to change later because `*Record`
and `Origin` are connector-visible. A `Ref`-keyed `SettleBatch`, an internal per-batch origin with derived
accessors, and a documented `*Record` ownership/pool rule would cost nothing now.

### D11 (major) — fourteen hand-synced `Caps` bools mirroring optional interfaces, enforced by an init-time panic that must construct a probe instance.

At fault: `SourceCaps` (`Discoverable`, `Nackable`, `ReportsBacklog`, `Heartbeats`, `Validates`, `Probes`,
`ProvidesChoices`) plus `SinkCaps` (`StructuredInput`, `ReturnsResponse`, `Partitions`, `AppliesSchema`,
`Prepares`, `Validates`, `Probes`, `ProvidesChoices`), and `Registry.AddSource`: "CROSS-CHECKS `Caps`
against the interfaces the returned value satisfies, **by constructing a zero-config probe instance where
possible and otherwise deferring to first construction**. Declaring `Caps.Discoverable` without implementing
`Discoverer` PANICS at registration."

The *reason* for the duplication is legitimate and I will not relitigate it: a type assertion does not cross
a process boundary, so the fact of a capability must be data. But the mechanism chosen imposes the cost on
the wrong side.

- The information is already in the compiler. `ResolveSource` performs exactly these assertions in exactly
  one place. For an in-process connector the bools could be **derived** at registration and the explicit
  declaration required only where the compiler cannot help (a future gRPC connector, which will have a
  different `Def` anyway). As written, every author maintains by hand a shadow copy of their own method set
  and is punished by a panic for drift.
- "Constructing a zero-config probe instance" means `init()` calls the connector's `New` with a config that
  cannot have passed `Spec.Validate` — `linefile`'s `New` does `cfg.String("path")` and would return an
  error, so the probe fails, so the check silently falls into the "deferring to first construction" branch.
  That branch is the dangerous one: a `Caps`/interface mismatch now panics at **pipeline build time, in
  production**, which is the 3am failure the design elsewhere prides itself on eliminating at submit time.
  So the guarantee is non-deterministic across connectors, and the connectors where it degrades are exactly
  the ones with required config, i.e. all of them.
- Running connector constructors during package `init()` also means an unrelated `go test ./...` can panic
  inside a third-party connector's `New`. That is a genuine testability regression against the otherwise
  excellent `Clone`/`With`/`Without` story.
- `AddSource(d SourceDef) error` **both** returns an error and panics for the same class of problem. The
  document's own flagship example discards the error (`registry.Default.AddSource(registry.SourceDef{...})`
  as a bare statement in `init`), which any linter will flag and which means the one line every connector
  author writes is non-idiomatic in the reference implementation. Benthos's answer — `RegisterX` returning
  an error plus `MustRegisterX` panicking — is right there and is not adopted.

### D12 (major) — `ledger.Ledger` has no `Close`, and `Acks()` is never documented to close. One leaked goroutine and one leaked pump per pipeline.

At fault: `func New(cfg Config) *Ledger` with `Config.GroupTTL` implying a reaper, `Acks() <-chan
connector.Ack`, `Drain(ctx) error`, `Leaks() []Leak` — and no `Close`/`Stop` anywhere.

If `GroupTTL` is enforced, something must be scanning; `New` is the only constructor, so the reaper starts
there and never stops. `Acks()` returns a receive-only channel that nothing is documented to close, so the
natural consumer (`for ack := range l.Acks()`) blocks forever after the pipeline finishes. §8's shutdown
ordering carefully names sink/buffer/transforms/source and does not mention the ledger. Consequence: a
long-lived `canal serve` process that starts and stops pipelines (config revisions, lease churn, a bounded
pipeline completing) leaks a goroutine and a `Ledger` — with its pending-group map — per pipeline
generation. This is the single most common Go review finding in the world and the proposal's most
goroutine-heavy component walks straight into it.

Related, in the same area: `Settle` has no ctx and no error, so if it must send on the `Acks()` channel it
can block a sink goroutine indefinitely with no cancellation, and if the commit pump has already stopped
(shutdown ordering does not say when it stops relative to the sink's last `Write`), it blocks forever.
Whether `Settle` is send-on-channel or set-a-flag-and-let-the-pump-poll is exactly the sort of thing a
signature should tell me and does not.

### D13 (minor) — the cold-start/warm-start branch is copy-pasted into every source, which is the trap the decision space explicitly names.

At fault: `LaneCtl.Assigned(ctx) ([]LaneAssignment, error)` as the only entry point, and the twelve lines in
walkthrough (a) that every source must reproduce:

```go
as, err := rt.Lanes().Assigned(ctx)
if err != nil { return err }
if len(as) == 0 {
    s.lane, err = rt.Lanes().Open(ctx, connector.LaneSpec{...})   // cold
} else {
    s.lane = as[0].ID                                             // warm
    if tok, _ := s.p.Restore(s.lane); tok != nil { ... }
}
```

`_decision-space.md` D5 praises FLIP-27 for exactly the opposite: "`createEnumerator` and
`restoreEnumerator` as **separate methods** so cold start and warm start cannot be confused (contrast
Connect's single `start(props)`)". This proposal reunifies them behind a `len(as) == 0` test — the same
shape as Benthos's `if i.streamSnapshot && pos == nil`, which D6 documents as the reason a 500M-row snapshot
restarts from zero. The state here is richer, so the *consequence* is less severe, but the ergonomic cost
lands on every author: the trickiest twelve lines in the file are boilerplate, and getting them wrong
(announcing when assignments already exist, or announcing with a `Name` that is not stably derived)
duplicates lanes or silently forks progress. Two methods — `Start(ctx, rt)` and `Restore(ctx, rt, []LaneAssignment)`
— would delete the branch, make the two paths separately testable, and cost one interface method.

The doc's own example also demonstrates the second hazard: it calls `s.p.Restore(s.lane)` and discards the
codec (`if tok, _ := ...`), in direct violation of the design's own normative rule that "a connector must
refuse a `TokenCodec` it does not recognise with `fault.PermanentContract`, so a downgrade fails loudly
instead of misparsing".

### D14 (minor) — `record.Value` is a sealed interface over `Map`, `List` and `Bytes`: `==` panics at runtime, `nil` and `Null{}` are two representations of absent, and every field boxes.

At fault: `type Value interface { isValue(); Kind() Kind }` with members
`Null struct{}`, `Bool bool`, `Int int64`, …, `List []Value`, `Map map[string]Value`.

- `Map` and `List` and `Bytes` are not comparable types. `if v == prev` over two `Value`s compiles and
  panics at runtime when the dynamic type is one of those three. A dedupe transform, a codec that
  short-circuits unchanged fields, a test using `assert.Equal`-style comparison — all natural, all a
  latent panic. Nothing in the doc warns.
- `Map{"x": nil}` and `Map{"x": Null{}}` both mean absent, and `Value(nil).Kind()` panics. Every codec,
  every mapping evaluation, every sink must nil-check defensively, and there is no stated rule about which
  form the core produces.
- Discrimination is doubled: `Kind()` *and* the dynamic type. The `Kind()` justification (a bounded metric
  label) is fine, but two ways to ask the same question is R9's smell, and a `switch v := val.(type)` with
  a `Kind()` guard elsewhere will drift.
- Every field of a structured record boxes into an interface. `Int(5)` in a `Value` allocates (Go's small-int
  static cache covers only single-byte values); `Time` (24 bytes) allocates; `Decimal` allocates. A
  twenty-field structured record is twenty allocations before any encoding happens. Benthos's `any` +
  documented type set has the same boxing but at least no wrapper conversions; this design pays for the
  sealed set with a second layer.

The sealing argument (third parties must not widen the set, because widening breaks the codec and the
checkpoint format simultaneously) is good and I would keep it. The pitfalls are all fixable with a
documented comparison helper, a mandated `Null{}` form, and either a variant struct instead of an interface
or an explicit "structured records are for transforms, bytes are the hot path" statement.

---

## Further findings, not itemised above

Each of these is real and cheap to fix; collectively they are what makes the difference between 6 and 8.

**`Pipeline.Run(ctx)`: "Cancelling ctx begins a graceful drain; a second cancellation is a hard stop."** A
context cannot be cancelled twice observably — `Done()` closes once and `Err()` is sticky. The described
protocol does not exist. `Shutdown(ctx)` already exists in the same section and is the right vehicle; Run's
doc contradicts it. This is the one place the design reads like signal handling transliterated from a
language with a cancel token that carries a level.

**Sink concurrency during reconnect is unspecified, and every concurrent sink therefore has a race.**
`Write` "may run concurrently up to `Caps.MaxConcurrency`"; `Open` is "same … re-callability as
`Source.Open`", i.e. called again after any method returns `ErrNotConnected`. With `MaxConcurrency: 8`,
seven `Write`s can still be in flight when the eighth returns `ErrNotConnected` and the engine calls `Open`.
Does the engine quiesce first? The doc does not say, so a sink author who rebuilds `k.client` in `Open` has
a data race and will not know it. `Close` versus in-flight `Write` has the same hole.

**`Buffer` and `Transform` have no stated concurrency contract at all.** `Buffer.Put` and `Buffer.Get` are
obviously called from different goroutines (that is the point of a buffer), `Depth()` is called from the
status path, and nothing says any of it must be safe for concurrent use. `Source.Read` gets an explicit
"NEVER called concurrently with itself"; the two plugin types that most need such a sentence do not have
one.

**`Batch.Reset()` ownership is unstated on the hot path.** The engine owns `dst` and reuses it; the example
source calls `dst.Reset()` as its first statement in `Read`. If the engine resets, that is redundant; if the
source must, the contract does not say so, and a source that forgets appends to the previous batch and
re-delivers it. One sentence on `Read` fixes it.

**Error-with-partially-filled-`dst` is undefined for `Read`.** The example appends records and *then* returns
`fault.Transient(fault.OpRead, err)` on a scanner error, and separately sets `dst.Position` and
`dst.EndOfLane` before returning `fault.ErrEndOfInput`. Are those records admitted, discarded, or
re-delivered? `driver.Rows.Next` — the cited precedent — has exactly one rule and this does not.

**The flagship example has a data-loss bug, which is the best available evidence on "is the simple case
simple".**

```go
dst.EndOfLane = dst.Len() == 0 || s.sc.Err() == nil && !s.sc.Scan()
```

Go precedence makes this `a || (b && c)`, so on the common path this calls `s.sc.Scan()` and throws away
`s.sc.Bytes()`: one line is silently consumed and never emitted at every 500-line batch boundary. `s.off` is
not advanced for it either, so the committed token points before the swallowed line — the loss is invisible
until a restart accidentally repairs it. If the author of the design cannot write the 20-line file source
correctly, the batch/position/EndOfLane protocol is not as simple as claimed.

**`Registry.With(defs ...any)` and `Walk(fn func(k Kind, name string, spec *config.Spec, caps any) bool)`
put `any` at the exact seam whose selling point is compile-time safety.** `With` must type-switch on
`SourceDef`/`SinkDef`/…, turning a compile error (passing the wrong Def, or a value where a pointer was
expected) into a runtime one; `Walk`'s `caps any` forces every consumer — `api/`, the UI serialiser, the
conformance kit — to type-switch on capability kind, which is the switch statement the proposal claims not
to need. Typed `WithSource(...)`/`WithSink(...)`, or per-kind `Walk` callbacks, or a small `CapsView`
interface, all cost less than the `any`.

**Invalid or actively wrong zero values in `Caps`, with no constructor.** `SinkCaps.RequiresCodec` is
documented as "false only for a sink that always takes structured input" — so the zero value is the rare,
dangerous case and every ordinary sink must remember `RequiresCodec: true` or silently get no encoder.
`SinkCaps.MaxConcurrency` is "Required, >= 1", so `SinkCaps{}` is invalid. Go's rule is that the zero value
should be the useful one; here `SinkCaps{}` is never valid and there is no `NewSinkCaps()` or defaulting
pass, so every author copies a struct literal out of an example. (`RetryPolicy.Terminal`'s invalid zero is
different and is *correct* — it exists to make unbounded retry inexpressible. Say which invalid zeros are
deliberate.)

**`Revoked(id record.LaneID) <-chan struct{}` forces `reflect.Select` or a goroutine per lane in connector
code.** A source with eight scan lanes plus a tail holds nine revocation channels, and Go cannot `select`
over a dynamic set. So the author either uses `reflect.Select` (nobody will) or spawns a goroutine per lane —
at which point the source needs internal locking, contradicting "Read is never called concurrently with
itself. The source may therefore be lock-free internally". A single `Revoked() <-chan record.LaneID` on
`LaneCtl` is the idiomatic shape and removes the problem. Worse, a revocation cannot interrupt a source
parked in `Read` at all (see D1), so the lane cannot actually be surrendered promptly.

**Three representations of "the resume blob" and three of "the in-flight budget".** Resume: `LaneSpec.Resume`
+ `ResumeCodec` (persisted with the lane row), `LaneAssignment.Committed.Token` + `TokenCodec` (the observed
committed position), and the `StateHandle` blob that `AutoPersist` writes. Walkthrough (b) step 9 has the
source "reconstruct each lane from `Spec.Resume` plus `Committed.Token`", so an author must understand which
of two opaque blobs with two independent codec numbers wins. Budget: a connector-declared
`config.Fields.LaneBudget("in_flight")` field, `engine.Spec.LaneBudget`, and `LaneSpec.Budget`, with no
stated precedence. R9 says a mapping function between two representations of one concept is evidence of a
modelling error; here there are two such clusters plus a third: `ledger.LaneStats` and
`telemetry.LaneStatus` are the same numbers under different names (`Admitted`→`RecordsRead`,
`Settled`→`RecordsCommitted`, `AbandonedTotal`→`RecordsAbandoned`, `OldestPendingAge`→
`OldestPendingAgeSeconds`), requiring a hand-written mapper that will drift. `connector.Backlog` and
`telemetry.Backlog` are a fourth.

**Name collisions across the five packages a connector imports together.** `record.Op` (change operation)
and `fault.Op` (pipeline stage) are both `uint8` and both metric labels. `record.Kind` (value kind) and
`registry.Kind` (component kind), plus `config.Field.ComponentKinds []Kind`. `config.Spec` and `engine.Spec`
— the worst, because both appear in a registration file and in pipeline assembly. `fault.New`,
`registry.New`, `ledger.New`, `config.NewSpec`. Also stutter: `[]connector.Boundedness{connector.BoundednessBounded}`
is a phrase no Go author enjoys typing; `connector.Bounded`/`connector.Unbounded`, `connector.Prefix`/
`connector.Discrete` are right there.

**`Meta` is stringly-typed in a design where every other vocabulary is a closed enum.** `Get(ns, key
string)`, `Set(ns, key string, v Value) error` (error only when `ns == "canal"` — a runtime error for a
programmer mistake, on the hot path, that nobody will check), `Namespaces() iter.Seq[string]`, and a fourth
"unlisted" secret namespace with a different value type (`SetSecret(key, v string)` takes a `string`, not a
`Value`) that `Namespaces()` presumably must hide. `SetSource`/`SetUser` methods and a `Secrets` sub-struct
would remove the string, the error and the invisible namespace. (The `iter.Seq2` usage is otherwise a
welcome, modern touch.)

**`Transform`'s Copy/Derive rule makes an identity or filter transform absurd as written.** "Records placed
in `out` MUST be derived from records in `in` — via `Copy` or `Derive`". Read literally, a filter that passes
through the surviving `*Record` pointers is illegal and must `Copy` (deep-copy!) every record. Read
charitably, pass-through is fine and the rule means "must belong to one of `in`'s groups" — which is what the
enforcement sentence actually says. Two readings, one of which costs a deep copy per record; say which.

**The per-record→byte-offset correspondence in `Request` is unenforced, and positional identity comes back
in.** "`Records` identifies what is in `Body`, in `Body`'s order". So a sink attributing a bulk-API
per-item error to a record indexes `req.Records[i]` — positional identity, at the exact place the design
congratulates itself on having abolished it. That is fine and workable, but nothing in the type system ties
`len(frames in Body)` to `len(Records)`: an `Encoder` that emits nothing for a record, or a `Framer` that
groups, silently shifts every attribution by one. The `Decoder` side explicitly supports one-frame-to-many;
the inverse invariant on the encode side is assumed and unstated.

**`rt.Batcher()` hands every source a timer dance.** `UntilNext() (time.Duration, bool)` requires the caller
to build and stop a `time.Timer` per iteration — allocation per batch and the classic
`if !t.Stop() { <-t.C }` footgun — and Benthos's version of this API is consumed by the *framework*, not by
every connector. It is also unclear why a "pure policy with no goroutine" needs `Close(ctx) error`.

**`Schema` is undefined, and it is load-bearing.** `StreamDesc.Schema *Schema`, `SchemaApplier.SupportedChanges()
[]SchemaChangeKind`, `ApplySchemaChange(ctx, ch SchemaChange)`, `record.SchemaRef`, `DriftPolicy`, and
`Build`'s refusal of "DriftEvolve against a sink whose `SupportedChanges` does not cover the stream's
possible changes" all rest on a type that never appears. Missing helper types in a proposal are normal
(`telemetry.Factory`, `Throughput`, `WorkerStatus`, `store.Lease`, `Event` are all absent too, harmlessly);
this one is not, because the entire drift story and one of the four capability negotiations cannot be
evaluated for implementability without it.

**One genuinely defensible use of generics, on a thin justification.** `Tracker[P any]` is the only type
parameter in the document — correctly, and the proposal deserves credit for not making `Record` generic (the
package comment's reasoning for that is exactly right). But the stated justification is "used with
`record.Position` in production and with plain ints in its own tests", which is the weak argument; the strong
one is that `P` avoids boxing an 80-byte `Position` into an `any` on every admission. State that instead.
Nothing else in the document is gratuitously generic, which on this axis is a pass.

---

## Mandatory boilerplate per connector, concretely

Measured against the document's own walkthrough (a), which is presented as "the whole cost of adding a
source".

**Source: 84 lines total, of which ~52 are mandatory non-domain code.**

| item | lines | avoidable? |
|---|---|---|
| imports (5 canal packages + stdlib) | 8 | no |
| `init` + `AddSource` + `Name` + `Spec` skeleton | 8 | no |
| `Caps` literal (`DefaultOrdering`, `Boundedness []T{...}`, `LaneKinds []T{...}`, `MaxLanes`, `Replayable`) | 7 | partly — the interface-backed bools are derivable (D11) |
| framework handles as struct fields (`rt`, `lane`, `p`) | 3 | no |
| cold/warm-start branch in `Open` (`Assigned` → announce or restore) | 12 | **yes** — two methods delete it (D13) |
| `Position` construction per `Read` (token encode, codec, `Safe`, `At`, `Label`) | 6 | partly — `At` and `Seq` are the core's to fill |
| `Commit` forwarding to `Persister`, `Close` | 2 | **yes** — embed `*connector.Persister` |
| `New` closure plumbing | 4 | no |
| **subtotal** | **~52** | |
| actual file-reading logic | ~32 | |

So **~62% of the simplest possible source is boilerplate**, and the single largest block of it (12 lines) is
the most error-prone logic in the file. For comparison, a Benthos input of equivalent function is ~25–30
lines total. The extra ~55 buys real things — declared capabilities, a config spec that drives the UI,
durable lane rows, opaque resume — so this is not damning; but the honest number to quote to the synthesiser
is **~50–55 lines of mandatory non-domain code per source, of which ~20 are removable without giving
anything up**.

**Sink: 21 lines total, ~17 mandatory, ~4 of logic.** This is genuinely excellent and is the strongest
single ergonomic result in the document. Add one capability (say `Partitioner` + `ResponseSink`) and it grows
by 2 bools + 2 methods + the D11 panic risk.

**Per optional capability: 1 `Caps` bool + 1–2 methods + a registration panic if they disagree.** A source
implementing all seven optional interfaces maintains fourteen facts about itself that the compiler already
knows.

**Discoverability from godoc alone: good, with two gaps.** The doc comments are the best part of this
proposal — they state the rule, name the prior-art scar, and say what not to do. A connector author reading
`connector`'s godoc would get most of the way. The gaps: the concurrency model is stated per-method and
inconsistently (`Read` yes, `Buffer`/`Transform`/sink-reconnect no), and the three-blob resume story
(`LaneSpec.Resume` vs `Committed.Token` vs `StateHandle`) cannot be understood from any single doc comment.

---

## What this proposal does better than any plausible alternative

Stated plainly, because the synthesiser should take these whether or not the rest survives.

1. **A sink with zero progress awareness.** `Write(ctx, *Request) error` where nil means durable, and the
   core alone maps that to a source's advance. No `preCommit`, no `flush` versus `commit`, no reporter, no
   "returning `{}` means I own offsets now". A sink author *cannot* get checkpointing wrong because
   checkpointing is not in their vocabulary. Every offset-store design has to work to reach this; this one
   gets it for free, and it is the single best ergonomic property in any of the proposals.

2. **`GroupTTL` + `Leaks()` as Go's answer to Rust's `Drop`.** Vector gets ack safety from finalizer drop
   semantics that Go cannot express. Naming a reaper instead, and turning "someone forgot to settle" from a
   silent stall into a `LeakDetected` condition carrying group, lane and stage, is strictly better than the
   thing it substitutes for. Keep this verbatim, and add the `Close` it is missing.

3. **The replay window *is* the backpressure budget, and it is exported.** `Tracker.Budget()` →
   `canal_lane_replay_window_records` gives an exact answer to "how much will I re-read after `kill -9`",
   because the same number caps admission. One knob, one guarantee, one metric. No prior-art ack pipeline
   can answer that question at all.

4. **`RetryPolicy.Terminal` has no valid zero, so unbounded retry is inexpressible.** Making the dangerous
   default *unconstructible* rather than merely discouraged is the right use of an invalid zero value, and it
   is what stops a poison record from livelocking the ledger (given D6's `Abandon` is repaired).

5. **`Position.Safe` as a core-respected field.** "Commit the last `Safe` position at or before the resolved
   prefix" moves transaction-boundary correctness out of every CDC connector and into one core rule, at the
   cost of one bool a connector sets to `true` and forgets. Cheapest correctness-per-line in the document.

6. **One type-assertion site.** `ResolveSource`/`ResolveSink` collapsing optional interfaces into a
   serialisable struct once, so the engine, the API, the UI and the conformance kit all read fields rather
   than asserting, is the correct reconciliation of "optional interfaces are idiomatic Go" with "a browser
   cannot type-assert". The `Caps` duplication (D11) is the wrong half of the same idea; this is the right
   half.

7. **`Batcher` as pure policy with no goroutine**, plus the `Splitter` that Benthos documents itself as
   lacking. `Add` returning "flush now" lets the owner keep its own `select` loop instead of fighting a
   timer it does not own.

8. **`Deframer.Split` being literally `bufio.SplitFunc`.** Every existing Go splitter is a canal deframer,
   and no author has to learn a new shape. More designs should steal this move rather than inventing.

---

## Verdict

Adopt the connector-facing *shape* — four-method `Source`, three-method `Sink`, progress-blind sink,
declared-caps-plus-optional-interfaces, `Position` with `Safe`/`Label`/opaque `Token`, `fault`'s closed
class set with `Retryable`/`Settles`/`Terminates`, the ledger's budget-as-replay-window. Do not adopt the
concurrency contract (D1), the record privacy model as layered (D2/D3), `Copy`-for-fan-out (D4), the
`Resolve` closure (D6), the `BatchError` triple (D7), the sentinel/`errors.Is` story (D8), the batch-scoped
position for discrete lanes (D9), or the init-time probe construction (D11) without repair. The nine-package
layout needs an acyclicity check before anything is frozen (D5).
