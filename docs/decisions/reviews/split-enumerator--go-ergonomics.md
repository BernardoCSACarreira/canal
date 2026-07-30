# Review — `split-enumerator.md`, through the Go-ergonomics-and-implementability lens

**Reviewer stance:** hostile expert. Lens: *would a competent Go engineer enjoy writing a connector against this,
and is this real Go rather than transliterated Java?* Correctness of the distributed protocol, extensibility and
prior-art fidelity are other reviewers' lenses; I touch them only where an ergonomic choice causes a correctness
consequence.

**Score: 5 / 10.**

**Verdict.** The *taste* is Go. Nobody transliterated Flink: `context.Context` is on every blocking method from
commit one, callbacks are replaced by values, the enumerator is confined to one goroutine instead of managed with
`callAsync`, IDs are comparable structs, the registry is a value type with a global default, enums are closed,
`iter.Seq2` and `bufio.SplitFunc` are reached for correctly, and no plugin interface carries a type parameter. Those
are the marks of an author who has written Go. But the document has not been *type-checked*, and two of its three
flagship mechanisms do not work as specified. Concretely: the leaf package layer contains a hard import cycle; the
central config accessor and the central config type have the same name in the same package; the registration-time
capability check verifies a phantom type parameter that is unrelated to the value being registered (falsified below
by a 20-line program); `opt.Opt` cannot round-trip through `encoding/json` at all and `omitempty` on it is dead on
Go 1.23; `record.Record` is **504 bytes** copied by value on the hot path (measured, below) and its metadata map is
shared across fan-out branches with no copy-on-write mechanism, which is `fatal error: concurrent map writes`; and
`prefix.Tracker.Track(seq, cursor, n)` demands the descendant count at ingest, before the transform chain has
decided it, with no method to correct it — so a single filtered record wedges the checkpoint permanently.

The sink side is genuinely excellent and I would enjoy writing one. The source side is the heaviest of any plausible
alternative: **~140 lines of mandatory boilerplate for a minimal real source, ~260 for a chunkable one**, of which
~60 lines are subtle concurrency the design pushes onto the connector author *because* it declined to write Flink's
`SplitFetcherManager`. The 35-line sugar path is real but sits at the bottom of a cliff, and it silently declares the
one capability §22.4 identifies as causing silent data loss when wrong.

---

## 1. What is genuinely good Go here

I state this first so the defect list is not read as a rejection of the whole document.

- **`context.Context` on every method that can block, from the first commit.** Part-0 axiom #7 taken literally.
  Connect's absence of it cost KIP-419 seven years; Benthos carries `// TODO V5: Add context here`. This proposal has
  no such debt anywhere.
- **Growing the core through `EnumeratorContext` / `ReaderContext` instead of through the plugin interface.** §7.4's
  justification is the single best-argued paragraph in the document, and it is correct *specifically because it is
  Go*: Go has no default methods, so adding a method to `Reader` breaks every connector, full stop. Putting every
  new core capability on a core-implemented interface is the only growth path that exists. Connect's
  `catch (NoSuchMethodError | NoClassDefFoundError e)` in its own javadoc is the counter-example, and the author
  cites it accurately.
- **No type parameters on any plugin interface.** §9.4 is right, right for the right reason (FLIP-191 could not
  remove `GlobalCommitter` "due to the typed parameters"), and it is the discipline that will keep canal's plugin
  surface evolvable. Constraint #5 satisfied in the direction that matters.
- **`SplitID` as a comparable struct with `String()` for logs only.** A perfect Go map key, and the anti-pattern it
  avoids (Flink CDC parsing `table:chunkID` back out in three places) is real.
- **`Kind` reduced to two values with boundedness read off `PositionRange.To`.** No mode enum, no pipeline-type
  field, nothing to switch on. `PhaseOf(PlanState) Phase` as a *function* rather than a field is the same instinct
  applied again and it genuinely does make `grep -r "case Phase"` return only the renderer.
- **`WriteResult` + `RecordFailure` correlated by a stable `RecordID`, with all four `(res, err)` combinations
  enumerated in the doc comment.** This is R7 discharged in the type system, and it is the most transplantable block
  in the document. Benthos's positional batch identity and its author's own `// Deprecated: This method is harmful`
  are the exact cost this avoids.
- **`Disposition` as a five-value closed enum with values-in / values-out `Commit`.** Replacing Flink's mutable
  `CommitRequest` callback object with a request slice and an outcome slice is wire-safe, table-testable and
  metric-labelable. Deleting `signalFailedWithKnownReason` is the right call.
- **`Registry` as a value type with `Clone`/`With`/`Without` plus a cached `Descriptor` that requires no
  instantiation.** Testable, sandboxable, and the connector-list page cannot be broken by a constructor that panics.
- **`Buffer.Durable() bool` deciding where the acknowledgement boundary sits**, and `PushOutcome` putting R6's
  rejection path in the return type. R4 and R6 as expressions rather than prose.
- **`blob.JSON[T]()`** as the "I have not thought about my state format yet" one-liner that is still
  forward-compatible. Small, and exactly the right ergonomic instinct.
- **`fail.Class` closed, with `Retryable()`/`Terminal()` derived in one place**, so a class and a flag cannot
  disagree. Per-stage `Stage`. This is the design-rules taxonomy turned into real Go and it is better than what any
  surveyed framework ships.
- **The sink really is small.** `Sink` 2 methods + `Writer` 4, and §18.4's JSONL sink is 45 lines including its
  registration spec. That is competitive with Benthos's three-method output and strictly more expressive.

---

## 2. Defects

Ordered by severity. Each names the interface or interleaving at fault and the concrete consequence.

### D-1 (fatal) `fail` ↔ `record` is an import cycle. The leaf layer does not compile.

§3 states the dependency direction as normative and claims `internal/archtest` enforces it:

```
├── record/  Record, Payload, Meta, Change, Origin, RecordID, Batch   (opt, ord, blob, fail, schema)
├── fail/    Class, Error, Stage, Diagnostic, Disposition             (nothing)
```

§5.1 then gives `Record` an `err *fail.Error` field and a `WithError(*fail.Error) Record` method — so `record`
imports `fail`, as declared. But §14.1 gives `fail.Error`:

```go
type Error struct {
	...
	Split   opt.Opt[split.SplitID]
	Record  opt.Opt[record.RecordID]
	...
}
```

`fail` imports `record`. `record` imports `fail`. That is a compile error, not a style problem, and it is in the two
packages every other package depends on. It also makes `fail`'s stated dependency set ("nothing") false in three
directions at once (`opt`, `split`, `record`), which means the `archtest` the proposal offers as its structural
guarantee against drift would fail on the proposal's own types on day one.

The fix is not free either: `fail.Error` needs `Split`/`Record` for the DLQ record and the per-record error path, so
either `RecordID`/`SplitID` move down into a new identity leaf (a fifth leaf package, and `SplitID` embeds
`schema.StreamID`, so `schema` moves too or `StreamID` splits), or `fail.Error` degrades to carrying strings — which
loses the typed provenance that §14.2's `DeadLetter` is built on. Neither is hard; both are unbudgeted, and the
choice reaches into §3's four "rules that make this a dependency *direction*".

### D-2 (fatal) `cfg.Field` is declared twice in package `cfg` — as the central spec type and as the central accessor.

§11.1:

```go
type Field struct {
	Path Path
	Kind Kind
	...
}
```

§11.2:

```go
func Field[T any](c Config, p ...string) T
```

Same package, same identifier. `Field redeclared in this block`. Both spellings are then used side by side in the
proposal's own sample connectors (§18.1 uses `cfg.Field{Path: cfg.P("url"), ...}` inside the spec literal and
`cfg.Field[string](c, "url")` in the constructor, eleven lines apart).

This matters beyond the rename because the accessor is the document's answer to the single ergonomic complaint it
levels hardest at Benthos — "a 20-line error ladder per constructor" — and it is presented as the flagship D10
divergence (`**No (T, error) accessor ladder**`). The flagship ergonomic has never been compiled. Renaming it
(`cfg.Get[T]`, `cfg.Value[T]`) is a two-minute fix; the signal is that nothing in §11 was run through a type checker,
which is exactly the thing a "the deliverable is the interface set" proposal cannot afford.

Two further problems in the same accessor, which survive the rename:

- `Field[T](c Config, ...)` takes `Config` **by value** while deferring errors to `c.Err()`. For that to work
  `Config` must hold a pointer to a shared error slot, which makes it a handle masquerading as a value: two
  constructors handed copies of the same `Config` cross-contaminate each other's errors, and two goroutines calling
  it race on the slot with no mutex mentioned. `Config struct{ /* unexported */ }` hides the hazard rather than
  removing it.
- The deferred-error idiom in Go (`bufio.Scanner.Err`, `sql.Rows.Err`) is safe because the *value* is not used on
  failure. Here it is: §18.1 builds `&poller{url: cfg.Field[string](c, "url"), ...}` and *then* returns `c.Err()`, so
  a mistyped path yields a poller with an empty URL that the engine must be trusted to discard. Acceptable, but it is
  strictly weaker than the `(T, error)` ladder it replaces, not merely shorter.

### D-3 (fatal) `RegisterSource[E, R]` verifies a phantom type parameter. The "a declared capability can never be a lie" property is false.

This is the load-bearing safety claim of §10.2 and it is repeated in §9.1, §9.4, §10.3, §20 (D9) and §21. The
mechanism:

```go
type SourceSpec[E connector.Enumerator, R connector.Reader] struct {
	Name string; Version string; Title, Summary, Docs, Notes string
	Support SupportLevel
	Config *cfg.Spec
	Capabilities connector.SourceCapabilities
	Open func(ctx context.Context, c cfg.Config, sc connector.SourceContext) (connector.Source, error)
}

func RegisterSource[E connector.Enumerator, R connector.Reader](s SourceSpec[E, R])
```

`E` and `R` appear in **no field of `SourceSpec`**. `Open` returns `connector.Source` — an interface whose
`NewEnumerator` may return literally any `Enumerator`. So the type parameters are phantoms: the compiler cannot
relate them to the value, and the init-time assertion `var e E; any(e).(SupportsChunkedSnapshot)` interrogates a type
the author *nominated*, not the type the source will *construct*.

I verified this against the real compiler (Go 1.23.6, `/private/tmp/.../scratchpad/optcheck/phantom`):

```
declares-chunkable           chunkable-detected=true
declares-chunkable           runtime-actually-chunkable=false
```

A spec registered as `RegisterSource[*chunkyEnum, *plainReader]` whose `Open` returns a `Source` producing a
`*notChunkyEnum` passes the registration check and then fails every `SupportsChunkedSnapshot` call at runtime.
`Chunkable: true` is a lie the mechanism cannot catch. §19.2's entire submit-time-refusal narrative
("Had the sink been append-only, the pipeline would be **refused here**") depends on these flags being unfakeable, and
they are not.

Second failure mode, from the same program:

```
interface-instantiated       chunkable-detected=false
```

Because `E`'s constraint *is* an interface, `RegisterSource[connector.Enumerator, connector.Reader]` is legal, and
`var e E` is then a nil *interface*, so `any(e)` is nil and **every** capability assertion returns false. Combined
with §10.2's reverse check ("an implemented optional interface is not declared" also panics), an author who
instantiates with the interface types — the natural move when the compiler complains about a phantom parameter — gets
either a false panic ("you declared Chunkable but do not implement it", when they do) or a silent pass on the reverse
check. The panic message will point at the wrong thing.

Third failure mode, structural: **five of the eleven source-side optional interfaces are not on `E` or `R` at all.**
`ConnectionTestable`, `DynamicChoices`, `Validatable`, `AdoptsState` and `Backlogger` are `Source`-level (§9.2's own
signatures take `cfg.Config` or a `StreamID`, not split state), and `Source` is produced by `Open` at runtime with no
type parameter to assert against. So `Testable: true ⇒ implements ConnectionTestable` is *uncheckable at init* by
construction. §10.2 lists six panic conditions; at least one of them cannot be implemented.

Fourth, `SupportsChunkedSnapshot` bundles `SplitKeySpace` (an enumerator concern, called from `Poll` per §19.2) and
`ReadRange` (a reader concern — §9.2 says "it is what the reader calls for a Scan split"). A single interface
straddling the enumerator/reader split means the check must assert it against `E` *or* `R` and the document never
says which; asserting against either one requires that type to carry the other role's method. The interface as
written cannot be implemented coherently by the split it exists to serve.

Note also the direct contradiction with §9.4: "generics on a plugin interface are a future migration" *and* "the type
parameters let `Register` verify the declared capabilities against the types that will implement them". Those two
rules cannot both hold. A sound check requires `Source[E, R]` to be generic — i.e. a type parameter on a plugin
interface. The document chose the right rule and then claimed the property that only the rejected rule buys.

### D-4 (fatal) `opt.Opt[T]` cannot marshal, and `omitempty` on it is dead on Go 1.23. Every persisted cursor is silently dropped.

`opt.Opt[T]` has unexported fields `v`, `ok` and no `MarshalJSON`/`UnmarshalJSON`. Every durable and wire struct in
the document wraps its most important fields in it with a json tag: `Split.Cursor`, `Split.Watermark`,
`Split.Schema`, `PositionRange.From/To`, `PlanState.ExpectedCompletions`, `StreamState.Retired`, `Opening.Restored`,
`Committable.Expires`, `CommitOutcome.Committable`, `Enumeration.Expect/Backlog`, `Backlog.Records/Bytes`,
`Origin.Pos`, `Change.Key/Before/After`, `cfg.Field.Default/RequiredIf/VisibleIf/Component`,
`PlanSummary.Fraction/ETA`, and every field of `SplitSummary`.

Measured, Go 1.23.6:

```
marshalled split: {"cursor":{},"watermark":{}}
```

Two independent failures in one line:

1. **The cursor bytes vanish.** §19.1's T8 walkthrough shows a checkpoint key containing
   `splits:[{id:{...}, cursor:{version:1,bytes:"2026-07-30T09:00:00Z"}, ordered:true}]`. As specified, that
   marshals as `"cursor":{}`, the restore hands the connector an absent cursor, and T9's "**No record is
   re-emitted and none is lost**" becomes "the pipeline restarts from the beginning of the split". The single
   milestone the whole build order gates on (§23 step 4, `kill -9` and resume) does not work with the types as
   written. This is silent — `encoding/json` reports no error for an unexported-field struct.
2. **`omitempty` never fires.** `encoding/json`'s `omitempty` applies to false, 0, nil pointers, nil interfaces and
   empty arrays/slices/maps/strings — never to structs. `omitzero` landed in Go **1.24**; `go.mod` says 1.23.6. So
   every absent `Opt` field emits `"key":{}`. A plan with 200,000 `Scan` splits (§19.2's own scale) emits ~200,000
   × 3 empty objects it does not need, in a design whose §16.4 is specifically about paging because Flink CDC's
   completion set "turned out too large to ship in one message".

Both are fixable — write `MarshalJSON`/`UnmarshalJSON` on `Opt[T]` (a value-receiver marshaller and a
pointer-receiver unmarshaller, plus a `null`-means-absent convention), and either move to Go 1.24 for `omitzero` or
give up on omission. But the *reason* this matters for my lens is that §4.1's stated justification for `opt` existing
at all is "a nil pointer in a serialisable struct is a wire-format hazard" — and the type built to avoid a wire
hazard is itself unserialisable. Meanwhile `ord.Ordinal` in the very next section uses `Order []byte` nil-as-absent
*and* `Scalar *float64`, i.e. both of the conventions `opt` exists to replace, in the type the document calls
"the load-bearing type of the whole proposal". Two absence conventions plus the type that was supposed to unify them
is an R9 smell in the leaf layer.

### D-5 (fatal) `prefix.Tracker.Track(ctx, seq, cursor, n)` needs the descendant count before the pipeline knows it. One filtered record wedges the checkpoint forever.

`n` is documented as "the number of DESCENDANTS the pipeline will produce from this record: 1 for a pass-through, 0
for a filtered record (which settles immediately), k for a 1→k expansion, and k for a fan-out to k sinks." §19.1's T5
places the call **at ingest**, immediately after the core stamps `Origin`:

```
prefix.Tracker.Track(seq=1, cursor=Some("2026-07-30T09:00:00Z"), n=1)
```

At ingest the core cannot know `n`. The transform chain has not run. `Transform.Process(ctx, b) ([]record.Batch, error)`
returns whatever it likes and reports nothing about what it dropped. The interleaving that breaks:

1. Records `Origin.Seq` 1..10 ingested; `Track(seq, cursor, n=1)` for each.
2. A transform filters seq 5 (a routing predicate, a schema-mismatch drop, `reject_errored`).
3. Nothing ever calls `Settle(5)`. `Fail(5, …)` is semantically wrong — that means permanent failure and dead-letter.
4. `Resolve()` returns `(_, false)` forever. The split's cursor never advances past seq 4.
5. `canal_checkpoint_age_seconds` climbs monotonically. `Stuck()` reports "seq 5 outstanding for 12 minutes" — an
   excellent diagnostic for a bug the type system created.
6. The in-flight bound fills (capacity is the in-flight limit), `Track` blocks, and the split stalls permanently.

There is no `Adjust(seq, delta)`, no `Drop(seq)`, no way to say "this became zero descendants". The only escape is for
the *engine* to diff each transform's input `Batch` against its output `[]Batch` by `RecordID → Seq` on every stage of
every batch — an O(n) set construction per stage per batch on the hot path, and it is neither in the interface nor in
the cost accounting.

Corroborating evidence that this was never traced: §19.3.1's T5 calls `Tracker.SettleDeadLettered(12_411)`, a method
that **does not exist** in §13.6's `Tracker` API (`Track`, `Settle`, `Fail`, `Resolve`, `Pending`, `Stuck`). The
walkthrough and the interface disagree at the exact point where the document says "this is the point where a design
either loses data or admits a decision".

`Track` is not a peripheral helper. §0 claims "the checkpoint stops being a separate subsystem"; §13.6 is where that
claim is cashed; and §20's D2/D4/D13 answers all reduce to "`Track(seq, cursor, n)` absorbs it". The one signature
carrying fan-out, filtering, 1→N decode and rebatching takes a parameter that cannot be known at the only call site
the document shows.

### D-6 (fatal) `Meta` is a map shared by value-copy with no copy-on-write. Fan-out is `fatal error: concurrent map writes`.

§5.1 asserts: "It is a value type; copying a `Record` is cheap and copies neither the payload bytes nor the metadata
map (both are copy-on-write behind the accessors)."

§5.3 then defines:

```go
type Meta struct{ m map[string]any }

func (m *Meta) Set(k, v string)
func (m Meta) Get(k string) (string, bool)
func (m Meta) All() iter.Seq2[string, any]
```

There is no copy-on-write mechanism. A `Meta` value copy shares the same `map` header. `Set` is a plain map write.
§15.5 ships fan-out as a first-class feature ("a `broker` sink with `mode ∈ {all, …}`") and §13.6 supports it with
`n = branches`. So:

1. `broker` in `all` mode hands `Record` copies to sink A and sink B. Both `Meta` structs point at one map.
2. Each branch runs on its own goroutine (that is the point of `all` rather than `all_sequential`).
3. Branch A's retry wrapper calls `Set("attempt", "2")`; branch B's `Encoder` ranges `Meta.All()`.
4. `fatal error: concurrent map read and map write` — or with two writers, `concurrent map writes`. Both are
   **unrecoverable**: no `recover()`, no per-pipeline isolation, the whole `canal serve` process dies taking every
   other tenant's pipeline with it.

The same defect in `Payload`:

```go
type Payload struct {
	bytes      []byte
	structured any
	owned      bool
}
func (p *Payload) AsStructuredMut() (any, error)  // "deep-cloning first if and only if the current value is shared"
```

`owned bool` cannot express "shared" across value copies. When the broker copies the `Record`, both copies get
`owned` with the same value and neither can observe the other's existence. If `owned == true`, `AsStructuredMut`
skips the clone and both branches mutate one `map[string]any`. Copy-on-write requires shared state — a pointer, a
refcount, an `atomic.Int32` — and a plain bool in a value struct is provably insufficient. Benthos gets this right
because its `Message` is a pointer with an internal part; the proposal copied the *API* (`AsBytes`/`AsStructured`/
`AsStructuredMut`/`HasBytes`) without the ownership machinery that makes it sound.

Note this also breaks the flagship immutability claim by a second route. `Batch` is `[]Record`, passed to
`Transform.Process(ctx, b record.Batch)`. Slices share their backing array, so a transform can write
`b[0] = b[3]` and overwrite the caller's provenance wholesale. Unexporting `Record.id`/`Record.origin` protects the
*fields*, not the *slot*. "A transform physically cannot corrupt checkpoint identity" (§5.1, §15.3, §20 D1) is false
for two lines of ordinary Go.

### D-7 (major) `Reader.Fetch` must not block, so every streaming connector hand-writes a goroutine, a channel and a shutdown protocol — and R6's "bounded by construction" is void there.

§7.3: "`Fetch` … **MUST NOT block indefinitely**: it returns `FetchIdle` with a `RetryAfter` when nothing is
available, and the core does not busy-wait." §7.3's threading note: "the core calls `Assign`, `Revoke`, `Fetch`,
`Snapshot` and `Close` from a single goroutine per `Reader`, never concurrently."

Those two sentences together are the design's biggest ergonomic cost, and it is self-inflicted. The reason `Fetch`
cannot block is the single-goroutine confinement: if `Fetch` blocked on a `pgx` replication read, the checkpointer
could never call `Snapshot`. So the confinement (a genuinely good idea) forces polling, and polling forces every
streaming source author to write:

```go
type reader struct {
	out    chan record.Record   // or chan SplitBatch
	cancel context.CancelFunc
	wg     sync.WaitGroup
	err    atomic.Pointer[error]
}
// a fetcher goroutine per split, started in Assign, stopped in Revoke and Close,
// with its own bound, its own error propagation, its own leak-free teardown,
// and its own cursor bookkeeping so Snapshot and Fetch agree.
```

That is 40–60 lines of the most error-prone Go there is, per source, and it is the exact code Flink writes *once* in
`SourceReaderBase` + `SplitFetcherManager` + `FutureCompletingBlockingQueue`. §12.1 boasts "Flink needed ~400 lines
(`FutureCompletingBlockingQueue`) to express it. In Go this is a buffered channel plus `select` on `ctx.Done()`" —
true, and the proposal then declines to write those few lines in core and makes N connector authors write them
instead. That is a cost *transfer* presented as a saving, and it lands on the one method the document itself calls
"the one hot-path method in the entire plugin surface".

Two further consequences:

- **R6 is violated at precisely the place the buffering was pushed to.** §12.1: "every edge in the pipeline is a
  bounded Go channel, so unbounded growth is not expressible anywhere". A connector-internal fetcher channel is not
  an edge the engine owns, is not bounded by anything the core can see, and is not visible to `Depth`/`Bytes`,
  the tap, or `blocked_seconds_total`. §13.4 step 1's parenthetical — "readers block on `Fetch`'s caller, not on
  I/O" — is false for exactly the readers this design forces into having an internal fetcher: they keep pulling from
  the network into their private buffer while admission is stopped. The design's answer to "a buffer without a
  rejection path is not a buffer" has a buffer with no rejection path in every streaming connector.
- **A latency floor equal to `RetryAfter`, chosen by the connector**, with no way for the core to wake a reader when
  something changes. There is no `Fetch(ctx)` variant that returns when ctx is cancelled *or* data arrives, which is
  the one thing Go makes trivial and Java does not.

The idiomatic alternative is one line of interface change and ~30 lines of core: let `Fetch(ctx)` block until data or
`ctx.Done()`, give the reader host a `context.WithCancel` per fetch round, and have the checkpointer cancel that ctx
to reclaim the goroutine before calling `Snapshot`. Go's `select` is the whole mechanism. The document reaches for
Flink's shape here and Go's shape everywhere else.

### D-8 (major) `record.Record` is 504 bytes, copied by value on every stage, and the proposal's own sink example defeats the payload cache.

Measured with the field set exactly as specified (`unsafe.Sizeof`, Go 1.23.6, amd64):

```
sizeof Ordinal:       64
sizeof Opt[Position]: 72
sizeof Origin:       128
sizeof Opt[Change]:  240
SIZEOF RECORD:       504
```

`Batch` is `[]Record`, so `for _, r := range b` copies 504 bytes per record per loop, and there is such a loop in
every sink, every encoder, every transform, and `Batch.Splits()`. The dominant term is `Opt[Change]` at 240 bytes —
carried inline on **every** record including the ~100% of records in a non-CDC pipeline where it is absent. The
standard Go fix is `Change *Change` (8 bytes, nil = absent) or `Payload`/`Meta`/`Change` behind one shared pointer,
which is what Benthos and Vector both do. The proposal's own §5.1 comment ("copying a Record is cheap") is off by
roughly a factor of ten.

Compounding it, the payload cache does not work through a value copy. §5.2 gives `AsBytes` a **pointer** receiver and
documents "It caches the result." §18.4's own sink then writes:

```go
for _, r := range b {
	p, err := r.Payload.AsBytes()
```

`r` is a copy; the cache lands in the copy and is discarded when the loop iterates. Every stage that touches the
payload re-encodes it from the structured view. To cache you must write `for i := range b { … b[i].Payload.AsBytes() }`,
which is not what the document shows, and which no connector author will infer from the godoc. The one performance
property the record model exists to provide — "a pipeline that never inspects the body pays nothing", "a sink can take
the cheap path" — is defeated by the loop form in the reference implementation of the reference sink.

### D-9 (major) `prefix.Tracker` blocks on `sync.Cond`, which cannot honour the `ctx` in its own signature; and per-split trackers contradict §17.1's cardinality rule.

§12.2: "the backpressure mechanism (`Track` blocks when full — four lines of `sync.Cond`, which is where
backpressure comes from for free)". §13.6: `func (t *Tracker[T]) Track(ctx context.Context, seq uint64, cursor opt.Opt[T], n int) error`.

`sync.Cond` has no `select` form and no context integration. A goroutine parked in `cond.Wait()` cannot be woken by
`ctx.Done()`. So either `Track` accepts a `ctx` it structurally ignores — which is worse than not taking one, because
every caller will assume cancellation works and graceful shutdown will hang on a full tracker — or each tracker needs
a watcher goroutine to `Broadcast` on cancellation. The correct Go primitive here is a buffered-channel semaphore or
`golang.org/x/sync/semaphore` (`Acquire(ctx, 1)`), both of which are ctx-aware and roughly the same four lines. The
"four lines of `sync.Cond`" claim is the one place the document reaches for a Java-shaped primitive where Go has a
better one, and it produces a shutdown hang.

Second problem in the same section. §13.6 says the tracker "must be **PER SPLIT**". §12.2 says "The engine exposes,
per split: `inflight_records`, `inflight_bytes`, `blocked_seconds_total`, and a `utilization` gauge in [0,1]". §19.2
runs 320 splits; §10.1's `MaxSplitsTotal` and §16.4 contemplate 200,000. §17.1 then states: "The metric label
vocabulary is **hard-closed to pipeline topology** — `{tenant, pipeline, generation, stage, component, class}` …
Unbounded per-stream and per-split detail is served from the **read model**, not from labels. That split is what stops
a 200,000-chunk snapshot from producing 200,000 metric series."

§12.2 and §17.1 are in direct contradiction: four per-split series × 200,000 splits is 800,000 series from the exact
mechanism §17.1 forbids. One of them has to go, and the choice is not free — §19.3.1's operator narrative
(`Tracker.Stuck()` rendering "split orders/change/0 sequence 12,207 outstanding 4.2s") is one of the best things in
the document and it needs per-split data from *somewhere*. It just cannot be Prometheus labels. Resource-wise:
200,000 trackers each with a `sync.Cond`, a mutex, a deque and (if ctx-cancellable) a watcher goroutine is a
non-trivial memory and scheduler cost that §22 does not name.

### D-10 (major) The unexported-provenance record cannot cross the process boundary the design exists to support.

`Record` has unexported `id`, `origin` and `err`. `SplitBatch` declares `Records []record.Record` with
`json:"records"`. §16.2's central claim is that `engine/remote` needs no new interface because "`Reader`'s methods are
all `(ctx, serialisable) → (serialisable, error)`", and that the proto "could be generated from the Go interfaces" —
"the property Connect fails on five counts".

It cannot, for `Record`:

- `id`, `origin` and `err` are unreachable from any other package, so a codegen tool has nothing to emit and
  `encoding/json` emits nothing. A remote reader that decodes bytes into records and a remote worker that ships a
  `SplitBatch` both lose provenance silently. Fixing it means `record` exports a marshaller — at which point
  "a transform physically cannot corrupt checkpoint identity" degrades to "a transform that imports `record`'s wire
  helper can", and the structural guarantee becomes a convention.
- `err *fail.Error` contains `Cause error`. `error` is an interface with no wire form. So the mark-and-route error
  model — §5.1's "Errors travel ON the record … which is what makes mark-and-route error handling need no extra
  interface vocabulary" — cannot cross the boundary at all. Concretely: in the coordinated deployment a worker's
  decoder classifies a record `ClassPermanentMapping`; the core (in the planner process) receives the record without
  the mark and writes it to the sink. §14.3's "strict mode is the default — a record carrying an error is rejected at
  the sink" is unimplementable for a remote reader.

This is not fatal because it is fixable (a `record.Wire` DTO with an explicit conversion in `record`; a
`fail.Error` wire form that flattens `Cause` to a string). But the fix is a transport DTO for the canonical record,
which is precisely the shape R2 exists to forbid ("no transport DTO is ever allowed to become it by omission") and
which §21's R2 row claims does not exist ("the wire form in `engine/remote` is *generated from* `record.Record`,
never the reverse").

### D-11 (major) `EnumeratorContext.Go` starts unowned goroutines, drops errors, and forces a serialise round-trip for a local call.

```go
Go(fn func(ctx context.Context) (EnumeratorEvent, error))
```

Every question a Go reviewer asks about a goroutine-spawning API is unanswered:

- **Who owns it?** The core. **Who stops it?** Unspecified. `Enumerator.Close` "is called on lease loss as well as on
  shutdown", and says nothing about outstanding `Go` calls. A planner that loses its lease and closes its enumerator
  leaves N goroutines running against a source connection the enumerator is about to close — use-after-close on a
  `*sql.DB`, or a leak.
- **What ctx does `fn` get?** Unspecified. If it is the pipeline ctx, a slow discovery survives lease loss. If it is
  per-call, there is no handle to cancel it.
- **Where does `fn`'s `error` go?** Unspecified. If it is logged and dropped, a failed discovery is invisible and the
  enumerator waits for an event that will never arrive — the pipeline idles at `PhaseDiscovering` with a green
  `Connected` condition. This is the same failure shape the document (correctly) condemns in Benthos
  (`if err = aFn(...); err != nil { …Error… }`).
- **Is there a bound?** No `n`, no semaphore, no error on oversubscription. An enumerator that calls `Go` once per
  discovered stream on a source with 40,000 objects starts 40,000 goroutines.
- **Worst, the result must be serialised.** `EnumeratorEvent` is `{From, Kind string, Payload blob.VersionedBlob}`.
  So an enumerator doing an in-process `SELECT` to list tables must `blob.Pack` its own `[]TableInfo` and
  `blob.Unpack` it back in the next `Poll`, in the same address space, to receive the answer to its own function call.
  That is a genuinely unpleasant ergonomic and it will be the single most-complained-about part of writing a real
  enumerator.

Go already has the shape: `errgroup.Group` with `WithContext`. Handing the connector a `*errgroup.Group` scoped to
the enumerator's lifetime, plus a typed result channel the core drains onto the enumerator goroutine, gets the same
confinement with ownership, bounds, error propagation and no serialisation.

### D-12 (major) The single-goroutine confinement is contradicted by lease loss, and the fire-and-forget context methods have a silent stall mode.

Two holes in the threading contract, which is otherwise the document's best Go idea.

**(a) `Close` versus the confinement.** §7.2: "the core calls every method on an `Enumerator` from a single
goroutine, and never concurrently" — and then "`Close` … is called on lease loss as well as on shutdown, so it must
be safe to call while a `Poll` is not in flight." That sentence guarantees the harmless case and is silent on the
hazard. Lease loss is detected by the lease-renewer goroutine (§16.1 `Renew`), which is not the enumerator goroutine.
So either:

- `Close` is queued onto the enumerator goroutine, in which case it cannot interrupt a `Poll` that is blocked on a
  30-second upstream query, and fencing takes 30 seconds to take effect while the *other* planner is already
  writing; or
- `Close` is called from the renewer, violating the confinement the interface doc promises, and every enumerator that
  believed the promise and wrote lock-free state now races.

The document cannot have both, and it needs the first for the promise and the second for the fencing latency. The Go
answer is available and unused: cancel the ctx handed to `Poll` from the renewer (ctx cancellation *is* the
cross-goroutine signal that does not violate confinement), then call `Close` on the enumerator goroutine after `Poll`
returns. That is one sentence in the doc comment and it is missing.

**(b) `RequestSplits`, `SendEvent`, `RequestCheckpoint` return nothing.** All three are called by connector code from
inside `Poll`/`Fetch`, i.e. from the confined goroutine, so each must be a non-blocking send onto a core channel. With
no return value the core's only options on a full channel are to block (stalling the confined goroutine, from inside a
method whose contract says it must return promptly) or to drop. `SendEvent` may drop — §7.5 makes that a contract, and
the `Reconciler` repairs it. But **`RequestSplits(n)` is load-bearing for pull-based assignment and has no repair
path**: §16.4 says "Placement is **pull-based**: `ReaderContext.RequestSplits(n)` raises `PlanView.Demand`, and the
placer places against demand." A dropped `RequestSplits` means `Demand` stays 0, the placer places nothing, `Poll`
sees `Demand: 0` and returns `PlanIdle`, and the pipeline sits at zero throughput with `Connected: True`,
`Assigned: False` and no error anywhere. The `Reconciler`'s three documented repairs (§7.5) do not include
re-deriving demand. The one control signal that must not be lost is the one delivered by the same lossy mechanism as
the ones that may be.

### D-13 (major) `PollSource` sugar auto-declares `ComparablePositions: true` from an arbitrary user string — the exact misdeclaration §22.4 says causes silent data loss.

§18.2:

```
NewReader → pollReader{}, which … wraps the returned cursor as ord.Position{Bytes: s, Order: []byte(s)}
Capabilities → {… ComparablePositions: true, MidSplitResume: true, …}
```

`Order` is contractually "an order-preserving encoding" (§4.3) and §22.4 correctly identifies getting it wrong as
producing "the chunk filter suppresses records it should emit (silent data loss)". The sugar produces it by casting
whatever string the operator's `cursor_field` yields. §18.1's default is `"updated_at"` (RFC3339 — lexicographic, so
fine). The moment an operator sets `cursor_field: "id"`, `Order` is `[]byte("9")` vs `[]byte("10")` and
`bytes.Compare` says 9 > 10. The connector author never typed the word `Order`, never saw the capability, and cannot
be blamed.

Immediate consequences: `canal_position_fraction` is wrong; `ord.Fraction`'s monotonicity precondition is violated;
any monotonicity assertion the core makes on an `Ordered: true` split (§4.3 lists "monotonicity assertions" as an
`Order` payoff) fires spuriously or, worse, passes while comparing wrongly. Downstream consequence: the §18.3
"upgrade path" narrative has the author replacing `oneSplit` with a real enumerator that emits N splits, inheriting
the lie into a `Chunkable` source where §22.4's silent-data-loss scenario is live.

This is the sugar contradicting §10's central premise. §10.2 panics at init to make a flag unable to lie; the sugar
sets the most dangerous flag in the system on the author's behalf, from data that is unknown until runtime config.
The honest sugar declares `ComparablePositions: false` and offers `canal.LexicographicCursor` / `canal.NumericCursor`
as explicit opt-ins.

### D-14 (major) 2PC recovery ordering is undefined against `Open`, and committables are not bound to a writer identity.

§13.4 step 10 calls `Commit` *after* the checkpoint is durable, in a running pipeline where `Open` has long since
run. §19.3.2's recovery path calls it in a different order: "`RestoreWriter(state)`, then — **before admitting any
record** — the core calls `Commit(ctx, [...])`." Nothing says whether `Open` has run at that point.

`Open` is documented as "assignment-scoped setup: create a session, begin a transaction, ensure a target exists"
(§8.2). A committer whose `Commit` finalises a manifest through a client created in `Open` nil-panics on recovery if
`Commit` precedes `Open`. If instead `Open` runs first, its `Opening.Restored` is `Some(id)` — fine — but `Open` is
also where a writer may "begin a transaction", and beginning a new transaction before publishing the previous
checkpoint's committables is exactly the interleaving 2PC exists to prevent. The document is precise about a dozen
harder orderings and silent about this one.

Second, in the coordinated deployment: `Checkpoint.WriterState` is `map[string][]blob.VersionedBlob` — keyed per
writer — but `Checkpoint.Committables` is `map[CheckpointID][]Committable` with no writer key. `Committable.Splits`
records which splits it covers, which is not the same thing. After a worker set change (the §19.5 `OOMKilled`
scenario), which writer is offered `c1..c4`? For an S3 multipart upload id or a database session-scoped temp table,
the answer determines whether `Commit` can succeed at all, and a `DispositionRetryLater` loop against a writer that
can never commit it is the failure mode. Flink binds committables to a subtask for this reason. Here it is
unspecified, and §22 does not list it.

### D-15 (minor→major) `Position` and `Key` are structurally interchangeable at every comparison site.

```go
type Position struct{ Ordinal }
type Key struct{ Ordinal }
func (o Ordinal) Compare(b Ordinal) (int, bool)
```

§4.3 claims: "`Position` and `Key` are distinct named types over one mechanism. They are not interchangeable and
there is no function mapping one to the other — which is precisely the R9 test."

Embedding promotes `Compare(Ordinal)` onto both, so its *parameter* is `Ordinal`, not the outer type. Therefore
`somePosition.Compare(someKey.Ordinal)` compiles, and `Key{Ordinal: someLSNOrdinal}` and
`Position{Ordinal: somePKOrdinal}` are both constructible with a struct literal from any `Ordinal`. §19.2's own
`ChunkFilter` writes `pos.Compare(c.Watermark.Ordinal)` — correct there, but it demonstrates that `.Ordinal` is the
normal way to call `Compare`, so nothing flags the wrong one. The type distinction holds for *storage* and dissolves
at *comparison*, which is the only place it was protecting anything. The fix is one line each:
`func (p Position) Compare(b Position) (int, bool)` and the same for `Key`, with `Ordinal.compare` unexported.

### D-16 (minor→major) `errors.Is` against the declared sentinels does not work.

```go
var (
	ErrNotConnected = &Error{Class: ClassNotConnected}
	ErrEndOfInput   = &Error{Class: ClassEndOfInput}
)
```
documented as "Sentinels for `errors.Is`, so a caller can test without switching on `Class`."

`*Error` has `Error()` and `Unwrap()` and no `Is(error) bool`. `errors.Is` therefore falls back to `==` on the
pointer. So `errors.Is(err, canal.ErrEndOfInput)` is **false** for the error the document tells connectors to
produce (`fail.EndOfInput()`, which allocates a new `*Error`), and false for any wrapped `fail.NotConnected(...)`.
§16.2 depends on this working — "A dead subprocess or a broken connection is reported as `fail.ErrNotConnected`, so
process supervision reuses the connection state machine" — and §21's R11/D9 answers do too. The whole
`ClassNotConnected`-drives-supervision story rests on a comparison that returns false.

Two lines fix it (`func (e *Error) Is(target error) bool { t, ok := target.(*Error); return ok && t.Class == e.Class }`).
Separately, the sentinels are *mutable* package-level pointers to structs with exported fields, unlike
`errors.New`'s opaque value: any code doing `err := canal.ErrNotConnected; err.Attempts = 3` mutates the global.
`Class` sentinels would be better expressed as `errors.Is` via the `Is` method plus unexported sentinel values.

### D-17 (minor) `ReaderContext.Framer()` hands out a `bufio.SplitFunc` instead of a `bufio.Scanner`.

§7.4 is proud of matching Go's signature: "Its shape deliberately matches `bufio.SplitFunc` — Go already has the
right signature for this and inventing a second one would be gratuitous." The signature is right and the *idiom* is
inverted. A `SplitFunc` is an argument you give to `bufio.Scanner`; the value of the stdlib pattern is that
`Scanner` owns buffer growth, `ErrTooLong`, refill, and the `Scan()/Bytes()/Err()` loop. Telling every byte-oriented
source to "loop on `Framer().Scan`" means each one reimplements `bufio.Scanner`'s buffer management, including the
token-too-long path, which is where the bugs live. The Go-shaped context method is
`ReaderContext.Scanner(io.Reader) *bufio.Scanner` (or a minimal interface over it) — same declared framer, zero
buffer code per connector.

### D-18 (minor) Four durable maps are keyed by a stringified struct that is also stored inside the value — R1's "one entity, two identifiers".

`PlanState.Assigned map[string]Assignment  // key = SplitID.String()` with `Assignment.Split SplitID` inside;
`Checkpoint.Workers map[string]WorkerState` with `WorkerState.Worker`; `WriterState map[string][]VersionedBlob`;
`DedupeState.Keys map[string]KeySet`.

`SplitID` is a perfectly comparable struct and an ideal Go map key; the stringification exists only because JSON
object keys must be strings. The result is that the document's own §6.1 rule — "`String()` exists for logs and metric
labels ONLY. No canal code parses it, and there is a test asserting that no canal package calls `strings.Split` on a
split id" — is defended by a test while the durable assignment table is keyed on that string, and the identifier is
duplicated inside the value. R1's named defect from the abandoned attempt was literally "one entity with two
identifiers". The Go fix is `[]Assignment` with a `SplitID`-keyed in-memory index built on load, or a
`MarshalJSON` on the map type. Either keeps `SplitID` as the only identifier.

### D-19 (minor) `Reader.Snapshot` is unpaged and unconditional on a path the document elsewhere pages.

`Snapshot(ctx, id) ([]split.Split, error)` returns "the current state of **EVERY** split the reader holds". §7.3
contemplates "a reader holding thousands of splits"; §16.4 pages `PlanView.Completions` "from day one" specifically
because Flink CDC's finished-split set outgrew one message. But the checkpoint path re-materialises and re-serialises
every held `Split` — including its immutable `Spec` blob, which by definition has not changed — at every checkpoint,
for every reader. With 200,000 splits across 24 readers at a checkpoint per chunk completion (§13.5 boundary #1),
that is the same scar the document pre-fixed one page earlier, on the hotter path. A `Snapshot` returning only
splits whose cursor moved since checkpoint `n-1`, plus the core diffing against `PlanState.Assigned` (which it
already does, per §7.3's own "makes reconciliation free"), removes it.

### D-20 (minor) `Meta` the interface collides with `Meta` the struct; `Child.Component any` reintroduces the type assertion the design confines to one place.

§15.2 declares `type Meta interface { Children() []Child }` with `type Child struct { Segment string; Component any }`.
No package is given. `record.Meta` is already a struct. If the interface lives in `connector` the collision is
cosmetic but confusing (`connector.Meta` vs `record.Meta` in the same file); if in `record`, it does not compile.

More substantively, `Component any` means the core must type-switch on `any` to find `Writer`s, `Transform`s and
`Buffer`s while walking the tree, and §15.2 says the core also uses it to "propagate `Snapshot(id)` into children" —
a second assertion for an undeclared `Snapshot` method. §9.1's rule is "Every optional interface is type-asserted in
**exactly one place**, `registry.resolve`". The topology walker is the second and third. A small closed sum
(`Child{Segment string; Writer connector.Writer; Transform connector.Transform; Buffer connector.Buffer}` with
exactly one non-nil, or four typed child accessors) keeps the rule.

### D-21 (minor) Per-connector versus per-stream capabilities are two vocabularies for one concept with no stated precedence.

`SourceCapabilities.Chunkable bool` is declared at registration and cross-checked against the type. `schema.Stream`
also carries `Chunkable bool` and `Resumable bool`, per stream, from `Discover`. §10.3's submit-time table checks the
*connector-level* flag ("chunked snapshot ⇒ source declares `ComparableKeys` + `ComparablePositions`"); §19.2's
stream picker constrains against the *per-stream* one. Nothing says what happens when they disagree — a Postgres
source that is `Chunkable: true` at the connector level and encounters a table with a UUIDv4 primary key it cannot
order. The honest answer (connector flag = "I have the code", stream flag = "this object supports it, at runtime")
is derivable but unstated, and until it is stated this is exactly R9's test: two representations of one concept with
a hand-maintained relationship between them.

### D-22 (minor) Graceful shutdown is expressible but not expressed.

The pieces are all present — `RunStopping`, `StoppingSince`, `DrainDeadline`, drained-vs-drain-timeout as distinct
events (§17.3, and that distinction is excellent), `Revoke` "for graceful drain", `FlushDrain`,
`Writer.Close` "called after a final `Flush` on graceful shutdown". What is missing is the ordering, and this is a
document that specifies orderings meticulously everywhere else. Nothing says SIGTERM triggers a final checkpoint;
§13.4's flow is initiated only by `RequestCheckpoint` from a timer, an enumerator, a sink or an operator. Nothing
says the drain sequence is `stop admission → drain → Flush(FlushDrain) → Revoke → Snapshot → Commit → Close`.
`Reader.Close`'s doc actively says splits are *not* returned and "the core recovers them from the last checkpoint" —
which is correct for `kill -9` and wrong as the graceful path, because it converts every clean restart into a replay
of up to one checkpoint interval. For a design whose §22.5 admits checkpoint intervals may be 10 seconds, that is
10 seconds of duplicates on every deploy.

---

## 3. Mandatory boilerplate per connector — concrete line counts

Counting only code an author *must* write before any business logic: interface satisfaction, registration, state
serialisers, and the concurrency the interfaces force. Excluding imports and blank lines. Derived from the
signatures in §7–§10 and calibrated against the proposal's own §18 samples.

| Connector shape | Mandatory lines | Of which subtle |
|---|---|---|
| **Sink, at-least-once** (`Sink` 2 + `Writer` 4 + spec) | **~50** | ~0 |
| **Sink + `SupportsCommitter`** (+3 methods, committable serialiser) | **~95** | ~20 (idempotent `Commit`, `Disposition` choice) |
| **Sugar `PollSource`** (§18.1) | **~40** | ~0 |
| **Minimal real source**, 1 stream, 1 unbounded split, no chunking | **~140** | ~60 |
| **Chunkable CDC source** (+`SupportsChunkedSnapshot`, `SupportsReplay`, `ord.Order` encoders, `Backlogger`) | **~260** | ~110 |

Breakdown of the 140-line minimal real source:

- registration spec + `SourceCapabilities` (18 fields, ~8 of which demand a deliberate decision) — **~28**
- `Source`: `Discover`, `NewEnumerator`, `RestoreEnumerator`, `NewReader`, `Close` — **~22**
- `Enumerator`: `Poll`, `Report` (must handle `Finished`/`Returned`/`Failed`), `Snapshot`, `Close` — **~30**
- `Reader`: `Assign` (idempotent-on-`SplitID`, needs a held-splits map), `Revoke` (must return *exactly* the named
  splits with cursors at which resume is *correct*), `Fetch` (+ the fetcher goroutine, internal channel, error
  propagation and leak-free teardown that D-7 forces), `Snapshot` (must include finished-but-unacknowledged splits),
  `Close` — **~55**
- two `blob.Serializer`s for `Spec` and `Cursor` — **~5** with `blob.JSON[T]()`, ~24 hand-written

The ~60 "subtle" lines in the source are the problem, not the ~80 mechanical ones. Four of `Reader`'s five methods
carry an invariant whose violation is a silent data bug and which is enforced by prose alone:

1. `Assign` must be idempotent on `SplitID` (else duplicate assignment double-reads).
2. `Fetch` must not block (else the pipeline can never checkpoint — a deadlock, not a slowdown).
3. `Snapshot` must include splits finished but not yet acknowledged (else a completion is lost and the handoff
   watermark is wrong).
4. `Revoke` must set `Cursor` to a position at which resume is *correct*, not merely the last position read (else
   data loss on rebalance).

None is expressible in Go's type system, none is discoverable from a signature, and all four are the kind of thing an
author gets right on the first connector and wrong on the third. The conformance suite is the right answer and it is
a promise, not an interface.

**Comparative judgement:** the sink surface is the best of any plausible alternative — 50 lines, no progress
concept, and a sink genuinely cannot get checkpointing wrong. The source surface is the worst: ~140 lines minimum
against Benthos's ~30 (`Connect`/`ReadBatch`/`Close`), with 14 methods across three types before business logic and
60 lines of hand-rolled concurrency that the framework should own. §22.8 concedes the method count; it does not
concede the concurrency transfer, which is the larger cost.

---

## 4. Discoverability from godoc alone

**Good.** The `canal` façade (§3) giving a connector author one import is the right call and is rare. Doc comments
are unusually informative and state contracts rather than restating signatures. `Opening` as a struct rather than
parameters, so a new field is not a breaking change, is a good Go instinct applied deliberately. Closed enums with
`iota + 1` (so the zero value is invalid and an unset field is caught) is correct and consistent.

**Bad.** Four of the five `Reader` invariants above are prose in doc comments with no compiler or vet support, and
they are where the data loss lives. `SourceCapabilities` has 18 fields with no constructor and no "you almost
certainly want this" default, so the discoverable path is "read all 18 and guess"; the reverse-direction registration
panic then punishes a guess with a message that (per D-3) may be wrong. `RegisterSource[E, R]`'s phantom parameters
are undiscoverable — nothing in godoc explains *why* you must nominate your enumerator and reader types when the
constructor returns neither. And there are no `Example` functions anywhere in the proposal; the on-ramp is a 296 KB
design document, which is not godoc.

**The single worst discoverability defect:** nothing in the type system or the godoc tells a `PollSource` author that
they have declared `ComparablePositions: true` (D-13). The capability with the highest blast radius is invisible on
the path most authors will take.

---

## 5. What this proposal does better than any plausible alternative, on my lens

Stated plainly, because the defect list above is long and the synthesiser needs to know what is worth preserving.

1. **Growing the core through the context, never through the plugin interface** (§7.4). This is the correct and
   possibly only answer to interface evolution in a language with no default methods, and the argument from
   Connect's `NoSuchMethodError`-in-the-javadoc is airtight. No alternative angle can beat it; every alternative
   should adopt it verbatim.
2. **`WriteResult` with all four `(res, err)` combinations enumerated at the method, correlated by a stable
   `RecordID`.** R7 in the type system, and it deletes Benthos's `Indexer`/`SortGroup`/`WalkMessages` positional
   machinery outright. This is the most transplantable block in the document and it is better than what any surveyed
   framework ships.
3. **`Disposition`: values in, values out, five closed outcomes.** Replacing Flink's mutable `CommitRequest`
   callback with slices is simultaneously wire-safe, table-testable and metric-labelable, and dropping
   `signalFailedWithKnownReason` closes a documented data-loss hole. Nothing else in the study achieves per-item
   commit outcomes without a callback.
4. **`Split` as one value with `Spec` (write-once) and `Cursor` (write-many) as differently-lifetimed fields.** This
   genuinely deletes Flink CDC's parallel `SplitState` hierarchy and its `asSnapshotSplitState()` downcasts, and it
   does so with plain Go: one type, one field changes. `Watermark`-presence-means-finished (completion as data, not a
   bool) is the same instinct and is right.
5. **`PhaseOf(PlanState) Phase` as a function.** Making the reporting label a pure derivation removes the field, the
   setter and the switch in one move. No competing angle can express "snapshot vs stream" with fewer moving parts.
6. **`Buffer.Durable()` deciding where the acknowledgement boundary sits.** R4 becomes one boolean the core reads
   when wiring the tracker, rather than a paragraph in an RFC that the implementation may contradict. Elegant, and
   the abandoned attempt's `202`-after-in-memory-append is genuinely unexpressible.
7. **The registry as a value type with `Clone`/`Without`/`With` plus a cached instantiation-free `Descriptor`.** The
   testability answer for an `init()`-registry, and the reason the connector-list endpoint cannot be broken by a
   panicking constructor.
8. **Confining the enumerator to one goroutine and saying so, instead of shipping a managed threading API.** The
   *idea* is right and is strictly nicer than `callAsync`/`runInCoordinatorThread` — D-11 and D-12 are failures of
   follow-through (ownership, error path, lease-loss interaction), not of the idea. Fix those three and this is the
   best concurrency contract of the four angles.

---

## 6. Summary judgement

This reads like a strong Go engineer's design document that was written straight through without a compiler open.
The idiom choices are consistently Go-native — that question, "is this real Go or transliterated Java", answers
firmly *real Go*. The implementability question answers *not yet*: two leaf packages have a cycle, the flagship
config accessor is a redeclaration, the flagship registration check verifies nothing, the canonical record is
504 bytes with a shared map and no copy-on-write, the durable format silently drops every cursor, and the one piece
of core machinery the whole checkpoint story rests on takes a parameter that cannot be known at its call site.

None of D-1, D-2, D-4, D-6 is architecturally deep — they are the cost of not compiling. D-3 and D-5 *are*
architecturally deep: they falsify "a declared capability can never be a lie" and "the checkpoint stops being a
separate subsystem" respectively, which are two of the document's three headline claims. D-7 is the ergonomic verdict:
by refusing to write Flink's fetcher machinery in core, the design hands 40–60 lines of the most bug-prone Go there is
to every streaming connector author, and voids R6 exactly there.

A competent Go engineer would enjoy writing a sink against this and would resent writing a source. That asymmetry is
fixable and worth fixing, because the split model's actual wins (D-4 through D-8 in §5 above) do not depend on the
reader surface being this heavy.
