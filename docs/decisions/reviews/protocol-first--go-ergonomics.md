# Review: `protocol-first, in-process today` — lens: Go ergonomics and implementability

**Reviewer role:** hostile expert, single lens. I judge only whether a competent Go engineer would enjoy
writing against this API and whether it can actually be built as written. I do not judge whether the
architecture is *correct* about checkpointing, chunking or distribution — other lenses own that.

**Score: 6 / 10.**

**Verdict in one paragraph.** This is real Go, not transliterated Java. The author has clearly read
`database/sql/driver` rather than skimmed it: `Fetch(ctx, *Batch) error` is `Rows.Next(dest)`, `probe()`
is `rowsColumnInfoSetupConnLocked`, the registry is a value with a default instance, `Close` takes a
context, and the refusal of `iter.Seq` on the plugin surface is argued from the iterator package's own
"single-use iterators" note rather than from taste. Two ideas here are better than anything I can
construct as an alternative: the framework-stamped `Batch.Record(split)` slot, which makes provenance
*unforgeable by the type system* rather than by prose, and the two-method `Sink` whose `nil` return is
the durability statement. Those are worth the whole document.

But three things in it cannot be implemented as written, and all three sit in the connector author's
first hour: the goroutine model of `Reader` is self-contradictory and, on the only consistent reading,
puts an undocumented mutex in every reader's hot loop; the `Batch` slab has interior pointers into a
growable slice and no defined behaviour at overflow, which is the most-called call site in the system;
and the `Spec`/`Bind`/`SourceDescriptor` triangle does not close, so *there is no declared way for a
connector to read its own configuration*. Below that tier, the thesis' central claim — "the Go
interfaces map 1:1 onto the frames" — is false in the document's own text: `Estimate` has four
spellings, and six of the fourteen frame kinds have a second, non-frame Go spelling. That is R9's
definition of a modelling error, sitting on top of the design whose whole point was one representation
per concept.

There is also a Rust tell. The author writes, in weakness #1, "Go has no borrow checker and I am
inventing borrow discipline." That is an accurate self-diagnosis and it is the single riskiest thing in
the proposal, because slice reuse is *idiomatic* Go and the tripwire designed to catch its misuse
catches the wrong half of the bug (§D2 below).

---

## What the score is made of

| Axis the brief names | Grade | One-line reason |
|---|---|---|
| Interface size | 8 | Sink 2, Transform 2, Source 4, Reader 5. Enumerator 7 and Emitter 5 are fat; the rest is right. |
| Accept interfaces, return structs | 4 | `Host`/`Emitter`/`Buffer` accepted as interfaces — correct. But `Sink.Write` and `Transform.Apply` accept the concrete `*proto.Batch` with its full mutating API, and `Frames()` returns a writable slice called a "read-only view". |
| Generics: justified vs gratuitous | 9 | Almost none, deliberately, with FLIP-191's typed-parameter scar cited. The one place generics are *needed* (`Bind`) is where the design does not close. |
| `context` and cancellation | 8 | ctx on every blocking method from commit one, `Close(ctx)`, and `Drain` distinct from cancellation. Loses points because the lifetime of the ctx passed to `Fetch` (per-call or per-session) is never stated, and drain correctness depends on which it is. |
| Error idiom / retriable-vs-fatal | 7 | Closed 9-class taxonomy + one core disposition table + one `Classify` call per raise site is the right economy. Marred by `Fault` having pointer-receiver methods while `WriteResult.Failed` takes it by value, and by `MigrateState` returning `(Blob, bool, string)`. |
| Goroutine ownership | 3 | The worst axis. See D1 and D10. |
| Channels vs method calls per seam | 8 | Method calls on the data path, channels only inside the engine, `Emitter` as an interface rather than a channel so the gRPC binding can replace it. Correct instinct throughout. |
| Hot-path allocation and copying | 6 | The slab and the caller-owned batch are the right shape; a 120-byte 14-pointer union copied by value in the document's own `for _, f := range` loop is not. |
| Registry testability | 8 | Registry-as-value with `Clone/With/Without` is exactly right and directly answers a design rule. `Register(d any)` throws away the type safety at the one call site every connector has. |
| Boilerplate per connector | 5 | See the concrete line count below. The sink is excellent; the source is not. |
| Discoverable from godoc alone | 5 | `Kind` and `ValueKind` constants share one prefix in one package; four ways to report an `Estimate` with no stated rule; several load-bearing types (`ConfigDoc`, `Committable`, `DropReason`) referenced and never declared. |
| Is the simple case simple? | 5 | The "complete source" is 24 lines of method bodies — good — but it depends on three undeclared helpers, one undefined function, and a struct field the example forgot to declare. |

---

## Mandatory boilerplate per connector, in lines

The brief asks for a concrete number. Counting only code that exists to satisfy canal — not code that
talks to the remote system:

**Sink, trivial (stdout):** 15 lines of methods + ~12 lines of `Spec`/descriptor/`Register` = **~27
lines.** This is genuinely good and I cannot construct a smaller honest surface that still carries
per-record outcomes. Credit where due.

**Source, trivial (ticker):** the document's own example is 24 lines of method bodies. Add ~8 for the
`Spec` and ~6 for the descriptor and `Register`: **~38 lines** — *provided* `connector.SingleStream`,
`connector.OneUnboundedSplit`, `connector.OpenSchema` and the example's `enc()` all exist. Three of
those four are asserted, never designed. Strip the helpers and the trivial source is ~85 lines, because
the author must hand-write a 7-method `Enumerator` that emits one split and never completes.

**Source, realistic** (five config fields, one stream, cursor-based incremental, no chunking):

| Ceremony | Lines |
|---|---|
| `conf` struct + `Spec` builder + `Bind` block — each field named **three times** | ~45 |
| Cursor codec: `Blob{Version, Bytes}` marshal, unmarshal, version migration | ~25 |
| `Split.Attrs` codec (same problem, second blob) | ~15 |
| `Streams()` building a `Catalog`/`Schema` by hand | ~25 |
| `Enumerator`: 7 methods, of which `Snapshot`/`Confirmed`/`ReaderLost` are real work | ~60 |
| `Reader`: 5 methods incl. the `MarkNow`↔`Fetch` handshake | ~35 |
| Descriptor + `Register` + capability declaration | ~12 |
| **Total canal-facing code before one byte of the source's own protocol** | **~215** |

Of that I classify **~110–130 lines as pure ceremony** — the triple-naming of every config field, and
the hand-rolled `Blob` codecs. That second item deserves emphasis: the checkpoint design rests entirely
on connector-authored versioned serialisers (`Blob{Version, Bytes}` for cursors, split attrs, enumerator
state, committables, migrations), the research explicitly nominates Flink's forty-line
`SimpleVersionedSerializer` as the thing to copy verbatim, and **the proposal declares no serialiser
interface and no helper of any kind.** Every source therefore hand-rolls versioned encoding for four
distinct blobs. That is precisely the "every connector reimplements the same non-trivial thing, out of
tree, with its own bugs" failure the proposal correctly attacks Benthos for.

Compared to alternatives: this is more boilerplate than a struct-tag design (which the proposal rejects
for stated reasons I accept) and roughly the same as Kafka Connect's `ConfigDef` + `AbstractConfig`,
which is not a flattering comparison. The proposal admits this in weakness #6 and offers no fix.

---

## Defects

Severity: **fatal** = cannot be implemented as written, or implementing it as written produces a race
or silent corruption. **major** = a bug class or a real ergonomic tax that a competent engineer will hit.
**minor** = wart.

### D1 (fatal) — the `Reader` goroutine model is self-contradictory, and every consistent reading puts an undocumented mutex in the hot loop

The contract, verbatim (§5.1):

> `AddSplits` and `RemoveSplits` are the reader half of the assignment protocol. A reader must accept
> splits **at any time, including while a Fetch is blocked** — so a reader that buffers internally
> serialises these against its own fetch loop, and **the engine never calls them concurrently with
> Fetch.**

Those two clauses are mutually exclusive. If the engine never calls `AddSplits` concurrently with
`Fetch`, it cannot call it while a `Fetch` is blocked. The phrase "serialises these against its own
fetch loop" concedes that the author expects the concurrent case.

`MarkNow` is where this stops being a doc bug and becomes a design hole. Its documented triggers are
"when a sink requests a boundary, when a size or time policy fires, **or on graceful drain**". All three
are asynchronous with respect to a `Fetch` that is documented as *expected to block until data*. So:

- If `MarkNow` is called on the fetch goroutine (i.e. only between `Fetch` calls), then **drain cannot
  complete while `Fetch` is blocked**. On an idle CDC stream that is unbounded. `Pipeline.Drain(ctx)` —
  the feature the proposal correctly identifies as KIP-419's seven-year hole — cannot work.
- If `MarkNow` is called on another goroutine, then it races the reader's own state. It cannot write to
  the `Batch` (there is no `Batch` parameter, and `Batch` is documented single-goroutine), so the only
  possible implementation is *set a pending-mark flag that `Fetch` reads*. That is shared mutable state
  across two goroutines in the hot path of **every reader ever written**, and the requirement for a
  mutex or atomic is stated nowhere.

The same ambiguity infects `RemoveSplits` (revocation during a blocked long poll — does the reader keep
emitting records for a split whose lease is gone?) and `Close` (is `Close` allowed while `Fetch` is
in flight? `io.Closer` conventions say nothing, `database/sql/driver` says `Rows.Close` may be called
concurrently with nothing).

Weakness #7 admits `Fetch`'s *blocking duration* is underspecified. It does not admit that the *thread*
is. That is the more serious of the two: a duration convention plus an engine-side spin detector is a
tolerable mitigation; an unstated goroutine boundary is a race in every connector, and `-race` finds it
only if the connector's own test suite happens to exercise a drain concurrent with an idle poll.

**Consequence.** Connector #1 is written with a guess. Half the ecosystem guesses "single goroutine" and
their pipelines will not drain; the other half guesses "concurrent" and adds a mutex nobody reviewed.
Both halves compile and both pass a happy-path test.

**Fix cost: low, and it must be paid before the interface freezes.** State it in the interface doc:
`Fetch`, `AddSplits`, `RemoveSplits` and `Close` are called from one goroutine and never concurrently;
`MarkNow` is called from another goroutine and implementations must be safe for concurrent use with
`Fetch`; the engine passes `Fetch` a ctx with an engine-chosen deadline and an empty batch with a `nil`
error is legal and expected. Then provide `connector.MarkGate` in the SDK — a ten-line
mutex-plus-flag helper — so no author writes the synchronisation twice. That last part is what turns
this from a documented hazard into a non-issue.

### D2 (fatal) — the `Batch` slab has interior pointers into a growable slice, and `Record()` has no failure mode

```go
type Batch struct {
	frames  []Frame        // reused across Fetch calls; len is reset, cap is kept
	records []Record       // slab; Frame.Record points into it
	maxRecs int
	...
}
func (b *Batch) Record(split SplitRef) *Record
func (b *Batch) Full() bool
```

Two distinct problems in four lines.

**(a) Interior pointers into a slice that may grow.** `Frame.Record` is a `*Record` into `b.records`. If
`b.records` is ever `append`ed past its capacity, Go copies the elements to a new array and every
previously handed-out `*Record` — including the ones already stored in `b.frames[i].Record` — points into
the abandoned array. Reads still return the right *values* (the old array is copied from, not zeroed), so
nothing crashes and nothing looks wrong. But the batch is now split across two arrays: frames 0..k point
into the dead one, frames k+1..n into the live one. `Reset()`, documented as "returns slots to the slab",
touches only the live one. A transform or codec that mutates `frames[i].Record.Value` for `i <= k`
mutates a record nobody else will read; a `canaldebug` poison pass poisons the wrong array. This is
invisible to `-race` because it is all one goroutine, and invisible to `go vet` entirely.

The intended design is obviously "preallocate `cap(records) == maxRecs` and never grow". That is a
one-clause fix and it is not written down anywhere — and it interacts with (b).

**(b) The most-called method in the system has undefined behaviour at its boundary.** `Full()` is
advisory: "a reader *should* return from `Fetch` when `Full` is true; the engine also enforces it."
`Record()` returns `*Record` with no second return value and no documented panic. So when a reader calls
`Record()` on a full batch — which it will, because `Full()` is a `should` and because the natural loop
is `for { r := b.Record(ref); if b.Full() { break } }` — the API has exactly three options and specifies
none:

1. grow the slab → problem (a), silently;
2. return `nil` → nil dereference on the next line (`r.Value.SetStructured(...)`), inside connector
   code, blamed on the connector;
3. panic → the only safe choice, and it means a connector that miscounts by one crashes the worker.

And "the engine also enforces it" is itself unspecified: if the engine discards frames past `maxRecs`
after `Fetch` returns, records are silently dropped *after* `Batch.Record` stamped them with an in-flight
id the resolver may already be tracking. Compare `database/sql/driver`'s `Rows.Next(dest []Value)`,
which the proposal explicitly models on: `dest` has a fixed length, the driver physically cannot
overflow it, and there is no boundary condition to specify.

**(c) The ownership tripwire catches the wrong half of the bug.** The stated rule is "ownership of every
byte slice transfers to the receiver; the sender must not read, mutate or retain them afterwards", with
`canaldebug` poisoning the sender's slices with `0xDE` on transfer. That catches *sender reads after
transfer*. It does not catch the far more common Go bug, which is the sender transferring a slice **it
never owned**:

```go
for s.scanner.Scan() {
	r := b.Record(ref)
	r.Value.SetBytes(s.scanner.Bytes())   // the obvious line. also data corruption.
}
```

`bufio.Scanner.Bytes()` documents that its slice is invalidated by the next `Scan`. The connector is not
reading after transfer — it never touches the slice again — so poisoning sees nothing wrong. The engine
then reads a buffer the scanner has overwritten. Same for `Reader.Read` into a reused buffer, `sql.Rows`
scanning into `[]byte`, and `bytes.Buffer.Bytes()`. Every one of those is idiomatic Go and every one is a
corruption bug here, and the *only* declared mitigation is a debug mode aimed at a different failure.

Weakness #1 says a safer design "would copy on admission and cost one allocation per record". That is the
right trade for v1 and it is what I would take: `SetBytes` copies into the slab's own arena, and
`SetBytesNoCopy` is the opt-in fast path with the borrow rule, so the dangerous behaviour requires typing
the word `NoCopy`. That single naming change moves the hazard from the default into the exception, and it
is exactly what the stdlib does with `unsafe.String`/`strings.Clone`.

### D3 (fatal) — `Spec` / `Bind` / `SourceDescriptor` does not close, so a connector cannot read its config

This is the deliverable's primary success criterion ("implement the interface, register it, done") and
the "register it" step does not compile. Four independent gaps, all in §6.1 and §7:

1. **`Bind` has no signature.** It appears once, as a chained call:
   `Bind(func(b *connector.Binder, c *conf) { ... })`. The package layout lists it as a builder in
   `connector/spec.go`. A non-generic builder method cannot accept `func(*Binder, *conf)` — it would have
   to accept `func(*Binder, any)` and reflect at registration, which is the reflection §6 rejects two
   pages earlier. A generic builder (`NewSpec[conf]()`) would work, but the example writes
   `connector.NewSpec()` with no type argument and Go does not infer type parameters backwards through a
   method chain. **The example as written cannot compile under either mechanism**, and both mechanisms are
   rejected elsewhere in the same document — reflection in §6, and type parameters on the registration
   surface by the research note "never put a type parameter on an interface a future feature might need
   to vary".
2. **The descriptor has nowhere to put the binding.** `SourceDescriptor` is exactly two fields:
   `proto.SourceInfo` (data, containing `Spec`) and `New func(ctx, OpenRequest) (Source, error)`. The
   claim "Registration cross-checks the binding against the spec in both directions ... or Register panics
   at init" requires `Register` to *see* the binding. It cannot.
3. **`proto.Spec` cannot reference `connector.Binder`.** The layout puts `Spec` in `proto/spec.go` and
   `Binder` in `connector/registry.go`/`spec.go`, with `connector` importing `proto` and `proto`
   importing nothing. So the fluent chain in the example — `.Field(...).Include(...).Test(...).Bind(...)`
   all on one value — is an import cycle if that value is `proto.Spec`, and requires a second
   `connector`-side type plus a hand-off if it is not. Resolvable, but unresolved, and it changes what
   `SourceInfo.Spec` is.
4. **Five of thirteen `FieldType`s have no `Binder` method.** Declared: `String`, `Int(*int64)`,
   `Duration`, `Secret`, `FieldPaths`, `Component`, `OneOf`, `Nested`. Missing: `Bool`, `Float`, `Size`,
   `Enum`, `Array`, `StreamRef`, and plain `int`. Combined with "every spec field must be bound ... or
   `Register` panics", **a connector with a single `bool` config field cannot be registered.**

On top of that, `proto.ConfigDoc` — the type in `OpenRequest.Config`, the thing every connector receives —
is referenced three times and never declared. So even if `Bind` worked, the shape of what it binds *from*
is unknown.

**Consequence.** The one thing the brief says is the immediate deliverable — the interface set — is
missing its most-used component. This is not a nitpick about an omitted helper; it is the config path, and
the design's own answer to "no accessor ladder, no reflection magic" is the part that is neither.

### D4 (major) — the frames and the Go surface are not 1:1, which is the thesis' own central claim

§5 opens: "idiomatic Go interfaces that map 1:1 onto the frames above". They do not. Counting from the
document:

| Concept | Frame | Second Go spelling | Third | Fourth |
|---|---|---|---|---|
| Estimate | `KindEstimate` / `Frame.Estimate` | `Emitter.Estimate(ctx, e, scope)` | `Host.Estimate(ctx, e, scope)` | `Split.Estimate`, `Stream.Estimate` |
| Head | `KindHead` / `Frame.Head` | `Headwatcher.Head(ctx, s) (Cursor, time.Time, error)` | | |
| Outcome | `KindOutcome` | `connector.WriteResult.Failed/Duplicate` | | |
| Committable | `KindCommittable` | `WriteResult.Committable(c)` | | |
| MarkRequest | `KindMarkRequest` | `WriteResult.RequestMark()` | | |
| Log | `KindLog` | `Host.Log() *slog.Logger` | | |
| MetricSample | `KindMetricSample` | `Host.Counter/Gauge` | | |
| StreamStatus | `KindStreamStatus` | `Host.StreamStatus(...)` | | |
| ConfigPatch | `KindConfigPatch` | `Host.PatchConfig(...)` | | |
| Fault | `KindFault` | `Emitter.Fault(ctx, f)` | returned `error` | |

Nine of fourteen kinds have a non-frame Go spelling, and `Estimate` has four with **no stated rule for
which to use**. A connector author who wants to report a row-count estimate can reach for
`Host.Estimate`, `Emitter.Estimate`, `Split.Estimate` or `Stream.Estimate`, and the document never says
which is authoritative or how they reconcile if two disagree.

I accept that a *transport* asymmetry is fine — `Host.Counter` returning a `Counter` handle is far nicer
Go than making the connector build `MetricSample` frames, and the Log/metric cases are defensible sugar.
But this is the design rule the proposal cites most often against others (R9: "a function mapping between
two representations of the same concept is evidence of a modelling error"), and the thesis' one sentence
of load-bearing argument is that there is a single closed union with bindings over it. As written, the
gRPC binding must collapse four `Estimate` paths into one frame and decide which wins — the exact
translation layer the protocol-first bet exists to avoid.

**Fix:** state the rule explicitly. Suggested: the *data plane* (Record/Mark/SchemaChange) is frames-only;
the *uplink* (Log/Metric/StreamStatus/Estimate/ConfigPatch/Fault) is `Host` methods only, and the frame
kinds for those exist solely as the wire encoding of `Host`, never as something a connector appends to a
`Batch`; `Emitter` loses `Estimate` and `Fault`. `Split.Estimate` and `Stream.Estimate` are planning data,
not reports, and should be renamed (`SizeHint`) so they are not four spellings of one word.

### D5 (major) — `Origin.Parent` has no writer, so the ack-aggregation mechanism cannot be populated

`Origin.Parent RecordID` is "non-zero if produced by an unframer from a larger frame", and §9 plus the
decisions table make it load-bearing: "The N records produced from one inbound frame share
`Origin.Parent`, so their acks aggregate to one upstream ack automatically: Benthos's ack-aggregating
scanner reduction, **made structural by the parent id** rather than by positional correlation."

The declared `Batch` API has exactly two record constructors:

- `Record(split SplitRef) *Record` — stamps a fresh `Origin`. No parent parameter.
- `Derive(in *Record) *Record` — "copying `in`'s `Origin` **verbatim**". Verbatim copy means the new
  record inherits `in.Origin.Parent`, i.e. the *grandparent*; it does not set `Parent = in.ID()`.

So nothing can set `Parent`, and `Unframer.Scan(ctx, src, b, split)` — which is the one caller that needs
it — has only `Record(split)` available. The feature that replaces Benthos's 45-line
`AutoAggregateBatchScannerAcks` reduction is unimplementable against the declared surface.

The fix is a third constructor (`b.Child(parent *Record) *Record`), which is trivial. What it exposes is
that `Derive`'s single documented behaviour is being asked to serve two different relationships —
*transform-of* (provenance identical, parent unchanged) and *unframed-from* (provenance identical, parent
set) — and the document only noticed one.

### D6 (major) — `TokenSink` is unimplementable against the declared `Mark`

```go
type TokenSink interface {
	LoadToken(ctx context.Context) (proto.Blob, error)
	// The token to store with the next Write's transaction is passed via
	// WriteResult's inbound side: proto.Batch carries it on the Mark frame.
}
```

`Mark` (§3) has `ID, Split, Cursor, Records, Bytes, Phase, At`. There is no token field. The write half of
the strongest delivery tier is a **comment where a method should be**, pointing at a field that does not
exist, on a frame the sink is elsewhere told is "informational". A sink author implementing `CapToken`
has nothing to implement.

Separately: a one-method interface named for the *other* direction is bad godoc. `LoadToken` alone reads
as "a sink that can be asked for a token", which is not what the capability means.

### D7 (major) — `connector.WriteResult` is opaque, cross-package, and duplicates three frame kinds

`type WriteResult struct{ /* per-record entries, keyed by RecordID */ }` lives in `connector`, is filled
by the sink through six mutating methods, and must be *read* by `engine/writerloop.go`. With unexported
fields and no declared accessor, `engine` cannot read it. There are three ways out and all cost
something:

1. Add exported readers → the sink can also read and mutate its own result, and can read another record's
   verdict, which is state it has no business seeing.
2. Move it to `proto` → but it holds `proto.Fault` and `proto.Committable`, and per the thesis it should
   then simply *be* frames, which brings us to (3).
3. Make it the `KindOutcome`/`KindCommittable`/`KindMarkRequest` frames it duplicates → which is what the
   protocol says already exists.

Option 3 is clearly right and the document did not take it, so the design carries two representations of
per-record write outcomes: the frame set that crosses a wire and the Go struct that a sink actually fills.
Every alternative binding must translate between them, and §13's boast of "four write outcomes, not
Flink's six" is stated about the struct while the frame set has its own vocabulary.

The out-param-builder *shape* is fine and idiomatic (it is `http.ResponseWriter`'s shape, and "silence
means success, so the common path allocates nothing" is a genuinely good call). It is the ownership and
the duplication that are wrong.

### D8 (major) — `Sink.Write` and `Transform.Apply` accept the concrete mutable `*Batch`; "accept interfaces" is inverted at the most important seam

`Write(ctx context.Context, b *proto.Batch, res *WriteResult) error` hands the sink the engine's batch
with its entire API attached: `Record()`, `Derive()`, `Mark()`, `Schema()`, `Emit()`, `Drop()` and
`Reset()` — the last of which is documented "engine-only" **by comment**. A sink that calls `b.Reset()`
destroys the engine's in-flight accounting mid-write. The compiler is fine with it.

Three consequences a Go engineer will feel:

- **Every sink hand-writes kind filtering.** The document's own example opens with
  `for _, f := range b.Frames() { if f.Kind != proto.KindRecord { continue } ... }`. The engine already
  filters `SchemaChange` per-sink (§5.2) — so the machinery to filter exists and is not used for Marks.
  One core-provided `b.Records()` iterator removes this from every sink in the ecosystem. Note the irony:
  §13.8 rejects `iter.Seq` on the plugin surface with five correct reasons, and this is the one place in
  the design where `iter.Seq[*Record]` is exactly right and is not used.
- **`Frames() []Frame` is called a "read-only view" and is not one.** A slice is writable. `frames[3].Record = nil`
  compiles. Either return an iterator or return a `RecordCursor` interface — this is the definition of
  "accept interfaces, return structs" applied at the seam that matters most.
- **120 bytes copied per iteration.** `Frame` is `Kind` plus fourteen pointers, thirteen of which are
  always nil. `for _, f := range b.Frames()` copies the whole struct each step; the document teaches that
  form in both sink walkthroughs. At the six-figures-per-second the `SplitRef` interning was justified by,
  that is ~12 MB/s of pure struct copy — larger than the string-vs-`SplitRef` saving the design paid two
  fields for. `for i := range` plus `&frames[i]`, or an iterator, costs nothing.

The `Transform.Apply(ctx, in, out *proto.Batch) error` seam has the same problem plus one more: nothing
says whether `out` may alias `in`, and a chain of N transforms with distinct out-batches multiplies the
slab N times, which quietly retires the "zero-alloc" claim as chains grow.

### D9 (major) — `Payload`'s accessors are unusable from a sink, and the document's own example calls a method that does not exist

```go
func (p *Payload) Bytes(ctx context.Context, enc Serializer) ([]byte, error)
func (p *Payload) Structured(ctx context.Context, dec Deserializer) (any, error)
```

A sink is never given a `Serializer` — §9 and walkthrough (a) step 9 say the *engine* applies the codec
before `Write`. So a sink cannot call `Bytes`. Confronted with this while writing the example, the author
wrote `f.Record.Value.Raw()` — a method declared nowhere in the document. That is the clearest possible
evidence that the declared `Payload` API is not the one the design needs: it needs a plain
`Bytes() []byte` for post-encode consumers, and the codec-taking form belongs on the engine's side of the
fence, not on the record.

Two further ergonomic costs:

- `Structured(...) (any, error)` returns `any`, so every transform and every `CapStructuredInput` sink
  writes `v, err := p.Structured(...); m, ok := v.(map[string]proto.Value); if !ok { ... }` per record.
  `Value any` with a closed documented set is defensible (`driver.Value` precedent, correctly cited), but
  stacking `Structured() any` on top of it means two layers of dynamic typing and a type assertion in
  every consumer's hot loop, in a document that had generics available and rejected them everywhere.
- `Payload` holds unexported `bytes`/`structured`/`shared` and is embedded **by value** in `Record` (twice)
  and by pointer in `Change` (twice). `r.Value = other.Value` compiles and copies the `shared` flag into a
  new owner, silently defeating the copy-on-write. There is no `noCopy` marker, no `Clone()`, and no
  statement that `Record` must not be copied — on the type the whole system is built around.

### D10 (major) — enumerator goroutine shutdown is unspecified, and the sanctioned pattern leaks

The `Enumerator` doc sanctions concurrency: "An enumerator that wants concurrency spawns its own and emits
from it; `Emitter` is safe for concurrent use." §12(e) says `Emitter` is "a bounded channel into
`engine/enumloop.go`". Now:

- **Who stops the connector's goroutine?** `Enumerator.Close(ctx)`, presumably. Nothing says `Close` is
  called after the engine stops draining the Emitter, and nothing says what `Emitter.Send` does once the
  enumerator loop is gone. If `Send` blocks on a full bounded channel that no longer has a reader, the
  connector's goroutine is wedged and `Close` — if it waits for it — deadlocks. If `Send` returns an
  error, that must be documented (a sentinel: `ErrEmitterClosed`) and it is not.
- **`Snapshot` races the sanctioned goroutine.** `Snapshot(ctx, m) (Blob, error)` is documented "It must
  be a pure read: the engine may call it while readers are mid-fetch." If enumerator methods are all
  called from one engine goroutine, that is satisfiable. But the connector's *own* discovery goroutine —
  the one the doc just sanctioned — mutates the state `Snapshot` serialises. So the officially blessed
  concurrency pattern races the checkpoint path, and no synchronisation guidance is given.

This is the "who starts, who stops, who closes channels" question the brief asks about, and the answer
here is "the connector starts, and nobody says who stops". Compare Flink, which the proposal cites
approvingly: Flink gives the enumerator a *managed single-threaded executor* (`callAsync`,
`runInCoordinatorThread`) precisely so this cannot happen, and the research notes Flink CDC *still* shipped
two `ConcurrentModificationException` fixes. The decision space's recommendation was explicit — "the
enumerator is a goroutine owning its state, driven by one `select` over a command channel ... No mutexes,
no prose warning about shared state" — and this proposal instead ships the prose warning.

**Fix:** offer `connector.EnumeratorLoop`, a helper that owns the goroutine and turns async discovery into
a command on the engine's own single-threaded queue, so the connector never spawns anything. That is both
nicer Go and closer to what the research recommended.

### D11 (major) — three identical `Close(ctx) error` methods, and the canonical example double-closes

`Source.Close`, `Enumerator.Close` and `Reader.Close` are all `Close(ctx context.Context) error`. The
document's own "complete source" returns itself from `Reader()`:

```go
func (t *ticker) Reader(ctx context.Context, cc proto.ConfiguredCatalog) (connector.Reader, error) {
	return t, nil
}
func (t *ticker) Close(ctx context.Context) error { return nil }
```

`ticker`'s single `Close` now serves as both the Source's and the Reader's. `Source.Close` is documented
"always called exactly once"; `Reader.Close` is presumably the same; the object gets two calls. For a
ticker returning `nil` that is harmless, and it is the most-copied example in the docs — so the pattern
propagates to a source holding a `*pgx.Conn`, where the second `Close` is a double-close, a
`sync.WaitGroup` reuse panic, or a send on a closed channel. Idempotence is never required and the
composition is never warned against.

A second, quieter cost: `Source` declares a **method** named `Reader` while the package declares a
**type** named `Reader`. That forecloses the most natural Go composition — `type pg struct { connector.Reader }`
gives a promoted field named `Reader`, which collides with the required method `Reader()`. So a source can
never embed a reader implementation. Renaming the methods (`NewReader`/`NewEnumerator`, or
`OpenReader`/`Plan`) costs nothing and recovers embedding.

### D12 (minor) — `Register(d any)` throws away type safety at the one call site every connector has

```go
func Register(d any)
```

The typed API exists two lines above (`Registry.WithSource(SourceDescriptor)`), so this is a deliberate
choice to accept `any` at the *only* function a connector author calls at init. Registering a `Source`
instead of a `SourceDescriptor`, or a `SinkDescriptor` where a source is meant, compiles and panics.
`Register` already has four documented panic conditions (duplicate name, capability mismatch, spec/binding
mismatch, protocol mismatch), all at `init()`, which means a *linked but unused* broken connector crashes
`canal --version`. There is also no `Register`/`MustRegister` split, which the research nominates as the
pattern to copy from Benthos, and no `RegisterSource`/`RegisterSink` pair. `func Register[T Descriptor](d T)`
or two typed functions would cost nothing and is the one place a generic genuinely helps.

### D13 (minor) — no versioned-serialiser helper exists, so every connector hand-rolls four blob codecs

Covered in the boilerplate section; recording it as a defect because it is a declared-interface omission,
not a taste issue. `Cursor`, `Split.Attrs`, enumerator state, `Committable` and `StateMigrator`'s input
are all `Blob{Version uint32; Bytes []byte}` authored by the connector. The research names
`SimpleVersionedSerializer` — "forty lines" — as the thing to take verbatim. The proposal takes the
*shape* and not the *interface*, so version dispatch and migration are re-invented per connector per blob.
The example's `enc(t.n)` is undefined, which is the tell.

### D14 (minor) — a cluster of idiom warts, each small, together a discoverability tax

- **`Fault` value vs pointer.** `func (f *Fault) Error() string` and `Is(target error) bool` are on the
  pointer, so `proto.Fault` (value) does **not** implement `error`. `Classify` returns `*Fault`.
  `WriteResult.Failed(id, f proto.Fault)` takes a value — hence the example's `*proto.Classify(...)`,
  which allocates a `Fault` on the heap purely to copy it into the result, per failed record, while the
  `Cause *Fault` chain stays shared. Pick one: `Failed(id, *Fault)`.
- **`MigrateState(ctx, b, from) (Blob, bool, string)`** is the "one bool and one string" return the same
  document condemns in Airbyte's `check` (§6, `Diagnostic`). It should be `(Blob, error)` with a typed
  `ErrIncompatibleState{Reason}`, which is also what makes the reason renderable in the UI the design
  cares about.
- **`Kind` and `ValueKind` share one constant prefix in one package.** `proto.KindRecord`/`KindMark`/`KindLog`
  are frame kinds; `proto.KindString`/`KindBytes`/`KindJSON` are value kinds. In godoc's constant list and
  in autocomplete on `proto.Kind…` they are indistinguishable, and `Frame.Kind Kind`,
  `SchemaChange.Kind SchemaChangeKind`, `FieldChange.Kind FieldChangeKind`, `Validator.Kind ValidatorKind`,
  `Predicate.Kind PredicateKind`, `Directive.Kind DirectiveKind` make `Kind` the most overloaded word in
  the package. `FrameKind`/`ValKind…` prefixes fix it for free before anything freezes.
- **`probe`'s redundant booleans.** `if v, ok := src.(connector.Chunker); ok { c.chunker, c.hasChunker = v, true }`
  — `c.chunker != nil` already carries that. In the one function showcased as exemplary Go, it is noise;
  and the function is documented as panicking on a mismatch that `Register` already guaranteed, in the
  build path.
- **Undeclared types referenced as if declared:** `DropReason`, `ConfigDoc`, `Committable`, `Batching`,
  `MarkPolicy`, `BufferPlan`, `Choice`, `TestSpec`, `CommitOutcome`, `Unit`, `StreamPhase`, `Group`,
  `Variant`. Acceptable in a proposal for most, but `ConfigDoc` and `Committable` are load-bearing for the
  config path and the exactly-once tier respectively.
- **`Emitter` is five methods where one is load-bearing.** `Send(ctx, Frame)` plus four wrappers over it.
  Every fake, every mock and every binding implements five. In Go those four are free functions taking an
  `Emitter` — the `io.Copy`-is-not-a-method rule. Same argument applies to `Host` at seven methods, though
  there the engine is the only implementer so the cost lands on test fakes rather than on bindings.

---

## What this proposal does better than any plausible alternative

I am obliged to be specific here, and there is real material.

1. **`Batch.Record(split) *Record` — provenance made unforgeable by the compiler.** `Record.id` and
   `Record.origin` are unexported and the only constructor is in `proto`, so a connector *cannot* mint a
   record with an `Origin` of its choosing and a transform *cannot* rewrite one. Kafka Connect needed
   KIP-793 (`originalTopic`/`originalKafkaPartition`/`originalKafkaOffset`) to retrofit this, and Connect's
   version is still two javadocs and a convention. No alternative I can construct — a `NewRecord()`
   constructor, an interface, a wrapper type — gets the same guarantee without either an allocation per
   record or a mutable `Origin`. This is the single best idea in the document and it should survive whatever
   else does not.
2. **`Sink` is two methods and its `nil` return *is* the durability claim.** A base-tier sink has zero
   progress awareness and therefore cannot get checkpointing wrong; there is no second way to say
   "accepted", so R4 becomes a property of the type rather than of prose in an RFC. Everything harder
   (writer state, 2PC, transactional token) is a separate optional interface. This is the correct Go
   answer and it is strictly better than Connect's `flush()`/`preCommit()` pair where returning `{}`
   silently means "I own offsets now".
3. **`WriteResult`'s silence-means-success.** The common path allocates nothing and writes nothing, and the
   exceptional path names records individually. It solves R7 (per-record failures) without taxing the happy
   path — which a `[]Outcome` return would.
4. **One `probe()` function holding every type assertion, materialising a plain struct.** Correctly
   attributed to `database/sql`'s `rowsColumnInfoSetupConnLocked`, and it kills the nine-hand-written-
   forwarders tax the research measured in Benthos. Adding a capability touches one file.
5. **Capability-as-data cross-checked against capability-as-interface, panicking at `Register`.** This is
   the only construction that keeps required interfaces tiny *and* lets the declaration cross a wire, and
   init-time panic is the right moment in Go — the alternative (nil-checks at use) is what Connect's
   `catch (NoSuchMethodError)` javadoc instructs authors to write.
6. **`Registry` as a value with `Clone`/`With`/`Without` plus a default instance.** Tests get an isolated
   registry, sandboxes get an allow-list, and no test has to work around module-scope global state.
7. **`Buffer.Push(ctx, *Batch) (accepted int, err error)` plus `WhenFull` in the type.** Rejection and
   partial acceptance are in the signature; unbounded growth is not expressible. Five methods, no prose.
8. **`Pipeline.Drain(ctx)` as a distinct operation from ctx cancellation, and the reasoning for why an
   in-process design has to build what a process boundary gives free.** Most Go designs conflate these and
   discover the difference during their first production SIGTERM.
9. **Refusing `iter.Seq` on the plugin surface, with five reasons drawn from the iterator package's own
   documentation, while keeping it as a `testkit` adapter.** Exactly the relationship `sql.Rows` has to
   `range`. This is the sort of judgement that separates a Go design from a Go-flavoured one.
10. **`*float64` / `*time.Time` / `nil`-means-unknown throughout the read model.** It looks unidiomatic and
    it is right: the alternative renders "lag: 0s" for a source that cannot measure lag.
11. **The import analyser in `tools/importcheck` enforcing that `connectors/*` cannot compile against
    `engine/`, and `api/` cannot import `engine/`.** A mechanism, not a rule — and in Go, package-graph
    enforcement is the only extensibility guarantee that cannot rot.

---

## What I would require before this interface set is frozen

In priority order, and all of it is cheap:

1. Write the `Reader` goroutine contract into the interface doc, and ship `connector.MarkGate` so the
   synchronisation is written once (D1).
2. Preallocate the slab, make `Record()` panic when full, and rename the borrow-semantics setter to
   `SetBytesNoCopy` with `SetBytes` copying (D2).
3. Close the `Spec`/`Bind`/descriptor triangle: pick generics-at-the-builder or a `Bind func(*Binder, any)`
   field on the descriptor, declare `ConfigDoc`, and complete the `Binder` method set to cover all
   thirteen `FieldType`s (D3).
4. State the frame-vs-method rule and delete the duplicate spellings, especially `Estimate`'s four (D4).
5. Add `Batch.Child(parent)`; give the sink a `Records()` iterator and stop handing it `*Batch` (D5, D8).
6. Add a `VersionedCodec` helper for `Blob` so cursor encoding is written once, not once per connector
   per blob (D13).

None of these is an architectural change. That is the real content of the 6: the *architecture* here is
mostly excellent Go, and the *specification* has three holes in the exact places a connector author
touches first.
