# Prior art: ConduitIO / Conduit

> **Status: analysis, verified against primary source.** Every signature, field name, metric name and
> constant below was read from the Go source at the pinned commits in the table. Where I did *not*
> read source, the text says so explicitly and the claim is repeated in the `unverified` list of the
> research receipt. Per `docs/design-rules.md` R12 this document is **draft** (descriptive prior art,
> not normative for canal).

## Pinned refs

All fetched 2026-07-30 via `raw.githubusercontent.com` at these exact commits.

| Repo | Commit | Committed | Notes |
| --- | --- | --- | --- |
| `ConduitIO/conduit` | `f201df1320c1982b1fc1984ca6552fc5b67de172` | 2026-07-30 | engine; nearest tag `v0.20.0-nightly.20260730`, last stable `v0.19.0` |
| `ConduitIO/conduit-connector-sdk` | `2722eab99281241eff321635ae10dc41ac28f789` | 2026-07-23 | `Source` / `Destination` |
| `ConduitIO/conduit-commons` | `ffd8adad00e604b8605a24d5be351122027dc2ce` | 2026-07-23 | `opencdc`, `config`, `schema`, `database` |
| `ConduitIO/conduit-connector-protocol` | `095d190c37363539e6c94f06d06cf7bde5ed090c` | 2026-07-28 | `pconnector` — the plugin boundary |
| `ConduitIO/conduit-connector-postgres` | `6fb4dd01397bd1cf970fe374cdfd1025d59c40a2` | 2026-07-26 | reference snapshot→CDC source |
| `ConduitIO/conduit-processor-sdk` | `8376e49ad512454ba5c8770d4f67f29a3d241c5e` | 2026-07-27 | `Processor` |

Two era-warnings for anyone re-reading this later:

1. The record model **has** moved. It now lives in `conduit-commons/opencdc`, not in the engine repo.
   Any older analysis that puts `Record` in `conduit/pkg/record` is describing a dead layout.
2. HEAD is well past the last widely-cited release. This tree carries a `CLAUDE.md`, an
   `docs/architecture-decision-records/` set dated 2026-07, `docs/postmortems/`, and a `tests/chaos`
   suite. Several of the most interesting findings below (the ack-before-persist sev-0, the
   deferred-ack delivery goroutine, the partition-claims RFC) are *recent* and are **not** in
   `v0.14`-era Conduit. Read the SHA, not the project name.
3. There are **two live pipeline engines in the tree**: `pkg/lifecycle` (v1, default, record-at-a-time
   node graph) and `pkg/lifecycle-poc` (v2, opt-in `-preview.pipeline-arch-v2`, batch "funnel").
   Everything in this dossier says which one it describes.

---

## 1. Core interfaces

There are **three** interface layers, and the distinction is the single most valuable thing in this
dossier for canal:

| Layer | Package | Who implements | Shape |
| --- | --- | --- | --- |
| **Author-facing** | `conduit-connector-sdk` | connector authors | `Source` / `Destination` — plain method calls, per-record/per-batch |
| **Boundary** | `conduit-connector-protocol/pconnector` | the SDK adapter (in-proc) *and* the gRPC client (out-of-proc) | `SourcePlugin` / `DestinationPlugin` — request/response structs + a bidi stream |
| **Engine-facing** | `conduit/pkg/plugin/connector` | builtin adapter and standalone adapter | `Dispenser` + `SourcePlugin` + `NewStream()` |

The author never sees the boundary layer. The engine never sees the author layer. That indirection is
what lets in-process and subprocess connectors be interchangeable (§10).

### 1.1 Author-facing `Source`

`conduit-connector-sdk/source.go`:

```go
// Source fetches records from 3rd party resources and sends them to Conduit.
// All implementations must embed UnimplementedSource for forward compatibility.
type Source interface {
	// Config returns the configuration that the source expects. It should return
	// a pointer to a struct that contains all the configuration keys that the
	// source expects. [...]
	Config() SourceConfig

	// Open is called after Configure to signal the plugin it can prepare to
	// start producing records. [...] The position parameter will contain the
	// position of the last record that was successfully processed, Source should
	// therefore start producing records after this position. The context passed
	// to Open will be cancelled once the plugin receives a stop signal from
	// Conduit.
	Open(context.Context, opencdc.Position) error

	// Read returns a new Record and is supposed to block until there is either
	// a new record or the context gets cancelled. It can also return the error
	// ErrBackoffRetry to signal to the SDK it should call Read again with a
	// backoff retry. [...]
	// Read can be called concurrently with Ack.
	Read(context.Context) (opencdc.Record, error)

	// ReadN is the same as Read, but returns a batch of records. The connector
	// is expected to return at most n records. [...]
	ReadN(context.Context, int) ([]opencdc.Record, error)

	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled [...]
	// Ack can be called concurrently with Read.
	Ack(context.Context, opencdc.Position) error

	// Teardown signals to the plugin that there will be no more calls to any
	// other function. [...]
	Teardown(context.Context) error

	// -- Lifecycle events -----------------------------------------------------

	LifecycleOnCreated(ctx context.Context, config config.Config) error
	LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error
	LifecycleOnDeleted(ctx context.Context, config config.Config) error

	mustEmbedUnimplementedSource()
}

// SourceConfig represents the configuration containing all configuration keys
// that a source expects.
type SourceConfig interface {
	Validatable

	mustEmbedUnimplementedSourceConfig()
}
```

Notes that matter:

- **`mustEmbedUnimplementedSource()` is unexported and unimplementable outside the package.** It is
  a compile-time forcing function: you cannot satisfy `Source` without embedding
  `sdk.UnimplementedSource`. That embed is what makes the interface **additive-safe** — Conduit can
  add a 10th method and every existing connector still compiles, inheriting the default from
  `UnimplementedSource`. This is the gRPC-generated-service trick applied to a hand-written interface.
- **There is no `Configure` method.** It was replaced by `Config() SourceConfig` returning a *pointer
  to a struct*; the SDK does the parsing (`Util.ParseConfig`) and populates the struct by reflection.
  The author writes zero parsing code.
- `Read` and `ReadN` **both** exist, and the SDK picks at runtime (§8.1). This is a live migration
  scar, not a design.
- `Ack` takes a single `Position`, but the engine acks in batches — the SDK loops
  (`sourcePluginAdapter.runAck`).

The **smallest legal source** is: `Config`, `Open`, and one of `Read`/`ReadN`, plus `Teardown`
(everything else defaults). `Ack` is explicitly optional:

```go
// Ack should be overridden if acks need to be forwarded to the source,
// otherwise it is optional.
func (UnimplementedSource) Ack(context.Context, opencdc.Position) error {
	return fmt.Errorf("action \"Ack\": %w", ErrUnimplemented)
}
```

`ErrUnimplemented` from an optional method is swallowed by the adapter. `Config()` is the one method
whose default **panics** rather than erroring:

```go
func (UnimplementedSource) Config() SourceConfig { panic("it is required to implement Config") }
```

### 1.2 Author-facing `Destination`

`conduit-connector-sdk/destination.go`:

```go
type Destination interface {
	Config() DestinationConfig

	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	Open(context.Context) error

	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	Write(ctx context.Context, r []opencdc.Record) (n int, err error)

	Teardown(context.Context) error

	LifecycleOnCreated(ctx context.Context, config config.Config) error
	LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error
	LifecycleOnDeleted(ctx context.Context, config config.Config) error

	mustEmbedUnimplementedDestination()
}
```

The `(n int, err error)` return is the whole partial-batch-failure contract, borrowed verbatim from
`io.Writer`. It is a **prefix** contract: records `[0,n)` succeeded, record `n` is the one that
failed, `[n, len)` are unattempted. There is no way to express "records 3 and 7 failed, the rest are
fine" — see §9.3 for what that costs.

The destination has **no `Ack` method**. Acks are produced by the SDK adapter from the `(n, err)`
return and pushed into the stream. Asymmetric with `Source`, deliberately.

### 1.3 Boundary layer (`pconnector`)

`conduit-connector-protocol/pconnector/source.go`:

```go
type SourcePlugin interface {
	Configure(context.Context, SourceConfigureRequest) (SourceConfigureResponse, error)
	Open(context.Context, SourceOpenRequest) (SourceOpenResponse, error)
	Run(context.Context, SourceRunStream) error
	Stop(context.Context, SourceStopRequest) (SourceStopResponse, error)
	Teardown(context.Context, SourceTeardownRequest) (SourceTeardownResponse, error)

	LifecycleOnCreated(context.Context, SourceLifecycleOnCreatedRequest) (SourceLifecycleOnCreatedResponse, error)
	LifecycleOnUpdated(context.Context, SourceLifecycleOnUpdatedRequest) (SourceLifecycleOnUpdatedResponse, error)
	LifecycleOnDeleted(context.Context, SourceLifecycleOnDeletedRequest) (SourceLifecycleOnDeletedResponse, error)
}

// SourceRunStream is the bidirectional stream interface for SourcePlugin.Run.
type SourceRunStream interface {
	// Client is only allowed to be used by the host (Conduit).
	Client() SourceRunStreamClient
	// Server is only allowed to be used by the plugin (connector).
	Server() SourceRunStreamServer
}

type SourceRunStreamClient interface {
	Send(SourceRunRequest) error
	Recv() (SourceRunResponse, error)
}
type SourceRunStreamServer interface {
	Send(SourceRunResponse) error
	Recv() (SourceRunRequest, error)
}

type SourceRunRequest struct  { AckPositions []opencdc.Position }
type SourceRunResponse struct { Records      []opencdc.Record   }

type SourceOpenRequest struct { Position opencdc.Position }
type SourceStopResponse struct { LastPosition opencdc.Position }
```

Every method is `(ctx, RequestStruct) → (ResponseStruct, error)`, even where the request is empty
(`type SourceStopRequest struct{}`). That is not verbosity for its own sake: it is exactly the shape
protobuf generates, so the same Go interface can be implemented by a gRPC client with a mechanical
`toproto`/`fromproto` pair and **no signature change**. Adding a field to a request struct is
non-breaking for every implementation.

Destination side is symmetric, and this is where per-record acks appear:

```go
type DestinationRunRequest struct  { Records []opencdc.Record }
type DestinationRunResponse struct { Acks    []DestinationRunResponseAck }

type DestinationRunResponseAck struct {
	Position opencdc.Position
	Error    string   // <-- an error degraded to a string at the boundary
}
type DestinationStopRequest struct { LastPosition opencdc.Position }
```

`Error string`, not `error`. That is the fidelity loss the wire forces, and it is paid **even for
in-process connectors** so that both paths behave identically.

### 1.4 Engine-facing layer

`conduit/pkg/plugin/connector/plugin.go` — the whole file:

```go
// Dispenser dispenses specifier, source and destination plugins.
type Dispenser interface {
	DispenseSpecifier() (SpecifierPlugin, error)
	DispenseSource() (SourcePlugin, error)
	DispenseDestination() (DestinationPlugin, error)
}

type SourcePlugin interface {
	pconnector.SourcePlugin
	NewStream() pconnector.SourceRunStream
}

type DestinationPlugin interface {
	pconnector.DestinationPlugin
	NewStream() pconnector.DestinationRunStream
}

type SpecifierPlugin interface {
	pconnector.SpecifierPlugin
}
```

The engine's only extra requirement over the boundary interface is `NewStream()` — the transport
factory. Builtin returns an in-memory channel stream; standalone returns a gRPC stream. **That one
method is the entire seam between in-process and out-of-process.**

### 1.5 Registration

`conduit-connector-sdk/connector.go` — the whole file:

```go
// Connector combines all constructors for each plugin into one struct.
type Connector struct {
	// NewSpecification should create a new Specification that describes the
	// connector. This field is mandatory, if it is empty the connector won't work.
	NewSpecification func() Specification
	// NewSource should create a new Source plugin. If the plugin doesn't
	// implement a source connector this field can be nil.
	NewSource func() Source
	// NewDestination should create a new Destination plugin. If the plugin
	// doesn't implement a destination connector this field can be nil.
	NewDestination func() Destination
}
```

One connector *package* exports one `Connector` value carrying up to three constructors. A
source-only connector leaves `NewDestination` nil. Note these are **factories**, not instances: the
engine calls `conn.NewSource()` per connector instance, so instance state is naturally per-instance.

Registration into the engine is *not* `init()`-time self-registration. It is a literal compile-time
map in `conduit/pkg/plugin/connector/builtin/registry.go`:

```go
// DefaultBuiltinConnectors contains the default built-in connectors.
// The key of the map is the import path of the module
// containing the connector implementation.
var DefaultBuiltinConnectors = map[string]sdk.Connector{
	"github.com/conduitio/conduit-connector-file":      file.Connector,
	"github.com/conduitio/conduit-connector-generator": generator.Connector,
	"github.com/conduitio/conduit-connector-kafka":     kafka.Connector,
	"github.com/conduitio/conduit-connector-log":       connLog.Connector,
	"github.com/conduitio/conduit-connector-postgres":  postgres.Connector,
	"github.com/conduitio/conduit-connector-s3":        s3.Connector,
}
```

The module path key is load-bearing: `Registry.loadPlugins` reads `debug.ReadBuildInfo()` and
**overwrites the connector's self-declared version with the resolved Go module version**, so a
built-in connector cannot lie about its version. `NewRegistry` takes the map as a parameter, so an
embedding application can pass its own — that is Conduit's answer to "add a connector without
forking".

For canal, note the trade-off honestly: this is a *core edit* (one map literal + one import) per
built-in connector. It buys build-time verification that the plugin exists and a real version
string. An `init()`-registry would remove the edit but lose both.

---

## 2. Record model

`conduit-commons/opencdc/record.go` — the entire envelope:

```go
// Record represents a single data record produced by a source and/or consumed
// by a destination connector.
// Record should be used as a value, not a pointer, except when (de)serializing
// the record.
type Record struct {
	// Position uniquely represents the record.
	Position Position `json:"position"`
	// Operation defines what triggered the creation of a record. There are four
	// possibilities: create, update, delete or snapshot. [...]
	Operation Operation `json:"operation"`
	// Metadata contains additional information regarding the record.
	Metadata Metadata `json:"metadata"`

	// Key represents a value that should identify the entity (e.g. database row).
	Key Data `json:"key"`
	// Payload holds the payload change (data before and after the operation occurred).
	Payload Change `json:"payload"`

	serializer RecordSerializer
}
```

Five public fields. That is the whole model. No timestamp field, no schema field, no source field,
no topic/table field, no headers — all of those live in `Metadata` as string keys (§2.4).

### 2.1 Operation

`opencdc/operation.go`:

```go
const (
	OperationCreate   Operation = iota + 1 // create
	OperationUpdate                        // update
	OperationDelete                        // delete
	OperationSnapshot                      // snapshot
)

// Operation defines what triggered the creation of a record.
type Operation int
```

Four members, `iota + 1` so the zero value is invalid-and-detectable. `MarshalText`/`UnmarshalText`
give `"create"` etc. on the wire (via `stringer -linecomment`). **`UnmarshalText` deliberately
accepts unknown numeric operations** (`Operation(7)`) rather than erroring — forward compatibility
for a future operation type.

`snapshot` being a peer of `create`/`update`/`delete` rather than a flag is the single highest-value
modelling decision in Conduit for canal (§4).

### 2.2 Key vs payload, and before/after

`opencdc/data.go`:

```go
type Change struct {
	// Before contains the data before the operation occurred. This field is
	// optional and should only be populated for operations OperationUpdate
	// OperationDelete (if the system supports fetching the data before the
	// operation).
	Before Data `json:"before"`
	// After contains the data after the operation occurred. This field should
	// be populated for all operations except OperationDelete.
	After Data `json:"after"`
}
```

Before/after is a property of the **payload**, not of the record. `Key` has no before/after — a key
change is modelled as delete+create by the connector, not expressed in the envelope. Both `Before`
and `After` are optional and independently nil-able; the operation type tells you which to expect,
but nothing enforces it. The SDK's `Util.Source.NewRecord*` helpers are the de-facto enforcement:

```go
func (SourceUtil) NewRecordUpdate(position, metadata, key, payloadBefore, payloadAfter) opencdc.Record {
	metadata.SetReadAt(time.Now())
	return opencdc.Record{
		Position: position, Operation: opencdc.OperationUpdate,
		Metadata: metadata, Key: key,
		Payload: opencdc.Change{Before: payloadBefore, After: payloadAfter},
	}
}
```

`NewRecordDelete` populates only `Before`; `NewRecordCreate` and `NewRecordSnapshot` only `After`.

### 2.3 Raw bytes and structured data coexisting

This is a **sealed interface with exactly two implementations** — Go's approximation of a sum type:

```go
// Data is a structure that contains some bytes. The only structs implementing
// Data are RawData and StructuredData.
type Data interface {
	isData() // Ensure structs outside of this package can't implement this interface.
	Bytes() []byte
	Clone() Data
	ToProto(*opencdcv1.Data) error
}

// StructuredData contains data in form of a map with string keys and arbitrary values.
type StructuredData map[string]interface{}

// RawData contains unstructured data in form of a byte slice.
type RawData []byte
```

`isData()` is unexported, so the set of implementations is closed at the package boundary. Consumers
type-switch:

```go
func (r Record) mapData(d Data) interface{} {
	switch d := d.(type) {
	case StructuredData: return map[string]interface{}(d)
	case RawData:        return []byte(d)
	}
	return nil
}
```

**Nobody converts automatically.** There is no `AsStructured()` on `Data`. A destination that needs
structured data must either type-assert and fail, or parse the raw bytes itself — `DebeziumConverter`
in the SDK does exactly that (`parseRawDataAsJSON`). Conversion is pushed to the edges, and the
consequence is a family of `unwrap` processors in the engine (`pkg/plugin/processor/builtin/impl/unwrap/`:
`opencdc.go`, `debezium.go`, `kafka_connect.go`) that exist purely to un-nest a record that arrived as
raw JSON containing an encoded record. See §13.

`StructuredData.Clone()` recurses into nested `map[string]any` values only; other reference types
(slices, custom structs) are copied by reference. A shallow-ish deep copy.

`RawData.MarshalJSON` is base64 by default, with a hand-rolled encoder because "this is in the hot
path", and a context-keyed option (`JSONMarshalOptions.RawDataAsString`) to emit a string instead.
A `ctx` threaded into `MarshalJSON` is a smell worth not copying.

### 2.4 Metadata

`Metadata` is `map[string]string` with a large set of typed accessor pairs and reserved keys
(`opencdc/metadata.go`):

```
opencdc.version                     opencdc.createdAt              opencdc.readAt
opencdc.collection
opencdc.key.schema.subject          opencdc.key.schema.version
opencdc.payload.schema.subject      opencdc.payload.schema.version
opencdc.file.name                   opencdc.file.size              opencdc.file.hash
opencdc.file.chunked                opencdc.file.chunk.index       opencdc.file.chunk.count
conduit.source.plugin.name          conduit.source.plugin.version
conduit.destination.plugin.name     conduit.destination.plugin.version
conduit.source.connector.id
conduit.dlq.nack.error              conduit.dlq.nack.node.id
```

Two namespaces: `opencdc.*` (spec-level, portable) and `conduit.*` (engine-level, provenance and
DLQ). Accessors are `Get<X>() (T, error)` / `Set<X>(T)`, e.g. `GetReadAt() (time.Time, error)`,
`SetPayloadSchemaVersion(int)`; missing keys return `ErrMetadataFieldNotFound`.

The good part: a string→string map serialises trivially over any transport, and typed accessors give
back compile-time safety at the call site. The bad part is visible in the list — `opencdc.file.*`
(six keys about *file chunking*) is a source-shaped concern that leaked into the shared spec. That is
precisely the failure mode canal's constraint #1 forbids. `conduit.*` keys are also how the engine
smuggles data through a "connector-owned" envelope, with no reservation enforcement: a connector can
write `conduit.dlq.nack.error` and nothing stops it.

`MetadataCollection` (`opencdc.collection`) is the generic "which table/topic/bucket" field. Getting
that *one* concept into the envelope generically (rather than per-connector) is the right call and is
what the schema-subject naming keys off.

### 2.5 Serialization hook

```go
type Record struct { /* ... */ serializer RecordSerializer }

// SetSerializer sets the serializer used to encode the record into bytes. If
// serializer is nil, the serializing behavior is reset to the default (JSON).
// This method mutates the receiver and is not thread-safe.
func (r *Record) SetSerializer(serializer RecordSerializer)

// Bytes returns the serialized representation of the Record. [...]
func (r Record) Bytes() []byte {
	if r.serializer != nil {
		b, err := r.serializer.Serialize(r)
		if err == nil { return b }
		// serializer produced an error, fallback to default format
	}
	b, err := json.Marshal(r)
	if err != nil { panic(...) }
	return b
}
```

A **private, mutable, non-thread-safe field on a value type documented to be used as a value**, whose
error path is a silent fallback to a different format. `Clone()` does not copy it. This is the worst
part of the record model and canal should not copy it: put codec selection in the pipeline
configuration, never on the record.

---

## 3. Checkpoint model

### 3.1 Position

`conduit-commons/opencdc/position.go` — the whole file:

```go
// Position is a unique identifier for a record being processed.
// It's a Source's responsibility to choose and assign record positions,
// as they will be used by the Source in subsequent pipeline runs.
type Position []byte

// String is used when displaying the position in logs.
func (p Position) String() string {
	if p != nil { return string(p) }
	return "<nil>"
}
```

Opaque `[]byte`. The **only** things the engine ever does with a position:

- `bytes.Equal` — to match an ack to the record it belongs to
  (`destination_acker.go`, `destinationPluginAdapter.Stop`)
- store it, hand it back to `Open`
- `string(p)` for log lines and message IDs (`Message.ID() = SourceID + "/" + string(Position)`)

It never parses, orders, or compares-for-greater-than a position. The engine's own comment on why is
worth quoting verbatim, because it is the design rule canal wants (`pkg/connector/source.go`):

```go
	// nextAckSeq is a purely engine-internal, monotonically increasing
	// counter assigned to each Ack call — NOT derived from the opaque
	// connector Position bytes, which Source cannot generically parse or
	// compare (a Position's structure, e.g. per-partition offsets, is
	// entirely connector-defined). It is what lets onPersistFlushed
	// determine, without understanding Position's contents, which queued
	// acks a given durable flush covers [...]
	nextAckSeq uint64
```

When the engine needs ordering over positions, it invents its **own** monotonic sequence rather than
imputing structure to the opaque bytes. That is the correct answer to "who interprets the position":
only the connector, ever.

Interpretation on the connector side is unconstrained. Postgres uses JSON
(`conduit-connector-postgres/source/position/position.go`):

```go
const CurrentPositionVersion = 1

type Position struct {
	// Version identifies the position format. [...] Zero (the JSON key absent)
	// means a legacy v0.14 position that predates DBZ-3's additive fields.
	Version   int               `json:"version,omitempty"`
	Type      Type              `json:"type"`
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
	LastLSN   string            `json:"last_lsn,omitempty"`
	SnapshotLowWatermarkLSN string `json:"snapshot_low_watermark_lsn,omitempty"`
}

type SnapshotPositions map[string]SnapshotPosition
type SnapshotPosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
}
```

Note the position is **composite** — one aggregate blob carrying a mode tag, a per-table snapshot
progress map, *and* the CDC LSN. A single `[]byte` per connector, holding a whole state machine.

### 3.2 Position format versioning

This directly answers canal's open decision #9. The Postgres connector's contract, quoted from
source:

```go
// Backward/forward compatibility contract [...]
//   - A legacy v0.14 position has no "version" key, so it deserializes with
//     Version == 0. Version == 0 MUST be treated as "no low watermark recorded,
//     no schema history — behave exactly as v0.14 did" until a later event
//     naturally populates the new fields.
//   - All new fields are additive and omitempty, so a position written by this
//     version is still readable by an older connector (it ignores unknown keys)
//     and by a newer one. We deliberately do NOT reject a position whose Version
//     is greater than CurrentPositionVersion: the format is additive-only, so a
//     newer position stays structurally readable, and rejecting it would break
//     the "readable by N+1 versions" rule.
```

And the version is stamped at **serialize** time, not construction time, so a parsed legacy position
is silently upgraded on first write:

```go
func (p Position) ToSDKPosition() opencdc.Position {
	p.Version = CurrentPositionVersion // p is a value copy; safe to mutate.
	v, err := json.Marshal(p)
	if err != nil { panic(err) }
	return v
}
```

The engine-level rule that backs this is `CLAUDE.md` invariant 2: *"Position serialization changes
require a versioned migration path and an upgrade test."* Four ingredients: additive-only JSON,
`version` field with 0 = legacy, never reject a *newer* version, stamp on write.

### 3.3 Who owns it, where it is stored

Ownership chain:

1. Connector mints a `Position` per record (`Record.Position`).
2. Engine stores the **last acked** position in `connector.Instance.State`:
   ```go
   type SourceState struct { Position opencdc.Position }
   ```
   Note `Instance.State` is typed `any` and cast: `s.Instance.State.(SourceState)`. Destinations have
   their own: `type DestinationState struct { Positions map[string]opencdc.Position }`.
3. `connector.Persister` batches instance writes into a `conduit-commons/database.DB` transaction.
4. On restart, `Source.open` reads it straight back:
   ```go
   func (s *Source) open(ctx context.Context) error {
   	_, err := s.plugin.Open(ctx, pconnector.SourceOpenRequest{ Position: s.state().Position })
   	...
   }
   ```

The store is a namespaced KV over a pluggable `database.DB`; backends are `badger`, `sqlite`,
`postgres`, `inmemory` (`conduit/pkg/conduit/config.go`: `db.type` accepts
`badger,postgres,inmemory,sqlite`). The position is **not** a separate offset store — it is a field
inside the serialised connector instance, written with the rest of the instance record. One store,
one write, atomic by the DB's transaction.

### 3.4 When it is committed — and the sev-0 that changed the answer

The persister debounces (`pkg/connector/persister.go`):

```go
const (
	DefaultPersisterDelayThreshold       = time.Second
	DefaultPersisterBundleCountThreshold = 10000
)
```

`Persist()` enqueues into an in-memory batch; the batch flushes on 1s elapsed **or** 10 000 items,
whichever first, inside a single `db.NewTransaction(ctx, true)` → `tx.Commit()`. Callbacks fire in
goroutines after commit, tracked by `callbackWg`.

Until 2026-07, `Source.Ack` sent the ack to the plugin **and then** enqueued the persist. That was a
confirmed sev-0. From `docs/postmortems/20260723-source-ack-persist-ordering.md`:

> `Source.Ack` [...] sends the ack to the connector plugin (`stream.Send`) before the resulting
> position write reaches durable storage [...] For a plugin whose upstream **prunes** data once it is
> told to commit (Postgres logical replication slots: acking advances `confirmed_flush_lsn`, which
> frees WAL for recycling), a crash inside that window causes Conduit to resume, on restart, from a
> durably-persisted position that is now _behind_ data the upstream has already discarded — a
> structural, unrecoverable gap.

The fix ("Approach A") inverts it. `pkg/connector/source.go`:

```go
// Invariant 1: ack only after the resulting position is durably persisted.
// The plugin ack (stream.Send, driven from onPersistFlushed) is deferred
// until the persister confirms this exact call's resulting state write has
// been durably flushed. [...] a plugin's own upstream commit — e.g. a Postgres
// replication slot's confirmed_flush_lsn advance, which frees WAL for
// recycling — must never be triggered before Conduit's own crash-recoverable
// record of that position exists on disk [...]
// Do not reintroduce a synchronous stream.Send here without re-reading that
// design doc.
func (s *Source) Ack(ctx context.Context, p []opencdc.Position) error {
	...
	s.Instance.Lock()
	defer s.Instance.Unlock()
	s.Instance.State = SourceState{Position: p[len(p)-1]}

	s.ackMu.Lock()
	s.nextAckSeq++
	seq := s.nextAckSeq
	s.pendingAcks = append(s.pendingAcks, pendingAck{seq: seq, positions: p})
	s.ackMu.Unlock()

	err = s.Instance.persister.Persist(ctx, s.Instance, func(err error) {
		s.onPersistFlushed(seq, err)
	})
	...
}
```

So the answer to "committed on sink ack or on source read" is: **the engine's durable write happens on
downstream ack; the connector's upstream commit happens only after that write is confirmed durable.**
Three-phase, not two.

Two subtleties worth stealing:

- `durableAckSeq` only ever advances, and `pendingAcks` drains from the head up to it, so
  out-of-order flush confirmations are safe no-ops rather than double-sends or gaps. Because
  `SourceState.Position` is cumulative and monotonic, *whichever* flush lands covers every earlier
  seq.
- The deferred plugin-ack is handed to a dedicated per-source goroutine
  (`deliverDeferredAcks`) rather than sent inline from the persister callback, because a slow plugin
  would otherwise block the **process-wide** flush cycle. Retry policy is bounded and explicit:
  ```go
  const (
  	DefaultDeferredAckMaxRetries     = 12
  	DefaultDeferredAckBackoffInitial = 10 * time.Millisecond
  	DefaultDeferredAckBackoffCap     = 500 * time.Millisecond
  )
  ```
  with a comment noting a *dropped* boundary ack is "a permanent handoff deadlock + silent
  post-snapshot CDC loss", not a benign no-op — because a snapshot-gating source blocks on it (§4.4).

Cost of the fix, stated in the source: durability schedule unchanged, but "the plugin only learns
about it once it's true, which delays a pruning upstream's WAL/log retention release by up to one
debounce interval — bounded, tunable, and the entire trade this fix makes."

### 3.5 Restart, replay

Restart = read `SourceState.Position` from the store, pass it to `Open`. No log replay, no WAL of
in-flight records. Anything read-but-not-durably-acked is re-read from the upstream on restart, which
is why the guarantee is at-least-once (§9). There is no "replay from checkpoint N-3" facility: the
engine keeps exactly one position per connector.

---

## 4. Snapshot handling

Conduit's answer, in one line: **snapshot is not a separate pipeline, a separate connector, or a
separate interface — it is an `Operation` value and a mode tag inside the connector's opaque
position.** The engine knows nothing about it.

### 4.1 What the core knows

Exactly one thing: `opencdc.OperationSnapshot` exists as a peer of create/update/delete. The SDK
offers `Util.Source.NewRecordSnapshot(...)` and `Util.Destination.Route(...)` takes a
`handleSnapshot` callback alongside the other three. Nothing else in the engine mentions snapshots.
There is no `Snapshotter` interface, no `SupportsSnapshot()` capability bit, no snapshot phase in the
pipeline lifecycle.

That is the whole reason it works generically: a source that has no snapshot concept simply never
emits `OperationSnapshot`, and no core code path changes.

### 4.2 How a snapshot-then-CDC source is structured

`conduit-connector-postgres` composes two iterators behind one that satisfies the same shape
(`source/logrepl/combined.go`):

```go
type iterator interface {
	NextN(context.Context, int) ([]opencdc.Record, error)
	Ack(context.Context, opencdc.Position) error
	Teardown(context.Context) error
}

type CombinedIterator struct {
	conf Config
	pool *pgxpool.Pool

	cdcIterator      *CDCIterator
	snapshotIterator *snapshot.Iterator
	activeIterator   iterator
	snapshotLowWatermarkLSN string
}
```

Note the connector defines its **own** internal `iterator` interface mirroring the SDK's read shape.
`Source.ReadN` delegates to `CombinedIterator.NextN`. Mode selection happens at `Open` time from the
position:

```go
	willSnapshot := c.conf.WithSnapshot && pos.Type != position.TypeCDC
```

i.e. snapshot-vs-stream is **a mode encoded in the position** (`position.Type` ∈
`{TypeInitial, TypeSnapshot, TypeCDC}`), gated by a config flag. Resuming a pipeline whose position
says `TypeCDC` skips the snapshot entirely, forever, with no extra state anywhere.

### 4.3 Ordering constraint: CDC first, then snapshot

```go
	// Initialize the CDC iterator. This creates (or, on a resume, reuses) the
	// replication slot, so the slot's consistent point is only known afterwards.
	if err := c.initCDCIterator(ctx, pos); err != nil { return nil, err }
	...
	// Initialize the snapshot iterator when snapshotting is enabled and not completed.
	// The CDC iterator must be initialized first when snapshotting is requested.
	if err := c.initSnapshotIterator(ctx, pos); err != nil { return nil, err }

	switch {
	case c.snapshotIterator != nil:
		c.activeIterator = c.snapshotIterator
	default:
		if err := c.cdcIterator.StartSubscriber(ctx); err != nil { ... }
		c.activeIterator = c.cdcIterator
	}
```

The CDC subscription is **created but not started** during the snapshot. Creating the replication slot
first pins a WAL position (`RestartLSN`) and yields an exported transaction snapshot ID
(`TXSnapshotID()`), which the snapshot workers then read under — so the snapshot is point-in-time
consistent *with* the CDC start point. Nothing between the two can be lost. Starting the subscriber
only at handoff is also what guarantees the slot's `confirmed_flush_lsn` cannot advance during the
snapshot.

### 4.4 The handoff

```go
func (c *CombinedIterator) NextN(ctx context.Context, n int) ([]opencdc.Record, error) {
	records, err := c.activeIterator.NextN(ctx, n)
	if err != nil {
		if !errors.Is(err, snapshot.ErrIteratorDone) {
			return nil, fmt.Errorf("failed to fetch records in batch: %w", err)
		}
		// Snapshot iterator is done, handover to CDC iterator
		if err := c.useCDCIterator(ctx); err != nil { return nil, err }
		sdk.Logger(ctx).Debug().Msg("Snapshot completed, switching to CDC mode")
		return c.NextN(ctx, n)
	}
	return records, nil
}
```

A sentinel error (`snapshot.ErrIteratorDone = errors.New("snapshot complete")`) drives the transition,
inside the read call, transparently to the SDK and engine. `useCDCIterator` tears down the snapshot
iterator, re-seeds the CDC handler's carry-forward fields, then starts the subscriber.

The snapshot iterator's completion is **gated on acks**, which is where the deferred-ack deadlock of
§3.4 came from (`source/snapshot/iterator.go`):

```go
	case batch, ok := <-i.data:
		if !ok { // closed
			if err := i.workersTomb.Err(); err != nil { ... }
			if err := i.acks.Wait(ctx); err != nil {   // <-- blocks until every emitted record is acked
				return nil, fmt.Errorf("failed to wait for acks: %w", err)
			}
			return nil, ErrIteratorDone
		}
```

`i.acks` is a `csync.WaitGroup` incremented per emitted record and decremented in `Ack`. So the
snapshot will not declare itself done — and CDC will not start — until the engine has acked every
snapshot record. Combined with §3.4's "ack only after durable persist", the boundary ack becomes a
hard liveness dependency, documented in
`docs/design-documents/20260728-snapshot-handoff-deferred-ack-deadlock.md` and
`docs/postmortems/20260729-snapshot-handoff-deferred-ack-deadlock.md`.

### 4.5 Resumability, chunking, parallelism

**Resumable: yes, and it is expressed in the same opaque position.** `Snapshots` is a per-table map
of `{last_read, snapshot_end}` written on every emitted record:

```go
func (i *Iterator) buildRecord(d FetchData) opencdc.Record {
	// merge this position with latest position
	i.lastPosition.Type = position.TypeSnapshot
	i.lastPosition.Snapshots[d.Table] = d.Position

	pos := i.lastPosition.ToSDKPosition()
	metadata := make(opencdc.Metadata)
	metadata[opencdc.MetadataCollection] = d.Table
	if i.conf.SnapshotResumed {
		metadata[MetadataSnapshotResumed] = "true"
	}
	rec := sdk.Util.Source.NewRecordSnapshot(pos, metadata, d.Key, d.Payload)
	...
}
```

Every snapshot record carries the **full cumulative** snapshot progress across all tables, plus the
CDC watermark. Restart mid-snapshot resumes each table from its own `last_read`. No side state, no
second store.

**Chunked and parallel: yes, one `FetchWorker` per table, all fanning into one channel.**

```go
func (i *Iterator) initFetchers(ctx context.Context) error {
	i.workers = make([]*FetchWorker, len(i.conf.Tables))
	for j, t := range i.conf.Tables { ... }
}
func (i *Iterator) startWorkers() {
	for _, worker := range i.workers {
		i.workersTomb.Go(func() error { ... worker.Run(ctx) ... })
	}
	go func() { <-i.workersTomb.Dead(); close(i.data) }()
}
```

Parallelism is **per-table, not intra-table** — no key-range sharding of one big table. `FetchSize`
paginates within a table.

Honesty note that canal should copy: a *resumed* snapshot cannot re-acquire the exported transaction
snapshot, so its consistency degrades, and the connector makes that **observable on the record**
rather than silent:

```go
// MetadataSnapshotResumed is set to "true" on every snapshot record emitted by a
// snapshot that is resuming from a prior run's persisted progress [...]
// It lets a downstream consumer distinguish a resumed-snapshot record — read
// without the transaction-snapshot pin, whose point-in-time consistency is
// guaranteed only by CDC replay reconciliation, not by snapshot isolation — from
// a first-run snapshot record read under a pinned exported snapshot.
const MetadataSnapshotResumed = "postgres.snapshot.resumed"
```

Note it is a connector-namespaced metadata key (`postgres.*`), not an `opencdc.*` one — the right
place for a source-specific fidelity caveat.

---

## 5. Schema handling

### 5.1 Out-of-band, by reference, with an in-band pointer

Schemas are **not** carried on records. Records carry a *subject + version* pair in metadata:

```
opencdc.key.schema.subject          opencdc.key.schema.version
opencdc.payload.schema.subject      opencdc.payload.schema.version
```

The schema itself lives in a schema service. `conduit-commons/schema/schema.go`:

```go
type Type int32
const ( TypeAvro Type = iota + 1 ) // avro

type Schema struct {
	Subject string
	Version int
	ID      int
	Type    Type
	Bytes   []byte
}

func (s Schema) Marshal(v any) ([]byte, error)
func (s Schema) Unmarshal(b []byte, v any) error
// Fingerprint returns a unique 64 bit identifier for the schema.
func (s Schema) Fingerprint() uint64 { return rabin.Bytes(s.Bytes) }
func (s Schema) Serde() (Serde, error)
```

`Type` has exactly **one** member: Avro. The abstraction is present (`KnownSerdeFactories map[Type]SerdeFactory`,
`ErrUnsupportedType`) but unexercised — a one-implementation pluggability layer.

`Serde` is the codec seam:

```go
type Serde interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
	String() string
}
type SerdeFactory struct {
	Parse        func([]byte) (Serde, error)   // textual schema -> serde
	SerdeForType func(v any) (Serde, error)    // Go value -> serde (inference)
}
```

Serdes are cached process-globally by Rabin fingerprint with TTL:

```go
var globalSerdeCache = cache.New[uint64, Serde](
	cache.AutoCleanInterval(time.Hour),
	cache.MaxAge(4*time.Hour),
)
```

### 5.2 Discovery: inference from the data, per record

The connector does not declare a schema. The SDK **infers** one from each record and registers it, in
the `SourceWithSchemaExtraction` middleware:

```go
// SourceWithSchemaExtraction is a middleware that extracts a record's
// payload and key schemas. The schema is extracted from the record data
// for each record produced by the source. The schema is registered with the
// schema service and the schema subject is attached to the record metadata.
type SourceWithSchemaExtraction struct {
	SchemaTypeStr  string  `json:"sdk.schema.extract.type" validate:"inclusion=avro" default:"avro"`
	PayloadEnabled *bool   `json:"sdk.schema.extract.payload.enabled" default:"true"`
	PayloadSubject *string `json:"sdk.schema.extract.payload.subject" default:"payload"`
	KeyEnabled     *bool   `json:"sdk.schema.extract.key.enabled" default:"true"`
	KeySubject     *string `json:"sdk.schema.extract.key.subject" default:"key"`
}
```

On by default. The subject is a config string, optionally prefixed by `opencdc.collection` (from the
field doc: *"If the record metadata contains the field `opencdc.collection` it is prepended to the
subject name and separated with a dot"*), so a multi-table source naturally gets one subject per
table without the middleware knowing what a table is. Contexts add another prefix
(`SourceWithSchemaContext`, `qualifiedSubject` → `"<ctx>:<subject>"`), so two pipelines can use the
same subject names without collision.

A connector that *does* know its schema attaches it directly — Postgres does:

```go
	if i.conf.WithAvroSchema {
		cschema.AttachKeySchemaToRecord(rec, d.KeySchema)
		cschema.AttachPayloadSchemaToRecord(rec, d.PayloadSchema)
	}
```

So there are two paths to the same metadata keys: infer-in-SDK, or declare-in-connector.

### 5.3 Who assigns the ID; where the service lives

The **service** does, and it is reached differently per plugin mode. `conduit-connector-sdk/schema/schema.go`:

```go
// Service is the schema service client that can be used to interact with the schema service.
// It is initialized with an in-memory service by default.
var Service pconnutils.SchemaService = newCachedSchemaService(newInMemoryService())

func init() {
	internal.StandaloneConnectorUtilities = append(internal.StandaloneConnectorUtilities, standaloneInitializer{})
}
func (standaloneInitializer) Init(conn *grpc.ClientConn) error {
	Service = newCachedSchemaService(client.NewSchemaServiceClient(conn))
	return nil
}
```

- **Standalone:** the SDK swaps `Service` for a gRPC client pointed at Conduit's *connector-utilities*
  service, whose address arrives via env var (`EnvConduitConnectorUtilitiesGRPCTarget`), authenticated
  by a per-connector token (`EnvConduitConnectorToken`).
- **Builtin:** the engine assigns the same object it uses internally —
  `builtin.NewRegistry` does `schema.Service = service` (a `*connutils.SchemaService`).

Behind that, `conduit/pkg/schemaregistry/` speaks Confluent Schema Registry, with a builtin
in-process implementation and a `confluent` option (`schema-registry.type` accepts
`builtin,confluent`).

Two things to note: a **package-level mutable `var Service`** is the injection mechanism (global
state; fine for a one-connector-per-process subprocess, questionable in-process), and the SDK caches
aggressively (`schema/cached.go`) because a per-record registry lookup would be fatal.

### 5.4 Evolution and drift

There is **no drift policy in the code I read**. Extraction computes a schema per record and calls
`CreateSchema`; the registry assigns a new version if the bytes differ, and the record's metadata
points at whichever version it got. Records with different shapes get different versions and flow on.
There is no halt/DLQ/coerce switch, no compatibility-mode setting, no `SchemaEvolutionPolicy`.

The intent exists only as an invariant in `CLAUDE.md`:

> 6. **Schema handling never silently mangles data.** Unknown fields, type mismatches, and drift
>    follow the configured policy (halt/DLQ/evolve) — never silent coercion or truncation.

"the configured policy" names a knob I could not find implemented. Treat this as aspiration, and as a
gap canal should close deliberately: **the policy must be a first-class pipeline config with three
named outcomes, and it must exist before the first schema-carrying connector ships**, because
retrofitting it changes record semantics.

Destinations get `DestinationWithSchemaExtraction` (decode side) with `sdk.schema.extract.*.enabled`
defaults of `true`, so the round-trip is symmetric by default.

---

## 6. Lifecycle

### 6.1 The state machine is explicit and guarded

The SDK adapter runs a real state machine, not implicit ordering
(`conduit-connector-sdk/internal/connectorstate.go`, used from `source.go`). Every boundary method
wraps its work in `state.DoWithLock` with declared pre/mid/post states:

```go
func (a *sourcePluginAdapter) Open(ctx context.Context, req pconnector.SourceOpenRequest) (pconnector.SourceOpenResponse, error) {
	err := a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates:       []internal.ConnectorState{internal.StateConfigured},
		StateBefore:          internal.StateStarting,
		StateAfter:           internal.StateStarted,
		WaitForExpectedState: false,
	}, func(_ internal.ConnectorState) error { ... })
	...
}
```

Observed states: `StateInitial`, `StateConfiguring`, `StateConfigured`, `StateStarting`,
`StateStarted`, `StateInitiatingRun`, `StateRunning`, `StateInitiatingStop`, `StateStopping`,
`StateStopped`, `StateTearingDown`, `StateTornDown`, `StateErrored`.

`Stop` uses `WaitForExpectedState: true` (it will *wait* for the connector to reach a stoppable state)
and accepts several: `{StateRunning, StateStopping, StateTornDown, StateErrored}`, logging a warning
and no-oping if not actually running. `Teardown` accepts **any** state (`ExpectedStates: nil`).

### 6.2 Callback order

Engine-side, `pkg/connector/source.go` `Open()`:

```
DispenseSource()
  → Configure(cfg)
  → triggerLifecycleEvent(lastActiveConfig, currentConfig)   // OnCreated / OnUpdated, or skip
      → persist(lastActiveConfig)  if triggered
  → Open(position)
  → Run(stream)                    // starts runRead + runAck goroutines in the plugin
  → Instance.connector = s; persister.ConnectorStarted()
  → go deliverDeferredAcks()       // started last, after every fallible step
```

and shutdown:

```
Stop(ctx)         → plugin.Stop → returns LastPosition
                    (source: cancels open+read ctx, waits for Read to stop)
Teardown(ctx)     → drains deferred acks, forces + awaits a bounded persister flush,
                    then plugin.Teardown → impl.Teardown
```

Destination shutdown is position-driven, which is unusual and worth understanding
(`destinationPluginAdapter.Stop`):

```go
// Stop will initiate the stop of the destination connector. It will first wait
// that the last position processed by the connector matches the last position
// in the request and then trigger a flush [...]
// If the requested last position is not encountered in 1 minute it will proceed
// flushing records received so far and return an error. [...] In the worst case
// this operation can thus take 2 minutes.
	actualLastPosition, err := a.lastPosition.Watch(watchCtx,
		func(val opencdc.Position) bool { return bytes.Equal(val, req.LastPosition) })
```

The source's `Stop` reports the last position it emitted; the engine passes that to the destination's
`Stop`; the destination blocks until it has *seen* that exact position, then flushes. That is how the
pipeline drains deterministically without counting records. Both waits are bounded by
`stopTimeout = time.Minute` (a package var with a `TODO make the timeout configurable`).

### 6.3 Teardown after a failed Open — guaranteed

Engine side, in `Source.Open`:

```go
	defer func() {
		// ensure the plugin gets torn down if something bad happens
		if err != nil {
			_, tdErr := s.plugin.Teardown(ctx, pconnector.SourceTeardownRequest{})
			...
			s.plugin = nil
		}
	}()
```

SDK side, matching:

```go
		// teardown can be called without "open" or "read" being called previously
		// e.g. when Conduit is validating a connector configuration,
		// it will call "configure" and then "teardown".
		if a.openCancel != nil { a.openCancel() }
		if a.readCancel != nil { a.readCancel() }
```

So `Configure → Teardown` with no `Open` is a **legal, expected sequence**, used for config
validation. Connector authors must handle it. This is a contract canal should state explicitly.

### 6.4 Context and cancellation — the detach trick

`Open`'s context must outlive the `Open` call (connections opened there are used later), but must be
cancellable when the connector is asked to stop. The SDK does this with `ccontext.Detach`:

```go
		// detach context, so we can control when it's canceled
		ctxOpen := ccontext.Detach(ctx)
		ctxOpen, a.openCancel = context.WithCancel(ctxOpen)

		startDone := make(chan struct{})
		defer close(startDone)
		go func() {
			// for duration of the Start call we propagate the cancellation of ctx to
			// ctxOpen, after Start returns we decouple the context and let it live
			// until the plugin should stop running
			select {
			case <-ctx.Done():  a.openCancel()
			case <-startDone:   // start finished before ctx was canceled, leave context open
			}
		}()
		return a.impl.Open(ctxOpen, req.Position)
```

`Detach` keeps values, drops cancellation and deadline. So: cancelling during `Open` aborts it;
cancelling after `Open` returns does not — only `Stop`/`Teardown` do, via `openCancel`. Two distinct
cancellable scopes exist per source: `openCtx` (connection lifetime) and `readCtx` (read loop),
cancelled in that order by `Stop`.

The documented meaning of cancellation in `Read` is a genuinely subtle contract, quoted from the
interface doc:

> If Read receives a cancelled context or the context is cancelled while Read is running it must stop
> retrieving new records from the source system and start returning records that have already been
> buffered. If there are no buffered records left Read must return the context error to signal a
> graceful stop. If Read returns ErrBackoffRetry while the context is cancelled it will also signal
> that there are no records left and Read won't be called again.

That is: **cancellation means "drain, then stop", not "abort"**. Almost nobody gets this right by
accident, which is why it needs to be stated as prose in the interface.

### 6.5 Error classification

Three mechanisms, in increasing sophistication:

**(a) Sentinels in the SDK** (`conduit-connector-sdk/error.go` — the whole file):

```go
var (
	// ErrBackoffRetry can be returned by Source.Read to signal the SDK there is
	// no record to fetch right now and it should try again later.
	ErrBackoffRetry = errors.New("backoff retry")

	// ErrUnimplemented is returned in functions of plugins that don't implement
	// a certain method.
	ErrUnimplemented = errors.New("the connector plugin does not implement this action, ...")
)
```

`ErrBackoffRetry` is the *only* retry signal a connector has, and it is a long-poll idiom, not a
failure classification. The SDK's backoff is hardcoded:

```go
	// TODO make backoff params configurable (https://github.com/ConduitIO/conduit/issues/184)
	b := &backoff.Backoff{ Factor: 2, Min: time.Millisecond * 100, Max: time.Second * 5 }
```

There is **no typed retryable-vs-fatal distinction available to a connector author.** Any error other
than `ErrBackoffRetry`/`context.Canceled` terminates the read loop:

```go
			default:
				return fmt.Errorf("read plugin error: %w", err)
```

**(b) `cerrors.FatalError` in the engine** (`pkg/foundation/cerrors/fatal.go`):

```go
// fatalError is an error type that will differentiate these from other errors that could be retried.
type fatalError struct{ Err error }
func FatalError(err error) error
func IsFatalError(err error) bool
```

Engine-internal only. Non-fatal pipeline errors trigger recovery-with-backoff; fatal ones do not.
Used e.g. when the DLQ nack threshold is exceeded and when recovery attempts are exhausted.

**(c) `conduiterr` codes at the API boundary** (`pkg/foundation/cerrors/conduiterr/`,
`pkg/http/api/status/status.go`). Errors carry a stable machine-readable `reason`, a gRPC category, and
optionally `configPath`, `suggestion`, and `fix`:

```go
	err := conduiterr.Wrap(
		conduiterr.CodeConnectorPluginNotFound,
		fmt.Sprintf("builtin connector plugin %q not found", fullName.PluginName()),
		plugin.ErrPluginNotFound,
	)
	err.Suggestion = "run `conduit connectors list` to see available built-in connectors"
```

with the sentinel deliberately preserved underneath:

```go
	// Invariant: errors.Is(err, plugin.ErrPluginNotFound) still holds — the
	// sentinel is wrapped, and the ConduitError adds the machine-actionable code.
```

There is a contract test (`codes_contract_test.go`) that fails the build if a status reaches the wire
without a code. Every un-migrated path falls back to reason `internal.unknown` rather than emitting a
codeless status.

**Summary for canal:** Conduit has excellent *operator-facing* error taxonomy and effectively **no
connector-facing** one. The gap is exactly canal's failure-mode-taxonomy requirement: a connector
cannot say "this write failed because the row is malformed (never retry, DLQ it)" versus "this write
failed because the network blipped (retry me)". Everything is one `error`, and the engine's only
response is nack→DLQ or die.

### 6.6 Panic containment

Builtin (in-process) plugin calls are wrapped so a connector panic becomes an error rather than
killing the engine (`pkg/plugin/connector/builtin/sandbox.go`):

```go
// runSandbox takes a function and runs it in a sandboxed mode that catches
// panics and converts them into an error instead.
func runSandbox[REQ any, RES any](
	f func(context.Context, REQ) (RES, error),
	ctx context.Context, req REQ, logger log.CtxLogger, name string,
) (RES, error) {
	c := sandboxChanPool.Get().(chan any)
	go func() {
		defer sandboxChanPool.Put(c)
		defer func() {
			if r := recover(); r != nil { ... returnResponse(ctx, emptyRes, err, c, logger) }
		}()
		res, err := f(ctx, req)
		returnResponse(ctx, res, err, c, logger)
	}()

	select {
	case <-ctx.Done():
		logger.Error(ctx).Msgf("context cancelled while waiting for builtin connector plugin to respond to %q, detaching from plugin", name)
		var emptyRes RES
		return emptyRes, ctx.Err()
	case v := <-c: ...
	}
}
```

This is the in-process substitute for process isolation, and it does more than catch panics: the
`select` on `ctx.Done()` gives the host the ability to **abandon** a hung connector call and keep
running — the other thing a subprocess boundary gives you for free. The goroutine leaks until the call
returns, which is the honest cost.

Note also the redaction discipline at the same boundary:

```go
	// in.Config routinely carries secrets (DB urls with embedded passwords,
	// SASL credentials, access keys). There is no per-parameter sensitivity
	// metadata yet, so log.RedactAll redacts every value
	s.logger.Debug(ctx).Any("config", log.RedactAll(in.Config)).Msg("calling Configure")
```

"No per-parameter sensitivity metadata yet" is a config-model gap (§7) worked around at the log site.

---

## 7. Config model

This is the section most directly load-bearing for canal's "frontend driven by what connectors
declare" goal, and Conduit's answer is: **a Go struct with tags is the single source of truth; a
code generator turns it into a machine-readable spec; the spec is a YAML file embedded in the
connector binary; the engine and the UI both read the spec, never the struct.**

### 7.1 The declared spec

`conduit-connector-protocol/pconnector/specifier.go`:

```go
type SpecifierPlugin interface {
	Specify(context.Context, SpecifierSpecifyRequest) (SpecifierSpecifyResponse, error)
}

// Specification is returned by a plugin when Specify is called.
type Specification struct {
	Name        string
	Summary     string
	Description string
	// Version string. Should be a semver prepended with `v`, e.g. `v1.54.3`.
	Version     string
	Author      string
	// SourceParams and DestinationParams are maps of named Parameters that
	// describe how to configure the plugins Destination or Source.
	SourceParams      config.Parameters
	DestinationParams config.Parameters
}
```

Source and destination parameter sets are **separate**, on one connector spec. `Specify` takes an
empty request and is callable before `Configure` — the engine calls it once at registry-load time to
build its catalogue.

### 7.2 The parameter type

`conduit-commons/config/parameter.go` — the whole meaningful content:

```go
// Parameters is a map of all configuration parameters.
type Parameters map[string]Parameter

// Parameter defines a single configuration parameter.
type Parameter struct {
	// Default is the default value of the parameter, if any.
	Default string `json:"default"`
	// Description holds a description of the field and how to configure it.
	Description string `json:"description"`
	// Type defines the parameter data type.
	Type ParameterType `json:"type"`
	// Validations list of validations to check for the parameter.
	Validations []Validation `json:"validations"`
}

type ParameterType int
const (
	ParameterTypeString   ParameterType = iota + 1 // string
	ParameterTypeInt                               // int
	ParameterTypeFloat                             // float
	ParameterTypeBool                              // bool
	ParameterTypeFile                              // file
	ParameterTypeDuration                          // duration
)
```

Six types. `Default` is a **string** regardless of type (parsed later) — which keeps the wire format
trivially JSON/proto-able at the cost of type safety in the spec itself. `required` is **not** a
field; it is a validation. There is no `Sensitive`/`Secret` flag — hence §6.6's `log.RedactAll`
sledgehammer. There is no `Enum` field either; enums are `ValidationInclusion`. There is no
display-name, ordering, or grouping metadata, so a UI gets alphabetical keys and nothing else to lay
out with.

Config values themselves are the flattest thing possible:

```go
// Config is a map of configuration values. The keys are the configuration
// parameter names and the values are the configuration parameter values.
type Config map[string]string
```

`map[string]string` end to end — from YAML, over gRPC, into the connector. Structure is recovered
from **dotted keys** (`Config.breakUp()` splits on `.` into nested maps before decoding), and
**wildcards** are supported in parameter *names*: `collection.*.format` matches
`collection.foo.format` (`getKeysForParameter`, `matchParameterKey`). That is how per-collection
configuration is expressed without the core knowing what a collection is — genuinely useful, and
worth stealing for canal's multi-collection story.

### 7.3 Validations are objects, evaluable anywhere

`conduit-commons/config/validation.go`:

```go
type Validation interface {
	Type() ValidationType
	Value() string

	Validate(string) error
}

type ValidationType int64
const (
	ValidationTypeRequired    ValidationType = iota + 1 // required
	ValidationTypeGreaterThan                           // greater-than
	ValidationTypeLessThan                              // less-than
	ValidationTypeInclusion                             // inclusion
	ValidationTypeExclusion                             // exclusion
	ValidationTypeRegex                                 // regex
)
```

Six validation kinds, each a struct implementing the interface, each with a `MarshalJSON` that
flattens to `{"type": ..., "value": ...}`:

```go
func jsonMarshalValidation(v Validation) ([]byte, error) {
	return json.Marshal(map[string]any{ "type": v.Type(), "value": v.Value() })
}
```

So a validation is (a) executable in Go via `Validate(string) error`, and (b) a two-string tuple on
the wire — which a browser can re-implement for **all six** kinds trivially (`required`,
`greater-than`, `less-than`, `inclusion` (comma-joined list), `exclusion`, `regex`). That is the
property that makes client-side validation possible with no per-connector frontend code. Note the
regex is a Go `regexp` (RE2) serialised as its source string, so a JS `RegExp` will accept most but
not all patterns — a real, small incompatibility.

`ValidationInclusion.Value()` is `strings.Join(v.List, ",")`, so a list member containing a comma is
unrepresentable. Same class of bug as any delimiter-joined list.

### 7.4 Parse / default / validate / decode, in one call

`conduit-connector-sdk/util.go`:

```go
func parseConfig(ctx context.Context, cfg config.Config, target any, params config.Parameters) error {
	c := cfg.Sanitize().ApplyDefaults(params)

	err := c.Validate(params)
	if err != nil { return fmt.Errorf("config invalid: %w", err) }

	if err := c.DecodeInto(target); err != nil { return err }

	if v, ok := target.(Validatable); ok {
		err := v.Validate(ctx)
		if err != nil { return fmt.Errorf("config invalid: %w", err) }
	}
	return err
}
```

Fixed order: **sanitize (trim keys and values) → apply defaults → validate against spec → decode into
struct → custom `Validate(ctx)` hook**. `Config.Validate` also rejects unknown keys
(`ErrUnrecognizedParameter`) and validates types by parse-attempt (`strconv.Atoi`,
`time.ParseDuration`, …). Errors are **accumulated**, not short-circuited:

```go
	return errors.Join(errs...)
```

so a UI gets every config error at once. `validateParamValue` has one important rule: an empty value
for a non-required parameter is valid and skips all other validations, so you can leave optionals
blank without tripping a `greater-than`.

Decoding is `mitchellh/mapstructure` with `TagName: "json"`, `WeaklyTypedInput: true`, `Squash: true`,
plus hooks for empty-string→zero-value, `StringToTimeDuration`, and comma-`StringToSlice`. So the
**same `json:` tag** names the wire key, the spec key, and the struct field. One tag, three jobs.

The `Validatable` escape hatch is the cross-field validation seam:

```go
// Validatable can be implemented by a SourceConfig or DestinationConfig or any
// embedded struct, to provide custom validation logic. Validate will be
// triggered automatically by the SDK after parsing the config.
type Validatable interface {
	Validate(context.Context) error
}
```

Both `SourceConfig` and `DestinationConfig` embed it, so *every* connector config is validatable, and
`DefaultSourceMiddleware.Validate` walks its own fields by reflection and joins every embedded
`Validatable`'s errors. Cross-field validations are Go-only — a UI cannot evaluate them, and must
round-trip to the engine. Conduit accepts that split rather than inventing an expression language.

### 7.5 Generation: struct tags → `connector.yaml`

The tags, from `conduit-commons/paramgen/paramgen/paramgen.go`:

```go
const (
	tagParamName     = "json"
	tagParamDefault  = "default"
	tagParamValidate = "validate"

	validationRequired    = "required"
	validationLT          = "lt"
	validationLessThan    = "less-than"
	validationGT          = "gt"
	validationGreaterThan = "greater-than"
	validationInclusion   = "inclusion"
	validationExclusion   = "exclusion"
	validationRegex       = "regex"

	tagSeparator      = ","
	validateSeparator = "="
	listSeparator     = "|"
	fieldSeparator    = "."
)
```

In practice (from the SDK's own middleware configs):

```go
type SourceWithBatch struct {
	// Maximum size of batch before it gets read from the source.
	BatchSize *int `json:"sdk.batch.size" default:"0" validate:"gt=-1"`
	// Maximum delay before an incomplete batch is read from the source.
	BatchDelay *time.Duration `json:"sdk.batch.delay" default:"0"`
}
```

**The Go doc comment above the field becomes `Parameter.Description`.** That is the detail that makes
this actually work: documentation cannot drift from the spec because it *is* the spec.
`validate:"inclusion=avro"` lists options; lists use `|` as the separator inside a tag
(`inclusion=a|b|c`) because `,` already separates tags. `required=false` is parsed and then
*discarded* rather than recorded as "explicitly optional".

Two generations of the tool exist: `conduit-commons/paramgen` (older, emits a Go file of
`config.Parameters` — you can see its output committed as `*_paramgen.go` in the engine's processors)
and `conduit-connector-sdk/conn-sdk-cli/specgen` (current for connectors, emits YAML). From
`specgen/README.md`:

> `specgen` is a tool that generates connector specifications and writes them to a `connector.yaml`.
> The input to `specgen` are source and destination configuration structs returned by the `Config()`
> methods in the connectors. [...]
> 1. Extracts the specifications from the source and destination configuration struct.
> 2. Combines the extracted specification with the existing one in `connector.yaml`.

Phase 2 is the interesting part: the YAML is **round-tripped and merged**, so hand-written prose
(`summary`, `description`, `author`) survives regeneration while parameters are recomputed.

The YAML itself is a versioned document with a changelog
(`conn-sdk-cli/specgen/model/v1/model.go`):

```go
const LatestVersion = "1.0"

// Changelog should be adjusted every time we change the specification and add
// a new config version. Based on the changelog the parser will output warnings.
var Changelog = evolviconf.Changelog{
	semver.MustParse("1.0"): {}, // initial version
}

type Specification struct {
	Version                string                 `json:"version" yaml:"version"`
	ConnectorSpecification ConnectorSpecification `json:"specification" yaml:"specification"`
}
```

and note `Parameters` here is a **slice**, not a map — because YAML order is meaningful for humans,
whereas `config.Parameters` (the runtime type) is a map. `ToConfig()` converts slice→map. Ordering
information is lost at that boundary, which is one reason a UI has no field order.

The connector wires the YAML in at compile time:

```go
//go:generate specgen
//go:embed connector.yaml
var specs string

var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs, ""),
	NewSource:        NewSource,
	NewDestination:   NewDestination,
}
```

(the README example shows the older one-argument `sdk.YAMLSpecification(specs)`; the current signature
is `YAMLSpecification(rawYaml, version string)` — stale doc, verified against `util.go`).

There is also a CLI path for humans and tooling: `Serve` intercepts `argv[1]` before the handshake:

```go
	case "spec", "specs", "specification", "specifications":
		spec := v1.Specification{}.FromConfig(c.NewSpecification())
		out, err := yaml.Marshal(spec)
		fmt.Println(string(out)); os.Exit(0)
	case "version":
		fmt.Println(c.NewSpecification().Version); os.Exit(0)
```

so `./my-connector spec` prints the machine-readable spec without any gRPC involvement. Cheap, and
exactly what a registry indexer or a doc generator wants.

### 7.6 Verdict for canal

Is the spec machine-readable enough to drive a UI with no per-connector frontend code? **Yes for
rendering and single-field validation; no for good UX.** You get name, type (6), default, description,
required-ness and 5 more validations. You do **not** get: secret/sensitive flag, field order, grouping
or sections, display labels, conditional visibility ("show `ssl.ca` only if `ssl.enabled`"), or units.
Those are exactly the fields canal should add to `Parameter` *before* the first connector ships,
because adding them later means editing every connector's tags.

---

## 8. Backpressure

### 8.1 The batching evolution (and its scar tissue)

Conduit read one record at a time (`Read`) and grew batching (`ReadN`) later. Both are on the
interface, and the SDK negotiates **at runtime, by catching a sentinel error**
(`conduit-connector-sdk/source.go`, `runRead`):

```go
	batching := true
	readFn := a.impl.ReadN

	for {
		recs, err := readFn(ctx, 1) // default is to read 1 record, the batch middleware can override it
		if err != nil {
			switch {
			case errors.Is(err, ErrBackoffRetry):
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil { return nil }
				continue

			case errors.Is(err, context.Canceled):
				return nil // not an actual error

			case errors.Is(err, ErrUnimplemented) && batching:
				Logger(ctx).Info().Msg("source does not support batch reads, falling back to single reads")
				readFn = func(ctx context.Context, _ int) ([]opencdc.Record, error) {
					rec, err := a.impl.Read(ctx)
					if err != nil { return nil, err }
					return []opencdc.Record{rec}, nil
				}
				batching = false
				continue

			default:
				return fmt.Errorf("read plugin error: %w", err)
			}
		}
		if len(recs) == 0 { continue }

		err = stream.Send(pconnector.SourceRunResponse{Records: recs})
		if err != nil { return fmt.Errorf("read stream error: %w", err) }
		a.lastPosition = recs[len(recs)-1].Position // store last sent position
		b.Reset()
	}
```

Capability negotiation by **failing the first call and inspecting the error** is the design canal
should avoid: it costs a wasted call, it conflates "I don't implement batching" with "my batch read
happened to return ErrUnimplemented from a dependency", and it caused a real bug —
conduit-connector-sdk#248, *"sourceWithEncoding middleware works impropertly in combination with
sourceWithBatch"*: because `runRead` prefers `ReadN`, a source implementing `ReadN` bypassed the
encoding middleware's `Read` override and records went out unencoded. A capability *declaration*
(a bool in the spec, or a separate optional interface type-asserted once) has none of these problems.

Also note `readFn(ctx, 1)` — the default batch size is **1**. Batching is opt-in via the middleware.

### 8.2 What actually bounds the system: unbuffered channels

v1 engine (`pkg/lifecycle/stream/base.go`):

```go
	n.msgChan = make(chan *Message)
	...
	out := make(chan *Message)
```

Every inter-node channel is **unbuffered**. The pipeline is
`SourceNode → SourceAckerNode → [ProcessorNodes] → DestinationNode → DestinationAckerNode`, connected
by `Pub()`/`Sub()`:

```go
func (n *pubNodeBase) Pub() <-chan *Message {
	if n.out != nil { panic(cerrors.New("can't connect PubNode to more than one out")) }
	out := make(chan *Message)
	n.out = out
	return out
}
```

So backpressure is **pure blocking rendezvous**, propagated hop by hop from the destination back to
the source's `stream.Recv()`, and from there — because the source plugin's `stream.Send` blocks —
into the connector's `Read`. There is no queue depth to tune, no high-water mark, no drop policy, and
no `Nack`-on-full path. A slow sink simply makes `Read` stop being called.

The in-memory plugin stream is likewise unbuffered:

```go
	s.stream = &inMemoryStream[...]{ ctx: ctx, reqChan: make(chan REQ), respChan: make(chan RES), stopChan: make(chan struct{}) }
```

The consequences are worth stating plainly, because they are the price of the simplicity:

- **No rejection outcome exists.** A source cannot be told "I'm full, come back later"; it is simply
  blocked. canal's R6 (an expressible rejection outcome) has no analogue here and cannot be retrofitted
  onto a rendezvous channel without changing the node contract.
- **Zero in-flight buffering means zero pipelining**, which is exactly why v2 exists (§8.4).
- Fan-out is explicitly *not* backpressure-isolating: `pkg/lifecycle/stream/fanout.go` exists, and one
  slow destination in a fan-out blocks the source for all of them.

Buffering, where it exists, is inside the connector adapters, not the pipeline:

- source-side: `SourceWithBatch` middleware (`readCh` has capacity `*s.config.BatchSize`);
- destination-side: `writeStrategyBatch` wrapping an `internal.Batcher`;
- destination acker: an unbounded `deque.Deque[*Message]` of in-flight messages awaiting acks
  (`destination_acker.go`) — the one genuinely unbounded structure in the data path, bounded in
  practice only by how far ahead `DestinationNode` can get, which unbuffered channels keep small.

### 8.3 Concurrency and rate limits

- **Read/Ack concurrency:** documented as safe (`Read can be called concurrently with Ack`), and
  implemented as two goroutines in one tomb (`runRead`, `runAck`).
- **Ack ordering is serialised by a semaphore**, not by a channel
  (`pkg/lifecycle/stream/source_acker.go`):
  ```go
  	// sem ensures acks are sent to the source in the correct order and only one
  	// at a time
  	sem semaphore.Simple
  	// fail is set to true once the first ack/nack fails and we can't guarantee
  	// that acks will be delivered in the correct order to the source anymore,
  	// at that point we completely stop processing acks/nacks
  	fail bool
  ```
  A ticket is taken at *enqueue* time (in pipeline order) and acquired at *ack* time, so acks reach the
  source in read order even though they complete out of order downstream. And a single ack failure
  **latches the node broken** — because after one out-of-order ack, the position guarantee is void.
  That latch is a good pattern: it converts a subtle correctness violation into a loud stop.
- **Parallel processing** exists as a node type (`pkg/lifecycle/stream/parallel.go`) for processors,
  not for connectors.
- **Rate limiting** is destination-side only, via middleware:
  ```go
  type DestinationWithRateLimit struct {
  	RatePerSecond float64 `json:"sdk.rate.perSecond" default:"0" validate:"gt=-1"`
  	Burst         int     `json:"sdk.rate.burst" default:"0" validate:"gt=-1"`
  }
  ```
- **Message size** is bounded at the gRPC boundary only:
  `grpc.MaxRecvMsgSize(grpcCfg.MaxReceiveRecordSize)`, configured via
  `EnvConduitConnectorMaxReceiveRecordSize`.

### 8.4 Why v2 exists — the measured cost of record-at-a-time

From `docs/architecture-decision-records/20260704-pipeline-architecture-v2.md`:

| Metric (per 100k records) | v1 (record-at-a-time) | v2 (1000-record batches) | v2 advantage |
| --- | --- | --- | --- |
| Allocations | ~6,905,000 | ~1,104,000 | ~6.3× fewer |
| Memory | ~250 MB | ~76 MB | ~3.3× less |
| Per record | ~69 allocs / ~2497 B | ~11 allocs / ~764 B | — |

> The driver is batching: v2's source returns 1000-record batches per read and the worker moves them
> as a unit, amortizing the per-record channel hops, function calls, and acking that v1 pays on every
> single record across four nodes.

**69 allocations and 2.5 KB per record**, for a pipeline with no-op connectors and no real I/O. That
is the number canal should keep in mind: a node-graph-of-goroutines-per-record architecture costs
roughly 6× what a batch pipeline costs, and the cost is structural, not a missing optimisation.

v2's unit of work is a `Batch` with per-record status (§9.3), moved by a `Worker` through `Task`s
(`SourceTask`, `ProcessorTask`, `DestinationTask`) rather than through channels between goroutines.

### 8.5 Destination batching, and its `Stop` interaction

```go
func (a *destinationPluginAdapter) configureWriteStrategy(ctx context.Context) {
	writeSingle := &writeStrategySingle{impl: a.impl, ackFn: a.ack}
	a.writeStrategy = writeSingle // by default we write single records

	batchConfig := (&destinationWithBatch{}).getBatchConfig(ctx)
	if batchConfig.BatchSize > 1 || batchConfig.BatchDelay > 0 {
		a.writeStrategy = newWriteStrategyBatch(writeSingle, batchConfig.BatchSize, batchConfig.BatchDelay)
	}
}
```

Two strategies behind one interface:

```go
type writeStrategy interface {
	Write(ctx context.Context, recs []opencdc.Record) error
	Flush(ctx context.Context) error
	SetStream(pconnector.DestinationRunStreamServer)
}
```

`writeStrategySingle` writes through and acks immediately. `writeStrategyBatch` accumulates until
`batchSize` or `batchDelay`, and — importantly — **reports the previous batch's error on a later
call**:

```go
	select {
	case result := <-w.batcher.Results():
		if result.Err != nil { return fmt.Errorf("last batch write failed: %w", result.Err) }
	default:
		// last batch was not flushed yet
	}
```

Async flush means an error surfaces one call late; the batch that failed has already had its acks
computed. Note also the batch strategy stashes a `context.Context` inside a queued item
(`type writeBatchItem struct { ctx context.Context; records []opencdc.Record }`, with a `//nolint:containedctx`)
and uses "the last record's context as the write context". A queued context is a landmine: it can be
cancelled while the batch is still pending.

---

## 9. Delivery guarantees

### 9.1 The stated guarantee: at-least-once, no more

From `CLAUDE.md`, the engine's own data-integrity invariants (all seven quoted, because as a set they
are the most transplantable artifact in this repo):

> 1. **Never acknowledge a record upstream before it is durably handled downstream.** Ack propagation
>    is end-to-end; no intermediate component may ack early for throughput. Any batching/buffering
>    change must prove ack correctness under crash.
> 2. **Positions/offsets are monotonic and crash-safe.** A restart must never skip records
>    (at-least-once) or corrupt position state. Position serialization changes require a versioned
>    migration path and an upgrade test.
> 3. **At-least-once is the floor.** Any path that could drop a record without delivering it or
>    routing it to a DLQ is a data-loss bug — including error, shutdown, and rebalance paths.
> 4. **Ordering guarantees are per-source-partition and documented.** Changes that could reorder
>    records within a partition key require a design doc and explicit sign-off.
> 5. **State and checkpoint writes are atomic.** Torn writes on crash must be impossible
>    (write-ahead + rename, or the store's transactional API). Every state feature ships with a
>    kill-mid-write recovery test.
> 6. **Schema handling never silently mangles data.** Unknown fields, type mismatches, and drift
>    follow the configured policy (halt/DLQ/evolve) — never silent coercion or truncation.
> 7. **Shutdown is graceful by default.** SIGTERM drains in-flight records and checkpoints before
>    exit. `kill -9` at any instant must be recoverable without loss — and we test exactly that.
>
> Where code upholds one of these, say so at the enforcement site:
> `// Invariant 1: ack only after destination confirms durable write`.

And the enforcement convention is real — `grep 'Invariant [0-9]'` across `pkg/connector/source.go`,
`pkg/lifecycle/stream/source_acker.go` and `conduit-connector-postgres/source/logrepl/combined.go`
returns comments that cite the invariant *and* the design doc at the exact line that upholds it.

**Exactly-once does not exist.** There is no transaction coordinator, no two-phase commit, no epoch
fencing in the data path, and no idempotency support in the record model (no dedup key, no producer
id/sequence). `docs/architecture-decision-records/20260704-local-state-only.md` scopes dedup as a
future *processor* feature ("deduplication with TTL") over local state, explicitly not a delivery
guarantee. Idempotency is entirely pushed onto the destination connector, unaided.

### 9.2 How acks flow

Source side, per record, v1:

```
DestinationAckerNode receives ack from destination plugin
  → bytes.Equal(msg.Record.Position, ack.Position)   // strict: mismatch = error, node dies
  → msg.Ack()  (or msg.Nack(err, nodeID))
      → SourceAckerNode's registered handler acquires its ordering ticket
      → Source.Ack(ctx, []Position{pos})
          → Instance.State = SourceState{Position: last}
          → persister.Persist(..., callback)
          → [after durable flush] deferred stream.Send to the plugin
              → sdk runAck loop → impl.Ack(ctx, pos) per position
```

Acks are **per record on the internal bus, per batch on the wire**
(`SourceRunRequest{AckPositions []Position}`, `Source.Ack(ctx, p []Position)`), and strictly ordered.
The proto states the ordering guarantee explicitly:

> Acknowledgments will be sent back to the connector in the same order as the records produced by the
> connector. If a record could not be processed by Conduit, the stream will be closed without an
> acknowledgment being sent back to the connector.

That last sentence is the whole nack protocol on the source side: **there is no negative
acknowledgment on the wire.** A missing ack + closed stream *is* the nack. From `source.proto`'s
`Stop` doc: *"If Conduit did not send an acknowledgment for a record after the stream is closed, it
should be interpreted as a negative acknowledgment."*

### 9.3 Partial batch failure — the exact gap

Destination side, the `(n, err)` contract is converted to acks in `writeStrategySingle.Write`:

```go
func (w *writeStrategySingle) Write(ctx context.Context, batch []opencdc.Record) error {
	n, err := w.impl.Write(ctx, batch)
	...
	if n == len(batch) && err != nil {
		err = fmt.Errorf("connector reported a successful write of all records in the batch and simultaneously returned an error, this is probably a bug in the connector. Original error: %w", err)
		n = 0 // nack all messages in the batch
	} else if n < len(batch) && err == nil {
		err = fmt.Errorf("batch contained %d messages, connector has only written %d without reporting the error, this is probably a bug in the connector", len(batch), n)
	}

	ackErr := w.ackFn(batch[:n], nil, w.stream)   // ack the prefix
	if ackErr != nil { return fmt.Errorf("ack error: %w", ackErr) }
	ackErr = w.ackFn(batch[n:], err, w.stream)    // nack the suffix, ALL with the same error
	if ackErr != nil { return fmt.Errorf("ack error: %w", ackErr) }

	return nil
}
```

So the answer to canal's R7 question is precise:

- A destination **can** partially fail a batch, but only as a **prefix**: `[0,n)` ack, `[n,len)` nack.
- **Every** nacked record in the batch carries the **same** error string, including the ones that were
  never attempted. A record that would have succeeded is dead-lettered with someone else's error
  message.
- Contract violations by the connector are detected and converted into loud errors rather than silent
  corruption — note it *nacks the whole batch* (`n = 0`) when a connector claims total success and
  also errors. Fail-safe direction.

`DestinationRunResponseAck.Error` is a `string`, so even the one error that is real has lost its type
and its wrap chain by the time the engine sees it. `pkg/connector/destination.go` re-inflates it into
`DestinationAck{Position, Error error}` — a fresh error with no `errors.Is` identity.

**For canal:** a per-record status array (v2's `Batch` already does this internally, see below) plus a
typed per-record failure reason is the fix, and it must be in the *boundary* type, not just the
engine's internal type — otherwise the wire flattens it again.

v2's internal model shows what the right shape looks like (`pkg/lifecycle-poc/funnel/batch.go`):

```go
type Batch struct {
	records        []opencdc.Record
	recordStatuses []RecordStatus
	positions      []opencdc.Position
	filterCount    int
	tainted        bool
	splitRecords   map[string]opencdc.Record
}

const (
	RecordFlagAck    RecordFlag = iota // ack
	RecordFlagNack                     // nack
	RecordFlagRetry                    // retry
	RecordFlagFilter                   // filter
)

func (b *Batch) Ack(i int, j ...int)
func (b *Batch) Nack(i int, errs ...error)
func (b *Batch) Retry(i int, j ...int)
func (b *Batch) Filter(i int, j ...int)
func (b *Batch) SetRecords(i int, recs []opencdc.Record)
func (b *Batch) SplitRecord(i int, recs []opencdc.Record)
```

Four per-record outcomes — **including `Retry`, which v1 has no concept of** — with per-record errors,
plus a `tainted` flag meaning "this batch must be split into runs of same-status records before it can
proceed". `positions` is snapshotted at construction:

```go
	// Store positions separately, as we need the original positions when acking
	// records in the source, in case a processor tries to change the position.
```

A processor mutating `Record.Position` would otherwise make the record unackable. Defending the
position against the transform chain is a subtlety canal will need too.

### 9.4 DLQ

The DLQ is **a destination connector, not a store**. `pkg/lifecycle/stream/dlq.go`:

```go
type DLQHandler interface {
	Open(context.Context) error
	Write(context.Context, opencdc.Record) error
	Close(context.Context) error
}
```

and the default implementation just wraps a normal destination *synchronously*
(`pkg/lifecycle/dlq.go`):

```go
// Write writes the record synchronously to the destination, meaning that it
// waits until an ack is received for the record before it returns. If the
// record write fails or the destination returns a nack, the function returns an
// error.
func (d *DLQDestination) Write(ctx context.Context, rec opencdc.Record) error {
	err := d.Destination.Write(ctx, []opencdc.Record{rec})
	...
	ack, err := d.Destination.Ack(ctx)
	...
	if ack[0].Error != nil { return ack[0].Error } // nack
	return nil
}
```

Synchronous write-and-await-ack is what makes invariant 1 hold through the DLQ: the source is only
acked after the DLQ write is confirmed (`source_acker.go`'s nack handler calls `DLQHandlerNode.Nack`
first, *then* `Source.Ack`).

The record written is a **re-wrapped** record, not the original:

```go
func (n *DLQHandlerNode) dlqRecord(msg *Message, nackMetadata NackMetadata) (opencdc.Record, error) {
	r := opencdc.Record{
		Position:  opencdc.Position(msg.ID()),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{},
		Key:       nil,
		Payload: opencdc.Change{
			Before: nil,
			After:  opencdc.StructuredData(msg.Record.Map()), // failed record is stored here
		},
	}
	r.Metadata.SetCreatedAt(time.Now())
	r.Metadata.SetConduitDLQNackError(nackMetadata.Reason.Error())
	r.Metadata.SetConduitDLQNackNodeID(nackMetadata.NodeID)
	return r, nil
}
```

The original becomes a `StructuredData` map nested under `payload.after`, with the failure cause and
the failing node id in metadata. This nesting is why the `unwrap` processors exist (§13.2), and note
the failure cause is again a **string** (`Reason.Error()`).

The circuit breaker is a **fixed-size ring buffer of ack/nack outcomes**:

```go
// dlqWindow is responsible for tracking the last N nacks/acks and enforce a
// threshold of nacks that should not be exceeded.
type dlqWindow struct {
	window        []bool // ring buffer, true = nack
	cursor        int
	nackThreshold int
	ackCount      int
	nackCount     int
}

func (w *dlqWindow) Nack() bool {
	w.store(true)
	return w.nackThreshold >= w.nackCount
}
```

Exceeding it is fatal:

```go
		if n.WindowNackThreshold > 0 {
			return cerrors.FatalError(cerrors.Errorf(
				"DLQ nack threshold exceeded (%d/%d), original error: %w",
				n.WindowNackThreshold, n.WindowSize, nackMetadata.Reason))
		}
		// DLQ is disabled, we don't need to wrap the error message
		return nackMetadata.Reason
```

Defaults make the DLQ **off** in the sense that matters (`pkg/pipeline/instance.go`):

```go
var DefaultDLQ = DLQ{
	Plugin: "builtin:log",
	Settings: map[string]string{ "level": "warn", "message": "record delivery failed" },
	WindowSize:          1,
	WindowNackThreshold: 0,
}
```

`WindowNackThreshold: 0` = "tolerate zero nacks" = the first nack stops the pipeline, and the
"DLQ destination" is a log line. That is the right default for a data tool: **fail loudly rather than
silently discard**. And there is a nice optimisation-with-a-comment: if the threshold is 0 the window
size is forced to 1, since it cannot matter.

If the DLQ write itself fails, the node latches:

```go
	defer func() {
		if err != nil {
			// write to the DLQ failed, this message is essentially lost
			// we need to stop processing more nacks and stop the pipeline
			n.state.Set(dlqHandlerNodeStateBroken)
		}
	}()
```

Finally, and this is a real product gap Conduit chose to own rather than paper over — from
conduit#2639: **dead-lettered record content is not queryable in-product.**

> The engine keeps no queryable copy of dead-lettered records — the DLQ is a destination connector,
> not a store — so v0.18 ships a **config-only DLQ view** [...] Dead-lettered record *content* is not
> queryable in-product, and the panel says so plainly.

canal wants a frontend showing "what failed and why". Conduit's answer is "look in whatever
destination you configured". If canal wants better, the bounded queryable DLQ store must be designed
in from the start; bolting it on later is explicitly deferred as Tier-1 data-path work there.

---

## 10. Plugin boundary

**This is the highest-value section for canal**, because canal has committed to in-process Go
interfaces that must later admit an out-of-process implementation unchanged. Conduit has already done
exactly this, and it works.

### 10.1 Is the in-process interface literally the same interface the gRPC client implements?

**Yes.** Both `builtin` and `standalone` produce a `connector.Dispenser`, and the objects it dispenses
satisfy `connector.SourcePlugin` = `pconnector.SourcePlugin` + `NewStream()`. The engine has no
knowledge of which it holds. `pkg/connector/instance.go`:

```go
// PluginDispenserFetcher can fetch a plugin dispenser.
type PluginDispenserFetcher interface {
	NewDispenser(logger log.CtxLogger, name string, connectorID string) (connectorPlugin.Dispenser, error)
}
```

Selection happens by **name prefix** (`plugin.FullName` = `<type>:<name>@<version>`, with
`PluginTypeBuiltin` / `PluginTypeStandalone`), resolved in `pkg/plugin/connector/service.go`. Two
registries, one interface, dispatch by name — no switch in the data path.

The builtin adapter's own doc comment is the design rule, and canal should copy it verbatim into its
own in-process adapter (`pkg/plugin/connector/builtin/source.go`):

```go
// sourcePluginAdapter implements connector.SourcePlugin used internally in
// Conduit and relays the calls to a source plugin defined in
// conduit-connector-protocol (pconnector). This adapter needs to make sure it
// behaves in the same way as the standalone plugin adapter, which communicates
// with the plugin through gRPC, so that the caller can use both of them
// interchangeably.
// All methods of sourcePluginAdapter use runSandbox to catch panics and convert
// them into errors. The adapter also logs the calls and clones the request and
// response objects to avoid any side effects.
```

### 10.2 What they gave up to make it true

Four things, all deliberate, all paid **even in-process**:

**(a) Structs instead of arguments.** Every method is `(ctx, Req) (Res, error)`. Verbose; makes field
addition non-breaking; matches protobuf 1:1.

**(b) Typed errors degraded to strings.** `DestinationRunResponseAck.Error string`. The in-process path
does not exploit its ability to pass a real `error` — because then the two paths would differ.

**(c) Deep-copy on every hop.** In-process gets no zero-copy advantage
(`pkg/plugin/connector/builtin/stream.go`):

```go
	// We clone the data before sending it into the stream to avoid
	// sharing the same data between the server and the client.
	case s.reqChan <- req.Clone():
		return nil
```

with the requirement expressed as a type constraint:

```go
type inMemoryStreamClient[REQ cloner[REQ], RES any] inMemoryStream[REQ, RES]
type inMemoryStreamServer[REQ any, RES cloner[RES]] inMemoryStream[REQ, RES]
type cloner[T any] interface { Clone() T }
```

Every request/response type in `pconnector` has a `Clone()` (`pconnector/clone.go`). This is a genuine
performance sacrifice made *to preserve semantic equivalence* — and it is also a correctness win: it
prevents a builtin connector from mutating a record the engine still holds, which would be impossible
over gRPC and therefore must be impossible in-process too. **Generics used to enforce a semantic
requirement, not for their own sake.** Good use.

**(d) A channel stream that emulates gRPC error semantics, including `io.EOF`.**

```go
func (s *inMemoryStreamServer[REQ, RES]) Recv() (REQ, error) {
	select {
	case <-s.ctx.Done():   return s.emptyReq(), s.ctx.Err()
	case <-s.stopChan:     return s.emptyReq(), io.EOF
	case req := <-s.reqChan: return req, nil
	}
}
func (s *inMemoryStreamClient[REQ, RES]) Recv() (RES, error) {
	select {
	case <-s.ctx.Done():   return s.emptyRes(), s.ctx.Err()
	case <-s.stopChan:     return s.emptyRes(), s.reason // client receives the reason for closing
	case resp := <-s.respChan: return resp, nil
	}
}
```

Note the asymmetry, which mirrors gRPC exactly: the **server** side sees a bare `io.EOF` on close; the
**client** side sees the *reason* the stream was closed. The engine relies on this —
`isClosedSourceStream(err) { return cerrors.Is(err, io.EOF) }` in `source_acker.go` treats `io.EOF`
from `Source.Ack` as "the source already stopped, suppress this derived error so it can't mask the real
one" (issue #1659).

The `Client()`/`Server()` split on `SourceRunStream` is what makes one type serve both roles, with the
allowed caller documented on the interface itself:

```go
	// Client is only allowed to be used by the host (Conduit).
	Client() SourceRunStreamClient
	// Server is only allowed to be used by the plugin (connector).
	Server() SourceRunStreamServer
```

Enforced by convention, plus a runtime type check in the builtin adapter:

```go
	inmemStream, ok := stream.(*InMemorySourceRunStream)
	if !ok { return fmt.Errorf("invalid stream type, expected %T, got %T", s.NewStream(), stream) }
	if inmemStream.stream != nil { return fmt.Errorf("stream has already been initialized") }
	inmemStream.Init(ctx)
```

One asymmetry remains and is worth noting for canal: **`Run` is asynchronous in the builtin adapter**
(it spawns a goroutine and returns nil immediately, closing the stream with the run error when the
plugin's `Run` returns), whereas over gRPC `Run` establishes a stream and returns. Same observable
contract, different mechanics.

### 10.3 Out-of-process mechanics

`hashicorp/go-plugin` over gRPC, subprocess per connector instance.
`conduit-connector-protocol/pconnector/pconnector.go` — the entire file:

```go
var HandshakeConfig = plugin.HandshakeConfig{
	MagicCookieKey:   "CONDUIT_PLUGIN_MAGIC_COOKIE",
	MagicCookieValue: "204e8e812c3a1bb73b838928c575b42a105dd2e9aa449be481bc4590486df53f",
}
```

Discovery is **directory scan + execute**. `standalone/registry.go` reads `pluginDir`, runs each file,
and calls `Specify` to learn what it is:

```go
	specPlugin, err := dispenser.DispenseSpecifier()
	if err != nil {
		return pconnector.Specification{}, cerrors.Errorf("failed to dispense connector specifier (tip: check if the file is a valid connector plugin binary and if you have permissions for running it): %w", err)
	}
```

with an honest TODO on the blueprint:

```go
type blueprint struct {
	FullName      plugin.FullName
	Specification pconnector.Specification
	Path          string
	// TODO store hash of plugin binary and compare before running the binary to
	// ensure someone can't switch the plugin after we registered it
}
```

Configuration reaches the subprocess as **environment variables only** — the process gets a
deliberately empty env (`cmd.Env = make([]string, 0)`) plus:

```go
	return NewDispenser(
		logger.ZerologWithComponent(),
		bp.Path,
		client.WithEnvVar(pconnutils.EnvConduitConnectorUtilitiesGRPCTarget, r.connUtilsAddr),
		client.WithEnvVar(pconnutils.EnvConduitConnectorToken, cfg.Token),
		client.WithEnvVar(pconnector.EnvConduitConnectorID, cfg.ConnectorID),
		client.WithEnvVar(pconnector.EnvConduitConnectorLogLevel, cfg.LogLevel),
		client.WithEnvVar(pconnector.EnvConduitConnectorMaxReceiveRecordSize, strconv.Itoa(r.maxReceiveRecordSize)),
	)
```

and the same struct is passed *by value* to builtin plugins, so both modes read the same config:

```go
type PluginConfig struct {
	Token       string
	ConnectorID string
	LogLevel    string
	Grpc GRPCConfig
}
type GRPCConfig struct {
	ConnectorUtilitiesTarget string
	MaxReceiveRecordSize     int
}
```

Note there is a **reverse channel**: `connUtilsAddr` is Conduit serving gRPC *to* the connector, for
the schema service (§5.3) — so an out-of-process connector can still register schemas in the engine's
registry. Authenticated per connector by `Token`. Any canal design with a subprocess boundary needs
this reverse seam identified up front, because retrofitting host services into a one-way boundary is
painful.

The SDK's `Serve` errors helpfully when an env var is missing, naming the minimum host version:

```go
func missingEnvError(envVar, conduitVersion string) error {
	return fmt.Errorf(
		"%q is not set. This indicates you are using an old version of Conduit."+
			"Please, consider upgrading to at least %v. ...", envVar, conduitVersion)
}
```

### 10.4 Protocol versioning

Two versions coexist, negotiated by `go-plugin`'s `VersionedPlugins`
(`pconnector/client/client.go`):

```go
// New creates a new plugin client. Path should point to the plugin
// executable. The client will support both v1 and v2 of the connector protocol.
	clientConfig := &plugin.ClientConfig{
		HandshakeConfig: pconnector.HandshakeConfig,
		VersionedPlugins: map[int]plugin.PluginSet{
			v1.Version: newPluginSet(clientv1.NewSpecifierPluginClient, clientv1.NewSourcePluginClient, clientv1.NewDestinationPluginClient),
			v2.Version: newPluginSet(clientv2.NewSpecifierPluginClient, clientv2.NewSourcePluginClient, clientv2.NewDestinationPluginClient),
		},
		Cmd: cmd,
		AllowedProtocols: []plugin.Protocol{ plugin.ProtocolGRPC },
		...
	}
```

with

```go
// Deprecated: v1 is deprecated. Use v2 instead.
package v1
const Version = 1
```
```go
package v2
const Version = 2
```

The whole versioned surface is duplicated per version: `pconnector/v1/{client,server,toproto,fromproto}`
and `pconnector/v2/{...}`, three plugin kinds each (`"specifier"`, `"source"`, `"destination"`). The
host supports N versions simultaneously; the plugin declares one; `go-plugin` picks the highest common
one during handshake. Mismatch = handshake failure at startup, before any data moves.

**The cost is visible and large**: 24 files of `v1/` + `v2/` conversion code for one protocol bump. The
benefit is that an old connector binary keeps working against a new engine with zero recompilation.
Inside a major version, compatibility is maintained by proto rules — the partition-claims RFC states
the discipline explicitly:

> All changes below are new fields (fresh field numbers) on existing messages, one new optional RPC,
> and new top-level messages. **Nothing existing is renamed, renumbered, or removed**

For canal's in-process-now/out-of-process-later plan, the transplantable lesson is: **define the
boundary types as if they were protobuf messages from day one** (structs of scalars/slices/maps, no
interfaces, no `error` fields, no funcs, `Clone()` on everything), and put a version constant next to
them even while there is only one. Then the "later" is mechanical.

### 10.5 The WASM path forks the interface

Connectors: builtin + standalone-subprocess, both `pconnector`. **Processors** additionally have a
WASM path (`pkg/plugin/processor/standalone/`, with `host_module.go`, `wasm/caller.go` in the
processor SDK). It does **not** share the connector protocol; it is a separate host-module ABI over
wazero.

And it has been walked back:
`docs/architecture-decision-records/20260722-wasm-component-model-deferred.md` defers the WASM
Component Model. `docs/design-documents/20260726-wasm-host-egress-capability.md` (55 KB) exists because
a sandboxed processor that needs network access needs a whole capability system (there is a
`pkg/plugin/processor/egress/` package with `ipguard.go`, `policy.go`, `secret.go`). The lesson is not
"don't do WASM" — it is that **a sandbox boundary is not just a serialization boundary**; every
capability the plugin needs has to be re-granted explicitly, and that cost is large enough to be its
own design doc.

Note also `docs/architecture-decision-records/20260727-processors-ride-connector-registry.md` —
processors were folded into the connector registry rather than getting their own, i.e. one distribution
mechanism for both plugin kinds. canal should decide this once, early.

---

## 11. Observability

### 11.1 Metrics

`pkg/foundation/metrics/measure/measure.go` is the single registration point ("Any changes in metrics
defined below should also be reflected in the metrics documentation"), backed by a tiny facade
(`metrics.NewLabeledGauge/Counter/Histogram/Timer`) with `prometheus` and `noop` implementations.
Exposed at `GET /metrics`.

Labels are declared as constants precisely to avoid drift:

```go
const (
	labelPipelineName = "pipeline_name"
	labelType         = "type"
	labelPlugin       = "plugin"
	// labelComponentID is the ID of the pipeline component (connector or
	// processor) a metric belongs to. It uses the same ID space as the
	// topology nodes and as the existing conduit_inspector_sessions /
	// conduit_inspector_dropped_records_total metrics [...] so per-node
	// dashboards can join across all of these metrics on this label.
	labelComponentID = "component_id"
)
```

The exact set, with exact names and label sets:

| Metric | Type | Labels |
| --- | --- | --- |
| `conduit_info` | gauge | `version` |
| `conduit_pipelines` | gauge | `status` |
| `conduit_pipeline_status` | gauge | `pipeline_name` |
| `pipeline_recovering_count` | counter | `pipeline_name` |
| `conduit_connectors` | gauge | `type` |
| `conduit_processors` | gauge | `type` |
| `conduit_inspector_sessions` | gauge | `component_id` |
| `conduit_inspector_dropped_records_total` | counter | `component_id` |
| `conduit_connector_bytes` | histogram | `pipeline_name`, `plugin`, `type`, `component_id` |
| `conduit_dlq_bytes` | histogram | `pipeline_name`, `plugin` |
| `conduit_pipeline_execution_duration_seconds` | timer/histogram | `pipeline_name` |
| `conduit_connector_execution_duration_seconds` | timer/histogram | `pipeline_name`, `plugin`, `type`, `component_id` |
| `conduit_processor_execution_duration_seconds` | timer/histogram | `pipeline_name`, `processor`, `component_id` |
| `conduit_dlq_execution_duration_seconds` | timer/histogram | `pipeline_name`, `plugin` |

Byte buckets are `1 KiB … 2 MiB` (powers of two); duration buckets are
`.001 .0025 .005 .01 .025 .05 .1 .25 .5 1 2.5 5` seconds.

Observations worth carrying into canal:

- **Granularity is per-pipeline × per-plugin × per-connector-instance.** `component_id` is the
  connector/processor *instance* id and — critically — **the same id space as the topology nodes and
  the inspector**, so a UI can join throughput, latency and live-record-stream on one key. Deciding
  that id space once, and using it everywhere, is the whole trick.
- **There is no record *count* metric.** Not one. Throughput must be derived from
  `conduit_connector_bytes_count` (the histogram's implicit `_count`), which conflates "records
  observed" with "records sized". `docs/metrics.md`'s own worked example of adding a metric is
  literally "count messages per pipeline" — i.e. the docs teach you to add the metric that should have
  existed.
- **`conduit_pipelines{status}` vs `conduit_pipeline_status{pipeline_name}`** — the first is a
  histogram-of-fleet ("how many pipelines are degraded"), the second is per-pipeline state as a
  number. Both are needed and they are different questions. Good.
- The naming is inconsistent: `pipeline_recovering_count` is missing the `conduit_` prefix and the
  `_total` suffix, while a newer counter carries a comment explaining that it fixed the convention
  rather than matching the neighbours:
  ```go
  	// _total suffix per Prometheus counter convention; this is a new metric with
  	// no consumer yet, so the name is fixed correctly now rather than matching the
  	// repo's older, suffix-less counters.
  ```
  A metric name is a public API. Get the convention right in the first commit.
- Adding a label to an existing metric **resets the series**, and `docs/metrics.md` says so plainly:
  > Because a new label starts a new Prometheus series, upgrading Conduit resets the
  > counters/histograms these three metrics contribute to.

  That is an operator-visible breaking change dressed as an additive one. canal should over-provision
  labels (including `component_id`) from the first release.
- Bytes are measured on the **JSON representation** of the record payload and key
  (`docs/metrics.md`), not on the wire encoding. So `conduit_connector_bytes` is a proxy for record
  size, not for network volume.

### 11.2 The status model, and the "up vs moving" distinction

Pipeline status is a small closed enum (`pkg/pipeline/instance.go`):

```go
const (
	StatusRunning Status = iota + 1
	StatusSystemStopped
	StatusUserStopped
	StatusDegraded
	StatusRecovering
)
```

Five states, and the useful discrimination is real: `SystemStopped` vs `UserStopped` distinguishes
"the engine stopped this" from "an operator stopped this", and `Degraded` vs `Recovering` distinguishes
"failed and giving up" from "failed and retrying with backoff". `Instance.Error string` carries the
cause. `pipeline_recovering_count` counts flaps.

Connector-level state is a separate, richer enum inside the SDK adapter
(`internal.ConnectorState`: `Initial → Configuring → Configured → Starting → Started →
InitiatingRun → Running → InitiatingStop → Stopping → Stopped`, plus `TearingDown → TornDown` and
`Errored`) — but **it is not exposed to the API**. The API surfaces pipeline status; the connector's
state machine is internal.

So, on canal's key question — does the model distinguish *the connector is up* from *data is moving*?
**No, not directly.** `StatusRunning` means "the pipeline's goroutines are alive". A source that is
connected and returning `ErrBackoffRetry` forever, or blocked on an unbuffered channel behind a wedged
destination, is `Running`. Detecting stall requires the operator to write a rate query over
`conduit_connector_bytes_count` themselves, and `conduit_pipeline_execution_duration_seconds` only
records records that *completed* — a fully-stalled pipeline emits no samples at all, so the histogram
is silent rather than alarming.

Conduit's fleet UI epic (#2624) hits this from the other side and had to add engine work for it:

> **P3** `stopReason`/`stopped_by` additive field on pipeline state → gates fleet-view
> "operator-stopped vs engine-restarted" accuracy

**For canal: put a monotonic `records_in`/`records_out` counter and a `last_record_at` timestamp per
connector instance in the status model from day one.** "Up" and "moving" are different facts and a
metrics UI that blurs them is worse than no UI. Neither can be reconstructed later from histograms.

### 11.3 What a UI can read

- `GET /metrics` — Prometheus.
- `GET /healthz` — `pkg/http/api/health_server.go`; `pkg/conduit/readyz.go` for readiness.
- gRPC + a grpc-gateway HTTP/JSON mirror + OpenAPI (`pkg/http/api/*_v1.go`, `pkg/http/openapi/`), with
  a websocket bridge (`pkg/foundation/grpcutil/websocket.go`) for streaming endpoints.
- **The stream inspector** — `pkg/inspector/inspector.go`. Per-component live tap on the actual records
  flowing through a connector or processor:
  ```go
  func (i *Instance) Inspect(ctx context.Context) *inspector.Session
  ```
  Sessions are buffered and **drop on full** rather than applying backpressure — which is why
  `conduit_inspector_dropped_records_total` had to be added (#2628, "Inspector drops-on-full silently
  today"). An observability tap that can silently lie about completeness is worse than one that
  refuses; metering the drops is the minimum fix.
- Errors on the wire carry structured detail: a stable `reason` code plus optional `configPath`,
  `suggestion`, `fix` (§6.5c), so a UI can render "this field is wrong, here's the fix" rather than a
  stack trace.
- Config values are **redacted at the API boundary** (`pkg/http/api/toproto/redact.go`) — with a known
  hole: conduit#2640, *"GetDLQ returns connector Settings unredacted — potential secret exposure via
  API"*. Same root cause as §7.2: no per-parameter sensitivity flag, so redaction is applied per
  call-site and one gets missed.

---

## 12. Deployment

### 12.1 Single-node by design, and they mean it

There is no clustering, no leader election, no work assignment, and no rebalancing. `db.type` accepts
`badger,postgres,inmemory,sqlite` and there is not one config key mentioning cluster, node, member,
lease, or replica. This is not an omission — it is a written, accepted decision.
`docs/architecture-decision-records/20260704-single-node-engine.md`:

> The Conduit engine is single-node by design. It will never grow cluster-membership protocols, leader
> election, gossip, or consensus (Raft/etcd). Distribution — running many pipelines across many
> instances, or one hot pipeline across several — is a scheduling concern solved a layer above the
> engine (Kubernetes operator / control plane), not inside it.

and the reasoning, which is a direct read on the closest prior art:

> Data-integration systems that put distribution _inside_ the engine pay for it forever. Kafka
> Connect's rebalance protocol is the canonical example: worker-cluster membership and task
> rebalancing are a persistent source of operational pain, stop-the-world pauses, and subtle
> correctness bugs.

with an enforcement clause:

> **No membership, no leader election, no gossip, no consensus in the engine.** Well-meaning clustering
> PRs are rejected and pointed at this ADR. If a design doc starts growing engine-level rebalancing,
> that is a stop-and-flag signal.

### 12.2 The three-level scale-out model

Stated in the same ADR:

1. **Level 1 — fleet sharding.** "A scheduler bin-packs pipelines onto instances. 'Who runs what' state
   lives in the control plane (Postgres- or Kubernetes-lease-backed), not in the engine." No engine
   change at all. The pre-scheduler version of this is static assignment:
   `conduit run --pipelines <dir>` + Helm.
2. **Level 2 — hot-pipeline parallelism via partition claims.** "Sources declare their partitionable
   units in the connector protocol; the scheduler assigns claims to instances." §12.3.
3. **HA — active/passive, resume-from-checkpoint.** "An instance dies, the scheduler reassigns its
   pipelines, and each pipeline resumes from its last checkpoint. This is correct by construction given
   the data-integrity invariants (crash-safe positions, atomic checkpoints, at-least-once). No
   consensus and no warm standbys in v1."

The consequence they accept: "static pipeline-to-instance assignment (Helm, `conduit run --pipelines
<dir>`) must cover users well before the operator exists."

**This is the most directly transplantable strategic decision in the entire dossier for canal**, which
wants both an enterprise multi-worker deployment and a standalone single binary from one codebase.
Conduit's answer is that those are *the same binary with nothing added* — the difference is entirely
outside the process. HA correctness "reduces to the data-integrity invariants rather than a bespoke
consensus implementation we would have to prove correct."

### 12.3 The seam they shipped early: partition claims

The ADR's key procedural insight: *"The connector protocol must carry a partition-claim concept earlier
than the scheduler needs it, to avoid a later breaking change."* So the RFC
(`docs/design-documents/20260723-partition-claims-protocol.md`) reserves wire vocabulary years before
the consumer exists. Its self-description:

> This RFC is that seam. It defines, and only defines:
> 1. What a **partitionable unit** is, per connector kind [...]
> 2. How a source **declares** the units it can offer, additively [...]
> 3. How a **future** scheduler (not designed here [...]) would consume that declaration [...]
> 4. The failure modes this seam must survive with **zero scheduler code running**, because the wire
>    fields exist starting the release this lands, and must be safe to leave dormant for however many
>    releases pass before a scheduler consumes them.

The capability enum, per connector kind:

| Connector kind | Partitionable unit | Mode |
| --- | --- | --- |
| Kafka (consumer-group source) | topic-partition | `MODE_EXTERNALLY_MANAGED` |
| Postgres CDC (logical replication) | table (or table-group) | `MODE_STATIC` |
| File/S3 | file or object-prefix shard | `MODE_DYNAMIC` |
| `generator`, non-partitionable | none | `MODE_UNSPECIFIED` |

```proto
message PartitioningCapability {
  enum Mode {
    MODE_UNSPECIFIED = 0;         // fallback: today's single-instance model, unchanged
    MODE_STATIC = 1;              // units fixed at Configure time
    MODE_DYNAMIC = 2;             // units can change at runtime; engine must re-poll
    MODE_EXTERNALLY_MANAGED = 3;  // units exist, but an external system (e.g. Kafka) already
                                  // assigns them
  }
  Mode mode = 1;
  string unit_label = 2; // human-readable, e.g. "kafka topic-partition", "postgres table"
}

message PartitionUnit {
  string id = 1;             // stable, connector-assigned identifier
  bytes opaque_metadata = 2; // connector-defined; round-tripped back inside PartitionClaim
}

message PartitionClaim {
  string unit_id = 1;
  bytes position = 2;  // per-unit resume position, connector-defined encoding (opaque, same
                       // contract as today's Open.Request.position, just scoped to one unit)
  uint64 epoch = 3;    // fencing token
}
```

Four properties worth copying exactly:

- **Zero value = today's behaviour.** `MODE_UNSPECIFIED = 0` and *"Empty [`claimed_units`] = 'claim
  everything' — the fallback that preserves today's single-instance behavior for every
  MODE_UNSPECIFIED connector and for any engine build that predates this RFC."*
- **`MODE_EXTERNALLY_MANAGED` is the humility slot.** Kafka already solves assignment; the engine must
  *not* try. Without that mode, a partition-aware engine would fight the broker.
- **Per-unit position is the same opaque-bytes contract, just scoped smaller.** The position model
  scales down from connector to unit with no new concept.
- **The Go-side declaration is an optional interface, type-asserted, never a required method:**
  ```go
  // PartitionAwareSource is an optional interface a Source may implement to declare
  // partitionable units. A Source that does not implement it is treated as
  // MODE_UNSPECIFIED automatically -- zero behavior change, zero required migration.
  type PartitionAwareSource interface {
      Source
      DeclareClaimableUnits(context.Context) ([]PartitionUnit, error)
  }
  ```
  > `NewSourcePlugin` type-asserts `impl.(PartitionAwareSource)`; existing `Source` implementations need
  > no changes at all, in perpetuity, unless they opt in.

  **This is the capability-negotiation pattern canal should adopt for every optional capability** —
  including the batching one Conduit got wrong via `ErrUnimplemented` (§8.1). The RFC names the
  precedent: *"the same duck-typed capability-negotiation idiom Go already uses (`io.ReaderFrom`,
  `sql.Scanner`)."*

The RFC is also honest that reserving a shape is not proving a design:

> invariant 4 (ordering) holds today for one reason only: the seam is inert until a consumer exists,
> not because this RFC has proven a multi-instance implementation correct.

and it flags `epoch` as **not yet decided** — committing to the field's shape while explicitly not
committing to a fencing implementation. Reserving a field you might not use is cheap; renumbering is
not.

### 12.4 State store and single-binary story

One embedded store, four backends, selected by `db.type`
(`badger` default-ish, `sqlite`, `postgres`, `inmemory`); interface in
`conduit-commons/database/database.go` with a `NewTransaction(ctx, update bool)` returning a
`Transaction` (`Commit`/`Discard`). Positions, connector instances, pipelines and processors all live
there.

The single-binary claim is real: `cmd/conduit` embeds the engine, the built-in connectors, the
processors, the HTTP+gRPC API, and (per `pkg/web/ui/ui.go` + `docs/design-documents/20260713-greenfield-built-in-ui.md`)
a `go:embed`ed React UI. `inmemory` DB + `--pipelines <dir>` is the local-dev mode; `sqlite`/`badger`
is single-node prod; `postgres` is what makes several instances share a control-plane store *without*
the engine becoming distributed.

`docs/architecture-decision-records/20260704-local-state-only.md` keeps the same line for stateful
processing:

> State is a local embedded KV store, checkpointed atomically with the pipeline. Recovery is
> resume-from-checkpoint [...] **Explicitly out of scope:** distributed snapshots,
> pluggable/tunable state backends, event-time watermarks, large distributed joins, late-data
> reprocessing frameworks.

> The temptation is to keep adding "just one more" stateful feature until Conduit is a worse Flink.

### 12.5 Recovery within one node

`pkg/lifecycle/service.go`:

```go
const InfiniteRetriesErrRecovery = -1

type ErrRecoveryCfg struct {
	MinDelay         time.Duration
	MaxDelay         time.Duration
	BackoffFactor    int
	MaxRetries       int64
	MaxRetriesWindow time.Duration
}
```

Exponential backoff per pipeline, with the sliding-window detail that matters:

```go
	// This results in a default delay progression of 1s, 2s, 4s, 8s, 16s, [...], 10m, 10m,...
	...
	time.AfterFunc(duration+s.errRecoveryCfg.MaxRetriesWindow, func() {
		rp.recoveryAttempts.Add(-1) // Decrement the number of attempts after delay.
	})
```

The attempt counter **decays**, so a pipeline that flaps once a day never exhausts its retries, while
one that flaps in a tight loop does. Exhaustion is fatal:

```go
	if s.errRecoveryCfg.MaxRetries != InfiniteRetriesErrRecovery && attempt > s.errRecoveryCfg.MaxRetries {
		return cerrors.FatalError(cerrors.Errorf("failed to recover pipeline %s after %d attempts: %w",
			rp.pipeline.ID, attempt, pipeline.ErrPipelineCannotRecover))
	}
```

Backoff state (`backoff`, `recoveryAttempts`) is deliberately carried across restarts of the same
pipeline (`rp.backoff = oldRp.backoff`), so a restart does not reset the flap detector.

---

## 13. What they got right / what they got wrong

### 13.1 Got right

**1. The three-layer interface split (§1).** Author-facing ergonomic interface → protobuf-shaped
boundary → engine-facing dispenser. It is the reason in-process and subprocess connectors are
interchangeable *and* the reason connector authors don't write request structs. Canal's constraint #3
is satisfiable, and this is the proof.

**2. `mustEmbedUnimplementedX()` (§1.1).** An unexported method makes embedding the default struct
mandatory at compile time, which makes the interface additive-safe forever. Nine methods on `Source`
and adding a tenth breaks nobody.

**3. `Position []byte` with the engine forbidden from parsing it (§3.1).** And when the engine needed
ordering, it minted its own `nextAckSeq` rather than imputing structure to the bytes. The comment
explaining why is better documentation than most projects' architecture guides.

**4. `snapshot` as an `Operation`, and snapshot-mode as a tag inside the opaque position (§4).** The
entire snapshot-then-CDC problem is solved with **zero core surface area**. No `Snapshotter` interface,
no phase in the pipeline lifecycle, no capability bit. A source with no snapshot concept is unaffected.

**5. `Data` as a sealed two-member interface (§2.3).** `isData()` closes the set; consumers type-switch;
no lossy auto-conversion.

**6. Struct-tag → generated spec → embedded YAML, with the Go doc comment as the description (§7.5).**
Documentation cannot drift from the spec because it *is* the spec. `./connector spec` prints it without
gRPC.

**7. Validations as serialisable objects (§7.3).** Six kinds, each a two-string tuple on the wire and a
`Validate(string) error` in Go. Same rules enforced in the browser and the engine, with no per-connector
frontend code.

**8. The seven data-integrity invariants, cited at enforcement sites (§9.1).** `// Invariant 1: ...`
next to the code that upholds it. This is how you keep a correctness property alive across refactors by
people who weren't there.

**9. Making the in-process path *worse* on purpose (§10.2).** Deep-cloning every request/response and
degrading errors to strings in-process, so the two transports cannot diverge. Also prevents a builtin
connector from mutating engine-held records.

**10. The optional-interface capability pattern (§12.3).** `PartitionAwareSource` type-asserted, absence
= default behaviour "in perpetuity". Compare to §8.1's `ErrUnimplemented` negotiation, which is the same
project's earlier attempt at the same problem, done badly.

**11. Reserving protocol vocabulary before the consumer exists (§12.3), and writing down what you will
never build (§12.1).** "Well-meaning clustering PRs are rejected and pointed at this ADR" is a
maintainable scope boundary.

**12. `AcceptanceTest(t, driver)` — a conformance suite the *framework* owns (§13.3 below).**

**13. Fail-loud defaults.** `WindowNackThreshold: 0` means the first nack stops the pipeline. The
ack-ordering node latches broken on the first out-of-order ack. Connector contract violations produce
"this is probably a bug in the connector" errors instead of silent corruption.

**14. The `sdk.*` config namespace.** Every framework-provided knob is `sdk.batch.size`,
`sdk.schema.extract.payload.enabled`, `sdk.rate.perSecond` — reserved prefix, no collision with
connector-owned keys, uniformly discoverable in the generated spec.

### 13.2 Got wrong — with sources

**a. `Read` *and* `ReadN`, negotiated by catching `ErrUnimplemented` (§8.1).** Batching was retrofitted
onto a per-record interface. Both methods are permanently on `Source`; the SDK calls `ReadN` first and
falls back on a sentinel error. Documented consequence — conduit-connector-sdk#248,
*"sourceWithEncoding middleware works impropertly in combination with sourceWithBatch"*:

> `sourcePluginAdapter` in `runRead` method first tries to call `ReadN` method and if it fails with
> Unimplemented error, it fallbacks on `Read` method. If Source implementation supports `ReadN` method,
> this method will be called instead of `Read` and the result will not be encoded propertly.

Two other batch-middleware bugs in the same repo: #275 *"Fix issue with closing channel in
SourceWithBatch"* (a `panic: send on closed channel` in `source_middleware.go:777` in production), and
#309 *"Warn if one of sdk.batch.size or sdk.batch.delay is set to 0"* — the fix for which is a **log
warning** rather than a validation:

```go
	if *s.config.BatchSize > 0 && *s.config.BatchDelay == 0 {
		Logger(ctx).Warn().
			Msg("sdk.batch.size is set but sdk.batch.delay is not, this might result in the connector waiting indefinitely for the batch to be filled")
	}
```

**Lesson for canal: batch is the primitive; single-record is the degenerate case (n=1). Never both.**

**b. Two pipeline engines in the tree, for years.** `pkg/lifecycle` (default) and `pkg/lifecycle-poc`
(preview). From the ADR that finally made it a decision:

> v2 was introduced as a preview in #1913 with **no ADR, no completion criteria, and no committed
> benchmark**. That left it an open question that silently taxes every data-path change: the metrics fix
> (#2268), the SIGTERM graceful-shutdown work, and the force-stop ack-safety follow-up (#2519) each have
> to be reasoned about in both implementations. v2 is also **incomplete** — its own flag help states it
> "currently supports only 1 source and 1 destination per pipeline."

The v1 architecture's measured cost is ~69 allocations and ~2.5 KB **per record** (§8.4). The v2 rewrite
is ~6.3× better and still not default. canal gets this for free by choosing batch-first on day one.

**c. Ack-before-persist: a confirmed sev-0 data-loss mechanism that survived to 2026 (§3.4).**
`docs/postmortems/20260723-source-ack-persist-ordering.md`:

> **Severity: sev-0** (confirmed data-loss class) [...] **Status: latent, confirmed, not yet fixed.**
> [...] The bug predates this repo having any chaos-testing infrastructure at all; it was found by the
> first test ever written to look for it.

The interesting part is *why* it survived: the guarantee depends on a property of the **upstream**
(prune-on-commit vs retention-based) that the interface does not express. Postgres slots prune on ack;
Kafka, MySQL binlog and Mongo change streams do not. The same engine code is a data-loss bug for one
class and benign for the other, and nothing in `Source` distinguishes them. The postmortem had to
classify each connector **by reading its source**.

**Lesson for canal: if a guarantee depends on connector semantics, the interface must let the connector
declare those semantics.** A `Source` capability like "my Ack drives an irreversible upstream commit"
would have made this a typed contract instead of an archaeology exercise.

**d. The fix's own follow-on bug (§4.4).** Deferring the ack until durable-persist created a deadlock:
`docs/postmortems/20260729-snapshot-handoff-deferred-ack-deadlock.md`. A snapshot-gating source blocks
its own completion on the boundary ack, so a dropped deferred ack became "a permanent handoff deadlock +
silent post-snapshot CDC loss". Fixing it required a per-source delivery goroutine, a bounded retry
policy, a `tearingDown` flag to suppress escalation during teardown, and ~200 lines of comments
explaining lock ordering. **Two postmortems, six days apart, on the same seam.** That is what
retrofitting an ordering guarantee into an existing ack path costs.

**e. Partial-batch failure is prefix-only, with one shared error string (§9.3).** `(n int, err error)`
cannot say "3 and 7 failed". Every nacked record gets the same error, including unattempted ones. And
`DestinationRunResponseAck.Error string` throws away the type. v2's `Batch` has the right shape
internally (per-record `RecordFlag` + per-record errors) but the **boundary** type still flattens it, so
the fidelity dies at the wire regardless.

**f. `Record.serializer` — a private mutable field on a value type (§2.5).** Documented "not
thread-safe", not copied by `Clone()`, and its error path silently falls back to a different output
format. Codec choice belongs in pipeline config, never on the record.

**g. `opencdc.file.*` in the shared record spec (§2.4).** Six reserved metadata keys about *file
chunking* (`file.name`, `file.size`, `file.hash`, `file.chunked`, `file.chunk.index`,
`file.chunk.count`) in the connector-agnostic envelope, added by conduit-commons#201. This is exactly
the "source-shaped assumption leaking into core" that canal's constraint #1 forbids. The right home was
a connector-namespaced key, like Postgres's own `postgres.snapshot.resumed` (§4.5) — which the same
organisation got right, later, in a different repo.

**h. No sensitivity flag on `Parameter` (§7.2), worked around at every call site.** Hence
`log.RedactAll(in.Config)` with the comment *"There is no per-parameter sensitivity metadata yet, so
`log.RedactAll` redacts every value"*, hence a separate `pkg/http/api/toproto/redact.go`, hence
conduit#2640: *"GetDLQ returns connector Settings unredacted — potential secret exposure via API"*. One
missing boolean in a spec type became a security bug class.

**i. No per-connector-facing error taxonomy (§6.5).** `ErrBackoffRetry` and `ErrUnimplemented` are the
entire vocabulary. A connector cannot distinguish "retry me" from "this record is poison, DLQ it" from
"my credentials are wrong, stop". Backoff parameters are hardcoded with a four-year-old TODO pointing at
conduit#184. Same for the one-minute stop/teardown timeouts (`TODO make the timeout configurable`,
conduit#183).

**j. The `unwrap` processor family is an admission (§2.3, §9.4).** `pkg/plugin/processor/builtin/impl/unwrap/`
contains `opencdc.go`, `debezium.go`, `kafka_connect.go` — three processors whose only job is to
un-nest a record that arrived as bytes containing an encoded record. This happens because
`Data` never auto-converts, because the DLQ re-wraps the original under `payload.after`, and because
records cross format boundaries (Kafka ↔ Conduit ↔ Debezium). Necessary given the design, but each one
is a place where the envelope failed to be self-describing.

**k. No record-count metric (§11.1), and no stall detection (§11.2).** Throughput must be derived from a
byte histogram's `_count`. `StatusRunning` cannot distinguish "up" from "moving". The fleet UI epic had
to add engine fields (`stopReason`/`stopped_by`) before it could tell operator-stopped from
engine-restarted.

**l. The inspector dropped records silently until 2026** (#2628 / `conduit_inspector_dropped_records_total`).
A live-tap that can lie about completeness.

**m. DLQ content is not queryable (§9.4, conduit#2639).** The DLQ is a destination, so the engine keeps
no copy. A real store is deferred as Tier-1 work needing its own ADR.

**n. Unresolved since 2023: conduit#859, "Bug: Possible race condition in destination"** — open, with a
precise mechanism described (a `DestinationNode` write racing a `DestinationAckerNode` teardown after a
nack with no DLQ configured). Still open at this commit. The `teardown` function in
`destination_acker.go` carries a hand-rolled mitigation goroutine "in case the destination plugin fails
to write a record and returns a nack while the pipeline doesn't have a nack handler configured [...]
which can cause a deadlock in the destination plugin".

**o. Protocol versioning is honest but expensive (§10.4).** 24 files of `v1/`+`v2/` conversion code, all
duplicated, to support one protocol bump.

**p. WASM was walked back (§10.5).** `20260722-wasm-component-model-deferred.md`, and a 55 KB design doc
for host egress capability. The sandbox boundary is not just a serialization boundary.

**q. Doc drift in the very place that generates docs.** `conn-sdk-cli/specgen/README.md` shows
`sdk.YAMLSpecification(specs)`; the actual signature is `YAMLSpecification(rawYaml, version string)`.
Small, but it is in the tool whose purpose is preventing drift.

### 13.3 One thing that is neither, and is worth more than most of the above

`conduit-connector-sdk/acceptance_testing.go` (39 KB) is a **conformance suite the framework owns and
every connector runs**:

```go
// AcceptanceTest is the acceptance test that all connector implementations
// should pass. It should manually be called from a test case in each
// implementation:
//
//	func TestAcceptance(t *testing.T) {
//	    sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
//	        Config: sdk.ConfigurableAcceptanceTestDriverConfig{
//	            Connector: myConnector,
//	            SourceConfig: config.Config{...},      // valid source config
//	            DestinationConfig: config.Config{...}, // valid destination config
//	        },
//	    })
//	}
func AcceptanceTest(t *testing.T, driver AcceptanceTestDriver)
```

The connector author implements a **driver**, not tests:

```go
type AcceptanceTestDriver interface {
	Context() context.Context
	Connector() Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig(*testing.T) config.Config
	DestinationConfig(*testing.T) config.Config

	BeforeTest(*testing.T)
	AfterTest(*testing.T)
	GoleakOptions(*testing.T) []goleak.Option

	// GenerateRecord will generate a new Record for a certain Operation. [...]
	// The generated record will contain mixed data types in the field Key and
	// Payload (i.e. RawData and StructuredData), unless configured otherwise
	GenerateRecord(*testing.T, opencdc.Operation) opencdc.Record

	// WriteToSource receives a slice of records that should be prepared in the
	// 3rd party system so that the source will read them. [...]
	WriteToSource(*testing.T, []opencdc.Record) []opencdc.Record
	// ReadFromDestination should return a slice with the records that were
	// written to the destination. [...]
	ReadFromDestination(*testing.T, []opencdc.Record) []opencdc.Record

	ReadTimeout() time.Duration
	WriteTimeout() time.Duration
}
```

`ConfigurableAcceptanceTestDriver` is a ready-made implementation for the common case. The tests
themselves cover the contract points a connector author would never think to test:

```
TestSpecifier_Exists                        TestSpecifier_Specify_Success
TestSource_Parameters_Success               TestSource_Configure_Success
TestSource_Configure_RequiredParams
TestSource_Open_ResumeAtPositionSnapshot    TestSource_Open_ResumeAtPositionCDC
TestSource_Read_Success                     TestSource_Read_Timeout
TestDestination_Parameters_Success          TestDestination_Configure_Success
TestDestination_Configure_RequiredParams    TestDestination_Write_Success
```

Note `TestSource_Open_ResumeAtPositionSnapshot` **and** `..._ResumeAtPositionCDC` — the framework tests
mid-snapshot resume and mid-CDC resume separately, because §4 showed they are different code paths.
Also `GoleakOptions` — goroutine-leak checking is on by default and a connector must *justify* an
exemption. That is how you make "implement the interface, register it, done" actually mean "done".

Note also what it does **not** cover: crash safety. That gap is filled at the *engine* level by
`tests/chaos` (`harness.go`, `child.go`, `upstream.go`) — SIGKILL/SIGTERM mid-batch and mid-checkpoint,
asserting invariants 1–7 on recovery, and it is a **graduation criterion** for the v2 engine. The two
suites are complementary and canal needs both: a per-connector conformance suite the framework owns,
and an engine-level kill-test suite.

---

## 14. Steal this

Ordered by value to canal. Each is one sentence plus the section to read.

1. **Three interface layers: ergonomic author interface, protobuf-shaped boundary struct interface,
   engine dispenser interface** — this is what makes in-process-now/out-of-process-later real rather
   than aspirational (§1, §10.1).
2. **`mustEmbedUnimplementedX()`**: an unexported no-op method on every core interface, forcing authors
   to embed a defaults struct, so canal can add interface methods forever without breaking a single
   connector (§1.1).
3. **Design boundary types today as if they were protobuf messages**: `(ctx, Req) (Res, error)` even for
   empty requests, only scalars/slices/maps in the structs, `Clone()` on every one, no `error`-typed
   fields — then the gRPC implementation is mechanical (§1.3, §10.4).
4. **Make the in-process path deliberately no faster than the wire path** — deep-clone on every hop,
   flatten errors to strings — so the two transports cannot semantically diverge, and enforce the clone
   with a `cloner[T]` type constraint (§10.2).
5. **`Position []byte`, and forbid the core from parsing, ordering or comparing it**; when the core needs
   ordering, mint an internal monotonic sequence instead (§3.1).
6. **Position format contract: additive-only JSON, a `version` field where 0 means legacy, never reject a
   *newer* version, stamp the version at serialize time** — this closes canal's open decision #9 (§3.2).
7. **Model snapshot as an `Operation` value plus a mode tag inside the connector's opaque position, with
   an in-`Read` sentinel-error handoff** — snapshot-then-CDC then costs the core exactly zero surface
   area (§4).
8. **Batch is the primitive; single-record is n=1.** Never ship both `Read` and `ReadN` — Conduit's
   dual-method runtime negotiation caused three separate SDK bugs and a ~6× allocation penalty in the
   engine it was retrofitted into (§8.1, §8.4, §13.2a).
9. **Per-record outcome flags on a batch — `Ack` / `Nack(err)` / `Retry` / `Filter`, each with its own
   error — expressed in the *boundary* type, not only in the engine's internals**, so partial batch
   failure never collapses to a prefix with one shared error string (§9.3).
10. **Snapshot `positions` at batch construction and ack against those**, so a transform chain that
    mutates `Record.Position` cannot make records unackable (§9.3).
11. **Three-phase ack: engine persists on downstream ack, then tells the connector only after the write
    is confirmed durable** — and put the deferred upstream-ack on its own bounded-retry goroutine so a
    slow connector can't stall the global flush (§3.4).
12. **Let a source declare whether its ack drives an irreversible upstream commit** (prune-on-commit vs
    retention-based); Conduit's sev-0 existed precisely because that semantic was invisible to the
    interface (§13.2c).
13. **Optional capabilities as separate interfaces, type-asserted once at construction, absence meaning
    "default behaviour in perpetuity"** — never by calling a method and inspecting the error (§12.3,
    contrast §8.1).
14. **Write the seven-ish data-integrity invariants down, then cite them in code at the enforcement
    site** (`// Invariant 3: ...`), so the property survives refactors by people who weren't there
    (§9.1).
15. **Config: struct tags (`json` + `default` + `validate`) as single source of truth, generated into an
    embedded machine-readable spec, with the Go doc comment becoming the parameter description** — docs
    cannot drift because they *are* the spec (§7.5).
16. **Validations as serialisable `{type, value}` objects with a `Validate(string) error` method**, so
    the browser and the engine enforce identical rules with no per-connector frontend code; keep the
    cross-field escape hatch (`Validatable`) Go-only and round-trip it (§7.3, §7.4).
17. **Add to `Parameter`, before the first connector ships, what Conduit lacks**: `sensitive`, `order`,
    `group`, `label`, `enum`, and a `dependsOn` for conditional visibility — one missing `sensitive`
    boolean became a security-bug class there (§7.2, §13.2h).
18. **Fixed config pipeline: sanitize → apply defaults → validate against spec → decode → custom
    `Validate(ctx)`, with all errors accumulated via `errors.Join`** so a UI shows every problem at once
    (§7.4).
19. **Reserved config namespace for framework knobs** (`sdk.*` there, e.g. `canal.*`), plus a
    `connector.yaml`-style `./connector spec` command that prints the spec with no RPC involved (§7.5,
    §13.1).
20. **Two metadata namespaces with a hard rule: spec-level keys are connector-agnostic, anything
    source-shaped gets a connector-namespaced key** — Conduit put `opencdc.file.chunk.count` in the
    shared envelope and `postgres.snapshot.resumed` in the connector's, and only the second is right
    (§2.4, §4.5, §13.2g).
21. **`opencdc.collection`-style generic "which entity" metadata key**, and derive schema subjects from
    it, so multi-table sources work without the core knowing what a table is (§2.4, §5.2).
22. **Sealed two-member `Data` interface (raw bytes | structured map) with an unexported marker method,
    and no implicit conversion** — but expose one explicit, well-named conversion helper so canal
    doesn't grow an `unwrap` processor family (§2.3, §13.2j).
23. **Keep the codec off the record**: pluggable `Converter` + `Encoder` composed into a named serializer
    (`opencdc/json`), selected by pipeline config, never a mutable private field on the record value
    (§2.5, §8.5).
24. **A `ProcessedRecord` sum type for transforms** — `SingleRecord` / `MultiRecord` / `FilterRecord` /
    `ErrorRecord{Error}` with an unexported marker — which gives split, filter and per-record failure in
    one return type (`conduit-processor-sdk/processor.go`, §13.1); and make `Process` idempotent by
    contract since it may re-run after a restart.
25. **Contextual cancellation with two scopes and a `Detach` helper**: an `openCtx` for connection
    lifetime that survives the `Open` call, a `readCtx` for the read loop, and the documented rule that
    cancellation means *drain then stop*, not abort (§6.4).
26. **`Configure → Teardown` with no `Open` is a legal, documented sequence** (used for config
    validation), and `Teardown` must run after a failed `Open` (§6.3).
27. **Position-driven drain on shutdown**: source reports its last emitted position, destination blocks
    until it has seen exactly that position, then flushes — deterministic drain without counting records
    (§6.2).
28. **Ack ordering via a ticket-at-enqueue / acquire-at-completion semaphore, and latch the node broken on
    the first ack failure** rather than continuing with a void ordering guarantee (§8.3).
29. **Panic-and-hang containment for in-process plugins**: run each call in a goroutine with
    `recover()`, and `select` on `ctx.Done()` so the host can abandon a wedged connector (§6.6).
30. **DLQ as a destination connector behind a 3-method interface, written synchronously and awaited
    before the source is acked, guarded by a fixed-size ack/nack ring-buffer circuit breaker, defaulting
    to "first nack stops the pipeline"** (§9.4).
31. **But decide early whether canal needs a queryable DLQ store** — Conduit's UI can show DLQ *config*
    and nothing else, and retrofitting the store is explicitly Tier-1 work (§9.4, §13.2m).
32. **One `component_id` id space shared by topology nodes, metrics labels, and the live-record inspector**,
    so a UI joins throughput, latency and record stream on one key (§11.1).
33. **Ship monotonic `records_in`/`records_out` counters and a `last_record_at` per connector instance
    from day one** — Conduit has neither, so "the connector is up" and "data is moving" are
    indistinguishable and no histogram can reconstruct them (§11.1, §11.2).
34. **Pipeline status enum that distinguishes user-stopped from system-stopped and degraded from
    recovering, plus a flap counter** (§11.2).
35. **Metric naming convention decided in the first commit** (`<prod>_<subsystem>_<thing>_total`), and
    over-provision labels immediately, because adding a label later resets every series (§11.1).
36. **A live per-component record inspector — with a dropped-records counter from the start**, since a
    tap that silently drops is worse than none (§11.3).
37. **Structured, coded errors at the API boundary**: stable `reason` code + gRPC category + optional
    `configPath` / `suggestion` / `fix`, with the underlying sentinel preserved so `errors.Is` still
    works, and a contract test that fails the build on a codeless status (§6.5c).
38. **A connector-facing failure-mode taxonomy — which canal must invent, because Conduit has none**: at
    minimum `Retryable` / `Poison` (DLQ this record) / `Fatal` (stop the pipeline), typed and carried
    per record (§6.5, §13.2i).
39. **Single-node engine; all distribution in a scheduling layer above it; HA = active/passive
    resume-from-checkpoint** — this is how canal gets "one binary, standalone and enterprise" without
    inheriting Kafka Connect's rebalance failure surface, and it makes HA correctness reduce to the
    checkpoint invariants (§12.1, §12.2).
40. **Reserve the partition/claim vocabulary in the interface now, inert, with zero-value = today's
    single-instance behaviour** — a mode enum including an `EXTERNALLY_MANAGED` slot for sources like
    Kafka whose assignment the engine must not touch, and a per-unit position using the same opaque-bytes
    contract (§12.3).
41. **Write the ADRs that say what canal will never build**, with an explicit "PRs doing this are
    rejected and pointed here" clause — Conduit's single-node and local-state-only ADRs are load-bearing
    scope defence (§12.1, §12.4).
42. **A framework-owned conformance suite where the connector author implements a driver, not tests** —
    including separate resume-mid-snapshot and resume-mid-CDC cases and default goroutine-leak checking
    (§13.3).
43. **An engine-level chaos suite (SIGKILL/SIGTERM mid-batch, mid-checkpoint) asserting the invariants on
    recovery, treated as a release gate** — Conduit's sev-0 was found by "the first test ever written to
    look for it" (§13.3, §13.2c).
44. **Persist positions via a debouncing batcher (time OR count threshold) in one store transaction, with
    the position living inside the connector-instance record** — one write, atomic, no separate offset
    store (§3.3, §3.4).
45. **Recovery backoff whose attempt counter decays over a sliding window**, so a pipeline that flaps once
    a day never exhausts retries while a tight-loop flapper does, and carry that state across restarts
    (§12.5).
46. **Overwrite a built-in connector's self-declared version with the resolved build version** from
    `debug.ReadBuildInfo()`, so a connector cannot lie about which build it is (§1.5).
47. **Wildcard parameter names (`collection.*.format`) for per-entity config**, so the core can default
    and validate keys it has never seen (§7.2).

---

## Appendix: limits of this analysis

Read and quoted from source (partial list of the files this dossier is built on):

- `conduit-connector-sdk`: `source.go`, `destination.go`, `connector.go`, `unimplemented.go`,
  `specifier.go`, `error.go`, `serve.go`, `util.go`, `record_serializer.go`, `source_middleware.go`,
  `destination_middleware.go`, `acceptance_testing.go`, `schema/schema.go`,
  `conn-sdk-cli/specgen/{README.md,specgen.go,model/v1/model.go}`
- `conduit-commons`: `opencdc/{record,data,operation,position,metadata,errors}.go`,
  `config/{config,parameter,validation}.go`, `schema/schema.go`, `paramgen/paramgen/paramgen.go`
- `conduit-connector-protocol`: `pconnector/{source,destination,specifier,pconnector,config}.go`,
  `pconnector/client/client.go`, `pconnector/v{1,2}/version.go`, `proto/connector/v2/source.proto`
- `conduit`: `pkg/plugin/connector/{plugin.go,builtin/{registry,dispenser,source,sandbox,stream}.go,standalone/registry.go}`,
  `pkg/connector/{instance,source,destination,persister,store}.go`,
  `pkg/lifecycle/{service.go,dlq.go,stream/{base,message,doc,source_acker,destination_acker,dlq}.go}`,
  `pkg/lifecycle-poc/funnel/{batch,source}.go`, `pkg/pipeline/instance.go`,
  `pkg/foundation/metrics/measure/measure.go`, `pkg/foundation/cerrors/fatal.go`,
  `pkg/http/api/status/status.go`, `pkg/conduit/config.go`, `CLAUDE.md`,
  `docs/architecture-decision-records/{20260704-single-node-engine,20260704-local-state-only,20260704-pipeline-architecture-v2}.md`,
  `docs/design-documents/20260723-partition-claims-protocol.md`,
  `docs/postmortems/20260723-source-ack-persist-ordering.md`, `docs/metrics.md`
- `conduit-connector-postgres`: `source/position/position.go`, `source/logrepl/combined.go`,
  `source/snapshot/iterator.go`
- `conduit-processor-sdk`: `processor.go`

**Not read, and therefore not asserted anywhere above:**

- **`conduit-connector-mongo`.** `github.com/ConduitIO/conduit-connector-mongo` returns 404 — the Mongo
  connector lives under the **`conduitio-labs`** org, not `ConduitIO`, per the ack-ordering postmortem's
  own reference (`conduitio-labs/conduit-connector-mongo`). The cross-check of a second snapshot→CDC
  source was therefore not performed. Everything §4 says is Postgres-derived. The postmortem's claims
  about Mongo (`Ack` is a no-op, `ChangeStreamHistoryLost` hard-errors, no silent "start from now"
  fallback) are quoted as *their* findings, not verified by me.
- **`conduit-connector-kafka`, `-file`, `-s3`, `-generator`, `-log`.** Not read. Statements about
  Kafka's partition model come from the partition-claims RFC's own table, not from the connector.
- **`pkg/lifecycle-poc/funnel/worker.go`** (22 KB) and `pkg/lifecycle-poc/service.go` (29 KB). I read
  `batch.go`, `source.go` and the ADR; the v2 worker's exact commit/ack sequencing is inferred from
  those, not read line by line. Treat v2 statements as less firm than v1 statements.
- **`tests/chaos/{harness,child,upstream}.go`.** Existence and stated purpose verified from the tree
  listing and the ADR/postmortems; the tests themselves were not read.
- **The gRPC halves of the boundary** (`pconnector/v2/{client,server,toproto,fromproto}/*.go`). I read the
  `.proto`, the version constants, and `client/client.go`; the generated/handwritten conversion files
  were listed but not read. The claim "the gRPC client implements the same Go interface" is supported by
  `client/client.go`'s `newPluginSet` wiring and `standalone/registry.go` returning a
  `connector.Dispenser`, not by reading every conversion function.
- **`docs/design-documents/20260723-source-ack-persist-ordering-fix.md`** (29 KB) and
  **`20260728-snapshot-handoff-deferred-ack-deadlock.md`** (16 KB). Read the postmortem for the first and
  the code comments that cite both; the design docs themselves were not read in full.
- **The UI.** `conduit-ui` is a separate repo; not fetched. §11.3 describes the API surface a UI reads,
  from engine source and issue #2624, not the UI itself.
- **Whether `conduit_pipeline_status`'s numeric values map to the API enum in the order §11.2 lists.** The
  metric's help text points at the buf.build API docs; I read the `pipeline.Status` Go enum, not the
  proto, so the numeric mapping between them is unconfirmed.
- **Schema drift policy.** I state that no halt/DLQ/evolve policy exists in the code I read. I read
  `commons/schema/*`, `sdk/schema/*` and the extraction middleware; I did **not** read
  `pkg/schemaregistry/*` in full, so a compatibility-mode setting could exist there.
