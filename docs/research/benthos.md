# Prior art: Benthos / Redpanda Connect

**What was analysed (exact refs):**

| Artefact | Ref | Date |
|---|---|---|
| `github.com/redpanda-data/benthos` (the MIT-licensed core engine + `public/service` plugin API) | commit `4ade611e0738be345a6aa732aefcd36a64793fb3` | 2026-07-29 |
| `github.com/redpanda-data/connect` (the connector distribution, incl. the gRPC/subprocess plugin boundary) | commit `017e01f9346d30a92f8948679371ef55b3521ac2` | 2026-07-29 |
| `github.com/Jeffail/checkpoint` (`capped.go`, `uncapped.go`) — the out-of-tree offset resolver every CDC input uses | `main` (fetched 2026-07-29) | — |

CHANGELOG top entry in the benthos clone: `## 4.76.0 - 2026-06-25`.

Everything quoted below is copied out of those trees. Where I am inferring rather than
quoting, I say so inline and it is repeated in the "unverified" list at the end.

Two repos matter because the interface set lives in `benthos/public/service` but the
*interesting* consumers of that interface set (CDC with snapshots, checkpoint stores,
out-of-process plugins) live in `connect/internal`. Reading only the former gives you a
misleadingly simple picture.

---

## 1. Core interfaces

The entire plugin surface is in `benthos/public/service`. It is deliberately tiny: four
verbs per component type, and only two shapes of component (single-message and batched).

### 1.1 The ack callback — the keystone of the whole design

`public/service/input.go`:

```go
// AckFunc is a common function returned by inputs that must be called once for
// each message consumed. This function ensures that the source of the message
// receives either an acknowledgement (err is nil) or an error that can either
// be propagated upstream as a nack, or trigger a reattempt at delivering the
// same message.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack then you can wrap your input implementation with AutoRetryNacks
// to get automatic retries.
type AckFunc func(ctx context.Context, err error) error
```

That is the *only* progress-tracking primitive in the core. There is no `Offset`, no
`Position`, no `Commit()`, no checkpoint store interface. Note the signature carefully:
it is `(ctx, err) error` — a *nack* is "call ack with a non-nil error", and the ack call
itself can fail. Both of those are load-bearing.

### 1.2 Source

`public/service/input.go`:

```go
// Input is an interface implemented by Benthos inputs. Calls to Read should
// block until either a message has been received, the connection is lost, or
// the provided context is cancelled.
type Input interface {
	// Establish a connection to the upstream service. Connect will always be
	// called first when a reader is instantiated, and will be continuously
	// called with back off until a nil error is returned.
	//
	// The provided context remains open only for the duration of the connecting
	// phase, and should not be used to establish the lifetime of the connection
	// itself.
	//
	// Once Connect returns a nil error the Read method will be called until
	// either ErrNotConnected is returned, or the reader is closed.
	Connect(context.Context) error

	// Read a single message from a source, along with a function to be called
	// once the message can be either acked (successfully sent or intentionally
	// filtered) or nacked (failed to be processed or dispatched to the output).
	//
	// The AckFunc will be called for every message at least once, but there are
	// no guarantees as to when this will occur. If your input implementation
	// doesn't have a specific mechanism for dealing with a nack then you can
	// wrap your input implementation with AutoRetryNacks to get automatic
	// retries.
	//
	// If this method returns ErrNotConnected then Read will not be called again
	// until Connect has returned a nil error. If ErrEndOfInput is returned then
	// Read will no longer be called and the pipeline will gracefully terminate.
	Read(context.Context) (*Message, AckFunc, error)

	Closer
}
```

```go
// BatchInput is an interface implemented by Benthos inputs that produce
// messages in batches, where there is a desire to process and send the batch as
// a logical group rather than as individual messages.
//
// Calls to ReadBatch should block until either a message batch is ready to
// process, the connection is lost, or the provided context is cancelled.
type BatchInput interface {
	Connect(context.Context) error

	// Read a message batch from a source, along with a function to be called
	// once the entire batch can be either acked (successfully sent or
	// intentionally filtered) or nacked (failed to be processed or dispatched
	// to the output).
	// ...
	ReadBatch(context.Context) (MessageBatch, AckFunc, error)

	Closer
}
```

Four things worth staring at:

1. **The connect context is scoped to connecting, not to the connection.** "The provided
   context remains open only for the duration of the connecting phase, and should not be
   used to establish the lifetime of the connection itself." This forces every real
   connector to keep its own `shutdown.Signaller`/context — see §6.
2. **`Connect` is idempotent-by-contract and re-callable.** The engine calls it in a
   backoff loop, and calls it *again* whenever `Read` returns `ErrNotConnected`.
3. **Two sentinel errors do control flow**: `ErrNotConnected` → reconnect; `ErrEndOfInput`
   → graceful pipeline termination. This is how a batch/snapshot pipeline terminates: a
   bounded source returns `ErrEndOfInput` and the whole process shuts down cleanly.
4. **`Read` returns exactly one ack func per call.** Not per message. For `BatchInput`,
   the ack is batch-wide, and the granular-failure case is expressed by passing a
   `*BatchError` *into* the ack (see §9).

### 1.3 Sink

`public/service/output.go`:

```go
// Output is an interface implemented by Benthos outputs that support single
// message writes. Each call to Write should block until either the message has
// been successfully or unsuccessfully sent, or the context is cancelled.
//
// Multiple write calls can be performed in parallel, and the constructor of an
// output must provide a MaxInFlight parameter indicating the maximum number of
// parallel write calls the output supports.
type Output interface {
	Connect(context.Context) error

	// Write a message to a sink, or return an error if delivery is not
	// possible.
	//
	// If this method returns ErrNotConnected then write will not be called
	// again until Connect has returned a nil error.
	Write(context.Context, *Message) error

	Closer
}
```

```go
// BatchOutput is an interface implemented by Benthos outputs that require
// Benthos to batch messages before dispatch in order to improve throughput.
// Each call to WriteBatch should block until either all messages in the batch
// have been successfully or unsuccessfully sent, or the context is cancelled.
//
// Multiple write calls can be performed in parallel, and the constructor of an
// output must provide a MaxInFlight parameter indicating the maximum number of
// parallel batched write calls the output supports.
type BatchOutput interface {
	Connect(context.Context) error
	WriteBatch(context.Context, MessageBatch) error
	Closer
}
```

The asymmetry with inputs is the single most important observation in this whole dossier:
**a sink has no ack callback.** It signals success by returning `nil` from `Write`. The
engine owns the mapping from "write returned nil" to "call the input's `AckFunc(nil)`".
A sink is therefore *stateless with respect to progress* — it cannot and need not know
anything about offsets.

### 1.4 Closer, and the universal lifecycle tail

`public/service/package.go`:

```go
// Closer is implemented by components that support stopping and cleaning up
// their underlying resources.
type Closer interface {
	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
```

And an *optional, capability-probed* interface — added later, and probed by type assertion
rather than being part of `Input`/`Output`:

```go
// ConnectionTestable is implemented by components that support testing the
// underlying connection separately to regular operation. This connection
// check can occur before and during normal operation.
type ConnectionTestable interface {
	// ConnectionTest attempts to establish whether the component is capable of
	// creating a connection. This will potentially require and test network
	// connectivity, but does not require the component to be initialized.
	ConnectionTest(ctx context.Context) ConnectionTestResults
}
```

```go
type ConnectionTestResult struct {
	Label string
	Path  []string
	Err   error
}
type ConnectionTestResults []*ConnectionTestResult
```

with helpers `ConnectionTestSucceeded()`, `ConnectionTestFailed(err)`,
`ConnectionTestNotSupported()`, and `(*ConnectionTestResult).AsList()`. Every internal
wrapper (`airGapReader`, `maxInFlight`, `autoRetryInputBatched`,
`forceTimelyNacksInputBatched`, …) has to manually forward the probe:

```go
func (a *airGapReader) ConnectionTest(ctx context.Context) component.ConnectionTestResults {
	t, ok := a.r.(ConnectionTestable)
	if !ok {
		return component.ConnectionTestNotSupported(a.o).AsList()
	}
	return t.ConnectionTest(ctx).intoInternal(a.o)
}
```

This is the visible cost of adding a capability to a frozen interface via type assertion:
N wrappers × one boilerplate forwarder each. Canal should decide up front which optional
capabilities exist and give them a single generic forwarding helper.

### 1.5 Transform

`public/service/processor.go`:

```go
// Processor is a Benthos processor implementation that works against single
// messages.
type Processor interface {
	// Process a message into one or more resulting messages, or return an error
	// if the message could not be processed. If zero messages are returned and
	// the error is nil then the message is filtered.
	//
	// When an error is returned the input message will continue down the
	// pipeline but will be marked with the error with *message.SetError, and
	// metrics and logs will be emitted. ...
	//
	// The Message types returned MUST be derived from the provided message, and
	// CANNOT be custom instantiations of Message. In order to copy the
	// provided message use the Copy method.
	Process(context.Context, *Message) (MessageBatch, error)

	Closer
}

// BatchProcessor is a Benthos processor implementation that works against
// batches of messages, which allows windowed processing.
type BatchProcessor interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed. If zero messages are
	// returned and the error is nil then all messages are filtered.
	//
	// The provided MessageBatch should NOT be modified, in order to return a
	// mutated batch a copy of the slice should be created instead.
	// ...
	ProcessBatch(context.Context, MessageBatch) ([]MessageBatch, error)

	Closer
}
```

Semantics packed into the return types, all with no extra vocabulary:
`1 → N` (expand), `1 → 0` (filter), `N → M batches` (regroup/window), error → mark-and-continue.
The "MUST be derived from the provided message" constraint exists because the ack
plumbing and the sort-group identity (§2.4) are carried on the underlying `*message.Part`,
not on the public wrapper. That is a **leaky abstraction the doc comment has to
apologise for** — see §13.

### 1.6 Buffer — a separate, explicitly-dangerous stage

`public/service/buffer.go`:

```go
// BatchBuffer is an interface implemented by Buffers able to read and write
// message batches. Buffers are a component type that are placed after inputs,
// and decouples the acknowledgement system of the inputs from the rest of the
// pipeline.
//
// Buffers are useful when implementing buffers intended to relieve back
// pressure from upstream components, or when implementing message aggregators
// where the concept of discrete messages running through a pipeline no longer
// applies (such as with windowing algorithms).
//
// Buffers are advanced component types that weaken delivery guarantees of a
// Benthos pipeline. Therefore, if you aren't absolutely sure that a component
// you wish to build should be a buffer type then it likely shouldn't be.
type BatchBuffer interface {
	// Write a batch of messages to the buffer, the batch is accompanied with an
	// acknowledge function. A non-nil error should be returned if it is not
	// possible to store the given message batch in the buffer.
	//
	// If a nil error is returned the buffer assumes responsibility for calling
	// the acknowledge function at least once during the lifetime of the
	// message.
	//
	// This could be at the point where the message is written to the buffer,
	// which weakens delivery guarantees but can be useful for decoupling the
	// input from downstream components. Alternatively, this could be when the
	// associated batch has been read from the buffer and acknowledged
	// downstream, which preserves delivery guarantees.
	WriteBatch(context.Context, MessageBatch, AckFunc) error

	// Read a batch of messages from the buffer. ...
	// When the buffer is closed (EndOfInput has been called and no more
	// messages are available) this method should return an ErrEndOfBuffer in
	// order to indicate the end of the buffered stream.
	//
	// It is valid to return a batch of only one message.
	ReadBatch(context.Context) (MessageBatch, AckFunc, error)

	// EndOfInput indicates to the buffer that the input has ended and that once
	// the buffer is depleted it should return ErrEndOfBuffer from ReadBatch in
	// order to gracefully shut down the pipeline.
	//
	// EndOfInput should be idempotent as it may be called more than once.
	EndOfInput()

	Closer
}
```

This is the cleanest thing in the whole API and the most transplantable single idea. The
buffer is *the* place where the ack chain is allowed to be cut, and the interface makes
that choice explicit and documented: "This could be at the point where the message is
written to the buffer, which weakens delivery guarantees … Alternatively, this could be
when the associated batch has been read from the buffer and acknowledged downstream,
which preserves delivery guarantees."

`EndOfInput()` + `ErrEndOfBuffer` is also how end-of-stream propagates *through* a
stateful stage, which is exactly the problem a snapshot→stream handoff has.

### 1.7 Scanner — the decode/split stage, ack-aggregating

`public/service/scanner.go`:

```go
// BatchScannerCreator is an interface implemented by Benthos scanner plugins.
// Calls to Create must create a new instantiation of BatchScanner that consumes
// the provided io.ReadCloser, produces batches of messages (batches containing
// a single message are valid) and calls the provided AckFunc once all derived
// data is delivered (or rejected).
type BatchScannerCreator interface {
	Create(io.ReadCloser, AckFunc, *ScannerSourceDetails) (BatchScanner, error)
	Close(context.Context) error
}

// BatchScanner ...
// The returned ack func will be called by downstream components once the
// produced message batch has been successfully processed and delivered. Only
// once all message batches extracted from a BatchScanner should the ack func
// provided at instantiation be called, unless an ack call is returned with an
// error.
//
// Once the input data has been fully consumed io.EOF should be returned.
type BatchScanner interface {
	NextBatch(context.Context) (MessageBatch, AckFunc, error)
	Close(context.Context) error
}
```

Note the *fan-out ack* contract: one source ack, N derived batch acks, and the
scanner must aggregate. Because that is fiddly and everyone would get it wrong, the API
ships the reduction:

```go
// SimpleBatchScanner is a reduced version of BatchScanner where managing the
// aggregation of acknowledgments from yielded message batches is omitted.
type SimpleBatchScanner interface {
	NextBatch(context.Context) (MessageBatch, error)
	Close(context.Context) error
}

// AutoAggregateBatchScannerAcks wraps a simplified SimpleBatchScanner in a
// mechanism that automatically aggregates acknowledgments from yielded batches.
func AutoAggregateBatchScannerAcks(strm SimpleBatchScanner, aFn AckFunc) BatchScanner
```

The `managedAckBatchScanner` implementation is 45 lines: a `pending int32` counter, a
`finished bool` set on `io.EOF`, an `ackOnce`-wrapped source ack, and
`doAck := s.pending == 0 && s.finished`. Any error nacks the source immediately. This is
the reference implementation of "fan-out ack aggregation" and it's worth copying almost
verbatim.

**This is the pattern for canal's codec/deserialiser stage**: a codec that turns one
source unit (file, WAL segment, gRPC response) into many records must own an ack
aggregator, and the framework should provide the aggregator so connector authors never
write it.

### 1.8 Supporting interfaces (state store, throttle, metrics sink)

`public/service/cache.go` — this is where "durable KV" lives, and it is *not* privileged
as a checkpoint store; it is just another plugin type:

```go
// Cache is an interface implemented by Benthos caches.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error
	// Add is the same operation as Set except that it returns an error if the
	// key already exists. ...
	Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error
	Delete(ctx context.Context, key string) error
	Closer
}
```

plus a *capability-probed* batch extension that is opt-in and invisible in the main
interface:

```go
// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type batchedCache interface {
	SetMulti(ctx context.Context, keyValues ...CacheItem) error
}
```

Note `batchedCache` is *unexported* — an implementer discovers it from docs, not from the
type system. That's a defect worth not copying; export the optional interface.

`public/service/rate_limit.go`:

```go
// RateLimit is an interface implemented by Benthos rate limits.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(context.Context) (time.Duration, error)
	Closer
}
```

`public/service/metrics.go` — the metrics *exporter* plugin type, deliberately
constructor-shaped so label values are bound once:

```go
// MetricsExporter is an interface implemented by Benthos metrics exporters.
type MetricsExporter interface {
	NewCounterCtor(name string, labelKeys ...string) MetricsExporterCounterCtor
	NewTimerCtor(name string, labelKeys ...string) MetricsExporterTimerCtor
	NewGaugeCtor(name string, labelKeys ...string) MetricsExporterGaugeCtor
	Close(ctx context.Context) error
}

type MetricsExporterCounterCtor func(labelValues ...string) MetricsExporterCounter
type MetricsExporterTimerCtor   func(labelValues ...string) MetricsExporterTimer
type MetricsExporterGaugeCtor   func(labelValues ...string) MetricsExporterGauge

type MetricsExporterCounter interface { Incr(count int64) }
type MetricsExporterTimer   interface { Timing(delta int64) }
type MetricsExporterGauge   interface { Set(value int64) }
```

with two admissions of a versioning mistake sitting right in the source:

```go
	// IncrFloat64 increments a counter metric by a decimal amount ...
	// TODO: V5 Add this (or replace the int based method)
	// IncrFloat64(count float64)
```

and, because they could not add the method, a runtime type assertion escape hatch in the
adapter (`airGapCounter.IncrFloat64` probes for `interface{ IncrFloat64(float64) }` and
falls back to `Incr(int64(count))`). Canal should not repeat this: pick float64 for
counters/gauges from day one.

### 1.9 Internal driver interfaces (what the public ones get adapted *to*)

The public interfaces are not what the engine runs. Every plugin is wrapped into an
"air gap" adapter and then into an internal driver. `internal/component/input/interface.go`:

```go
// Streamed is a common interface implemented by inputs and provides channel
// based streaming APIs.
type Streamed interface {
	// TransactionChan returns a channel used for consuming transactions from
	// this type. Every transaction received must be resolved before another
	// transaction will be sent.
	TransactionChan() <-chan message.Transaction

	ConnectionTest(ctx context.Context) component.ConnectionTestResults
	ConnectionStatus() component.ConnectionStatuses

	// TriggerStartConsuming instructs the input to start consuming data, and attempting
	// to write it to the transaction channel.
	TriggerStartConsuming()
	// TriggerStopConsuming instructs the input to start shutting down resources
	// once all pending messages are delivered and acknowledged. This call does
	// not block.
	TriggerStopConsuming()
	// TriggerCloseNow triggers the shut down of this component but should not
	// block the calling goroutine.
	TriggerCloseNow()
	// WaitForClose is a blocking call to wait until the component has finished
	// shutting down and cleaning up resources.
	WaitForClose(ctx context.Context) error
}

type AsyncAckFn func(context.Context, error) error

// Async is a type that reads Benthos messages from an external source and
// allows acknowledgements for a message batch to be propagated asynchronously.
type Async interface {
	ConnectionTest(ctx context.Context) component.ConnectionTestResults
	Connect(ctx context.Context) error
	ReadBatch(ctx context.Context) (message.Batch, AsyncAckFn, error)
	Close(ctx context.Context) error
}
```

`internal/component/output/async_writer.go`:

```go
// AsyncSink is a type that writes Benthos messages to a third party sink. If
// the protocol supports a form of acknowledgement then it will be returned by
// the call to Write.
type AsyncSink interface {
	ConnectionTest(ctx context.Context) component.ConnectionTestResults
	Connect(ctx context.Context) error
	// WriteBatch should block until either the message is sent (and
	// acknowledged) to a sink, or a transport specific error has occurred, or
	// the Type is closed.
	WriteBatch(ctx context.Context, msg message.Batch) error
	Close(ctx context.Context) error
}
```

**The three-layer structure is the real architecture:**

```
public plugin iface (Input/BatchInput)
   ↓  airGapReader / airGapBatchBuffer / airGapWriter   ← "air gap": public⇄internal type + error translation
internal driver iface (input.Async / output.AsyncSink)
   ↓  input.NewAsyncReader / output.NewAsyncWriter      ← owns connect-backoff, read-backoff, ack goroutines, metrics, tracing
internal stream iface (input.Streamed / output.Streamed) ← channel-of-transactions
```

The public interface never sees a channel. The *internal* one is channel-of-transactions.
This is a good split for canal: connector authors get blocking method calls; the runtime
gets channels and concurrency. The "air gap" naming is theirs and the pattern is
explicit — see the error translation in §6.4.

The internal transaction type is where an ack becomes a first-class value:
`message.NewTransaction(payload, resChan)` and `message.NewTransactionFunc(payload, fn)`
(from `output.go` / `input.go` usage). `ResourceInput.ReadBatch` shows the reverse
adaptation — pulling a transaction off a channel and re-exposing it as an `AckFunc`:

```go
	return b, func(c context.Context, r error) error {
		r = publicToInternalErr(r)
		return tran.Ack(c, r)
	}, nil
```

### 1.10 Constructors and their return signatures

`public/service/plugins.go` — the constructor is where per-instance capabilities are
declared, and it is the only place the engine learns them:

```go
type BatchBufferConstructor func(conf *ParsedConfig, mgr *Resources) (BatchBuffer, error)
type CacheConstructor       func(conf *ParsedConfig, mgr *Resources) (Cache, error)
type InputConstructor       func(conf *ParsedConfig, mgr *Resources) (Input, error)
type BatchInputConstructor  func(conf *ParsedConfig, mgr *Resources) (BatchInput, error)

// OutputConstructor is a func that's provided a configuration type and access
// to a service manager, and must return an instantiation of a writer based on
// the config and a maximum number of in-flight messages to allow, or an error.
type OutputConstructor func(conf *ParsedConfig, mgr *Resources) (out Output, maxInFlight int, err error)

// BatchOutputConstructor is a func that's provided a configuration type and
// access to a service manager, and must return an instantiation of a writer
// based on the config, a batching policy, and a maximum number of in-flight
// message batches to allow, or an error.
type BatchOutputConstructor func(conf *ParsedConfig, mgr *Resources) (out BatchOutput, batchPolicy BatchPolicy, maxInFlight int, err error)

type ProcessorConstructor      func(conf *ParsedConfig, mgr *Resources) (Processor, error)
type BatchProcessorConstructor func(conf *ParsedConfig, mgr *Resources) (BatchProcessor, error)
type RateLimitConstructor      func(conf *ParsedConfig, mgr *Resources) (RateLimit, error)
type MetricsExporterConstructor func(conf *ParsedConfig, log *Logger) (MetricsExporter, error)
type OtelTracerProviderConstructor func(conf *ParsedConfig) (trace.TracerProvider, error)
type BatchScannerCreatorConstructor func(conf *ParsedConfig, mgr *Resources) (BatchScannerCreator, error)
type CustomResourceConstructor  func(conf *ParsedConfig, mgr *Resources) (any, error)
```

The sink constructor returning `(sink, batchPolicy, maxInFlight, err)` is a genuinely
good idea, and one canal should steal and then generalise: **the connector declares its
own concurrency and batching envelope as data, and the framework enforces it** rather
than the connector implementing either. `RegisterBatchOutput`'s doc comment states the
caveat plainly:

```
// The constructor of a batch output is able to return a batch policy to be
// applied before calls to write are made, creating batches from the stream of
// messages. However, batches can also be created by upstream components
// (inputs, buffers, etc).
//
// If a batch has been formed upstream it is possible that its size may exceed
// the policy specified in your constructor.
```

Two design smells to avoid: (a) the tuple return grows with every new capability, and
(b) `maxInFlight < 1` is validated at *construction* with a stringly error
(`fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)` in `environment.go`) —
an option struct with validated defaults would be better.

---

## 2. Record model

### 2.1 The envelope

`public/service/message.go`:

```go
// Message represents a single discrete message passing through a Benthos
// pipeline. It is safe to mutate the message via Set methods, but the
// underlying byte data should not be edited directly.
//
// A Message is not safe for concurrent use by multiple goroutines. To process
// a message in parallel, create independent copies using Copy or DeepCopy and
// pass each copy to its own goroutine.
type Message struct {
	part  *message.Part
	onErr func(err error)
}

// MessageBatch describes a collection of one or more messages.
type MessageBatch []*Message
```

That is the whole public envelope: **an opaque handle over an internal part, plus an
error callback.** There are no exported fields at all. Everything is accessed through
methods. `MessageBatch` is a plain slice — deliberately, so batches are cheap to build,
filter, and re-order, and so `WriteBatch` needs no iterator ceremony.

There is **no key, no partition, no timestamp, no operation type, no before/after image,
and no schema field.** This is the single most consequential decision in the whole
system: the record model is *domain-free*. Everything CDC-shaped lives in metadata by
convention (§2.5).

### 2.2 Payload: raw bytes and structured data coexist, lazily and copy-on-write

```go
// AsBytes returns the underlying byte array contents of a message or, if the
// contents are a structured type, attempts to marshal the contents as a JSON
// document and returns either the byte array result or an error.
//
// It is NOT safe to mutate the contents of the returned slice.
func (m *Message) AsBytes() ([]byte, error)

// AsStructured returns the underlying structured contents of a message or, if
// the contents are a byte array, attempts to parse the bytes contents as a JSON
// document and returns either the structured result or an error.
//
// It is NOT safe to mutate the contents of the returned value if it is a
// reference type (slice or map). In order to safely mutate the structured
// contents of a message use AsStructuredMut.
func (m *Message) AsStructured() (any, error)

// AsStructuredMut ...
// It is safe to mutate the contents of the returned value even if it is a
// reference type (slice or map), as the structured contents will be lazily deep
// cloned if it is still owned by an upstream component.
func (m *Message) AsStructuredMut() (any, error)

func (m *Message) SetBytes(b []byte)
func (m *Message) HasBytes() bool       // "true if the raw message bytes are readily available and cached"
func (m *Message) HasStructured() bool  // "true if the structured message data is readily available and cached"

// SetStructured ...
// The provided structure is considered read-only, which means subsequent
// processors will need to fully clone the structure in order to perform
// mutations on the data.
func (m *Message) SetStructured(i any)

// SetStructuredMut ...
// The provided structure is considered mutable, which means subsequent
// processors might mutate the structure without performing a deep copy.
func (m *Message) SetStructuredMut(i any)
```

This is the most sophisticated part of the record model and the part canal should copy
most carefully. The rules:

- **One payload, two views.** Bytes ⇄ structured conversion is lazy and cached, so
  `bytes → structured → bytes` pays the cost once, and a pipeline that never inspects the
  body pays nothing. `HasBytes()`/`HasStructured()` let a sink pick the cheap path
  ("if it's already bytes, write the bytes; else marshal").
- **Mutability is a *typed* property of the accessor, not of the value.** `AsStructured`
  vs `AsStructuredMut`, `SetStructured` vs `SetStructuredMut`. The `Mut` variants deep-clone
  lazily *only if the value is still owned upstream*. This is copy-on-write with the
  ownership question pushed into the method name, which is about as good as Go gets
  without a borrow checker.
- The structured shape is constrained to a JSON-ish universe by contract:
  "This structured value should be a scalar Go type, or either a `map[string]interface{}`
  or `[]interface{}` containing the same types all the way through the hierarchy, this
  ensures that other processors are able to work with the contents and that they can be
  JSON marshalled when coerced into a byte array."

The constraint is what makes a generic transform language possible at all. It is also the
ceiling on fidelity: anything that isn't JSON-representable (int64 precision, decimals,
binary) has to be encoded, which is exactly why they later had to bolt on a decimal type
(§5).

`AsBytes` carries an unresolved wart, visible in the source:

```go
func (m *Message) AsBytes() ([]byte, error) {
	// TODO: Escalate errors in marshalling once we're able.
	return m.part.AsBytes(), nil
}
```

The signature has an `error` it never returns, because marshalling failures cannot be
surfaced without breaking compatibility. Canal: return the error and mean it.

### 2.3 Metadata: three value flavours

Metadata is the escape hatch that carries everything the envelope omits, and it grew
three tiers:

```go
func (m *Message) MetaGet(key string) (string, bool)          // string view
func (m *Message) MetaSet(key, value string)                  // "If the value is an empty string the metadata key is deleted."
func (m *Message) MetaGetMut(key string) (any, bool)           // mutable any
func (m *Message) MetaSetMut(key string, value any)            // "The caller must not mutate the value after passing it to MetaSetMut."
func (m *Message) MetaGetImmut(key string) (any, bool)         // read-only any
func (m *Message) MetaSetImmut(key string, value ImmutableValue)
func (m *Message) MetaDelete(key string)
func (m *Message) MetaWalk(fn func(string, string) error) error
func (m *Message) MetaWalkMut(fn func(key string, value any) error) error
```

with the immutable tier defined as:

```go
// ImmutableValue is implemented by values stored via MetaSetImmut. The engine
// calls Copy() automatically when a downstream component requests a mutable
// version of the value via MetaGetMut, delivering a fresh instance without
// affecting the original.
//
// Copy() must be safe to call concurrently from multiple goroutines. After a
// DeepCopy, immutable entries are shared across message copies that may be
// processed in parallel, and each parallel branch may call Copy() on the same
// ImmutableValue simultaneously.
type ImmutableValue interface {
	Copy() any
}

// ImmutableAny wraps an arbitrary value as an ImmutableValue. ...
// Copy() deeply clones []any and map[string]any structures. Other reference
// types (e.g. []string, map[string]string, custom structs) are returned by
// reference and should therefore not be stored in metadata values unless they
// are themselves mutable.
type ImmutableAny struct{ V any }

func (i ImmutableAny) Copy() any { return message.CopyJSON(i.V) }
```

`MetaSet` deleting on empty string is a **bug-shaped convenience**: you cannot represent
"present but empty". Do not copy that.

`MetaSetImmut` is how expensive derived values (notably schemas — see §5) ride along
without being copied per message. That is the mechanism canal wants for "attach the
resolved schema to the record without paying a clone per record."

There is also an explicit output-side filter component so that metadata → sink-header
mapping is configuration rather than code (`public/service/config_metadata_filter.go`):

```go
// NewMetadataFilterField creates a config field spec for describing which
// metadata keys to include for a given purpose. This includes prefix based and
// regular expression based methods. This field is often used for making
// metadata written to output destinations explicit.
func NewMetadataFilterField(name string) *ConfigField

type MetadataFilter struct { f *metadata.IncludeFilter }
func (m *MetadataFilter) IsEmpty() bool
func (m *MetadataFilter) Match(k string) bool
func (m *MetadataFilter) Walk(msg *Message, fn func(key, value string) error) error
```

Every sink that writes headers takes a `NewMetadataFilterField` and walks it. That's the
right shape: **which metadata escapes to the sink is a config decision, uniform across all
sinks, implemented once.**

### 2.4 Error state and batch identity

```go
// SetError marks the message as having failed a processing step and adds the
// error to it as context. ...
func (m *Message) SetError(err error) {
	if m.onErr != nil {
		m.onErr(err)
	}
	m.part.ErrorSet(err)
}
func (m *Message) GetError() error
```

The error is *carried on the record*, not returned. That is what makes "mark and continue"
routing (`try_catch`, `reject_errored`, `fallback`, `error_handling.strict`) possible
without any special vocabulary in the interfaces.

Identity across reordering is solved with an explicit indexer rather than an ID field:

```go
// Index mutates the batch in situ such that each message in the batch retains
// knowledge of where in the batch it currently resides. An indexer is then
// returned which can be used as a way of re-acquiring the original order of a
// batch derived from this one even after filtering, duplication and reordering
// has been done by other components.
func (b MessageBatch) Index() *Indexer

type Indexer struct {
	wrapped     *message.SortGroup
	sourceBatch MessageBatch
}

// IndexOf attempts to obtain the index of a message as it occurred within the
// origin batch known at the time the indexer was created. If the message is an
// orphan and does not originate from that batch then -1 is returned. It is
// possible that zero, one or more derivative messages yield any given index of
// the origin batch due to filtering and/or duplication enacted on the batch.
func (s *Indexer) IndexOf(m *Message) int
```

Plus the batch copy semantics:

```go
// Copy creates a new slice of the same messages, which can be modified without
// changing the contents of the original batch.
func (b MessageBatch) Copy() MessageBatch

// DeepCopy creates a new slice of the same messages, which can be modified
// without changing the contents of the original batch and are unchanged from
// deep mutations performed on the source message.
//
// This is required in situations where a component wishes to retain a copy of a
// message batch beyond the boundaries of a process or write command. This is
// specifically required for buffer implementations that operate by keeping a
// reference to the message.
func (b MessageBatch) DeepCopy() MessageBatch
```

and the helper that makes "batched sink that actually writes one-by-one" correct:

```go
// WalkWithBatchedErrors walks a batch and executes a closure function for each
// message. If the provided closure returns an error then iteration of the batch
// is not stopped and instead a *BatchError is created and populated.
//
// The one exception to this behaviour is when an error is returned that is
// considered fatal such as ErrNotConnected, in which case iteration is
// terminated early and that error is returned immediately.
//
// This is a useful pattern for batched outputs that deliver messages
// individually.
func (b MessageBatch) WalkWithBatchedErrors(fn func(int, *Message) error) error
```

### 2.5 CDC shape: entirely by convention, in metadata

There is nothing CDC-specific in the core. The MySQL CDC input
(`connect/internal/impl/mysql/event.go`) defines its *own* operation vocabulary:

```go
type position = mysql.Position

// MessageOperation is a string type specifying message operation
type MessageOperation string

const (
	// MessageOperationRead represents read from snapshot
	MessageOperationRead   MessageOperation = "read"
	MessageOperationInsert MessageOperation = "insert"
	MessageOperationUpdate MessageOperation = "update"
	MessageOperationDelete MessageOperation = "delete"

	// messageOperationSnapshotComplete is an internal sentinel emitted after all
	// snapshot rows have been sent so the checkpoint can advance once all snapshot batches
	// are acknowledged
	messageOperationSnapshotComplete MessageOperation = "snapshot_complete"

	// messageOperationXID is an internal sentinel emitted when a transaction commits
	// so readMessages can advance the checkpoint to a transaction boundary, ensuring
	// we never resume mid-transaction (which would miss the TABLE_MAP_EVENT).
	messageOperationXID MessageOperation = "xid"
)

// MessageEvent represents a message from mysql cdc plugin
type MessageEvent struct {
	Row       map[string]any   `json:"row"`
	Table     string           `json:"table"`
	Operation MessageOperation `json:"operation"`
	Position  *position        `json:"position"`
}
```

and then flattens it onto the generic envelope (`input_mysql_stream.go`):

```go
			mb := service.NewMessage(nil)
			mb.SetStructuredMut(me.Row)
			mb.MetaSet("operation", string(me.Operation))
			mb.MetaSet("table", me.Table)
			if me.Position != nil {
				mb.MetaSet("binlog_position", binlogPositionToString(*me.Position))
			}

			// Add table schema if available
			if tableSchema := i.getOrExtractTableSchemaByName(me.Table); tableSchema != nil {
				mb.MetaSetImmut("schema", service.ImmutableAny{V: tableSchema})
			}
```

Postgres does the same with a different key set (`input_pg_stream.go`):

```go
				batchMsg.MetaSet("table", msg.Table)
				batchMsg.MetaSet("operation", string(msg.Operation))
				// ...
					batchMsg.MetaSet("lsn", *msg.LSN)
				// ...
					batchMsg.MetaSet("commit_ts_ms", strconv.FormatInt(msg.CommitTime.UnixMilli(), 10))
```

**Consequences, stated plainly:**

- `operation` values are *not* unified: MySQL emits `read` for snapshot rows;
  Postgres additionally emits `begin`/`commit` markers with null payloads behind a
  `include_transaction_markers` flag. Position keys differ (`binlog_position` vs `lsn`).
  There is **no cross-source contract at all.**
- There is no before-image. An update carries only `row` (the after image). Postgres
  exposes a `unchanged_toast_value` field precisely because it *cannot* always produce a
  complete after image: "The value to emit when there are unchanged TOAST values in the
  stream. This occurs for updates and deletes where REPLICA IDENTITY is not FULL."
- No primary-key/identity field on the envelope. A sink that needs an upsert key has to be
  told the key columns in *its own* config.

This is the clearest lesson for canal. Benthos-style total genericity is right for the
*transport* and wrong for the *semantics*: because the envelope has no operation/key/
before-image, every CDC source invents its own dialect and every CDC-aware sink has to
special-case each source — which is exactly the coupling constraint #1 forbids.

The resolution that respects constraint #1: keep the **core** record envelope generic
(bytes ⇄ structured + metadata + error, as here), but define **one optional, versioned,
well-known "change event" projection** (op, key, before, after, source position, commit
timestamp) as a *typed accessor over the same envelope* — a facet, not a subtype. Sources
that can populate it do; sources that can't don't; sinks that want it ask for it and get
`(facet, ok)`. Nothing in the core switches on source type, and the generic path is
unchanged.

### 2.6 Sync-response side channel

There is also a request/response channel bolted onto the envelope, for HTTP-server-style
inputs:

```go
func (m *Message) WithSyncResponseStore() (*Message, *SyncResponseStore)
func (m *Message) AddSyncResponse() error
func (b MessageBatch) WithSyncResponseStore() (MessageBatch, *SyncResponseStore)
func (b MessageBatch) AddSyncResponse() error

type SyncResponseStore struct { /* ... */ }
func (s *SyncResponseStore) Read() []MessageBatch
```

Worth knowing it exists and worth *not* copying into a data-movement tool unless
request/response pipelines are a goal — it makes the envelope carry a return path.

---

## 3. Checkpoint model

**This is the section that matters most for canal, and the honest headline is: the core
has no checkpoint model at all.** It has an ack graph. Durable progress is a
*connector-private* concern implemented on top of the ack graph plus a `Cache` resource.

### 3.1 What the core owns: an ack tree, and nothing else

The engine's contract is: every batch handed out by an input gets exactly one
`AckFunc(ctx, err)` call, and the input decides what that means. The wiring is in
`internal/component/input/async_reader.go`:

```go
	for {
		msg, ackFn, err := r.reader.ReadBatch(closeAtLeisureCtx)
		// ... error/reconnect handling ...

		startedAt := time.Now()

		resChan := make(chan error, 1)
		tracing.InitSpans(r.mgr.Tracer(), traceName, msg)
		select {
		case r.transactions <- message.NewTransaction(msg, resChan):
		case <-r.shutSig.SoftStopChan():
			return
		}

		pendingAcks.Add(1)
		go func(
			m message.Batch,
			aFn AsyncAckFn,
			rChan chan error,
		) {
			defer pendingAcks.Done()

			var res error
			select {
			case res = <-rChan:
			case <-r.shutSig.HardStopChan():
				// Even if the pipeline is terminating we still want to attempt
				// to propagate an acknowledgement from in-transit messages.
				return
			}

			mLatency.Timing(time.Since(startedAt).Nanoseconds())
			tracing.FinishSpans(m)

			if err = aFn(closeNowCtx, res); err != nil {
				r.mgr.Logger().Error("Failed to acknowledge message: %v\n", err)
			}
		}(msg, ackFn, resChan)
	}
```

Read that carefully; a lot of the design falls out of it:

- **Acks resolve out of order, concurrently, one goroutine per in-flight batch.** There is
  no ordering constraint on ack delivery whatsoever. Ordering, if you want it, is the
  input's problem.
- **The read loop is not blocked on acks.** `Read` is called again immediately after the
  transaction is handed off. Flow control comes from the unbuffered `r.transactions`
  channel and from optional in-flight limiters (§8).
- **A failed ack is logged and dropped.** `if err = aFn(closeNowCtx, res); err != nil { …Error… }`
  — there is no retry of the ack itself and no way for the connector to force the pipeline
  to stop. If your checkpoint write fails, you have delivered data and lost the progress
  record, silently but for a log line. **Canal should treat "commit failed" as a
  first-class, escalatable condition.**
- Ack goroutines are tracked and drained on shutdown:
  ```go
	pendingAcks := sync.WaitGroup{}
	defer func() {
		r.mgr.Logger().Debug("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		r.mgr.Logger().Debug("Pending acks resolved.")
	}()
  ```
- The ack goroutine is given `closeNowCtx` (hard-stop scoped), while the read loop uses
  `closeAtLeisureCtx` (soft-stop scoped) — so acks survive a graceful stop.

The output side closes the loop with a single line, in `internal/component/output/async_writer.go`:

```go
			latency, err := w.latencyMeasuringWrite(closeLeisureCtx, payload)
			// ... connection/close handling, logging ...
			_ = ackFn(closeLeisureCtx, err)
```

`err == nil` → ack; `err != nil` → nack. **That is the entire delivery-guarantee
mechanism.** Note also `_ =`: the writer discards the ack error too.

### 3.2 What "no offset store" buys

1. **Sinks need zero progress awareness.** `WriteBatch(ctx, batch) error` is the complete
   sink contract. A new sink is genuinely "implement 3 methods, register, done", which is
   exactly canal's extensibility criterion.
2. **Any topology composes.** Because progress is a *callback graph* rather than a
   *scalar per partition*, fan-out, fan-in, brokers, retries, filters, 1→N expansion and
   N→M rebatching all compose without the framework understanding them. Fan-out is
   "ack when all branches ack"; filtering is "ack immediately"; expansion is "ack when all
   children ack". None of that needs a code path in the core. An explicit offset store
   would need a merge rule for every one of those cases.
3. **The source's native mechanism is used directly.** Kafka consumer-group commits,
   SQS delete-message, AMQP ack, Pub/Sub ack, GCS/S3 "delete after read" — each is just
   "what the connector does inside its ack func". No translation to a framework offset
   format, no dual-write, no divergence between framework state and broker state.
4. **Ordering is not forced.** Out-of-order ack resolution means slow records don't head-
   of-line-block fast ones; the input can choose how much reordering it tolerates.
5. **It's ~50 lines of core.** Compare a checkpoint coordinator, barrier alignment, state
   backend, and restore protocol.

### 3.3 What it costs

1. **Progress is not observable by the framework.** Nothing in the core can answer
   "where are we?", "what is the lag?", "what would we replay on restart?". There is
   no `GetCheckpoint()` anywhere. A frontend cannot show position/lag generically — only
   per-connector metrics the connector chose to emit. **For canal, whose end-state
   includes a metrics/config frontend, this is disqualifying on its own.**
2. **Every source that needs durable progress reimplements the same algorithm.** The
   algorithm is non-trivial: contiguous-prefix resolution of out-of-order acks. It lives
   out-of-tree (§3.4) and each connector re-wires it, with its own cache key, own
   serialisation format, own flush policy, and own bugs.
3. **No cross-connector atomicity.** Checkpoint writes are per-connector `Cache.Set`
   calls, unversioned, no CAS (there is `Add` for "fail if exists", but no
   compare-and-swap on value), no fencing token. Two workers on the same slot will
   silently clobber each other. There is nothing resembling an epoch or ownership lease.
4. **In-flight state is unbounded unless the connector bounds it.** The ack tree lives in
   memory; a slow sink means growing retained state. Hence `checkpoint_limit`,
   `max_in_flight`, and `Capped` (§3.4, §8) — three separate mechanisms that all exist to
   re-impose the back-pressure the ack model gave away.
5. **Restart granularity is whatever the connector last persisted**, and the duplicate
   window is explicitly acknowledged in the docs. From
   `connect/docs/modules/components/pages/inputs/redpanda.adoc`:
   > "Determines how many messages of the same partition can be processed in parallel
   > before applying back pressure. When a message of a given offset is delivered to the
   > output the offset is only allowed to be committed when all messages of prior offsets
   > have also been delivered, this ensures at-least-once delivery guarantees. However,
   > this mechanism also increases the likelihood of duplicates in the event of crashes or
   > server faults, reducing the checkpoint limit will mitigate this."
6. **Ack-func lifetime is a latent leak.** "The AckFunc will be called for every message
   at least once, but there are no guarantees as to when this will occur." A closure held
   forever pins its captured batch. They had to add a *timeout* to force resolution
   (§6.5).
7. **Nacks can livelock.** Auto-retry is unbounded by default (§9.3), so a permanently
   poisonous record retries forever, and the community-reported symptom is a pipeline that
   makes no progress (see §13).

### 3.4 What a real connector actually does — the `Capped` checkpointer

The reusable piece is `github.com/Jeffail/checkpoint`, **outside both repos**. It is a
generic contiguous-prefix resolver:

```go
// Capped receives an ordered feed of integer based offsets being tracked, and
// an unordered feed of integer based offsets that are resolved, and is able to
// return the highest offset currently able to be committed such that an
// unresolved offset is never committed.
//
// If the number of unresolved tracked values meets a given cap the next attempt
// to track a value will be blocked until the next value is resolved.
//
// This component is safe to use concurrently across goroutines.
type Capped[T any] struct {
	t    *Uncapped[T]
	cap  int64
	cond *sync.Cond
}

func NewCapped[T any](capacity int64) *Capped[T]

// Track a new unresolved integer offset. This offset will be cached until it is
// marked as resolved. While it is cached no higher valued offset will ever be
// committed. If the provided value is lower than an already provided value an
// error is returned.
func (c *Capped[T]) Track(ctx context.Context, payload T, batchSize int64) (func() *T, error)

func (c *Capped[T]) Highest() *T
func (c *Capped[T]) Pending() int64
```

The blocking-cap is the back-pressure valve, and it is four lines:

```go
	pending := c.t.Pending()
	for pending > 0 && pending+batchSize > c.cap {
		c.cond.Wait()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pending = c.t.Pending()
	}
```

Underneath, `Uncapped[T]` is a doubly-linked list where each node holds a `payload T` and
a cumulative `position int64`, and resolution splices the node out, promoting its payload
into the predecessor (or into the committed checkpoint if it is the head):

```go
// Uncapped keeps track of a sequence of pending checkpoint payloads, and as
// pending checkpoints are resolved it retains the latest fully resolved payload
// in the sequence where all prior sequence checkpoints are also resolved.
//
// Also keeps track of the logical size of the unresolved sequence, which allows
// for limiting the number of pending checkpoints.
type Uncapped[T any] struct {
	checkpointPosition int64
	checkpoint         *T
	start, end *node[T]
}

func (t *Uncapped[T]) Track(payload T, batchSize int64) func() *T {
	newNode := &node[T]{payload: payload, position: batchSize}
	if t.start == nil { t.start = newNode }
	if t.end != nil {
		newNode.prev = t.end
		newNode.position += t.end.position
		t.end.next = newNode
	} else {
		newNode.position += t.checkpointPosition
	}
	t.end = newNode

	return func() *T {
		if newNode.prev != nil {
			newNode.prev.position = newNode.position
			newNode.prev.payload = newNode.payload
			newNode.prev.next = newNode.next
		} else {
			tmp := newNode.payload
			t.checkpoint = &tmp
			t.checkpointPosition = newNode.position
			t.start = newNode.next
		}
		if newNode.next != nil {
			newNode.next.prev = newNode.prev
		} else {
			t.end = newNode.prev
		}
		return t.checkpoint
	}
}
```

**Semantics to internalise:** `Track` returns a `resolve func() *T`. Calling it returns
non-nil *only when this resolution advanced the committed prefix*, in which case the value
is the new highest safely-committable payload. `nil` means "resolved, but an earlier
tracked item is still outstanding — commit nothing." `T` is opaque, so it works for a
Kafka offset, an LSN, a binlog coordinate, a resume token, a file+byte offset — anything.
`batchSize` is *logical weight*, which is how the cap counts messages while the payload is
per-batch.

This is the single best artefact in the prior art. **Canal should own an equivalent as a
core type, generic over the position type, and hand it to connectors** — rather than
leaving it as an out-of-tree dependency each connector must discover.

### 3.5 Composing it: the MySQL CDC input, end to end

Config (`connect/internal/impl/mysql/input_mysql_stream.go`) — the checkpoint store is
declared as a *reference to a cache resource*, i.e. plugged, not built in:

```go
	fieldCheckpointKey             = "checkpoint_key"
	fieldCheckpointCache           = "checkpoint_cache"
	fieldCheckpointLimit           = "checkpoint_limit"
```

```go
		service.NewStringField(fieldCheckpointCache).
			Description("A ... cache resource ... to use for storing the current latest BinLog Position that has been successfully delivered, this allows Redpanda Connect to continue from that BinLog Position upon restart, rather than consume the entire state of the table."),
		service.NewStringField(fieldCheckpointKey).
			Description("The key to use to store the snapshot position in `"+fieldCheckpointCache+"`. An alternative key can be provided if multiple CDC inputs share the same cache."),
```

Construction validates the referenced resource exists at build time:

```go
	if i.binLogCache, err = conf.FieldString(fieldCheckpointCache); err != nil { ... }
	if !mgr.HasCache(i.binLogCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", i.binLogCache)
	}
	if i.binLogCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil { ... }
	i.cp = checkpoint.NewCapped[*position](int64(i.checkPointLimit))
```

Batch emission tracks a position and installs a commit-on-ack closure:

```go
func (i *mysqlStreamInput) flushBatch(
	ctx context.Context,
	checkpointer *checkpoint.Capped[*position],
	batch service.MessageBatch,
	checkpointPos *position,
) error {
	if len(batch) == 0 {
		return nil
	}

	resolveFn, err := checkpointer.Track(ctx, checkpointPos, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("tracking checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			i.checkpointMu.Lock()
			defer i.checkpointMu.Unlock()
			maxOffset := resolveFn()
			// Nothing to commit, this wasn't the latest message
			if maxOffset == nil {
				return nil
			}
			offset := *maxOffset
			// This has no offset - it's a snapshot message
			if offset == nil {
				return nil
			}
			return i.setCachedBinlogPosition(ctx, *offset)
		},
	}
	select {
	case i.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *mysqlStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.msgChan:
		return m.msg, m.ackFn, nil
	case <-i.shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}
```

Restore, on `Connect`:

```go
	pos, err := i.getCachedBinlogPosition(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached binlog position: %s", err)
	}
```

and the cache access itself (note `Resources.AccessCache`'s callback form, and the
double error unwrap):

```go
		cacheVal, cErr = c.Get(ctx, i.binLogCacheKey)
	// ...
	} else if cErr != nil {
		return nil, fmt.Errorf("unable read checkpoint from cache: %w", cErr)
	} else if cacheVal == nil {
		// (treated as "no checkpoint")
	}
	pos, err := parseBinlogPosition(string(cacheVal))
```

Position serialisation is a hand-rolled, deliberately sortable string:

```go
func binlogPositionToString(pos position) string {
	// Pad the position so this string is lexicographically ordered.
	return fmt.Sprintf("%s@%08X", pos.Name, pos.Pos)
}
```

**Four things worth stealing verbatim:**

- **`_ error` in the ack signature.** `ackFn: func(ctx context.Context, _ error) error`.
  The input *ignores nacks entirely* — because it delegates nack handling to
  `AutoRetryNacks` (§9.3). A nack therefore also advances the checkpoint. This is only
  sound because the retry wrapper guarantees redelivery before the ack propagates. It is
  extremely easy to get wrong and completely invisible in the type system.
- **Checkpoints only advance to transaction boundaries.** From `readMessages`:
  ```go
	// latestXIDPos tracks the most recently committed transaction boundary.
	// Checkpoints only advance to XID positions so that on restart canal.RunFrom
	// always resumes at the start of a new transaction, ensuring TABLE_MAP_EVENTs
	// are received before any row events.
	var latestXIDPos *position
  ```
  The committed position is *not* the position of the last delivered record; it is the
  last transaction boundary at or before it. **Canal's position type must be able to
  express "safe resume point" separately from "record position."**
- **Position is nullable to mean "not resumable."** Snapshot records carry
  `checkpointPos == nil`, and the ack closure explicitly skips committing for them
  (`// This has no offset - it's a snapshot message`).
- **A mutex around commit even though `Capped` is thread-safe.** `i.checkpointMu` serialises
  the *cache write*, not the resolve — because two concurrent resolves could otherwise
  write positions out of order.

### 3.6 Heartbeats: keeping the source's own retention from exploding

An externality of "the source owns progress" that canal will hit: if you never ack, the
upstream cannot reclaim log space. Postgres CDC handles it in config
(`input_pg_stream.go`):

> `heartbeat_interval` — "The interval at which to write heartbeat messages. Heartbeat
> messages are needed in scenarios when the subscribed tables are low frequency, but there
> are other high frequency tables writing. Due to the checkpointing mechanism for
> replication slots, not having new messages to acknowledge will prevent postgres from
> reclaiming the write ahead log, which can exhaust the local disk. Having heartbeats allows
> Redpanda Connect to safely acknowledge data periodically and move forward the committed
> point in the log so it can be reclaimed. Setting the duration to 0s will disable
> heartbeats entirely. Heartbeats are created by periodically writing logical messages to
> the write ahead log using `pg_logical_emit_message`."

`checkpoint_limit`'s description states the ordering rule that makes the whole thing
at-least-once:

> "The maximum number of messages that can be processed at a given time. Increasing this
> limit enables parallel processing and batching at the output level. Any given LSN will
> not be acknowledged unless all messages under that offset are delivered in order to
> preserve at least once delivery guarantees."

### 3.7 Verdict for canal

The ack graph is **necessary but not sufficient**. Canal wants both:

- Keep the ack callback as the *composition* primitive — it is what makes fan-out,
  filtering, expansion and rebatching work without core knowledge, and it is what keeps
  sinks progress-free.
- Add an **explicit, first-class position/checkpoint abstraction in the core** that the
  ack graph *resolves into*: a generic `Position` (opaque, comparable-by-source,
  serialisable), a core-owned contiguous-prefix resolver (the `Capped` algorithm), a
  `CheckpointStore` with versioned/CAS writes and fencing, and framework-owned commit
  scheduling with commit-failure escalation. Then the framework can answer "where are we",
  a UI can render it, and multi-worker coordination has something to own.

That is the single largest deliberate divergence canal should make from this prior art.

---

## 4. Snapshot handling

**Not modelled in the core. At all.** There is no snapshot phase, no `Snapshot()` method,
no bounded-vs-unbounded flag on `Input`, no phase state machine. What the core provides is
exactly two primitives that a connector can build a snapshot on:

- `ErrEndOfInput` — "Read will no longer be called and the pipeline will gracefully
  terminate." That gives you *pure batch* pipelines: a `sql_select`/`file`/`csv` input runs
  to completion and the process exits cleanly.
- `AckFunc` + a `Cache` — lets a connector remember "snapshot already done".

Everything else is per-connector. Here is what a real hybrid snapshot-then-stream source
looks like, because this is the pattern canal must design *into* the core.

### 4.1 The handoff, as implemented (MySQL)

Phase selection happens in `Connect`, keyed on the presence of a checkpoint
(`connect/internal/impl/mysql/input_mysql_stream.go`):

```go
	pos, err := i.getCachedBinlogPosition(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached binlog position: %s", err)
	}
	// create snapshot instance if we were requested and haven't finished it before.
	var snapshot *Snapshot
	if i.streamSnapshot && pos == nil {
		db, err := sql.Open("mysql", i.mysqlConfig.FormatDSN())
		if err != nil {
			return fmt.Errorf("connecting to MySQL server: %s", err)
		}
		snapshot = NewSnapshot(i.logger, db)
	}
```

Then three goroutines under one `errgroup`, all owned by the connector:

```go
		wg.Go(func() error { return i.readMessages(ctx) })
		wg.Go(func() error { return i.startMySQLSync(ctx, pos, snapshot) })
```

and the handoff itself:

```go
func (i *mysqlStreamInput) startMySQLSync(ctx context.Context, pos *position, snapshot *Snapshot) error {
	// If we are given a snapshot, then we need to read it.
	if snapshot != nil {
		startPos, err := snapshot.prepareSnapshot(ctx, i.tables, i.snapshotMaxWorkers)
		if err != nil { _ = snapshot.close(); return fmt.Errorf("unable to prepare snapshot: %w", err) }
		if err = i.readSnapshot(ctx, snapshot); err != nil { ... }
		if err = snapshot.releaseSnapshot(ctx); err != nil { ... }
		if err = snapshot.close(); err != nil { ... }
		// Signal snapshot completion. readMessages will flush any partial batch
		// and pre-resolve a checkpoint entry for startPos so the cache is
		// updated once the last snapshot batch is acknowledged.
		select {
		case i.rawMessageEvents <- MessageEvent{Operation: messageOperationSnapshotComplete, Position: startPos}:
		case <-ctx.Done():
			return ctx.Err()
		}
		pos = startPos
		// ...
	} else if pos == nil {
		coords, err := i.canal.GetMasterPos()
		if err != nil { return fmt.Errorf("unable to get start binlog position: %w", err) }
		pos = &coords
	}
	i.logger.Infof("starting MySQL CDC stream from binlog %s at offset %d", pos.Name, pos.Pos)
	i.currentBinlogName = pos.Name
	i.canal.SetEventHandler(i)
	if err := i.canal.RunFrom(*pos); err != nil {
		return fmt.Errorf("starting streaming: %w", err)
	}
	return nil
}
```

The correctness argument: `prepareSnapshot` captures the log position **as of the snapshot's
consistent read point** and returns it; streaming then starts *from that exact position*,
so no change is missed and changes concurrent with the snapshot are replayed (hence
at-least-once, upsert-shaped output). The consistency mechanism (`snapshot.go`) is
`FLUSH TABLES ... WITH READ LOCK` → open N worker connections → `START TRANSACTION WITH
CONSISTENT SNAPSHOT` on each → read log coordinates → `UNLOCK TABLES`:

```go
	// inside the FLUSH TABLES WITH READ LOCK window so all readers see identical data.
	// ...
	// FLUSH TABLES WITH READ LOCK prevents any writes to tables while we:
	// ...
	// across multiple tables — START TRANSACTION WITH CONSISTENT SNAPSHOT
	// ...
		if _, txErr = tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); txErr != nil {
	// ...
	// "SHOW BINARY LOG STATUS" replaces "SHOW MASTER STATUS" IN MySQL 8.4+
		if err = scanRow(s.lockConn.QueryRowContext(ctx, "SHOW MASTER STATUS")); err != nil {
```

And the completion checkpoint is *pre-resolved* so that the boundary position lands in the
cache once the final snapshot batch is acked (`readMessages`):

```go
			if me.Operation == messageOperationSnapshotComplete {
				// Flush any remaining messages before post snapshot checkpoint
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil { return fmt.Errorf("flushing snapshot completion batch: %w", err) }
				if err := i.flushBatch(ctx, i.cp, flushedBatch, latestXIDPos); err != nil { ... }

				if me.Position != nil {
					resolveFn, err := i.cp.Track(ctx, me.Position, 1)
					if err != nil { return fmt.Errorf("tracking snapshot completion checkpoint: %w", err) }

					// No mutex needed: checkpoint.Capped is thread-safe and snapshot batches never write to the cache
					if maxOffset := resolveFn(); maxOffset != nil {
						if offset := *maxOffset; offset != nil {
							if err := i.setCachedBinlogPosition(ctx, *offset); err != nil {
								return fmt.Errorf("persisting snapshot checkpoint: %w", err)
							}
							i.logger.Infof("Checkpointed binlog position following snapshot")
						}
					}
				}
				nextTimedBatchChan = nil
				continue
			}
```

### 4.2 Chunking and parallelism — connector-owned, keyset pagination

Parallelism is *per table*, one worker transaction each, from a bounded pool
(`readSnapshot`):

```go
	tableQueue := make(chan string, len(i.tables))
	for _, table := range i.tables { tableQueue <- table }
	close(tableQueue)

	wg, wgCtx := errgroup.WithContext(ctx)
	for _, tx := range snapshot.workerTxs {
		wg.Go(func() error {
			for table := range tableQueue {
				if err := i.snapshotTable(wgCtx, snapshot, tx, table); err != nil { return err }
			}
			return nil
		})
	}
	return wg.Wait()
```

Chunking within a table is **keyset pagination on the primary key**, not `OFFSET`:

```go
	tablePks, err := snapshot.getTablePrimaryKeys(ctx, tx, table)
	// ...
	lastSeenPksValues := map[string]any{}
	for _, pk := range tablePks {
		lastSeenPksValues[pk] = nil
	}
```

with the query built as `... ORDER BY <pk cols> ... LIMIT ?` (`querySnapshotTable`,
`buildOrderByClause`, `quoteIdentifiers`). Hence the hard config requirement, stated in
the Postgres field description:

> `stream_snapshot` — "When set to true, the plugin will first stream a snapshot of all
> existing data in the database before streaming changes. In order to use this the tables
> that are being snapshot MUST have a primary key set so that reading from the table can be
> parallelized. Note that this has no effect if `tables` is left empty, since the snapshot
> is only planned for tables listed there."

Postgres exposes the same knobs under different names:
`snapshot_batch_size` ("The number of rows to fetch in each batch when querying the
snapshot", default 1000), `max_parallel_snapshot_tables` ("Int specifies a number of tables
that will be processed in parallel during the snapshot processing stage", default 1), and a
`snapshot_memory_safety_factor` that is **`.Deprecated()`** in the spec — a fossil of a
memory-based throttle that didn't work out.

### 4.3 Resumability: it isn't

This is the concrete gap, and it follows directly from having no snapshot model:

- The snapshot runs only when `pos == nil`, i.e. **when no checkpoint exists**.
- During the snapshot, `latestXIDPos` is `nil`, so every snapshot batch is tracked with a
  `nil` position, and the ack closure explicitly declines to commit
  (`// This has no offset - it's a snapshot message`).
- The first checkpoint is written **only at `snapshot_complete`**.

Therefore: **a crash at 90% of a 500M-row snapshot restarts the snapshot from zero.** There
is no per-table cursor, no "snapshot in progress" state, no resume token for the scan.
Worse, the phase decision is a two-valued function of one cache key
(`pos == nil` → snapshot), so there is no representation for "partially snapshotted".

This is also why `mysqlStreamInput` needs `snapshot != nil` threaded through `Connect` →
goroutine → `startMySQLSync`: phase is an ad-hoc variable rather than persisted state.

### 4.4 What canal should model instead

The prior art tells you the *requirements* precisely, and tells you the core is the wrong
place to leave them out:

1. **Phase is durable state, not a nil check.** Persist `{phase, snapshot cursor per split,
   stream position}`. Phases: `pending → snapshotting → catch-up → streaming` (and back to
   `snapshotting` for an added table).
2. **A snapshot is a set of splits with independent cursors.** "Split" is generic: table +
   PK range, file + byte range, shard + key range, collection + `_id` range. Splits are the
   unit of parallelism *and* of resumability *and* of work assignment (§12). One concept,
   three jobs.
3. **The handoff needs a "consistent position at snapshot start" contract**, and the
   framework should own the invariant `stream_from = position_captured_at_snapshot_start`
   rather than each connector re-deriving it.
4. **Snapshot records must be distinguishable and must not advance the stream position.**
   Their model does this with `nil` position + `operation: read`; canal should make it a
   typed property.
5. **`ErrEndOfInput` is the right way to end a bounded pipeline** and canal should keep it —
   it makes batch/snapshot-only pipelines a special case of the same runtime, with no
   separate "batch mode".
6. **`EndOfInput()` + `ErrEndOfBuffer` from `BatchBuffer` is the right way to propagate
   end-of-phase through a stateful stage.** Reuse that shape for phase transitions rather
   than inventing a signalling channel per connector (which is what the MySQL input does
   with its internal `messageOperationSnapshotComplete` sentinel on
   `i.rawMessageEvents`).

Note also what they got *right* and canal should copy: the snapshot emits into **the same
record envelope and the same batch/ack machinery** as the stream. There is no separate
snapshot pipeline, no separate sink path, no separate serialisation. Snapshot rows are just
records with `operation: read` and a nil position. That is the correct instinct — one
pipeline, two producers.

---

## 5. Schema handling

Two entirely separate stories: the *config* schema (rich, first-class, §7) and the *data*
schema (absent from the core until very recently, then added as a side-car library).

### 5.1 Historically: no data schema at all

The record envelope has no schema field (§2.1). The structured payload is
constrained to "scalar / `map[string]any` / `[]any` all the way down"
(`SetStructured` doc comment). Schema was purely an in-band property of the bytes,
handled by *processors* — `avro`, `protobuf`, `parquet_encode/decode`,
`schema_registry_encode/decode` in `connect/internal/impl/{avro,protobuf,parquet,confluent}`.
Format conversion was N×M point-to-point.

### 5.2 Now: `public/schema` — a canonical, fingerprinted, out-of-band schema type

Added recently (CHANGELOG 4.72.0/4.73.0, 2026-05) as a *sibling* of `public/service`, not a
change to the record envelope. `benthos/public/schema/common.go`:

```go
// Package schema implements a common standard for describing data schemas
// within the domain of benthos. The intention for these schemas is to encourage
// schema conversion between multiple common formats such as avro, parquet, and
// so on.
```

```go
// CommonType represents types supported by common schemas.
type CommonType int

// Supported common types
const (
	Boolean    CommonType = 1
	Int32      CommonType = 2
	Int64      CommonType = 3
	Float32    CommonType = 4
	Float64    CommonType = 5
	String     CommonType = 6
	ByteArray  CommonType = 7
	Object     CommonType = 8
	Map        CommonType = 9
	Array      CommonType = 10
	Null       CommonType = 11
	Union      CommonType = 12
	Timestamp  CommonType = 13
	Any        CommonType = 14
	Decimal    CommonType = 15
	BigDecimal CommonType = 16
	Date       CommonType = 17
	TimeOfDay  CommonType = 18
	UUID       CommonType = 19
)
```

```go
type Common struct {
	Name     string
	Type     CommonType
	Optional bool
	Children []Common

	// Logical holds parameters for parameterised types (e.g. Decimal). Only
	// the field within LogicalParams that corresponds to Type should be
	// populated; setting parameters that do not apply to the type is a
	// validation error.
	Logical *LogicalParams
}

// LogicalParams groups the optional parameter blocks that parameterised
// CommonType values may carry. Each parameterised type has its own field;
// at most one is expected to be non-nil for any given Common schema.
type LogicalParams struct {
	Decimal   *DecimalParams
	Timestamp *TimestampParams
	TimeOfDay *TimeOfDayParams
}

// DecimalParams describes a fixed-precision decimal number.
//
// Precision is the total number of significant digits and must be in
// [DecimalMinPrecision, DecimalMaxPrecision]. Scale is the number of digits
// to the right of the decimal point and must be in [0, Precision]. These
// constraints describe the lossless intersection across Avro, Parquet and
// Oracle NUMBER.
type DecimalParams struct {
	Precision int32
	Scale     int32
}

// TimestampParams describes the precision and timezone semantics of a
// [Timestamp] schema. Unit selects the resolution at which the timestamp is
// expressed; AdjustToUTC distinguishes a UTC instant (true) from a civil /
// "local" datetime that carries no timezone offset (false).
//
// A nil [LogicalParams.Timestamp] on a [Timestamp]-typed schema is permitted
// for backwards compatibility and is treated as {Unit: TimeUnitMillis,
// AdjustToUTC: true}; see [Common.EffectiveTimestamp].
type TimestampParams struct {
	Unit        TimeUnit
	AdjustToUTC bool
}

// TimeOfDayParams ...
// Unlike [TimestampParams], a [TimeOfDay]-typed schema must have non-nil
// [LogicalParams.TimeOfDay] — there is no historical default to fall back to.
type TimeOfDayParams struct {
	Unit        TimeUnit
	AdjustToUTC bool
}
```

```go
// Decimal precision bounds. The upper bound matches the widest precision that
// can be represented losslessly across Avro, Parquet and Oracle NUMBER.
const (
	DecimalMinPrecision int32 = 1
	DecimalMaxPrecision int32 = 38
)
```

Design points worth taking:

- **The type set is the intersection of what the ecosystem can round-trip**, explicitly
  justified in the comments ("the lossless intersection across Avro, Parquet and Oracle
  NUMBER"). It is not a superset and not a JSON-shaped minimum.
- **Parameterised logical types are a separate optional struct**, one field per
  parameterised type, with a validation rule ("setting parameters that do not apply to the
  type is a validation error"). This is how they added `Decimal`, `Timestamp` units and
  `TimeOfDay` *without* widening `CommonType` into a combinatorial mess, and without
  breaking existing schemas.
- **Backwards compatibility is spelled out per type.** `Timestamp` with `nil` params means
  `{Millis, UTC}`; `TimeOfDay` with nil params is invalid because there is no legacy default.
  There is an `EffectiveTimestamp()` accessor that applies the default:
  ```go
	return TimestampParams{Unit: TimeUnitMillis, AdjustToUTC: true}
  ```
- **`BigDecimal` exists specifically for sources that can't tell you precision.** From the
  CHANGELOG: "Use it for sources that lack column-level precision (Postgres `numeric`
  without `(p, s)`, Oracle `NUMBER` with no `DATA_PRECISION`, MongoDB `Decimal128`)." A
  canonical type system for data movement needs an "unbounded/unknown precision" escape
  hatch or it will silently corrupt data.

### 5.3 Propagation: `ToAny`, `ParseFromAny`, and a self-describing fingerprint

```go
const (
	anyFieldType        = "type"
	anyFieldName        = "name"
	anyFieldOptional    = "optional"
	anyFieldChildren    = "children"
	anyFieldFingerprint = "fingerprint"
	anyFieldPrecision   = "precision"
	anyFieldScale       = "scale"
	anyFieldUnit        = "unit"
	anyFieldAdjustToUTC = "adjust_to_utc"
)

// ToAny serializes the common schema into a generic Go value, with structured
// schemas being represented as map[string]any and []any. This could be further
// manipulated using generic mapping tools such as bloblang, before either
// bringing back into a Common representation or serializing into another
// format.
//
// The serialized format includes a "fingerprint" field at the top level, which
// can be used to optimize cache lookups via SchemaCache.GetOrConvertFromAny,
// avoiding the need to parse the Any format and recalculate the fingerprint.
//
// NOTE: Ironically, the schema for this serialization is not something that can
// be precisely represented as a Common schema. The Children field requires an
// Array of structured schema objects, which cannot be described accurately
// within the Common type system.
func (c *Common) ToAny() any

// ParseFromAny deserializes a common schema from a generic Go value. The
// resulting schema is validated via [Common.Validate] before being returned.
func ParseFromAny(v any) (Common, error)
```

That `NOTE:` is an honest and instructive admission: the canonical type system is not
expressive enough to describe itself (no array-of-object with heterogeneous recursion).
Canal will hit the same wall; better to know now that the schema-of-schemas is a separate
concern from the schema-of-data.

Parameters are only emitted when present, deliberately, to preserve fingerprint stability:

```go
	// Timestamp parameters are only emitted when present, so legacy schemas
	// (Type == Timestamp with nil Logical) keep their pre-parameterised
	// fingerprint and ToAny output exactly.
```

Because `ToAny()` produces `map[string]any`, a schema is **manipulable by the same
transform language as the data** ("could be further manipulated using generic mapping tools
such as bloblang"). That is an elegant unification: schema mapping needs no second DSL.

Fingerprinting: `(*Common).fingerprint()` is SHA-256 over a canonical structural encoding
(type, name, optional, params, child count, recurse) returned hex-encoded, and it is
**unexported** — you only ever see it via `ToAny`'s `fingerprint` field or via the cache.

```go
// Two schemas with the same structure will produce the same fingerprint,
// ...
// The fingerprint is computed using SHA-256 and returned as a hex-encoded string.
func (c *Common) fingerprint() string
```

### 5.4 Conversion caching: the N×M problem, solved once, generically

`benthos/public/schema/cache.go`:

```go
type ConvertFunc[T any] func(Common) (T, error)

type Cache[T any] struct { /* ... */ }

func NewCache[T any](convert ConvertFunc[T]) *Cache[T]
func (sc *Cache[T]) GetOrConvert(schema Common) (T, error)
func (sc *Cache[T]) GetOrConvertFromAny(anySchema any) (T, error)
func (sc *Cache[T]) Size() int
func (sc *Cache[T]) Clear()
```

with the intended usage documented in the package comment:

```go
//	// Create a cache for Parquet schema conversions
//	cache := schema.NewSchemaCache(func(c schema.Common) (ParquetSchema, error) {
//	    return convertToParquet(c)
//	})
//
//	// First access converts and caches
//	parquet1, err := cache.GetOrConvert(mySchema)
//
//	// Second access uses cached result (no conversion)
//	parquet2, err := cache.GetOrConvert(mySchema)
```

and the fast path:

```go
//	// Consumer side: optimized cache lookup
//	cache := schema.NewSchemaCache(convertFunc)
//	result, err := cache.GetOrConvertFromAny(anySchema)
//	// Fast path: if cached, avoids ParseFromAny and Fingerprint calculation
```

This is exactly right and directly transplantable: **a sink declares one
`Common → NativeSchema` conversion function and gets memoised, drift-safe schema handling
for free.** Every sink writes one converter, not one converter per source.

### 5.5 Discovery and drift

Discovery is per-connector. The MySQL input maintains `i.tableSchemas`, extracts on first
sight, and attaches via immutable metadata:

```go
			if tableSchema := i.getOrExtractTableSchemaByName(me.Table); tableSchema != nil {
				mb.MetaSetImmut("schema", service.ImmutableAny{V: tableSchema})
			}
```

Drift is handled by cache invalidation on DDL:

```go
// We invalidate the cached schema so it will be re-extracted on the next row event.
	// Only invalidate cache for tables we're tracking
	// ...
		i.logger.Infof("Schema cache invalidated for table %s.%s due to DDL change", schema, table)
```

(helpers: `getTableSchema`, `getOrExtractTableSchemaByName`, `invalidateTableSchema` in
`connect/internal/impl/mysql/input_mysql_stream.go`; `schema.go` in the same package does
the extraction.)

So the propagation model is: **out-of-band schema, carried per-record as immutable
metadata under a conventional key (`"schema"`), invalidated on DDL, converted lazily and
memoised by fingerprint at the sink.** There is no schema registry in the core, no
schema-change *event* in the stream, no compatibility checking, and no versioning — a DDL
change simply produces later records with a different attached schema, and the sink's
fingerprint-keyed cache converts the new one on demand.

### 5.6 Verdict for canal

Take almost all of this, but promote it:

- Adopt the **canonical `Common`-style type set defined as the lossless intersection** of
  the formats you intend to support, with a separate optional `LogicalParams` for
  parameterised types and an explicit unknown-precision type.
- Adopt **structural fingerprinting** as the schema identity, and the
  **`Cache[T]` + one `ConvertFunc[T]` per sink** pattern. This is the correct answer to N×M.
- Adopt **`ToAny`/`ParseFromAny` into the generic value space** so schemas are
  transformable by the same machinery as data.
- **Diverge**: put schema on the record envelope as a typed, optional facet rather than a
  conventional metadata string key, and model a **schema-change event** in the stream
  (so a sink can DDL its target *before* the first record that needs it) rather than
  leaving sinks to discover drift record-by-record. Also decide the compatibility policy in
  the core (reject / evolve / dead-letter), because "attach whatever we found" pushes an
  unanswerable question onto every sink.

---

## 6. Lifecycle

### 6.1 The public plugin lifecycle is four calls and no states

From the connector author's point of view there is no state machine to implement:

```
ctor(conf *ParsedConfig, mgr *Resources)   → construct; no I/O expected
Connect(ctx) error                          → called first, retried with backoff until nil
Read(ctx) / ReadBatch(ctx) / Write(ctx,…)   → called repeatedly while connected
   ↳ ErrNotConnected                        → back to Connect
   ↳ ErrEndOfInput / ErrEndOfBuffer         → graceful pipeline termination
Close(ctx) error                            → blocks until cleaned up or ctx cancelled
[optional] ConnectionTest(ctx)              → probe, "does not require the component to be initialized"
```

There is **no** `Configure`/`Open`/`Start`/`Stop`/`Commit`/`Teardown`. Configuration
happens in the constructor (which receives an already-parsed, already-validated, already-
defaulted config — §7). Commit does not exist as a lifecycle callback; it happens inside
an ack closure (§3).

That is a remarkably small surface and it is the main reason "implement the interface,
register it, done" is actually true here. Canal should keep the count this low; each extra
callback is a state a connector author can get wrong.

### 6.2 The driver owns all the retry/backoff policy

`internal/component/input/async_reader.go` — two independent backoffs, both created by the
framework, neither configurable by the plugin:

```go
	connBoff := backoff.NewExponentialBackOff()
	connBoff.InitialInterval = time.Millisecond * 100
	connBoff.MaxInterval = time.Second
	connBoff.MaxElapsedTime = 0

	readBoff := backoff.NewExponentialBackOff()
	readBoff.InitialInterval = time.Millisecond * 100
	readBoff.MaxInterval = time.Second
	readBoff.MaxElapsedTime = 0
```

The connect loop:

```go
	initConnection := func() bool {
		for {
			if r.shutSig.IsSoftStopSignalled() {
				return false
			}
			if err := r.reader.Connect(closeAtLeisureCtx); err != nil {
				if r.shutSig.IsSoftStopSignalled() || errors.Is(err, component.ErrTypeClosed) {
					return false
				}
				r.connection.Store(component.ConnectionFailing(r.mgr, err))
				r.mgr.Logger().Error("Failed to connect to %v: %v", r.typeStr, err)
				mFailedConn.Incr(1)

				var nextBoff time.Duration

				var e *component.ErrBackOff
				if errors.As(err, &e) {
					nextBoff = e.Wait
				} else {
					nextBoff = r.connBackoff.NextBackOff()
				}

				if nextBoff == backoff.Stop {
					r.mgr.Logger().Error("Maximum number of connection attempt retries has been met, gracefully terminating input %v", r.typeStr)
					return false
				}
				if sleepWithCancellation(closeAtLeisureCtx, nextBoff) != nil {
					return false
				}
			} else {
				r.connBackoff.Reset()
				return true
			}
		}
	}
	if !initConnection() {
		return
	}
```

and the read loop's error classification:

```go
		// If our reader says it is not connected.
		if errors.Is(err, component.ErrNotConnected) {
			mLostConn.Incr(1)
			r.connection.Store(component.ConnectionFailing(r.mgr, component.ErrNotConnected))
			// Continue to try to reconnect while still active.
			if !initConnection() { return }
			mConn.Incr(1)
			r.connection.Store(component.ConnectionActive(r.mgr))
			continue
		}

		// Close immediately if our reader is closed.
		if r.shutSig.IsSoftStopSignalled() || errors.Is(err, component.ErrTypeClosed) {
			return
		}

		if err != nil || len(msg) == 0 {
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, component.ErrTimeout) && !errors.Is(err, component.ErrNotConnected) {
				r.mgr.Logger().Error("Failed to read message: %v\n", err)
			}
			nextBoff := r.readBackoff.NextBackOff()
			if nextBoff == backoff.Stop {
				r.mgr.Logger().Error("Maximum number of read attempt retries has been met, gracefully terminating input %v", r.typeStr)
				return
			}
			select {
			case <-time.After(nextBoff):
			case <-r.shutSig.SoftStopChan():
				return
			}
			continue
		}

		r.readBackoff.Reset()
```

Note `err != nil || len(msg) == 0` — **an empty batch is treated identically to a
transient error** and triggers read backoff. That's a subtle and useful convention: a
polling source can return `(nil, nil, nil)` to mean "nothing right now" and get free
backoff, without inventing an `ErrNoData`.

### 6.3 Error taxonomy

`public/service/errors.go` — the entire public error vocabulary is three sentinels plus two
structured errors:

```go
var (
	// ErrNotConnected is returned by inputs and outputs when their Read or
	// Write methods are called and the connection that they maintain is lost.
	// This error prompts the upstream component to call Connect until the
	// connection is re-established.
	ErrNotConnected = errors.New("not connected")

	// ErrEndOfInput is returned by inputs that have exhausted their source of
	// data to the point where subsequent Read calls will be ineffective. This
	// error prompts the upstream component to gracefully terminate the
	// pipeline.
	ErrEndOfInput = errors.New("end of input")

	// ErrEndOfBuffer is returned by a buffer Read/ReadBatch method when the
	// contents of the buffer has been emptied and the source of the data is
	// ended (as indicated by EndOfInput). This error prompts the upstream
	// component to gracefully terminate the pipeline.
	ErrEndOfBuffer = errors.New("end of buffer")
)
```

Plus a *plugin-controlled backoff override*, which is the one place a connector can
influence framework retry policy — and note the explicit, honest scope limitation:

```go
// ErrBackOff is an error that plugins can optionally wrap another error with
// which instructs upstream components to wait for a specified period of time
// before retrying the errored call.
//
// Not all plugin methods support this error, for a list refer to the
// documentation of NewErrBackOff.
type ErrBackOff struct {
	Err  error
	Wait time.Duration
}

// NewErrBackOff wraps an error with a specified time to wait. ...
// NOTE: ErrBackOff is opt-in for upstream components and therefore only a
// subset of plugin calls will respect this error. Currently the following
// methods are known to support ErrBackOff:
//
// - Input.Connect
// - BatchInput.Connect
// - Output.Connect
// - BatchOutput.Connect
func NewErrBackOff(err error, wait time.Duration) *ErrBackOff
```

"Only a subset of plugin calls will respect this error" is a design smell: a
retry-hint error type whose effect depends on which method you returned it from is not
discoverable. **Canal should make retry classification explicit and total** — e.g. a single
`ErrorClass` (Retryable / RetryableAfter(d) / NotConnected / Terminal / Poison) that every
call site honours, so a connector author cannot return something the framework ignores.

### 6.4 The "air gap" error translation

The public↔internal boundary translates sentinels in both directions, which is worth seeing
because it is where a bespoke framework's error vocabulary earns its keep
(`public/service/errors.go`):

```go
func publicToInternalErr(err error) error {
	if err == nil {
		return nil
	}

	var e *ErrBackOff
	if errors.As(err, &e) {
		return &component.ErrBackOff{Err: publicToInternalErr(e.Err), Wait: e.Wait}
	}

	var bErr *BatchError
	if errors.As(err, &bErr) {
		return bErr.wrapped
	}

	if errors.Is(err, ErrEndOfInput) {
		return component.ErrTypeClosed
	}
	if errors.Is(err, ErrEndOfBuffer) {
		return component.ErrTypeClosed
	}
	if errors.Is(err, ErrNotConnected) {
		return component.ErrNotConnected
	}
	return err
}

// If the provided error is not nil and can be cast to an internal batch error
// we return a public batch error.
func toPublicBatchError(err error) error {
	var bErr *batch.Error
	if err != nil && errors.As(err, &bErr) {
		err = &BatchError{wrapped: bErr}
	}
	return err
}
```

Every `airGap*` method is a one-liner wrapping this, e.g.:

```go
func (a *airGapReader) Connect(ctx context.Context) error {
	return publicToInternalErr(a.r.Connect(ctx))
}
```

The lesson for canal: if you want the option of an out-of-process implementation later
(constraint #3), you need this translation layer **anyway** — an error must be reducible to
a small closed set of wire-representable classes. Benthos's gRPC plugin proto does exactly
that (§10.3). Design the closed set first; it is cheaper than retrofitting.

### 6.5 Shutdown: three-phase, with a nack-timeout escape hatch

Shutdown uses `github.com/Jeffail/shutdown`'s `Signaller` with two escalating triggers plus
a terminal signal. From `internal/component/input/interface.go`:

- `TriggerStartConsuming()` — start the loop (idempotent via `sync.Once`)
- `TriggerStopConsuming()` — "instructs the input to start shutting down resources once all
  pending messages are delivered and acknowledged. This call does not block."
- `TriggerCloseNow()` — "triggers the shut down of this component but should not block the
  calling goroutine."
- `WaitForClose(ctx)` — blocks until finished

and inside the driver, the two contexts derived from those:

```go
	closeAtLeisureCtx, calDone := r.shutSig.SoftStopCtx(context.Background())
	defer calDone()

	closeNowCtx, cnDone := r.shutSig.HardStopCtx(context.Background())
	defer cnDone()

	defer func() {
		_ = r.reader.Close(context.Background())
		r.connection.Store(component.ConnectionClosed(r.mgr))
		close(r.transactions)
		r.shutSig.TriggerHasStopped()
	}()
```

Reads use the *soft* context (stop pulling new data promptly); acks use the *hard* context
(keep resolving in-flight acks). Note `r.reader.Close(context.Background())` — Close gets an
un-cancelled context, deliberately.

`Stream.Stop` escalates on the *caller's* deadline (`public/service/stream.go`):

```go
// StopWithin attempts to close the stream within the specified timeout period.
// Initially the attempt is graceful, but as the timeout draws close the attempt
// becomes progressively less graceful.
//
// An ungraceful shutdown increases the likelihood of processing duplicate
// messages on the next start up, but never results in dropped messages as long
// as the input source supports at-least-once delivery.
func (s *Stream) StopWithin(timeout time.Duration) error
```

That doc comment is the delivery-guarantee contract in one sentence, and it is the right
one to promise.

Because "ack may never come" is a real failure mode (§3.3), there is an **experimental
forced-nack timer** — `public/service/input_force_timely_nacks.go`:

```go
// ForceTimelyNacksBatched wraps an input implementation with a timely ack
// mechanism only if a field defined by NewForceTimelyNacksField has been
// specified and is a duration greater than zero.
func ForceTimelyNacksBatched(c *ParsedConfig, i BatchInput) (BatchInput, error)

var errForceTimelyNacks = errors.New("message acknowledgement exceeded maximum wait duration and has been rejected")
```

```go
func (i *forceTimelyNacksInputBatched) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batch, ackFn, err := i.inner.ReadBatch(ctx)
	if err != nil { return nil, nil, err }

	var ackOnce, closeAckedChanOnce sync.Once
	ackedChan := make(chan struct{})

	go func() {
		select {
		case <-ackedChan:
			return
		case <-time.After(i.maxWaitDuration):
			ackOnce.Do(func() {
				i.logger.With("duration", i.maxWaitDuration.String()).Warn("Message acknowledgement exceeded configured maximum wait duration and is being rejected as a result")
				_ = ackFn(context.Background(), errForceTimelyNacks)
			})
		}
	}()

	return batch, func(ctx context.Context, err error) (ackErr error) {
		closeAckedChanOnce.Do(func() { close(ackedChan) })
		ackOnce.Do(func() { ackErr = ackFn(ctx, err) })
		return
	}, nil
}
```

with the config field admitting what it's for:

```go
func NewForceTimelyNacksField() *ConfigField {
	return NewDurationField(ForceTimelyNacksFieldName).
		Description("EXPERIMENTAL: Specify a maximum period of time in which each message can be consumed and awaiting either acknowledgement or rejection before rejection is instead forced. This can be useful for avoiding situations where certain downstream components can result in blocked confirmation of delivery that exceeds SLAs.").
		Advanced().
		Optional()
}
```

One goroutine and a timer **per batch**. That is an expensive fix for a structural problem,
and it exists because the ack contract has no deadline. **Canal should put a deadline in the
contract**: a delivery lease with a timeout the framework owns and enforces centrally
(one timer wheel, not one goroutine per batch).

The `sync.Once`-wrapped ack (`ackOnce`) is itself a pattern worth institutionalising: ack
funcs must be idempotent, and it should be the framework that guarantees that, not each
connector. Note `ackOnce` appears independently in at least three places
(`forceTimelyNacks`, `scanner.ackOnce`, `autoretry.wrapPendingAck`).

### 6.6 The output driver's loop, and the absence of write retry

`internal/component/output/async_writer.go` runs `maxInflight` copies of `writerLoop`:

```go
	wg := sync.WaitGroup{}
	wg.Add(w.maxinflight)   // (spelled maxInflight in source)
	// ...
	for i := 0; i < w.maxInflight; i++ {
		go writerLoop()
	}
	wg.Wait()
```

with reconnect serialised behind a mutex and a "someone else already fixed it" fast path:

```go
	connectMut := sync.Mutex{}
	connectLoop := func(msg message.Batch) (latency int64, err error) {
		w.connection.Store(component.ConnectionFailing(w.mgr, component.ErrNotConnected))

		connectMut.Lock()
		defer connectMut.Unlock()

		// If another goroutine got here first and we're able to send over the
		// connection, then we gracefully accept defeat.
		if w.connection.Load().Connected {
			if latency, err = w.latencyMeasuringWrite(closeLeisureCtx, msg); err != component.ErrNotConnected {
				return
			} else if err != nil {
				mError.Incr(1)
			}
		}
		mLostConn.Incr(1)

		// Continue to try to reconnect while still active.
		for {
			if !initConnection() {
				err = component.ErrTypeClosed
				return
			}
			if latency, err = w.latencyMeasuringWrite(closeLeisureCtx, msg); err != component.ErrNotConnected {
				w.connection.Store(component.ConnectionActive(w.mgr))
				mConn.Incr(1)
				return
			} else if err != nil {
				mError.Incr(1)
			}
		}
	}
```

Crucially: **a non-`ErrNotConnected` write failure is not retried by the driver.** It is
logged and nacked:

```go
			if err != nil {
				if w.typeStr != "reject" {
					// TODO: Maybe reintroduce a sleep here if we encounter a
					// busy retry loop.
					w.log.Error("Failed to send message to %v: %v\n", w.typeStr, err)
				} else {
					w.log.Debug("Rejecting message: %v\n", err)
				}
			} else {
				mBatchSent.Incr(1)
				mSent.Incr(int64(batch.MessageCollapsedCount(payload)))
				mLatency.Timing(latency)
			}
			// ...
			_ = ackFn(closeLeisureCtx, err)
```

Retry of a *write* is therefore either (a) the input replaying after a nack, or (b) an
explicit `retry` output wrapper in config (§9.4). The `TODO: Maybe reintroduce a sleep here
if we encounter a busy retry loop` and the `w.typeStr != "reject"` string comparison are
both blemishes worth noting: special-casing a component by *name* inside the generic driver
is exactly the "core knowledge of the connector" that canal's constraint #4 forbids.

---

## 7. Config model

This is the strongest part of the system and the part canal should copy most directly.
Config is **programmatically declared, self-documenting, machine-exportable, lintable, and
parsed-before-construction**.

### 7.1 Field specs: a fluent builder with a closed type set

`public/service/config.go`:

```go
type ConfigField struct {
	field docs.FieldSpec
}
```

Constructors (the complete list):

```go
func NewAnyField(name string) *ConfigField
func NewAnyListField(name string) *ConfigField
func NewAnyMapField(name string) *ConfigField
func NewStringField(name string) *ConfigField
func NewDurationField(name string) *ConfigField
func NewStringEnumField(name string, options ...string) *ConfigField
func NewStringAnnotatedEnumField(name string, options map[string]string) *ConfigField
func NewStringListField(name string) *ConfigField
func NewStringListOfListsField(name string) *ConfigField
func NewStringMapField(name string) *ConfigField
func NewIntField(name string) *ConfigField
func NewIntListField(name string) *ConfigField
func NewIntMapField(name string) *ConfigField
func NewFloatField(name string) *ConfigField
func NewFloatListField(name string) *ConfigField
func NewFloatMapField(name string) *ConfigField
func NewBoolField(name string) *ConfigField
func NewObjectField(name string, fields ...*ConfigField) *ConfigField
func NewObjectListField(name string, fields ...*ConfigField) *ConfigField
func NewObjectMapField(name string, fields ...*ConfigField) *ConfigField
func NewInternalField(ifield docs.FieldSpec) *ConfigField   // internal use
```

Modifiers (all return `*ConfigField`, all chainable):

```go
func (c *ConfigField) Description(d string) *ConfigField

// ShortDescription adds a plain text summary of the field to be shown as inline
// help within a form UI, where the full Description would be too verbose. It
// should contain no markup and span at most a sentence or two. When unset,
// consumers fall back to the value provided by Description.
func (c *ConfigField) ShortDescription(d string) *ConfigField

// Advanced marks a config field as being advanced, and therefore it will not
// appear in simplified documentation examples.
func (c *ConfigField) Advanced() *ConfigField

func (c *ConfigField) Deprecated() *ConfigField

// Default specifies a default value that this field will assume if it is
// omitted from a provided config. Fields that do not have a default value are
// considered mandatory, and so parsing a config will fail in their absence.
func (c *ConfigField) Default(v any) *ConfigField

// Optional specifies that a field is optional even when a default value has not
// been specified. When a field is marked as optional you can test its presence
// within a parsed config with the method Contains.
func (c *ConfigField) Optional() *ConfigField

// Secret marks this field as being a secret, which means it represents
// information that is generally considered sensitive such as passwords or
// access tokens.
func (c *ConfigField) Secret() *ConfigField

func (c *ConfigField) Example(e any) *ConfigField
func (c *ConfigField) Examples(e ...any) *ConfigField

// Version specifies the specific version at which this field was added to the
// component.
func (c *ConfigField) Version(v string) *ConfigField

// LintRule adds a custom linting rule to the field in the form of a bloblang
// mapping. ...
func (c *ConfigField) LintRule(blobl string) *ConfigField
```

Five of these are pure UI/UX affordances that cost the framework nothing and buy a frontend
everything: `ShortDescription` (explicitly "inline help within a form UI"), `Advanced`
(progressive disclosure), `Secret` (mask it), `Example(s)` (placeholder text), `Version`
(feature gating / "new in"). **`Default` doubles as the required/optional marker** — "Fields
that do not have a default value are considered mandatory" — with `Optional()` as the third
state. That tri-state (`required` / `has default` / `optional-absent`) plus `Contains(path)`
to test presence is the minimum viable model and canal should adopt it as-is.

### 7.2 Component specs

```go
// ConfigSpec describes the configuration specification for a plugin
// component. This will be used for validating and linting configuration files
// and providing a parsed configuration struct to the plugin constructor.
type ConfigSpec struct {
	component docs.ComponentSpec
}

func NewConfigSpec() *ConfigSpec

// Stable / Beta / Deprecated set a documentation label on the component ...
func (c *ConfigSpec) Stable() *ConfigSpec
func (c *ConfigSpec) Beta() *ConfigSpec
func (c *ConfigSpec) Deprecated() *ConfigSpec
func (c *ConfigSpec) SupportLevel(l string) *ConfigSpec
func (c *ConfigSpec) Categories(categories ...string) *ConfigSpec
func (c *ConfigSpec) Version(v string) *ConfigSpec
func (c *ConfigSpec) Summary(summary string) *ConfigSpec
func (c *ConfigSpec) Description(description string) *ConfigSpec
func (c *ConfigSpec) Footnotes(description string) *ConfigSpec

// Field sets the specification of a field within the config spec, used for
// linting and generating documentation for the component.
//
// If the provided field has an empty name then it is registered as the value at
// the root of the config spec.
func (c *ConfigSpec) Field(f *ConfigField) *ConfigSpec
func (c *ConfigSpec) Fields(fs ...*ConfigField) *ConfigSpec

// Example adds an example to the plugin configuration spec that demonstrates
// how the component can be used. An example has a title, summary, and a YAML
// configuration showing a real use case.
func (c *ConfigSpec) Example(title, summary, config string) *ConfigSpec

func (c *ConfigSpec) LintRule(blobl string) *ConfigSpec
func (c *ConfigSpec) EncodeJSON(v []byte) error   // undocumented/experimental
func (c *ConfigSpec) ParseYAML(yamlStr string, env *Environment) (*ParsedConfig, error)  // for tests
```

`Field(f)` with an empty name meaning "the whole component config *is* this field" is how
scalar-configured components work (`file: ./foo.json` instead of `file: {path: …}`). Nice
trick, worth having.

Cross-field validation is a Bloblang mapping over the config object — i.e. **validation
logic is itself data**, which means a UI can run it too:

```go
// LintRule adds a custom linting rule to the ConfigSpec in the form of a
// bloblang mapping. The mapping is provided the value of the fields within
// the ConfigSpec as the context `this`, and if the mapping assigns to `root` an
// array of one or more strings these strings will be exposed to a config author
// as linting errors.
//
// For example, if we wanted to add a linting rule for several ConfigSpec fields
// that ensures some fields are mutually exclusive and some require others we
// might use the following:
//
// root = match {
//   this.exists("meow") && this.exists("woof") => [ "both `meow` and `woof` can't be set simultaneously" ],
//   this.exists("reticulation") && (!this.exists("splines") || this.splines == "") => [ "`splines` is required when setting `reticulation`" ],
// }.
```

This is genuinely clever and I'd flag it as a *maybe* for canal: it makes cross-field rules
declarative and portable to a UI, at the cost of requiring an embedded expression language.
If canal has no such language, the pragmatic substitute is a Go `Validate(*ParsedConfig) error`
hook plus a small declarative set (`mutuallyExclusive`, `requires`, `atLeastOneOf`) that a
UI *can* interpret.

### 7.3 Parsed config: path-addressed typed accessors

```go
// ParsedConfig represents a plugin configuration that has been validated and
// parsed from a ConfigSpec, and allows plugin constructors to access
// configuration fields.
type ParsedConfig struct {
	i   *docs.ParsedConfig
	mgr bundle.NewManagement
}

func (p *ParsedConfig) EngineVersion() string
func (p *ParsedConfig) Resources() *Resources
func (p *ParsedConfig) Namespace(path ...string) *ParsedConfig
func (p *ParsedConfig) Contains(path ...string) bool

func (p *ParsedConfig) FieldAny(path ...string) (any, error)
func (p *ParsedConfig) FieldAnyList(path ...string) ([]*ParsedConfig, error)
func (p *ParsedConfig) FieldAnyMap(path ...string) (map[string]*ParsedConfig, error)
func (p *ParsedConfig) FieldString(path ...string) (string, error)
func (p *ParsedConfig) FieldDuration(path ...string) (time.Duration, error)
func (p *ParsedConfig) FieldStringList(path ...string) ([]string, error)
func (p *ParsedConfig) FieldStringListOfLists(path ...string) ([][]string, error)
func (p *ParsedConfig) FieldStringMap(path ...string) (map[string]string, error)
func (p *ParsedConfig) FieldInt(path ...string) (int, error)
func (p *ParsedConfig) FieldIntList(path ...string) ([]int, error)
func (p *ParsedConfig) FieldIntMap(path ...string) (map[string]int, error)
func (p *ParsedConfig) FieldFloat(path ...string) (float64, error)
func (p *ParsedConfig) FieldFloatList(path ...string) ([]float64, error)
func (p *ParsedConfig) FieldFloatMap(path ...string) (map[string]float64, error)
func (p *ParsedConfig) FieldBool(path ...string) (bool, error)
func (p *ParsedConfig) FieldObjectList(path ...string) ([]*ParsedConfig, error)
func (p *ParsedConfig) FieldObjectMap(path ...string) (map[string]*ParsedConfig, error)
```

`path ...string` (variadic segments, not a dotted string) is the right call — no escaping
problems, no parsing. `Namespace(path…)` re-roots a sub-config so a shared helper can be
written once and mounted anywhere.

The **cost** of this design, and it is real: `FieldX` returns `(T, error)` on every access,
so every constructor is a 20-line ladder of `if x, err = conf.FieldY(…); err != nil { return }`.
See the `FieldBatchPolicy` implementation for the canonical example
(`public/service/config_batch_policy.go`):

```go
func (p *ParsedConfig) FieldBatchPolicy(path ...string) (conf BatchPolicy, err error) {
	if conf.Count, err = p.FieldInt(append(path, "count")...); err != nil {
		return
	}
	if conf.ByteSize, err = p.FieldInt(append(path, "byte_size")...); err != nil {
		return
	}
	if conf.Check, err = p.FieldString(append(path, "check")...); err != nil {
		return
	}
	if conf.Period, err = p.FieldString(append(path, "period")...); err != nil {
		return
	}
	conf.procs, err = p.fieldProcessorListConfigs(append(path, "processors")...)
	return
}
```

Since the spec already guarantees type and presence, these errors are almost all
"impossible". **Canal, on Go 1.23 with generics, can do better**: either a
`Field[T](conf, path...) T` that panics-as-bug on spec mismatch with a single
`conf.Err()` check at the end, or struct-tag decoding into a typed config struct *derived
from* the same spec. Do not reproduce the error ladder; it is the single biggest ergonomic
tax in this API.

### 7.4 Reusable composite field specs — the killer feature

Because a `ConfigField` is a value, the framework ships **pre-built config fragments with
matching extractors**. This is how "retry/backoff/batching/TLS/auth as config not code"
actually works. The full set in `public/service`:

| Field constructor | Extractor | File |
|---|---|---|
| `NewBatchPolicyField(name)` | `FieldBatchPolicy(path…) (BatchPolicy, error)` | `config_batch_policy.go` |
| `NewBackOffField(name, allowUnbounded, defaults)` | `FieldBackOff(path…) (*backoff.ExponentialBackOff, error)` | `config_backoff.go` |
| `NewBackOffToggledField(...)` | `FieldBackOffToggled(path…) (*backoff.ExponentialBackOff, bool, error)` | `config_backoff.go` |
| `NewOutputMaxInFlightField()` | `FieldMaxInFlight() (int, error)` | `config_max_in_flight.go` |
| `NewInputMaxInFlightField()` | `FieldMaxInFlight()` | `input_max_in_flight.go` |
| `NewAutoRetryNacksToggleField()` | `AutoRetryNacksBatchedToggled(conf, input)` | `config_input.go` |
| `NewForceTimelyNacksField()` | `ForceTimelyNacksBatched(conf, input)` | `config_input.go` |
| `NewMetadataFilterField(name)` | `FieldMetadataFilter(path…)` | `config_metadata_filter.go` |
| `NewScannerField(name)` | `FieldScanner(path…) (*OwnedScannerCreator, error)` | `config_scanner.go` |
| `NewInputField/ListField/MapField` | `FieldInput/FieldInputList/FieldInputMap` | `config_input.go` |
| `NewOutputField…` | `FieldOutput…` | `config_output.go` |
| `NewProcessorField…` | `FieldProcessor…` | `config_processor.go` |
| `NewInterpolatedStringField(name)` | `FieldInterpolatedString(path…)` | `config_interpolated_string.go` |
| `NewBloblangField(name)` | `FieldBloblang(path…)` | `config_bloblang.go` |
| `NewTLSField/NewTLSToggledField` | `FieldTLS…` | `config_tls.go` |
| `NewURLField/NewURLListField` | `FieldURL…` | `config_url.go` |
| HTTP client fields | — | `config_http.go` |
| `NewExtractTracingSpanMappingField()` | — | `config_extract_tracing.go` |
| `NewInjectTracingSpanMappingField()` | — | `config_inject_tracing.go` |

`NewBackOffField` is the exemplar — three duration fields with sensible defaults, docs,
examples, and a documented policy about unboundedness:

```go
// NewBackOffField defines a new object type config field that describes an
// exponential back off policy, often used for timing retry attempts. It is then
// possible to extract a *backoff.ExponentialBackOff from the resulting parsed
// config with the method FieldBackOff.
//
// It is possible to configure a back off policy that has no upper bound (no
// maximum elapsed time set). In cases where this would be problematic the field
// allowUnbounded should be set `false` in order to add linting rules that
// ensure an upper bound is set.
//
// The defaults struct is optional, and if provided will be used to establish
// default values for time interval fields. Otherwise the chosen defaults result
// in one minute of retry attempts, starting at 500ms intervals.
func NewBackOffField(name string, allowUnbounded bool, defaults *backoff.ExponentialBackOff) *ConfigField {
	// ...
	return NewObjectField(name,
		NewDurationField("initial_interval").
			Description("The initial period to wait between retry attempts.").
			Default(initDefault).Example("50ms").Example("1s"),
		NewDurationField("max_interval").
			Description("The maximum period to wait between retry attempts").
			Default(maxDefault).Example("5s").Example("1m"),
		maxElapsedTime,
	).Description("Determine time intervals and cut offs for retry attempts.")
}
```

(with `// TODO: Add linting rule to ensure we aren't unbounded if necessary.` — the
`allowUnbounded` parameter currently only changes the *description*, not the lint. Noted as
a real, in-source unfinished promise.)

**This "field spec + matching extractor" pairing is the single most transplantable idea in
the config model.** It means every connector's `retry:`, `batching:`, `tls:`,
`max_in_flight:` block looks identical, documents itself identically, and renders in a UI
identically — with zero coordination between connector authors and zero core switches.

`config_input.go`/`config_output.go`/`config_processor.go` go further: **a connector's config
can contain other components**, resolved by the framework into owned instances:

```go
// FieldInput accesses a field from a parsed config that was defined with
// NewInputField and returns an OwnedInput, or an error if the configuration was
// invalid.
func (p *ParsedConfig) FieldInput(path ...string) (*OwnedInput, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	conf, err := input.FromAny(p.mgr.Environment(), field)
	if err != nil { return nil, err }
	iproc, err := p.mgr.IntoPath(path...).NewInput(conf)
	if err != nil { return nil, err }
	return &OwnedInput{iproc}, nil
}
```

Note `p.mgr.IntoPath(path...)` — the child gets an observability namespace derived from its
config path, automatically (§11). **Composability of components via config, with automatic
observability nesting, is how brokers/fallback/retry/switch are built without any core
special-casing.** That is the mechanism canal needs for fan-out/fan-in and transform chains,
and it should be designed in from the start.

### 7.5 Machine-readable export: docs and UI

`ConfigView` + `ConfigSchema` are the export surface. `public/service/config.go`:

```go
// ConfigView is a struct returned by a Benthos service environment when walking
// the list of registered components and provides access to information about
// the component.
type ConfigView struct {
	prov      docs.Provider
	component docs.ComponentSpec
}

func (c *ConfigView) Summary() string
func (c *ConfigView) Description() string
func (c *ConfigView) IsDeprecated() bool

// FormatJSON returns a byte slice of the component configuration formatted as a
// JSON object. The schema of this method is undocumented and is not intended
// for general use.
func (c *ConfigView) FormatJSON() ([]byte, error)
```

`public/service/config_docs.go` — a *structured* template model, then AsciiDoc rendering:

```go
type TemplateDataPlugin struct { /* ... */ }
type TemplatDataPluginExample struct { /* ... */ }   // (sic — typo in the exported name)
type TemplateDataPluginField struct { /* ... */ }

func (c *ConfigView) TemplateData() (TemplateDataPlugin, error)
func (c *ConfigView) RenderDocs() ([]byte, error)
```

`public/service/stream_schema.go` — whole-environment schema export, including JSON Schema:

```go
type ConfigSchema struct { /* ... */ }

func (e *Environment) FullConfigSchema(version, dateBuilt string) *ConfigSchema
func (e *Environment) CoreConfigSchema(version, dateBuilt string) *ConfigSchema
func (e *Environment) TemplateSchema(version, dateBuilt string) *ConfigSchema

func (s *ConfigSchema) MarshalJSONV0() ([]byte, error)
func (s *ConfigSchema) MarshalJSONSchema() ([]byte, error)
func ConfigSchemaFromJSONV0(jBytes []byte) (*ConfigSchema, error)
func (s *ConfigSchema) SetFieldDefault(value any, path ...string)
func (s *ConfigSchema) TemplateData(path ...string) (TemplateDataSchema, error)
```

Plus registry walking (`environment.go`):

```go
func (e *Environment) WalkInputs(fn func(name string, config *ConfigView))
func (e *Environment) GetInputConfig(name string) (*ConfigView, bool)
// ... and the same pair for Outputs, Processors, Buffers, Caches, RateLimits, Metrics, Tracers, Scanners
```

And a per-field flag set that a form generator can consume directly — CHANGELOG 4.x:

> "Exporting a schema with the format `jsonschema` now includes `is_advanced`,
> `is_deprecated`, `is_optional`, `is_secret` extra fields."

The docs in `connect/docs/modules/components/pages/**/*.adoc` are generated from this; every
one carries the banner:

```
////
     THIS FILE IS AUTOGENERATED!

     To make changes, edit the corresponding source file under:

     https://github.com/redpanda-data/connect/tree/main/internal/impl/<provider>.

     And:

     https://github.com/redpanda-data/connect/tree/main/cmd/tools/docs_gen/templates/plugin.adoc.tmpl
////
```

and shows Common/Advanced tabs with defaults filled in — all derived from the spec.

**This is the answer to canal's frontend requirement, and the answer is "make the config
spec the single source of truth and export it."** One `ConfigSpec` per connector →
validation + linting + defaults + reference docs + JSON Schema for editors + a form UI, with
no per-connector UI code and therefore no core change when a connector is added
(constraint #1's "specialized UI/UX later must NOT require core changes" is satisfied by
`ShortDescription`/`Advanced`/`Secret`/`Examples` plus, if needed, an extensible annotation
map).

### 7.6 Linting

`public/service/lints.go`:

```go
type LintType int

const (
	LintDeprecated LintType = iota   // "means a field is deprecated and should not be used"
	// LintCustom, LintFailedRead, LintMissingEnvVar, LintInvalidOption, LintBadLabel,
	// LintMissingLabel, LintDuplicateLabel, LintBadBloblang, LintShouldOmit,
	// LintComponentMissing, LintComponentNotFound, LintUnknown, LintMissing,
	// LintExpectedArray, LintExpectedObject, LintExpectedScalar
)

type Lint struct { /* Line, Column, Type, What ... */ }
func (l Lint) Error() string

type LintError []Lint
func (e LintError) Error() string
```

with `ComponentConfigLinter` / `StreamConfigLinter` as the public entry points, both
supporting:

```go
func (c *ComponentConfigLinter) SetRejectDeprecated(v bool) *ComponentConfigLinter
func (s *StreamConfigLinter) SetRejectDeprecated(v bool) *StreamConfigLinter
```

A typed lint enum with line/column is exactly what an editor/LSP or a web config editor
needs. `LintUnknown` (unrecognised field) as a first-class lint type is the thing that
makes typo'd YAML detectable — canal must have it, because silently-ignored config keys are
the classic YAML-driven-tool failure.

There's also config *walking* and *marshalling* for round-tripping edits
(`stream_config_walker.go`, `stream_config_marshaller.go`) with
`SetOmitDeprecated`, and `sanitConf.RemoveDeprecated = false` handling in
`stream_builder.go` — i.e. they can normalise/redact/re-emit a config, which a UI needs for
"load, edit, save without destroying comments/unknowns".

---

## 8. Backpressure

There is no explicit credit/window protocol. Backpressure is **the natural consequence of
unbuffered channels plus blocking method calls**, with four optional bounding mechanisms
layered on top. Worth understanding as a set, because the fact that there are *four* is
itself the finding.

### 8.1 The base mechanism: an unbuffered transaction channel

`AsyncReader` creates `transactions: make(chan message.Transaction)` — **unbuffered**. The
read loop blocks on:

```go
		select {
		case r.transactions <- message.NewTransaction(msg, resChan):
		case <-r.shutSig.SoftStopChan():
			return
		}
```

So if nothing downstream is receiving, `Read`/`ReadBatch` is simply not called again. The
sink's `WriteBatch` blocking is what stops the pipeline. **This is the whole design: a slow
sink means a blocked channel send means the source stops being polled.** No credits, no
watermarks, no explicit signalling — and consequently, nothing to get wrong, and nothing to
observe either.

Note the internal `Streamed` contract: "Every transaction received must be resolved before
another transaction will be sent." That is per-consumer, and parallelism comes from *N
consumers* (`maxInflight` writer loops, `pipeline.threads` processor workers) each pulling
from the same channel, not from buffering.

### 8.2 Sink-side concurrency: `maxInFlight`, declared by the connector

Declared in the constructor return (§1.10), enforced by the driver:

```go
	for i := 0; i < w.maxInflight; i++ {
		go writerLoop()
	}
```

and validated at construction (`environment.go`):

```go
			if maxInFlight < 1 {
				return nil, fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)
			}
```

The user-facing field (`config_max_in_flight.go`):

```go
func NewOutputMaxInFlightField() *ConfigField {
	return NewIntField("max_in_flight").
		Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
		Default(64)
}
```

Default 64. The connector's constructor typically reads the user's value and returns it,
so "how parallel" is a config decision with a connector-chosen default and a
framework-enforced floor.

### 8.3 Source-side in-flight bound: an optional semaphore wrapper

`public/service/input_max_in_flight.go` — a decorator, not a core feature:

```go
// InputWithMaxInFlight wraps an input with a component that limits the number of
// messages being processed at a given time. When the limit is reached a new
// message will not be consumed until an ack/nack has been returned.
func InputWithMaxInFlight(n int, i Input) Input {
	if n <= 0 { return i }
	return &maxInFlight{ i: i, ackSema: semaphore.NewWeighted(int64(n)) }
}

func (m *maxInFlight) Read(ctx context.Context) (*Message, AckFunc, error) {
	if err := m.ackSema.Acquire(ctx, 1); err != nil {
		return nil, nil, err
	}
	mRes, aFn, err := m.i.Read(ctx)
	if err != nil {
		m.ackSema.Release(1)
		return nil, nil, err
	}
	return mRes, func(ctx context.Context, err error) error {
		aerr := aFn(ctx, err)
		m.ackSema.Release(1)
		return aerr
	}, nil
}
```

plus `InputBatchedWithMaxInFlight` (identical, counting batches). Ten lines, and it is the
correct general solution to "bound the ack tree". The field description contains the
warning that matters:

```go
func NewInputMaxInFlightField() *ConfigField {
	return NewIntField("max_in_flight").
		Description("Optionally sets a limit on the number of messages that can be flowing through a Redpanda Connect stream pending acknowledgment from the input at any given time. Once a message has been either acknowledged or rejected (nacked) it is no longer considered pending. If the input produces logical batches then each batch is considered a single count against the maximum. **WARNING**: Batching policies at the output level will stall if this field limits the number of messages below the batching threshold. Zero (default) or lower implies no limit.").
		Default(0).
		Advanced()
}
```

**That WARNING is a genuine deadlock, documented rather than prevented**: input
`max_in_flight: 10` + output `batching.count: 100` ⇒ permanent stall. Two independent knobs
whose composition is unsound, with the interaction left to the user. Canal should either
detect this at lint time (both values are in the config; a cross-component lint is possible)
or make the batcher time-bounded by default so it cannot wait forever.

### 8.4 Source-side offset-window bound: `checkpoint_limit` / `Capped`

The third mechanism (§3.4): `checkpoint.Capped.Track` blocks when
`pending > 0 && pending+batchSize > cap`. Surfaced per-connector as `checkpoint_limit`
(Postgres/MySQL default 1024, Kafka description below). From
`connect/docs/modules/components/pages/inputs/kafka.adoc`:

> "The maximum number of messages of the same topic and partition that can be processed at
> a given time. Increasing this limit enables parallel processing and batching at the output
> level to work on individual partitions. Any given offset will not be committed unless all
> messages under that offset are delivered in order to preserve at least once delivery
> guarantees."

and from `redpanda.adoc`:

> "Determines how many messages of the same partition can be processed in parallel before
> applying back pressure. When a message of a given offset is delivered to the output the
> offset is only allowed to be committed when all messages of prior offsets have also been
> delivered, this ensures at-least-once delivery guarantees. However, this mechanism also
> increases the likelihood of duplicates in the event of crashes or server faults, reducing
> the checkpoint limit will mitigate this."

Note this one is *per partition/per key*, which `max_in_flight` is not. That's why both
exist. **The duplicate-window/throughput tradeoff being a user-facing dial is honest and
right**, and canal should expose it — but as *one* concept with a documented scope, not
three overlapping ones.

### 8.5 Batching as a reusable, first-class component

`public/service/config_batch_policy.go`:

```go
// BatchPolicy describes the mechanisms by which batching should be performed of
// messages destined for a Batch output. This is returned by constructors of
// batch outputs.
type BatchPolicy struct {
	ByteSize int
	Count    int
	Check    string
	Period   string

	// Only available when using NewBatchPolicyField.
	procs []processor.Config
}

// IsNoop returns true if the batching policy does not have any batching
// mechanisms configured.
func (b BatchPolicy) IsNoop() bool
```

Four orthogonal triggers (size in bytes, count, a *predicate over the message*, and a
period), plus **processors that run at flush time** (which is how e.g. `archive`/`compress`
"batch → single blob" transforms attach to the flush boundary rather than to the stream).

The imperative counterpart, for connectors that need to batch on the *input* side (as the
CDC inputs do):

```go
// Batcher provides a batching mechanism where messages can be added one-by-one
// with a boolean return indicating whether the batching policy has been
// triggered.
//
// Upon triggering the policy it is the responsibility of the owner of this
// batcher to call Flush, which returns all the pending messages in the batch.
//
// This batcher may contain processors that are executed during the flush,
// therefore it is important to call Close when this batcher is no longer
// required, having also called Flush if appropriate.
type Batcher struct { /* ... */ }

// Add a message to the batch. Returns true if the batching policy has been
// triggered by this new addition, in which case Flush should be called.
func (b *Batcher) Add(msg *Message) bool

// UntilNext returns a duration indicating how long until the current batch
// should be flushed due to a configured period. A boolean is also returned
// indicating whether the batching policy has a timed factor, if this is false
// then the duration returned should be ignored.
func (b *Batcher) UntilNext() (time.Duration, bool)

// Flush pending messages into a batch, apply any batching processors that are
// part of the batching policy, and then return the result.
func (b *Batcher) Flush(ctx context.Context) (batch MessageBatch, err error)

func (b *Batcher) Close(ctx context.Context) error

// NewBatcher creates a batching mechanism from the policy.
func (b BatchPolicy) NewBatcher(res *Resources) (*Batcher, error)
```

The `Add → bool` + `UntilNext → (d, ok)` + `Flush` triple is a **beautifully minimal
batching API** because it inverts control: the connector owns its select loop and the
batcher is pure policy with no goroutine. The MySQL input's usage is the reference:

```go
			if i.batchPolicy.Add(mb) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil { return fmt.Errorf("flush batch error: %w", err) }
				if err := i.flushBatch(ctx, i.cp, flushedBatch, latestXIDPos); err != nil { ... }
			} else {
				d, ok := i.batchPolicy.UntilNext()
				if ok {
					nextTimedBatchChan = time.After(d)
				}
			}
```

**Steal this exactly.** It is goroutine-free, testable, and composes with any source loop.

Also available as an input-side decorator: `(*OwnedInput).BatchedWith(b *Batcher)` and
`(*OwnedOutput).BatchedWith(b *Batcher)` wrap an existing component in the internal
`batcher.New(...)`.

The stated limitation, from the official batching doc:

> "A batch policy has the capability to _create_ batches, but not to break them down."

Hence a `split` processor is needed for the inverse, and `RegisterBatchOutput`'s warning
that an upstream batch "may exceed the policy specified in your constructor". **Canal should
model both directions** (a coalescer *and* a splitter) as framework primitives, since sinks
almost always have a hard maximum request size.

### 8.6 Processor concurrency

`internal/pipeline/constructor.go`:

```go
var threadsField = docs.FieldInt("threads", "The number of threads to execute processing pipelines across.").HasDefault(-1)
```

```go
// ... threads specified. Processors are executed on each message in the order that
// ...
// threads, or use a memory buffer.
type Config struct {
	Threads    int  `json:"threads" yaml:"threads"`
	// ...
}
```

`internal/pipeline/pool.go`:

```go
func NewPool(threads int, strict bool, log log.Modular, msgProcessors ...processor.V1) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}
	// ...
		workers: make([]processor.Pipeline, threads),
```

`-1` → `runtime.NumCPU()`. N independent worker copies of the whole processor chain, each
pulling transactions. Parallelism therefore **destroys ordering** across the pipeline stage
whenever `threads > 1` — which is consistent with the documented ordering position (§9.2)
but is not called out at the config field.

### 8.7 Buffers as the decoupling valve

The fourth mechanism. From `connect/docs/modules/components/pages/buffers/memory.adoc`:

> "Stores consumed messages in memory and acknowledges them at the input level. During
> shutdown Redpanda Connect will make a best attempt at flushing all remaining messages
> before exiting cleanly."
>
> "This buffer is appropriate when consuming messages from inputs that do not gracefully
> handle back pressure and where delivery guarantees aren't critical."
>
> "This buffer has a configurable limit, where consumption will be stopped with back
> pressure upstream if the total size of messages in the buffer reaches this amount. Since
> this calculation is only an estimate, and the real size of messages in RAM is always
> higher, it is recommended to set the limit significantly below the amount of RAM
> available."
>
> "== Delivery guarantees
> This buffer intentionally weakens the delivery guarantees of the pipeline and therefore
> should never be used in places where data loss is unacceptable."

`limit: 524288000` (500 MiB) by default, and there is a `sqlite` buffer for the durable
variant. Two honest admissions in there: the size accounting is an *estimate*, and the
guarantee weakening is *intentional and documented*. That is the correct way to ship a
dangerous option.

**Summary of the four bounding knobs** — this is the finding:

| Knob | Scope | Blocks what | Where |
|---|---|---|---|
| unbuffered transaction chan | whole pipeline | next `Read` | core, always on |
| output `max_in_flight` | per sink | parallel `WriteBatch` count | connector-declared |
| input `max_in_flight` | per input | unacked batches | optional wrapper |
| `checkpoint_limit` (`Capped`) | per partition/key | `Track` | per-connector |
| buffer `limit` | whole pipeline | buffer writes | optional stage |

Five mechanisms, three user-facing names, one documented deadlock between two of them, and
no single place to observe "why is this pipeline slow". **Canal should unify: one in-flight
accounting concept, scoped per-split, owned and exposed by the framework**, with batching as
policy on top of it rather than as a peer knob.

---

## 9. Delivery guarantees

### 9.1 The claim, stated in primary sources

At-least-once, and *only* at-least-once. There is no exactly-once mode, no transactional
sink protocol, no two-phase commit, and no idempotency framework in the core. From
`connect/docs/modules/components/pages/inputs/redpanda.adoc`:

> "When using consumer groups the offsets of "delivered" records will be committed
> automatically and continuously, and in the event of restarts these committed offsets will
> be used in order to resume from where the input left off. Redpanda Connect guarantees at
> least once delivery by ensuring that records are only considered to be delivered when all
> configured outputs that the record is routed to have confirmed delivery."

That sentence is the definition of the whole model: **"delivered" ≡ "every routed output
returned nil"**, and the ack graph is the mechanism that computes it.

From `public/service/stream.go`:

> "An ungraceful shutdown increases the likelihood of processing duplicate messages on the
> next start up, but never results in dropped messages as long as the input source supports
> at-least-once delivery."

Note the conditional: the guarantee is inherited from the source, not provided by the
framework. A source with no redelivery mechanism (a UDP socket, an HTTP push) gets
at-most-once and there is nothing the framework can do about it.

### 9.2 Ordering

Explicitly not guaranteed under error handling. From `redpanda.adoc` / `redpanda_common.adoc`:

> "However, one way in which the order of records can be mixed is when delivery errors occur
> and error handling mechanisms kick in. Redpanda Connect always leans towards at least once
> delivery unless instructed otherwise, and this includes reattempting delivery of data when
> the ordering of that data can no longer be guaranteed."

Ordering is additionally lost to: `pipeline.threads > 1` (§8.6), output `max_in_flight > 1`
(§8.2), out-of-order ack resolution (§3.1), and `fan_out`/`greedy`/`round_robin` brokers.
The honest summary: **ordering is a property you can preserve by configuring everything to
1, and otherwise not.** Canal should say so as plainly and, better, should model
per-key ordering explicitly (order within a split, not across) so it can be *both* parallel
and ordered.

### 9.3 Nack handling: unbounded auto-retry, opt-out

`public/service/config_input.go`:

```go
const AutoRetryNacksToggleFieldName = "auto_replay_nacks"

func NewAutoRetryNacksToggleField() *ConfigField {
	return NewBoolField(AutoRetryNacksToggleFieldName).
		Description("Whether messages that are rejected (nacked) at the output level should be automatically replayed indefinitely, eventually resulting in back pressure if the cause of the rejections is persistent. If set to `false` these messages will instead be deleted. Disabling auto replays can greatly improve memory efficiency of high throughput streams as the original shape of the data can be discarded immediately upon consumption and mutation.").
		Default(true)
}
```

Read the two halves of that description: default `true` means **retry forever** and
"eventually resulting in back pressure if the cause of the rejections is persistent" — i.e.
a single poison record stops the pipeline. Setting it `false` means "these messages will
instead be deleted" — i.e. silent data loss. **There is no third option in this field: no
dead-letter, no bounded attempts, no poison quarantine.** (You can get a DLQ by other means:
the `fallback` output, or `reject_errored`.) That binary is a real design gap and canal
should not reproduce it: retry policy needs `maxAttempts` and a terminal disposition
(dead-letter / quarantine / drop / halt) as first-class config.

The implementation, `public/service/input_auto_retry_batched.go`:

```go
// AutoRetryNacksBatched wraps a batched input implementation with a component
// that automatically reattempts messages that fail downstream. This is useful
// for inputs that do not support nacks, and therefore don't have an answer for
// when an ack func is called with an error.
//
// When messages fail to be delivered they will be reattempted with back off
// until success or the stream is stopped.
func AutoRetryNacksBatched(i BatchInput) BatchInput
```

It is a wrapper around the generic `autoretry.List[T]` (`internal/autoretry/auto_retry_list.go`):

```go
// ReadFunc is a closure used to obtain a new T, this is done asynchronously
// from retries but the result is given lower priority than retries. If the read
// func returns an error then it is returned as a highest priority.
type ReadFunc[T any] func(context.Context) (t T, aFn AckFunc, err error)

// MutatorFunc is an optional closure used to mutate a T about to be scheduled
// for retry based on the returned error. This is useful for reducing a batch
// based on a batch error, etc.
type MutatorFunc[T any] func(t T, err error) T

// List contains a slice of items that are pending an acknowledgement, once
// items are added it's required that all rejected adopted T values are
// recirculated either via TryShift (non-blocking) or Shift.
type List[T any] struct { /* pendingRead, pendingRetry, cond, ... */ }

func NewList[T any](reader ReadFunc[T], mutator MutatorFunc[T]) *List[T]

// Shift blocks until either a T needs retrying and returns it, enableRead is
// set true and a new T is ready, or the context is cancelled. Returns
// ErrExhausted if all messages are exhausted and enableRead is set to false.
func (l *List[T]) Shift(ctx context.Context, enableRead bool) (t T, fn AckFunc, err error)

// Exhausted returns true if all adopted Ts have been acknowledged.
func (l *List[T]) Exhausted() bool
```

Three details worth extracting:

1. **Retries have strict priority over new reads.** In `Shift`: check retry queue first,
   only then take a completed pending read. And the backoff only kicks in after two
   attempts, specifically to avoid hot-looping:
   ```go
	resend.attempts++
	if resend.attempts > 2 {
		// This sleep prevents a busy loop on permanently failed messages.
		if tout := resend.boff.NextBackOff(); tout > 0 {
			select {
			case <-time.After(tout):
			case <-ctx.Done():
				return
			}
		}
	}
   ```
2. **The ack wrapper is `sync.Once`-guarded and routes nack→retry-queue, ack→upstream:**
   ```go
	func (l *List[T]) wrapPendingAck(t *pendingT[T]) AckFunc {
		var ackOnce sync.Once
		return func(ctx context.Context, err error) (outErr error) {
			ackOnce.Do(func() {
				l.cond.L.Lock()
				defer func() { l.cond.Broadcast(); l.cond.L.Unlock() }()
				if err != nil {
					t.t = l.mutator(t.t, err)
					l.pendingRetry = append(l.pendingRetry, t)
					return
				}
				l.retryInFlight--
				outErr = t.aFn(ctx, nil)
			})
			return
		}
	}
   ```
   **The upstream ack is only called on success.** A nack never propagates to the real
   source; it becomes an internal retry. That is precisely why the MySQL input can write
   `func(ctx context.Context, _ error) error` and ignore the error (§3.5).
3. **End-of-input is deferred until retries drain.** `Connect`:
   ```go
	err := i.child.Connect(ctx)
	// If our source has finished but we still have messages in flight then
	// we act like we're still open. Read will be called and we can either
	// return the pending messages or wait for them.
	if errors.Is(err, ErrEndOfInput) && !i.retryList.Exhausted() {
		atomic.StoreInt32(&i.inputClosed, 1)
		err = nil
	}
   ```
   and `ReadBatch` translates `autoretry.ErrExhausted` → `ErrEndOfInput`. This is the
   "drain before terminate" pattern, and it's fiddly enough that canal should own it rather
   than leave it to connectors.

There is also a dangling, disabled cleanup path, kept in-tree with a comment that is itself
a finding:

```go
// TODO: Ensure docs around auto retry and all implementations are okay with
// nacks on termination, otherwise we leave them.
//
//nolint:unused // Keeping this around for now.
func (l *List[T]) nackAllPending() error
```

Translation: **on shutdown, pending retries are neither nacked nor delivered** — they are
just dropped, relying on the source's own uncommitted position to replay them. Fine for
Kafka; silently lossy for anything where the ack-func *was* the only record of the message.

### 9.4 Granular batch failure: `BatchError`

The one place the ack model gets richer than a boolean. `public/service/errors.go`:

```go
// BatchError groups the errors that were encountered while processing a
// collection (usually a batch) of messages and provides methods to iterate
// over these errors.
type BatchError struct { wrapped *batch.Error }

// NewBatchError creates a BatchError that can be returned by batched outputs.
// The advantage of doing so is that nacks and retries can potentially be
// granularly dealt out in cases where only a subset of the batch failed.
//
// A headline error must be supplied which will be exposed when upstream
// components do not support granular batch errors.
func NewBatchError(b MessageBatch, headline error) *BatchError

// Failed stores an error state for a particular message of a batch. Returns a
// pointer to the underlying error, allowing the method to be chained.
//
// If Failed is not called then all messages are assumed to have failed. If it
// is called at least once then all message indexes that aren't explicitly
// failed are assumed to have been processed successfully.
func (err *BatchError) Failed(i int, merr error) *BatchError

func (err *BatchError) IndexedErrors() int
```

The "headline error for consumers that don't understand granularity" is a good
forward/backward-compatibility idea: **a rich error that degrades gracefully to a simple
one.**

The correlation problem it creates is severe enough to have produced a deprecation:

```go
// WalkMessagesIndexedBy applies a closure to each message of a batch that is
// included in this batch error. ...
//
// However, the shape of the batch of messages at the time the errors occurred
// may differ significantly from the batch known by the component receiving this
// error. For example a processor that dispatches a batch to a list of child
// processors may receive a batch error that occurred after filtering and
// re-ordering has occurred to the batch. In such cases it is not possible to
// simply inspect the indexes of errored messages in order to associate them
// with the original batch as those indexes could have changed.
//
// Therefore, in order to solve this particular use case it is possible to
// create a batch indexer before dispatching the batch to the child components.
// Then, when a batch error is received WalkMessagesIndexedBy can be used as way
// to walk the errored messages with the indexes (and message contents) of the
// original batch.
//
// Important! The order of messages walked is not guaranteed to match that of
// the source batch. It is also possible for any given index to be represented
// zero, one or more times.
func (err *BatchError) WalkMessagesIndexedBy(s *Indexer, fn func(int, *Message, error) bool)

// WalkMessages ...
// Deprecated: This method is harmful and should be avoided as indexes are not
// guaranteed to match a hypothetical origin batch that they might be compared
// against. Use WalkMessagesIndexedBy instead.
func (err *BatchError) WalkMessages(fn func(int, *Message, error) bool)
```

`// Deprecated: This method is harmful` — in the public API, from the author. That is the
clearest possible signal that **positional identity within a batch is the wrong model** and
that records need stable identity if you want per-record error routing. CHANGELOG 4.x
confirms: "Plugin API: The `(*BatchError).WalkMessages` method has been deprecated in favour
of `WalkMessagesIndexedBy`."

**Canal should give records a stable, framework-assigned in-flight ID from the start**, so
that partial-failure reporting, retry targeting, and dead-lettering never need positional
correlation or a sort-group side-table.

The retry side consumes this to shrink the retried batch — `AutoRetryNacksBatched`'s
mutator:

```go
			func(t MessageBatch, err error) MessageBatch {
				var bErr *batch.Error
				if len(t) == 0 || !errors.As(err, &bErr) || bErr.IndexedErrors() == 0 {
					return t
				}
				sortGroup := message.TopLevelSortGroup(t[0].part)
				if sortGroup == nil {
					// We can't associate our source batch with the one that's associated
					// with the batch error, therefore we fall back towards treating every
					// message as if it was errored the same.
					return t
				}
				// ... rebuild newBatch from only the errored parts, dedup by index ...
			}
```

Note the fallback: when correlation fails, **retry the whole batch** — i.e. amplify
duplicates rather than lose data. Correct priority, but it shows the cost of weak identity.

### 9.5 Strict mode: rejecting already-failed messages at the sink

Recent (CHANGELOG 4.76.0) and the direction of travel:

> "Config: Added an `error_handling.strict` field (default `false`). When enabled, a
> processing error is terminal for the affected message: it skips the remaining processors in
> its pipeline and is rejected (nacked) at the output rather than written. Expected errors are
> recovered by wrapping the fallible step within a `try_catch` (or `retry`) processor. This
> behaviour is expected to become the default in the next major version."

The implementation is in the output driver, and the *mixed-batch* handling is the
interesting part (`internal/component/output/async_writer.go`):

```go
			// In strict mode, messages that have already failed a processing step
			// are rejected (nacked) rather than written, on a per-message basis.
			if w.strict {
				var rejectErr *batch.Error
				var sampleErr error
				for i, m := range payload {
					mErr := m.ErrorGet()
					if mErr == nil { continue }
					if sampleErr == nil { sampleErr = mErr }
					mErr = fmt.Errorf("rejected due to failed processing: %w", mErr)
					if rejectErr == nil { rejectErr = batch.NewError(payload, mErr) }
					rejectErr.Failed(i, mErr)
				}

				if rejectErr != nil {
					mRejected.Incr(int64(rejectErr.IndexedErrors()))
					w.log.Warn("Rejecting %v of %v message(s) for output '%v' because they failed a processing step and strict error handling is enabled (example error: %v). To recover from expected errors and allow these messages through, wrap the failing step within a try_catch (or retry) processor; otherwise they will be nacked and retried by the input.\n", rejectErr.IndexedErrors(), len(payload), w.typeStr, sampleErr)

					if rejectErr.IndexedErrors() == len(payload) {
						// Every message failed: nack the whole batch without writing.
						_ = ts.Ack(closeLeisureCtx, rejectErr)
						continue
					}

					// Mixed batch: write only the messages that did not fail, and
					// merge any write failure back into the rejection error.
					sortGroup, sortedBatch := message.NewSortGroup(payload)
					forwardBatch := make(message.Batch, 0, len(payload)-rejectErr.IndexedErrors())
					rejectErr.WalkPartsNaively(func(i int, _ *message.Part, err error) bool {
						if err == nil { forwardBatch = append(forwardBatch, sortedBatch[i]) }
						return true
					})
					payload = forwardBatch
					ackFn = func(ctx context.Context, werr error) error {
						if werr == nil { return ts.Ack(ctx, rejectErr) }
						var tmpBatchErr *batch.Error
						if errors.As(werr, &tmpBatchErr) {
							tmpBatchErr.WalkPartsBySource(sortGroup, sortedBatch, func(i int, _ *message.Part, err error) bool {
								if err != nil { rejectErr.Failed(i, err) }
								return true
							})
							return ts.Ack(ctx, rejectErr)
						}
						for _, p := range forwardBatch {
							if i := sortGroup.GetIndex(p); i >= 0 { rejectErr.Failed(i, werr)	}
						}
						return ts.Ack(ctx, rejectErr)
					}
				}
			}
```

~60 lines of sort-group index gymnastics to answer "some of this batch is bad". Every line
of it exists because the record has no stable ID and the ack is batch-granular. This is the
strongest concrete argument in the entire dossier for **per-record ack granularity (or at
least per-record identity) in canal's design**.

Note also the excellent operator-facing warning text — it names the count, the output, an
example error, the fix, and the consequence. That's the standard to hold canal's log lines to.

### 9.6 Idempotency and dedup: userland

No framework support. The tools are: a `dedupe` processor over a `cache` resource (with the
`Add`-returns-`ErrKeyAlreadyExists` primitive as the compare-and-set), sink-specific upsert
modes configured per connector, and `Cache.Add`:

```go
	// Add is the same operation as Set except that it returns an error if the
	// key already exists. It is okay for caches to return nil on duplicates if
	// it isn't possible to implement.
```

"It is okay for caches to return nil on duplicates if it isn't possible to implement" —
**the one primitive that could underpin exactly-once is explicitly allowed to be
unreliable.** That is a deliberate scope decision (keep the cache interface implementable by
anything) with a real cost (no portable dedup). Canal, if it wants dedup or fencing, needs a
*separate*, strictly-CAS store interface and should not overload the cache.

### 9.7 Transactional / 2PC sinks

Absent. The `retry` output exists precisely because the alternative — propagating a nack —
reprocesses the message, which is unsafe if processing had side effects
(`internal/impl/pure/output_retry.go`):

```go
		Summary("Attempts to write messages to a child output and if the write fails for any reason the message is retried either until success or, if the retries or max elapsed time fields are non-zero, either is reached.").
		Description(`
All messages in Redpanda Connect are always retried on an output error, but this would usually involve propagating the error back to the source of the message, whereby it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on the event of a failed send. We might, for example, have a deduplication processor that we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a different output target (a dead letter queue). In which case you should instead use the `+"xref:components:outputs/fallback.adoc[`fallback`]"+` output type.`).
		Fields(retries.CommonRetryBackOffFields(0, "500ms", "3s", "0s")...).
		Fields(
			service.NewOutputField(roFieldOutput).
				Description("A child output."),
		)
```

Registered as an ordinary batch output whose `maxInFlight = 1` and which wraps a child
output resolved from its own config:

```go
	service.MustRegisterBatchOutput(
		"retry", retryOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1
			var s output.Streamed
			if s, err = retryOutputFromConfig(conf, interop.UnwrapManagement(mgr)); err != nil { return }
			out = interop.NewUnwrapInternalOutput(s)
			return
		})
```

**"Retry at the sink instead of replaying from the source" being a composable config
wrapper rather than a core feature is exactly right** and canal should copy the shape:
`retry`, `fallback`, `switch`, `broker`, `reject_errored`, `drop_on` are all just
components-containing-components. Note however the escape hatch it needs: `interop.Unwrap*`
plus the `Unwrap() output.Streamed` type assertion in `RegisterBatchOutput`:

```go
			if u, ok := op.(interface {
				Unwrap() output.Streamed
			}); ok {
				return u.Unwrap(), nil
			}
```

That's a **hole punched through the public interface** so that meta-components can bypass
the air gap (and avoid double-wrapping in batching/max-in-flight). Canal should provide a
sanctioned way to express "this component *is* a pipeline stage, not a leaf" rather than an
undocumented `Unwrap()` convention discovered by type assertion. There are also `XUnwrapper()
any` methods on `OwnedInput`/`OwnedOutput`/`OwnedProcessor`/`Batcher`/`Resources`, each
labelled `// XUnwrapper is for internal use only, do not use this.` — i.e. the public API has
a private back door in five places.

### 9.8 Fan-out and its guarantee

`public/service/output.go` enumerates the broker patterns as public constants:

```go
// OutputBrokerPatternType describes the brokering pattern type for a broker output.
type OutputBrokerPatternType string

const (
	// OutputBrokerPatternFanOut sends each message to all outputs in parallel.
	OutputBrokerPatternFanOut OutputBrokerPatternType = "fan_out"
	// OutputBrokerPatternFanOutFailFast sends each message to all outputs in parallel and output failures will not be
	// automatically retried.
	OutputBrokerPatternFanOutFailFast OutputBrokerPatternType = "fan_out_fail_fast"
	// OutputBrokerPatternFanOutSequential sends each message to all outputs sequentially.
	OutputBrokerPatternFanOutSequential OutputBrokerPatternType = "fan_out_sequential"
	// OutputBrokerPatternFanOutSequentialFailFast ...
	OutputBrokerPatternFanOutSequentialFailFast OutputBrokerPatternType = "fan_out_sequential_fail_fast"
	// OutputBrokerPatternRoundRobin sends each message to a single output following their order.
	OutputBrokerPatternRoundRobin OutputBrokerPatternType = "round_robin"
	// OutputBrokerPatternGreedy sends each message to a single output, which is determined by allowing outputs to claim
	// messages as soon as they are able to process them.
	OutputBrokerPatternGreedy OutputBrokerPatternType = "greedy"
)
```

Fan-out with at-least-once means: if sink B fails, the record is nacked and **re-sent to
sink A as well** — duplicates in A are the price of not losing it in B (unless
`_fail_fast`). This is the correct default and worth documenting explicitly for canal, since
fan-out is in canal's end-state goals. The `_fail_fast` variants are the escape hatch, and
their existence is the admission that the default is sometimes wrong.

Relatedly, from the CHANGELOG (v4 breaking changes): "The `switch` output field
`retry_until_success` now defaults to `false`" and "Added a linting rule that warns against
having a `reject` output under a `switch` broker without `retry_until_success` disabled" —
i.e. the internal-retry default caused a documented indefinite-block bug with `reject`
outputs, was patched with a *lint*, and then the default was flipped in a major version.

### 9.9 Verdict

The guarantee model is: **at-least-once, inherited from the source, computed by an ack
graph, with unbounded retry by default and no dedup, no ordering, no transactions.** For a
"connect anything to anything" tool that is a defensible, coherent choice — and its
simplicity is why the sink interface is three methods.

For canal, the parts to keep are the ack graph as the composition primitive, the
"delivered ≡ all routed sinks confirmed" definition, `BatchError`'s
degrade-to-headline design, the `retry`/`fallback` composable wrappers, and the
`_fail_fast` explicitness. The parts to fix are: per-record identity (kills 200 lines of
sort-group code), bounded retry with a terminal disposition, an explicit ordering scope
(per-split), and a separate CAS/fencing store so that idempotent sinks and multi-worker
ownership are expressible.

---

## 10. Plugin boundary

This is where Benthos is most directly relevant to canal's constraint #3, because it has
**both** an in-process Go-interface registry **and**, in the `connect` repo, an
out-of-process gRPC/subprocess implementation of *the same interfaces* — which is precisely
the "design for that future without building it now" target.

### 10.1 In-process: a global registry populated at init time

`public/service/plugins.go` — the global functions, each delegating to a package-level
`globalEnvironment`:

```go
func RegisterBatchBuffer(name string, spec *ConfigSpec, ctor BatchBufferConstructor) error {
	return globalEnvironment.RegisterBatchBuffer(name, spec, ctor)
}
// MustRegisterBatchBuffer is the same as RegisterBatchBuffer but panics on error.
func MustRegisterBatchBuffer(name string, spec *ConfigSpec, ctor BatchBufferConstructor) {
	if err := RegisterBatchBuffer(name, spec, ctor); err != nil {
		panic(err)
	}
}
```

The full registration surface: `RegisterInput`, `RegisterBatchInput`, `RegisterOutput`,
`RegisterBatchOutput`, `RegisterProcessor`, `RegisterBatchProcessor`, `RegisterBatchBuffer`,
`RegisterCache`, `RegisterRateLimit`, `RegisterMetricsExporter`,
`RegisterOtelTracerProvider`, `RegisterBatchScannerCreator`, `RegisterTemplateYAML`, and
`(*Environment).RegisterCustomResource` — each with a `MustRegisterX` panicking twin.

Usage is `func init()` in the connector's own package, and the connector is linked in by an
import for side effects. From `example_output_batched_plugin_test.go`:

```go
import (
	"github.com/redpanda-data/benthos/v4/public/service"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)
```

and the canonical registration:

```go
	spec := service.NewConfigSpec().
		Field(service.NewBatchPolicyField("batching"))

	// Register our new output, which doesn't require a config schema.
	err := service.RegisterBatchOutput(
		"batched_json_stdout", spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, policy service.BatchPolicy, maxInFlight int, err error) {
			if policy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			maxInFlight = 1
			out = &batchOfJSONWriter{}
			return
		})
```

That is the whole story for adding a sink: implement `Connect`/`WriteBatch`/`Close`, build a
spec, register. **Zero core edits, zero switch statements** — canal's constraint #4 is
demonstrably achievable with exactly this shape. `public/components/{pure,all,…}` are curated
side-effect-import bundles, which is a good pattern for "single static binary but you choose
the payload".

### 10.2 Registries are values, not globals — `Environment`

The crucial refinement, `public/service/environment.go`:

```go
// Environment is a collection of Benthos component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This can be useful for sandboxing, testing, etc, but most plugin
// authors do not need to create an Environment and can simply use the global
// environment.
type Environment struct {
	internal    *bundle.Environment
	bloblangEnv *bloblang.Environment
	fs          ifs.FS
}

var globalEnvironment = &Environment{
	internal:    bundle.GlobalEnvironment,
	bloblangEnv: bloblang.GlobalEnvironment(),
	fs:          ifs.OS(),
}

func GlobalEnvironment() *Environment

// NewEnvironment creates a new environment that inherits all globally defined
// plugins, but can have plugins defined on it that are isolated.
func NewEnvironment() *Environment { return globalEnvironment.Clone() }

// NewEmptyEnvironment creates a new environment with zero registered plugins.
func NewEmptyEnvironment() *Environment

func (e *Environment) Clone() *Environment

// Without creates a clone of an existing environment with a variadic list of
// plugin names excluded from the resulting environment.
func (e *Environment) Without(names ...string) *Environment

// With creates a clone of an existing environment with only a variadic list of
// plugin names included from the resulting environment.
func (e *Environment) With(names ...string) *Environment

func (e *Environment) UseBloblangEnvironment(bEnv *bloblang.Environment)

// UseFS configures the service environment to use an instantiation of *FS as
// its filesystem. This provides extra control over the file access of all
// Benthos components within the stream. However, this functionality is opt-in
// and there is no guarantee that plugin implementations will use this method
// of file access.
func (e *Environment) UseFS(fs *FS)
```

plus per-type narrowing: `WithInputs(names…)`, `WithOutputs(names…)`, `WithProcessors(…)`,
`WithCaches(…)`, `WithRateLimits(…)`, `WithMetrics(…)`, `WithTracers(…)`, `WithScanners(…)`,
`WithBuffers(…)`.

**A global registry that is really a default instance of a value type is the right design**,
and canal should adopt it verbatim: `init()`-time registration for ergonomics, a
first-class `Registry`/`Environment` value for tests, sandboxing, multi-tenant
allow-listing, and the frontend's "which connectors does this build have?" query. `UseFS`'s
honest caveat ("this functionality is opt-in and there is no guarantee that plugin
implementations will use this method of file access") is a warning: if you want sandboxable
side effects, the *only* path to them must go through injected capabilities, or the
sandbox is advisory.

### 10.3 Out-of-process: the same interfaces over gRPC — `connect/internal/rpcplugin`

The proof that the interface set is transport-agnostic. `connect/proto/redpanda/runtime/v1alpha1/input.proto`
is a near-verbatim translation of `service.BatchInput`, including the doc comments:

```proto
// BatchInput is an interface implemented by Benthos inputs that produce
// messages in batches, where there is a desire to process and send the batch as
// a logical group rather than as individual messages.
//
// Calls to ReadBatch should block until either a message batch is ready to
// process, the connection is lost, or the RPC deadline is reached.
service BatchInputService {
  // Init is the first method called for a batch input and it passes the user's
  // configuration to the input.
  //
  // The schema for the input configuration is specified in the `plugin.yaml`
  // file provided to Redpanda Connect.
  rpc Init(BatchInputInitRequest) returns (BatchInputInitResponse);
  // Establish a connection to the upstream service. ...
  rpc Connect(BatchInputConnectRequest) returns (BatchInputConnectResponse);
  // Read a message batch from a source, along with a function to be called
  // once the entire batch can be either acked ... or nacked ...
  rpc ReadBatch(BatchInputReadRequest) returns (BatchInputReadResponse);
  // Acknowledge a message batch. This function ensures that the source of the
  // message receives either an acknowledgement (error is missing) or an error
  // that can either be propagated upstream as a nack, or trigger a reattempt at
  // delivering the same message.
  rpc Ack(BatchInputAckRequest) returns (BatchInputAckResponse);
  // Close the component, blocks until either the underlying resources are
  // cleaned up or the RPC deadline is reached.
  rpc Close(BatchInputCloseRequest) returns (BatchInputCloseResponse);
}
```

**The single most important design detail in this whole section**: the closure becomes a
correlation ID.

```proto
message BatchInputReadResponse {
  // The ID of the batch, which is used in the ack request to identify the batch
  // used. These IDs are opaque to the connect framework but IDs should be
  // unique per process.
  uint64 batch_id = 1;
  // The batch of messages to be processed.
  MessageBatch batch = 2;
  // If present, then there was an error reading messages.
  Error error = 3;
}

message BatchInputAckRequest {
  // The ID of the batch.
  uint64 batch_id = 1;
  // If present, then this is a nack request.
  // If auto_replay_nacks is enabled in the InitResponse, then this should never
  // be present.
  Error error = 2;
}
```

`AckFunc` — a Go closure — survives the process boundary as `(batch_id) → Ack RPC`. That
works **only because the ack signature is `(ctx, err) error` and carries no other state**.
A richer ack (e.g. one that returned a committed position, or took a partial-success map)
would have needed a schema. This is the concrete lesson: **keep the ack primitive tiny and
serialisable, and the in-process/out-of-process equivalence falls out for free.**

Two other capabilities crossed the boundary as *data* rather than as behaviour:

```proto
message BatchInputInitResponse {
  // If present, then the input configuration is invalid and an error should be
  // surfaced at pipeline construction time.
  Error error = 1;
  // If true, then any nacks are automatically retried. This is useful for
  // inputs that don't have a mechanism for dealing with nacks, and want to
  // just automatically retry them until they succeed.
  bool auto_replay_nacks = 2;
}
```

i.e. the "wrap me in `AutoRetryNacks`" decision, which in-process is a Go function call, is
a **boolean in the init response** out-of-process. The host applies it:

```go
			if autoRetryNacks {
				return service.AutoRetryNacksBatched(i), nil
			}
			return i, nil
```

**Capabilities-as-data is the pattern that makes an interface transport-portable.** Canal
should therefore express connector capabilities (batching envelope, in-flight limit, retry
semantics, snapshot support, ordering guarantees) as a *declarative capability struct
returned at init*, not as optional Go interfaces discovered by type assertion — because
type assertions cannot cross a process boundary, and `ConnectionTestable` (§1.4) already
shows the cost of that choice in-process.

The error model is a closed, wire-representable set — exactly the taxonomy §6.4 needed:

```proto
// An error in the context of a data pipeline.
message Error {
  // The error message. If non empty, then the error is valid and
  // if empty the error is ignored as if a success (due to proto3 empty
  // semantics).
  string message = 1;
  message NotConnected {}
  message EndOfInput {}
  // Additional error details for specific Redpanda Connect behavior.
  // If one of these fields is set, then message must be non-empty.
  oneof detail {
    // BackOff is an error that plugins can optionally wrap another error with
    // which instructs upstream components to wait for a specified period of
    // time before retrying the errored call.
    //
    // Only supported by Connect methods in the Input and Output services.
    google.protobuf.Duration backoff = 2;
    NotConnected not_connected = 3;
    EndOfInput end_of_input = 4;
  }
}
```

And the record envelope survives the boundary with the bytes/structured duality intact —
`connect/proto/redpanda/runtime/v1alpha1/message.proto`:

```proto
// `Value` represents a dynamically typed value which can be used to represent
// a value within a Redpanda Connect pipeline.
message Value {
  oneof kind {
    NullValue null_value = 1;
    string string_value = 2;
    int64 integer_value = 3;
    double double_value = 4;
    bool bool_value = 5;
    google.protobuf.Timestamp timestamp_value = 6;
    bytes bytes_value = 7;
    StructValue struct_value = 8;
    ListValue list_value = 9;
  }
}

// Message represents a piece of data or an event that flows through the
// runtime.
message Message {
  oneof payload {
    bytes bytes = 1;
    Value structured = 2;
  }
  StructValue metadata = 3;
  Error error = 4;
}

message MessageBatch { repeated Message messages = 1; }
```

`oneof payload { bytes | Value }` is the wire form of §2.2, and `metadata` is a
`StructValue` (so metadata values are typed, not just strings — matching `MetaSetMut`).
Note `Value` includes `timestamp_value`, which the in-process structured type system does
*not* (it insists on JSON-ish scalars) — a small but real divergence between the two
representations.

**This proto is the single best artefact to copy for canal's plugin-boundary design**: it is
a complete, working proof that the four-verb + ack-id + closed-error-set + bytes-or-value
shape is transport-neutral. Design canal's Go interfaces so that *this* proto could be
generated from them.

### 10.4 Host-side mechanics: subprocess supervision and restart

`connect/internal/rpcplugin/input.go` — the out-of-process implementation is itself just an
ordinary in-process plugin:

```go
type input struct {
	cfgValue any
	proc     *subprocess.Subprocess
	client   runtimepb.BatchInputServiceClient
}

var _ service.BatchInput = (*input)(nil)
```

`var _ service.BatchInput = (*input)(nil)` is the whole thesis in one line: **the
out-of-process transport satisfies the same interface, so the core is unchanged.**

Wiring, per instance: a unix socket path, env var handoff, gRPC client created *before* the
subprocess starts (so cleanup is simple), then `Init` with the parsed config marshalled to
`Value`:

```go
		socketPath, err := newUnixSocketAddr()
		// ...
		// No I/O happens in NewClient, so we can do this before we start the subprocess.
		// This simplifies the cleanup if there is a failure.
		conn, err := grpc.NewClient(
			socketPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		// ...
		cleanup = append(cleanup, conn.Close)
		spec.Env["REDPANDA_CONNECT_PLUGIN_ADDRESS"] = socketPath
		proc, err := subprocess.New(
			spec.Cmd,
			spec.Env,
			subprocess.WithLogger(res.Logger()),
			subprocess.WithCwd(spec.Cwd),
		)
```

```go
	value, err := runtimepb.AnyToProto(cfgValue)
	// ...
	// Retry to wait for the process to start
	autoRetryNacks, err = backoff.RetryWithData(func() (bool, error) {
		resp, err := client.Init(ctx, &runtimepb.BatchInputInitRequest{
			Config: value,
		})
		if err != nil {
			if !proc.IsRunning() {
				return false, backoff.Permanent(fmt.Errorf("plugin exited early: %w", err))
			}
			return false, err
		}
		if err = runtimepb.ProtoToError(resp.Error); err != nil {
			return false, backoff.Permanent(err)
		}
		return resp.AutoReplayNacks, nil
	}, backoff.NewExponentialBackOff(exponentialBackoffOpts()...))
```

Crash recovery is folded into `Connect`, exploiting the framework's existing
"Connect is retried with backoff" contract:

```go
// Connect implements service.BatchInput.
func (i *input) Connect(ctx context.Context) (err error) {
	var resp *runtimepb.BatchInputConnectResponse
	// If the plugin crashes attempt to restart the process up to retryCount times.
	for range retryCount {
		resp, err = i.client.Connect(ctx, &runtimepb.BatchInputConnectRequest{})
		if err != nil {
			err = fmt.Errorf("unable to reach plugin: %w", err)
			if i.proc.IsRunning() {
				return err
			}
			if err := i.proc.Close(ctx); err != nil {
				return fmt.Errorf("unable to restart plugin process: %w", err)
			}
			if _, err := startInputPlugin(ctx, i.proc, i.client, i.cfgValue); err != nil {
				return fmt.Errorf("unable to restart plugin: %w", err)
			}
			continue
		}
		return nil
	}
	// ...
}
```

and a dead process is reported as `ErrNotConnected`, which the framework already knows how
to handle:

```go
func (i *input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	resp, err := i.client.ReadBatch(ctx, &runtimepb.BatchInputReadRequest{})
	if err != nil {
		if !i.proc.IsRunning() {
			return nil, nil, service.ErrNotConnected
		}
		return nil, nil, fmt.Errorf("unable to read from plugin: %w", err)
	}
	// ...
	return batch, func(ctx context.Context, err error) error {
		resp, err := i.client.Ack(ctx, &runtimepb.BatchInputAckRequest{
			BatchId: id,
			Error:   runtimepb.ErrorToProto(err),
		})
		if err != nil {
			return fmt.Errorf("unable to ack batch with ID %d: %w", id, err)
		}
		return runtimepb.ProtoToError(resp.Error)
	}, nil
}
```

**`ErrNotConnected` doing double duty as "the plugin process died" is the elegant part**:
process supervision reuses the connection state machine, so the core learns nothing new.
Canal should make sure its connection-state vocabulary is broad enough to absorb
"transport/host failure" the same way.

Note the *unresolved* correctness issue: on restart, `ReadBatch` returns `ErrNotConnected`
and outstanding `batch_id`s become meaningless — but the ack closures for in-flight batches
still exist in the host and will RPC into a *fresh* process with stale IDs. The proto's
"IDs should be unique per process" doesn't help across restarts. I did not find fencing or
generation-numbering; see "unverified".

### 10.5 Discovery and versioning

Out-of-process plugins are discovered from `plugin.yaml` manifests, and the manifest is
translated into a real `ConfigSpec` — so **an out-of-process plugin is as self-documenting
as an in-process one** (`connect/internal/rpcplugin/config.go`):

```go
// Config describes a dynamic plugin over gRPC.
type Config struct {
	Name        string `yaml:"name"`
	Summary     string `yaml:"summary"`
	Description string `yaml:"description"`
	// The command to run for the plugin.
	Cmd    []string      `yaml:"command"`
	Cwd    string        `yaml:"cwd"`
	Type   ComponentType `yaml:"type"`
	Fields []FieldConfig `yaml:"fields"`
}

// FieldConfig describes a configuration field used in the template.
type FieldConfig struct {
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	Type        *FieldType `yaml:"type,omitempty"`
	Kind        *FieldKind `yaml:"kind,omitempty"`
	Default     *any       `yaml:"default,omitempty"`
	Advanced    bool       `yaml:"advanced"`
}

const (
	FieldTypeString  FieldType = "string"
	FieldTypeInt     FieldType = "int"
	FieldTypeFloat   FieldType = "float"
	FieldTypeBool    FieldType = "bool"
	FieldTypeUnknown FieldType = "unknown"
)
const (
	FieldKindScalar FieldKind = "scalar"
	FieldKindMap    FieldKind = "map"
	FieldKindList   FieldKind = "list"
)
const (
	ComponentTypeInput     ComponentType = "input"
	ComponentTypeProcessor ComponentType = "processor"
	ComponentTypeOutput    ComponentType = "output"
)
```

```go
// DiscoverAndRegisterPlugins discovers and registers plugins from the given paths.
//
// Paths can be either absolute paths or globs. The function will read the manifest files
// and then register the plugins with the given environment.
func DiscoverAndRegisterPlugins(fs fs.FS, env *service.Environment, paths []string) error
```

The `(type, kind)` product → `ConfigField` mapping (`FieldConfig.toSpec`) is where the
manifest field vocabulary meets the in-process one, and it is visibly *narrower* — with the
gaps marked:

```go
	case FieldKindList:
		switch fieldType {
		case FieldTypeBool:
			// TODO: This should be a BoolListField, but we don't have one yet.
			f = service.NewAnyListField(c.Name)
	// ...
	case FieldKindMap:
		switch fieldType {
		case FieldTypeBool:
			// TODO: This should be a BoolMapField, but we don't have one yet.
			f = service.NewAnyMapField(c.Name)
```

and no `Secret`, no `Optional`, no `Examples`, no `LintRule`, no nested objects, no
`ShortDescription`. **The out-of-process config vocabulary is a strict subset of the
in-process one**, which means out-of-process plugins get a degraded UI. That is the
predictable outcome of designing the in-process spec first and the manifest second. Canal
should define the *declarative* spec (the one a manifest or a proto can express) as the
primary artefact and make the Go builder a convenience constructor over it, so the two can
never diverge.

**Versioning and compatibility, honestly:**

- Package path is `v1alpha1`. `grpc.WithTransportCredentials(insecure.NewCredentials())`
  over a unix socket. `protogen.go` exists for regeneration.
- Compatibility is protobuf's: additive field numbers, `oneof` for extension.
- CHANGELOG 4.70.0: "API: Switch plugins from Experimental to Stable by default" — so this
  is recent and still stabilising.
- There is **no version negotiation handshake** in `Init` (no client/server version
  exchange, no capability list beyond `auto_replay_nacks`). A plugin built against an older
  proto that gains a required semantic will not know. This is a gap canal should close on
  day one with an explicit `(api_version, capabilities)` exchange in init — it costs one
  message and buys the entire future.
- In-process versioning is Go module versioning: `github.com/redpanda-data/benthos/v4`, with
  breaking changes accumulated behind `// TODO: V5` markers (§1.8, §13) and the v4 migration
  having removed "All components, features and configuration fields that were marked as
  deprecated" (CHANGELOG). The `Deprecated()` spec modifier + `SetRejectDeprecated` linting
  is the deprecation machinery, and it works — but note that **the plugin API itself has no
  per-component API version**, so a connector cannot say "I speak core API 2".

### 10.6 What the host injects: `Resources`

The capability bundle handed to every constructor (`public/service/resources.go`):

```go
func (r *Resources) EngineVersion() string
func (r *Resources) Label() string
func (r *Resources) Path() []string
func (r *Resources) StreamID() string
func (r *Resources) Logger() *Logger
func (r *Resources) Metrics() *Metrics
func (r *Resources) OtelTracer() trace.TracerProvider
func (r *Resources) FS() *FS

func (r *Resources) AccessCache(ctx context.Context, name string, fn func(c Cache)) error
func (r *Resources) HasCache(name string) bool
func (r *Resources) AccessInput(ctx context.Context, name string, fn func(i *ResourceInput)) error
func (r *Resources) HasInput(name string) bool
func (r *Resources) AccessOutput(ctx context.Context, name string, fn func(o *ResourceOutput)) error
func (r *Resources) HasOutput(name string) bool
func (r *Resources) AccessRateLimit(ctx context.Context, name string, fn func(r RateLimit)) error
func (r *Resources) HasRateLimit(name string) bool
func (r *Resources) AccessProcessor(ctx context.Context, name string, fn func(*ResourceProcessor)) error
func (r *Resources) HasProcessor(name string) bool
func (r *Resources) AccessCustomResource(ctx context.Context, typeName, label string, fn func(any)) error
func (r *Resources) HasCustomResource(typeName, label string) bool

func (r *Resources) GetGeneric(key any) (any, bool)
func (r *Resources) GetOrSetGeneric(key, value any) (actual any, loaded bool)
func (r *Resources) SetGeneric(key, value any)

func (r *Resources) ConnectionTest(ctx context.Context) ([]*ConnectionTestResult, error)
func (r *Resources) IntoPath(pathSegments ...string) *Resources
func (r *Resources) ManagedBatchOutput(typeName string, maxInFlight int, b BatchOutput) (*OwnedOutput, error)
func MockResources(opts ...MockResourcesOptFn) *Resources
```

Design notes:

- **`AccessX(ctx, name, fn func(X))` — callback-scoped resource borrowing, not a getter.**
  The framework controls lifetime, so a resource can be hot-reloaded (streams-mode
  `POST /resources/{type}/{id}`) without the borrower holding a stale pointer. **Steal
  this**; it is the mechanism that makes live reconfiguration safe.
- **`HasX(name)` for construction-time validation** — this is how the CDC input fails fast
  on a bad `checkpoint_cache` (§3.5) rather than at first commit.
- **`IntoPath(segments…)`** derives a child observability scope (§11), so nested components
  are auto-namespaced.
- **`RegisterCustomResource` + `AccessCustomResource(typeName, label, fn)`** (added
  CHANGELOG 4.69.0) is the extension point that lets a *connector family* share a
  configurable resource type without a core change:
  ```go
  // RegisterCustomResource registers a named custom resource type. The provided
  // spec defines the YAML configuration for the resource (a "label" field is
  // added automatically). The name is used as-is for the top-level YAML config
  // field and can be accessed by components via Resources.AccessCustomResource.
  //
  // Returns an error if the name conflicts with a built-in config field or a
  // previously registered custom resource type.
  //
  // If the value returned by the constructor implements Closer (with a
  // Close(context.Context) error method) it will be closed with the shutdown
  // context when the stream shuts down. As a fallback, io.Closer is also
  // supported but does not receive the shutdown context.
  ```
  This is a good answer to "how do connectors share a connection pool / a schema registry
  client / a checkpoint store" without the core knowing what those are. **Canal wants
  exactly this, and should use it for the checkpoint store** so that the store is pluggable
  without being privileged.
- `MockResources(...)` shipping in the public API means connector unit tests need no
  harness. Cheap, high-value.
- `GetGeneric/SetGeneric` on an `any` key is a typed-context escape hatch — pragmatic, and
  the kind of thing that ends up load-bearing. Note it cannot cross a process boundary.

### 10.7 Verdict

Benthos's answer to constraint #3 is: **keep the plugin interface to four blocking verbs
plus a single (ctx, err) ack, express capabilities as init-time data, and keep the error set
closed and small.** Under those conditions the gRPC/subprocess implementation is ~200 lines
per component type and needs zero core changes. That is the strongest possible validation of
canal's stated plan, and it also tells canal exactly which three things to get right up
front: (1) ack carries no state beyond an ID, (2) capabilities are data not type
assertions, (3) errors are a closed enum. Everything else can be retrofitted.

---

## 11. Observability

### 11.1 Metric names: fixed, flat, per-stage

The complete set of engine-emitted metrics, extracted from the source
(`GetCounter`/`GetTimer`/`GetGauge`/`*Vec` call sites under `internal/`):

**Input** (`internal/component/input/async_reader.go`)
```
input_received            counter   (incremented by msg.Len())
input_connection_up       counter
input_connection_failed   counter
input_connection_lost     counter
input_latency_ns          timer     (read → ack round trip)
```

**Output** (`internal/component/output/async_writer.go`)
```
output_sent               counter   (batch.MessageCollapsedCount(payload))
output_batch_sent         counter
output_error              counter
output_rejected           counter   (strict-mode rejections)
output_connection_up      counter
output_connection_failed  counter
output_connection_lost    counter
output_latency_ns         timer
```

**Processor**
```
processor_received        counter
processor_batch_received  counter
processor_sent            counter
processor_batch_sent      counter
processor_error           counter
processor_latency_ns      timer
```

**Buffer**
```
buffer_received           counter
buffer_batch_received     counter
buffer_sent               counter
buffer_batch_sent         counter
buffer_latency_ns         timer
```

**Rate limit / cache / batching** (`*Vec` — labelled)
```
rate_limit_checked        counter
rate_limit_triggered      counter
rate_limit_error          counter
cache_success             counter (vec)
cache_error               counter (vec)
cache_not_found           counter (vec)
cache_duplicate           counter (vec)
batch_created             counter (vec)
```

Observations:

- **Both message-count and batch-count are emitted** (`output_sent` vs `output_batch_sent`,
  `processor_received` vs `processor_batch_received`). Necessary once batching exists, and
  easy to forget.
- `input_latency_ns` is **read→ack**, i.e. end-to-end pipeline latency measured at the
  source, computed in the ack goroutine:
  ```go
	startedAt := time.Now()
	// ... later, in the ack goroutine:
	mLatency.Timing(time.Since(startedAt).Nanoseconds())
  ```
  That is the single most useful number in the system and it is a free consequence of the
  ack graph. **Canal gets this for free too and should emit it from day one.**
- `_ns` suffix on timers is a stated convention: "Delta should be measured in nanoseconds
  for consistency with other Benthos timing metrics."
- Connection lifecycle is counted (`_up`/`_failed`/`_lost`), which is what an operator needs
  to distinguish "never connected" from "flapping".
- **What is absent is conspicuous: no lag, no position, no queue depth, no in-flight gauge,
  no snapshot progress.** Consequence of §3 — the framework has no position concept, so it
  cannot emit one. Postgres CDC has to emit its own (`pg_wal_monitor_interval` — "How often
  to report changes to the replication lag"). So lag is per-connector, differently named
  per connector, and a UI cannot render it generically. **This is the observability
  consequence of the missing checkpoint model, and the direct argument for fixing it.**

### 11.2 Labels: automatic, derived from config position

Not per-connector code — the manager derives them (`internal/manager/type.go`):

```go
func (t *Type) forStream(id string) *Type {
	newT := *t
	newT.stream = id
	newT.logger = t.logger.WithFields(map[string]string{"stream": id})
	newT.stats = t.stats.WithLabels("stream", id)
	return &newT
}

func (t *Type) forLabel(name string) *Type {
	newT := *t
	newT.label = name
	newT.logger = t.logger.WithFields(map[string]string{"label": name})
	newT.stats = t.stats.WithLabels("label", name)
	return &newT
}

// IntoPath returns a variant of this manager to be used by a particular
// component path, which is a child of the current component, where
// observability components will be automatically tagged with the new path.
func (t *Type) IntoPath(segments ...string) bundle.NewManagement { return t.intoPath(segments...) }

func (t *Type) intoPath(segments ...string) *Type {
	newT := *t
	// ... append segments to componentPath ...
	pathStr := "root." + query.SliceToDotPath(newComponentPath...)
	newT.logger = t.logger.WithFields(map[string]string{"path": pathStr})
	newT.stats = t.stats.WithLabels("path", pathStr)
	return &newT
}
```

So every metric and every log line is tagged with `stream`, `label` (the user's optional
component label), and `path` (`root.pipeline.processors.2.branch.…`). And critically, the
manager is *the same object* that gets passed into child components via
`p.mgr.IntoPath(path...)` in `FieldInput`/`FieldOutput`/`FieldProcessor` (§7.4) — so
**nesting a component inside another component automatically nests its observability
namespace, with no cooperation from either connector.**

That is a genuinely excellent piece of design and canal should copy it exactly: a single
"observability scope" value, threaded through construction, that derives logger fields and
metric labels together from the config path.

Also: `stream` label is added even in single-stream mode so cardinality is stable:

```go
// ... This ensures that a label "stream" is added to metrics.
			t.stats = t.stats.WithLabels("stream", "")
```

Rewriting/filtering metric names is configurable via a Bloblang `mapping`
(`internal/component/metrics/mapping.go`, `namespaced.go`) — returning an empty string drops
the metric:

```go
	for _, mapping := range n.mappings {
		if newPath, labelKeys, labelValues = mapping.mapPath(newPath, labelKeys, labelValues); newPath == "" {
```

"Metric renaming/dropping as a config-level mapping" replaced earlier dedicated
`rename`/`whitelist`/`blacklist` metrics components (CHANGELOG: "The `rename`, `whitelist`
and `blacklist` metrics types are now deprecated, and the `path_mapping` field should be
used instead") — a good example of replacing three components with one expression.

### 11.3 Custom metrics from connectors

`public/service/metrics.go` — label keys bound at creation, values at use:

```go
func (m *Metrics) NewCounter(name string, labelKeys ...string) *MetricCounter
func (m *Metrics) NewTimer(name string, labelKeys ...string) *MetricTimer
func (m *Metrics) NewGauge(name string, labelKeys ...string) *MetricGauge

func (c *MetricCounter) Incr(count int64, labelValues ...string)
func (c *MetricCounter) IncrFloat64(count float64, labelValues ...string)
func (t *MetricTimer) Timing(delta int64, labelValues ...string)
func (g *MetricGauge) Set(value int64, labelValues ...string)
func (g *MetricGauge) SetFloat64(value float64, labelValues ...string)
func (g *MetricGauge) Incr(value int64, labelValues ...string)
func (g *MetricGauge) Decr(value int64, labelValues ...string)
```

with a nil-safety contract stated in the type doc — "It's safe to pass around a nil pointer
for testing components" — and implemented uniformly:

```go
func (c *MetricCounter) Incr(count int64, labelValues ...string) {
	if c == nil { return }
	c.cv.With(labelValues...).Incr(count)
}
```

**Nil-safe metric handles are a small, excellent decision**: connector tests never need a
metrics backend and never nil-panic. Copy it.

### 11.4 Health and status model

`ConnectionStatus` is the framework's health primitive, and it is a *list* to accommodate
composites (`internal/component/input/interface.go`):

```go
	// ConnectionStatus returns the current status of the given component
	// connection. The result is a slice in order to accommodate higher order
	// components that wrap several others.
	ConnectionStatus() component.ConnectionStatuses
```

Publicly (`public/service/stream.go`):

```go
// ConnectionStatus represents a current plugin component connection. The
// component can be identified by the label and/or the path of the component as
// found in a parsed config.
type ConnectionStatus struct {
	label     string
	path      []string
	connected bool
	err       error
}

func (c ConnectionStatus) Label() string
func (c ConnectionStatus) Path() []string
func (c ConnectionStatus) Active() bool

// Err returns an error preventing the connection when appropriate. An inactive
// connection may still yield a nil error in cases where the connection has not
// yet been attempted (during initialisation) or if the connection was
// intentionally closed (during shutdown).
func (c ConnectionStatus) Err() error

// ConnectionStatuses returns a list of connection statuses, one for each
// currently active plugin component. Not all components will yield a
// connectivity status, this is true for all broker types and orchestration
// components, but the child components they manage will yield where possible.
func (r *RunningStreamSummary) ConnectionStatuses() []ConnectionStatus
```

States are stored atomically by the drivers via `component.ConnectionPending`,
`ConnectionFailing(mgr, err)`, `ConnectionActive(mgr)`, `ConnectionClosed(mgr)` — e.g.:

```go
	rdr.connection.Store(component.ConnectionPending(rdr.mgr))
	// ...
	r.connection.Store(component.ConnectionFailing(r.mgr, err))
	// ...
	r.connection.Store(component.ConnectionActive(r.mgr))
	// ...
	r.connection.Store(component.ConnectionClosed(r.mgr))
```

**`(label, path, connected, err)` per component, aggregated up a tree, with the
"inactive but nil error means not-yet-attempted or intentionally-closed" nuance spelled out,
is the right health model** for a UI: it renders as a tree with a red/amber/green dot and a
tooltip. Canal should adopt it and extend the state enum (Pending/Active/Failing/Closed is
already better than a boolean, and `connected bool + err` is a lossy encoding of it —
publish the enum).

### 11.5 HTTP surface

Always registered (`internal/api/api.go`):

```go
	t.RegisterEndpoint("/ping", "Ping me.", handlePing)
	t.RegisterEndpoint("/version", "Returns the service version.", handleVersion)
	t.RegisterEndpoint("/endpoints", "Returns this map of endpoints.", handleEndpoints)

	// If we want to expose a stats endpoint we register the endpoints.
	if wHandlerFunc := stats.HandlerFunc(); wHandlerFunc != nil {
		t.RegisterEndpoint("/stats", "Exposes service-wide metrics in the format configured.", wHandlerFunc)
		t.RegisterEndpoint("/metrics", "Exposes service-wide metrics in the format configured.", wHandlerFunc)
	}
```

Behind `DebugEndpoints`: `/debug/config/json`, `/debug/config/yaml`, `/debug/stack`, and the
full `/debug/pprof/*` set (profile, heap, goroutine, block, mutex — with a `?rate=` form
value for mutex fraction — allocs, symbol, trace).

Readiness, single-stream (`internal/stream/type.go`):

```go
		if atomic.LoadUint32(&t.closed) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			healthCheckRes.Error = "stream terminated"
		} else if !inputStatuses.AllActive() || !outputStatuses.AllActive() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(healthCheckRes); err != nil { ... }
	t.manager.RegisterEndpoint(
		"/ready",
		"Returns 200 OK if all inputs and outputs are connected, otherwise a 503 is returned.",
		healthCheck,
	)
```

```go
// IsReady returns a boolean indicating whether both the input and output layers
// of the stream are connected.
func (t *Type) IsReady() bool {
	return t.inputLayer.ConnectionStatus().AllActive() && t.outputLayer.ConnectionStatus().AllActive()
```

**`/ready` returns a status code *and* a JSON body of per-component statuses** — so the same
endpoint serves a k8s probe and a UI. Good.

`/endpoints` returning the endpoint map with descriptions is a nice self-describing touch,
and `RegisterEndpoint(path, desc, handler)` requiring a description is the mechanism that
makes it free:

```go
// RegisterEndpoint registers a http.HandlerFunc under a path with a
// ... (description used for the /endpoints map)
func (t *Type) RegisterEndpoint(path, desc string, handlerFunc http.HandlerFunc)
```

Streams mode adds (`internal/stream/manager/api.go`):

```go
	m.manager.RegisterEndpoint(
		"/ready",
		"Returns 200 OK if the inputs and outputs of all running streams are connected, otherwise a 503 is returned. If there are no active streams 200 is returned.",
		m.HandleStreamReady,
	)
	// ... and, when CRUD is enabled:
	"/resources/{type}/{id}"  // POST: Create or replace a given resource configuration of a specified type. Types supported are `cache`, `input`, `output`, `processor` and `rate_limit`.
	"/streams/{id}/stats"     // GET a structured JSON object containing metrics for the stream.
	"/streams/{id}"           // POST (Create), GET (Read), PUT (Update), PATCH (Patch update), DELETE (Delete)
	"/streams"                // GET: List all streams along with their status and uptimes. POST: replace the whole set.
```

Response shapes a UI can consume — stream list:

```go
	type confInfo struct {
		Active    bool    `json:"active"`
		Uptime    float64 `json:"uptime"`
		UptimeStr string  `json:"uptime_str"`
	}
	infos := map[string]confInfo{}
```

(and the single-stream GET adds `Config any \`json:"config"\``), and per-stream stats:

```go
			values := map[string]any{}
			for k, v := range info.metrics.GetCounters() {
				values[k] = v
			}
			for k, v := range info.metrics.GetTimings() {
				ps := v.Percentiles([]float64{0.5, 0.9, 0.99})
				values[k] = struct {
					P50 float64 `json:"p50"`
					P90 float64 `json:"p90"`
					P99 float64 `json:"p99"`
				}{ P50: ps[0], P90: ps[1], P99: ps[2] }
			}
			values["uptime_ns"] = info.Uptime().Nanoseconds()
```

**Per-stream metrics with p50/p90/p99 over a JSON API, no Prometheus required, is exactly
what canal's frontend needs** — a local-dev single binary can serve its own dashboard with
zero external dependencies. Steal the shape.

Config linting is exposed through the same API (`lintStreamConfigNode`,
`type lintErrors struct { LintErrs []string \`json:"lint_errors"\` }`), so a UI can validate
before submitting.

### 11.6 What a UI can and cannot read — summary

Can: registered component list + full config schema + JSON Schema (§7.5); per-field
`ShortDescription`/`is_advanced`/`is_secret`/`is_optional`/`is_deprecated`; lint results with
line/column; per-stream active/uptime/config; per-stream counters and timer percentiles;
per-component connection status tree with label/path/error; `/ready`; `/version`;
`/endpoints`; pprof.

Cannot: source position / lag / replication delay (per-connector, unnamed);
snapshot progress; in-flight depth; per-split state; anything about *where* the pipeline is
in its data. Also no structured event/audit stream — only logs.

---

## 12. Deployment

### 12.1 Two modes, one binary

**Single-stream (`run`)**: one `input → buffer → pipeline → output` per process
(`internal/stream/config.go`):

```go
// Config is a configuration struct representing all four layers of a Benthos
// stream.
type Config struct {
	Input    input.Config    `yaml:"input"`
	Buffer   buffer.Config   `yaml:"buffer"`
	Pipeline pipeline.Config `yaml:"pipeline"`
	Output   output.Config   `yaml:"output"`

	rawSource any
}
```

Four layers, fixed. Fan-out/fan-in/routing are achieved *inside* the input/output layers via
broker components, not by generalising the topology. **That is a deliberate and effective
constraint**: the config has one shape, so docs, UI and mental model stay simple, while
`broker`/`switch`/`fallback`/`retry` recursion supplies arbitrary topology. Canal, which
wants "multiple types of pipelines … possibly fan-out/fan-in and transform chains", should
consider keeping the *outer* shape fixed and pushing variety into composable components,
rather than making the pipeline a free-form DAG in config.

**Streams mode (`streams`)**: N independent streams in one process, CRUD'd over HTTP
(`internal/cli/streams.go`):

```
Run {{.ProductName}} in streams mode, where multiple pipelines can be executed in a
single process and can be created, updated and removed via REST HTTP
endpoints.

  {{.BinaryName}} streams
  {{.BinaryName}} streams -o ./root_config.yaml
  {{.BinaryName}} streams ./path/to/stream/configs ./and/some/more
  {{.BinaryName}} streams -o ./root_config.yaml ./streams/*.yaml

The config field specified with the --observability/-o flag is known as the root
config and should only contain observability and service-wide config fields such
as http, metrics, logger, resources, and so on.
```

with flags `--no-api`, `--prefix-stream-endpoints` (default true — "Whether HTTP endpoints
registered by stream configs should be prefixed with the stream ID").

**The root-config / stream-config split is a good idea worth stealing**: service-wide
concerns (http, logger, metrics, tracer, resources) in one file; per-pipeline concerns in
many. It makes "same pipeline, different environment" trivial and it is what lets streams be
managed as data.

### 12.2 Embedding: `StreamBuilder` and `ResourceBuilder`

`public/service/stream_builder.go` (31 KB) and `resource_builder.go` give a programmatic
path: `NewStreamBuilder()`, `SetYAML(...)`, `AddConsumerFunc`/`AddBatchConsumerFunc`,
`AddProducerFunc`, `Build() (*Stream, error)`, `Stream.Run(ctx)`. The example in §10.1 shows
the shape. `Stream` also exposes:

```go
// ConnectionTest attempts to test the connections of each configured component
// within the stream without actually starting the stream itself. This is safe
// to call before Run.
func (s *Stream) ConnectionTest(ctx context.Context) (ConnectionTestResults, error)
```

"Validate connectivity of the whole config without running it" is a strong UX affordance for
a config UI — steal it.

And a candid API-design admission sitting in `stream_builder.go`:

```go
// TODO V5: Add context here, Travis is onto us.
```
```go
		// TODO: V5 Prevent default input/output
```

i.e. `Build()`-family functions lack `context.Context` and cannot be fixed without a major
version. **Put `ctx` on every method that could block, from the start.**

### 12.3 Work assignment, rebalancing, coordination: none in the core

There is no cluster membership, no leader election, no partition assigner, no shard
manager, no distributed state store, and no rebalancing protocol anywhere in either repo's
core. `internal/stream/manager` is a `map[string]*StreamStatus` guarded by a mutex —
process-local.

Horizontal scaling is therefore **delegated entirely to the source's own protocol**:

- Kafka/Redpanda: consumer groups. From `redpanda_common.adoc`: "When using consumer groups
  the offsets of "delivered" records will be committed automatically and continuously, and
  in the event of restarts these committed offsets will be used in order to resume from where
  the input left off." Run N pods, the broker rebalances. Coordination is Kafka's.
- SQS/Pub/Sub: competing consumers; the broker's visibility timeout is the lease.
- Postgres/MySQL CDC: **a replication slot is single-consumer.** There is no way to shard it.
  Scaling is vertical only, and two processes pointed at the same
  `checkpoint_cache`/`checkpoint_key` will clobber each other (§3.3 — no CAS, no fencing).
- Files/objects/HTTP polling: no assignment mechanism at all; N replicas duplicate work
  unless the user shards by config.

Consequently the deployment story is "stateless replicas of a stateless process, plus
whatever the source gives you". For a k8s user that is genuinely convenient — a `Deployment`,
a `/ready` probe, an HPA on `output_sent` rate — and it is why Benthos is easy to operate.
`pipeline.threads: -1 → runtime.NumCPU()` also means vertical scaling is free.

### 12.4 State store

There isn't one, in the framework sense. There is:

- **`Cache` resources** (§1.8) — the de facto progress store for CDC connectors, chosen by
  the user per input (`checkpoint_cache: my_redis`), unversioned, no CAS on value, no
  fencing token, no ownership.
- **`Buffer`** — optional local durability (`memory`, `sqlite`, `system_window`), explicitly
  guarantee-weakening for `memory`.
- **Custom resources** (§10.6) — the sanctioned way to add a shared, configurable,
  lifecycle-managed store type without a core change.
- **`resources` in the root config** + `POST /resources/{type}/{id}` for hot replacement.

### 12.5 Verdict for canal

Canal's end-state explicitly requires "horizontal, k8s, multi-worker, coordinated" *and* a
standalone single binary. Benthos gives you the second for free and **explicitly does not
attempt the first** — it borrows coordination from the source. That works for
partition-sharded sources and fails for exactly the case canal cares about (a generic source
with a snapshot phase and a single logical stream).

What to take:
- **The single binary with two run modes and the root-config/stream-config split.** This is
  the right local-dev↔production story and it costs almost nothing.
- **The fixed four-layer stream shape plus recursive composable components** for topology
  variety.
- **`/ready` returning a status tree**, `/streams` CRUD, per-stream stats.
- **Streams-as-data**: a stream is a config document with an ID; create/update/delete over
  HTTP. That is the substrate a control plane needs.

What canal must add, and should design for now even if built later:
- **Splits as the unit of work assignment** (same concept as §4.2's snapshot splits and
  §8.4's per-partition checkpoint scope). If splits are first-class, assignment is a
  function `splits × workers → plan`, and rebalancing is a diff of plans.
- **Ownership leases with fencing tokens** in the checkpoint store, so a rebalance or a
  zombie worker cannot double-commit. This is the missing primitive that makes multi-worker
  safe, and it is the reason `Cache` cannot be the checkpoint store as-is.
- **A coordination interface, pluggable and single-node by default** (an in-process
  implementation for the standalone binary; etcd/k8s-lease/database for the cluster), so the
  single-binary and clustered builds run the same code path with a different plugged
  coordinator.

---

## 13. What they got right / what they got wrong

### 13.1 Got right

1. **Four verbs per component, and no state machine.** `Connect / Read|Write / Close` plus a
   constructor. There is genuinely nothing else to implement. This is why "implement the
   interface, register it, done" is true here and not aspirational.
2. **`AckFunc(ctx, err) error` as the single progress primitive.** It composes across
   fan-out, filtering, 1→N expansion and N→M rebatching with zero core knowledge, it lets
   sinks be completely progress-free, and — because it carries no state — it survives
   translation to a `batch_id` over gRPC (§10.3). A tiny primitive that bought an enormous
   amount.
3. **Sinks have no ack callback.** `WriteBatch(ctx, batch) error`. Return nil or don't. The
   asymmetry with inputs is correct and is the reason a new sink is ~40 lines.
4. **Batched and unbatched variants of every interface, with the framework adapting between
   them.** `airGapReader` wraps a single-message `Read` into a one-element `message.Batch`;
   `OnlySinglePayloads` splits batches for unbatched sinks. Connector authors implement
   whichever is natural to their protocol.
5. **The config spec system.** One `ConfigSpec` per connector yields validation, defaults,
   required/optional, linting with line/column and a typed lint enum, reference docs, JSON
   Schema, and form-UI hints (`ShortDescription`, `Advanced`, `Secret`, `Examples`,
   `Version`). Docs are generated and carry a "THIS FILE IS AUTOGENERATED" banner. This is
   the best-executed part of the system and it is the whole answer to canal's frontend goal.
6. **Reusable composite field specs paired with extractors.** `NewBatchPolicyField`/
   `FieldBatchPolicy`, `NewBackOffField`/`FieldBackOff`, `NewTLSField`, `NewOutputMaxInFlightField`.
   Every connector's `batching:`/`retry:`/`tls:` block is identical without any coordination,
   and retry/backoff is genuinely configuration rather than code.
7. **The `Batcher` API: `Add() bool` + `UntilNext() (d, ok)` + `Flush(ctx)`.** Goroutine-free
   inverted-control batching that drops into any select loop. Perfect.
8. **`BatchBuffer` as an explicit, documented guarantee-weakening seam.** The one place the
   ack chain may be cut, with the tradeoff written into the interface doc comment and into
   the component docs ("This buffer intentionally weakens the delivery guarantees of the
   pipeline and therefore should never be used in places where data loss is unacceptable").
   Dangerous options should always be shipped this way.
9. **`EndOfInput()` + `ErrEndOfBuffer` / `ErrEndOfInput`.** End-of-stream as a value that
   propagates through stateful stages and terminates the process gracefully. This is what
   makes batch/snapshot pipelines the *same runtime* as streaming pipelines rather than a
   separate mode.
10. **Automatic observability namespacing via `IntoPath`.** Metric labels and log fields
    (`stream`, `label`, `path`) derived from the config path, threaded through construction,
    inherited by nested components. Zero connector cooperation.
11. **`Resources.AccessX(ctx, name, fn)` — callback-scoped resource borrowing.** The
    framework keeps lifetime control, which is what makes `POST /resources/{type}/{id}` hot
    replacement safe.
12. **`Environment` as a value with `Clone`/`With`/`Without`.** A global registry that is
    really a default instance. Sandboxing, tests, allow-listing and "what's in this build?"
    all fall out.
13. **`MockResources()` and nil-safe metric handles.** Connector unit tests need no harness
    and cannot nil-panic on metrics.
14. **The gRPC/subprocess plugin as an ordinary in-process plugin.**
    `var _ service.BatchInput = (*input)(nil)`, with `ErrNotConnected` doubling as "the
    plugin process died" so supervision reuses the connection state machine. Zero core
    changes. This is the existence proof canal needs.
15. **Capabilities as init-time data.** `auto_replay_nacks` as a bool in
    `BatchInputInitResponse`, `maxInFlight`/`BatchPolicy` as constructor return values. Data
    crosses process boundaries; type assertions don't.
16. **`read → ack` latency as a first-class metric** (`input_latency_ns`), a free consequence
    of the ack graph and the most useful number in the system.
17. **`/ready` returning both a status code and a JSON status tree**, plus `/streams` CRUD and
    per-stream p50/p90/p99 without requiring Prometheus.
18. **Fixed four-layer stream shape + recursive composable components.** Topology variety via
    `broker`/`switch`/`fallback`/`retry` rather than a free-form DAG in config. Keeps docs,
    UI and mental model tractable.
19. **Streams-as-data**, plus the root-config/stream-config split. The substrate a control
    plane needs.
20. **`Stream.ConnectionTest(ctx)` — validate all connectivity without running.** Excellent
    config-UI affordance.

### 13.2 Got wrong — concrete, documented pain

**(a) No checkpoint model. This is the big one.**

There is no position type, no checkpoint store interface, no commit scheduling, and nothing
in the core that can answer "where are we?". Consequences visible in-tree:

- The reusable checkpointer lives **outside both repos** (`github.com/Jeffail/checkpoint`),
  and each CDC connector re-wires it with its own cache key, its own serialisation
  (`fmt.Sprintf("%s@%08X", pos.Name, pos.Pos)` — hand-rolled per connector), its own flush
  policy and its own mutex (`i.checkpointMu`).
- **Commit failure is unreportable.** `async_reader.go`: `if err = aFn(closeNowCtx, res); err != nil { …Logger().Error… }`
  and `async_writer.go`: `_ = ackFn(closeLeisureCtx, err)`. A failed checkpoint write is a log
  line. Data is delivered, progress is lost, nothing escalates.
- **No CAS, no fencing, no ownership.** `Cache` has `Add` ("returns an error if the key already
  exists") but the doc explicitly permits unreliability: "It is okay for caches to return nil
  on duplicates if it isn't possible to implement." Two workers on one
  `checkpoint_cache`/`checkpoint_key` silently clobber each other.
- **Lag/position is unobservable generically**, so a UI cannot render it (§11.1). Postgres
  invents `pg_wal_monitor_interval`; MySQL doesn't have an equivalent; names and semantics
  differ per connector.

**(b) Snapshots are not resumable, and there is no phase state.**

`if i.streamSnapshot && pos == nil { … }` is the entire phase decision. Snapshot batches are
tracked with a `nil` position and the ack closure declines to commit
(`// This has no offset - it's a snapshot message`). The first checkpoint is written only at
`snapshot_complete`. **A crash at 90% of a large snapshot restarts from zero**, and there is
no representation for "partially snapshotted". The connector also has to invent an in-band
sentinel (`messageOperationSnapshotComplete` on an internal channel) to signal its own phase
transition, because the framework has no vocabulary for it. Related fossil:
`snapshot_memory_safety_factor` is `.Deprecated()` in the Postgres spec — a memory-based
snapshot throttle that didn't work out.

**(c) No record identity, so partial-batch failure is index gymnastics.**

The clearest signal is a `Deprecated` marker on a *public* method with the word "harmful":

```go
// Deprecated: This method is harmful and should be avoided as indexes are not
// guaranteed to match a hypothetical origin batch that they might be compared
// against. Use WalkMessagesIndexedBy instead.
func (err *BatchError) WalkMessages(fn func(int, *Message, error) bool)
```

Its replacement requires the caller to have created an `Indexer` *before* dispatch, and even
then: "Important! The order of messages walked is not guaranteed to match that of the source
batch. It is also possible for any given index to be represented zero, one or more times."
The costs, all in-tree:

- ~60 lines of `NewSortGroup`/`WalkPartsBySource`/`GetIndex` in `async_writer.go` strict mode
  just to answer "some of this batch is bad" (§9.5).
- `AutoRetryNacksBatched`'s mutator must reconstruct the failed subset and, when correlation
  fails, gives up and retries the whole batch: `// We can't associate our source batch with the one that's associated
  // with the batch error, therefore we fall back towards treating every message as if it was errored the same.`
- The `Processor` contract has to forbid constructing messages: "The Message types returned
  MUST be derived from the provided message, and CANNOT be custom instantiations of Message"
  — because identity and ack plumbing live on the hidden `*message.Part`.

**(d) The retry/error-handling story is a binary with no middle.**

`auto_replay_nacks` defaults to `true` = "automatically replayed indefinitely, eventually
resulting in back pressure if the cause of the rejections is persistent"; `false` = "these
messages will instead be deleted". No max attempts, no dead-letter, no quarantine in that
field. The community-visible symptom is exactly what you'd predict — a pipeline stuck
retrying a poison record and making no progress (redpanda-data/connect Discussion #2444,
"Bethos seems to be stuck retrying failed messages"). Related in-tree evidence:

- `autoretry` has an unbounded retry queue and a disabled cleanup path kept in the tree:
  ```go
  // TODO: Ensure docs around auto retry and all implementations are okay with
  // nacks on termination, otherwise we leave them.
  //
  //nolint:unused // Keeping this around for now.
  func (l *List[T]) nackAllPending() error
  ```
  i.e. on shutdown, pending retries are neither nacked nor delivered.
- `async_writer.go` has `// TODO: Maybe reintroduce a sleep here if we encounter a busy retry loop`.
- The `switch` output's internal-retry default caused an indefinite block with `reject`
  outputs; it was first patched with a **lint rule** ("Added a linting rule that warns
  against having a `reject` output under a `switch` broker without `retry_until_success`
  disabled") and only fixed by flipping the default in v4 ("The `switch` output field
  `retry_until_success` now defaults to `false`").
- `error_handling.strict` (4.76.0) is the retrofit: "This behaviour is expected to become the
  default in the next major version." Terminal-error semantics arrived in a minor release of
  v4, ten years in.

**(e) The ack has no deadline, so a per-batch goroutine+timer was bolted on.**

"The AckFunc will be called for every message at least once, but there are no guarantees as
to when this will occur." The fix is `timely_nacks_maximum_wait`, marked `EXPERIMENTAL`,
which spawns **one goroutine and one timer per batch**
(`input_force_timely_nacks.go`), with a description that names the real problem: "This can be
useful for avoiding situations where certain downstream components can result in blocked
confirmation of delivery that exceeds SLAs." Community-visible as
redpanda-data/connect Issue #2383 ("Asyn preserver blocking shutdowns"). Also note
`ackOnce`-style `sync.Once` wrapping is reimplemented independently in at least three places
(`forceTimelyNacks`, `scanner.ackOnce`, `autoretry.wrapPendingAck`) — ack idempotency should
have been a framework guarantee.

**(f) `ErrBackOff` is a retry hint the framework mostly ignores.**

```go
// NOTE: ErrBackOff is opt-in for upstream components and therefore only a
// subset of plugin calls will respect this error. Currently the following
// methods are known to support ErrBackOff:
//
// - Input.Connect
// - BatchInput.Connect
// - Output.Connect
// - BatchOutput.Connect
```

Return it from `Read` or `Write` and nothing happens. A retry-classification mechanism whose
effect depends on which method you returned it from is undiscoverable.

**(g) Optional capabilities via type assertion don't scale (and don't cross processes).**

`ConnectionTestable` must be manually forwarded by every wrapper — `airGapReader`,
`airGapBatchReader`, `airGapWriter`, `airGapBatchWriter`, `maxInFlight`,
`maxInFlightBatched`, `autoRetryInput`, `autoRetryInputBatched`,
`forceTimelyNacksInputBatched` — nine near-identical `t, ok := x.(ConnectionTestable)` blocks.
`batchedCache` (the `SetMulti` optimisation) is **unexported**, so implementers must learn it
from prose. And none of it can cross the gRPC boundary, which is why the out-of-process path
had to express its one capability as a bool in the init response instead.

**(h) Undocumented back doors through the public API.**

`RegisterBatchInput`/`RegisterBatchOutput` probe for an undocumented
`interface{ Unwrap() input.Streamed }` / `Unwrap() output.Streamed` so meta-components can
bypass the air gap, and five public types expose `XUnwrapper() any` marked
`// XUnwrapper is for internal use only, do not use this.` (`OwnedInput`, `OwnedOutput`,
`OwnedProcessor`, `Batcher`, `Resources`), plus `interop.UnwrapManagement` /
`interop.NewUnwrapInternalOutput` used by first-party components like `retry`. There is no
sanctioned way to say "I am a pipeline stage, not a leaf" — so the first-party
composite components use a private channel that third parties can't rely on.

**(i) The generic driver special-cases a component by name.**

```go
			if err != nil {
				if w.typeStr != "reject" {
					w.log.Error("Failed to send message to %v: %v\n", w.typeStr, err)
				} else {
					w.log.Debug("Rejecting message: %v\n", err)
				}
```

A string comparison against a component name inside the generic output driver — precisely the
"core knowledge of the connector" that canal's constraint #4 forbids. Small, but it is the
thin end of the wedge, and it exists because "this sink is expected to fail" isn't
expressible.

**(j) Composable knobs that deadlock, documented rather than prevented.**

```
**WARNING**: Batching policies at the output level will stall if this field limits the
number of messages below the batching threshold.
```

Input `max_in_flight: 10` + output `batching.count: 100` ⇒ permanent stall. Both values are in
the same config document; a cross-component lint was possible and wasn't written. More
broadly there are five overlapping bounding mechanisms (§8.8 table) under three user-facing
names, with no single place to observe which one is binding.

**(k) The config accessor error ladder.**

Every `FieldX` returns `(T, error)` even though the spec already guaranteed type and presence,
so every constructor is a ladder of near-impossible error checks (`FieldBatchPolicy` is five
of them for four fields). Pre-generics API, and now unfixable without a major version.

**(l) Signatures frozen with known defects — the `V5` backlog.**

In-tree, in the public API:
```go
	// TODO: V5 Add this (or replace the int based method)
	// IncrFloat64(count float64)
```
```go
	// TODO: V5 Add this (or replace the int based method)
	// SetFloat64(value float64)
```
```go
// TODO V5: Add context here, Travis is onto us.
```
```go
		// TODO: V5 Prevent default input/output
```
```go
func (m *Message) AsBytes() ([]byte, error) {
	// TODO: Escalate errors in marshalling once we're able.
	return m.part.AsBytes(), nil
}
```
```go
type airGapGauge struct {
	// TODO: This is a hack and we don't really use incr/decr internally in our
	// metrics. Can we ditch it?
	v         int64
```
Plus `// TODO: Add linting rule to ensure we aren't unbounded if necessary.` twice in
`config_backoff.go` — so `NewBackOffField`'s `allowUnbounded` parameter currently only
changes the description text, not the lint. And an exported typo that can never be fixed:
`type TemplatDataPluginExample struct` (missing "e").

Lessons: put `context.Context` on every blocking method; choose float64 for
counters/gauges; never return an `error` you don't populate; and design the
capability-extension mechanism *before* v1 so additions don't need a major version.

**(m) No exactly-once, no transactional sinks, no portable dedup — and ordering is
conditional.**

At-least-once only, inherited from the source ("as long as the input source supports
at-least-once delivery"). Ordering is lost to error handling
("this includes reattempting delivery of data when the ordering of that data can no longer be
guaranteed"), `pipeline.threads > 1`, output `max_in_flight > 1`, out-of-order acks, and
fan-out brokers. Dedup is a `dedupe` processor over a `Cache` whose CAS primitive is
explicitly allowed to be unreliable. All defensible for a "connect anything" tool; all
material if canal wants stronger semantics.

**(n) A deep, non-portable expression language as a hard dependency.**

Bloblang is embedded in the *interfaces*, not just the features: `ConfigField.LintRule(blobl)`
and `ConfigSpec.LintRule(blobl)` express **config validation** as Bloblang; `BatchPolicy.Check`
expresses **batch triggering** as Bloblang; metrics renaming/dropping is a Bloblang mapping;
`Environment.UseBloblangEnvironment` exists because the language is a first-class
sub-environment. That is a large, load-bearing surface and a real adoption cost — the common
community complaint is the learning curve plus non-transferability, and the practical one is
debuggability (there is no print/log facility inside a mapping). Canal should decide
deliberately: either commit to an expression language and get declarative validation and
predicates that a UI can also evaluate, or keep the interfaces language-free and pay for
validation/predicates in Go.

**(o) Memory behaviour is a recurring operational complaint, structurally.**

Retained state = in-flight ack tree + retry queue + batching buffers + `memory` buffer +
structured-payload clones. Every one of those is a policy the *user* must bound, via
different knobs, with the guidance being estimates ("Since this calculation is only an
estimate, and the real size of messages in RAM is always higher, it is recommended to set the
limit significantly below the amount of RAM available"). Community issues in
redpanda-data/connect (#2404 memory growth in a dedupe pipeline, #997 memory spike with
broker + batching) are symptoms of the same root: no single accounting of retained bytes.

**(p) Licensing/governance discontinuity — a real ecosystem risk, verifiable in-tree.**

The core engine (`redpanda-data/benthos`) is MIT (`LICENSE`: "Permission is hereby granted,
free of charge…"). The connector distribution is **per-file dual-licensed**: under
`connect/internal/impl`, **540 files carry the Apache-2.0 header and 299 carry**

```
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md
```

`connect/licenses/` contains `Apache-2.0.txt`, `rcl.md`, `cla.md`, and separate header
templates (`Apache-2.0_header.go.txt`, `rcl_header.go.txt`). The MySQL CDC connector analysed
throughout this dossier is one of the RCL files. So roughly a third of the connector estate
is under a source-available licence, per file, after the 2024 acquisition and rebrand — which
prompted community discussion (redpanda-data/connect issue #2621 "Document change in
licensing") and community forks. **The lesson for canal is not about licence choice; it is
that "adopt proven patterns, own the interfaces" (constraint #2) is vindicated:** taking the
*design* of `AckFunc`, `ConfigSpec`, `BatchPolicy` and the plugin proto costs nothing and
carries no governance risk, whereas depending on the framework would have exposed canal to a
relicensing event mid-project.

### 13.3 The one-line summary

Benthos is a **near-perfect transport-and-composition framework with no notion of position**.
Everything it does well follows from the tiny ack primitive; everything it does badly —
unresumable snapshots, unobservable lag, unsafe multi-worker, per-connector checkpoint
reinvention, no fencing — follows from refusing to model a checkpoint. Canal needs both
halves.

---

## 14. Steal this

Ordered roughly by value. Each is one sentence plus the file to read.

**Interface shape**

1. Keep the source/sink surface to four verbs — `Connect(ctx)`, `Read/ReadBatch(ctx)` or
   `Write/WriteBatch(ctx, …)`, `Close(ctx)` — with no `Configure`/`Open`/`Commit` callbacks,
   because config lands pre-parsed in the constructor and commit lives in the ack.
   (`benthos/public/service/{input,output}.go`)
2. Make the ack a single tiny value — `func(ctx, err) error` — so it composes across fan-out,
   filtering and rebatching *and* survives translation to an opaque `batch_id` over a wire.
   (`input.go`; `connect/proto/redpanda/runtime/v1alpha1/input.proto`)
3. Give sinks no progress callback at all: returning `nil` from `WriteBatch` *is* the ack, so
   a new sink can never get checkpointing wrong. (`output.go`)
4. Scope the connect context to the connect attempt only, and say so in the doc comment, so
   connectors are forced to own their own connection lifetime. (`input.go`)
5. Ship both batched and unbatched variants of every interface and let the framework adapt
   between them, so connector authors implement whichever matches their protocol.
   (`airGapReader` / `airGapBatchReader` in `input.go`)
6. Use `ErrEndOfInput` (and `EndOfInput()` + `ErrEndOfBuffer` through stateful stages) so
   bounded/batch/snapshot pipelines terminate gracefully on the *same* runtime as streaming
   ones. (`errors.go`, `buffer.go`)
7. Make the buffer/decoupling stage a separate pluggable component whose interface doc states
   exactly when it weakens delivery guarantees. (`buffer.go`)
8. Provide an ack-aggregating codec/scanner stage plus a `SimpleBatchScanner` +
   `AutoAggregateBatchScannerAcks` reduction, so no connector author hand-writes fan-out ack
   counting. (`scanner.go`)

**Record model**

9. Give the record one payload with two lazily-cached views (bytes ⇄ structured) and encode
   mutability in the accessor name (`AsStructured` vs `AsStructuredMut`), with
   `HasBytes()`/`HasStructured()` so sinks can take the cheap path. (`message.go`)
10. Offer three metadata tiers — string, mutable-any, and `ImmutableValue` with an engine-called
    `Copy()` — so expensive derived values like schemas ride along without a per-record clone.
    (`message.go`)
11. Carry processing errors *on* the record (`SetError`/`GetError`) rather than returning them,
    which is what makes mark-and-route error handling possible with no extra vocabulary.
    (`message.go`)
12. Make "which metadata escapes to the sink" a single reusable config field
    (`NewMetadataFilterField`) implemented once, not per sink.
    (`config_metadata_filter.go`)
13. Do **not** copy their positional batch identity — give every record a stable
    framework-assigned in-flight ID, and delete the ~200 lines of sort-group correlation their
    strict mode and retry mutator need. (read `errors.go` `WalkMessages` deprecation +
    `async_writer.go` strict block as the cautionary tale)

**Checkpointing (the biggest divergence)**

14. Own a generic contiguous-prefix checkpoint resolver in the core, whose `Track(ctx, pos, weight)`
    returns a `resolve func() *Pos` that yields non-nil only when the committable prefix
    advances, and whose capacity blocks `Track` to give you back-pressure for free.
    (`Jeffail/checkpoint` `capped.go` + `uncapped.go` — copy the algorithm, own the type)
15. Model the position type so it can express "safe resume point" distinctly from "record
    position", because CDC must resume only at transaction boundaries. (see `latestXIDPos` in
    `connect/internal/impl/mysql/input_mysql_stream.go`)
16. Allow a record to carry a *null* position meaning "not resumable" (snapshot rows), and make
    the framework skip committing those. (same file, `// This has no offset - it's a snapshot message`)
17. Treat a failed commit as an escalatable condition, not a log line — their
    `_ = ackFn(...)` and `if err = aFn(...); err != nil { …Error… }` silently lose progress
    after successful delivery. (`async_reader.go`, `async_writer.go`)
18. Keep the checkpoint store *pluggable but not a cache*: it needs versioned/CAS writes and a
    fencing token, which their `Cache.Add` explicitly declines to guarantee
    ("It is okay for caches to return nil on duplicates if it isn't possible to implement").
    (`cache.go`) — register it as a custom resource type (`RegisterCustomResource`) so it stays
    unprivileged.
19. Expose committed position and lag as framework metrics so a UI can render progress
    generically — the absence of these is the direct cost of their design. (§11.1)

**Snapshot / splits**

20. Make "split" a first-class concept (table+PK-range, file+byte-range, shard+key-range) and
    use it as the single unit of snapshot chunking, resumability, per-key ordering, in-flight
    accounting *and* multi-worker assignment. (their snapshot parallelism in
    `connect/internal/impl/mysql/snapshot.go` has the first, and nothing else)
21. Persist phase as durable state (`pending → snapshotting → catch-up → streaming`) with a
    per-split cursor, so a crash mid-snapshot resumes — theirs restarts from zero because the
    phase decision is `pos == nil`. (`input_mysql_stream.go` `Connect`)
22. Let the framework own the handoff invariant "stream from the position captured at snapshot
    start", and chunk with keyset pagination on the split key rather than `OFFSET`.
    (`snapshot.go`: `START TRANSACTION WITH CONSISTENT SNAPSHOT` + `SHOW MASTER STATUS`,
    `buildOrderByClause`)
23. Emit snapshot records through the *same* envelope, batcher, ack path and sinks as stream
    records — one pipeline, two producers. (they got this right)

**Schema**

24. Define one canonical schema type as the lossless intersection of the formats you support,
    with parameterised logical types in a separate optional `LogicalParams` struct so new
    parameterised types don't widen the enum or break existing schemas.
    (`benthos/public/schema/common.go`)
25. Include an unknown/arbitrary-precision escape hatch (`BigDecimal`) for sources that cannot
    report precision, or you will silently corrupt numerics. (`bigdecimal.go`)
26. Identify schemas by a structural SHA-256 fingerprint, and give each sink a
    `Cache[T]` + one `ConvertFunc[T]` so N×M format conversion becomes N+M and is memoised.
    (`public/schema/cache.go`)
27. Serialise schemas into the generic value space (`ToAny`/`ParseFromAny`) so schema mapping
    uses the same machinery as data mapping — and note their honest admission that the
    schema-of-schemas isn't expressible in the schema type. (`common.go` `ToAny` NOTE)
28. Diverge: put schema on the envelope as a typed optional facet and model a schema-change
    *event*, rather than attaching it under a conventional metadata key and leaving each sink
    to discover drift record-by-record. (their `MetaSetImmut("schema", …)`)

**Config**

29. Make one `ConfigSpec` per connector the single source of truth for validation, defaults,
    required/optional, docs, JSON Schema and form UI — this is the entire answer to canal's
    frontend goal with zero per-connector UI code. (`config.go`, `stream_schema.go`,
    `config_docs.go`)
30. Copy the field modifier set — `Description`, `ShortDescription` ("inline help within a form
    UI"), `Advanced`, `Secret`, `Optional`, `Default`, `Examples`, `Version`, `Deprecated` —
    and the tri-state `required / has-default / optional` with `Contains(path…)` to test
    presence. (`config.go`)
31. Ship reusable composite field specs paired with extractors (`NewBatchPolicyField` +
    `FieldBatchPolicy`, `NewBackOffField` + `FieldBackOff`, TLS, `max_in_flight`), so
    retry/backoff/batching are configuration and every connector's blocks look identical.
    (`config_batch_policy.go`, `config_backoff.go`)
32. Let a connector's config *contain other components* (`NewOutputField` → `FieldOutput` →
    `OwnedOutput`) with automatic observability nesting via `IntoPath`, because that is how
    `retry`/`fallback`/`switch`/`broker` exist without any core special-casing — and it is how
    canal gets fan-out/fan-in and transform chains. (`config_output.go`, `manager/type.go`)
33. Address config by variadic path segments (`FieldString("a","b")`), never a dotted string.
    (`config.go`)
34. Emit a typed lint enum with line/column, including `LintUnknown` for unrecognised fields, and
    expose linting over the API so a UI validates before submitting. (`lints.go`,
    `stream/manager/api.go`)
35. Do **not** copy the `(T, error)` accessor ladder — with generics, use
    `Field[T](conf, path…)` plus one deferred `conf.Err()`, or decode into a typed struct
    derived from the same spec. (`config_batch_policy.go` `FieldBatchPolicy` is the anti-pattern)

**Backpressure & batching**

36. Steal the `Batcher` API exactly — `Add(msg) bool`, `UntilNext() (d, ok)`, `Flush(ctx)`,
    `Close(ctx)` — goroutine-free inverted-control batching that drops into any select loop.
    (`config_batch_policy.go`; usage in `input_mysql_stream.go` `readMessages`)
37. Support four orthogonal batch triggers (count, byte size, period, predicate) plus
    flush-time processors, and also model the inverse (splitting), since "a batch policy has the
    capability to create batches, but not to break them down" is a real gap.
38. Let the connector declare its concurrency envelope (`maxInFlight`) and batching policy as
    *data* returned from construction, and have the framework enforce it — but return an
    options struct, not a growing tuple. (`plugins.go` `BatchOutputConstructor`)
39. Unify in-flight accounting into one framework-owned, per-split concept instead of their five
    overlapping mechanisms — and lint the deadlock they merely document ("**WARNING**: Batching
    policies at the output level will stall if this field limits the number of messages below
    the batching threshold"). (`input_max_in_flight.go`)
40. Put a deadline in the ack contract and enforce it centrally with one timer wheel, rather than
    retrofitting a goroutine-plus-timer per batch as they did. (`input_force_timely_nacks.go`)
41. Guarantee ack idempotency in the framework — they reimplement `sync.Once` wrapping in three
    places. (`scanner.go` `ackOnce`, `autoretry` `wrapPendingAck`, `input_force_timely_nacks.go`)

**Errors & retries**

42. Define a small **closed** set of error classes that every call site honours, so a
    connector cannot return a hint the framework ignores — their `ErrBackOff` is respected only
    on `Connect`. (`errors.go`; the closed set already exists in `message.proto` `Error`)
43. Make retry policy `{maxAttempts, backoff, terminal disposition}` where disposition includes
    dead-letter/quarantine — not their binary "replay indefinitely" vs "delete".
    (`config_input.go` `NewAutoRetryNacksToggleField`)
44. Copy `BatchError`'s degrade-gracefully design: a rich per-record error that carries a
    "headline error which will be exposed when upstream components do not support granular batch
    errors". (`errors.go`)
45. Provide `retry` and `fallback` as composable component wrappers so "retry at the sink" and
    "route to a DLQ" are config, not core — but give them a *sanctioned* way to declare
    themselves a pipeline stage instead of an undocumented `Unwrap()` type assertion.
    (`internal/impl/pure/output_retry.go`; the anti-pattern is `XUnwrapper() any`)
46. Make ordering scope explicit (ordered within a split, unordered across) so canal can be both
    parallel and ordered — they can only be one at a time.

**Plugin boundary**

47. Register plugins at `init()` into a **global registry that is really a default instance of a
    value type**, with `Clone`/`With`/`Without` for tests, sandboxing and allow-listing.
    (`environment.go`)
48. Pair every `RegisterX` with a `MustRegisterX`, and ship curated side-effect-import bundles
    (`public/components/{pure,all}`) so one static binary can choose its payload.
    (`plugins.go`)
49. Express connector capabilities as declarative init-time **data**, never as optional Go
    interfaces discovered by type assertion, because data crosses a process boundary and type
    assertions don't — their `ConnectionTestable` needs nine hand-written forwarders and their
    gRPC path had to demote `AutoRetryNacks` to a bool.
    (`package.go` vs `input.proto` `BatchInputInitResponse.auto_replay_nacks`)
50. Design the Go interfaces so a proto like theirs could be generated from them — four RPCs,
    an opaque `batch_id` for the ack, `oneof payload { bytes | Value }`, `StructValue metadata`,
    and a closed `Error` with `not_connected`/`end_of_input`/`backoff`.
    (`connect/proto/redpanda/runtime/v1alpha1/{input,message}.proto`)
51. Let the out-of-process host be an ordinary in-process plugin
    (`var _ service.BatchInput = (*input)(nil)`) and report a dead subprocess as
    `ErrNotConnected` so process supervision reuses the connection state machine.
    (`connect/internal/rpcplugin/input.go`)
52. Define the *declarative* config spec as primary and the Go builder as sugar over it, so the
    manifest and in-process vocabularies can't diverge — theirs did, and out-of-process plugins
    lost `Secret`, `Optional`, `Examples`, nesting and lint rules.
    (`connect/internal/rpcplugin/config.go` `FieldConfig.toSpec`)
53. Include an explicit `(api_version, capabilities)` handshake in plugin init — theirs has
    none, so an older plugin cannot know it is missing a required semantic.
54. Hand connectors a `Resources` capability bundle with `Logger`, `Metrics`, `OtelTracer`,
    `FS`, `Label`, `Path`, `StreamID`, `EngineVersion`, and use
    `AccessX(ctx, name, fn func(X))` callback-scoped borrowing plus `HasX(name)` for
    construction-time validation — borrowing is what makes hot resource replacement safe.
    (`resources.go`)
55. Add `RegisterCustomResource` + `AccessCustomResource(typeName, label, fn)` from day one as
    the way connector families share pooled/configurable infrastructure (and make canal's
    checkpoint store one of these). (`environment.go`)
56. Ship `MockResources(...)` and nil-safe metric handles so connector tests need no harness and
    cannot nil-panic. (`resources.go`, `metrics.go`)

**Observability & deployment**

57. Derive `stream`/`label`/`path` metric labels and log fields from one observability scope
    threaded through construction via `IntoPath`, so nested components self-namespace with zero
    cooperation. (`internal/manager/type.go`)
58. Emit both message counts and batch counts per stage, connection-lifecycle counters
    (`_up`/`_failed`/`_lost`), and a read→ack `input_latency_ns` — the last is free from the ack
    graph and is the single most useful number. (`async_reader.go`, `async_writer.go`)
59. Model health as `(label, path, state, err)` per component aggregated up a tree, with the
    "inactive but nil error means not-yet-attempted or intentionally-closed" nuance — and expose
    the state enum rather than a boolean. (`stream.go` `ConnectionStatus`,
    `component.Connection{Pending,Failing,Active,Closed}`)
60. Serve `/ready` with both a status code and a JSON status tree, plus `/streams` CRUD,
    per-stream `active`/`uptime`/`config`, and per-stream counters with p50/p90/p99 — a local
    single binary can then serve its own dashboard with no Prometheus.
    (`internal/stream/type.go`, `internal/stream/manager/api.go`)
61. Require a description when registering an HTTP endpoint and expose `/endpoints` as a
    self-describing map. (`internal/api/api.go`)
62. Keep the outer pipeline shape fixed (`input → buffer → pipeline → output`) and get topology
    variety from recursive composable components, so config, docs and UI stay tractable.
    (`internal/stream/config.go`)
63. Ship one binary with two modes and a root-config/stream-config split — service-wide
    observability in one file, N pipeline documents managed as data over HTTP. This is canal's
    local-dev↔production story almost for free. (`internal/cli/streams.go`)
64. Offer `ConnectionTest(ctx)` over a whole configured stream without running it — an excellent
    config-UI affordance. (`stream.go`)
65. Add what they refuse to: splits as the assignment unit, ownership leases with fencing tokens
    in the checkpoint store, and a pluggable coordinator that is in-process for the single
    binary and etcd/k8s-lease/DB for the cluster — so both builds run the same code path.
66. Put `context.Context` on every method that can block, from the start — their
    `// TODO V5: Add context here, Travis is onto us.` is unfixable without a major version.
    (`stream_builder.go`)
67. Own the interfaces rather than depending on the framework: roughly a third of the connector
    estate (299 of 839 files under `connect/internal/impl`) carries a Redpanda Community License
    header rather than Apache-2.0, so a dependency would have exposed canal to a relicensing
    event mid-project. (`connect/licenses/`, per-file headers)

---

## Appendix: verification notes

Verified by reading source in the pinned trees above: every quoted signature, doc comment,
config-field description, metric name, proto message, endpoint registration, licence header
and CHANGELOG line.

Verified from in-repo generated docs (`connect/docs/modules/components/pages/**`): the
at-least-once / ordering / checkpoint-limit statements, and the `memory` buffer's
guarantee-weakening text.

**Could not verify against primary source** (stated as such above, and listed in the
structured return):

- Community-reported symptoms referenced in §13.2 (redpanda-data/connect issues/discussions
  #2383, #2404, #2444, #997, #2621) — I have the titles and search-result summaries but did
  not fetch the issue bodies, so treat the *specifics* as indicative rather than quoted. The
  in-tree evidence I cite alongside each (the `nolint:unused nackAllPending`, the
  `EXPERIMENTAL` timely-nacks field, the `TODO: Maybe reintroduce a sleep`, the buffer size
  "estimate" caveat) is primary and stands on its own.
- The claim in §10.4 that in-flight `batch_id`s become stale across a subprocess restart with
  no fencing/generation number: I read `input.go`, `output.go`, `processor.go`, `config.go`
  and the protos in `connect/internal/rpcplugin` and found no generation counter, but I did
  not read `subprocess/` in full, so I cannot assert the absence conclusively.
- Whether any *first-party* connector implements `ConnectionTestable`, and how widely
  `MetaSetImmut`/`public/schema` are adopted across the connector estate — I only traced the
  MySQL and Postgres CDC inputs.
- Bloblang's internals and the exact set of things expressible in a `LintRule` mapping: I read
  the public API surface (`ConfigField.LintRule`, `ConfigSpec.LintRule`, `config_bloblang.go`)
  and the doc-comment examples, not `internal/bloblang`.
- Fork/governance history beyond what is in-tree: the per-file licence split and the licence
  texts are primary; the surrounding narrative (acquisition timing, named community forks) is
  from web search only and is not load-bearing for any recommendation here.
