# 0015 — The out-of-process seam: same interfaces, a subprocess, not a sandbox

**Status:** accepted, normative.

## Context

Constraint #3 is precise: connectors are in-process Go packages registered at `init` into a single static
binary, **but** the interface must be designed so an out-of-process implementation can later satisfy the
**same** interface without changing the core. Design for that future without building it now.

The evidence is decisive about what makes this possible or impossible.

- Kafka Connect fails the wire-shippability checklist on five counts — `Class<? extends Task> taskClass()`,
  `Map<String,?>` payloads, behavioural `Schema`/`Struct`, `Future<Void>`/`Callback<Void>`, and no `Context` —
  and that is precisely why **no out-of-process Connect exists and cannot**.
- Benthos passes it, which is why its gRPC boundary satisfies the same interfaces — and the reason its ack
  closure survives is that its signature is `(ctx, err) error` and carries no other state, so it becomes an
  opaque `uint64` on the wire. A richer ack would have needed a schema.
- Conduit's entire in-process/out-of-process seam is **one method**, a transport factory. Everything else is
  shared. It also deliberately makes the in-process path *worse* — every request and response is deep-cloned,
  enforced by a type constraint — so the two transports cannot semantically diverge and a builtin connector
  cannot mutate a record the engine still holds.
- One reviewed proposal's best idea was that the remote implementation should **be** an implementation of the
  plugin interface (`engine/remote` satisfying `connector.Reader`) rather than a separate `Transport`
  abstraction. Another proposal was found **fatal** on exactly this point: no arrangement of a gRPC shim could
  satisfy its capability resolution, because the resolution function was unexported and its panic rule made a
  mono-shim illegal.
- Conduit also shows an axis every proposal missed: builtin plugin calls run inside a sandbox with `recover()`
  **and** a `select` on `ctx.Done()`, because a subprocess gives panic containment and hang abandonment for
  free, and an in-process boundary that gives neither is not *behaviourally* the same boundary.

## Decision

**canal v1 ships one binary with in-process connectors. Eleven choices, all made now, make the remote future
mechanical.**

1. **Every plugin method is `(ctx, serialisable) → (serialisable, error)`.** No class handles, no futures, no
   callbacks in a request or response, no behavioural schema objects, no interface-typed request fields, no
   channels.
2. **No closures anywhere on the plugin surface.** The progress primitive is `Source.Commit(ctx, Ack)` — a
   method over a plain struct — so canal never has the problem Benthos's `AckFunc` only barely survives.
3. **Everything durable or boundary-crossing is `record.Blob{Version, Bytes}`.** Cursors, lane specs,
   committables, writer state, transform state. No live mutating context object is ever persisted or shipped.
4. **Every request and response is a struct, even when empty** — the shape protobuf generates. Adding a field
   is source-compatible for every implementation, and that is the growth mechanism that lets the required
   interfaces be frozen.
5. **Capabilities are declared data and the type assertion happens once, on canal's side.** `ResolveSource` and
   `ResolveSink` are **exported**, take `(name, iface, caps)` and return a struct of nilable handles, so a
   remote adapter fills `SourceCaps` from the wire and gets a resolved struct with no assertion crossing the
   boundary. That export is the direct fix for the fatal above, and it is exercised now by a fake in tests.
6. **The runtimes are interfaces, and they are the reverse channel.** `SourceRuntime.Lanes()`, `.State()`,
   `.Metrics()`, `.Note()`, `CodecRuntime.Schemas()`. A subprocess connector needs canal to serve it;
   retrofitting a reverse seam into a one-way boundary is expensive, and declaring it now costs nothing.
7. **`engine/remote` will contain implementations of `connector.Source`, `Sink` and `Buffer` — not a
   `Transport` interface.** There is no `WorkerClient`, no `PluginProxy` and no second plugin vocabulary. The
   engine cannot tell a remote source from a local one. The single seam is a `Dispenser` with `NewSource` and
   `NewSink`.
8. **A dead subprocess is reported as `fault.ErrNotConnected`**, so process supervision reuses the connection
   state machine and needs no new states.
9. **A CI generator asserts wire-shippability from commit one.** It walks the `connector` package and fails if
   any exported method's parameters or returns are not JSON-round-trippable. The property cannot rot before the
   RPC exists.
10. **`error` degrades to a string at the boundary — and the in-process path pays the same cost.** A `Fault`
    that crosses is reconstructed from its nine declared fields and its wrap chain is not preserved. If only
    the remote path lost fidelity the two transports would diverge semantically, and divergence is worse than
    loss.
11. **In-process calls are sandboxed so the two boundaries fail alike.** `engine.sandbox` runs every plugin call
    in a goroutine with `recover()` and a `select` on `ctx.Done()`: a panic becomes `PermanentInternal` naming
    the component; a hang is **abandoned** and the host keeps running.

**The out-of-process future is a subprocess speaking gRPC, not a WASM sandbox.** Stated now because the costs are
not comparable: a sandboxed plugin that needs the network needs an entire capability-granting system — egress
policy, IP guards, secret brokering — on top of the serialisation cost, and Conduit's WASM path forked the plugin
interface entirely and then deferred the component model by ADR. If a sandbox is ever wanted it is a *second*
`Dispenser`, not a redesign.

## Alternatives rejected

- **A separate author-facing layer plus a boundary layer** (Conduit's three-layer split, which its own dossier
  calls the single most valuable thing it has for canal: the author never sees the boundary layer and the engine
  never sees the author layer). The strongest rejected alternative. Rejected because constraint #3 says the
  out-of-process implementation must satisfy **the same** interface, and because a second layer is a second
  vocabulary to keep in sync, a second set of docs, and 24 duplicated files per protocol bump. The
  wire-shippability discipline plus the exported `Resolve*` gets the same interchangeability with one layer. The
  cost is real: canal's author-facing interface is constrained by wire-shippability, so it can never grow an
  ergonomic-but-unshippable convenience.
- **A `Transport` interface the engine speaks to.** Rejected: it is a second plugin vocabulary, and the remote
  implementation satisfying the plugin interface directly is strictly simpler.
- **Building the gRPC binding now.** Rejected: constraint #3 says design for it, not build it. The CI generator
  is what keeps the design honest without the code.
- **No sandbox in-process.** Rejected: two adapters that differ on "a connector panic kills the host" and "a
  wedged connector wedges the pipeline" are not interchangeable no matter how identical their signatures.
- **Zero-copy in-process for records handed to plugins.** Rejected where a semantic depends on it: a builtin
  connector mutating a record the engine still holds would be impossible over gRPC and must therefore be
  impossible in-process.
- **Preserving typed errors in-process only.** Rejected: deliberate symmetry beats deliberate asymmetry here.

## Consequences

- Positive: the remote binding is mechanical and touches no core file; process supervision reuses an existing
  state machine; the reverse channel exists; the property is CI-enforced before the code exists.
- **Negative, accepted:** the author-facing interface is constrained by wire-shippability forever. Every
  ergonomic convenience must also be a struct.
- **Negative, accepted:** a fault's wrap chain is lost even in-process. `Fault.Dev` carries the formatted chain.
- **Negative, accepted:** every plugin call costs one goroutine, and a wedged call leaks one until it returns.
  Counted as `canal_abandoned_plugin_calls`; a non-zero value is alertable.
- **Negative, accepted:** deep-copying at boundaries where a semantic depends on it costs allocations that an
  in-process-only design would not pay.
