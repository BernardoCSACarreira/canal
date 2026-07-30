# 0013 — Capability = data, behaviour = interface, cross-checked one way

**Status:** accepted, normative.

## Context

This is the fork that constrains every other decision's ability to evolve: how does canal add a capability in
v3 without breaking a connector written for v1?

The evidence is unusually clean.

- **A fat interface with nullable defaults** (Kafka Connect) produces tri-state capabilities as nullable
  enums, and Java `default` methods prevent compilation breakage but not *linkage* breakage — the official
  javadoc for `ConnectorContext.pluginMetrics()` instructs connector authors to write
  `catch (NoSuchMethodError | NoClassDefFoundError e)`.
- **Optional interfaces discovered by type assertion** (Flink's written FLIP-372 rule: *"every new feature
  should be added with a `Supports<FeatureName>` interface"*) is the only mechanism that keeps required
  interfaces tiny and frozen — but a type assertion **cannot cross a process boundary**, and Benthos's own gRPC
  path had to demote `AutoRetryNacks` to a boolean in the init response. Benthos also pays nine hand-written
  forwarders per capability because it asserts at every internal wrapper.
- **Declarative capability data** crosses a wire and is queryable without instantiating anything — but a flag
  with no methods behind it is worthless.
- **Conduit tried both in one repository and only one worked.** `mustEmbedUnimplementedSource()` makes a default
  method safe for an optional *method*; the same `ErrUnimplemented` used for runtime capability *negotiation*
  (call `ReadN`, fall back on the sentinel) caused three filed bugs including a silent correctness failure
  where records went out unencoded. Meanwhile `PartitionAwareSource` — an optional interface, type-asserted once
  at construction — works. Two attempts in one repo; one works.

Two reviewed proposals also made the same **fatal** mistake in the enforcement: registration panicked in *both*
directions, so a v2 core adding an optional interface retroactively panics every unchanged v1 connector that
happens to satisfy it.

## Decision

**Behaviour is an optional exported interface. The *fact* of that behaviour is declared data.**

**1. Required interfaces are tiny and frozen.** `Source` is four methods, `Sink` is three, `Buffer` is seven,
`Transform` is three. No method will ever be added to any of them.

**2. Growth goes through two unlimited channels**, both of which the *core* implements or owns:
- the `*Runtime` **interfaces** — adding a method breaks no connector, because a connector only calls them;
- **request and response structs** (`Opening`, `Request`, `Ack`, `WriteResult`) — adding a field is
  source-compatible for every implementation and generates the same proto shape.

**3. Every capability exists twice**: as an exported optional interface, and as a named field on a `Caps`
struct. Every registered kind has a `Caps` struct and every one embeds `Caps{APIVersion int; Unknown []string}`
— transforms, buffers, codecs, framers and compressors included, not only sources and sinks.

**4. The cross-check is one-directional, and this is the correction:**

| Situation | Result |
|---|---|
| declared, not implemented | **panic at `init`** |
| implemented, not declared | **`CapUndeclared` on the descriptor** — a warning, never fatal |
| declared, unrecognised by this core | **`CapUnknown`** — ignored and reported, never an error |

Declaring what you do not have is the dangerous mistake and the author sees it on their first `go test`.
Panicking in the other direction makes a core upgrade a breaking change for third-party connectors, and
erroring on an unknown capability makes a *newer* connector unusable by an older core — the downgrade path
nobody tests.

**5. The check needs no instance and no reflection.** The single type parameter on `AddSource`/`AddSink` **is
the return type of `New`**, inferred from the func literal, so `var z S; _, ok := any(z).(Discoverer)` works
because method sets belong to types. No probe instance built from invalid config; no hand-maintained bitmask.

**6. The type assertion happens in exactly one place**, `ResolveSource`/`ResolveSink`, producing a struct of
nilable handles that the engine, the API, the frontend and the conformance kit all read as **fields**. One
function instead of nine forwarders per capability, and a shape a remote adapter can fill from wire-declared
data.

**7. Capability identity is a string name, never an iota**, in every persisted, signed or exported artefact.
One proposal persisted tier-grouped iota ids inside operator-signed downgrade records, so inserting a source
capability renumbered every reader and sink id in durable state.

**8. Absence is explained, not blank.** `CapReport{Name, Title, Present, Source, Reason, Unlocks, Iface}` with
`Reason` **required when absent and forbidden when present**, asserted by a core test. This is the only
mechanism in the surveyed field that explains a missing capability instead of rendering nothing, and `Iface`
turns "impossible pipeline" into a connector-authoring task list.

**9. A capability may be declined for a configuration, once, during negotiation.** `fault.ErrDeclined` is legal
only from a method the engine calls inside `Build` or from `Validate`; it removes the capability from the
resolved set *before* admission, with its message becoming the report's `Reason`. There is **no per-call
"skip this fast path" sentinel**: `database/sql`'s `ErrSkip` is silent, per-call and invisible, and a sentinel
valid only on a prose allowlist that grows with every optional interface is the ignored-hint bug in a new
costume.

**10. Never nest an interface inside another interface, and never put a type parameter on a plugin interface.**
FLIP-191 needed a whole new package because a `GlobalCommitter` could not be removed "due to the typed
parameters"; FLIP-372 names inner-interface coupling as what prevented `TwoPhaseCommittingSink`'s evolution.
`StatefulTransform` therefore does **not** embed `Transform`.

## Alternatives rejected

- **Fat interfaces with defaults.** Rejected: Go has no `default`, and the Java version's own docs prescribe
  catching linkage errors.
- **`mustEmbedUnimplementedX()` forced embedding.** The strongest rejected alternative, and it genuinely
  relaxes "never add a method to a required interface" into "never add one that lacks a safe default", with the
  safe default compile-time-guaranteed. Rejected on two costs its own evidence names: the interface becomes
  unimplementable by a bare struct so every mock and test double must embed, and a nine-method interface with
  five optional members has a *documentation* problem the type system does not solve — nothing tells an author
  which four matter. Frozen four-method interfaces plus growable runtimes and request structs get the same
  forward compatibility with neither cost.
- **Runtime negotiation by calling and inspecting the error.** Rejected on Conduit's three filed bugs, one of
  them silent data corruption.
- **Capability flags only, with no interfaces.** Rejected: a flag with no methods is worthless.
- **Interfaces only, with no declared data.** Rejected: cannot cross a wire, cannot be served to a UI without
  instantiating a connector, cannot be checked at submit time.
- **Panicking in both directions.** Rejected as fatal.
- **A capability bitmask.** Rejected: it caps the count, it must be hand-maintained against the method set, and
  it renumbers under insertion.

## Consequences

- Positive: the required surface is four methods and three; every other decision in the architecture gains a
  safe growth path; the UI gets capabilities without instantiating anything; a future RPC binding needs no new
  concept; absence is explained.
- **Negative, accepted:** a connector author declares capabilities that the compiler could in principle infer.
  One-directional panicking makes over-declaration impossible to ship, and the declaration is what crosses the
  wire.
- **Negative, accepted:** `CapUndeclared` means a connector can implement something and not get it, silently
  from the code's point of view. Mitigated by it being visible in the descriptor and in the UI's capability
  report.
- **Negative, accepted:** one careful `Resolve*` function per role plus one registration cross-check per kind is
  real code to maintain, and a bijection test between `Caps` fields and `Resolved*` fields must be kept green.
