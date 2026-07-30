# 0022 — Serialization is three registered stages attached per sink node

**Status:** accepted, normative.

## Context

R2's originating defect was a stage named `source_canonical_event_serializer` with no definition and no
implementation. The positive form of the question is: does a connector implement encoding, or only transport?

The evidence is unusually one-sided. Kafka Connect's `Converter`, configured per-pipeline as
`key.converter`/`value.converter`/`header.converter`, means **any connector composes with any wire format** — the
same connector's output goes out as schemaless JSON, JSON-with-embedded-schema, or a registry-backed Avro id with
no connector change. Its dossier calls this "the single cleanest idea in Connect's design". Vector splits it
further into `encoding.codec` × `framing.method` × `compression`, and the split is real: newline-JSON,
length-prefixed protobuf and comma-delimited text are two independent choices, not three codecs. Benthos, by
contrast, has each sink marshal for itself, so every sink reimplements JSON and compression and codec bugs
multiply by connector count.

But the reviewed proposals got the *plumbing* wrong in three distinct ways, each fatal or major:

- **One proposal registered encoders, framers and compressors that nothing could reach**: nothing produced the
  reader a framer needed and nothing consumed encoded bytes. Every byte-oriented connector reimplemented framing.
- **Another had `Encoder.Encode` return `[]byte` while `Writer.Write` took a batch**, so an encoded payload had
  literally no path into a sink, and its own reference sink re-encoded and re-framed itself.
- **A third put a single `Encode` codec reference on the pipeline**, so fan-out to two sinks in two wire formats
  was inexpressible and required a sink that re-implemented encode, frame, compress, batch and split.
- **All of them gave codecs no context and no runtime**, so a schema-registry-backed Avro or protobuf codec — the
  central justification for pluggable codecs existing — could not be written.

## Decision

**Sources and sinks implement transport only.** A source produces bytes or structured values; a sink takes an
already-encoded, already-framed, already-compressed `Request.Body` and makes a request.

**Encoder, framer and compressor (and their inverses) are three independently registered component kinds**, each
with a `Spec`, a `CodecCaps` embedding `Caps{APIVersion}`, and a `Descriptor`. Add a codec and every sink gains
it; add a sink and it gains every codec. N × M never multiplies.

**The codec is attached per SINK NODE, as a stage-standard field.** The registry **appends** the
`config.Fields.Codec("codec")` composite to every registered sink's spec unless the sink declares
`SinkCaps.Structured`. The connector author writes nothing; the operator configures it per node; the **engine**
reads it and owns the encode chain.

That one placement decision fixes all three plumbing failures at once:

- the codec stages have a caller (the engine's encode node) and a consumer (`Request.Body`);
- fan-out to two wire formats is two sink nodes with two `codec:` blocks;
- no sink ever sees a codec, and no connector ever names one.

Stage-standard fields by kind:

| Kind | Appended by the registry |
|---|---|
| every kind | `retry`, `when_full` |
| `sink` | `codec` (unless `Structured`), `batching`, `max_in_flight`, `dedupe` |
| `source` | `lane_budget`, `heartbeat_interval` (if `Heartbeats`) |
| `buffer` | `capacity`, `when_full` |

**Every codec gets `Open(ctx, CodecRuntime)` and every hot method takes a context.** `CodecRuntime.Schemas()`
resolves and registers schemas, which is precisely what makes a registry-backed Avro codec writable.

**One frame to many records is in the signature.** `Decoder.Decode(ctx, frame, carrier, dst)` appends through
`dst.Derive(carrier)`, so a JSON array in one frame, a multi-record WAL message or a multiline log entry is
correct — and its settlement accounting is the same `Origin.refs` mechanism as a 1→N transform, needing no
special case and no hand-written fan-out ack counter.

**`Deframer.Split` is literally `bufio.SplitFunc`'s signature**, so every existing Go splitter is a canal deframer
and no author learns a new shape.

**The structured escape hatch is a declared capability, not a runtime fallback.** A sink whose SDK requires
structured input declares `StructuredSink`; `Request.Rows` is populated and `Request.Body` is not, **decided at
Build time**. So there is no per-request branch, no possibility of double-encoding, and the core refuses to
attach an encoder to such a sink rather than silently doing both.

**Codecs participate in submit-time validation.** `CodecCaps.Accepts` versus what the source produces, and
`CarriesMeta`/`CarriesChange`/`CarriesSchema` versus what the pipeline needs, are checked at `Build` — so
configuring a codec that cannot express what flows through it is a diagnostic, not a discovery on record 1.

## Alternatives rejected

- **The connector owns serialisation** (Benthos). Rejected: every sink reimplements JSON and compression, codec
  bugs multiply by connector count, and "the same source to a different wire format" becomes a code change.
- **One codec per pipeline.** Rejected: it makes per-sink wire formats inexpressible, which is a major defect.
- **Codec choice expressed in the type system at construction** (the Debezium-engine style `create(Json.class)`).
  Rejected: it is not data, so the UI cannot render it and it cannot cross a wire.
- **Encoding as a transform node in the graph.** Genuinely tempting — it would need no stage-standard field — and
  rejected because encoding is not optional for a byte sink, so making it a node the operator can forget to add
  turns a required step into a configuration error. A stage-standard field is always present with a default.
- **Codec as a sink config field the sink author declares.** Rejected: it is per-connector boilerplate that must
  be identical everywhere, which is exactly what stage-standard appending removes.
- **A runtime `ErrSkip`-style fallback between structured and byte paths.** Rejected — ADR 0013 item 9: silent
  per-call degradation.
- **A single combined codec doing encode+frame+compress.** Rejected: three orthogonal choices, and combining them
  makes newline-delimited-protobuf a new component instead of a configuration.

## Consequences

- Positive: connectors get smaller and uniform, which is constraint #4; the codec registry is a second instance of
  the registry machinery and so validates it early; schema *encoding* is the codec's choice while schema
  *presence* stays on the record, which is Kafka Connect's cleanest decision; fan-out to two formats is free.
- **Negative, accepted:** three config blocks per sink node instead of one. Mitigated by the composite being one
  `codec:` block with defaults, appended automatically and documented identically everywhere.
- **Negative, accepted:** a codec needing transport-level context — a schema-registry magic byte prefix — must be
  given it, which is why `CodecRuntime` exists and why every codec pays an `Open` it may not need.
- **Negative, accepted:** the registry mutating a component's spec by appending fields is surprising the first
  time. Made explicit: the appended set is documented per kind, an author may pre-declare a field of the same
  name to override it, and the descriptor shows the final spec.
- **Negative, accepted:** a structured sink cannot also accept bytes. Decided at Build, so it is a submit-time
  refusal rather than a runtime surprise.
