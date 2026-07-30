# 0016 — No embedded expression language

**Status:** accepted, normative.

## Context

Benthos's Bloblang makes four separate things *data a browser could in principle evaluate*: config lint rules,
batch-flush predicates, templated partition keys, and Segment-style default field mappings from a generic record
onto a sink's own fields. Without an expression language, each of those needs a bespoke closed vocabulary.

This is a single decision with reach into config self-description, flow control and topology, and the
decision-space document explicitly did not cost it.

The case *for* is strong. Segment's action-destinations model — where a sink's field `default` values are path
expressions over the inbound event, so the connector ships a default mapping and the UI renders that mapping as
an editable form — is the most complete answer in the evidence base to "how does a generic UI configure a
*specialised* sink without the core knowing anything about it", which is constraint #1 plus the frontend goal
simultaneously.

## Decision

**No embedded expression language.** Four closed vocabularies instead, each small, each evaluable identically
in Go and in TypeScript.

| Need | Mechanism |
|---|---|
| conditional field visibility and required-ness | `config.Predicate` — eight operators (`equals`, `not_equals`, `in`, `present`, `truthy`, `gt`, `lt`, `matches`) plus `All`/`Any`/`Not`, addressed by path **segments** |
| cross-field config lints | `config.Lint{When Predicate, Code, Path, Severity, Message, Hint}` — the same predicate vocabulary |
| batch flush triggers | `BatchPolicy.FlushOn *config.Predicate` — the same vocabulary again, over the record |
| sink field mappings | `config.Mapping{Target, Source, Required, Default}` where `Source.Kind` is one of six closed values: `payload_field`, `payload`, `meta_field`, `origin_key`, `event_time`, `literal` |

**One vocabulary, four uses.** `config.Predicate` is deliberately reused for config predicates, lints and record
predicates rather than growing a second language for records — that reuse is R9 applied to the thing most likely
to sprout a dialect.

**The escape hatch is a transform, not an expression.** A mapping that the six source kinds cannot express is a
registered `transform` node: real Go, real tests, real types, a version, and a config spec of its own.

**A predicate is validated against the enclosing spec at registration**: every `Predicate.Path` must reference a
declared field, checked at `init`, so a form whose conditional fields never appear is a registration panic rather
than a mystery.

## Alternatives rejected

- **Bloblang or an equivalent embedded language.** Rejected on four grounds. (a) It is a real dependency with a
  parser, a runtime, a type system, error messages, a security surface (a mapping is arbitrary computation over
  the payload) and a documentation burden. (b) It must be evaluable in the **browser** too, or the "predicates are
  data the UI can evaluate" property collapses — so it is a Go implementation *and* a JS implementation kept
  bit-identical, which is exactly the multi-runtime drift R11 exists to prevent. (c) It becomes the thing
  operators debug instead of the pipeline. (d) Sunk-cost gravity: once expressions exist, every new feature
  becomes an expression rather than a type.
- **A tiny expression subset** (field paths plus string concatenation plus coalesce). Rejected as the worst of
  both: it needs a parser and a browser implementation anyway, and it will grow.
- **CEL or another off-the-shelf language.** Rejected: it solves the parser problem and not the browser problem
  or the debuggability problem, and it is a large dependency for four small jobs.
- **Go plugins for mappings.** Rejected: it is not data, so the UI cannot render or edit it, which defeats the
  purpose.
- **Dropping sink field mappings entirely.** Rejected: it is the mechanism by which specialised sink UX arrives
  with no core change, which constraint #1 explicitly requires.

## Consequences

- Positive: no parser, no runtime, no second implementation, no security surface, no debugging surface. Every
  predicate and mapping is a small JSON object the browser evaluates with about fifty lines of TypeScript.
  Specialised sink UX still works.
- **Negative, accepted, and it is the real cost:** roughly one mapping in ten will need something the six source
  kinds cannot express — a concatenation, a conditional default, a unit conversion, a nested lookup. Those become
  a transform node, which is more work than a one-line expression and is not editable in a form.
- **Negative, accepted:** the eight predicate operators will be asked to grow. Each addition is a core change plus
  a frontend change, which is the friction that keeps the set small; growth requires an amendment to this record.
- **Negative, accepted:** `matches` embeds a regular expression, which is an expression language by the back
  door. Bounded deliberately: it is a *predicate* returning a bool, never a transformation producing a value, and
  `PatternHint` exists because a regex is not a message.
