# canal

A connector and data-movement tool in Go.

## Goals

- **Many sources, many sinks.** Adding either means implementing an interface and registering it — no core
  changes, no per-connector branches anywhere in the engine.
- **Several kinds of pipeline.** Streaming change capture, batch and full-scan, and the hybrid
  snapshot-then-stream case where an initial scan hands off to an incremental stream without gaps or
  duplicates.
- **Serialization** as a pluggable concern, separate from connector logic.
- **Checkpointing** that survives `kill -9` and restarts exactly where it should.
- **Initial / state scan** for sources that support it, resumable mid-scan.
- **A frontend** for live metrics and configuration, driven by what connectors declare about themselves rather
  than by connector-specific UI code.
- **Two deployment shapes from one binary:** a standalone dev mode with no external dependencies, and a
  horizontally scaled enterprise mode.

The core is deliberately source- and sink-agnostic. No specific system's shape is allowed to leak into it.

## Status

Early. The current work is the architecture and the core interface set — extensibility is the property being
designed for, so the interfaces come before the connectors.

## Layout

```
docs/design-rules.md   normative constraints on every architectural decision
docs/decisions/        ADRs
```

## Reading order

Start with [docs/design-rules.md](docs/design-rules.md). It is short, it is normative, and every rule in it
was paid for.
