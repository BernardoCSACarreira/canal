# 0004 — The per-record partial-failure response shape

**Status:** accepted, normative. Closes `design-rules.md` open decision 4 and satisfies R7.

## Context

R7 exists because the abandoned attempt required adapters to retry "only the failed subset" of a partially
accepted batch and to checkpoint only when the response "indicates no unresolved failures", while the
response schema was `{accepted: int, duplicateIds: string[]}`. **No per-event error array existed**, and no
partial-failure code path existed in any implementation. Both rules were unimplementable as written.

The prior art shows the cost of each wrong shape:

- **Positional identity.** Benthos's `BatchError` correlates by index; its author marked
  `(*BatchError).WalkMessages` *"Deprecated: This method is harmful"*, and `Indexer`/`SortGroup` plus ~60
  lines of strict-mode gymnastics exist only because records have no stable identity. When correlation fails
  the fallback retries the whole batch.
- **A prefix count.** Conduit's `Write(ctx, records) (n int, err error)` can only express "the first n
  worked"; every nacked record — including the unattempted ones — carries the same error string, and the
  boundary type degrades `error` to `string`, so the one real error loses its type and wrap chain.
- **An ambiguous default.** One reviewed proposal specified "calling `Fail` at least once switches the batch
  to everything not named here succeeded", and its reviewer found that *fatal*: a sink reporting only the
  rows the server rejected commits the source's position past rows it never wrote. The same proposal also
  offered `Succeeded(id)` alongside `Fail(id)`, leaving records named by neither undefined.

## Decision

```go
func (s *Sink) Write(ctx context.Context, req *Request) (WriteResult, error)

type WriteResult struct {
    Failed     []fault.RecordFault // keyed on record.RecordID
    Duplicates []record.RecordID
    Written    int64
    Bytes      int64
    DestToken  string
}

type RecordFault struct {
    Record record.RecordID
    Class  Class
    Op     Op
    User   string // operator-facing
    Dev    string // developer-facing
}
```

**All four quadrants are specified, and the default is safe in every one:**

| Return | Meaning |
|---|---|
| `(res, nil)`, `Failed` empty | every record in the request is **durable** |
| `(res, nil)`, `Failed` non-empty | every record **not** named in `Failed` is durable; the named ones are not |
| `(res, err)`, `Failed` non-empty | as above; `err` is the headline for logs and status |
| `(res, err)`, `Failed` empty | **nothing** is claimed durable; the whole request is retried |

Plus five rules:

1. **A sink must not partially apply and report total success.** If it cannot know what landed it returns an
   error with `Failed` empty and class `Indeterminate`.
2. **Reconciliation is mandatory, not advisory.** `Written + len(Failed) == req.Count`. The core checks it and
   raises `fault.PermanentContract` on a mismatch, because a sink that miscounts is a sink whose durability
   claim cannot be trusted. This is what closes the fatal defect above from the other side: even a sink that
   reports only server-rejected rows is caught.
3. **Duplicates count as success**, and are reported separately so the rate is visible. That is the direct fix
   for R5's "permanently unresubmittable" bug: `duplicate` must mean "already durably stored".
4. **The two message audiences are separate fields.** One string cannot serve an operator's UI and a
   developer's log.
5. **`Flusher.Flush` returns the same `WriteResult` shape.** A partial flush must be able to name what did not
   make it; reporting an integer there — as one proposal did — makes the durable set uncomputable and the
   prefix unresolvable.

**The reason this works at all is `record.RecordID`:** framework-assigned, stable through transforms,
rebatching and fan-out, so there is no positional correspondence to lose and no correlation machinery to
write.

## Alternatives rejected

- **Positional identity** — deprecated by its own author, and the fallback amplifies duplicates.
- **`(n int, err error)`** — prefix-only, one error string for every record, and unattributable.
- **`Fail`-switches-the-default** — fatal as shown.
- **`Fail` plus `Succeeded`** — records named by neither are undefined, and the constructor still demanded a
  headline that applied to nobody.
- **A per-record error slice parallel to the input** — a second positional scheme with extra allocation.
- **A mutable per-item callback** (Flink's `CommitRequest` with six mutator methods) — not wire-safe, not
  table-testable, not metric-labelable, and one of its six outcomes is documented as silently discarding the
  item.
- **An out-parameter `*WriteResult`** so the happy path allocates nothing. Genuinely tempting, and rejected
  only because a returned value cannot be accidentally retained or mutated after `Write` returns; the
  allocation is one slice header on a path that already did network I/O.

## Consequences

- Positive: R7 is satisfied in the type system on day one, including partial acceptance, keyed by identity
  rather than by position; the safe default holds in all four quadrants; a lying sink is caught by arithmetic.
- **Negative, accepted:** a sink author must set `Written`. It is one field and the `AllWritten(n)` helper
  covers the happy path, and the alternative is that no cross-check is possible.
- **Negative, accepted:** the mandatory reconciliation makes a previously-tolerable sloppiness a hard failure,
  which will surface in third-party sinks as `PermanentContract`. That is the intent, and the diagnostic names
  the arithmetic.
- **Negative, accepted:** `fault.RecordFault` carries two strings per failed record, so a request that fails
  entirely with per-record detail allocates proportionally. Bounded by the request size and only on the
  failure path.
