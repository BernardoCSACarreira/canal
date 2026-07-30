# 0030 — Throttling is flow control, and flow control does not spend a retry attempt

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

ADR 0014 fixed the fault class set at thirteen members and defended the closure hard: a closed set is a
legitimate bounded metric label, it makes "a hint the framework ignores" impossible, and it can be rendered
by a UI with no connector-specific code. ADR 0014 was also explicit that `Class` names a **fact**, never a
behaviour, and that retryability is computed by the engine from `(class, capabilities, policy)`.

The no-cursor REST hostile case found a fact with no member and a computation with no rule.

**The fact.** A 429 with `Retry-After: 30` is not `TransientUpstream`. Both mean "the effect did not land",
but one means "the remote is broken" and the other means "the remote is working and asked you to wait". The
only available class was `TransientUpstream`.

**The computation.** `fault.RetryPolicy` has `MaxAttempts` (default 4) and `Terminal` (default
`TerminalStop`), **and deliberately no unbounded option** — ADR 0014 and `retry.go` both argue that a
framework whose default is unbounded livelocks on a poison record. Nothing anywhere stated whether a fault
returned from `Read` consumes an attempt. So a source politely honouring a 429 four times over 90 seconds
reached `TerminalStop` and **stopped a healthy pipeline because its upstream was working correctly**, and no
configuration made honouring a rate limit safe.

The connector's report is precise about the trap: `Fault.RetryAfter` exists and is documented as "honoured
verbatim by the engine's backoff", so the *wait* was expressible. The *not counting* was not.

## Decision

**One new class and one new method.**

```go
// fault
Throttled                        // the remote is rate-limiting and told us to come back
func (c Class) Counted() bool     // does a fault of this class SPEND a RetryPolicy attempt?
func Throttle(op Op, after time.Duration, err error) *Fault
```

`Class.Counted()` returns false for exactly three classes, and all three are the remote or the component
saying "not now" rather than anything being wrong:

| Class | Why uncounted |
|---|---|
| `Throttled` | the remote is working and asked us to wait |
| `NotConnected` | control flow: the component is asking to be reopened |
| `EndOfInput` | not a failure at all |

Everything else is counted, so a poison record still reaches a terminal disposition within `MaxAttempts` and
"retry forever" stays inexpressible.

**An uncounted wait is bounded by `RetryPolicy.Backoff.MaxElapsed` and by nothing else.** That is the honest
bound: a rate limit is a wall-clock problem, not an attempt-count problem. Sustained throttling surfaces as
`ReasonSustainedBackoff` on a *running* pipeline rather than as a dead one.

**`Source.Read`'s contract now states the attempt rule explicitly**, because the previous state of affairs
was not a missing member so much as a missing sentence.

## Why a method on `Class`, not a field on `RetryPolicy`

The obvious alternative was `RetryPolicy.CountThrottles bool`, defaulting false. It is worse for the reason
ADR 0014 gives about class-versus-behaviour, applied in the other direction: whether honouring a rate limit
consumes a poison-record allowance is not an operator preference. There is no configuration of a correct
system in which it should. Making it configurable creates a knob whose only settings are "correct" and
"stops healthy pipelines", and someone will set the second one.

Putting it next to `Class.Terminal()` also puts it where a connector author looks. `Terminal()` answers "can
retrying help"; `Counted()` answers "does trying cost me". Same kind of computed fact, same place.

## Why not reuse `TransientUpstream` with a non-zero `RetryAfter` as the signal

That was proposed and is the shape of the ignored-hint bug ADR 0014 exists to prevent. It makes the class set
lie: two faults with the same class would have different engine behaviour depending on a field, so the class
is no longer the fact the metric label claims it is, and a connector wrapping a rate-limit fault without
copying `RetryAfter` silently converts flow control into failure. `RetryAfterOf` walks the chain precisely
because wrapping loses fields; the *class* must carry the meaning.

## Consequences

- One major breakage closes; the fault set goes from thirteen members to fourteen and stays closed.
- `Class.Blames()` puts `Throttled` under `BlameUpstream`, and `Class.Terminal()` returns false for it.
- Metric label cardinality grows by one value. That is the entire cost.
- The conformance kit gains an obligation: a source declaring it honours rate limits must be shown to
  survive N consecutive `Throttled` returns with `N > MaxAttempts`.
