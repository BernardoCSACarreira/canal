# 0014 — The closed fault class set, and `Indeterminate`

**Status:** accepted, normative.

## Context

The failure-mode taxonomy carried forward from the abandoned attempt is better than most shipping frameworks
have: transient-upstream, transient-internal, permanent-upstream, permanent-mapping, permanent-contract,
duplicate-idempotent-success, clock-skew, each with a prescribed behaviour and a distinct operator signal. It
was never turned into real Go error classification.

Three failure modes in the prior art constrain how to do that.

- **A hint the framework ignores.** Benthos's `ErrBackOff` wrapper is honoured *only* on `Connect`. A connector
  must not be able to return a classification the framework silently discards.
- **Unbounded retry as the default.** Benthos's `AutoRetryNacks` retries forever, so a permanently poisonous
  record retries forever and the community-reported symptom is a pipeline making no progress.
- **A missing class.** Every reviewed proposal was found to have no representable class for *a write that timed
  out after the request was sent*. The retry-safety obligation forbids calling that `TransientUpstream`, and
  `PermanentUpstream` terminates a healthy pipeline — so, as one reviewer put it, every sink will violate the
  normative rule. In two proposals the enum also **fused the fact with the behaviour** (`Terminates()`,
  `Retryable()` and blame all derived from one member), which is why there was no room for a member meaning
  "I do not know".

## Decision

**1. `Class` names a FACT, not a behaviour.** Retryability, termination and dead-lettering are computed by the
engine from `(class, component capabilities, configured policy)`. Fusing fact and behaviour is what left no
room for ambiguity.

**2. Thirteen closed members**, each a stable `snake_case` token that is simultaneously the wire form, the
metric label value and the i18n key:

`Unclassified` (always a defect, counted, asserted zero) · `TransientUpstream` · `TransientInternal` ·
**`Indeterminate`** · `PermanentUpstream` · `PermanentInternal` · `PermanentMapping` · `PermanentContract` ·
`DuplicateIdempotent` · `ClockSkew` · **`Fenced`** · `NotConnected` · `EndOfInput`.

**3. `Indeterminate` is the new member and it is load-bearing.** It means "the operation may have been applied
and I cannot tell". The engine's response is computed, never guessed:

```
sink declares Idempotent          -> retry (a duplicate is absorbed)
otherwise RetryPolicy.OnIndeterminate -> Retry | DeadLetter | Stall     (default Stall)
```

`Stall` means settle nothing, block the lane, raise `Condition{Degraded, True, reason:
indeterminate_write}` naming the record. **Failing loud on an ambiguous write is the correct default for a
data-movement tool**, and making it a *policy* means the operator chooses the trade rather than the framework
guessing.

**4. `Fenced` is the second new member.** A stale epoch on a durable write revokes the **lane**, not the
pipeline. Classifying it as `PermanentContract` made a fenced worker's stale CAS on one lane terminate every
other lane it validly held — a major defect in the rooted proposal.

**5. `Blame` is a separate three-value projection** (`Config` / `Upstream` / `Canal`), because "whose problem is
this" is the question an operator UI needs answered and it is not the same question as "should we retry".

**6. The retry-safety obligation is normative prose plus a conformance case:**

> A connector may return `TransientUpstream` **only** when it knows the effect did not land. If the remote may
> have applied the write, the class is `Indeterminate`, `PermanentUpstream` or `DuplicateIdempotent`.

This is `driver.ErrBadConn`'s rule generalised, and `Indeterminate` exists so obeying it does not require
lying. The conformance kit injects a post-send timeout and asserts the class.

**7. `RetryPolicy.Terminal` has no valid zero value**, so **unbounded retry is inexpressible** rather than
merely discouraged. `Validate` rejects `TerminalInvalid`, and `Build` refuses `TerminalDeadLetter` with no
failed-edge route.

**8. Every classification is honoured at every call site.** `Fault.RetryAfter` is honoured verbatim wherever it
appears, not only on connect. A CI check asserts every engine call site routes through `fault.ClassOf`.

**9. `ClassOf` returns the OUTERMOST declared class.** A wrapper that deliberately re-classifies — the engine's
own "this transient error has now failed four times" — must win. Returning the innermost class makes deliberate
re-classification by wrapping impossible.

**10. `(*Fault).Is` exists**, so `errors.Is(err, fault.ErrNotConnected)` works for a connector-constructed fault.
Without it, a control-flow bug looks like a data bug.

**11. Both message audiences are separate fields.** `User` names the count, the component, an example, the fix
and the consequence; `Dev` carries the upstream string and the stack. One string cannot serve both.

**12. `Op` is a closed nineteen-member set** and it is a metric label. Classifying per operation is Kafka
Connect's one good error idea.

## Alternatives rejected

- **Two-valued retriable/not** (Kafka Connect's `RetriableException`). Rejected: no per-class policy, no
  circuit breaker, no backoff-then-park, and no way to express indeterminacy.
- **Sentinel errors doing control flow with a hint the framework may ignore.** Rejected: the `ErrBackOff` bug.
- **An open class set, or a connector-supplied string code.** Rejected: it is a metric label and an i18n key, so
  it must be closed and bounded. Never label with an error message or an upstream error code.
- **Deriving retryability from the class alone.** Rejected: it is why `Indeterminate` had nowhere to live.
- **Letting a connector override retryability.** Rejected: "the connector said retry but the framework
  disagreed" is the bug the closed set exists to prevent.
- **`Indeterminate` defaulting to retry.** Rejected: it silently converts an unknown into a duplicate for a
  non-idempotent destination. The operator must choose.
- **A per-call fallback sentinel** (`ErrSkip`). Rejected — see ADR 0013 item 9.

## Consequences

- Positive: a sink can be honest about a timed-out write for the first time in the surveyed field; a fenced lane
  does not kill a worker; unbounded retry cannot be configured; the class set is a legitimate bounded label; the
  UI can render blame and an actionable message with no connector-specific code.
- **Negative, accepted:** `Stall` on an indeterminate write against a non-idempotent sink halts the lane and
  requires an operator decision. That is the honest response, and the alternative is silent loss or silent
  duplication.
- **Negative, accepted:** thirteen classes is more than most frameworks have, and a connector author must pick
  the right one. Mitigated by five named constructors covering the common cases and by a conformance case that
  fails an unclassified error.
- **Negative, accepted:** `Fault` carries nine fields plus two strings, so it is not a cheap value. It is only
  constructed on failure paths.
