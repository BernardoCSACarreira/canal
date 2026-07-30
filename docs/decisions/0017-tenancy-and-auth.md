# 0017 — Tenancy and authentication at every ingress

**Status:** accepted, normative. Closes `design-rules.md` open decision 6.

## Context

R13 exists in part because the abandoned attempt's control plane had **no authentication, no authorization and
no tenant scoping**, and realised "tenancy" as "single OS user" while storing operator bindings in an in-memory
dict and returning UUIDs with `createdAt`/`updatedAt` timestamps. R13's rule ends: *tenancy is decided before
the first multi-tenant field, not after.*

The prior art contributes one sharp warning. Conduit had no `Sensitive` flag on its config-parameter type, so
redaction became a per-call-site discipline: a redact-everything pass at the log site, a second pass at the API
boundary, and then a filed bug where one endpoint returned connector settings **unredacted** because it was
missed. Discipline failed; a flag does not.

## Decision

**1. `record.TenantID` exists from commit one and is on every durable key, every API path, every metric label
and every log line.** In a single-tenant deployment it is the constant `"default"`. Enabling multi-tenancy is a
configuration change, never a migration, because the column already exists.

**2. Durable keys are structured, never concatenated.**

```go
type Key struct {
    Tenant record.TenantID
    Space  Space // lane | checkpoint | schema | dedupe | connector
    Parts  []string
}
```

A pipeline id containing a separator therefore cannot escape its tenant.

**3. Every ingress authenticates. Three modes, chosen in the root config, and no fourth:**

| Mode | Use | Constraint |
|---|---|---|
| `none` | local development | **the binary refuses to start with `auth: none` bound to anything but loopback** |
| `token` | machine clients, simple deployments | static bearer tokens, each mapped to a tenant and a role, hashed at rest |
| `oidc` | enterprise | JWT with issuer and audience validation, JWKS refresh, tenant read from a configured claim |

**4. Authorization is three roles plus one non-grantable:** `viewer` (read status and redacted config),
`operator` (create, update, pause, resume, edit offsets), `admin` (manage tenants, tokens, stores), and
`system` — reserved for worker-to-worker status reporting and **not grantable to a human**.

**5. The tenant is resolved from the credential, never from the path.** A path segment that disagrees with the
credential is a `403`, not a redirect and not a silent scope switch.

**6. Secrets are handled in exactly one place.** `config.Field.Secret` drives redaction; the API returns
`Config.Redacted()` and nothing else; `Config.Secret(path...)` is a distinct accessor so every read is
greppable and countable; `Meta`'s secrets compartment is never serialised, logged, exported to the read model
or shown to a codec. A fixture test round-trips a spec containing a secret through **every** endpoint and
asserts the value never appears — which is the mechanism Conduit's missed endpoint lacked.

**7. Every mutating call is audited** with actor, tenant, generation, and a diff of the *redacted* spec. Offset
edits are audited and bump `Generation`.

**8. Metric labels carry `tenant` but never anything per-tenant-unbounded.** Per-tenant detail is served from the
read model.

## Alternatives rejected

- **Deferring tenancy to "when we need it".** Rejected: it is precisely what R13 forbids, and retrofitting a
  tenant column into durable keys, metric labels and API paths is a migration of everything.
- **Tenant as a prefix on the pipeline id.** Rejected: a separator in an id becomes a privilege-escalation bug,
  and it is two representations of one concept (R9).
- **Row-level security in Postgres as the enforcement.** Rejected as the *primary* mechanism: it does not exist in
  bbolt, so the two deployment modes would enforce tenancy differently — the one thing that must never differ. It
  may be added as defence in depth.
- **More than three roles, or per-resource ACLs.** Rejected for v1: three roles cover viewer, operator and admin,
  and an ACL system is a product. A fourth role requires an amendment to this record.
- **Redaction as a middleware pass.** Rejected on Conduit's filed bug: a pass can be missed, a flag on the field
  cannot.
- **`auth: none` bindable to any address behind a "you were warned" log line.** Rejected: a data-movement tool's
  API can pause pipelines and edit offsets. The binary refuses.
- **A pluggable authorization backend.** Rejected for v1: it is a second extension surface with no current
  consumer.

## Consequences

- Positive: tenancy costs nothing to enable later; secrets have one enforcement point with a test that would have
  caught the known real-world bug; the two deployment modes enforce identical rules.
- **Negative, accepted:** every durable key and every API path is more verbose, and single-tenant users carry a
  constant `"default"` they did not ask for. That constant is the entire cost of never migrating.
- **Negative, accepted:** `auth: none` refusing a non-loopback bind will annoy someone in a trusted network. The
  workaround is `token` with one token, which is two lines of config.
- **Negative, accepted:** OIDC brings a JWKS cache, clock-skew tolerance on token validation, and a refresh
  failure mode. Contained in one package and behind a mode flag.
- **Negative, accepted:** three roles will be too coarse for someone. An amendment, not a plugin point.
