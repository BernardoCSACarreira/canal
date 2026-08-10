# 0033 — The coordinated store is a nested module; the core's zero dependencies stay mechanical

**Status:** proposed. Merging this document to `main` is its acceptance, after which it is
normative. Drafted with the alternatives argued so the acceptance is a review, not a blank page.

## Context

Two accepted claims collide the moment M1's first line of code is written.
[ADR 0021](0021-store-seam.md) chose Postgres as the coordinated deployment's store and accepted
the cost in as many words: *"Postgres is a real dependency for coordinated mode. Judged the
smallest possible one."* The README's third sentence advertises **no third-party dependencies**,
and CI enforces it mechanically — the `zero dependencies` job fails if `go list -m all` returns
anything beyond the module itself. A Postgres driver is a third-party module; `database/sql` ships
no driver. Both claims are load-bearing — 0021's because hand-rolling a coordination layer over
anything weaker than a transactional database is how the surveyed field got its scars, the README's
because `canal run` working on a laptop with nothing installed is R3's adoption lever — and no
document says how they coexist. Until one does, every durable-Coordinator branch starts by quietly
breaking one of them.

Two facts shape the answer. First, the CI guard's scope is the ROOT MODULE: `go list -m all` reads
the module graph of the `go.mod` it runs under, so a nested module with its own `go.mod` is
invisible to it — the guard keeps meaning exactly what it says about the core without edits.
Second, Go's `internal` visibility rule is **path-prefix based, not module based**: a module whose
path sits under `github.com/BernardoCSACarreira/canal/` may import
`canal/internal/engine` — which is what lets the coordinated shape be, as §3's composition-root
comment has always put it, *"a different main with different stores and the same engine"*, without
exporting the engine to the world.

## Decision

1. **The coordinated store is a nested Go module** at `store/postgres` — the path §30 has named
   since it was written — with module path `github.com/BernardoCSACarreira/canal/store/postgres`
   and its own `go.mod`. It implements the four `pkg/store` interfaces plus `store.Coordinator`
   over one dependency: the `pgx/v5` driver, chosen because 0021's primitive list needs
   `LISTEN`/`NOTIFY` and advisory locks, and pgx is maintained, cgo-free and pure Go.

2. **The dependency direction is one way, and the existing guard is the enforcement.** The nested
   module requires the root; the root never requires the nested module — it cannot, without adding
   it to the root `go.mod`, at which point the `zero dependencies` job fails the build. No new
   check is needed; the guard that protects the claim protects the boundary.

3. **The coordinated composition root lives in the nested module**, as `store/postgres/cmd/...`.
   Whatever logic that main shares with `cmd/canal` — flag shapes, the serve loop, spec loading —
   lives under the root's `internal/`, importable by both mains through the path-prefix rule,
   because `cmd` packages are not importable and duplicating a composition root is how two binaries
   drift. `cmd/canal` itself stays zero-dependency and standalone-only.

4. **The nested module is held to the same contracts, by the same kits.** `pkg/storetest` runs
   against the Postgres store the way it runs against `wal` and `memstore` — that suite is the
   reason a second real store can arrive without the contract being proved wrong twice — and the
   engine's revocation and lease tests gain a real-store variant where the in-memory Coordinator
   is today's only subject.

5. **CI grows one job and changes none.** The new job runs in the nested module's directory against
   a Postgres service container: build, vet, `storetest`, and the coordinated tests. The `zero
   dependencies` job, the cross-compile matrix and every root `./...` invocation are untouched —
   a nested module is invisible to all of them, which is the point.

6. **The schema is versioned from its first migration**, in 0032's shape: a version marker the
   store reads at open, a one-step window, migrate forward mechanically, refuse loudly outside the
   window naming the way forward. The DDL and its migration mechanism are the nested module's first
   design duty (completeness audit G11), not an afterthought of its first feature.

7. **The README's claim gets one precise sentence** in place of the absolute — the core module has
   zero third-party dependencies, enforced in CI; coordinated mode is a separate module that adds
   exactly one, the Postgres driver — **in the same change that first creates the module**, not
   before: today's absolute claim is still literally true, and rewording it ahead of the module
   would be documenting machinery that does not exist. An advertised property that survives by
   wording rather than enforcement is R8's failure mode, which is why rule 2 makes the wording
   mechanical.

## Alternatives rejected

- **One module, with the claim asterisked.** Add pgx to the root `go.mod` and soften the README to
  "minimal dependencies". Rejected: it kills the CI guard (the job cannot distinguish the accepted
  dependency from the next accidental one), makes every `go build ./...` on the standalone path
  download a driver it will never call, and turns the headline property into fine print. The
  standalone user is the adoption lever and pays the cost first.

- **Build tags inside one module.** Tempting and wrong for a mechanical reason: `go.mod` is
  per-module, not per-tag. The requirement lands in the root module graph whether or not the tagged
  files compile, so `go list -m all` fails the guard anyway. Tags select code; they do not remove
  dependencies.

- **Hand-rolling the Postgres wire protocol.** This repository hand-rolled a WAL and can point at
  the byte-offset sweep that keeps it honest. pgwire is a different animal: SCRAM-SHA-256, TLS
  negotiation, auth against arbitrary server configurations — a security-sensitive surface owned
  by people who do nothing else. 0021 already judged the driver "the smallest possible" dependency;
  re-litigating that here would be deciding the same fork twice.

- **A separate repository.** The cleanest-looking boundary and the highest-friction one: cross-repo
  versioning for a pre-1.0 single-author project, PRs that cannot atomically change the interface
  and its implementation, and a conformance suite that runs against a moving target. The interface
  is already the seam; the repository does not need to be.

## Consequences

- M1's store work has a home and starts as ordinary work: the module skeleton, the DDL with its
  versioned migration (rule 6), `StateStore` first — `storetest` makes that arrival checkable —
  then `Coordinator`, then the composition root.
- Development ergonomics: a `go.work` at the repository root ties the two modules together locally;
  the nested `go.mod` carries a `replace` until the root module is tagged. Neither file affects
  what CI enforces.
- The root's architecture guards are unaffected by construction: `internal/arch` walks the root
  packages, and the doc-link test already walks every markdown file wherever it lives.
- The standalone binary's story does not change by a byte: `cmd/canal`, zero dependencies, wal
  underneath, laptop with nothing installed.
- When the out-of-process seam (`engine/remote`, ADR 0015) eventually wants its own dependencies,
  this decision is its precedent: the core stays clean by mechanism, and each deliberate dependency
  lives in a module that declares it.
