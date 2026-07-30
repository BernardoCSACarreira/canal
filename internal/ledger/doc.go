// Package ledger owns canal's acknowledgement graph.
//
// Two surveyed frameworks leave this to connectors and both pay the same two prices: the framework
// cannot report progress, and every source reimplements a non-trivial algorithm out of tree. One of
// them keeps its reusable version OUTSIDE both of its repositories, so each connector re-wires it with
// its own key, its own serialisation and its own bugs.
//
// Here it is core, generic over the payload, and NO CONNECTOR EVER SEES IT. That is why this package
// lives under internal: a connector importing it could learn about progress, and a component that can
// learn about progress is a component that can get progress wrong.
//
// # The hard question this package answers
//
// How do you get restart-safety and gap-free resume for a source whose progress is a single monotonic
// cursor, when acknowledgements complete out of order — and how does that survive a cluster?
//
//  1. The CORE assigns sequence, not the connector. Order is never imputed to a connector's opaque
//     bytes.
//  2. Out-of-order resolution, in-order commit. A gap is structurally unrepresentable: to resolve
//     position N the tracker must have observed every position below N resolve.
//  3. record.Position.Safe handles the sub-batch hazard. The commit rule is the last safe position at
//     or before the resolved prefix, and it is a CORE invariant rather than a per-connector
//     convention.
//  4. The replay window is COMPUTED and exported honestly, alongside — never instead of — the
//     configured budget.
//  5. Head-of-line blocking cannot become a livelock, because [Tracker.Abandon] advances the prefix
//     and every retry policy has a terminal disposition.
//  6. A lane with no cursor uses the other resolver: each delivery settles individually.
//  7. The cluster case is FENCED, not trusted: a revoked lane's acknowledgement is discarded rather
//     than delivered.
package ledger
