// Package registry holds component definitions keyed by kind and name.
//
// The global registry is a DEFAULT INSTANCE OF A VALUE TYPE with Clone, With and Without, so a
// test or a sandbox gets an isolated registry instead of mutating process-global state.
//
// # The whole obligation of adding a connector
//
// Implement the interface, declare capabilities, declare config, register in init. No core file
// changes. No switch statement anywhere gains a case. The core never learns your connector's
// name, and it cannot, because a connector package imports only record, fault, schema, config,
// connector and registry — the core's own types are not reachable from it.
//
// # The one-directional cross-check
//
// Registration PANICS when a DECLARED capability has no corresponding interface, and merely
// RECORDS A WARNING when an interface is implemented without being declared.
//
// That asymmetry is load-bearing. Panicking in both directions means a v2 core that adds an
// optional interface retroactively panics every unchanged v1 connector that happens to satisfy
// it by coincidence. One direction catches the dangerous mistake — declaring a capability you do
// not have — at the author's first `go test`; the other is surfaced as [CapUndeclared] in the
// capability report, visible in the UI, and never fatal.
package registry
