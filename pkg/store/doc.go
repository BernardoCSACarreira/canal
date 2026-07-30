// Package store is the deployment seam. FOUR INTERFACES. If a fifth appears, the abstraction is
// wrong.
//
// Every interface here is bytes-in/bytes-out or deals in leaf types, and NONE sees a live domain
// object — which is precisely what makes the standalone-to-coordinated swap free. A byte-buffer-in,
// byte-buffer-out offset store is the single best idea in one surveyed design, and this is that idea
// generalised.
//
// # What differs between a laptop and a cluster
//
//	ConfigStore   a bbolt file, or a YAML file projected read-only | Postgres
//	StateStore    a bbolt file: one Set is one transaction        | Postgres with per-key CAS and an epoch column
//	Coordinator   always leader, every lane local, epoch 1        | advisory-lock election, a leases table, claim by SKIP LOCKED
//	StatusStore   in-process                                      | worker-status rows with a TTL
//
// The connector-facing API is byte-identical in both modes. Anything that differs is a defect, and a
// test builds the same spec against both assemblies and asserts identical negotiation output.
//
// # The property this seam buys
//
// THE LEADER ONLY PLANS. It writes assignment rows. It does not route data, proxy status, hold a
// checkpoint, or own anything the data path reads. Therefore the data plane keeps running and keeps
// checkpointing with the entire control plane down, because a worker holding a valid lease needs
// nothing from anyone until it expires. This is the single most important deployment property in the
// design and it is worth sacrificing elegance for.
//
// Kafka as the coordination store is EXPLICITLY REJECTED: a compacted topic cannot provide atomic
// multi-key writes, and the surveyed implementation that tried needs seven record types plus a commit
// marker to fake set-atomicity and documents an unrecoverable state in its own javadoc.
package store
