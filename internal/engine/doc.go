// Package engine assembles and runs pipelines.
//
// It is the only package that imports everything, and nothing imports it. That direction is what makes
// the extensibility claim checkable: a connector cannot reach an engine type, so the engine cannot grow
// a switch on connector identity even if someone wanted one.
//
// # The three phases of commit, which is the most important thing in this package
//
//	1  Source.Read fills a batch through Batch.Add. Records carry stamped provenance.
//	2  Ledger.Admit assigns the position sequence and the group, and blocks when the lane's budget is
//	   full. BLOCKING HERE IS THE ENTIRE SOURCE-SIDE BACKPRESSURE MECHANISM.
//	3  The graph runs: decode, transforms, buffer, batching, encode.
//	4  Sink.Write returns a clean result, or Flusher.Flush covers the request. The engine settles each
//	   record with a terminal disposition.
//	5  A group's refcount reaches zero, the prefix may advance, and the ledger computes the last SAFE
//	   position at or before it.
//	6  PHASE TWO. One durable record through store.StateStore.Set: lane cursors, the schema epoch, the
//	   sink's pending committables and the dedupe additions, atomically, epoch-fenced per lane.
//	7  The write is FLUSHED.
//	8  PHASE THREE. Only now does the commit pump call Source.Commit — and only for lanes this worker
//	   still holds.
//
// Invariant 1: a source's own upstream commit — a replication slot's flush position advancing, which
// frees log for recycling — must NEVER be triggered before canal's own crash-recoverable record of that
// position exists on disk. Do not move phase three above the flush. See
// docs/decisions/0006-three-phase-commit.md.
//
// The two-phase version of this protocol was a confirmed severity-zero defect in a shipped system that
// survived for years, because nothing in its interface distinguished a pruning upstream from a benign
// one.
//
// # Node loops
//
// Each node runs one goroutine with one select. The edges are bounded channels of record batches with
// capacity two — small bounded buffers, chosen now, remove an entire subsystem later; the alternative is
// two major features spent undoing an early deep-buffering default.
//
// A SOURCE node runs exactly two goroutines: the read goroutine (Open, Read, Close) and the control
// goroutine (Commit, Heartbeat, Backlog, Nack, and assignment refreshes). Every other node kind runs one.
package engine
