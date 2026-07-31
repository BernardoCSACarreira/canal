package store

import (
	"context"
	"fmt"
	"iter"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// StateStore is the durability substrate. BYTES IN, BYTES OUT.
//
// It backs lane cursors and specs, checkpoints, the schema table, the dedupe set, writer state and
// connector.StateHandle. One interface, reserved key prefixes, one atomicity guarantee — because
// separate stores cannot be written atomically together, and atomicity is what makes "the schema epoch
// cannot diverge from the positions it decodes" true.
type StateStore interface {
	// Get reads several keys. A key that is absent is simply missing from the result map, which is
	// distinguishable from a key whose value is empty.
	Get(ctx context.Context, keys []Key) (map[string]Versioned, error)

	// Range iterates every key under a prefix, in key order.
	Range(ctx context.Context, prefix Key) (iter.Seq2[Key, Versioned], error)

	// Set MUST be atomic across the whole batch, with per-key compare-and-set AND per-key epoch
	// fencing: one SQL transaction, one bbolt transaction, one etcd transaction.
	//
	// It MUST NOT return before the bytes are durable. A Set that returns early breaks the three-phase
	// commit protocol at phase two, and no amount of downstream care recovers from it.
	//
	// A stale epoch is fault.ErrFenced naming the lane, not a pipeline-level failure: the worker's
	// other validly-held lanes keep running.
	Set(ctx context.Context, w Batch) error

	Delete(ctx context.Context, keys []Key) error

	// Capabilities lets Build refuse a guarantee tier the deployment's store cannot support, rather
	// than corrupting it at recovery time. The deployment's own stores are part of what is validated.
	Capabilities() StoreCaps
}

// StoreCaps is what a state store can actually promise.
type StoreCaps struct {
	// AtomicMultiKey is required for any tier above at-least-once: a checkpoint is one record spanning
	// lane cursors, the schema epoch, the pending committables and the dedupe additions, and a partial
	// write of it is unrecoverable.
	AtomicMultiKey bool `json:"atomic_multi_key"`

	CAS          bool `json:"cas"`
	EpochFencing bool `json:"epoch_fencing"`

	Durability connector.Durability `json:"durability"`

	// FlushIsDurable must be true. A Set that returns before the bytes are durable breaks the commit
	// protocol at phase two. It is a declared field rather than an assumption so that Build can refuse
	// the deployment rather than trusting it.
	FlushIsDurable bool `json:"flush_is_durable"`
}

// Supports reports whether these capabilities are sufficient for a guarantee tier, and why not when
// they are not. The reason is operator-facing and becomes a submit-time diagnostic.
func (c StoreCaps) Supports(g connector.Guarantee) (bool, string) {
	if !c.FlushIsDurable {
		return false, "the configured state store does not guarantee that a write is durable when it returns, so canal cannot promise any delivery tier on it"
	}
	if !c.CAS || !c.EpochFencing {
		return false, "the configured state store has no compare-and-set or no epoch fencing, so two workers could both write one lane's progress"
	}

	// Durability is the DOMAIN a write survives in, and it is a SEPARATE question from FlushIsDurable,
	// which only says the write is durable by the time Set returns. A store can answer FlushIsDurable
	// truthfully and still hold everything in RAM — which is what the in-memory example store does.
	// Because this check did not exist, that store passed exactly_once.
	//
	// The requirement is deliberately confined to the tiers that need it. At-most-once promises
	// nothing. At-least-once survives a volatile position honestly: losing it means re-reading from
	// the last durable point, or from the beginning, which produces duplicates and not loss — and
	// duplicates are what at-least-once already permits.
	//
	// Above at-least-once the calculus inverts. Both effectively-once and exactly-once are collapse
	// mechanisms: the dedupe additions and the pending committables ARE the guarantee. If they do not
	// outlive the process, a restart re-emits with no memory of what already landed, and the tier is
	// a claim rather than a mechanism. See ADR 0024.
	if g > connector.AtLeastOnce && c.Durability < connector.DurabilityNode {
		return false, fmt.Sprintf(
			"the configured state store's durability domain is %s, so the dedupe state and pending committables that %s depends on do not survive a restart",
			c.Durability, g)
	}
	if g > connector.AtLeastOnce && !c.AtomicMultiKey {
		return false, "the configured state store cannot write several keys atomically, so a checkpoint could be torn between its cursors and its committables"
	}
	return true, ""
}
