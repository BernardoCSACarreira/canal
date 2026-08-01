package store

import (
	"context"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// WorkerID identifies one canal worker process.
type WorkerID string

// AssignmentID identifies one assignable unit: exactly one lane of one pipeline generation.
type AssignmentID string

// WorkerInfo is what a worker publishes when it joins.
type WorkerInfo struct {
	ID      WorkerID        `json:"id"`
	Tenant  record.TenantID `json:"tenant"`
	Address string          `json:"address,omitempty"`
	Version string          `json:"version"`
	Started time.Time       `json:"started"`

	// Labels are deployment-supplied and opaque: a zone, a node name, a capability tag. The coordinator
	// stores and returns them; nothing in the core interprets them.
	Labels map[string]string `json:"labels,omitempty"`
}

// Membership is a live view of the worker set. Closing Done means the membership is no longer valid
// and the worker must rejoin.
type Membership interface {
	// Workers returns the currently-known worker set, as a snapshot.
	Workers(ctx context.Context) ([]WorkerInfo, error)

	// Changes is closed and replaced whenever the worker set changes.
	Changes() <-chan struct{}

	// Leave withdraws this worker.
	Leave(ctx context.Context) error
}

// Leadership is a planning-only claim.
//
// LEADERSHIP IS NEVER TRUSTED FOR CORRECTNESS. A widely-used leader-election implementation documents
// that it "does not guarantee that only one client is acting as a leader", and that clients infer
// leadership from LOCALLY CAPTURED TIMESTAMPS. So anything whose correctness depends on
// single-leadership is broken by construction. The LEASE EPOCH is the fencing token and the state
// store's per-key compare-and-set is the second fence.
type Leadership interface {
	// IsLeader reports the local belief. It is advisory.
	IsLeader() bool

	// Lost is closed when this process should stop planning. A former leader that keeps planning writes
	// assignment rows that lose a compare-and-set; it does not corrupt anything.
	Lost() <-chan struct{}

	// Resign gives up the claim.
	Resign(ctx context.Context) error
}

// Coordinator provides membership, leader election and assignment leases.
type Coordinator interface {
	Join(ctx context.Context, w WorkerInfo) (Membership, error)
	Campaign(ctx context.Context) (Leadership, error)

	// Plan writes the assignment rows for a pipeline's announced lanes. Called only by the leader.
	//
	// THE PLAN IS DURABLE STATE, not a leader's in-memory result, so a leader crash loses nothing and a
	// worker holding a valid lease needs nothing from anyone.
	//
	// Note the parameter type: [LaneRow], whose spec is a record.Blob. No store implementation ever
	// serialises a connector domain type, so the lane spec's encoding stays versioned by the engine and
	// a store cannot silently change it.
	//
	// Plan also enforces the gates: a lane whose StartAfter group still has an unfinished member is
	// left unassigned and is not offered to any worker. That is what makes the snapshot-to-stream
	// handoff hold cluster-wide rather than being a per-connector convention.
	Plan(ctx context.Context, t record.TenantID, id record.PipelineID, gen uint64, rows []LaneRow) error

	// Claim, Renew and Release are the ENTIRE placement protocol.
	//
	// There is no stop-the-world rebalance, because there is nothing global to agree on: assignment is
	// per-lane, claimed by lease, and the plan is durable. A rebalance storm is not solved here; it is
	// absent.
	Claim(ctx context.Context, a AssignmentID, w WorkerID, ttl time.Duration) (Lease, error)
	Renew(ctx context.Context, l Lease) (Lease, error)
	Release(ctx context.Context, l Lease) error

	// Assignments returns the current assignment rows for a pipeline.
	Assignments(ctx context.Context, t record.TenantID, id record.PipelineID) ([]Assignment, error)
}

// LaneRow is a lane as the coordinator sees it: identity, gating and opaque bytes.
type LaneRow struct {
	ID    record.LaneID    `json:"id"`
	Name  string           `json:"name"`
	Group record.LaneGroup `json:"group,omitempty"`

	// After names the lane groups that must be finished and durable before this lane may be assigned.
	After []record.LaneGroup `json:"after,omitempty"`

	// Spec is OPAQUE. The engine owns its codec and its version; the store moves bytes.
	Spec record.Blob `json:"spec"`

	Bounded  bool `json:"bounded"`
	Finished bool `json:"finished"`

	// FinishedAt records WHEN the finish became durable, not merely that it happened. A gate that fires
	// on "finished" without knowing whether that fact survived a crash is a gate that can open twice.
	FinishedAt time.Time `json:"finished_at,omitempty"`

	// Weight is an estimated record count for progress reporting. Zero means unknown.
	Weight uint64 `json:"weight,omitempty"`
}

// Assignment is one lane's placement.
type Assignment struct {
	ID       AssignmentID      `json:"id"`
	Tenant   record.TenantID   `json:"tenant"`
	Pipeline record.PipelineID `json:"pipeline"`

	// Generation is the config revision this assignment was planned for. An assignment from an older
	// generation is not claimed.
	Generation uint64 `json:"generation"`

	Lane LaneRow `json:"lane"`

	// Worker and Epoch are empty and zero while the row has never been claimed, and are cleared by
	// Release. THEY ARE NOT CLEARED WHEN A LEASE MERELY EXPIRES: a lapsed row goes on naming its
	// previous holder, because that identity is what DefaultReassignmentDelay reserves it for. So
	// LeaseExpires is the discriminator and not Worker — a row whose expiry has passed names whoever
	// held it last, not whoever holds it now, and [Lease.Valid] is the comparison to make.
	//
	// Reading Worker as "the current holder" without checking the expiry is therefore wrong in
	// exactly the situation an operator is most likely to be looking: a worker that just died.
	Worker WorkerID `json:"worker,omitempty"`
	Epoch  uint64   `json:"epoch,omitempty"`

	LeaseExpires time.Time `json:"lease_expires,omitempty"`

	// Gated is true while the lane's StartAfter groups still have unfinished members. The read model
	// renders it as "waiting on", so "why is nothing happening" answers itself.
	Gated bool `json:"gated"`
}

// Lease carries the epoch.
//
// Every durable write on a lane's behalf carries it, and a worker whose Renew fails treats every lane
// in that lease as revoked. The epoch is the fencing token: the loser of a race writes with a stale
// epoch, the store rejects it, and the loser's lane — not its whole process — is revoked.
type Lease struct {
	Assignment AssignmentID `json:"assignment"`
	Worker     WorkerID     `json:"worker"`
	Epoch      uint64       `json:"epoch"`
	Expires    time.Time    `json:"expires"`
}

// Valid reports whether the lease has not yet expired at the given instant. It is advisory on the
// holder's side — the authority is the store rejecting a stale epoch — and exists so a worker can stop
// early rather than discovering the fence on a write.
func (l Lease) Valid(now time.Time) bool { return now.Before(l.Expires) }

// Default lease timings. Reassignment is deliberately DELAYED against the lease TTL so a bouncing
// worker reclaims its own lanes instead of triggering a cluster-wide reshuffle.
const (
	DefaultLeaseTTL          = 30 * time.Second
	DefaultRenewInterval     = 10 * time.Second
	DefaultReassignmentDelay = 120 * time.Second
)
