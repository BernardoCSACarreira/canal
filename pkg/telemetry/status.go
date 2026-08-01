package telemetry

import "time"

// Phase is the pipeline's coarse state.
//
// Kubernetes-shaped: ONE phase plus ORTHOGONAL conditions, because one enum provably cannot describe
// "running, healthy connection, forty minutes behind, sink returning 429s" and a product of enums is
// a combinatorial nightmare.
//
// The legacy operator vocabulary maps on with no second enum and no cross-map function (design rule
// R9): healthy is [PhaseRunning] with no degraded condition; degraded is [PhaseRunning] with
// [CondDegraded] true plus a reason and a last fault; paused is [PhasePaused]; terminal is
// [PhaseFailed].
type Phase string

const (
	PhasePending  Phase = "pending"
	PhaseStarting Phase = "starting"
	PhaseRunning  Phase = "running"
	PhasePaused   Phase = "paused"
	PhaseDraining Phase = "draining"
	PhaseStopped  Phase = "stopped"
	PhaseFailed   Phase = "failed"

	// PhaseCompleted is the terminal SUCCESS of a bounded pipeline. Its absence in one surveyed
	// system means a finished batch job looks identical to a stalled stream.
	PhaseCompleted Phase = "completed"
)

// ConditionType is the closed condition set.
//
// Nine types times three statuses is 27 bounded series per pipeline, which is why conditions are
// metrics as well as read-model fields — so "my config change silently did not apply" becomes an
// alert instead of a mystery.
type ConditionType string

const (
	CondConfigured  ConditionType = "configured"
	CondAssigned    ConditionType = "assigned"
	CondSourceReady ConditionType = "source_connected"
	CondSinkReady   ConditionType = "sink_connected"

	// CondProgressing means the DURABLE cursor is advancing. Not that the connection is up, not that
	// records are being read: that canal's own record of progress moved.
	CondProgressing ConditionType = "progressing"

	CondCaughtUp      ConditionType = "caught_up"
	CondBackpressured ConditionType = "backpressured"
	CondDegraded      ConditionType = "degraded"

	// CondSpecApplied is true when ObservedGeneration equals Generation. It answers "did my config
	// change take effect?", which one surveyed status API structurally cannot.
	CondSpecApplied ConditionType = "spec_applied"
)

// ConditionTypes is every condition, in a stable order for rendering and for fixtures.
var ConditionTypes = []ConditionType{
	CondConfigured, CondAssigned, CondSourceReady, CondSinkReady,
	CondProgressing, CondCaughtUp, CondBackpressured, CondDegraded, CondSpecApplied,
}

// Status is a condition's three-valued state. Unknown is a first-class answer, not a stand-in for
// false.
type Status string

const (
	StatusUnknown Status = "unknown"
	StatusTrue    Status = "true"
	StatusFalse   Status = "false"
)

// The closed reason vocabulary. A reason is simultaneously the wire token, the metric label value and
// the i18n key suffix (design rule R9), so it must be a named constant and never an ad-hoc string at
// a call site.
const (
	ReasonStarting              = "starting"
	ReasonNoLanes               = "no_lanes"
	ReasonLaneGated             = "lane_gated"
	ReasonCommitFailed          = "commit_failed"
	ReasonIndeterminateWrite    = "indeterminate_write"
	ReasonStateStoreUnreachable = "state_store_unreachable"
	ReasonStateWrittenByNewer   = "state_written_by_newer_build"
	ReasonBufferFull            = "buffer_full"
	ReasonBudgetExhausted       = "budget_exhausted"
	ReasonSustainedBackoff      = "sustained_backoff"
	ReasonGroupLeaked           = "group_leaked"
	ReasonCommittableExpired    = "committable_expired"
	ReasonDowngradeAcknowledged = "downgrade_acknowledged"
	ReasonAbandonedPluginCall   = "abandoned_plugin_call"
	ReasonReconcileDelta        = "reconcile_delta"
	ReasonSchemaChangePending   = "schema_change_pending"
	// ReasonRetriesExhausted and ReasonTerminalFault are the two ways a record reaches a terminal
	// disposition, and they are separate because the operator response differs. Exhausted means the
	// remote kept failing and MaxAttempts ran out — raise the budget, or fix the remote. Terminal
	// means retrying could never have helped, because the class says so: a mapping error, a
	// permanent refusal, a contract violation. Reporting both as "failed" hid which one it was.
	ReasonRetriesExhausted = "retries_exhausted"
	ReasonTerminalFault    = "terminal_fault"

	ReasonDrainTimeout = "drain_timeout"
	ReasonDrained      = "drained"
	ReasonCaughtUp     = "caught_up"
	ReasonProgressing  = "progressing"
	ReasonConnected    = "connected"
	ReasonNotConnected = "not_connected"
	ReasonApplied      = "applied"
	ReasonPending      = "pending"
)

// Condition is one orthogonal fact about a pipeline.
type Condition struct {
	Type   ConditionType `json:"type"`
	Status Status        `json:"status"`

	// Reason is from the closed vocabulary above, and is also the i18n key.
	Reason string `json:"reason"`

	// Message is the human sentence. It may name counts, components and examples.
	Message string `json:"message"`

	LastTransitionTime time.Time `json:"lastTransitionTime"`

	// ObservedGeneration is the config revision this condition was computed against, so a stale
	// condition is identifiable as stale.
	ObservedGeneration uint64 `json:"observedGeneration"`
}
