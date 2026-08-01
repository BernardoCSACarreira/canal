package telemetry

// The closed metric-name set, pinned by a golden-file test against real /metrics output.
//
// NEVER SHIP A METRIC CALLED "LAG". Ship the four separately-named quantities, each documented as an
// explicit subtraction: event-time lag per node, backlog records and bytes, position fraction, and
// buffer depth. A single number called lag is four different questions wearing one name.
//
// WHEN A QUANTITY IS UNMEASURABLE, OMIT THE SERIES ENTIRELY — never emit 0. A fully stalled pipeline
// must alarm, and [MCheckpointAge] is the one always-available, unfakeable metric that catches every
// stall mode; a design where a stall produces no samples fails silently.
const (
	MRecordsRead      = "canal_records_read_total"      // {pipeline,lane,connector}
	MRecordsWritten   = "canal_records_written_total"   // {pipeline,node,connector}
	MRecordsCommitted = "canal_records_committed_total" // {pipeline,lane}
	MRecordsAbandoned = "canal_records_abandoned_total" // {pipeline,lane,reason}
	MRecordsDuplicate = "canal_records_duplicate_total" // {pipeline,node}
	MRecordsDeduped   = "canal_records_deduped_total"   // {pipeline,node,layer}
	MFaults           = "canal_faults_total"            // {pipeline,node,op,class,blame}

	// MCheckpointAge is the PRIMARY ALERT signal: seconds since this lane's durable cursor advanced.
	MCheckpointAge = "canal_checkpoint_age_seconds" // {pipeline,lane}

	// MInFlight and MInFlightBudget are two honestly named series. MReplayRecords is the MEASURED
	// worst-case re-read; exporting the budget as if it were the measurement is a config value dressed
	// up as an observation.
	MInFlight       = "canal_lane_inflight_records"        // {pipeline,lane}
	MInFlightBudget = "canal_lane_inflight_budget_records" // {pipeline,lane}
	MReplayRecords  = "canal_lane_replay_records"          // {pipeline,lane}

	MOldestPending = "canal_oldest_pending_age_seconds" // {pipeline,lane}
	MBlocked       = "canal_node_blocked_seconds_total" // {pipeline,node}

	// MUtilization is the bottleneck finder: the fraction of wall time a node spent working.
	MUtilization = "canal_node_utilization_ratio" // {pipeline,node}

	// MBackoff is cumulative TIME, not attempts: a retry count says retries happened, retry seconds
	// says the pipeline spends eighty percent of its life backing off.
	MBackoff = "canal_backoff_seconds_total" // {pipeline,node,class}

	MBufferDepth   = "canal_buffer_depth_records" // {pipeline,node}
	MBufferRefused = "canal_buffer_refused_total" // {pipeline,node,reason}

	// MRevokedUnsettled is the named cost of fencing a lane: records in flight when a lease lapsed,
	// which the new holder will re-deliver.
	MRevokedUnsettled = "canal_lane_revoked_unsettled_records" // {pipeline,lane}

	// MStateStaleness makes phase two's failure visible: if the state store is unreachable the ack
	// chain keeps working, nothing is lost, nothing is falsely acknowledged, and THIS climbs.
	MStateStaleness = "canal_state_persist_staleness_seconds" // {pipeline}

	MCommitLatency = "canal_commit_latency_seconds" // {pipeline,phase}

	// MRestorePhase instruments restart in phases — state load, restore, connect, first record —
	// because if restart-and-resume is a headline feature then restart time is a metric and not an
	// assumption.
	MRestorePhase = "canal_restore_phase_seconds" // {pipeline,phase}

	// MLedgerLeaks and MUnclassified MUST stay zero. The conformance kit asserts it.
	MLedgerLeaks  = "canal_ledger_leaks_total"        // {pipeline,node}
	MUnclassified = "canal_unclassified_faults_total" // {pipeline,node}

	// MAbandonedPluginCalls counts wedged plugin calls the host abandoned. Each leaks one goroutine;
	// a non-zero value is an alertable condition, and it is the disclosed cost of making an in-process
	// boundary behave like a subprocess boundary.
	MAbandonedPluginCalls = "canal_abandoned_plugin_calls_total" // {pipeline,node}

	// MReconcileDelta is records in minus records out per checkpoint.
	MReconcileDelta = "canal_reconcile_delta_records" // {pipeline}

	// MConditions exports every condition as a bounded series, so a silently unapplied config change
	// becomes an alert instead of a mystery.
	MConditions = "canal_condition" // {pipeline,condition,status}
)

// MetricNames is every metric this build exports, for the golden-file test that pins them. A metric
// that is not in this list is a metric nothing agreed to.
var MetricNames = []string{
	MRecordsRead, MRecordsWritten, MRecordsCommitted, MRecordsAbandoned,
	MRecordsDuplicate, MRecordsDeduped, MFaults, MCheckpointAge,
	MInFlight, MInFlightBudget, MReplayRecords, MOldestPending,
	MBlocked, MUtilization, MBackoff, MBufferDepth, MBufferRefused,
	MRevokedUnsettled, MStateStaleness, MCommitLatency, MRestorePhase,
	MLedgerLeaks, MUnclassified, MAbandonedPluginCalls, MReconcileDelta, MConditions,
}

// MetricHelp is the one-line description exported as # HELP.
//
// It lives beside the names rather than at the exporter, because a name and its meaning drifting
// apart is how a dashboard ends up alerting on a quantity nobody can define. TestEveryMetricHasHelp
// fails on a name added here without one.
var MetricHelp = map[string]string{
	MRecordsRead:      "Records read from a source, by lane.",
	MRecordsWritten:   "Records a sink reported durable.",
	MRecordsCommitted: "Records whose position reached the source as a durable acknowledgement.",
	MRecordsAbandoned: "Records that reached a terminal disposition and will not be delivered.",
	MRecordsDuplicate: "Records a destination recognised as already present, which count as durable.",
	MRecordsDeduped:   "Records suppressed by a dedupe layer.",
	MFaults:           "Faults raised, by class and by who owns the problem.",

	MCheckpointAge: "Seconds since this lane's durable cursor last advanced. THE primary alert signal.",

	MInFlight:       "Records admitted and not yet settled.",
	MInFlightBudget: "The configured in-flight bound for this lane.",
	MReplayRecords:  "Measured worst-case re-read: records admitted since the last durable position.",

	MOldestPending: "Age of the oldest unsettled record in this lane.",
	MBlocked:       "Cumulative seconds a node spent blocked on backpressure.",
	MUtilization:   "Fraction of wall time a node spent working.",
	MBackoff:       "Cumulative seconds spent waiting to retry, by fault class.",

	MBufferDepth:   "Records held in a buffer node.",
	MBufferRefused: "Records a buffer refused, by reason.",

	MRevokedUnsettled: "Records in flight when a lane lease lapsed, which the new holder re-delivers.",
	MStateStaleness:   "Seconds since canal last wrote its own state durably.",
	MCommitLatency:    "Seconds per phase of the three-phase commit.",
	MRestorePhase:     "Seconds per phase of restart and restore.",

	MLedgerLeaks:  "Settlement groups abandoned by the leak reaper. MUST stay zero.",
	MUnclassified: "Faults a connector returned with no class. MUST stay zero.",

	MAbandonedPluginCalls: "Plugin calls the host gave up on. Each leaks one goroutine.",
	MReconcileDelta:       "Records in minus records out for one checkpoint.",
	MConditions:           "Every pipeline condition as a bounded series.",
}

// The CLOSED label vocabulary, enforced at registration.
//
// Nothing per-record-key, no error message, no upstream error code, and no unbounded stream label at
// pipeline granularity. Per-stream detail is served from the read model, not from labels: a label set
// that can grow with the data is a cardinality explosion waiting for a busy Tuesday.
const (
	LabelTenant    = "tenant"
	LabelPipeline  = "pipeline"
	LabelNode      = "node"
	LabelLane      = "lane"
	LabelConnector = "connector"
	LabelKind      = "kind"
	LabelClass     = "class"
	LabelOp        = "op"
	LabelBlame     = "blame"
	LabelOutcome   = "outcome"
	LabelReason    = "reason"
	LabelPhase     = "phase"
	LabelCondition = "condition"
	LabelStatus    = "status"
	LabelWorker    = "worker"
	LabelLayer     = "layer"
)

// Labels is the whole permitted label vocabulary. A connector requesting a label outside it gets an
// error, not a cardinality explosion.
var Labels = []string{
	LabelTenant, LabelPipeline, LabelNode, LabelLane, LabelConnector, LabelKind,
	LabelClass, LabelOp, LabelBlame, LabelOutcome, LabelReason, LabelPhase,
	LabelCondition, LabelStatus, LabelWorker, LabelLayer,
}

// LabelPermitted reports whether name is in the closed vocabulary.
func LabelPermitted(name string) bool {
	for _, l := range Labels {
		if l == name {
			return true
		}
	}
	return false
}

// The restart phases [MRestorePhase] is labelled by.
const (
	PhaseStateLoad   = "state_load"
	PhaseRestore     = "restore"
	PhaseConnect     = "connect"
	PhaseFirstRecord = "first_record"
)

// The commit phases [MCommitLatency] is labelled by. They are the three phases of the commit protocol,
// named so that "which phase is slow" is answerable.
const (
	CommitPhaseResolve = "resolve"  // the prefix advanced
	CommitPhasePersist = "persist"  // canal's own durable write, flushed
	CommitPhaseUpstrm  = "upstream" // Source.Commit
)
