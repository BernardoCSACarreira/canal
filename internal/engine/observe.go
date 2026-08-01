package engine

import (
	"time"

	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// obs is the engine's instrument set, registered once per pipeline.
//
// Instruments are resolved UP FRONT rather than looked up per event, so the hot path costs a map
// lookup on the label tuple and nothing else, and so a mistake in a name or a label fails at start
// instead of on the first fault at three in the morning.
//
// Every field may be nil. A pipeline built without a registry is instrumented by a set of nils, and
// every method on *metrics.Counter and friends is nil-safe, so the whole surface degrades to
// nothing rather than to a panic. That is what keeps a metric call safe to put on any path.
type obs struct {
	pipeline string

	recordsRead      *metrics.Counter
	recordsWritten   *metrics.Counter
	recordsCommitted *metrics.Counter
	recordsAbandoned *metrics.Counter
	recordsDuplicate *metrics.Counter

	faults       *metrics.Counter
	unclassified *metrics.Counter
	backoff      *metrics.Counter
	leaks        *metrics.Counter
	abandonedRPC *metrics.Counter

	checkpointAge *metrics.Gauge
	inFlight      *metrics.Gauge
	inFlightMax   *metrics.Gauge
	replay        *metrics.Gauge
	staleness     *metrics.Gauge

	commitLatency *metrics.Histogram
}

// newObs registers the engine's own metrics.
//
// A registration error is returned rather than swallowed: every name here is a constant from
// telemetry.MetricNames and every label from telemetry.Labels, so a failure means one of those two
// closed sets has been edited without this file, which is a build-time mistake and not a runtime
// condition to tolerate.
func newObs(r *metrics.Registry, pipeline record.PipelineID) (*obs, error) {
	o := &obs{pipeline: string(pipeline)}
	if r == nil {
		return o, nil
	}

	var err error
	counter := func(name string, labels ...string) *metrics.Counter {
		if err != nil {
			return nil
		}
		var c *metrics.Counter
		c, err = r.Counter(name, labels...)
		return c
	}
	gauge := func(name string, labels ...string) *metrics.Gauge {
		if err != nil {
			return nil
		}
		var g *metrics.Gauge
		g, err = r.Gauge(name, labels...)
		return g
	}

	P, N, L := telemetry.LabelPipeline, telemetry.LabelNode, telemetry.LabelLane

	o.recordsRead = counter(telemetry.MRecordsRead, P, L, telemetry.LabelConnector)
	o.recordsWritten = counter(telemetry.MRecordsWritten, P, N, telemetry.LabelConnector)
	o.recordsCommitted = counter(telemetry.MRecordsCommitted, P, L)
	o.recordsAbandoned = counter(telemetry.MRecordsAbandoned, P, L, telemetry.LabelReason)
	o.recordsDuplicate = counter(telemetry.MRecordsDuplicate, P, N)

	o.faults = counter(telemetry.MFaults, P, N, telemetry.LabelOp, telemetry.LabelClass, telemetry.LabelBlame)
	o.unclassified = counter(telemetry.MUnclassified, P, N)
	o.backoff = counter(telemetry.MBackoff, P, N, telemetry.LabelClass)
	o.leaks = counter(telemetry.MLedgerLeaks, P, N)
	o.abandonedRPC = counter(telemetry.MAbandonedPluginCalls, P, N)

	o.checkpointAge = gauge(telemetry.MCheckpointAge, P, L)
	o.inFlight = gauge(telemetry.MInFlight, P, L)
	o.inFlightMax = gauge(telemetry.MInFlightBudget, P, L)
	o.replay = gauge(telemetry.MReplayRecords, P, L)
	o.staleness = gauge(telemetry.MStateStaleness, P)

	if err == nil {
		o.commitLatency, err = r.Histogram(telemetry.MCommitLatency, nil, P, telemetry.LabelPhase)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// laneLabel renders a lane id as a label value. LaneID is already a string; the helper exists so
// every call site spells the conversion the same way.
func laneLabel(id record.LaneID) string { return string(id) }

// fault records one classified failure.
//
// The UNCLASSIFIED COUNTER IS INCREMENTED FROM THE RAW CLASS, before routing normalises it to
// PermanentInternal. pkg/fault says that counter must stay zero for a compliant connector and the
// conformance kit asserts it — which only works if the normalisation happens after the count. It is
// the one place in the engine where the pre-normalised class is the interesting one.
func (o *obs) fault(node record.NodeID, err error) {
	if o == nil || err == nil {
		return
	}
	class := fault.ClassOf(err)
	if class == fault.Unclassified {
		o.unclassified.Add(1, o.pipeline, string(node))
	}
	op := fault.OpUnknown
	if f, ok := err.(*fault.Fault); ok {
		op = f.Op
	}
	o.faults.Add(1, o.pipeline, string(node), op.String(), class.String(), class.Blames().String())
}

// waited records time spent backing off.
//
// Cumulative SECONDS, not attempts. A retry count says retries happened; retry seconds says the
// pipeline spends eighty percent of its life waiting, which is the question an operator is actually
// asking.
func (o *obs) waited(node record.NodeID, class fault.Class, d time.Duration) {
	if o == nil || d <= 0 {
		return
	}
	o.backoff.Add(d.Seconds(), o.pipeline, string(node), class.String())
}

// abandonedCall records a plugin call the host gave up on.
//
// Each one leaks a goroutine until the wedged call returns, which sandbox.go names as the accepted
// cost of making an in-process boundary behave like a subprocess boundary. A non-zero value is an
// alertable condition, and it could not be alerted on while nothing counted it.
func (o *obs) abandonedCall(node record.NodeID) {
	if o == nil {
		return
	}
	o.abandonedRPC.Add(1, o.pipeline, string(node))
}
