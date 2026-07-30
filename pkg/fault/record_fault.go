package fault

import "github.com/BernardoCSACarreira/canal/pkg/record"

// RecordFault is one record's outcome, classified by the connector at the point of
// raise, keyed on framework-assigned identity rather than on a batch index.
//
// This is design rule R7 made concrete: the failure shape is written in the same
// package, at the same time, as the success shape it accompanies. A partial-batch
// contract that says "retry only the failed subset" and offers no per-record error
// array is unimplementable as written, and that is exactly what R7 exists to
// prevent.
type RecordFault struct {
	// Record names which record failed. Never an index: positional identity within a
	// batch does not survive filtering, reordering or rebatching.
	Record record.RecordID `json:"record"`

	Class Class `json:"class"`
	Op    Op    `json:"op"`

	// User is shown to an operator: what was wrong with this record and what to do.
	User string `json:"user"`

	// Dev is for the log.
	Dev string `json:"dev,omitempty"`
}

// Fault renders f as a *Fault, for the engine's routing and metric paths which deal
// in one error type.
func (f RecordFault) Fault() *Fault {
	return &Fault{
		Class:  f.Class,
		Op:     f.Op,
		Record: f.Record,
		User:   f.User,
		Dev:    f.Dev,
	}
}
