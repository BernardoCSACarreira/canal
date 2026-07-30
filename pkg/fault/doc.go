// Package fault defines canal's closed error-classification set, the per-record
// failure shape, and the retry policy.
//
// The set is closed for three reasons: a closed set is a legitimate bounded metric
// label; a closed set makes "a hint the framework ignores" impossible (one surveyed
// framework honours its own back-off hint only on connect, which is exactly that
// bug); and a closed set can be rendered by a UI with no connector-specific code.
//
// The package imports only [github.com/BernardoCSACarreira/canal/pkg/record] and
// the standard library. record does NOT import fault: a record's attached fault is
// stored as a plain error and read through record.Record.Failed, so the dependency
// direction stays strictly downward with no cycle.
//
// Design rule R7 — write the failure shape at the same time as the success shape —
// is why [RecordFault] lives here rather than being invented later at a call site.
package fault
