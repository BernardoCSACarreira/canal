// Package telemetry owns metric naming, the closed label vocabulary, and the single read-model
// document.
//
// The core owns naming and export; a connector registers through connector.Metrics and can never
// name a metric or invent a label.
//
// # Two disciplines that keep the read model honest
//
// EVERY UNKNOWN IS A NIL POINTER, never a zero. The frontend has one shared "unknown" renderer, and
// a pinned fixture in which every optional field is absent asserts the UI renders no zeros — so "the
// connector cannot tell you the lag" never displays as "the lag is zero".
//
// And [PipelineStatus.Complete] is false with [PipelineStatus.Missing] naming the workers when the
// aggregator has not heard from every one, so a partial document says "partial" instead of quietly
// under-reporting.
//
// # The honesty invariant, asserted by a test
//
// [CondSourceReady] True must never be able to imply [CondProgressing] True. A fixture in which the
// source and sink are connected and the durable cursor has not moved for an hour must render as
// unhealthy. A metrics UI that cannot distinguish "the endpoint answered" from "your data arrived" is
// actively misleading.
package telemetry
