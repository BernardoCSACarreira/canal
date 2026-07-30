// Package connector defines every interface a connector author implements and every
// handle the core injects.
//
// THE RULE FOR THIS WHOLE PACKAGE: behaviour is an optional exported interface; the
// FACT of that behaviour is declarative data in a Caps struct; registration
// cross-checks declared-against-implemented in one direction only; and the core
// type-asserts in exactly one place (registry.ResolveSource / registry.ResolveSink).
//
// The reason for both halves: a type assertion cannot cross a process boundary, and a
// flag with no methods behind it is worthless. Data crosses the wire; interfaces do the
// work.
//
// # What a connector author must know
//
// A source is four methods ([Source]); a sink is three ([Sink]). Both are FROZEN: no
// method will ever be added to either. New core capabilities arrive through the
// [SourceRuntime] and [SinkRuntime] interfaces — which the core implements, so growing
// them breaks nothing — or as new fields on request structs such as [Opening] and
// [Request], or as new optional interfaces plus a Caps field.
//
// One surveyed framework added a method to a required interface and its official
// documentation now instructs connector authors to catch NoSuchMethodError. Another has
// five default-throwing methods and three sink-API rewrites. Both are the cost of not
// freezing.
//
// # The out-of-process seam
//
// Every method in this package is (ctx, serialisable) -> (serialisable, error). No
// closures, no channels in a request or response, no interface-typed payload fields, no
// behavioural schema objects. Everything durable or boundary-crossing is a
// record.Blob. That is what lets a gRPC or subprocess implementation satisfy these same
// interfaces later with no core change, and it is why the required interfaces can be
// frozen at all.
package connector
