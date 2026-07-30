// Package schema owns canal's canonical type set, schema identity, and the
// change-event vocabulary.
//
// It imports nothing but the standard library, so a record can carry a schema
// reference without importing anything. That property is load-bearing: it is
// what keeps [github.com/BernardoCSACarreira/canal/pkg/record] a leaf package
// and therefore keeps the canonical record model (design rule R2) free of every
// transport.
//
// Growth discipline: the canonical [Type] set is closed and is a metric label.
// Parameterised detail lives in [Logical] so that adding a parameterised type
// never widens [Type]. This is deliberately NOT Kafka Connect's convention of a
// magic name string plus a parameters map, which is its documented source of
// silent disagreement between bad connectors and bad converters.
package schema
