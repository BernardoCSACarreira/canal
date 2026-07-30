// Package spec is a pipeline's declarative definition: the thing the config store holds, the API
// accepts, and the engine builds from.
//
// It is a GRAPH, not a fixed stage list. An earlier attempt froze eight stages into its OpenAPI
// schema as `stages` with minItems 8 and maxItems 8 and an ordinal constrained 1..8, so adding a
// transform was a breaking contract change — and it modelled buffers TWICE, as stages 3/5/7 and
// again as segments keyed by the ordinal they followed. Design rule R1 exists because of that
// document, and this package satisfies it structurally: one representation per entity, no stage
// count anywhere, and adding a node kind is data.
//
// Dependency direction: spec imports record, config, fault, connector and registry. It is a leaf as
// far as store is concerned — the config store deals in spec.Spec and never in an engine type —
// which is what keeps the deployment seam free of the engine.
package spec
