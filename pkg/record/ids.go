package record

import "strings"

// TenantID scopes everything durable. It is present from the first commit, not
// retrofitted: design rule R13 exists because "tenancy" was once realised as
// "single OS user". In a single-tenant deployment it is the constant "default",
// and every durable key still contains it, so enabling multi-tenancy later is a
// configuration change and not a migration.
type TenantID string

// DefaultTenant is the constant every single-tenant deployment uses. It exists as
// a named constant so that no call site spells the string itself.
const DefaultTenant TenantID = "default"

// PipelineID identifies a configured pipeline. Stable across restarts and across
// config revisions of the same pipeline.
type PipelineID string

// NodeID identifies one vertex of a pipeline graph. Assigned by the operator in
// the spec, stable across revisions, used as the metric label for the node and as
// the edge endpoint.
type NodeID string

// StreamName is a source-declared logical stream: a table, a topic, a collection,
// an endpoint, a file glob. The core never parses it and attaches no meaning to
// its shape. A source with exactly one stream uses [DefaultStream].
type StreamName string

// DefaultStream is what a source with exactly one logical stream announces.
const DefaultStream StreamName = "default"

// LaneID is the core's handle for one lane. Derived deterministically from
// (tenant, pipeline, node, LaneSpec.Name) by [DeriveLaneID] so the same announced name
// is the same lane across restarts and reuses its persisted state.
type LaneID string

// DeriveLaneID is the derivation, exported.
//
// It is exported because the derivation being unexported forced a lane that must refer
// to ANOTHER lane — a tail lane naming the chunk lanes it waits behind, a planner lane
// naming what it planned — to smuggle an opaque LaneID through LaneSpec.Spec and hope
// the core's spelling never changed. A documented derivation whose implementation is
// unreachable is not a documented derivation.
//
// The encoding escapes each component's separators, so a pipeline id containing a
// slash cannot forge another pipeline's lane. It is stable: changing it invalidates
// every persisted lane row, so it changes only with a state-format migration.
func DeriveLaneID(t TenantID, p PipelineID, n NodeID, name string) LaneID {
	var b strings.Builder
	for i, part := range [...]string{string(t), string(p), string(n), name} {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(strings.NewReplacer("%", "%25", "/", "%2F").Replace(part))
	}
	return LaneID(b.String())
}

// LaneGroup is a connector-authored label grouping lanes for the purpose of
// ordering constraints. It is opaque to the core: the core only ever tests two
// LaneGroups for equality. "scan", "tail", "chunk-set-3" are all legal.
type LaneGroup string

// GroupID identifies one settlement group: the set of records admitted to the
// ledger together, which resolve together. One source batch becomes one group.
type GroupID uint64

// RecordID is a stable, framework-assigned, generation-local identity for one
// in-flight record. It is assigned once at admission and never changes — not
// through a transform, not through rebatching, not through fan-out.
//
// Positional identity within a batch is a proven mistake: Benthos marks its own
// WalkMessages deprecated "as harmful" for exactly this reason, and its
// Indexer/SortGroup machinery exists solely to recover positions after filtering
// and reordering. Every per-record outcome in canal — partial batch failure,
// retry targeting, dead-lettering, settlement, dedupe — is keyed on RecordID and
// never on an index.
//
// It is NOT durable and never appears in persisted state. Durable identity is
// [Origin].Key plus the lane cursor. A RecordID in a dead-letter record is
// provenance for a human, not a key.
type RecordID uint64

// Blob is the universal shape for anything that crosses a role boundary or hits
// disk: a connector-authored payload plus the version of the connector's own
// serialiser that wrote it.
//
// Flink's SimpleVersionedSerializer is forty lines and buys binary-upgrade
// compatibility and the future out-of-process boundary in one move. This is that,
// as a value.
//
// The four-part format contract every Blob obeys:
//
//  1. Additive-only encoding, so state written by a newer build stays
//     structurally readable by an older one.
//  2. Absent or zero Version means legacy: behave exactly as the previous version
//     did. Never "reject" and never "assume the default is correct".
//  3. Never reject state whose Version is greater than the current one, unless
//     the encoding genuinely cannot tolerate it — in which case fail LOUDLY with
//     fault.Contract naming both versions.
//  4. Stamp Version at serialise time, not at construction time, so a parsed
//     legacy record is silently upgraded on its first write.
type Blob struct {
	Version uint32 `json:"version"`
	Bytes   []byte `json:"bytes"`
}

// IsZero reports whether b carries nothing. Used to distinguish "no progress yet"
// from "progress recorded by an unknown version".
func (b Blob) IsZero() bool { return b.Version == 0 && len(b.Bytes) == 0 }

// Clone returns a Blob owning its own bytes.
func (b Blob) Clone() Blob {
	if b.Bytes == nil {
		return Blob{Version: b.Version}
	}
	c := make([]byte, len(b.Bytes))
	copy(c, b.Bytes)
	return Blob{Version: b.Version, Bytes: c}
}
