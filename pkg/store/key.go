package store

import (
	"strings"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Space is the reserved key namespace within a tenant. One [StateStore] backs every kind of durable
// state, separated by space, because a fifth store interface would be an abstraction failure and
// separate stores cannot be written atomically together.
type Space string

const (
	// SpaceLane holds per-lane rows: the write-once construction spec and the write-many cursor.
	SpaceLane Space = "lane"
	// SpaceCheckpoint holds the pipeline's checkpoint records.
	SpaceCheckpoint Space = "checkpoint"
	// SpaceSchema holds the pipeline's schema table, keyed by fingerprint, so a record referencing a
	// fingerprint always resolves after a restart.
	SpaceSchema Space = "schema"
	// SpaceDedupe holds the seen-set, written in the SAME atomic write that advances the cursor —
	// which is what makes "duplicate means already durably stored" true.
	SpaceDedupe Space = "dedupe"
	// SpaceConnector backs connector.StateHandle.
	SpaceConnector Space = "connector"
)

// Key is STRUCTURED, never a concatenated string, so a tenant can never be spoofed by a pipeline id
// containing a separator.
//
// Tenant isolation is therefore a property of the type rather than of every call site's string
// hygiene.
type Key struct {
	Tenant record.TenantID
	Space  Space
	Parts  []string
}

// String renders k for logs and for a map key within one process. It is NOT the storage encoding: an
// implementation is free to store the tenant and space as columns, and must not rely on this format.
func (k Key) String() string {
	var b strings.Builder
	b.WriteString(escapeKeyPart(string(k.Tenant)))
	b.WriteByte('/')
	b.WriteString(escapeKeyPart(string(k.Space)))
	for _, p := range k.Parts {
		b.WriteByte('/')
		b.WriteString(escapeKeyPart(p))
	}
	return b.String()
}

// escapeKeyPart makes the separator unambiguous, the same way record.DeriveLaneID does.
//
// WITHOUT IT TWO DIFFERENT KEYS RENDER ALIKE. Pipeline "a" with node "b/c" and pipeline "a/b" with
// node "c" both produced "acme/connector/a/b/c/lane", and this string is the identity a store
// indexes by — the WAL keys its in-memory map on it and store.Batch keys its writes on it — so one
// pipeline's connector state silently overwrote another's inside the same tenant. Node and pipeline
// ids are operator-chosen, so a slash in one is all it takes.
//
// It is safe to change: the WAL persists the Key STRUCT, with each part length-prefixed, and never
// this rendering. Nothing on disk moves.
func escapeKeyPart(s string) string {
	if !strings.ContainsAny(s, "%/") {
		return s
	}
	return strings.NewReplacer("%", "%25", "/", "%2F").Replace(s)
}

// Prefix reports whether k is within p: same tenant, same space, and every part of p leading k's.
func (k Key) Prefix(p Key) bool {
	if k.Tenant != p.Tenant || k.Space != p.Space || len(p.Parts) > len(k.Parts) {
		return false
	}
	for i := range p.Parts {
		if k.Parts[i] != p.Parts[i] {
			return false
		}
	}
	return true
}

// LaneKey is the key for one lane's durable row.
func LaneKey(t record.TenantID, p record.PipelineID, lane record.LaneID) Key {
	return Key{Tenant: t, Space: SpaceLane, Parts: []string{string(p), string(lane)}}
}

// CheckpointKey is the key for a pipeline's checkpoint record. There is exactly ONE per pipeline: no
// checkpoint history, because retaining history implies a compaction policy, a retention policy and a
// restore UI, none of which the goals ask for. Re-reading is done by editing the offset.
func CheckpointKey(t record.TenantID, p record.PipelineID) Key {
	return Key{Tenant: t, Space: SpaceCheckpoint, Parts: []string{string(p)}}
}

// SchemaKey is the key for one schema body in a pipeline's table, addressed by content fingerprint.
func SchemaKey(t record.TenantID, p record.PipelineID, fingerprint string) Key {
	return Key{Tenant: t, Space: SpaceSchema, Parts: []string{string(p), fingerprint}}
}

// DedupeKey is the key for one dedupe entry. The scope is fixed and not configurable below
// tenant-pipeline-node-stream: an earlier attempt keyed on the bare event id, so two connectors or
// two tenants emitting 1, 2, 3 silently discarded each other's events (design rule R5).
func DedupeKey(t record.TenantID, p record.PipelineID, node record.NodeID, stream record.StreamName, layer, identity string) Key {
	return Key{Tenant: t, Space: SpaceDedupe, Parts: []string{
		string(p), string(node), string(stream), layer, identity,
	}}
}

// ConnectorKey is the key backing connector.StateHandle for one lane.
func ConnectorKey(t record.TenantID, p record.PipelineID, node record.NodeID, lane record.LaneID) Key {
	return Key{Tenant: t, Space: SpaceConnector, Parts: []string{string(p), string(node), string(lane)}}
}

// ConnectorNodeKey is the key backing connector.StateHandle's NODE-scoped slot: state that must exist
// before any lane does.
//
// The reserved part "@node" cannot collide with a lane id, because record.DeriveLaneID percent-escapes
// every component and never produces a leading "@".
func ConnectorNodeKey(t record.TenantID, p record.PipelineID, node record.NodeID) Key {
	return Key{Tenant: t, Space: SpaceConnector, Parts: []string{string(p), string(node), "@node"}}
}

// Versioned is one value plus its compare-and-set version.
type Versioned struct {
	Key   Key
	Value []byte

	// IfVersion is the version the write requires; 0 means "must not exist".
	IfVersion uint64

	// Version is the version the store returns.
	Version uint64

	// Epoch fences THIS key. Zero means the batch's Epoch, which is every single-lane case.
	//
	// A batch-wide epoch is unfenceable for a multi-lane write, and a multi-lane atomic write is the
	// only reason StateHandle.SetMany exists. Each lane is its own assignment with its own lease epoch,
	// so a worker holding 32 lanes at 32 epochs had one number to offer: too high and a fenced worker's
	// write is accepted for the lanes it has lost, too low and a valid write is refused for the lanes
	// it holds. Per-key epochs make the guarantee the interface advertises actually reachable.
	Epoch uint64
}

// EpochFor reports the epoch that fences v within batch b: v's own when set, otherwise b's.
func (b *Batch) EpochFor(v Versioned) uint64 {
	if v.Epoch != 0 {
		return v.Epoch
	}
	return b.Epoch
}

// Batch is one atomic write. The store rejects the WHOLE batch if the epoch is stale for ANY key it
// touches, and reports which — so a fenced worker's partial write cannot exist.
type Batch struct {
	// Epoch is the DEFAULT fence for writes in this batch that do not carry their own.
	Epoch uint64

	// Writes is keyed by Key.String so that two writes to one key in one batch are impossible by
	// construction.
	Writes map[string]Versioned

	// Deletes carry their own epoch for the same reason Writes do, and until they did, a delete was
	// the ONE lane mutation nothing could refuse.
	//
	// StateStore.Delete takes bare keys and no epoch, so a store will do as it is told. Every other
	// fenced operation degrades to a rejected write; that one degrades to destroying state the new
	// holder owns and is reading — which makes the fence matter MORE there, not less. Routing a lane's
	// delete through a batch also makes it atomic with the writes that accompany it, which retiring a
	// lane wants anyway.
	Deletes []Deletion
}

// Deletion is one key to remove, plus the fence that authorises removing it.
type Deletion struct {
	Key Key

	// Epoch fences THIS key. Zero means the batch's Epoch, exactly as [Versioned.Epoch] does.
	Epoch uint64
}

// NewBatch returns an empty batch whose default fence is the given epoch.
func NewBatch(epoch uint64) *Batch {
	return &Batch{Epoch: epoch, Writes: map[string]Versioned{}}
}

// Put adds a write to the batch, fenced by the batch's default epoch.
func (b *Batch) Put(k Key, value []byte, ifVersion uint64) {
	if b.Writes == nil {
		b.Writes = map[string]Versioned{}
	}
	b.Writes[k.String()] = Versioned{Key: k, Value: value, IfVersion: ifVersion}
}

// PutFenced adds a write fenced by its OWN epoch, for a key whose lease is not the batch's.
func (b *Batch) PutFenced(k Key, value []byte, ifVersion, epoch uint64) {
	if b.Writes == nil {
		b.Writes = map[string]Versioned{}
	}
	b.Writes[k.String()] = Versioned{Key: k, Value: value, IfVersion: ifVersion, Epoch: epoch}
}

// Del adds a delete fenced by the batch's default epoch, for a key that is not one lane's.
func (b *Batch) Del(k Key) { b.Deletes = append(b.Deletes, Deletion{Key: k}) }

// DelFenced adds a delete fenced by its OWN epoch, for a key whose lease is not the batch's.
func (b *Batch) DelFenced(k Key, epoch uint64) {
	b.Deletes = append(b.Deletes, Deletion{Key: k, Epoch: epoch})
}

// EpochForDelete reports the epoch that fences d within batch b: d's own when set, otherwise b's.
func (b *Batch) EpochForDelete(d Deletion) uint64 {
	if d.Epoch != 0 {
		return d.Epoch
	}
	return b.Epoch
}

// Len reports how many mutations the batch carries.
func (b *Batch) Len() int { return len(b.Writes) + len(b.Deletes) }
