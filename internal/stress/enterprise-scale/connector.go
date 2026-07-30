// Package enterprise is a HOSTILE CONNECTOR written against canal's real interfaces.
//
// It is not a useful connector and is not meant to ship. It is the enterprise-scale
// deployment case implemented for real, so that the places where the interface set cannot
// express the case are compiler errors and named method signatures rather than opinions.
//
// THE CASE
//
//	400 pipelines across 40 worker pods on k8s
//	workers dying and rejoining
//	one pipeline whose reader parallelism must scale 1 -> 32 with no duplication and no loss
//	rolling upgrades: two binary versions against one checkpoint store
//	tenant isolation, per-pipeline rate limits and quotas
//	secrets that rotate UNDER a running pipeline
//	the same code as one local binary with zero external dependencies
//
// The upstream modelled here is deliberately generic: "a system that exposes an ordered
// per-shard log, a snapshot read of existing state, a shard count that can change while
// running, and credentials with a short TTL". Nothing below names a vendor. Kafka, Mongo,
// Postgres, Kinesis, DynamoDB streams, an S3 manifest walk and a paginated HTTP API all fit
// that shape, which is the point: the breakages below are not "canal is not Mongo-shaped",
// they are "canal cannot express a shard count that changes".
//
// FINDINGS INDEX. Each has a full argument at its own marker below.
//
//	BREAKAGE 1 (fatal)  one Source instance cannot emit for more than one lane, because
//	                    record.Batch.Add stamps Origin.Lane from the batch's unexported
//	                    Allocator and nothing exported rebinds it. Kills 1 -> 32.
//	BREAKAGE 2 (fatal)  the same Allocator fixes Origin.Stream per lane, so a single-cursor
//	                    multi-stream source (every transaction-log CDC source there is)
//	                    cannot stamp per-record stream identity.
//	BREAKAGE 3 (fatal)  connector.StateHandle.SetMany claims atomic multi-lane writes, but
//	                    each lane is a separate store.AssignmentID with its own lease epoch
//	                    and connector.Write carries no epoch, so the write cannot be fenced.
//	BREAKAGE 4 (major)  re-parallelising a live lane is inexpressible: LaneSpec.Spec is
//	                    write-once and must be authored BEFORE the gate opens, but the value
//	                    it must carry is only known AFTER it opens; and StateHandle.Set is
//	                    fenced on the target lane's epoch, so the outgoing holder cannot seed
//	                    the incoming lanes.
//	BREAKAGE 5 (major)  there is no protocol for retiring an UNBOUNDED lane. Ack.LaneFinished
//	                    and Batch.EndOfLane are both documented bounded-only, so nothing
//	                    tells a source "that lane is finished and durable, its keyspace is
//	                    yours now".
//	BREAKAGE 6 (major)  a secret that rotates under a running pipeline cannot be delivered.
//	                    Source and Sink are frozen, there is no Reconfigure, and no runtime
//	                    method re-reads config.
//	BREAKAGE 7 (major)  SourceCaps.MaxLanes is static per binary and HARD-ENFORCED at
//	                    Announce against a durable, shared lane plan, so a rolling upgrade
//	                    that narrows it fails the pipeline on re-announce.
//	BREAKAGE 8 (minor)  declarative caps cannot be declined for a configuration.
//	                    UpstreamRetention in particular is a config choice here and must be
//	                    over-declared as PrunesOnCommit for every deployment.
//	BREAKAGE 9 (minor)  no exported LaneID derivation, so cross-lane references must smuggle
//	                    an opaque LaneID through LaneSpec.Spec.
//	BREAKAGE 10 (fatal) Origin.Key and Origin.Upstream cannot be written by a source at all,
//	                    so SourceCaps.StableKeys — which registration DEMANDS be documented,
//	                    which SinkCaps.RequiresKey refuses a pipeline without, and which
//	                    Request.IdempotencyKey is derived from — is unhonourable. All three
//	                    idempotency layers and both dedupe layers are unreachable.
//
// This package imports only what a third-party connector may import: record, fault, schema,
// config, connector, registry. No engine, ledger, store, telemetry or spec.
package enterprise

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Format versions. Everything durable this connector authors is (version, bytes) and obeys
// the four-part contract: additive-only, zero means legacy, never reject a newer version
// unless the encoding genuinely cannot tolerate it, stamp at serialise time.
const (
	planV1   uint32 = 1
	cursorV1 uint32 = 1
	stageV1  uint32 = 1
)

// maxReadBatch is a courtesy bound; record.Batch has its own hard cap and Add returns nil
// at it.
const maxReadBatch = 512

// ---------------------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------------------

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*shardSource]{
		Meta: registry.Meta{
			Name:    "stress_shardlog",
			Version: "0.0.1",
			Title:   "Sharded change log (enterprise stress)",
			Summary: "A deliberately hostile source: many shards, many streams per shard, live re-sharding, rotating credentials.",
			Notes: "Origin.Key is the canonical encoding of (stream, primary key) as " +
				"len-prefixed UTF-8 segments, taken verbatim from the upstream row's declared " +
				"key columns in declaration order. It is stable across re-reads because the " +
				"upstream guarantees the key columns are immutable, and it is prefixed with the " +
				"stream so two tables with colliding integer keys cannot collide in the dedupe set.",
			Support: registry.SupportCommunity,
		},
		Spec: sourceSpec(),
		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.Bounded, connector.Unbounded},
			LaneKinds: []connector.LaneKind{
				connector.LaneKindScan, connector.LaneKindStream, connector.LaneKindBackfill,
			},

			// BREAKAGE 7 lives here. See the block at (*shardSource).announceGeneration.
			MaxLanes: 64,

			// BREAKAGE 8 lives here. See the block below sourceSpec.
			UpstreamRetention: connector.PrunesOnCommit,

			ReplayWindow:   6 * time.Hour,
			UnitAssignment: connector.UnitsDynamic,

			Discoverable:   true,
			Nackable:       true,
			ReportsBacklog: true,
			Heartbeats:     true,
			Validates:      true,
			Probes:         true,
			Choices:        true,
			AdoptsState:    true,

			ProducesEventTime:   true,
			ProducesChange:      true,
			ProducesSchema:      true,
			CompleteImages:      true,
			ComparablePositions: true,
			Replayable:          true,
			StableKeys:          true,
			MidLaneResume:       true,
		},
		New: newShardSource,
	})

	registry.AddSink(registry.Default, registry.SinkDef[*stageSink]{
		Meta: registry.Meta{
			Name:    "stress_staged",
			Version: "0.0.1",
			Title:   "Two-phase staged writer (enterprise stress)",
			Summary: "Stages request bodies, publishes them on Commit, and holds in-progress work across restarts.",
			Notes:   "Idempotency is the destination's: a staged artifact is named by its IdempotencyKey, so re-publishing one is a no-op.",
			Support: registry.SupportCommunity,
		},
		Spec: sinkSpec(),
		Caps: connector.SinkCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			MaxConcurrency:    8,
			MaxRequestRecords: 10000,
			MaxRequestBytes:   32 << 20,
			Idempotent:        true,
			PartialFailure:    true,
			Modes: []connector.DestMode{
				connector.DestAppend, connector.DestUpsert, connector.DestOverwrite,
			},
			RequiresCompleteImages: true,
			RequiresKey:            true,
			SchemaChanges: []schema.ChangeKind{
				schema.CreateStream, schema.AddField, schema.AlterNullability,
			},
			Flushes:       true,
			Partitions:    true,
			AppliesSchema: true,
			Commits:       true,
			KeepsState:    true,
			Prepares:      true,
			Validates:     true,
			Probes:        true,
			Choices:       true,
		},
		New: newStageSink,
	})
}

func sourceSpec() *config.Spec {
	return config.NewSpec().
		Describe(
			"Reads a sharded, ordered change log with an optional initial scan.",
			"Announces one scan lane per key chunk and one stream lane per shard. The shard count may change while running.",
		).
		Field(config.Field{
			Name:        "endpoint",
			Type:        config.TypeString,
			Description: "Address of the upstream's coordinator.",
			Short:       "host:port",
			Examples:    []any{"logs.internal:9443"},
		}).
		Field(config.Field{
			Name:        "auth_token",
			Type:        config.TypeString,
			Secret:      true,
			Optional:    true,
			Description: "Static bearer token. Redacted everywhere by the core. Prefer token_file, which can rotate under a running pipeline; see BREAKAGE 6.",
		}).
		Field(config.Field{
			Name:        "token_file",
			Type:        config.TypeString,
			Optional:    true,
			Description: "Path to a file containing the bearer token, re-read when its mtime changes. This is the only rotation path the interface set permits.",
		}).
		Field(config.Field{
			Name:        "streams",
			Type:        config.TypeArray,
			Description: "Logical streams (tables, collections, topics) carried by the log. All of them share one cursor per shard.",
			Item:        &config.Field{Name: "stream", Type: config.TypeString, Description: "One stream name.", Choices: "streams"},
		}).
		Field(config.Field{
			Name:        "shards",
			Type:        config.TypeInt,
			Default:     1,
			Optional:    true,
			Min:         floatPtr(1),
			Max:         floatPtr(32),
			Description: "Reader parallelism for the incremental phase. May be raised or lowered while the pipeline runs.",
		}).
		Field(config.Field{
			Name:        "scan_chunks",
			Type:        config.TypeInt,
			Default:     0,
			Optional:    true,
			Min:         floatPtr(0),
			Max:         floatPtr(32),
			Description: "How many bounded chunk lanes the initial scan is split into. Zero skips the scan entirely.",
		}).
		Field(config.Field{
			Name:        "commit_deletes_upstream",
			Type:        config.TypeBool,
			Default:     true,
			Optional:    true,
			Description: "Whether acting on a Commit prunes the upstream's retained log. This is a per-configuration fact that SourceCaps cannot express; see BREAKAGE 8.",
		}).
		Field(config.Fields.RateLimit("rate_limit")).
		Field(config.Fields.TLS("tls")).
		Example(config.Example{
			Title:       "Thirty-two shards, eight scan chunks",
			Description: "The enterprise-scale shape: a snapshot split eight ways, then a thirty-two-way tail.",
			Config: map[string]any{
				"endpoint":    "logs.internal:9443",
				"streams":     []any{"public.orders", "public.order_lines"},
				"shards":      32,
				"scan_chunks": 8,
			},
		})
}

func sinkSpec() *config.Spec {
	return config.NewSpec().
		Describe(
			"Stages encoded request bodies and publishes them on a two-phase commit.",
			"Every staged artifact is named by the engine's idempotency key, so a re-publish is a no-op at the destination.",
		).
		Field(config.Field{
			Name:        "root",
			Type:        config.TypeString,
			Description: "Directory or bucket prefix under which staged artifacts are written.",
		}).
		Field(config.Field{
			Name:        "max_record_bytes",
			Type:        config.TypeSize,
			Default:     "1MiB",
			Optional:    true,
			Description: "Records larger than this are rejected individually with PermanentMapping rather than failing the request.",
		}).
		Field(config.Field{
			Name:        "stage_ttl",
			Type:        config.TypeDuration,
			Default:     "1h",
			Optional:    true,
			Description: "How long a staged, unpublished artifact may sit before the core aborts it.",
		}).
		Example(config.Example{
			Title:  "Local staging directory",
			Config: map[string]any{"root": "/var/lib/canal/stage"},
		})
}

func floatPtr(f float64) *float64 { return &f }

/*
BREAKAGE 8 (minor) — a declarative capability cannot be declined for a configuration.

SIGNATURE THAT BLOCKS: connector.SourceCaps.UpstreamRetention Retention  (a struct field,
fixed at registry.AddSource time, with no method behind it)

This source has a config field commit_deletes_upstream. When it is true, acting on a Commit
advances the upstream's retained-log floor and the upstream discards data: that is
PrunesOnCommit, and canal MUST have flushed its own record of the position before calling
Commit. When it is false, the upstream keeps a fixed retention window regardless: that is
RetentionWindow, and commit ordering is a latency question rather than a correctness one.

Retention is declared once, per registered name, at init, before any config exists. So the
connector must declare the strictest value it could ever need — PrunesOnCommit — for every
deployment. The documented consequence, from StateHandle's own doc comment, is that Build
"refuses such a source against a deployment with no usable state store". So the 400-pipeline
laptop story ("zero external dependencies") is refused for the 380 pipelines that configured
commit_deletes_upstream: false and do not need a state store at all.

fault.ErrDeclined exists for exactly this shape of problem and cannot help: it is "legal only
from a method called during the negotiation window", and UpstreamRetention has no method. The
same argument applies verbatim to Replayable, StableKeys, CompleteImages,
ComparablePositions, MidLaneResume and MaxLanes, all of which are genuinely config-dependent
for a source of this shape.

The workaround a real author reaches for is registering the same Go type twice under two
names — "stress_shardlog" and "stress_shardlog_destructive" — which doubles the connector
catalogue, doubles the docs, and makes flipping one boolean a pipeline rewrite rather than an
edit.

SMALLEST FIX: an additive optional interface, resolved during the negotiation window exactly
where Validator already is.

	// CapsRefiner narrows a component's declared capabilities for one configuration.
	// The core takes the WEAKER of declared and refined, never the stronger, so a
	// connector cannot use it to claim more than it registered.
	type CapsRefiner interface {
	    RefineSourceCaps(ctx context.Context, declared SourceCaps) (SourceCaps, error)
	}

plus SourceCaps.RefinesCaps bool for the registration cross-check. Breaks no already-written
connector: absence of the interface means the declared caps stand, which is today's
behaviour. "Never stronger" keeps the submit-time refusal table honest.
*/

// ---------------------------------------------------------------------------------------
// Durable payloads this connector authors
// ---------------------------------------------------------------------------------------

// lanePlan is what goes in LaneSpec.Spec: the WRITE-ONCE construction payload for one lane.
//
// It is JSON because the four-part format contract wants additive-only encoding and a
// fixed-width struct cannot be additive. Every field is optional on read.
type lanePlan struct {
	Gen      uint64   `json:"gen"`
	Phase    string   `json:"phase"` // "scan" | "tail"
	Shard    int      `json:"shard"`
	ShardsOf int      `json:"of"`
	LoKey    string   `json:"lo,omitempty"`
	HiKey    string   `json:"hi,omitempty"`
	Streams  []string `json:"streams"`

	// Parent is the lane whose durable connector state seeds this one when the shard count
	// changes. It is an opaque record.LaneID carried verbatim because BREAKAGE 9 means this
	// connector cannot derive another lane's id from its name.
	Parent record.LaneID `json:"parent,omitempty"`

	// SeedFloor is the best lower bound available AT ANNOUNCE TIME, which is not the
	// parent's final cursor. See BREAKAGE 4.
	SeedFloor uint64 `json:"seed_floor,omitempty"`
}

func encodePlan(p lanePlan) (record.Blob, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return record.Blob{}, err
	}
	// Rule four: stamp the version at serialise time, not at construction time.
	return record.Blob{Version: planV1, Bytes: b}, nil
}

func decodePlan(b record.Blob) (lanePlan, error) {
	var p lanePlan
	if b.IsZero() {
		return p, nil
	}
	// Rule three: never reject a newer version when the encoding is additive. JSON is.
	if err := json.Unmarshal(b.Bytes, &p); err != nil {
		return p, fault.Contract(fault.OpOpen,
			fmt.Errorf("lane spec version %d is not decodable by build %d: %w", b.Version, planV1, err))
	}
	return p, nil
}

// encodeCursor renders an upstream log sequence as a Position's three coupled facets: the
// opaque Token, the order-preserving Order, and the monotone Scalar.
//
// A big-endian uint64 IS an order-preserving encoding, which is why Order and Token.Bytes are
// the same eight bytes here. Position's doc makes Order part of Token.Version's contract, so
// changing this would bump cursorV1.
func encodeCursor(seq uint64, at time.Time, safe bool) record.Position {
	order := make([]byte, 8)
	binary.BigEndian.PutUint64(order, seq)
	scalar := float64(seq)
	return record.Position{
		Token:  record.Blob{Version: cursorV1, Bytes: order},
		Order:  order,
		Scalar: &scalar,
		Safe:   safe,
		At:     at,
		Label:  fmt.Sprintf("log seq %d", seq),
	}
}

func decodeCursor(b record.Blob) (uint64, error) {
	if b.IsZero() {
		return 0, nil
	}
	if len(b.Bytes) < 8 {
		// Fixed-width: a genuinely unreadable encoding, so fail LOUDLY naming both versions
		// rather than guessing, which is rule three's stated exception.
		return 0, fault.Contract(fault.OpOpen,
			fmt.Errorf("cursor version %d is %d bytes; build %d needs 8", b.Version, len(b.Bytes), cursorV1))
	}
	return binary.BigEndian.Uint64(b.Bytes[:8]), nil
}

// canonicalKey is the documented Origin.Key derivation: length-prefixed UTF-8 segments,
// stream first. It is a function so the registered Notes describe one implementation.
func canonicalKey(stream record.StreamName, parts ...string) []byte {
	var out []byte
	write := func(s string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s)))
		out = append(out, n[:]...)
		out = append(out, s...)
	}
	write(string(stream))
	for _, p := range parts {
		write(p)
	}
	return out
}

// ---------------------------------------------------------------------------------------
// Credentials that rotate under a running pipeline
// ---------------------------------------------------------------------------------------

// credentials holds the bearer token in use, and knows how to notice that it changed.
//
// TWO ROTATION PATHS, ONE OF WHICH WORKS.
//
// The one that works is a projected file: k8s writes a new token into the same path, this
// connector notices the mtime change on its next use, re-reads, and carries on. No core
// involvement is needed and none is missing. That path is genuinely fine and is implemented
// below without complaint.
//
// The one that does not work is a secret held in canal's own config, which is the path
// config.Field.Secret documents and the path an operator will actually use. See BREAKAGE 6.
type credentials struct {
	mu        sync.RWMutex
	token     string
	file      string
	loadedAt  time.Time
	mtime     time.Time
	rotations uint64
}

func newCredentials(static, file string) *credentials {
	return &credentials{token: static, file: file}
}

// get returns the current token, re-reading the file when it has changed underneath.
//
// It is called on every upstream request rather than once at Open, because "re-authenticate
// on 401" is a strictly worse contract: it turns every rotation into at least one failed
// request, and a 401 is indistinguishable from a revoked grant.
func (c *credentials) get() (string, bool) {
	c.mu.RLock()
	f, cur := c.file, c.token
	c.mu.RUnlock()
	if f == "" {
		return cur, cur != ""
	}
	st, err := os.Stat(f)
	if err != nil {
		// A missing token file during rotation is transient by nature: the writer unlinks and
		// relinks. Keep using what we have and let the request classify its own failure.
		return cur, cur != ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if st.ModTime().Equal(c.mtime) {
		return c.token, c.token != ""
	}
	b, err := os.ReadFile(f)
	if err != nil {
		return c.token, c.token != ""
	}
	c.mtime = st.ModTime()
	c.loadedAt = time.Now()
	next := strings.TrimSpace(string(b))
	if next != c.token {
		c.token = next
		c.rotations++
	}
	return c.token, c.token != ""
}

func (c *credentials) rotationCount() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rotations
}

/*
BREAKAGE 6 (major) — a rotated secret cannot reach a running component.

SIGNATURES THAT BLOCK:

	type Source interface { Open; Read; Commit; Close }   // "FROZEN: no method will ever be added"
	type Sink   interface { Open; Write; Close }           // "FROZEN"
	SourceDef.New func(ctx context.Context, cfg *config.Config) (S, error)
	type SourceRuntime interface { Context; Lanes; State; Log; Metrics; Batcher; Note; Tenant; Pipeline; Node }

Compiler output for every hook a connector author would reach for first:

	rt.Config undefined (type connector.SourceRuntime has no field or method Config)
	rt.Secret undefined (type connector.SourceRuntime has no field or method Secret)
	rt.Reload undefined (type connector.SourceRuntime has no field or method Reload)

The config tree is handed to New once. There is no Configure callback — deliberately, and the
reasoning is sound — but the consequence is that a value inside *config.Config is frozen for
the component's whole life. When the operator's secret rotates, the only paths are:

 1. Bump the pipeline's config revision, which is a new generation: re-plan, re-claim every
    lane, re-Open, re-Build. For 400 pipelines with 90-minute credential TTLs that is roughly
    6,400 pipeline generations a day, each of which drops and re-takes leases and re-does the
    submit-time negotiation. Nothing is lost — the design's restart story is good — but this
    is a control-plane load and a lane-churn cost driven purely by an unchanged pipeline
    whose password changed.

 2. Mutate the map behind *config.Config in place. Config.Raw() does return the live map, so
    the core physically can. It is a data race — Config has no lock and Get walks the map
    while the mutator writes — and it is undocumented, so no connector may rely on it.

 3. Ignore the config field and demand a file path, as this connector does. That works, and
    it is what every mature connector ends up doing. But it means config.Field.Secret — the
    one flag the architecture credits with making redaction structural rather than a
    per-call-site discipline — is unusable for any credential that rotates, i.e. for every
    credential in a compliant enterprise deployment. The flag survives; its main use case
    does not.

There is also no way to REPORT a rotation as anything better than a free-text event, because
EventKind has no member for it and Note takes an Event. That part is minor.

SMALLEST FIX: one additive method on the runtimes, which is exactly the growth path the
design already names ("Adding a method here does not break a single connector, because the
core implements it and the connector only calls it").

	// Credentials returns the CURRENT value of a config field declared Secret, read
	// through the same resolver that produced it at construction. It exists because a
	// secret's value has a shorter lifetime than the component holding it.
	//
	// It is a pull, not a push: a callback cannot cross a process boundary, and a
	// connector that re-reads at the point of use cannot hold a stale value.
	Credential(path ...string) (string, bool)

Add it to SourceRuntime, SinkRuntime and CodecRuntime. Breaks no already-written connector:
they simply do not call it. Pairing it with a root-config `secret://` resolver that re-reads
gives rotation with no generation bump and no lane churn. A push-shaped alternative
(Reconfigure on the component) would require unfreezing Source and Sink and is therefore
strictly worse.
*/

// ---------------------------------------------------------------------------------------
// Rate limiting and quota
// ---------------------------------------------------------------------------------------

// bucket is a goroutine-free token bucket, shaped after connector.Batcher: pure policy that
// drops into a select loop rather than owning one.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rps float64, burst int) *bucket {
	if rps <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &bucket{rate: rps, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

// take reserves one token and reports how long the caller must wait first.
func (b *bucket) take() time.Duration {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

// tenantQuotas is a PROCESS-WIDE map, which is as wide as a connector can reach.
//
// A per-pipeline rate limit is expressible: it is a config field on the node, this connector
// reads it, and the limiter above enforces it. That part fits, though note that
// config.Fields.RateLimit is offered as a reusable field fragment with NO matching extractor
// — unlike Retry, Batching, Codec, TLS and Buffer — so every connector hand-rolls the two
// Get calls and they will drift. A (*Config).RateLimit() extractor returning a small
// config-owned policy struct would close that, additively.
//
// A per-TENANT quota across 400 pipelines is not expressible. Forty pods each hold a slice
// of a tenant's pipelines; each connector instance sees only its own config; nothing on
// SourceRuntime hands out a core-owned admission token:
//
//	rt.Limiter undefined (type connector.SourceRuntime has no field or method Limiter)
//
// This is NOT a breakage, and it is worth saying so explicitly rather than inflating the
// count: cluster-wide quota is a core responsibility, the core's growth path for it is a
// method on SourceRuntime, and adding one there breaks nothing. The map below is the correct
// connector-side scope — one process — and is honest about being that.
var tenantQuotas sync.Map // record.TenantID -> *bucket

// ---------------------------------------------------------------------------------------
// The upstream, modelled
// ---------------------------------------------------------------------------------------

// row is one upstream change. Note that it carries its OWN stream: one shard's log
// interleaves every stream, which is the shape BREAKAGE 2 is about.
type row struct {
	stream record.StreamName
	key    string
	upID   string
	seq    uint64
	at     time.Time
	op     record.Op
	before record.Value
	after  record.Value

	// handle is the upstream's own delivery handle, present only when the shard is served
	// through the queue-shaped API rather than the log-shaped one.
	handle []byte
}

// upstream is the vendor client, reduced to what the connector needs. It is an interface so
// the connector is unit-testable against a fake, and the fake below is what makes this
// package build and run with zero external dependencies — the standalone half of the case.
type upstream interface {
	// Shards reports the current shard count, which may change while running.
	Shards(ctx context.Context) (int, error)

	// Fetch returns rows strictly after from, plus the sequence after the last row and
	// whether the returned prefix ends on a transaction boundary.
	Fetch(ctx context.Context, token string, shard int, from uint64, max int) (rows []row, after uint64, safe bool, err error)

	// ScanChunk reads part of the existing state. done is true on the final page.
	ScanChunk(ctx context.Context, token string, lo, hi string, from uint64, max int) (rows []row, after uint64, done bool, err error)

	// Advance tells the upstream it may prune its retained log below seq. This is the
	// destructive commit that forces UpstreamRetention: PrunesOnCommit.
	Advance(ctx context.Context, token string, shard int, seq uint64) error

	// Lag reports how far behind a shard is, for BacklogReporter.
	Lag(ctx context.Context, shard int, from uint64) (records uint64, bytes uint64, newest time.Time, err error)

	// Keepalive holds a shard's server-side reader slot open while nothing is arriving.
	Keepalive(ctx context.Context, token string, shard int) error

	// Streams lists the streams the log carries, for Discoverer and ChoiceProvider.
	Streams(ctx context.Context, token string) ([]string, error)

	Close() error
}

// fake is a deterministic in-memory upstream. It exists so that this stress connector is a
// real, compiling, runnable connector rather than a sketch.
type fake struct {
	mu      sync.Mutex
	shards  int
	streams []string
	seq     map[int]uint64
	floor   map[int]uint64
}

func newFake(shards int, streams []string) *fake {
	return &fake{shards: shards, streams: streams, seq: map[int]uint64{}, floor: map[int]uint64{}}
}

func (f *fake) Shards(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shards, nil
}

func (f *fake) Streams(context.Context, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.streams...), nil
}

func (f *fake) Fetch(_ context.Context, token string, shard int, from uint64, max int) ([]row, uint64, bool, error) {
	if token == "" {
		return nil, from, false, fault.Permanent(fault.OpRead, errors.New("no credential"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if from < f.floor[shard] {
		// The upstream pruned past where we are asking. This is the failure that
		// SourceCaps.ReplayWindow exists to make a submit-time refusal instead.
		return nil, from, false, fault.Permanent(fault.OpRead,
			fmt.Errorf("shard %d retained log starts at %d; asked for %d", shard, f.floor[shard], from))
	}
	n := max
	if n > 8 {
		n = 8
	}
	out := make([]row, 0, n)
	at := time.Now()
	for i := 0; i < n; i++ {
		from++
		s := record.StreamName(f.streams[int(from)%len(f.streams)])
		out = append(out, row{
			stream: s,
			key:    fmt.Sprintf("%s-%d", s, from),
			upID:   fmt.Sprintf("%d:%d", shard, from),
			seq:    from,
			at:     at,
			op:     record.OpUpdate,
			after:  record.Map{"id": record.Uint(from), "shard": record.Int(int64(shard))},
		})
	}
	if from > f.seq[shard] {
		f.seq[shard] = from
	}
	// Only every other page ends on a transaction boundary, so Position.Safe is exercised.
	return out, from, from%2 == 0, nil
}

func (f *fake) ScanChunk(_ context.Context, token string, lo, hi string, from uint64, max int) ([]row, uint64, bool, error) {
	if token == "" {
		return nil, from, false, fault.Permanent(fault.OpRead, errors.New("no credential"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	const chunkRows = 24
	if from >= chunkRows {
		return nil, from, true, nil
	}
	n := uint64(max)
	if n > 8 {
		n = 8
	}
	if from+n > chunkRows {
		n = chunkRows - from
	}
	out := make([]row, 0, n)
	at := time.Now()
	for i := uint64(0); i < n; i++ {
		from++
		s := record.StreamName(f.streams[int(from)%len(f.streams)])
		out = append(out, row{
			stream: s,
			key:    fmt.Sprintf("%s-%s-%d", s, lo, from),
			upID:   fmt.Sprintf("scan:%s:%d", lo, from),
			seq:    from,
			at:     at,
			op:     record.OpScanRead,
			after:  record.Map{"id": record.Uint(from), "lo": record.String(lo), "hi": record.String(hi)},
		})
	}
	return out, from, from >= chunkRows, nil
}

func (f *fake) Advance(_ context.Context, token string, shard int, seq uint64) error {
	if token == "" {
		return fault.Permanent(fault.OpCommitSource, errors.New("no credential"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq > f.floor[shard] {
		f.floor[shard] = seq
	}
	return nil
}

func (f *fake) Lag(_ context.Context, shard int, from uint64) (uint64, uint64, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	head := f.seq[shard]
	if head < from {
		return 0, 0, time.Now(), nil
	}
	n := head - from
	return n, n * 256, time.Now(), nil
}

func (f *fake) Keepalive(_ context.Context, token string, _ int) error {
	if token == "" {
		return fault.Permanent(fault.OpUnknown, errors.New("no credential"))
	}
	return nil
}

func (f *fake) Close() error { return nil }

// ---------------------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------------------

// laneReader is one lane's private reader. There is one goroutine per lane, because the
// alternative — round-robining 32 lanes on the one goroutine Read is allowed to occupy —
// makes an idle tail lane block a hot scan lane.
type laneReader struct {
	id   record.LaneID
	plan lanePlan
	spec connector.LaneSpec

	// epoch is this worker's fencing token for the lane, from LaneAssignment.Epoch. The
	// connector never uses it to write; it is carried so the log and events can show it.
	epoch uint64

	cursor   atomic.Uint64
	finished atomic.Bool

	stop chan struct{}
	once sync.Once
}

func (l *laneReader) halt() { l.once.Do(func() { close(l.stop) }) }

// fetched is one lane's worth of upstream rows, ready to be turned into records.
type fetched struct {
	lane      record.LaneID
	reader    *laneReader
	rows      []row
	after     uint64
	safe      bool
	endOfLane bool
}

type shardSource struct {
	// Config, read once in New. New does no I/O.
	endpoint     string
	streams      []string
	wantShards   int
	scanChunks   int
	destructive  bool
	staticSecret string
	tokenFile    string
	rps          float64
	burst        int

	rt      connector.SourceRuntime
	lanes   connector.LaneCtl
	persist *connector.Persister
	creds   *credentials
	limiter *bucket
	up      upstream

	// gen is the re-sharding generation. It is bumped when the shard count changes and it
	// appears in every lane name, so a re-shard produces new lanes rather than mutating old
	// ones — LaneSpec.Name must be stable and LaneSpec.Spec is write-once.
	gen atomic.Uint64

	// One mutex, exactly as the concurrency contract promises: it guards only the state
	// shared between the read path and the progress path. Persister needs no protection of
	// its own because it is already safe for concurrent use.
	mu          sync.Mutex
	readers     map[record.LaneID]*laneReader
	lastErr     error
	lastErrLane record.LaneID

	ready    chan *fetched
	draining atomic.Bool
	wg       sync.WaitGroup
	closed   chan struct{}
	closeOne sync.Once

	// Metrics. The core owns naming, tagging and cardinality; this connector may only
	// register and observe.
	mRows      connector.Counter
	mFenced    connector.Counter
	mRotations connector.Counter
	mLag       connector.Gauge
	mFetch     connector.Histogram
}

func newShardSource(_ context.Context, c *config.Config) (*shardSource, error) {
	s := &shardSource{
		endpoint:    config.Must[string](c, "endpoint"),
		streams:     config.Must[[]string](c, "streams"),
		wantShards:  config.Must[int](c, "shards"),
		scanChunks:  config.Must[int](c, "scan_chunks"),
		destructive: config.Must[bool](c, "commit_deletes_upstream"),
		tokenFile:   optString(c, "token_file"),
		readers:     map[record.LaneID]*laneReader{},
		ready:       make(chan *fetched, 64),
		closed:      make(chan struct{}),
	}
	if c.Has("auth_token") {
		// The DISTINCT accessor, so a review can grep every read of a secret. It refuses a
		// field the spec did not mark Secret, which is why this cannot silently become a Get.
		tok, err := c.Secret("auth_token")
		if err != nil {
			return nil, err
		}
		s.staticSecret = tok
	}
	if c.Has("rate_limit") {
		sub, err := c.Object("rate_limit")
		if err != nil {
			return nil, err
		}
		s.rps, _ = config.Get[float64](sub, "requests_per_second")
		s.burst, _ = config.Get[int](sub, "burst")
	}
	s.creds = newCredentials(s.staticSecret, s.tokenFile)
	s.limiter = newBucket(s.rps, s.burst)
	if len(s.streams) == 0 {
		s.streams = []string{string(record.DefaultStream)}
	}
	return s, c.Err()
}

func optString(c *config.Config, path ...string) string {
	if !c.Has(path...) {
		return ""
	}
	v, _ := config.Get[string](c, path...)
	return v
}

// Open establishes the connection and reconstructs from whatever lanes this worker holds.
//
// It is idempotent because it is re-called with backoff after any method returns
// ErrNotConnected, and because "distribution is restart with a different subset" means it is
// also the re-entry point after every lease change.
func (s *shardSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt
	s.lanes = rt.Lanes()

	// AutoPersist over the interface. The core has already persisted the lane cursor before
	// Commit is called; this exists because THIS source needs a second, source-shaped write
	// (the upstream's prune floor) and wants its own encoding for the seed handoff.
	if s.persist == nil {
		s.persist = connector.AutoPersist(rt)
	}
	if s.up == nil {
		s.up = newFake(s.wantShards, s.streams)
	}
	if err := s.registerMetrics(); err != nil {
		return err
	}

	as, err := s.lanes.Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		// COLD START. The discriminator is data — "did Assigned return anything" — never a
		// nil position test.
		if err := s.announceGeneration(ctx, 1, s.wantShards, nil, 0); err != nil {
			return err
		}
		if as, err = s.lanes.Assigned(ctx); err != nil {
			return err
		}
	}

	if err := s.reconcile(ctx, as); err != nil {
		return err
	}

	// One watcher for the assigned set, one for the shard count. Both take the
	// COMPONENT-lifetime context, never ctx, which "may be cancelled the instant Open
	// returns".
	s.wg.Add(2)
	go s.watchAssignments(rt.Context())
	go s.watchShardCount(rt.Context())
	return nil
}

func (s *shardSource) registerMetrics() error {
	m := s.rt.Metrics()
	var err error
	if s.mRows == nil {
		if s.mRows, err = m.Counter("rows_read", "phase"); err != nil {
			return err
		}
	}
	if s.mFenced == nil {
		if s.mFenced, err = m.Counter("lanes_fenced"); err != nil {
			return err
		}
	}
	if s.mRotations == nil {
		if s.mRotations, err = m.Counter("credential_rotations"); err != nil {
			return err
		}
	}
	if s.mLag == nil {
		if s.mLag, err = m.Gauge("shard_lag_records", "shard"); err != nil {
			return err
		}
	}
	if s.mFetch == nil {
		buckets := []float64{0.001, 0.01, 0.1, 1, 10}
		if s.mFetch, err = m.Histogram("fetch_seconds", buckets, "phase"); err != nil {
			return err
		}
	}
	return nil
}

// reconcile brings the running reader set in line with what this worker holds. It is the
// whole of "workers dying and rejoining" on the connector side, and it fits: the interface
// gives Assigned, Changes and Revoked, and nothing more is needed.
func (s *shardSource) reconcile(ctx context.Context, as []connector.LaneAssignment) error {
	held := make(map[record.LaneID]struct{}, len(as))
	for i := range as {
		a := as[i]
		held[a.ID] = struct{}{}

		s.mu.Lock()
		_, running := s.readers[a.ID]
		s.mu.Unlock()
		if running {
			continue
		}

		plan, err := decodePlan(a.Spec.Spec)
		if err != nil {
			return err
		}
		start, err := decodeCursor(a.Cursor.Token)
		if err != nil {
			return err
		}
		if start == 0 {
			// No core cursor for this lane. Fall back to this connector's own durable copy,
			// then to the plan's seed floor. Never to "start from now": a cold cursor means
			// "no progress yet", which is a different fact.
			if b, ok, err := s.persist.Load(ctx, a.ID); err != nil {
				return err
			} else if ok {
				if start, err = decodeCursor(b); err != nil {
					return err
				}
			}
		}
		if start == 0 && plan.Parent != "" {
			seed, err := s.inheritFromParent(ctx, plan)
			if err != nil {
				return err
			}
			start = seed
		}
		if start < plan.SeedFloor {
			start = plan.SeedFloor
		}

		lr := &laneReader{id: a.ID, plan: plan, spec: a.Spec, epoch: a.Epoch, stop: make(chan struct{})}
		lr.cursor.Store(start)
		s.mu.Lock()
		s.readers[a.ID] = lr
		s.mu.Unlock()

		s.wg.Add(1)
		go s.runLane(s.rt.Context(), lr)

		s.rt.Note(connector.Event{
			At:      time.Now(),
			Kind:    connector.EventLaneAnnounced,
			Lane:    a.ID,
			Message: fmt.Sprintf("reading %s from log seq %d at epoch %d", plan.Phase, start, a.Epoch),
			Detail:  a.Spec.Label,
		})
	}

	// Stop readers for lanes we no longer hold.
	s.mu.Lock()
	var gone []*laneReader
	for id, lr := range s.readers {
		if _, ok := held[id]; !ok || s.lanes.Revoked(id) {
			gone = append(gone, lr)
			delete(s.readers, id)
		}
	}
	s.mu.Unlock()
	for _, lr := range gone {
		lr.halt()
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventLaneRevoked, Lane: lr.id,
			Message: "lease lost; stopped producing for this lane",
		})
	}
	return nil
}

func (s *shardSource) watchAssignments(ctx context.Context) {
	defer s.wg.Done()
	for {
		ch := s.lanes.Changes()
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case <-ch:
			as, err := s.lanes.Assigned(ctx)
			if err != nil {
				continue
			}
			if err := s.reconcile(ctx, as); err != nil {
				s.rt.Log().Error("reconcile failed", "err", err)
			}
		}
	}
}

// announceGeneration declares one generation's lanes: scanChunks bounded scan lanes in group
// scan-<gen>, then shards unbounded tail lanes in group tail-<gen> gated behind the scan.
//
// parents maps a new shard index to the lane whose keyspace it inherits, and floor is the
// best lower bound known at announce time. Both are empty for generation one.
func (s *shardSource) announceGeneration(ctx context.Context, gen uint64, shards int, parents map[int]record.LaneID, floor uint64) error {
	s.gen.Store(gen)
	scanGroup := record.LaneGroup(fmt.Sprintf("scan-%d", gen))
	tailGroup := record.LaneGroup(fmt.Sprintf("tail-%d", gen))

	// The scan lanes. Bounded, prefix-ordered, one per key chunk. Only generation one scans:
	// a re-shard of a live tail must not re-snapshot.
	if gen == 1 {
		for i := 0; i < s.scanChunks; i++ {
			lo, hi := chunkBounds(i, s.scanChunks)
			plan := lanePlan{
				Gen: gen, Phase: "scan", Shard: i, ShardsOf: s.scanChunks,
				LoKey: lo, HiKey: hi, Streams: s.streams,
			}
			blob, err := encodePlan(plan)
			if err != nil {
				return fault.Bug(fault.OpOpen, err)
			}
			if _, err := s.lanes.Announce(ctx, connector.LaneSpec{
				// Name is derived from stable content properties — the generation and the key
				// range — never from an ephemeral handle, so the same name across restarts is
				// the same lane and reuses its persisted state.
				Name:        fmt.Sprintf("g%d/scan/%s-%s", gen, lo, hi),
				Stream:      record.StreamName(s.streams[0]), // see BREAKAGE 2
				Kind:        connector.LaneKindScan,
				Ordering:    connector.OrderingPrefix,
				Boundedness: connector.Bounded,
				Group:       scanGroup,
				Spec:        blob,
				Weight:      24,
				Budget:      4000, // a scan wants a bigger in-flight budget than a tail
				Label:       fmt.Sprintf("scan chunk %d/%d: key in [%q,%q)", i+1, s.scanChunks, lo, hi),
			}); err != nil {
				return err
			}
		}
	}

	// The tail lanes. Unbounded, prefix-ordered, one per shard, gated behind this
	// generation's scan AND behind the previous generation's tail when re-sharding.
	after := []record.LaneGroup{}
	if gen == 1 && s.scanChunks > 0 {
		after = append(after, scanGroup)
	}
	if gen > 1 {
		after = append(after, record.LaneGroup(fmt.Sprintf("tail-%d", gen-1)))
	}

	for i := 0; i < shards; i++ {
		plan := lanePlan{
			Gen: gen, Phase: "tail", Shard: i, ShardsOf: shards,
			Streams: s.streams, Parent: parents[i], SeedFloor: floor,
		}
		blob, err := encodePlan(plan)
		if err != nil {
			return fault.Bug(fault.OpOpen, err)
		}
		if _, err := s.lanes.Announce(ctx, connector.LaneSpec{
			Name:        fmt.Sprintf("g%d/tail/%d-of-%d", gen, i, shards),
			Stream:      record.StreamName(s.streams[0]), // see BREAKAGE 2
			Kind:        connector.LaneKindStream,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Unbounded,
			Group:       tailGroup,
			StartAfter:  after,
			Spec:        blob,
			Label:       fmt.Sprintf("changelog tail, shard %d/%d (generation %d)", i, shards, gen),
		}); err != nil {
			return err
		}
	}
	return nil
}

func chunkBounds(i, n int) (string, string) {
	if n <= 1 {
		return "", ""
	}
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	step := len(alphabet) / n
	lo := string(alphabet[i*step])
	if i == n-1 {
		return lo, ""
	}
	return lo, string(alphabet[(i+1)*step])
}

/*
BREAKAGE 7 (major) — a rolling upgrade that narrows MaxLanes fails the pipeline.

SIGNATURES THAT BLOCK:

	SourceCaps.MaxLanes int  // "HARD-ENFORCED at announce time: exceeding it fails the pipeline"
	LaneCtl.Announce(ctx context.Context, spec LaneSpec) (record.LaneID, error)
	// "returns a fault.PermanentContract when the announcement would exceed SourceCaps.MaxLanes"

MaxLanes is a compile-time constant of the BINARY. The lane plan is DURABLE STATE shared by
every binary version that touches the pipeline. During a rolling upgrade both facts are live
at once, and Announce is the only place they meet.

Concretely, with 40 pods draining and restarting one at a time:

  - v2 raises MaxLanes from 8 to 64 and this source announces 32 tail lanes. The rows are
    durable. Fine.
  - a v1 pod that has not been replaced yet — or a v2 pod that gets ROLLED BACK, which is the
    case an upgrade plan must actually survive — claims some of those lanes, calls Open, and
    re-announces. Announce is documented idempotent on Name, so the intent is clearly that
    re-announcing an existing lane is free. But the MaxLanes check is described as a check on
    "announced lanes", and there are 32 of them against a cap of 8. If the check counts the
    durable lane set, the ninth call returns PermanentContract, and PermanentContract STOPS
    THE PIPELINE. One rolled-back pod takes down a pipeline whose data path was healthy.
  - the reverse — v2 NARROWS the cap because 32 readers turned out to hammer the upstream —
    is worse: every already-planned lane beyond the new cap is now un-re-announceable, so the
    pipeline cannot start on the new binary at all and cannot be resumed by the old one
    either once the old artefact is gone.

Nothing durable records which cap the plan was made under. store.Assignment.Generation is the
CONFIG revision, not the binary's capability set, and store.WorkerInfo.Version records a
worker's version but is not consulted by Announce.

This is not hypothetical bookkeeping: MaxLanes is precisely the knob an operator turns during
an upgrade to change reader parallelism, so an upgrade is exactly when it is most likely to
differ between two running binaries.

SMALLEST FIX, and it is entirely core-side with zero connector impact: make Announce's
MaxLanes check count only lanes this call CREATES.

	Announce enforces SourceCaps.MaxLanes against the number of lanes this pipeline would
	hold AFTER the call. An idempotent re-announce of an existing lane with an identical
	Spec never trips it, even when the durable lane count already exceeds the cap; the
	core instead raises a degraded condition with reason lane_cap_exceeded and names both
	numbers.

That turns a pipeline-stopping contract violation into a visible, non-fatal disclosure, which
is what a rolling upgrade needs. Breaks no already-written connector: the only behaviour that
changes is a refusal becoming a warning for a lane that already exists.
*/

// inheritFromParent reads the parent lane's durable connector state so a re-sharded lane
// resumes where its predecessor stopped.
//
// This is BREAKAGE 4's workaround, and it is the ugliest code in this file.
func (s *shardSource) inheritFromParent(ctx context.Context, plan lanePlan) (uint64, error) {
	// StateHandle.Get takes an arbitrary record.LaneID and is NOT epoch-fenced, so reading
	// another lane's blob compiles and works. StateHandle's own doc calls itself
	// "lane-scoped", so this is reaching outside the documented scope of the handle.
	b, _, err := s.rt.State().Get(ctx, plan.Parent)
	if err != nil {
		return 0, err
	}
	if b.IsZero() {
		// The parent never wrote a token, so we cannot tell "the parent made no progress"
		// from "the parent's progress lives only in the core's own lane cursor, which this
		// interface does not expose". Refuse rather than silently restarting a shard from
		// the seed floor and re-reading, or worse, from zero.
		return 0, fault.Contract(fault.OpOpen, fmt.Errorf(
			"lane %s inherits from %s, which has no connector-authored cursor; "+
				"canal's own cursor for that lane is not readable through StateHandle",
			plan.Phase, plan.Parent))
	}
	return decodeCursor(b)
}

// watchShardCount performs the 1 -> 32 rescale. It is the case's centrepiece and it does not
// fit.
func (s *shardSource) watchShardCount(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case <-t.C:
			n, err := s.up.Shards(ctx)
			if err != nil {
				continue
			}
			if err := s.rescale(ctx, n); err != nil {
				s.rt.Log().Error("rescale failed", "err", err, "shards", n)
			}
		}
	}
}

func (s *shardSource) rescale(ctx context.Context, shards int) error {
	gen := s.gen.Load()
	s.mu.Lock()
	var current []*laneReader
	for _, lr := range s.readers {
		if lr.plan.Phase == "tail" && lr.plan.Gen == gen {
			current = append(current, lr)
		}
	}
	s.mu.Unlock()
	if len(current) == 0 || len(current) == shards {
		return nil
	}
	sort.Slice(current, func(i, j int) bool { return current[i].plan.Shard < current[j].plan.Shard })

	// Step 1: stop producing for the outgoing lanes so their prefix can settle.
	for _, lr := range current {
		lr.halt()
	}

	// Step 2: work out what the incoming lanes inherit. THIS IS WHERE IT BREAKS.
	//
	// The value each incoming lane needs is its parent's FINAL committed cursor. We do not
	// have it: the readers have only just been told to stop, their in-flight records have not
	// settled, and Commit for the last of them has not been called. The best available number
	// is the last cursor this process observed, which is an upper bound on what is durable —
	// exactly the wrong direction. Taking the minimum across parents turns it into a lower
	// bound and therefore into re-reads, which at-least-once tolerates but which for a
	// destructive upstream means asking for log the upstream may already have pruned.
	floor := ^uint64(0)
	parents := map[int]record.LaneID{}
	for i := 0; i < shards; i++ {
		src := current[i%len(current)]
		parents[i] = src.id
		if c := src.cursor.Load(); c < floor {
			floor = c
		}
	}
	if floor == ^uint64(0) {
		floor = 0
	}

	if err := s.announceGeneration(ctx, gen+1, shards, parents, floor); err != nil {
		return err
	}

	// Step 3: retire the outgoing lanes. See BREAKAGE 5: for an UNBOUNDED lane there is no
	// signal back that this completed, so the gate on tail-<gen> either opens on a fact we
	// cannot observe or never opens at all.
	for _, lr := range current {
		if err := s.lanes.Finish(ctx, lr.id); err != nil {
			return err
		}
	}
	s.rt.Note(connector.Event{
		At:       time.Now(),
		Kind:     connector.EventNote,
		Severity: fault.TransientInternal,
		Message:  fmt.Sprintf("re-sharding %d -> %d readers as generation %d", len(current), shards, gen+1),
		Detail:   fmt.Sprintf("incoming lanes seeded from log seq %d, which is a LOWER bound on what settled", floor),
	})
	return nil
}

/*
BREAKAGE 4 (major) — re-parallelising a live lane is inexpressible.

SIGNATURES THAT BLOCK:

	LaneSpec.Spec record.Blob
	    "the write-once opaque payload the source needs to CONSTRUCT this lane"
	LaneCtl.Announce(ctx, spec LaneSpec) (record.LaneID, error)
	    "re-announcing an existing lane with a DIFFERENT Spec returns a
	     fault.PermanentContract"
	LaneSpec.StartAfter []record.LaneGroup
	    "names lane groups that must be FINISHED AND DURABLE before this lane may be
	     assigned or read"
	StateHandle.Set(ctx, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error)
	    "writes if ... the caller still holds the lane's epoch"

Scaling one reader to thirty-two means handing one lane's keyspace to thirty-two new lanes,
each of which must begin exactly where the old one durably stopped. The interface set gets
tantalisingly close and then contradicts itself.

StartAfter is the right primitive and it works: announce the thirty-two incoming lanes gated
on the outgoing lane's group, and the core will not assign or read them until that group is
finished and durable, cluster-wide, from the durable lane table. Nothing else in the surveyed
field has this and it is genuinely good.

But the payload that tells an incoming lane where to start is LaneSpec.Spec, and Spec is
WRITE-ONCE. It must be authored at Announce time — which is BEFORE the gate opens, by
construction, since the gate is what Announce establishes. The value it must carry is the
outgoing lane's final durable cursor, which is only knowable AFTER the gate opens. The field
whose lifetime is "write once at construction" is being asked to carry a value whose lifetime
is "known only at destruction". That is the exact dual-representation trap LaneSpec's own doc
comment congratulates itself on avoiding, arriving from the other direction.

Every workaround fails on inspection:

 1. Re-announce the incoming lane with a corrected Spec once the parent finishes. Explicitly
    PermanentContract: "silently rewriting a lane's construction payload is how a resume
    lands at the wrong place".

 2. Seed the incoming lane's StateHandle instead of its Spec. This is what a connector author
    genuinely tries first, and Set is fenced on the TARGET lane's epoch. The outgoing lane's
    holder does not hold the incoming lanes' epochs — those lanes are gated, hence unassigned,
    hence have no epoch and no holder at all. So the only process that knows the value cannot
    write it, and the only process that may write it does not exist yet. Fenced writes are
    fault.ErrFenced, which correctly revokes the lane rather than the pipeline, so this
    doesn't even fail loudly: it fails as a revocation.

 3. Have the incoming lane READ the parent's state at Open, which is what inheritFromParent
    above does. Get is not epoch-fenced, so it compiles and it works. Three problems. It
    reaches outside StateHandle's documented lane scope. It only sees what the PARENT chose
    to write, so it requires the parent to have used Persister even where it had no other
    need to — a source whose progress lives entirely in canal's cursor has nothing to inherit,
    because LaneAssignment.Cursor is readable only for lanes you hold. And nobody ever deletes
    the parent's blob, so a pipeline that rescales weekly accumulates dead lane state forever
    with no owner and no reaper.

 4. Give up on inheritance and make the incoming lanes deterministic from a static boundary.
    Works for scan-then-stream, which is why that case looks fine. Does not work for
    rescaling a live tail, because the boundary IS the progress.

SMALLEST FIX: one additive method on LaneCtl, which is injected and therefore free to grow.

	// Seed writes the initial durable cursor for a lane that has been announced but never
	// assigned. It is fenced on the CALLER's lane — the caller must hold a lane whose
	// group the target's StartAfter names — and it is refused once the target has been
	// assigned even once, so it can never overwrite live progress.
	//
	// It exists because re-parallelising a lane means transferring a position between
	// lanes, and the only process that knows the position is the one giving it up.
	Seed(ctx context.Context, target record.LaneID, cursor record.Position) error

Compiler output today:

	l.Seed undefined (type connector.LaneCtl has no field or method Seed)

Breaks no already-written connector: LaneCtl is core-implemented, so adding a method breaks
nothing, and a source that never rescales never calls it. The "refused once assigned" clause
is what keeps it from being a back door into another worker's progress.
*/

/*
BREAKAGE 5 (major) — an unbounded lane cannot be retired observably.

SIGNATURES THAT BLOCK:

	Ack.LaneFinished bool
	    "true on the final ack for a BOUNDED lane"
	record.Batch.EndOfLane bool
	    "set by the source on the final batch of a BOUNDED lane"
	LaneCtl.Finish(ctx, id record.LaneID) error
	    "requests retirement ... this is a request, not an assertion"

Finish is a request whose completion is unobservable for an unbounded lane. The two signals
that could report it are both scoped to bounded lanes by their own documentation, and there is
no third:

	a.LaneRetired undefined (type connector.Ack has no field or method LaneRetired)

For the rescale above this matters twice. The source cannot know when it is safe to consider
generation N's keyspace handed over, so it cannot log it, cannot report it, and cannot
sequence anything after it. And a source with a DESTRUCTIVE upstream cannot know when it may
release the outgoing shard's server-side reader slot — release it too early and the upstream
prunes log the incoming lanes have not read; hold it forever and the slot leaks, once per
rescale, which is the exact failure Heartbeater exists to prevent for the idle case.

There is a second, quieter question with no answer in the interface at all: is Finish even
LEGAL on a lane declared Unbounded? Boundedness says "Unbounded means the lane tails forever",
which reads as a prohibition, and Batch.EndOfLane's contract says a source sets it only for a
bounded lane, so a source cannot signal the end through the batch either. Yet StartAfter's
gate is defined on "finished and durable", so gating a new generation on an old one REQUIRES
finishing an unbounded lane. Two parts of the same design need opposite answers and the
interface does not say which wins. A connector author reading only the doc comments cannot
determine whether this rescale is legal, and that ambiguity is itself the defect.

SMALLEST FIX: two words of contract and one field.

  - state on LaneCtl.Finish that it is legal for a lane of either Boundedness, and that for
    an unbounded lane the source signals nothing through the batch;
  - widen Ack.LaneFinished's contract from "bounded" to "any lane whose Finish has completed
    and become durable", and say so on the field.

Breaks no already-written connector: a source that never calls Finish on an unbounded lane
never sees the new ack, and a bounded-lane source sees exactly today's behaviour.
*/

// runLane is one lane's fetch loop. One goroutine per lane, feeding the shared ready channel.
func (s *shardSource) runLane(ctx context.Context, lr *laneReader) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lr.stop:
			return
		case <-s.closed:
			return
		default:
		}
		if s.draining.Load() {
			return
		}
		if lr.finished.Load() {
			return
		}

		// The per-pipeline rate limit, enforced at the point of use. The tenant bucket is
		// process-wide, which is as wide as a connector can reach; see tenantQuotas.
		if d := s.limiter.take(); d > 0 {
			if !sleepCtx(ctx, lr.stop, d) {
				return
			}
		}
		if tb, ok := tenantQuotas.Load(s.rt.Tenant()); ok {
			if d := tb.(*bucket).take(); d > 0 {
				if !sleepCtx(ctx, lr.stop, d) {
					return
				}
			}
		}

		token, ok := s.creds.get()
		if !ok {
			// No usable credential. NotConnected asks the engine to call Open again with
			// backoff rather than dead-lettering data, which is right: this is control flow.
			s.fail(lr, fault.New(fault.NotConnected, fault.OpRead,
				errors.New("no usable credential; waiting for rotation")))
			if !sleepCtx(ctx, lr.stop, time.Second) {
				return
			}
			continue
		}
		if n := s.creds.rotationCount(); n > 0 && s.mRotations != nil {
			s.mRotations.Add(0) // counters are monotone; the real add happens on transition
		}

		from := lr.cursor.Load()
		started := time.Now()
		var (
			rows  []row
			after uint64
			safe  bool
			done  bool
			err   error
		)
		switch lr.plan.Phase {
		case "scan":
			rows, after, done, err = s.up.ScanChunk(ctx, token, lr.plan.LoKey, lr.plan.HiKey, from, maxReadBatch)
			safe = true // every scan page boundary is a legal resume point
		default:
			rows, after, safe, err = s.up.Fetch(ctx, token, lr.plan.Shard, from, maxReadBatch)
		}
		if s.mFetch != nil {
			s.mFetch.Observe(time.Since(started).Seconds(), lr.plan.Phase)
		}
		if err != nil {
			s.fail(lr, err)
			if !sleepCtx(ctx, lr.stop, 500*time.Millisecond) {
				return
			}
			continue
		}
		if len(rows) == 0 && !done {
			if !sleepCtx(ctx, lr.stop, 100*time.Millisecond) {
				return
			}
			continue
		}

		f := &fetched{lane: lr.id, reader: lr, rows: rows, after: after, safe: safe, endOfLane: done}
		select {
		case s.ready <- f:
			lr.cursor.Store(after)
			if done {
				lr.finished.Store(true)
				return
			}
		case <-ctx.Done():
			return
		case <-lr.stop:
			return
		case <-s.closed:
			return
		}
	}
}

func sleepCtx(ctx context.Context, stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	}
}

// fail records a lane-scoped failure. A source cannot report an outcome through the runtime —
// Note is best-effort by contract and "a component reports outcomes through its RETURN VALUE"
// — so this parks the error for the next Read to return.
func (s *shardSource) fail(lr *laneReader, err error) {
	s.mu.Lock()
	s.lastErr = err
	s.lastErrLane = lr.id
	s.mu.Unlock()
}

// Read hands the engine one lane's worth of records.
//
// CANCELLATION MEANS DRAIN, NOT ABORT: on a cancelled context the lane goroutines stop
// fetching, everything already fetched is still delivered, and ctx.Err() comes only when
// nothing is left. Records already produced into dst are never discarded on an error path,
// because the engine admits what is in the batch BEFORE handling the error.
func (s *shardSource) Read(ctx context.Context, dst *record.Batch) error {
	dst.Reset()

	f, err := s.take(ctx)
	if f == nil {
		return err
	}

	// ==================================================================================
	// BREAKAGE 1 and BREAKAGE 2 both bite on the next twelve lines. Full argument below.
	// ==================================================================================

	// The doc for Read says "Read fills dst and sets dst.Lane". So:
	dst.Lane = f.lane

	for i := range f.rows {
		r := dst.Add()
		if r == nil {
			// At the batch's hard cap. Push the remainder back and stop; the position must
			// describe only what we actually produced.
			f.rows = f.rows[i:]
			f.after = f.rows[0].seq - 1
			f.endOfLane = false
			s.pushBack(f)
			break
		}
		row := f.rows[i]

		// Dest is a public field, so ROUTING is expressible per record.
		r.Dest = row.stream

		// IDENTITY is not. What is needed here, and cannot be written:
		//
		//	r.SetStream(row.stream)   -- r.SetStream undefined (type *record.Record has no
		//	                             field or method SetStream)
		//
		// Origin().Stream is whatever LaneSpec.Stream said, for every row in the lane.

		r.EventTime = row.at
		r.Payload = record.StructPayload(row.after)
		r.Change = &record.Change{
			Version:        record.ChangeVersion,
			Op:             row.op,
			Keys:           [][]string{{"id"}},
			BeforeComplete: record.CompletenessAbsent,
			AfterComplete:  record.CompletenessComplete,
			CommitTime:     row.at,
		}
		if err := r.Meta.Set(record.NSSource, "shard", record.Int(int64(f.reader.plan.Shard))); err != nil {
			return fault.Bug(fault.OpRead, err)
		}

		// BREAKAGE 10, REPAIRED. This connector declares SourceCaps.StableKeys and registration
		// made it document the derivation in Notes; canonicalKey IS that derivation, and it now
		// has somewhere to go. The engine's dedupe, Ref.Key and Request.IdempotencyKey all read
		// what these two lines write.
		r.SetKey(canonicalKey(row.stream, row.key))
		r.SetUpstream([]byte(row.upID))
	}

	if s.mRows != nil {
		s.mRows.Add(float64(dst.Len()), f.reader.plan.Phase)
	}

	// The position after the last record, for a prefix lane. Seq is NOT set here: the core
	// assigns it at admission and overwrites anything a connector puts there.
	dst.Position = encodeCursor(f.after, time.Now(), f.safe)
	if f.endOfLane {
		// Legal only because scan lanes are declared Bounded. A tail lane may never set this,
		// which is the other half of BREAKAGE 5.
		dst.EndOfLane = true
	}
	return nil
}

// take blocks for one lane's fetched rows, honouring the drain-not-abort contract.
func (s *shardSource) take(ctx context.Context) (*fetched, error) {
	// Anything already buffered goes out first, even on a cancelled context.
	select {
	case f := <-s.ready:
		return f, nil
	default:
	}

	s.mu.Lock()
	err, lane := s.lastErr, s.lastErrLane
	s.lastErr, s.lastErrLane = nil, ""
	s.mu.Unlock()
	if err != nil {
		s.rt.Log().Warn("lane read failed", "lane", string(lane), "err", err)
		return nil, err
	}

	if s.allFinished() {
		return nil, fault.ErrEndOfInput
	}

	select {
	case f := <-s.ready:
		return f, nil
	case <-ctx.Done():
		// Stop retrieving new records, then hand back whatever is already buffered.
		s.draining.Store(true)
		select {
		case f := <-s.ready:
			return f, nil
		default:
			return nil, ctx.Err()
		}
	case <-s.closed:
		return nil, fault.ErrEndOfInput
	}
}

func (s *shardSource) pushBack(f *fetched) {
	select {
	case s.ready <- f:
	default:
		// The channel is full, so the remainder would be dropped. Rewind the lane's cursor
		// instead and let the reader re-fetch: duplicates are permitted, gaps are not.
		f.reader.cursor.Store(f.after)
	}
}

func (s *shardSource) allFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.readers) == 0 {
		return false
	}
	for _, lr := range s.readers {
		if !lr.finished.Load() {
			return false
		}
		if lr.plan.Phase == "tail" {
			return false // an unbounded lane is never finished
		}
	}
	return len(s.ready) == 0
}

/*
BREAKAGE 1 (fatal) — one Source instance cannot emit records for more than one lane.

SIGNATURES THAT BLOCK:

	Source.Read(ctx context.Context, dst *record.Batch) error
	    "Read fills dst and sets dst.Lane"
	    "CONCURRENCY: Read is never called concurrently with itself."
	record.Batch{ Records []*Record; Lane LaneID; Position Position; alloc *Allocator ... }
	func (b *Batch) Add() *Record
	    origin.Lane is stamped from b.alloc.lane, NOT from b.Lane
	func record.NewAllocator(t, p, n, l LaneID, stream StreamName, firstID, firstGroup) *Allocator
	    "The engine creates one Allocator per (lane, generation)"

Batch.Lane is a public field. Origin.Lane is stamped by Add from the batch's UNEXPORTED
allocator. Assigning dst.Lane — which Read's own documentation instructs the source to do —
therefore changes the batch's claim about its lane and changes nothing about the records in
it. A worker holding lanes L1..L32 and one Source produces, for every lane, records whose
Origin().Lane is L1.

Origin.Lane is not decorative. record.Ref carries it, so every per-record sink outcome is
attributed to the wrong lane. Ack.Handles is keyed per lane, so a discrete-ordering lane
settles the wrong upstream deliveries. fault.Fault.Lane, the dead-letter envelope's
provenance, and the per-lane metric labels are all wrong. And Batch.Position, which the ledger
resolves into a committed cursor, describes L7 while the records it covers claim L1 — so the
prefix for L1 advances past positions it never contained.

Compiler output for every rebind an author would try:

	dst.SetLane undefined (type *record.Batch has no field or method SetLane)
	dst.Rebind undefined (type *record.Batch has no field or method Rebind)
	dst.alloc undefined (cannot refer to unexported field alloc)
	r.SetLane undefined (type *record.Record has no field or method SetLane)
	r.SetOrigin undefined (type *record.Record has no field or method SetOrigin)

record.NewAllocator and record.NewBatch are exported, so a source CAN mint correctly-stamped
records for any lane it likes. It cannot deliver them: Read must fill the engine's dst, and
the only route in is dst.Records = append(dst.Records, mine...), which record.Batcher.Flush
itself does. Doing that hands the engine records whose RecordIDs came from a private allocator
starting at zero — colliding with the engine's generation-local ids, which every per-record
outcome, retry target, dead-letter route and dedupe entry is keyed on — and whose Origin.Group
belongs to a settlement group the engine never opened, so the group's reference count is
wrong and it resolves early. Early group resolution is a committed cursor past unwritten data.
That is not a workaround a real author would ship, and if they shipped it, it silently
corrupts settlement.

There is a second reading of the interface in which the engine allocates one Batch PER LANE
and calls Read once per lane, the source discovering the lane from dst.Lane on entry. Two
things say so: NewBatch sets b.Lane from a.lane, and the architecture says one Allocator per
(lane, generation). Three things say otherwise: Read's doc tells the SOURCE to set dst.Lane,
Batch's own doc says "The engine allocates one per node and passes the same pointer every
iteration", and Reset's doc says "a source reading one lane per call sets Lane once". Those
cannot all be true.

And the per-lane reading does not rescue the case anyway. Read is never called concurrently
with itself and "blocks until at least one record is available". With 32 lanes on one worker
and one serialised Read, a call for an idle tail lane blocks the 31 hot lanes behind it. The
only escape is for the source to return an empty batch immediately for any lane that has
nothing — which the contract does not permit, and which would make Read a busy-poll across 32
lanes.

So: the sole in-tree example connector declares MaxLanes: 1, and that is not a coincidence.
Nothing in the design's own examples exercises the multi-lane path, and the multi-lane path is
the horizontal-scaling story.

SMALLEST FIX. Read and Batch are both load-bearing, and Source is frozen, so the fix has to be
on Batch — which is not an interface and can grow a method without breaking an implementation.

	// Retarget rebinds this batch to another of the source's lanes, opening a new
	// settlement group. It is legal only for a lane the caller holds, the engine
	// pre-registers one Allocator per held lane, and it is how a source holding many
	// lanes produces for each of them through one Read.
	//
	// Records already added to the batch are flushed to the engine first, so a batch
	// never contains two lanes' records.
	func (b *Batch) Retarget(lane LaneID, stream StreamName) error

Concretely: the engine constructs the batch with a map of (lane -> *Allocator) for the lanes
this instance holds, populated at Open and updated on every assignment change; Retarget swaps
b.alloc and calls b.Reset. Zero new interfaces, no unfreezing, and the change is invisible to
a single-lane source, which never calls it.

The alternative fix — declare that the engine runs one Source INSTANCE per lane — is much
bigger than it looks and I do not recommend it: LaneCtl.Announce is a method on the running
source, so it must be answered "which instance announces the lanes?", and Assigned returning
a SLICE stops making sense. It also multiplies connection pools by 32.

BREAKAGE 2 (fatal) — the same Allocator fixes Origin.Stream, so a single-cursor multi-stream
source cannot stamp stream identity.

SIGNATURES THAT BLOCK:

	record.NewAllocator(..., stream StreamName, ...)   // one stream per allocator
	connector.LaneSpec.Stream record.StreamName        // one stream per lane
	func (b *Batch) Add() *Record                      // origin.Stream = a.stream; Dest = a.stream

	r.SetStream undefined (type *record.Record has no field or method SetStream)

One lane is one stream, structurally. But a transaction log is ONE ordered resource with ONE
cursor carrying EVERY stream interleaved. That is not a niche shape — it is the shape of every
log-based CDC source, of every append-only audit stream, of every multi-tenant event bus
topic, and of the shard API this connector wraps. The two options are both wrong:

  - one lane per shard, LaneSpec.Stream set to an arbitrary member, r.Dest set per record (what
    this connector does above). Routing is correct because Ref() carries Dest. Identity is not:
    Origin.Stream says "public.orders" for every row of "public.order_lines". The dedupe key is
    documented as (tenant, pipeline, source-node, stream, layer, identity); store.DedupeKey
    takes a stream parameter and no engine code fills it in yet, so which field it receives is
    still open. If it receives Origin.Stream — the identity field, which is the one a dedupe
    key should key on — then 200 tables collapse into one dedupe namespace, which is design
    rule R5's own bug arriving through a different door. Per-stream progress reporting,
    per-stream drift attribution and the per-stream metric label are wrong either way, because
    those read identity and not routing.

  - one lane per (shard, stream), i.e. 32 x 200 = 6400 lanes. Each lane must then read the
    whole shard log and discard 199/200 of it, so read amplification is 200x, and MaxLanes
    would have to be 6400. Every lane's Position is the same LSN, so 200 lanes hold 200 copies
    of one cursor and the prefix for each advances only when all 200 have settled. This is
    strictly worse than the first option and it is not viable at all.

SMALLEST FIX: let a source stamp the stream on a record it just created, bounded to the set
the lane declared.

	// SetStream stamps this record's stream identity. It is legal only from the source that
	// produced the record, before it returns from Read — the same window as SetHandle — and
	// only for a stream the lane declared through LaneSpec.Streams.
	//
	// It exists because one ordered upstream cursor can carry many logical streams, and a
	// source that cannot say which stream a record belongs to cannot be deduplicated,
	// attributed or reported per stream.
	func (r *Record) SetStream(s StreamName)

plus LaneSpec gaining `Streams []record.StreamName` alongside the existing singular Stream
(additive; the singular field stays as the default and the common case never sets the plural).
The "only from the source, before Read returns" rule is already enforced for SetHandle, so the
enforcement machinery exists. A transform still cannot touch it, so the KIP-793 property this
design is proud of — a transform structurally cannot corrupt settlement identity — is
preserved: this widens the SOURCE's window, not a transform's.

Breaks no already-written connector: a single-stream source never calls SetStream and gets
today's behaviour from LaneSpec.Stream.

BREAKAGE 10 (fatal) — no source can populate Origin.Key or Origin.Upstream, so
SourceCaps.StableKeys cannot be honoured by anyone.

SIGNATURES THAT BLOCK:

	record.Origin struct { ... Key []byte; Upstream []byte ... }   // fields of an UNEXPORTED
	record.Record struct { origin Origin ... }                     // struct field
	func (r *Record) Origin() Origin                               // returns a COPY
	// the only exported mutators on Record are SetHandle, MarkFailed and the Dest field

Compiler output for every route:

	r.SetKey undefined (type *record.Record has no field or method SetKey)
	r.SetUpstream undefined (type *record.Record has no field or method SetUpstream)
	cannot assign to r.Origin().Key (neither addressable nor a map index expression)
	r.SetOrigin undefined (type *record.Record has no field or method SetOrigin)

Batch.Add stamps origin with tenant, pipeline, node, lane, stream, group, id, root and ReadAt.
It does not stamp Key or Upstream, and it cannot: they are per-record facts only the source
knows. Add returns the record with both fields nil, and nothing exported can ever change that.
Origin's doc comment says Key "May be nil", so nil is legal — but it is the ONLY reachable
value.

The blast radius is the whole idempotency story, and every part of it is asserted elsewhere in
the interface set as if it worked:

  - SourceCaps.StableKeys is documented as "Origin.Key is populated and stable across
    re-reads", and registry.AddSource PANICS if it is declared with empty Meta.Notes:
    "document how Origin.Key is derived". So the registry compels a connector to document a
    derivation it is structurally prevented from performing. This connector does exactly that
    above — the Notes are real, canonicalKey implements them, and the result has nowhere to go.
  - SinkCaps.RequiresKey — "every record must carry Origin.Key" — refuses a pipeline whose
    source does not declare StableKeys. This connector's sink declares RequiresKey and its
    source declares StableKeys, and registration accepted both without a warning. The refusal
    table therefore certifies a pipeline in which every record's key is nil.
  - Request.IdempotencyKey is "present when the source declares StableKeys". It is engine-
    derived from a field that is always nil, so idempotency layer three is derived from
    nothing.
  - spec.DedupeLayer has exactly two members: DedupeUpstream keys on Origin.Upstream, DedupeKey
    keys on Origin.Key. Both fields are unwritable, so BOTH dedupe layers are unreachable and
    the engine-owned dedupe — with its required window, its atomic trim and its whole
    design-rule-R5 fix — has no key to work with.
  - Guarantee.EffectivelyOnce is defined as "AtLeastOnce plus an idempotent sink plus stable
    keys", so the tier is unreachable, and ExactlyOnce via Committer inherits the problem for
    any destination that dedupes on key rather than on artifact name.
  - record.Ref.Key, the field a sink reports outcomes against and would use for an upsert, is
    always nil, so DestUpsert against a generic sink cannot work either.

Meta is not a workaround. It is a separate namespace by design, filtered by the stage-standard
meta filter, invisible to Ref, and unknown to the engine's dedupe and idempotency paths. Using
it means every sink must learn one connector's Meta key, which is the N-times-M coupling the
Request design exists to eliminate.

This is the same root cause as BREAKAGES 1 and 2 — Origin is stamped wholesale by the
Allocator and has no per-record source write window — but it is worth listing separately
because it is the one with the largest declared surface depending on it, and because unlike
Lane and Stream it has no partially-correct fallback: there is no public Key field the way
there is a public Dest field.

SMALLEST FIX: the same window SetHandle already has, which proves the enforcement machinery
exists.

	// SetKey stamps this record's stable identity: the canonical encoding of the thing the
	// record is about. Legal only from the source that produced the record, before it
	// returns from Read — the same window as SetHandle — and required from a source
	// declaring SourceCaps.StableKeys.
	func (r *Record) SetKey(k []byte)

	// SetUpstream stamps the vendor's own id, idempotency layer one. Same window.
	func (r *Record) SetUpstream(u []byte)

Breaks no already-written connector: a source with no natural identity never calls either, and
Key stays nil exactly as today. The conformance kit should then assert that a source declaring
StableKeys actually produces non-nil keys, which today it cannot even check.
*/

// Commit acts on settled progress. For this source that means advancing the upstream's prune
// floor — a DESTRUCTIVE act, which is why UpstreamRetention is PrunesOnCommit and why the core
// has already flushed its own record of the position before we get here.
//
// It runs on the core's single control goroutine, never concurrently with itself, and may run
// concurrently with Read.
func (s *shardSource) Commit(ctx context.Context, a connector.Ack) error {
	s.mu.Lock()
	lr := s.readers[a.Lane]
	s.mu.Unlock()
	if lr == nil {
		// Commit is never called for a lane whose lease we lost, so this is a lane we retired
		// ourselves. Nothing to advance.
		return nil
	}

	if a.Abandoned > 0 {
		// The core surfaces the number and the source chooses. A destructive commit must not
		// prune log covering records that were dead-lettered or dropped, so refuse to advance
		// and let the operator see it.
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventDegraded, Lane: a.Lane,
			Severity: fault.PermanentMapping,
			Message: fmt.Sprintf("not advancing the upstream prune floor: %d of %d records in this ack were abandoned",
				a.Abandoned, a.Records),
			Detail: "the retained log covering them is kept so another consumer or a replay can reach them",
		})
		return nil
	}

	seq, err := decodeCursor(a.Through.Token)
	if err != nil {
		return err
	}
	if seq == 0 {
		return nil
	}

	if s.destructive {
		token, ok := s.creds.get()
		if !ok {
			// Escalated, not logged and dropped: the engine classifies, retries per policy,
			// and raises commit_failed if it cannot succeed.
			return fault.New(fault.NotConnected, fault.OpCommitSource,
				errors.New("no usable credential to advance the upstream prune floor"))
		}
		if err := s.up.Advance(ctx, token, lr.plan.Shard, seq); err != nil {
			return err
		}
	}

	// The second, source-shaped durable write: this connector's own copy of the cursor, which
	// is what a re-sharded successor lane inherits. Persister honours the stored CAS version,
	// so a fenced worker's write fails rather than overwriting the new holder's progress.
	if err := s.persist.Commit(ctx, a); err != nil {
		if errors.Is(err, fault.ErrFenced) {
			// A fence revokes the LANE, not the pipeline. Stop this reader and let the
			// assignment watcher pick up the new truth.
			if s.mFenced != nil {
				s.mFenced.Add(1)
			}
			lr.halt()
			s.mu.Lock()
			delete(s.readers, a.Lane)
			s.mu.Unlock()
			s.rt.Note(connector.Event{
				At: time.Now(), Kind: connector.EventLaneRevoked, Lane: a.Lane,
				Severity: fault.Fenced,
				Message:  fmt.Sprintf("epoch %d is stale for this lane; another worker holds it", a.Epoch),
			})
			return nil
		}
		return err
	}

	if a.LaneFinished {
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventLaneFinished, Lane: a.Lane,
			Message: fmt.Sprintf("lane complete at log seq %d", seq),
		})
	}
	return nil
}

/*
BREAKAGE 3 (fatal) — a multi-lane atomic write cannot be fenced, so StateHandle.SetMany is
unusable for its stated purpose.

SIGNATURES THAT BLOCK:

	StateHandle.SetMany(ctx context.Context, w map[record.LaneID]Write) error
	    "writes several lanes atomically — all or nothing across the whole map"
	    "Atomicity here is not a nicety."
	type Write struct { Blob record.Blob; IfVersion uint64 }
	store.Batch struct { Epoch uint64; Writes map[string]Versioned; Deletes []Key }
	    "Epoch fences every write in this batch"
	    "The store rejects the WHOLE batch if the epoch is stale for any lane it touches"
	store.AssignmentID
	    "identifies one assignable unit: exactly ONE LANE of one pipeline generation"
	store.Coordinator.Claim(ctx, a AssignmentID, w WorkerID, ttl) (Lease, error)
	store.Lease struct { Assignment AssignmentID; Worker WorkerID; Epoch uint64; Expires time.Time }

Assignment is per lane. A lease is per assignment. An epoch is per lease. Therefore a worker
holding 32 lanes holds 32 leases with 32 DISTINCT epochs.

store.Batch carries exactly one Epoch for the whole atomic write, and it is required to fence
"every write in this batch" with per-key epoch checking. With two lanes at epochs 3 and 7,
there is no value of Batch.Epoch that passes both checks. So an atomic write spanning more
than one lane is either unfencible or impossible, and the interface asserts it is both atomic
AND fenced.

connector.Write, the per-lane entry of SetMany's map, is where the missing value would go:

	unknown field Epoch in struct literal of type connector.Write

This is not a store-implementation detail leaking into the connector surface — it is
observable entirely from the connector surface, because SetMany's whole reason to exist is
"several lanes atomically", and a source that calls it for two lanes cannot be told which of
the two fenced it. Set, the single-lane sibling, is fine and this connector uses it via
Persister.

The same contradiction hits canal's own checkpoint even harder, and that is worth stating even
though it is core-side, because it is the enterprise deployment story:

	store.CheckpointKey(t, p) — "There is exactly ONE per pipeline"
	StoreCaps.AtomicMultiKey — "required for any tier above at-least-once: a checkpoint is
	    one record spanning lane cursors, the schema epoch, the pending committables and
	    the dedupe additions, and a partial write of it is unrecoverable"

One pipeline's 32 lanes are spread over up to 32 workers. No worker can author that one record
— none of them can see the other lanes' cursors — and all of them CAS-contend on one key.
Guarantees above at-least-once are declared to require exactly the write that cannot be
constructed in the deployment the design is for. Sink-side, connector.Committer inherits it:
PrepareCommit(id uint64)'s contract is "ids strictly increase and a higher id SUBSUMES every
lower one", and Opening.Restored is a single *uint64, but 32 workers each running a sink
instance for one pipeline produce 32 interleaved id sequences against one destination, so
"subsumes every lower one" is false at the destination and AbortStale cannot tell another
worker's live committable from its own stale one.

SMALLEST FIX for the connector-visible half: move the epoch to where the lane is.

	type Write struct {
	    Blob      record.Blob
	    IfVersion uint64

	    // Epoch fences this ENTRY. Zero means "use the epoch of the lease the caller holds
	    // for this lane", which is what a single-lane write has always meant.
	    Epoch uint64
	}

and in store, replace Batch.Epoch with per-Versioned epochs (or add
`Epochs map[record.LaneID]uint64` and deprecate the scalar). Breaks no already-written
connector: zero preserves today's meaning exactly, and every existing call site passes zero
because the field does not exist yet.

The checkpoint half needs a core decision I should not make from a connector, but the shape is
forced: either the checkpoint key is per (pipeline, worker) or per (pipeline, lane-group) with
an aggregating read, or checkpoint ids are minted by the coordinator so they are globally
monotone. The connector-facing consequence of the second option is nil; of the first, also
nil. So this is fixable without touching Sink, Committer or Opening — which is worth knowing
before anyone reaches for a Committer redesign.
*/

// Close releases everything. Called EXACTLY ONCE, ALWAYS — including after a failed Open and
// including when Open was never called at all, which is why every field is nil-checked.
func (s *shardSource) Close(ctx context.Context) error {
	s.closeOne.Do(func() { close(s.closed) })
	s.mu.Lock()
	for _, lr := range s.readers {
		lr.halt()
	}
	s.readers = map[record.LaneID]*laneReader{}
	s.mu.Unlock()

	// Bound the wait by the grace period the fresh context carries. Every network call in
	// Close must have a timeout, and so must every join.
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	if s.up != nil {
		return s.up.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------------------
// Optional source interfaces. Every one of these fits without complaint.
// ---------------------------------------------------------------------------------------

// Discover enumerates what this source can read, before a pipeline runs.
func (s *shardSource) Discover(ctx context.Context) (connector.Catalog, error) {
	token, ok := s.creds.get()
	if !ok {
		return connector.Catalog{}, fault.Permanent(fault.OpDiscover, errors.New("no usable credential"))
	}
	up := s.up
	if up == nil {
		up = newFake(s.wantShards, s.streams)
	}
	names, err := up.Streams(ctx, token)
	if err != nil {
		return connector.Catalog{}, err
	}
	cat := connector.Catalog{At: time.Now(), Streams: make([]connector.StreamDesc, 0, len(names))}
	for _, n := range names {
		cat.Streams = append(cat.Streams, connector.StreamDesc{
			Name: record.StreamName(n),
			Schema: &schema.Schema{
				Fields: []schema.Field{
					{Name: "id", Type: schema.TypeInt64},
					{Name: "shard", Type: schema.TypeInt64, Nullable: true},
				},
				Keys: [][]string{{"id"}},
				Open: true,
			},
			Keys:      [][]string{{"id"}},
			KeysFixed: true,
			Supports: []connector.LaneKind{
				connector.LaneKindScan, connector.LaneKindStream, connector.LaneKindBackfill,
			},
			Label: n,
		})
	}
	return cat, nil
}

// Nack observes terminal failures in the SOURCE's own vocabulary — a position for a prefix
// lane, never a record.RecordID, which this source has never seen.
func (s *shardSource) Nack(_ context.Context, lane record.LaneID, ns []connector.Nack) error {
	for i := range ns {
		seq, err := decodeCursor(ns[i].Position.Token)
		if err != nil {
			continue
		}
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventNote, Lane: lane, Severity: ns[i].Class,
			Message: fmt.Sprintf("upstream notified: log seq %d abandoned after %d attempts", seq, ns[i].Attempts),
			Detail:  ns[i].Reason,
		})
	}
	return nil
}

// Backlog answers "how much is left", as an estimate with an as-of time.
func (s *shardSource) Backlog(ctx context.Context, lane record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	lr := s.readers[lane]
	s.mu.Unlock()
	if lr == nil {
		return connector.Backlog{}, fault.ErrFenced
	}
	recs, bytes, newest, err := s.up.Lag(ctx, lr.plan.Shard, lr.cursor.Load())
	if err != nil {
		return connector.Backlog{}, err
	}
	if s.mLag != nil {
		s.mLag.Set(float64(recs), fmt.Sprint(lr.plan.Shard))
	}
	b := connector.Backlog{Records: connector.Count(recs), Bytes: connector.Count(bytes), Exact: false, AsOf: time.Now()}
	if !newest.IsZero() {
		// Nil rather than zero when there is no event time: zero reads as "caught up".
		lag := newest.Sub(time.Unix(0, 0))
		if recs == 0 {
			lag = 0
		}
		b.EventTimeLag = &lag
	}
	return b, nil
}

// Heartbeat keeps an idle shard's server-side reader slot alive so a pruning upstream does not
// pin its own retention. It carries no position and cannot advance a cursor.
func (s *shardSource) Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error {
	s.mu.Lock()
	lr := s.readers[lane]
	s.mu.Unlock()
	if lr == nil {
		return nil
	}
	token, ok := s.creds.get()
	if !ok {
		return fault.New(fault.NotConnected, fault.OpUnknown, errors.New("no usable credential"))
	}
	if err := s.up.Keepalive(ctx, token, lr.plan.Shard); err != nil {
		return err
	}
	if idle > 5*time.Minute {
		s.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventNote, Lane: lane,
			Message: fmt.Sprintf("shard %d idle for %s; holding the reader slot", lr.plan.Shard, idle.Round(time.Second)),
		})
	}
	return nil
}

// Validate is tier two: it may do I/O and returns ALL per-field diagnostics at once.
func (s *shardSource) Validate(ctx context.Context) config.Diagnostics {
	var d config.Diagnostics
	if _, ok := s.creds.get(); !ok {
		d = d.Errorf(config.CodeAuthFailed, []string{"auth_token"},
			"no usable credential: auth_token is empty and token_file is unset or unreadable",
			"set auth_token, or point token_file at a projected credential file")
	}
	up := s.up
	if up == nil {
		up = newFake(s.wantShards, s.streams)
	}
	n, err := up.Shards(ctx)
	if err != nil {
		d = d.Errorf(config.CodeUnreachable, []string{"endpoint"},
			"could not reach the upstream coordinator: "+err.Error(),
			"check the address and that this pod's egress policy permits it")
		return d
	}
	if s.wantShards > n {
		d = d.Warnf(config.CodeOutOfRange, []string{"shards"},
			fmt.Sprintf("shards is %d but the upstream currently has %d", s.wantShards, n),
			"the extra readers will idle until the upstream grows")
	}
	token, _ := s.creds.get()
	known, err := up.Streams(ctx, token)
	if err == nil {
		have := map[string]bool{}
		for _, k := range known {
			have[k] = true
		}
		for i, want := range s.streams {
			if !have[want] {
				d = d.Errorf(config.CodeNotFound, []string{"streams", fmt.Sprintf("[%d]", i)},
					fmt.Sprintf("stream %q does not exist upstream", want),
					"pick one of: "+strings.Join(known, ", "))
			}
		}
	}
	return d
}

// Probe is a cheap liveness check returning a LIST of named results, because "the endpoint
// answered" and "I can actually read the log" are different facts.
func (s *shardSource) Probe(ctx context.Context) connector.ProbeResults {
	out := connector.ProbeResults{}
	if _, ok := s.creds.get(); ok {
		out = append(out, connector.ProbeResult{Label: "credential present"})
	} else {
		out = append(out, connector.ProbeFailed("credential present", errors.New("no usable token"))...)
	}
	up := s.up
	if up == nil {
		up = newFake(s.wantShards, s.streams)
	}
	if _, err := up.Shards(ctx); err != nil {
		out = append(out, connector.ProbeFailed("coordinator reachable", err)...)
	} else {
		out = append(out, connector.ProbeResult{Label: "coordinator reachable"})
	}
	token, _ := s.creds.get()
	if _, _, _, err := up.Fetch(ctx, token, 0, 0, 1); err != nil {
		out = append(out, connector.ProbeFailed("shard 0 readable", err)...)
	} else {
		out = append(out, connector.ProbeResult{Label: "shard 0 readable"})
	}
	return out
}

// Choices backs config.Field.Choices. A named hook rather than a live callback, because a
// callback cannot cross a process boundary.
func (s *shardSource) Choices(ctx context.Context, hook string, partial *config.Config) ([]config.EnumValue, error) {
	switch hook {
	case "streams":
		token := s.staticSecret
		if partial != nil && partial.Has("auth_token") {
			if t, err := partial.Secret("auth_token"); err == nil {
				token = t
			}
		}
		up := s.up
		if up == nil {
			up = newFake(1, s.streams)
		}
		names, err := up.Streams(ctx, token)
		if err != nil {
			return nil, err
		}
		out := make([]config.EnumValue, 0, len(names))
		for _, n := range names {
			out = append(out, config.EnumValue{Value: n, Title: n})
		}
		return out, nil
	default:
		return nil, fault.Contract(fault.OpValidate, fmt.Errorf("unknown choices hook %q", hook))
	}
}

// AdoptsStateOf lets a rename be a declaration rather than an operator runbook.
func (s *shardSource) AdoptsStateOf() []string { return []string{"stress_shardlog_v0"} }

// ---------------------------------------------------------------------------------------
// The sink: two-phase commit, staged artifacts, restart-resumable
// ---------------------------------------------------------------------------------------

// staged is one request that has been written but not published.
type staged struct {
	key      string
	lanes    []record.LaneID
	firstRec record.RecordID
	lastRec  record.RecordID
	records  int64
	bytes    int64
	flushed  bool
}

// stageHandle is the Committable payload. Connector-authored and versioned, like everything
// that crosses a role boundary or hits disk.
type stageHandle struct {
	Key      string          `json:"key"`
	Lanes    []record.LaneID `json:"lanes"`
	Records  int64           `json:"records"`
	FirstRec record.RecordID `json:"first_rec"`
	LastRec  record.RecordID `json:"last_rec"`
}

type stageSink struct {
	root        string
	maxRecBytes int64
	stageTTL    time.Duration

	rt        connector.SinkRuntime
	guarantee connector.Guarantee
	modes     map[record.StreamName]connector.DestMode

	mu        sync.Mutex
	pending   []staged // written, not yet flushed
	durable   []staged // flushed, not yet published
	published map[string]bool
	restored  *uint64
	lastPrep  uint64

	mWritten connector.Counter
	mStaged  connector.Gauge
}

func newStageSink(_ context.Context, c *config.Config) (*stageSink, error) {
	k := &stageSink{
		root:      config.Must[string](c, "root"),
		modes:     map[record.StreamName]connector.DestMode{},
		published: map[string]bool{},
	}
	if raw, ok := c.Raw()["max_record_bytes"]; ok {
		n, err := config.ParseSize(raw)
		if err != nil {
			return nil, err
		}
		k.maxRecBytes = n
	} else {
		k.maxRecBytes = 1 << 20
	}
	d, err := config.Get[time.Duration](c, "stage_ttl")
	if err == nil {
		k.stageTTL = d
	} else {
		k.stageTTL = time.Hour
	}
	return k, c.Err()
}

// Open receives what it needs to create or alter the destination BEFORE the first record that
// needs it, plus the tier the core computed.
func (k *stageSink) Open(ctx context.Context, rt connector.SinkRuntime, o connector.Opening) error {
	k.rt = rt
	k.guarantee = o.Guarantee
	k.restored = o.Restored

	for _, cs := range o.Streams {
		k.modes[cs.Stream] = cs.Mode
		if cs.Mode == connector.DestUpsert && len(cs.Keys) == 0 {
			// A sink MAY assert on what it was handed rather than write wrong data.
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"stream %q is configured for upsert with no keys", cs.Stream))
		}
	}
	if o.Guarantee < connector.EffectivelyOnce {
		rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventDowngrade,
			Message: fmt.Sprintf("staging writer running at %s; its two-phase commit can support exactly_once", o.Guarantee),
		})
	}

	var err error
	if k.mWritten, err = rt.Metrics().Counter("records_staged", "stream"); err != nil {
		return err
	}
	if k.mStaged, err = rt.Metrics().Gauge("unpublished_artifacts"); err != nil {
		return err
	}
	if err := os.MkdirAll(k.root, 0o750); err != nil {
		return fault.Transient(fault.OpOpen, err)
	}
	return nil
}

// Write delivers one already-encoded, already-framed, already-compressed request. The sink
// implements TRANSPORT ONLY.
//
// All four quadrants of the success/failure shape are honoured: a clean result means every
// record is durable-or-staged, a non-empty Failed names exactly what did not land, and an
// error with Failed empty claims nothing.
func (k *stageSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if req.Count == 0 {
		return connector.AllWritten(0), nil
	}

	// Per-record rejection: a record too large for the destination is PermanentMapping, which
	// dead-letters that record and leaves the pipeline healthy.
	var failed []fault.RecordFault
	perRec := int64(0)
	if req.Count > 0 {
		perRec = int64(req.UncompressedBytes) / int64(req.Count)
	}
	if k.maxRecBytes > 0 && perRec > k.maxRecBytes {
		for i := range req.Records {
			failed = append(failed, fault.RecordFault{
				Record: req.Records[i].ID,
				Class:  fault.PermanentMapping,
				Op:     fault.OpWrite,
				User: fmt.Sprintf("record is about %d bytes; this destination accepts at most %d",
					perRec, k.maxRecBytes),
				Dev: "estimated from Request.UncompressedBytes / Request.Count",
			})
		}
		// Nothing landed, and we can say exactly which records. Written must equal
		// Count - len(Failed); the core CHECKS it.
		return connector.WriteResult{Failed: failed, Written: 0}, nil
	}

	key := req.IdempotencyKey
	if key == "" {
		// No stable source key, so no server-side idempotency. Fall back to content identity
		// so a retry of the same request at least does not stage twice.
		key = fmt.Sprintf("%s/%d/%d", req.Partition, req.Records[0].ID, req.Count)
	}

	k.mu.Lock()
	if k.published[key] {
		// Already durably stored. A duplicate counts as SUCCESS — that is the whole point of
		// an idempotent write — and is reported separately so the rate is visible.
		dups := make([]record.RecordID, 0, len(req.Records))
		for i := range req.Records {
			dups = append(dups, req.Records[i].ID)
		}
		k.mu.Unlock()
		return connector.WriteResult{
			Duplicates: dups,
			Written:    int64(req.Count),
			Bytes:      int64(len(req.Body)),
			DestToken:  key,
		}, nil
	}
	st := staged{
		key:      key,
		lanes:    lanesOf(req.Records),
		firstRec: req.Records[0].ID,
		lastRec:  req.Records[len(req.Records)-1].ID,
		records:  int64(req.Count),
		bytes:    int64(len(req.Body)),
	}
	k.pending = append(k.pending, st)
	k.mu.Unlock()

	if err := os.WriteFile(k.stagePath(key), req.Body, 0o640); err != nil {
		// The bytes may or may not have landed. Indeterminate is the honest class; the engine
		// retries it because this sink declares Idempotent.
		return connector.WriteResult{}, fault.Unknown(fault.OpWrite, err)
	}
	if k.mWritten != nil {
		stream := record.DefaultStream
		if len(req.Records) > 0 {
			stream = req.Records[0].Stream
		}
		k.mWritten.Add(float64(req.Count), string(stream))
	}

	// NOT durable yet: this sink declares Flusher, so the core settles on the Flush that
	// covers these records, not here.
	return connector.WriteResult{Written: int64(req.Count), Bytes: int64(len(req.Body)), DestToken: key}, nil
}

func (k *stageSink) stagePath(key string) string {
	safe := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(key)
	return k.root + string(os.PathSeparator) + safe + ".stage"
}

func lanesOf(refs []record.Ref) []record.LaneID {
	seen := map[record.LaneID]struct{}{}
	var out []record.LaneID
	for i := range refs {
		if _, ok := seen[refs[i].Lane]; ok {
			continue
		}
		seen[refs[i].Lane] = struct{}{}
		out = append(out, refs[i].Lane)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Flush makes every request written since the previous successful Flush durable. A partial
// flush names what did not make it, keyed on record.RecordID.
func (k *stageSink) Flush(_ context.Context, reason connector.FlushReason) (connector.WriteResult, error) {
	k.mu.Lock()
	batch := k.pending
	k.pending = nil
	k.mu.Unlock()

	var (
		res    connector.WriteResult
		failed []fault.RecordFault
		kept   []staged
	)
	for _, st := range batch {
		f, err := os.Open(k.stagePath(st.key))
		if err != nil {
			failed = append(failed, fault.RecordFault{
				Record: st.firstRec, Class: fault.TransientUpstream, Op: fault.OpFlush,
				User: "staged artifact " + st.key + " could not be reopened for sync",
				Dev:  err.Error(),
			})
			continue
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil {
			failed = append(failed, fault.RecordFault{
				Record: st.firstRec, Class: fault.Indeterminate, Op: fault.OpFlush,
				User: "staged artifact " + st.key + " may not be durable",
			})
			continue
		}
		st.flushed = true
		kept = append(kept, st)
		res.Written += st.records
		res.Bytes += st.bytes
	}

	k.mu.Lock()
	k.durable = append(k.durable, kept...)
	if k.mStaged != nil {
		k.mStaged.Set(float64(len(k.durable)))
	}
	k.mu.Unlock()

	if reason == connector.FlushEndOfInput {
		if err := os.WriteFile(k.root+string(os.PathSeparator)+"_MANIFEST", []byte("complete\n"), 0o640); err != nil {
			return res, fault.Transient(fault.OpFlush, err)
		}
	}
	res.Failed = failed
	if len(failed) > 0 {
		return res, fault.Transient(fault.OpFlush, errors.New("some staged artifacts did not sync"))
	}
	return res, nil
}

// PrepareCommit mints the committables for checkpoint id. Every one names the lanes and the
// record range it covers, so a failed commit can be dead-lettered with the records named.
func (k *stageSink) PrepareCommit(_ context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
	id := p.ID
	k.mu.Lock()
	defer k.mu.Unlock()

	// The subsuming contract: ids strictly increase and a higher id subsumes every lower one.
	// Enforcing it locally is all a sink instance can do; see BREAKAGE 3 for why that is not
	// enough across 32 workers of one pipeline.
	if id <= k.lastPrep {
		return nil, fault.Contract(fault.OpCommitSink, fmt.Errorf(
			"checkpoint id %d does not exceed the last prepared id %d", id, k.lastPrep))
	}
	k.lastPrep = id

	out := make([]connector.Committable, 0, len(k.durable))
	for _, st := range k.durable {
		h, err := json.Marshal(stageHandle{
			Key: st.key, Lanes: st.lanes, Records: st.records,
			FirstRec: st.firstRec, LastRec: st.lastRec,
		})
		if err != nil {
			return nil, fault.Bug(fault.OpCommitSink, err)
		}
		out = append(out, connector.Committable{
			Checkpoint: id,
			Handle:     record.Blob{Version: stageV1, Bytes: h},
			Lanes:      st.lanes,
			FirstRec:   st.firstRec,
			LastRec:    st.lastRec,
			Records:    st.records,
			Expires:    time.Now().Add(k.stageTTL),
		})
	}
	return out, nil
}

// Commit publishes everything up to and including the checkpoint the committables name. It is
// idempotent and signals per item.
func (k *stageSink) Commit(_ context.Context, cs []connector.Committable) ([]connector.CommitOutcome, error) {
	out := make([]connector.CommitOutcome, 0, len(cs))
	for _, c := range cs {
		var h stageHandle
		if err := json.Unmarshal(c.Handle.Bytes, &h); err != nil {
			out = append(out, connector.CommitOutcome{
				Handle:      c.Handle,
				Disposition: connector.DispositionDeadLetter,
				Fault: fault.Contract(fault.OpCommitSink, fmt.Errorf(
					"committable version %d is not decodable by build %d: %w", c.Handle.Version, stageV1, err)),
			})
			continue
		}
		k.mu.Lock()
		already := k.published[h.Key]
		k.mu.Unlock()
		if already {
			out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionAlreadyCommitted})
			continue
		}
		if err := os.Rename(k.stagePath(h.Key), k.livePath(h.Key)); err != nil {
			if os.IsNotExist(err) {
				// The artifact is gone. Do NOT silently discard it: dead-letter routes the
				// covered records and does not advance the prefix past them.
				out = append(out, connector.CommitOutcome{
					Handle: c.Handle, Disposition: connector.DispositionDeadLetter,
					Fault: fault.Permanent(fault.OpCommitSink, err),
				})
				continue
			}
			out = append(out, connector.CommitOutcome{
				Handle: c.Handle, Disposition: connector.DispositionRetryLater,
				Fault: fault.Transient(fault.OpCommitSink, err),
			})
			continue
		}
		k.mu.Lock()
		k.published[h.Key] = true
		k.durable = dropStaged(k.durable, h.Key)
		if k.mStaged != nil {
			k.mStaged.Set(float64(len(k.durable)))
		}
		k.mu.Unlock()
		out = append(out, connector.CommitOutcome{Handle: c.Handle, Disposition: connector.DispositionCommitted})
	}
	return out, nil
}

func (k *stageSink) livePath(key string) string {
	safe := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(key)
	return k.root + string(os.PathSeparator) + safe + ".live"
}

func dropStaged(in []staged, key string) []staged {
	out := in[:0]
	for _, st := range in {
		if st.key != key {
			out = append(out, st)
		}
	}
	return out
}

// AbortStale discards committables found in a recovered checkpoint that this sink no longer
// recognises, and ones whose Expires has passed. Abort means "as if never triggered", NOT
// "discard the artifacts" — so the bytes stay and only the claim is dropped.
func (k *stageSink) AbortStale(_ context.Context, cs []connector.Committable) error {
	for _, c := range cs {
		var h stageHandle
		if err := json.Unmarshal(c.Handle.Bytes, &h); err != nil {
			continue
		}
		k.mu.Lock()
		k.durable = dropStaged(k.durable, h.Key)
		k.mu.Unlock()
		k.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventNote, Severity: fault.TransientInternal,
			Message: fmt.Sprintf("abandoned the commit claim on staged artifact %s from checkpoint %d", h.Key, c.Checkpoint),
			Detail:  "the bytes are retained; the next successful checkpoint covers a longer span",
		})
	}
	return nil
}

// SnapshotState and RestoreState carry in-progress work across a restart.
func (k *stageSink) SnapshotState(_ context.Context, id uint64) ([]record.Blob, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]record.Blob, 0, len(k.durable)+len(k.pending))
	for _, st := range append(append([]staged{}, k.durable...), k.pending...) {
		b, err := json.Marshal(stageHandle{
			Key: st.key, Lanes: st.lanes, Records: st.records,
			FirstRec: st.firstRec, LastRec: st.lastRec,
		})
		if err != nil {
			return nil, fault.Bug(fault.OpPersist, err)
		}
		out = append(out, record.Blob{Version: stageV1, Bytes: b})
	}
	_ = id
	return out, nil
}

func (k *stageSink) RestoreState(_ context.Context, bs []record.Blob) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, b := range bs {
		var h stageHandle
		if err := json.Unmarshal(b.Bytes, &h); err != nil {
			// Additive JSON, so a newer version is readable. A genuinely broken blob is a
			// contract failure that must stop the pipeline rather than lose staged work.
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"writer state version %d is not decodable by build %d: %w", b.Version, stageV1, err))
		}
		k.durable = append(k.durable, staged{
			key: h.Key, lanes: h.Lanes, records: h.Records,
			firstRec: h.FirstRec, lastRec: h.LastRec, flushed: true,
		})
	}
	return nil
}

// Partition supplies the key; the engine keeps one open batch per key with its own limits.
// Per-stream, per-day batching with no batching code in the sink.
func (k *stageSink) Partition(r *record.Record) (string, error) {
	day := r.EventTime
	if day.IsZero() {
		day = r.Origin().ReadAt
	}
	return string(r.Dest) + "/" + day.UTC().Format("2006-01-02"), nil
}

// ApplySchemaChange acts on a change the engine has already quiesced the stream for.
func (k *stageSink) ApplySchemaChange(_ context.Context, ch schema.Change) error {
	switch ch.Kind {
	case schema.CreateStream, schema.AddField, schema.AlterNullability:
		k.rt.Note(connector.Event{
			At: time.Now(), Kind: connector.EventSchemaChange, Stream: record.StreamName(ch.Stream),
			Message: fmt.Sprintf("applied %s at schema epoch %d", ch.Kind, ch.Epoch),
		})
		return nil
	default:
		// Declared SchemaChanges is the contract; anything else is a structural disagreement.
		return fault.Contract(fault.OpSchemaApply, fmt.Errorf(
			"this destination cannot apply %s", ch.Kind))
	}
}

// Prepare creates or verifies the destination before any data flows.
func (k *stageSink) Prepare(_ context.Context, streams []connector.ConfiguredStream, _ []schema.Entry) error {
	for _, cs := range streams {
		dir := k.root + string(os.PathSeparator) + strings.ReplaceAll(string(cs.Stream), "/", "_")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fault.Transient(fault.OpPrepare, err)
		}
	}
	return nil
}

func (k *stageSink) Validate(_ context.Context) config.Diagnostics {
	var d config.Diagnostics
	if k.root == "" {
		return d.Errorf(config.CodeMissingField, []string{"root"}, "root is empty", "set a writable directory")
	}
	if err := os.MkdirAll(k.root, 0o750); err != nil {
		return d.Errorf(config.CodePermission, []string{"root"},
			"cannot create or write "+k.root+": "+err.Error(),
			"grant the worker write access, or mount a volume there")
	}
	return d
}

func (k *stageSink) Probe(_ context.Context) connector.ProbeResults {
	if _, err := os.Stat(k.root); err != nil {
		return connector.ProbeFailed("staging root writable", err)
	}
	return connector.ProbeOK("staging root writable")
}

func (k *stageSink) Choices(_ context.Context, hook string, _ *config.Config) ([]config.EnumValue, error) {
	if hook != "roots" {
		return nil, fault.Contract(fault.OpValidate, fmt.Errorf("unknown choices hook %q", hook))
	}
	return []config.EnumValue{{Value: "/var/lib/canal/stage", Title: "default staging directory"}}, nil
}

// Close flushes and releases. Called exactly once, always, including after a failed Open.
func (k *stageSink) Close(ctx context.Context) error {
	if k.rt == nil {
		return nil // Open was never called; config validation constructed and closed us
	}
	_, err := k.Flush(ctx, connector.FlushDrain)
	return err
}

// Compile-time assertions that this connector satisfies every interface it declares. These
// belong in the connector, not in the core: registration cross-checks the declared caps
// against the method set, but only at init, and a compile-time assertion fails earlier.
var (
	_ connector.Source          = (*shardSource)(nil)
	_ connector.Discoverer      = (*shardSource)(nil)
	_ connector.Nackable        = (*shardSource)(nil)
	_ connector.BacklogReporter = (*shardSource)(nil)
	_ connector.Heartbeater     = (*shardSource)(nil)
	_ connector.Validator       = (*shardSource)(nil)
	_ connector.Prober          = (*shardSource)(nil)
	_ connector.ChoiceProvider  = (*shardSource)(nil)
	_ connector.StateAdopter    = (*shardSource)(nil)

	_ connector.Sink           = (*stageSink)(nil)
	_ connector.Flusher        = (*stageSink)(nil)
	_ connector.Partitioner    = (*stageSink)(nil)
	_ connector.SchemaApplier  = (*stageSink)(nil)
	_ connector.Committer      = (*stageSink)(nil)
	_ connector.WriterState    = (*stageSink)(nil)
	_ connector.Preparer       = (*stageSink)(nil)
	_ connector.Validator      = (*stageSink)(nil)
	_ connector.Prober         = (*stageSink)(nil)
	_ connector.ChoiceProvider = (*stageSink)(nil)
)
