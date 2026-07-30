package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
)

// The stage-standard field names. The registry APPENDS these composite fields to every
// registered component's spec, by kind, unless the component already declares a field
// of that name.
//
// The connector author writes none of them; the operator configures them per node; the
// ENGINE reads them. This is what makes a per-sink wire format expressible — fan-out to
// two sinks in two formats is two sink nodes with two codec blocks — and it is why the
// codec stages have a caller at all.
const (
	FieldRetry             = "retry"
	FieldWhenFull          = "when_full"
	FieldCodec             = "codec"
	FieldBatching          = "batching"
	FieldMaxInFlight       = "max_in_flight"
	FieldDedupe            = "dedupe"
	FieldLaneBudget        = "lane_budget"
	FieldHeartbeatInterval = "heartbeat_interval"
	FieldCapacity          = "capacity"
)

// Fields provides pre-built, reusable field fragments with matching extractors.
//
// This pairing is the single most transplantable idea in the surveyed config models:
// every component's retry, batching, TLS and codec block looks identical, documents
// itself identically and renders identically, with zero coordination between connector
// authors and zero switches in the core.
var Fields fields

type fields struct{}

// num is the idiomatic way to write an inclusive bound: Min and Max are pointers
// because "no bound" and "a bound of zero" are different facts.
func num(v float64) *float64 { return &v }

// Retry declares the standard retry block. Note that terminal has no valid zero and no
// "forever" member: unbounded retry is inexpressible, not merely discouraged.
func (fields) Retry(name string) Field {
	return Field{
		Name:        name,
		Type:        TypeObject,
		Title:       "Retry",
		Description: "How failures at this node are retried, and what happens when the attempts run out.",
		Optional:    true,
		Fields: []Field{
			{Name: "max_attempts", Type: TypeInt, Default: 4, Optional: true, Min: num(1),
				Description: "Total attempts including the first. 1 means no retry."},
			{Name: "initial", Type: TypeDuration, Default: "100ms", Optional: true,
				Description: "First backoff delay. Backoff is full-jitter exponential; it is the only strategy canal offers."},
			{Name: "max", Type: TypeDuration, Default: "5s", Optional: true,
				Description: "Ceiling on a single backoff delay."},
			{Name: "max_elapsed", Type: TypeDuration, Optional: true,
				Description: "Ceiling on total time spent retrying. Absent means bounded only by max_attempts."},
			{Name: "multiplier", Type: TypeFloat, Default: 2.0, Optional: true, Min: num(1),
				Description: "Exponential growth factor."},
			{Name: "terminal", Type: TypeEnum, Default: "dead_letter",
				Description: "What happens to a record whose attempts are exhausted.",
				Enum: []EnumValue{
					{Value: "dead_letter", Title: "Dead-letter", Description: "Route on this node's failed edge and settle the record as abandoned."},
					{Value: "drop", Title: "Drop", Description: "Discard and count. Settles the record as abandoned."},
					{Value: "stop", Title: "Stop the pipeline", Description: "Settle nothing and stop."},
				}},
			{Name: "on_indeterminate", Type: TypeEnum, Default: "stall", Optional: true,
				Description: "What happens when a write may or may not have landed and the sink is not idempotent.",
				Enum: []EnumValue{
					{Value: "stall", Title: "Stall the lane", Description: "Settle nothing, block the lane, and raise a degraded condition. The honest default."},
					{Value: "retry", Title: "Retry", Description: "Accept possible duplicates."},
					{Value: "dead_letter", Title: "Dead-letter", Description: "Accept possible duplicates in the dead-letter route."},
				}},
		},
	}
}

// Batching declares the standard batching block: four orthogonal triggers, one of them
// a declarative predicate, which is how "close the batch at a transaction boundary" is
// expressed without an expression language.
func (fields) Batching(name string) Field {
	return Field{
		Name:        name,
		Type:        TypeObject,
		Title:       "Batching",
		Description: "When an open batch is closed and handed to the sink. Triggers are independent; the first to fire wins.",
		Optional:    true,
		Fields: []Field{
			{Name: "max_records", Type: TypeInt, Default: 500, Optional: true, Min: num(1),
				Description: "Close the batch at this many records."},
			{Name: "max_bytes", Type: TypeSize, Optional: true,
				Description: "Absent means no byte trigger. The engine still respects the sink's declared request limits."},
			{Name: "max_age", Type: TypeDuration, Default: "1s", Optional: true,
				Description: "Close the batch this long after its first record, so a trickle of traffic still makes progress."},
		},
	}
}

// Codec declares the standard encoder/framer/compression block, attached per SINK NODE.
// A connector never names a codec and a codec never names a connector, which is what
// turns N codecs times M connectors into N plus M.
func (fields) Codec(name string) Field {
	return Field{
		Name:        name,
		Type:        TypeObject,
		Title:       "Serialization",
		Description: "How records are encoded, delimited and compressed for this sink node.",
		Optional:    true,
		Fields: []Field{
			{Name: "encoder", Type: TypeString, Default: "json", Optional: true,
				Description: "Registered encoder name.", Choices: "encoders"},
			{Name: "encoder_config", Type: TypeMap, Optional: true, Advanced: true,
				Description: "Encoder-specific settings, passed through to the named encoder.",
				Item:        &Field{Name: "value", Type: TypeString}},
			{Name: "framer", Type: TypeString, Optional: true,
				Description: "Registered framer name. Absent means the encoder is self-delimiting.", Choices: "framers"},
			{Name: "compressor", Type: TypeString, Optional: true,
				Description: "Registered compressor name. Absent means no compression.", Choices: "compressors"},
		},
	}
}

// TLS declares the standard transport-security block.
func (fields) TLS(name string) Field {
	return Field{
		Name:     name,
		Type:     TypeObject,
		Title:    "TLS",
		Optional: true,
		Fields: []Field{
			{Name: "enabled", Type: TypeBool, Default: false, Optional: true,
				Description: "Whether to use TLS at all."},
			{Name: "server_name", Type: TypeString, Optional: true,
				Description: "Name to verify the certificate against, when it differs from the host being dialled."},
			{Name: "ca_file", Type: TypeString, Optional: true,
				Description: "PEM file of certificate authorities to trust. Absent means the system pool."},
			{Name: "cert_file", Type: TypeString, Optional: true,
				Description: "Client certificate, for mutual TLS. Must be given together with key_file."},
			{Name: "key_file", Type: TypeString, Optional: true,
				Description: "Private key for cert_file."},
			{Name: "insecure_skip_verify", Type: TypeBool, Default: false, Optional: true, Advanced: true,
				Description: "Disables certificate verification. Never appropriate outside a test."},
		},
	}
}

// BasicAuth declares the standard username/password block, with the password marked
// secret so redaction is structural rather than a per-call-site discipline.
func (fields) BasicAuth(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Title: "Basic authentication", Optional: true,
		Fields: []Field{
			{Name: "username", Type: TypeString, Description: "Account name."},
			{Name: "password", Type: TypeString, Secret: true,
				Description: "Redacted everywhere by the core: logs, metrics, the read model, error messages and every API response."},
		},
	}
}

// OAuth2 declares the standard client-credentials block.
func (fields) OAuth2(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Title: "OAuth 2.0", Optional: true,
		Fields: []Field{
			{Name: "token_url", Type: TypeString, Description: "Token endpoint to exchange the client credentials at."},
			{Name: "client_id", Type: TypeString, Description: "Client identifier."},
			{Name: "client_secret", Type: TypeString, Secret: true,
				Description: "Redacted everywhere by the core."},
			{Name: "scopes", Type: TypeArray, Optional: true,
				Description: "Scopes to request.",
				Item:        &Field{Name: "scope", Type: TypeString}},
		},
	}
}

// HTTPClient declares the standard outbound-HTTP block.
func (fields) HTTPClient(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Title: "HTTP client", Optional: true,
		Fields: []Field{
			{Name: "timeout", Type: TypeDuration, Default: "30s", Optional: true,
				Description: "Whole-request timeout, including connection and body."},
			{Name: "max_idle_conns", Type: TypeInt, Default: 10, Optional: true, Advanced: true,
				Description: "Idle connections to keep in the pool."},
			Fields.TLS("tls"),
		},
	}
}

// RateLimit declares the standard client-side rate-limit block.
func (fields) RateLimit(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Title: "Rate limit", Optional: true,
		Fields: []Field{
			{Name: "requests_per_second", Type: TypeFloat, Min: num(0),
				Description: "Sustained request rate this node may issue."},
			{Name: "burst", Type: TypeInt, Default: 1, Optional: true, Min: num(1),
				Description: "How many requests may be issued back to back before the sustained rate applies."},
		},
	}
}

// LaneBudget declares the in-flight allowance per lane.
//
// It is ONE number and it is simultaneously the backpressure trigger, the in-flight
// bound and an input to the exported replay window. It replaces the five overlapping
// knobs one surveyed framework accumulated.
func (fields) LaneBudget(name string) Field {
	return Field{
		Name: name, Type: TypeInt, Default: 1000, Optional: true, Min: num(1),
		Title:       "In-flight budget per lane",
		Description: "Records a lane may have unsettled at once. Admission blocks at this number, which is how backpressure reaches the source. It also bounds the worst-case re-read after a crash — but the real replay window is measured and exported separately, never inferred from this value.",
	}
}

// MetaFilter declares which metadata namespaces and keys survive into a sink.
func (fields) MetaFilter(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Title: "Metadata filter", Optional: true,
		Fields: []Field{
			{Name: "include", Type: TypeArray, Optional: true,
				Description: "Metadata keys to carry into the destination. Absent means all of them.",
				Item:        &Field{Name: "pattern", Type: TypeString}},
			{Name: "exclude", Type: TypeArray, Optional: true,
				Description: "Metadata keys to drop, applied after include.",
				Item:        &Field{Name: "pattern", Type: TypeString}},
		},
	}
}

// WhenFull declares what happens when a bounded edge or buffer refuses.
//
// There is no "unbounded" member: unbounded growth to OOM is inexpressible (design rule
// R6). The engine reads the token with Get[string] and maps it onto its own closed
// enum; config does not name a connector type, which is what keeps this package out of
// an import cycle.
func (fields) WhenFull(name string) Field {
	return Field{
		Name: name, Type: TypeEnum, Default: "block", Optional: true,
		Title: "When full",
		Enum: []EnumValue{
			{Value: "block", Title: "Apply backpressure", Description: "Block admission. The default, and the only one that never loses data."},
			{Value: "drop_newest", Title: "Drop the newest", Description: "Discard incoming records and count them. Never the oldest: the oldest may already be inside a prefix the source was told is safe."},
			{Value: "reject", Title: "Reject", Description: "Settle the group as abandoned so the source is told, through a non-zero abandoned count, and can decide."},
			{Value: "overflow", Title: "Overflow to the next buffer", Description: "Spill to the chained buffer in the graph."},
		},
	}
}

// MaxInFlight declares how many requests a sink node may have outstanding. It is capped by
// the sink's declared SinkCaps.MaxConcurrency, which the engine enforces.
func (fields) MaxInFlight(name string) Field {
	return Field{
		Name: name, Type: TypeInt, Default: 1, Optional: true, Min: num(1),
		Title:       "Requests in flight",
		Description: "How many write requests this node may have outstanding. Capped by what the sink declares it can handle.",
	}
}

// Capacity declares a buffer's bound. There is no unlimited value: bounded by construction
// (design rule R6).
func (fields) Capacity(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Optional: true, Title: "Capacity",
		Description: "The bound. A buffer with no bound is not a buffer.",
		Fields: []Field{
			{Name: "records", Type: TypeInt, Default: 10000, Optional: true, Min: num(1),
				Description: "Records the buffer may hold before Put starts refusing."},
			{Name: "bytes", Type: TypeSize, Optional: true,
				Description: "Absent means bounded by records alone."},
		},
	}
}

// HeartbeatInterval declares how often an idle lane is heartbeated. The registry appends it
// only to a source declaring the heartbeat capability, so an operator is never shown a knob
// that does nothing.
func (fields) HeartbeatInterval(name string) Field {
	return Field{
		Name: name, Type: TypeDuration, Default: "10s", Optional: true,
		Title:       "Heartbeat interval",
		Description: "How often an idle lane is heartbeated, so a pruning upstream does not pin its own retention while nothing is arriving.",
	}
}

// Dedupe declares the engine-owned keyed dedupe block for a sink node.
//
// Dedupe is a property of a SINK NODE, not a transform: a transform returns immediately and
// has no channel through which to observe settlement, so a transform-based dedupe cannot mark
// a key seen AFTER the write. The window is required and has no default, because a
// process-lifetime cache described in documentation as a retention window matches neither
// semantics (design rule R5).
func (fields) Dedupe(name string) Field {
	return Field{
		Name: name, Type: TypeObject, Optional: true, Title: "Deduplication",
		Description: "Drop records already durably written. The key always carries tenant, pipeline, source node and stream; it is never the bare record id.",
		Fields: []Field{
			{Name: "layer", Type: TypeEnum, Default: "key",
				Description: "Which identity layer to key on.",
				Enum: []EnumValue{
					{Value: "upstream", Title: "Upstream id", Description: "The vendor's own id, carried verbatim on the record."},
					{Value: "key", Title: "Canonical key", Description: "canal's canonical identity, deterministically derived by the source."},
				}},
			{Name: "window", Type: TypeDuration,
				Description: "The retention over which \"duplicate\" is meaningful. Required: entries older than this are trimmed in the same durable write that advances the cursor."},
		},
	}
}

// Retry extracts the standard retry block as a fault.RetryPolicy.
//
// Every composite extractor returns a type owned by config or by fault, never by
// connector. That rule is why config is a leaf of the connector-facing graph rather
// than a participant in a cycle.
func (c *Config) Retry(path ...string) (fault.RetryPolicy, error) {
	p := fault.DefaultRetry
	sub, err := c.Object(path...)
	if err != nil {
		return p, err
	}
	if n, err := Get[int](sub, "max_attempts"); err == nil {
		p.MaxAttempts = n
	}
	if d, err := Get[time.Duration](sub, "initial"); err == nil {
		p.Backoff.Initial = d
	}
	if d, err := Get[time.Duration](sub, "max"); err == nil {
		p.Backoff.Max = d
	}
	if sub.Has("max_elapsed") {
		if d, err := Get[time.Duration](sub, "max_elapsed"); err == nil {
			p.Backoff.MaxElapsed = d
		}
	}
	if f, err := Get[float64](sub, "multiplier"); err == nil {
		p.Backoff.Multiplier = f
	}
	if s, err := Get[string](sub, "terminal"); err == nil {
		switch s {
		case "dead_letter":
			p.Terminal = fault.TerminalDeadLetter
		case "drop":
			p.Terminal = fault.TerminalDrop
		case "stop":
			p.Terminal = fault.TerminalStop
		default:
			return p, fmt.Errorf("config: unknown retry terminal %q", s)
		}
	}
	if s, err := Get[string](sub, "on_indeterminate"); err == nil {
		switch s {
		case "stall":
			p.OnIndeterminate = fault.IndeterminateStall
		case "retry":
			p.OnIndeterminate = fault.IndeterminateRetry
		case "dead_letter":
			p.OnIndeterminate = fault.IndeterminateDeadLetter
		default:
			return p, fmt.Errorf("config: unknown on_indeterminate %q", s)
		}
	}
	return p, p.Validate()
}

// BatchPolicy is the batching contract, owned by config so that the extractor need not
// name a connector type.
//
// Four orthogonal triggers: count, byte size, period, and a declarative predicate over
// the record — which is how "close the batch at a transaction boundary" is expressed
// with no second language.
type BatchPolicy struct {
	MaxRecords int           `json:"max_records,omitempty"`
	MaxBytes   int64         `json:"max_bytes,omitempty"`
	MaxAge     time.Duration `json:"max_age,omitempty"`

	// FlushOn forces a flush when the predicate holds for an incoming record. It uses
	// the same closed operator set as every other predicate in canal.
	FlushOn *Predicate `json:"flush_on,omitempty"`
}

// Batching extracts the standard batching block.
func (c *Config) Batching(path ...string) (BatchPolicy, error) {
	p := BatchPolicy{MaxRecords: 500, MaxAge: time.Second}
	sub, err := c.Object(path...)
	if err != nil {
		return p, err
	}
	if n, err := Get[int](sub, "max_records"); err == nil {
		p.MaxRecords = n
	}
	if sub.Has("max_bytes") {
		raw, _ := sub.lookup([]string{"max_bytes"})
		n, err := ParseSize(raw)
		if err != nil {
			return p, err
		}
		p.MaxBytes = n
	}
	if d, err := Get[time.Duration](sub, "max_age"); err == nil {
		p.MaxAge = d
	}
	if p.MaxRecords <= 0 && p.MaxBytes <= 0 && p.MaxAge <= 0 && p.FlushOn == nil {
		return p, errors.New("config: a batch policy with no trigger would never flush")
	}
	return p, nil
}

// CodecRef names a codec chain by registered name plus its own config. It is a
// reference, not a codec: the engine turns it into a live chain, and config does not
// know that codecs are objects.
type CodecRef struct {
	Encoder          string         `json:"encoder,omitempty"`
	EncoderConfig    map[string]any `json:"encoder_config,omitempty"`
	Framer           string         `json:"framer,omitempty"`
	FramerConfig     map[string]any `json:"framer_config,omitempty"`
	Compressor       string         `json:"compressor,omitempty"`
	CompressorConfig map[string]any `json:"compressor_config,omitempty"`
}

// IsZero reports whether no codec was configured at all.
func (r CodecRef) IsZero() bool {
	return r.Encoder == "" && r.Framer == "" && r.Compressor == ""
}

// Codec extracts the standard codec block as a reference.
func (c *Config) Codec(path ...string) (CodecRef, error) {
	var r CodecRef
	sub, err := c.Object(path...)
	if err != nil {
		return r, err
	}
	r.Encoder, _ = Get[string](sub, "encoder")
	r.Framer, _ = Get[string](sub, "framer")
	r.Compressor, _ = Get[string](sub, "compressor")
	if sub.Has("encoder_config") {
		r.EncoderConfig, _ = Get[map[string]any](sub, "encoder_config")
	}
	if sub.Has("framer_config") {
		r.FramerConfig, _ = Get[map[string]any](sub, "framer_config")
	}
	if sub.Has("compressor_config") {
		r.CompressorConfig, _ = Get[map[string]any](sub, "compressor_config")
	}
	return r, nil
}

// BufferRef names a buffer by registered name plus its own config, for the same reason
// [CodecRef] exists.
type BufferRef struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config,omitempty"`
}

// Buffer extracts a buffer reference.
func (c *Config) Buffer(path ...string) (BufferRef, error) {
	var r BufferRef
	sub, err := c.Object(path...)
	if err != nil {
		return r, err
	}
	r.Name, _ = Get[string](sub, "name")
	if sub.Has("config") {
		r.Config, _ = Get[map[string]any](sub, "config")
	}
	return r, nil
}

// TLS extracts the standard TLS block as a live *tls.Config, or nil when TLS is not
// enabled. It is one of only two extractors that touch the filesystem, and it does so
// because a certificate path with no file is a config error the operator should see at
// validate time rather than at connect time.
func (c *Config) TLS(path ...string) (*tls.Config, error) {
	sub, err := c.Object(path...)
	if err != nil {
		return nil, err
	}
	on, _ := Get[bool](sub, "enabled")
	if !on {
		return nil, nil
	}
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if s, err := Get[string](sub, "server_name"); err == nil {
		out.ServerName = s
	}
	if skip, err := Get[bool](sub, "insecure_skip_verify"); err == nil {
		out.InsecureSkipVerify = skip
	}
	if ca, err := Get[string](sub, "ca_file"); err == nil && ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("config: reading ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("config: ca_file %q contains no certificates", ca)
		}
		out.RootCAs = pool
	}
	certFile, _ := Get[string](sub, "cert_file")
	keyFile, _ := Get[string](sub, "key_file")
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("config: cert_file and key_file must be given together")
	}
	if certFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("config: loading client certificate: %w", err)
		}
		out.Certificates = []tls.Certificate{pair}
	}
	return out, nil
}
