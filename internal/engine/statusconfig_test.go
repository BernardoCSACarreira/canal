package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// A DECLARED FIELD AND A DECLARED REDACTOR, NEITHER OF WHICH WAS CONNECTED TO THE OTHER.
//
// PipelineStatus.Config said it was "the REDACTED config tree, the only form that ever leaves the
// process" and nothing assigned it. config.Config.Redacted said "the read model, every log line and
// every API response use this and nothing else" and had no caller outside its own tests. Two documents
// each describing a relationship that did not exist, which is the shape this module keeps producing.
//
// The assertion that matters is not that a tree appears. It is that a value marked secret does NOT.

// secretSink is a sink whose config declares a password, so redaction has something to redact.
type secretSink struct{ collector }

func registerSecretSink(t *testing.T, s *secretSink) string {
	t.Helper()
	name := fmt.Sprintf("secret_sink_%d", sinkSeq.Add(1))
	spec := config.NewSpec()
	spec.Fields = append(spec.Fields,
		config.Fields.BasicAuth("auth"),
		config.Field{Name: "endpoint", Type: config.TypeString, Optional: true,
			Description: "Where to write, which is not a secret."},
	)
	registry.AddSink(registry.Default, registry.SinkDef[*secretSink]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Sink with a secret",
			Summary: "Declares a password so the read model has something to redact.",
			Support: registry.SupportCommunity,
		},
		Spec: spec,
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
		},
		New: func(context.Context, *config.Config) (*secretSink, error) { return s, nil },
	})
	return name
}

const thePassword = "hunter2-do-not-leak"

func buildWithSecret(t *testing.T) *engine.Pipeline {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 2)

	state, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { state.Close() })

	s := pipelineSpec(registerSecretSink(t, &secretSink{}), path)
	s.Graph[1].Config = map[string]any{
		"codec":    map[string]any{"encoder": "raw", "framer": "newline"},
		"endpoint": "https://example.invalid/ingest",
		"auth":     map[string]any{"username": "canal", "password": thePassword},
	}

	p, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{
		State: state, Worker: "w1", FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	return p
}

// THE SECRET NEVER LEAVES, and this is asserted against the SERIALISED document, because that is what
// leaves. Reading the map would miss a secret that only appears once the tree is marshalled.
func TestTheConfigTreeRedactsADeclaredSecret(t *testing.T) {
	p := buildWithSecret(t)

	doc := p.Status(telemetry.StatusQuery{IncludeConfig: true})
	if len(doc.Config) == 0 {
		t.Fatal("the config tree is absent when it was asked for")
	}

	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshalling the status document: %v", err)
	}
	if strings.Contains(string(body), thePassword) {
		t.Errorf("the status document contains the plaintext password.\n"+
			"  This is the document that goes to an HTTP response and to store.StatusStore, so a "+
			"secret in it has left the process:\n  %s", body)
	}
	if !strings.Contains(string(body), config.RedactedMarker) {
		t.Errorf("no redaction marker appears, so the password was not redacted so much as dropped — "+
			"and an operator cannot tell a configured secret from an absent one:\n  %s", body)
	}

	// The NON-secret fields are still there, or the tree is useless for the thing it exists for.
	if !strings.Contains(string(body), "example.invalid") {
		t.Errorf("the endpoint is missing, so redaction took the whole node with it:\n  %s", body)
	}
	if !strings.Contains(string(body), "canal") {
		t.Error("the username is missing; only the field marked secret should have gone")
	}
}

// THE TREE IS KEYED BY NODE, because a config belongs to a node and a merge of two nodes' fields is
// not any node's config.
func TestTheConfigTreeIsKeyedByNode(t *testing.T) {
	p := buildWithSecret(t)
	doc := p.Status(telemetry.StatusQuery{IncludeConfig: true})

	for _, id := range []record.NodeID{"in", "out"} {
		if _, ok := doc.Config[string(id)]; !ok {
			t.Errorf("node %s has no entry in the config tree; got keys %v", id, keysOf(doc.Config))
		}
	}
}

// ABSENT UNLESS ASKED FOR. The zero query is what a scrape and a health banner send, and neither
// renders configuration — and a per-worker status report would otherwise carry one pipeline's config
// once per worker on every interval.
func TestTheConfigTreeIsAbsentUnlessAskedFor(t *testing.T) {
	p := buildWithSecret(t)

	if doc := p.Status(telemetry.StatusQuery{}); doc.Config != nil {
		t.Errorf("the zero query returned a config tree with %d entries", len(doc.Config))
	}
	// Nil rather than empty, so omitempty keeps it out of the wire form entirely.
	//
	// CHECKED AS A KEY, not as a substring. Searching the body for `"config"` matched the string inside
	// the negotiation's own defaults — {"path":["config","config_interval"]} — so the first version of
	// this assertion failed against correct code. A JSON key is a structural fact and grep is not a
	// parser.
	body, err := json.Marshal(p.Status(telemetry.StatusQuery{}))
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		t.Fatalf("decoding the document: %v", err)
	}
	if raw, ok := top["config"]; ok {
		t.Errorf("the unasked-for document carries a top-level config key: %s", raw)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
