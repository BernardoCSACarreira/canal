package registry

import (
	"encoding/json"

	"github.com/BernardoCSACarreira/canal/pkg/config"
)

// Descriptor is the cached, INSTANTIATION-FREE projection the control API serves and the UI
// renders.
//
// Producing it runs no connector code, which is what makes the connector-list endpoint fast and
// unbreakable by a panicking constructor.
type Descriptor struct {
	Kind    Kind    `json:"kind"`
	Name    string  `json:"name"`
	Version string  `json:"version"`
	Title   string  `json:"title"`
	Summary string  `json:"summary"`
	Docs    string  `json:"docs"`
	Notes   string  `json:"notes,omitempty"`
	Support Support `json:"support"`

	Config *config.Spec `json:"config"`

	// Caps is the kind's capability struct, marshalled once at registration.
	Caps json.RawMessage `json:"caps"`

	// JSONSchema is GENERATED from Config, never hand-maintained.
	JSONSchema json.RawMessage `json:"json_schema"`

	// Capabilities is the operator-facing capability REPORT: every capability the core knows
	// about, present or absent, with a reason when absent and the consequences of its presence.
	//
	// This is the only mechanism in the surveyed field that EXPLAINS a missing capability instead
	// of rendering a blank.
	Capabilities []CapReport `json:"capabilities"`

	// Warnings records registration-time observations that are not fatal — chiefly an interface
	// implemented without being declared.
	Warnings []string `json:"warnings,omitempty"`
}

// CapReport is one capability's presence, provenance and consequence.
type CapReport struct {
	// Name is the stable machine token, for example "reports_backlog". A capability's identity is
	// a STRING, never an iota, in every persisted or signed artefact.
	Name string `json:"name"`

	// Title is an i18n key, not a sentence.
	Title string `json:"title"`

	Present bool      `json:"present"`
	Source  CapSource `json:"source"`

	// Reason is REQUIRED when Present is false and MUST be empty when Present is true. A core test
	// walks every entry of every fixture and fails on a violation: design rule R8 applied to the
	// capability report.
	Reason string `json:"reason,omitempty"`

	// Unlocks is the operator-facing consequence list, rendered next to the absence reason, so
	// "comparable positions: absent — the connector supplies no order encoding" is immediately
	// followed by "would enable: mid-lane resume assertions, position-fraction progress".
	Unlocks []string `json:"unlocks,omitempty"`

	// Iface names the exact Go interface to implement, which turns "impossible pipeline" into a
	// connector-authoring task list.
	Iface string `json:"iface,omitempty"`
}

// CapSource says how the core learned about a capability.
type CapSource uint8

const (
	// CapProbed means the Go type implements the interface.
	CapProbed CapSource = iota + 1
	// CapAbsent means the Go type does not.
	CapAbsent
	// CapDeclined means implemented, but declined for this configuration through
	// fault.ErrDeclined.
	CapDeclined
	// CapRemote means an out-of-process component declared it over the wire.
	CapRemote
	// CapUndeclared means implemented but not declared: a warning, not an error.
	CapUndeclared
	// CapUnknown means declared by name and unrecognised by this core: ignored and reported.
	CapUnknown
)

var capSourceNames = map[CapSource]string{
	CapProbed:     "probed",
	CapAbsent:     "absent",
	CapDeclined:   "declined",
	CapRemote:     "remote",
	CapUndeclared: "undeclared",
	CapUnknown:    "unknown",
}

// String returns the stable snake_case token for s.
func (s CapSource) String() string {
	if n, ok := capSourceNames[s]; ok {
		return n
	}
	return "absent"
}

// MarshalText serialises a CapSource as its token.
func (s CapSource) MarshalText() ([]byte, error) { return []byte(s.String()), nil }
