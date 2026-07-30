package config

import (
	"strings"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Severity is a diagnostic's weight. Only [SeverityError] blocks a build.
type Severity uint8

const (
	SeverityError Severity = iota
	SeverityWarning
	SeverityInfo
)

var severityNames = [...]string{
	SeverityError:   "error",
	SeverityWarning: "warning",
	SeverityInfo:    "info",
}

// String returns the stable snake_case token for s.
func (s Severity) String() string {
	if int(s) < len(severityNames) {
		return severityNames[s]
	}
	return "error"
}

// Code is the closed diagnostic vocabulary: both a metric label and an i18n key
// namespace (design rule R9).
//
// It is a string rather than an iota so that adding a code is data rather than a
// coordinated core-plus-frontend edit.
type Code string

const (
	// CodeUnknownField is the one that catches typo'd YAML — the classic silent
	// failure of every config-driven tool.
	CodeUnknownField     Code = "unknown_field"
	CodeMissingField     Code = "missing_field"
	CodeWrongType        Code = "wrong_type"
	CodeOutOfRange       Code = "out_of_range"
	CodeInvalidEnum      Code = "invalid_enum"
	CodeInvalidPattern   Code = "invalid_pattern"
	CodeDeprecated       Code = "deprecated"
	CodeUnknownComponent Code = "unknown_component"
	CodeGraphInvalid     Code = "graph_invalid"
	CodeCapability       Code = "capability"
	CodeGuarantee        Code = "guarantee"

	// Tier-two codes: these require I/O and come from connector.Validator.
	CodeUnreachable Code = "unreachable"
	CodeAuthFailed  Code = "auth_failed"
	CodeNotFound    Code = "not_found"
	CodePermission  Code = "permission_denied"

	CodeCustom Code = "custom"
)

// Diagnostic is one problem, anchored to a field path so a form renders it inline.
type Diagnostic struct {
	Path     []string      `json:"path"`
	Node     record.NodeID `json:"node,omitempty"`
	Severity Severity      `json:"severity"`
	Code     Code          `json:"code"`
	Message  string        `json:"message"`

	// Hint is what to do about it.
	Hint string `json:"hint,omitempty"`

	// Iface names the Go interface to implement, when the diagnostic is a capability
	// refusal. That turns "impossible pipeline" into a connector-authoring task list.
	Iface string `json:"iface,omitempty"`

	Line   int `json:"line,omitempty"`
	Column int `json:"column,omitempty"`
}

// Diagnostics is the return of every validation entry point. ALL problems at once,
// never fail-fast: a form that shows one error at a time is a form operators fight.
type Diagnostics []Diagnostic

// HasErrors reports whether any diagnostic is [SeverityError].
func (d Diagnostics) HasErrors() bool {
	for i := range d {
		if d[i].Severity == SeverityError {
			return true
		}
	}
	return false
}

// Error renders every diagnostic, one per line, so Diagnostics satisfies error and can
// be returned from a function that has only an error to give.
func (d Diagnostics) Error() string {
	var b strings.Builder
	for i := range d {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(d[i].Severity.String())
		b.WriteString(" [")
		b.WriteString(string(d[i].Code))
		b.WriteString("] ")
		if len(d[i].Path) > 0 {
			b.WriteString(joinPath(d[i].Path))
			b.WriteString(": ")
		}
		b.WriteString(d[i].Message)
	}
	return b.String()
}

// Errorf appends an error-severity diagnostic. It exists so that the several hundred
// call sites that build diagnostics do not each spell the struct.
func (d Diagnostics) Errorf(code Code, path []string, msg, hint string) Diagnostics {
	return append(d, Diagnostic{Path: path, Severity: SeverityError, Code: code, Message: msg, Hint: hint})
}

// Warnf appends a warning-severity diagnostic.
func (d Diagnostics) Warnf(code Code, path []string, msg, hint string) Diagnostics {
	return append(d, Diagnostic{Path: path, Severity: SeverityWarning, Code: code, Message: msg, Hint: hint})
}
