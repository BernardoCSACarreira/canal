package connector

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/config"
)

// Validator is tier two of two-tier validation: it MAY do I/O and it returns per-field
// diagnostics, ALL of them, never a fail-fast throw and never one bool plus one string.
//
// The same interface serves sources and sinks, because "can I reach this thing and am I
// allowed to use it" is the same question on both sides.
type Validator interface {
	Validate(ctx context.Context) config.Diagnostics
}

// Prober is a cheap liveness check, callable without initialising the component, and
// returning a LIST of named results rather than a bool.
//
// A list because "the endpoint answered" and "I can actually read the stream" are
// different facts, and a probe that collapses them into one boolean is the health check
// that returns 200 for a broken pipeline.
type Prober interface {
	Probe(ctx context.Context) ProbeResults
}

// ProbeResult is one named check.
type ProbeResult struct {
	Label string   `json:"label"`
	Path  []string `json:"path,omitempty"`

	// Err is the local error. It is not serialised; Error carries the wire form, because
	// an error degrades to a string at every boundary and the in-process path pays the
	// same cost deliberately.
	Err error `json:"-"`

	Error string `json:"error,omitempty"`
}

// ProbeResults is the full set of checks a component ran.
type ProbeResults []ProbeResult

// OK reports whether every check passed.
func (rs ProbeResults) OK() bool {
	for i := range rs {
		if rs[i].Err != nil || rs[i].Error != "" {
			return false
		}
	}
	return true
}

// ProbeOK returns a single passing result.
func ProbeOK(label string) ProbeResults {
	return ProbeResults{{Label: label}}
}

// ProbeFailed returns a single failing result, with the error rendered into the wire
// field so the two representations cannot disagree.
func ProbeFailed(label string, err error) ProbeResults {
	r := ProbeResult{Label: label, Err: err}
	if err != nil {
		r.Error = err.Error()
	}
	return ProbeResults{r}
}

// ChoiceProvider backs config.Field.Choices: a NAMED HOOK returning valid values for a
// field given the partial config typed so far. "List the tables in this database", "list
// the topics", "list the buckets".
//
// A named hook rather than a live callback, because a callback cannot cross a process
// boundary. This is how a specialised connector UI is built with zero core knowledge: the
// core exposes GET /v1/connectors/{name}/choices/{hook} and forwards a string and a
// config tree. It has no idea that tables exist.
type ChoiceProvider interface {
	Choices(ctx context.Context, hook string, partial *config.Config) ([]config.EnumValue, error)
}
