package registry

import (
	"fmt"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
)

// capCheck is one interface-backed capability: its stable token, the Go interface behind it, what
// the component declared, what the type actually implements, and what its presence unlocks.
type capCheck struct {
	name        string
	title       string
	iface       string
	declared    bool
	implemented bool
	unlocks     []string
}

// implements reports whether the zero value of a registered component's concrete type satisfies T.
//
// It interrogates a METHOD SET, not an instance: `var z S` is a nil pointer for the usual
// pointer-receiver connector, and asserting on a nil pointer's dynamic type is legal and calls
// nothing. This is what makes the cross-check possible without constructing a component from
// config that may not exist yet.
//
// The one case it cannot see through is a definition whose type parameter is itself an interface,
// where the zero value carries no dynamic type at all. Such a definition is reported as
// implementing nothing, which surfaces as a loud over-declaration panic rather than a silent pass.
func implements[T any](z any) bool {
	_, ok := z.(T)
	return ok
}

// report turns the checks into the operator-facing capability report and the non-fatal warning
// list, and returns an error naming every over-declaration.
//
// The asymmetry is the whole point: declared-without-implemented is fatal, because it is a lie the
// engine would act on. Implemented-without-declared is a warning, because a v2 core adding an
// optional interface must not retroactively break a v1 connector that satisfies it by coincidence.
func report(kind Kind, name string, checks []capCheck, unknown []string) ([]CapReport, []string, error) {
	out := make([]CapReport, 0, len(checks)+len(unknown))
	var warnings []string
	var overDeclared []string

	for _, c := range checks {
		r := CapReport{Name: c.name, Title: c.title, Iface: c.iface, Unlocks: c.unlocks}
		switch {
		case c.declared && c.implemented:
			r.Present, r.Source = true, CapProbed
		case c.declared && !c.implemented:
			r.Present, r.Source = false, CapAbsent
			r.Reason = fmt.Sprintf("declared but %s is not implemented", c.iface)
			overDeclared = append(overDeclared, fmt.Sprintf("%s (needs %s)", c.name, c.iface))
		case !c.declared && c.implemented:
			// Present in the Go type, absent from the declaration. The engine uses the
			// DECLARATION, so the capability is not active — and the report says so.
			r.Present, r.Source = false, CapUndeclared
			r.Reason = fmt.Sprintf("%s is implemented but not declared in Caps, so it is not used", c.iface)
			warnings = append(warnings,
				fmt.Sprintf("%s %q implements %s without declaring it; set the matching Caps field to use it", kind, name, c.iface))
		default:
			r.Present, r.Source = false, CapAbsent
			r.Reason = fmt.Sprintf("the connector does not implement %s", c.iface)
		}
		out = append(out, r)
	}

	for _, u := range unknown {
		// An unknown capability is IGNORED and REPORTED, never an error. Anything else makes a
		// newer connector unusable by an older core, which is the downgrade path nobody tests.
		out = append(out, CapReport{
			Name:    u,
			Title:   u,
			Present: false,
			Source:  CapUnknown,
			Reason:  "declared by the component and unrecognised by this build of canal; ignored",
		})
	}

	if len(overDeclared) > 0 {
		return out, warnings, fmt.Errorf(
			"%s %q declares capabilities it does not implement: %v", kind, name, overDeclared)
	}
	return out, warnings, nil
}

// declaredPlain appends the capability report entries for declarations that have no interface
// behind them. They are still reported, because an operator asking "why is there no scan progress"
// needs to see that the source declared no comparable positions.
func declaredPlain(out []CapReport, name, title string, present bool, reason string, unlocks []string) []CapReport {
	r := CapReport{Name: name, Title: title, Present: present, Unlocks: unlocks}
	if present {
		r.Source = CapProbed
	} else {
		r.Source = CapAbsent
		r.Reason = reason
	}
	return append(out, r)
}

// checkCommon validates the parts of every Caps struct that are identical across kinds.
func checkCommon(kind Kind, name string, c connector.Caps) error {
	if name == "" {
		return fmt.Errorf("%s: Name is required and is immutable once published", kind)
	}
	if c.APIVersion < connector.MinAPIVersion || c.APIVersion > connector.APIVersion {
		return fmt.Errorf(
			"%s %q declares connector API version %d; this build of canal supports %d to %d",
			kind, name, c.APIVersion, connector.MinAPIVersion, connector.APIVersion)
	}
	return nil
}
