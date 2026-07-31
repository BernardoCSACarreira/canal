// The two closed enums in this package could be written but not read back.
//
// Support and CapSource each had a MarshalText and no UnmarshalText, so a Descriptor serialised
// cleanly and then failed to decode from its own encoding:
//
//	json: cannot unmarshal string into Go struct field Descriptor.support of type registry.Support
//
// The compliance audit rated it fatal, and it is the reason no client could round-trip a component
// descriptor — which is the document the whole config-driven UI is meant to be built from.
//
// The same reasoning as pkg/connector's enumtext.go applies: the token is the wire form, and an
// unknown token is an error rather than a silent zero value.
package registry

import "fmt"

// UnmarshalText parses a token written by [Support.MarshalText].
func (s *Support) UnmarshalText(text []byte) error {
	str := string(text)
	for ord, name := range supportNames {
		if name != "" && name == str {
			*s = Support(ord)
			return nil
		}
	}
	return fmt.Errorf("registry: %q is not a valid Support", str)
}

// UnmarshalText parses a token written by [CapSource.MarshalText].
func (s *CapSource) UnmarshalText(text []byte) error {
	str := string(text)
	for val, name := range capSourceNames {
		if name == str {
			*s = val
			return nil
		}
	}
	return fmt.Errorf("registry: %q is not a valid CapSource", str)
}
