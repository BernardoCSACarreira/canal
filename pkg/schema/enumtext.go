// Every closed enum in package schema marshals as its stable snake_case token, in both directions.
//
// architecture.md §2 already declares this the contract: "every closed enum has exactly one
// String() producing stable snake_case tokens; THOSE TOKENS ARE THE WIRE FORM". The code did not
// implement it, and the compliance audit found the consequences shipping today:
//
//   - A scalar enum marshalled as its ORDINAL ({"guarantee":1}). An ordinal is a wire format whose
//     meaning silently changes the day someone inserts a constant into the middle of an iota block.
//   - A SLICE of one marshalled as BASE64 ({"lane_kinds":"AQ=="}), because a []T over a uint8 base
//     type is a []byte as far as encoding/json is concerned. Both shipped example connectors emit
//     exactly that today, in their registration descriptors.
//   - Nothing round-tripped: a Descriptor could not be decoded from its own encoding.
//
// MarshalText rather than MarshalJSON, for two reasons. encoding/json skips its base64 path for a
// byte-slice element precisely when that element implements encoding.TextMarshaler, so this is what
// actually fixes the slice case; and a TextMarshaler is also usable as a map key, which a
// MarshalJSON is not. registry.CapSource already set the precedent.
//
// An unknown token is an ERROR, not a silent fall back to the zero value. Quietly reading an
// unrecognised token as "the first constant" is how a typo in a config file becomes a delivery tier
// nobody chose.
package schema

import "fmt"

// MarshalText writes ChangeKind's stable token. See the note at the top of this file.
func (e ChangeKind) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [ChangeKind.MarshalText].
func (e *ChangeKind) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range changeKindNames {
		if name != "" && name == s {
			*e = ChangeKind(ord)
			return nil
		}
	}
	return fmt.Errorf("schema: %q is not a valid ChangeKind", s)
}

// MarshalText writes TimeUnit's stable token. See the note at the top of this file.
func (e TimeUnit) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [TimeUnit.MarshalText].
func (e *TimeUnit) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range timeUnitNames {
		if name != "" && name == s {
			*e = TimeUnit(ord)
			return nil
		}
	}
	return fmt.Errorf("schema: %q is not a valid TimeUnit", s)
}

// MarshalText writes Type's stable token. See the note at the top of this file.
func (e Type) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Type.MarshalText].
func (e *Type) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range typeNames {
		if name != "" && name == s {
			*e = Type(ord)
			return nil
		}
	}
	return fmt.Errorf("schema: %q is not a valid Type", s)
}
