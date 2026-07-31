// Every closed enum in package fault marshals as its stable snake_case token, in both directions.
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
package fault

import "fmt"

// MarshalText writes Blame's stable token. See the note at the top of this file.
func (e Blame) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Blame.MarshalText].
func (e *Blame) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range blameNames {
		if name != "" && name == s {
			*e = Blame(ord)
			return nil
		}
	}
	return fmt.Errorf("fault: %q is not a valid Blame", s)
}

// MarshalText writes Class's stable token. See the note at the top of this file.
func (e Class) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Class.MarshalText].
func (e *Class) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range classNames {
		if name != "" && name == s {
			*e = Class(ord)
			return nil
		}
	}
	return fmt.Errorf("fault: %q is not a valid Class", s)
}

// MarshalText writes Indeterminacy's stable token. See the note at the top of this file.
func (e Indeterminacy) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Indeterminacy.MarshalText].
func (e *Indeterminacy) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range indeterminacyNames {
		if name != "" && name == s {
			*e = Indeterminacy(ord)
			return nil
		}
	}
	return fmt.Errorf("fault: %q is not a valid Indeterminacy", s)
}

// MarshalText writes Op's stable token. See the note at the top of this file.
func (e Op) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Op.MarshalText].
func (e *Op) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range opNames {
		if name != "" && name == s {
			*e = Op(ord)
			return nil
		}
	}
	return fmt.Errorf("fault: %q is not a valid Op", s)
}

// MarshalText writes Terminal's stable token. See the note at the top of this file.
func (e Terminal) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Terminal.MarshalText].
func (e *Terminal) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range terminalNames {
		if name != "" && name == s {
			*e = Terminal(ord)
			return nil
		}
	}
	return fmt.Errorf("fault: %q is not a valid Terminal", s)
}
