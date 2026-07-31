// Every closed enum in package connector marshals as its stable snake_case token, in both directions.
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
package connector

import "fmt"

// MarshalText writes Boundedness's stable token. See the note at the top of this file.
func (e Boundedness) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Boundedness.MarshalText].
func (e *Boundedness) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range boundednessNames {
		if name != "" && name == s {
			*e = Boundedness(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Boundedness", s)
}

// MarshalText writes DestMode's stable token. See the note at the top of this file.
func (e DestMode) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [DestMode.MarshalText].
func (e *DestMode) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range destModeNames {
		if name != "" && name == s {
			*e = DestMode(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid DestMode", s)
}

// MarshalText writes Disposition's stable token. See the note at the top of this file.
func (e Disposition) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Disposition.MarshalText].
func (e *Disposition) UnmarshalText(text []byte) error {
	s := string(text)
	for val, name := range dispositionNames {
		if name == s {
			*e = val
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Disposition", s)
}

// MarshalText writes Durability's stable token. See the note at the top of this file.
func (e Durability) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Durability.MarshalText].
func (e *Durability) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range durabilityNames {
		if name != "" && name == s {
			*e = Durability(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Durability", s)
}

// MarshalText writes EventKind's stable token. See the note at the top of this file.
func (e EventKind) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [EventKind.MarshalText].
func (e *EventKind) UnmarshalText(text []byte) error {
	s := string(text)
	for val, name := range eventKindNames {
		if name == s {
			*e = val
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid EventKind", s)
}

// MarshalText writes FlushReason's stable token. See the note at the top of this file.
func (e FlushReason) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [FlushReason.MarshalText].
func (e *FlushReason) UnmarshalText(text []byte) error {
	s := string(text)
	for val, name := range flushReasonNames {
		if name == s {
			*e = val
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid FlushReason", s)
}

// MarshalText writes Guarantee's stable token. See the note at the top of this file.
func (e Guarantee) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Guarantee.MarshalText].
func (e *Guarantee) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range guaranteeNames {
		if name != "" && name == s {
			*e = Guarantee(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Guarantee", s)
}

// MarshalText writes LaneKind's stable token. See the note at the top of this file.
func (e LaneKind) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [LaneKind.MarshalText].
func (e *LaneKind) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range laneKindNames {
		if name != "" && name == s {
			*e = LaneKind(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid LaneKind", s)
}

// MarshalText writes Ordering's stable token. See the note at the top of this file.
func (e Ordering) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Ordering.MarshalText].
func (e *Ordering) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range orderingNames {
		if name != "" && name == s {
			*e = Ordering(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Ordering", s)
}

// MarshalText writes Retention's stable token. See the note at the top of this file.
func (e Retention) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [Retention.MarshalText].
func (e *Retention) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range retentionNames {
		if name != "" && name == s {
			*e = Retention(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid Retention", s)
}

// MarshalText writes UnitAssignment's stable token. See the note at the top of this file.
func (e UnitAssignment) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [UnitAssignment.MarshalText].
func (e *UnitAssignment) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range unitAssignmentNames {
		if name != "" && name == s {
			*e = UnitAssignment(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid UnitAssignment", s)
}

// MarshalText writes WhenFull's stable token. See the note at the top of this file.
func (e WhenFull) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses a token written by [WhenFull.MarshalText].
func (e *WhenFull) UnmarshalText(text []byte) error {
	s := string(text)
	for ord, name := range whenFullNames {
		if name != "" && name == s {
			*e = WhenFull(ord)
			return nil
		}
	}
	return fmt.Errorf("connector: %q is not a valid WhenFull", s)
}
