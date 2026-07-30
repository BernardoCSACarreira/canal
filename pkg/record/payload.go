package record

const (
	hasBytes  uint8 = 1 << 0
	hasStruct uint8 = 1 << 1
)

// Payload is a dual-view body: raw bytes and/or a structured value, whichever is
// currently materialised.
//
// CRITICAL PROPERTY: Payload holds no codec, no context and no engine handle, and
// its accessors never convert. A Payload whose Bytes accessor "materialises using
// the pipeline's configured encoder" requires either global mutable codec state or
// an import cycle, and makes a Record untestable in isolation. Here conversion
// happens in the engine's decode and encode nodes, which write the other view back
// into the payload.
//
// A sink that needs bytes and is handed a payload with none is a pipeline the core
// refused at Build time: an encoder is required on every sink node unless the sink
// declares StructuredInput.
//
// Mutability is in the accessor name rather than in a flag, because a Mutable()
// bool is a rule nobody reads.
type Payload struct {
	b   []byte
	v   Value
	has uint8
}

// BytesPayload returns a payload whose only materialised view is b. The caller
// hands over ownership of b.
func BytesPayload(b []byte) Payload { return Payload{b: b, has: hasBytes} }

// StructPayload returns a payload whose only materialised view is v.
func StructPayload(v Value) Payload { return Payload{v: v, has: hasStruct} }

// Bytes returns the encoded body. ok is false when only a structured view exists.
// The caller must not retain or modify the returned slice.
func (p *Payload) Bytes() (b []byte, ok bool) {
	if p.has&hasBytes == 0 {
		return nil, false
	}
	return p.b, true
}

// BytesCopy returns an owned copy of the encoded body.
func (p *Payload) BytesCopy() ([]byte, bool) {
	if p.has&hasBytes == 0 {
		return nil, false
	}
	c := make([]byte, len(p.b))
	copy(c, p.b)
	return c, true
}

// Structured returns a read-only structured view. Mutating it is a contract
// violation; use [Payload.StructuredMut] instead.
func (p *Payload) Structured() (Value, bool) {
	if p.has&hasStruct == 0 {
		return nil, false
	}
	return p.v, true
}

// StructuredMut returns a structured view the caller may mutate, deep-copying
// first so that no upstream holder observes the mutation. It invalidates the byte
// view, because the bytes no longer describe the value.
func (p *Payload) StructuredMut() (Value, bool) {
	if p.has&hasStruct == 0 {
		return nil, false
	}
	p.v = CloneValue(p.v)
	p.b, p.has = nil, hasStruct
	return p.v, true
}

// SetBytes installs the encoded view. It does not clear a structured view: the
// engine's encode node populates both deliberately, so a later stage can still see
// the structure it encoded.
func (p *Payload) SetBytes(b []byte) {
	p.b = b
	p.has |= hasBytes
}

// SetStructured installs the structured view. It does not clear the byte view, for
// the same reason as [Payload.SetBytes].
func (p *Payload) SetStructured(v Value) {
	p.v = v
	p.has |= hasStruct
}

// ClearBytes drops the encoded view, for a transform that has changed the
// structure and must not leave stale bytes behind.
func (p *Payload) ClearBytes() {
	p.b, p.has = nil, p.has&^hasBytes
}

// HasBytes reports whether the encoded view is materialised.
func (p *Payload) HasBytes() bool { return p.has&hasBytes != 0 }

// HasStructured reports whether the structured view is materialised.
func (p *Payload) HasStructured() bool { return p.has&hasStruct != 0 }

// Len returns the encoded length if the byte view exists, else -1. It is -1 rather
// than 0 because "no encoded form yet" and "an empty encoded form" are different
// facts, and byte accounting must not silently treat them alike.
func (p *Payload) Len() int {
	if p.has&hasBytes == 0 {
		return -1
	}
	return len(p.b)
}

// IsEmpty reports whether neither view is materialised.
func (p *Payload) IsEmpty() bool { return p.has == 0 }

// Clone returns a payload that shares nothing with p.
func (p *Payload) Clone() Payload {
	c := Payload{has: p.has}
	if p.has&hasBytes != 0 {
		c.b = make([]byte, len(p.b))
		copy(c.b, p.b)
	}
	if p.has&hasStruct != 0 {
		c.v = CloneValue(p.v)
	}
	return c
}
