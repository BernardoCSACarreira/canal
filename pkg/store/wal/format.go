package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// The on-disk format is written out explicitly rather than delegated to encoding/gob, because ADR
// 0020 makes the persisted state format a compatibility surface with a stated contract. A format
// nobody can read without running the program that wrote it is not a contract, and gob's encoding
// changes with the Go type it was derived from — a field rename would silently become a format
// break.
//
// FILE
//
//	magic    8 bytes  "CANALWAL"
//	version  2 bytes  big-endian; formatVersion below
//	then a sequence of FRAMEs to end of file
//
// FRAME
//
//	length   4 bytes  big-endian, byte count of payload
//	crc      4 bytes  big-endian, Castagnoli CRC-32 of payload
//	payload  length bytes
//
// The length-then-CRC pairing is what makes a torn tail recoverable rather than fatal. A process
// killed mid-append leaves either a short frame (length exceeds what remains) or a complete-looking
// frame whose CRC fails. Both are detected, and replay stops at the first one: everything before it
// was fsynced and is real, everything from it on never happened. See [Open].
//
// PAYLOAD is a batch of applied mutations — a REDO log, not a request log. Preconditions were
// already checked when the batch was accepted, and the version each key landed at is recorded, so
// replay is a straight apply with no compare-and-set re-evaluation and no dependence on replay
// order matching the original concurrency.
//
//	u8      op: opBatch
//	uvarint number of writes
//	  KEY, bytes value, uvarint version, uvarint epochSeen
//	uvarint number of deletes
//	  KEY
//
// KEY
//
//	string tenant, string space, uvarint number of parts, string part...
const (
	magic         = "CANALWAL"
	formatVersion = uint16(1)
	headerLen     = len(magic) + 2
	frameHeader   = 8 // 4 length + 4 crc

	opBatch = uint8(1)

	// maxFrame bounds a single frame so that a corrupt length cannot make replay allocate wildly.
	// A checkpoint batch is kilobytes; 64 MiB is four orders of magnitude of headroom.
	maxFrame = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errTornTail signals that replay reached an incomplete or corrupt frame. It is not an error the
// caller sees: [Open] treats it as the end of the durable prefix, which is what it is.
var errTornTail = errors.New("wal: torn tail")

// entry is one atomic batch as it was applied.
type entry struct {
	writes  []appliedWrite
	deletes []store.Key
}

type appliedWrite struct {
	key       store.Key
	value     []byte
	version   uint64
	epochSeen uint64
}

// --- encoding ---------------------------------------------------------------

func appendUvarint(b []byte, v uint64) []byte { return binary.AppendUvarint(b, v) }

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendString(b []byte, s string) []byte { return appendBytes(b, []byte(s)) }

func appendKey(b []byte, k store.Key) []byte {
	b = appendString(b, string(k.Tenant))
	b = appendString(b, string(k.Space))
	b = appendUvarint(b, uint64(len(k.Parts)))
	for _, p := range k.Parts {
		b = appendString(b, p)
	}
	return b
}

// encode renders an entry as a framed payload ready to append.
func encode(e entry) []byte {
	p := make([]byte, 0, 256)
	p = append(p, opBatch)
	p = appendUvarint(p, uint64(len(e.writes)))
	for _, w := range e.writes {
		p = appendKey(p, w.key)
		p = appendBytes(p, w.value)
		p = appendUvarint(p, w.version)
		p = appendUvarint(p, w.epochSeen)
	}
	p = appendUvarint(p, uint64(len(e.deletes)))
	for _, k := range e.deletes {
		p = appendKey(p, k)
	}

	frame := make([]byte, frameHeader, frameHeader+len(p))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(p)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(p, castagnoli))
	return append(frame, p...)
}

// --- decoding ---------------------------------------------------------------

// cursor reads a payload. Every read is bounds-checked, because the bytes may be corrupt: a decoder
// that panics on a bad length turns a recoverable torn tail into a crash loop.
type cursor struct {
	b   []byte
	i   int
	err error
}

func (c *cursor) fail(format string, args ...any) {
	if c.err == nil {
		c.err = fmt.Errorf(format, args...)
	}
}

func (c *cursor) u8() uint8 {
	if c.err != nil {
		return 0
	}
	if c.i >= len(c.b) {
		c.fail("wal: payload truncated reading a byte at %d", c.i)
		return 0
	}
	v := c.b[c.i]
	c.i++
	return v
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.i:])
	if n <= 0 {
		c.fail("wal: payload truncated reading a varint at %d", c.i)
		return 0
	}
	c.i += n
	return v
}

func (c *cursor) bytes() []byte {
	if c.err != nil {
		return nil
	}
	n := c.uvarint()
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.b)-c.i) {
		c.fail("wal: payload claims a %d-byte field with %d bytes left", n, len(c.b)-c.i)
		return nil
	}
	// Copied, not aliased: the payload buffer is reused across frames during replay, and a value
	// aliasing it would mutate under the index.
	out := make([]byte, n)
	copy(out, c.b[c.i:c.i+int(n)])
	c.i += int(n)
	return out
}

func (c *cursor) str() string { return string(c.bytes()) }

func (c *cursor) key() store.Key {
	k := store.Key{
		Tenant: record.TenantID(c.str()),
		Space:  store.Space(c.str()),
	}
	n := c.uvarint()
	if c.err != nil {
		return k
	}
	if n > uint64(len(c.b)-c.i) {
		c.fail("wal: key claims %d parts with %d bytes left", n, len(c.b)-c.i)
		return k
	}
	k.Parts = make([]string, 0, n)
	for i := uint64(0); i < n; i++ {
		k.Parts = append(k.Parts, c.str())
	}
	return k
}

// decode parses one payload.
func decode(p []byte) (entry, error) {
	c := &cursor{b: p}
	var e entry

	if op := c.u8(); op != opBatch {
		return e, fmt.Errorf("wal: unknown opcode %d", op)
	}
	nw := c.uvarint()
	if c.err != nil {
		return e, c.err
	}
	if nw > uint64(len(p)) {
		return e, fmt.Errorf("wal: payload claims %d writes in %d bytes", nw, len(p))
	}
	e.writes = make([]appliedWrite, 0, nw)
	for i := uint64(0); i < nw; i++ {
		w := appliedWrite{key: c.key(), value: c.bytes()}
		w.version = c.uvarint()
		w.epochSeen = c.uvarint()
		if c.err != nil {
			return entry{}, c.err
		}
		e.writes = append(e.writes, w)
	}

	nd := c.uvarint()
	if c.err != nil {
		return e, c.err
	}
	if nd > uint64(len(p)) {
		return e, fmt.Errorf("wal: payload claims %d deletes in %d bytes", nd, len(p))
	}
	e.deletes = make([]store.Key, 0, nd)
	for i := uint64(0); i < nd; i++ {
		e.deletes = append(e.deletes, c.key())
	}
	if c.err != nil {
		return entry{}, c.err
	}
	return e, nil
}

// readFrame reads one frame from r.
//
// It returns errTornTail for any incomplete or CRC-failing frame, which the caller treats as the end
// of the durable prefix rather than as corruption to report. That is the whole recovery model: a
// frame is durable only once its bytes AND its checksum are on disk, so a partial frame is a write
// that never happened.
func readFrame(r io.Reader) ([]byte, error) {
	var h [frameHeader]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF // a clean end, exactly on a frame boundary
		}
		return nil, errTornTail // a partial header
	}
	n := binary.BigEndian.Uint32(h[0:4])
	want := binary.BigEndian.Uint32(h[4:8])
	if n == 0 || n > maxFrame {
		return nil, errTornTail
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r, p); err != nil {
		return nil, errTornTail
	}
	if crc32.Checksum(p, castagnoli) != want {
		return nil, errTornTail
	}
	return p, nil
}
