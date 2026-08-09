// Fuzz targets for the two frame decoders — ADR 0032 rule 5's standing price of a format bump, and
// the completeness audit's unowned WAL fuzz target, owned.
//
// The property under fuzz is NARROW AND ABSOLUTE: no input may panic or allocate unboundedly.
// Wrong-but-graceful is fine — a corrupt payload returning an error is the design working — because
// replay's caller distinguishes torn tails from parse failures, not this layer. The cursor's
// bounds-checking is what carries the property, and these are the tests that keep it honest as the
// format grows versions.
package wal

import (
	"bytes"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/store"
)

func fuzzSeeds() [][]byte {
	k := store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: []string{"p1", "l1"}}
	var seeds [][]byte

	// A payload with a write and an epoch-carrying delete, as the current encoder emits it.
	full := encode(entry{
		writes:  []appliedWrite{{key: k, value: []byte("v"), version: 3, epochSeen: 7}},
		deletes: []appliedDelete{{key: k, epoch: 9}},
	})
	seeds = append(seeds, full[frameHeader:])

	// A version-1 delete payload: the key alone, crafted the way writeV1Log pins it.
	p := []byte{opBatch}
	p = appendUvarint(p, 0)
	p = appendUvarint(p, 1)
	p = appendKey(p, k)
	seeds = append(seeds, p)

	seeds = append(seeds, []byte{}, []byte{opBatch})
	return seeds
}

func FuzzDecodeBothVersions(f *testing.F) {
	for _, s := range fuzzSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, p []byte) {
		// Both readable versions over every input: a payload that parses under one and panics
		// under the other is exactly the class of defect a version bump introduces.
		_, _ = decode(p, formatV1)
		_, _ = decode(p, formatVersion)
	})
}

func FuzzReadFrame(f *testing.F) {
	for _, s := range fuzzSeeds() {
		full := make([]byte, frameHeader, frameHeader+len(s))
		full[0], full[1], full[2], full[3] = byte(len(s)>>24), byte(len(s)>>16), byte(len(s)>>8), byte(len(s))
		f.Add(append(full, s...))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = readFrame(bytes.NewReader(b))
	})
}
