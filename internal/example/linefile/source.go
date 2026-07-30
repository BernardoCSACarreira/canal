// Package linefile reads a file one line per record.
//
// It is the smallest useful source, and it is reproduced in full because it is the measurement of the
// whole architecture: if this is not short, the architecture has failed. Everything below the
// registration block is four methods.
//
// For that to be an honest measurement, this package must live where a third-party connector would live
// and import only what a third-party connector may import: record, fault, schema, config, connector and
// registry. It imports no engine, ledger, store, telemetry or spec package, and it could not — the
// core's types are not reachable from here.
package linefile

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
)

// tokenV1 versions this connector's cursor encoding. Everything durable is (version, bytes), and a
// version this build cannot read is a LOUD failure.
const tokenV1 = 1

// maxBatch is how many lines one Read produces at most. The engine's batch has its own hard cap and Add
// returns nil at it, so this is a courtesy rather than a correctness bound.
const maxBatch = 500

func init() {
	registry.AddSource(registry.Default, registry.SourceDef[*src]{
		Meta: registry.Meta{
			Name:    "line_file",
			Version: "1.0.0",
			Title:   "Line-delimited file",
			Summary: "Reads a local file, one record per line.",
			Notes:   "Origin.Key is not populated: a text line has no stable identity.",
			Support: registry.SupportCommunity,
		},

		Spec: config.NewSpec().
			Field(config.Field{
				Name:        "path",
				Type:        config.TypeString,
				Description: "File to read, line by line.",
				Short:       "Absolute or relative path.",
				Examples:    []any{"/var/log/app.log"},
			}),

		Caps: connector.SourceCaps{
			Caps:                connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:     connector.OrderingPrefix,
			Boundedness:         []connector.Boundedness{connector.Bounded},
			LaneKinds:           []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:            1,
			UpstreamRetention:   connector.RetentionUnbounded,
			UnitAssignment:      connector.UnitsStatic,
			Replayable:          true,
			MidLaneResume:       true,
			ComparablePositions: true,
		},

		// New does NO I/O. Config is already parsed, defaulted and validated, so there is no Configure
		// callback and no map to re-parse.
		New: func(_ context.Context, c *config.Config) (*src, error) {
			s := &src{path: config.Must[string](c, "path")}
			return s, c.Err()
		},
	})
}

type src struct {
	path string
	rt   connector.SourceRuntime
	lane record.LaneID
	f    *os.File
	sc   *bufio.Scanner
	off  int64
	done bool
}

func (s *src) Open(ctx context.Context, rt connector.SourceRuntime) error {
	s.rt = rt

	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		// Cold start. Announce is durable before it returns.
		s.lane, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
			Name:        s.path,
			Stream:      "lines",
			Kind:        connector.LaneKindScan,
			Ordering:    connector.OrderingPrefix,
			Boundedness: connector.Bounded,
			Group:       "scan",
			Label:       "reading " + s.path,
		})
		if err != nil {
			return err
		}
	} else {
		// Warm start. The cursor is the same bytes this connector authored, handed back verbatim.
		s.lane = as[0].ID
		switch tok := as[0].Cursor.Token; {
		case tok.IsZero():
			// No progress yet.
		case tok.Version == tokenV1:
			s.off = int64(binary.BigEndian.Uint64(tok.Bytes))
		default:
			// Rule three of the format contract says never reject a NEWER version when the format is
			// additive. This encoding is fixed-width, so a different version genuinely is unreadable,
			// and failing loudly with both numbers named is correct.
			return fault.Contract(fault.OpOpen,
				fmt.Errorf("cursor version %d unreadable by build %d", tok.Version, tokenV1))
		}
	}

	if s.f, err = os.Open(s.path); err != nil {
		return fault.Transient(fault.OpOpen, err) // Open is retried with backoff
	}
	if _, err = s.f.Seek(s.off, io.SeekStart); err != nil {
		return fault.Permanent(fault.OpOpen, err)
	}
	s.sc = bufio.NewScanner(s.f)
	return nil
}

func (s *src) Read(ctx context.Context, dst *record.Batch) error {
	if s.done {
		return fault.ErrEndOfInput
	}
	dst.Reset() // dst.Lane is already this lane; a source must never retarget it

	for dst.Len() < maxBatch && s.sc.Scan() {
		line := s.sc.Bytes()
		s.off += int64(len(line)) + 1
		r := dst.Add() // identity and provenance are ALREADY stamped
		if r == nil {
			break
		}
		r.Payload = record.BytesPayload(slices.Clone(line))
	}
	if err := s.sc.Err(); err != nil {
		return fault.Transient(fault.OpRead, err)
	}

	// The final batch may be empty and still carry both EndOfLane and the last position. A
	// zero-record positioned batch is admitted with zero references and resolves in prefix
	// order, so the closing offset is committed rather than wedging the lane.
	if dst.Len() == 0 {
		s.done = true
		dst.EndOfLane = true
	}
	dst.Position = s.at()
	return nil
}

// at renders the current offset as a Position. Order and Scalar are free here because a
// big-endian uint64 IS an order-preserving encoding of a byte offset, and supplying them buys
// mid-lane monotonicity assertions and a progress percentage with no further code.
func (s *src) at() record.Position {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(s.off))
	scalar := float64(s.off)
	return record.Position{
		Token:  record.Blob{Version: tokenV1, Bytes: b[:]},
		Order:  b[:],
		Scalar: &scalar,
		Safe:   true, // every line boundary is a legal resume point
		At:     time.Now(),
		Label:  fmt.Sprintf("byte %d", s.off),
	}
}

// Commit does nothing: this source's progress IS canal's cursor, which the core already persisted in
// phase two before calling here. A source with an upstream that needs telling — a replication slot, a
// consumer group, a queue delete — does it here.
func (s *src) Commit(context.Context, connector.Ack) error { return nil }

// Close is safe on a never-opened source, because the core calls it after a failed Open and after config
// validation.
func (s *src) Close(context.Context) error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}
