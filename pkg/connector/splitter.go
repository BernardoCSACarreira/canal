package connector

import "github.com/BernardoCSACarreira/canal/pkg/record"

// Splitter breaks a batch that exceeds a sink's declared [SinkCaps.MaxRequestRecords] or
// [SinkCaps.MaxRequestBytes] into batches that do not.
//
// The inverse of batching exists because sinks almost always have a hard maximum request
// size, and a framework that can only combine and never divide leaves the author to
// rediscover the limit at runtime.
type Splitter struct {
	maxRecords int
	maxBytes   int64
}

// NewSplitter returns a splitter enforcing the given limits. A limit of zero or less means
// unlimited on that axis; a splitter with no limits passes batches through unchanged.
func NewSplitter(maxRecords int, maxBytes int64) *Splitter {
	return &Splitter{maxRecords: maxRecords, maxBytes: maxBytes}
}

// Split divides in into batches within the configured limits, appending them to out and
// returning the extended slice so the engine reuses one slice per node.
//
// The returned batches share in's allocator and settlement group: splitting is a change of
// framing, never of identity, so no record gets a new id and no group resolves early. When
// in already fits, the single element appended is in itself.
//
// A single record larger than maxBytes is emitted alone rather than dropped: the sink must be
// the one to refuse it, with a per-record mapping fault naming the record, because only the
// sink knows whether its limit is a hard protocol bound or a soft preference.
func (s *Splitter) Split(in *record.Batch, out []*record.Batch) []*record.Batch {
	if in == nil {
		return out
	}
	if s == nil || (s.maxRecords <= 0 && s.maxBytes <= 0) || s.fits(in) {
		return append(out, in)
	}

	cur := record.NewBatchLike(in, s.chunkCap(in.Len()))
	var curBytes int64
	for _, r := range in.Records {
		n := int64(0)
		if l := r.Payload.Len(); l > 0 {
			n = int64(l)
		}
		overRecords := s.maxRecords > 0 && cur.Len() >= s.maxRecords
		overBytes := s.maxBytes > 0 && cur.Len() > 0 && curBytes+n > s.maxBytes
		if overRecords || overBytes {
			out = append(out, cur)
			cur = record.NewBatchLike(in, s.chunkCap(in.Len()))
			curBytes = 0
		}
		cur.Records = append(cur.Records, r)
		curBytes += n
	}
	if cur.Len() > 0 {
		// Only the final chunk of a bounded lane may carry the end-of-lane marker, and
		// only the final chunk may carry the position: an earlier chunk's records are not
		// all settled when it is written.
		cur.Position = in.Position
		cur.EndOfLane = in.EndOfLane
		out = append(out, cur)
	}
	return out
}

func (s *Splitter) fits(b *record.Batch) bool {
	if s.maxRecords > 0 && b.Len() > s.maxRecords {
		return false
	}
	if s.maxBytes > 0 && b.Bytes() > s.maxBytes {
		return false
	}
	return true
}

func (s *Splitter) chunkCap(total int) int {
	if s.maxRecords > 0 && s.maxRecords < total {
		return s.maxRecords
	}
	if total < 1 {
		return 1
	}
	return total
}
