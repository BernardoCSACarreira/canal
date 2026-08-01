// Package wal is a durable, single-process [store.StateStore] backed by an append-only log.
//
// It is the first store in canal that survives a restart, and therefore the first that can honestly
// declare a durability domain above [connector.DurabilityNone]. Design rule R3's milestone — one
// record moving from a real source to a real sink with a checkpoint that survives kill -9 — is not
// reachable without it.
//
// # Why hand-rolled
//
// The obvious choice for this job is bbolt, and the architecture document names it. It was not taken
// because canal advertises zero third-party dependencies and now enforces that in CI, and because
// the workload this store actually sees is the one an append-only log is best at: small values, a
// handful of keys per pipeline, overwhelmingly append-and-overwrite, no range scans over millions of
// rows, no secondary indexes. A B-tree buys page-level concurrency and large-dataset performance
// that a checkpoint store does not need.
//
// The cost is real and worth stating plainly: this is crash-correctness code, and crash-correctness
// code is where hand-rolling has its bad reputation. The mitigation is that the recovery path is
// tested by truncating a real log at EVERY byte offset and asserting the store still opens with a
// prefix of the committed batches, plus the same sweep with a corrupted byte at every offset. See
// wal_test.go.
//
// # Durability model
//
//   - One [StateStore.Set] is one frame, appended and fsynced before the call returns. That is what
//     [store.StoreCaps.FlushIsDurable] promises, and phase two of the three-phase commit depends on
//     it exactly.
//   - Atomicity is a property of the frame: a batch is one length-prefixed, CRC-checked unit, so it
//     is either entirely present at replay or entirely absent. There is no partial batch.
//   - Durability is [connector.DurabilityNode]: the bytes survive the process and the machine's page
//     cache, and are lost with the disk. No cluster claim is made, and Build will refuse an
//     exactly-once pipeline that needs more.
//
// # What this is not
//
// It is single-process. Two processes opening one directory will corrupt each other, which the lock
// file makes an error rather than a mystery. Multi-worker deployments need the coordinated store
// that [store.Coordinator] describes, and that is not this.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

const (
	logName     = "state.wal"
	compactName = "state.wal.compacting"
	lockName    = "state.lock"

	// compactMinBytes keeps a small log from being rewritten constantly. Below this, the wasted
	// space is not worth an fsync-heavy rewrite.
	compactMinBytes = 1 << 20

	// compactLiveRatio triggers a rewrite when live data is less than this fraction of the file.
	// A checkpoint store overwrites the same handful of keys forever, so without compaction the log
	// grows without bound while the live set stays tiny.
	compactLiveRatio = 0.5
)

// StateStore is an append-only-log-backed [store.StateStore].
type StateStore struct {
	dir string

	mu   sync.Mutex
	f    *os.File
	lock *os.File

	// data and epochs are the in-memory index, rebuilt from the log at Open. The log is the
	// authority; these are the read path.
	data   map[string]store.Versioned
	epochs map[string]uint64

	// bytes is the log's size, and live is a running estimate of the bytes belonging to entries not
	// yet superseded. Their ratio drives compaction.
	bytes int64
	live  int64

	closed bool
}

// Open opens or creates the store in dir, replaying any existing log.
//
// A torn or corrupt tail is TRUNCATED, not an error: the frames before it were fsynced and are real,
// and the partial one is a write that never completed. Truncating is what makes the next append land
// on a frame boundary instead of after garbage.
func Open(dir string) (*StateStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: creating %s: %w", dir, err)
	}

	lock, err := acquireLock(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}

	s := &StateStore{
		dir:    dir,
		lock:   lock,
		data:   map[string]store.Versioned{},
		epochs: map[string]uint64{},
	}

	// A compaction that was interrupted leaves a temporary file. It is always discardable: the
	// rename that would have made it authoritative never happened, so the original log is intact.
	_ = os.Remove(filepath.Join(dir, compactName))

	if err := s.replay(); err != nil {
		_ = lock.Close()
		return nil, err
	}

	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("wal: opening the log for append: %w", err)
	}
	s.f = f
	return s, nil
}

// replay rebuilds the index from the log and truncates any torn tail.
func (s *StateStore) replay() error {
	path := filepath.Join(s.dir, logName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("wal: opening the log: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat: %w", err)
	}
	if fi.Size() == 0 {
		hdr := append([]byte(magic), byte(formatVersion>>8), byte(formatVersion))
		if _, err := f.Write(hdr); err != nil {
			return fmt.Errorf("wal: writing the header: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("wal: syncing the header: %w", err)
		}
		s.bytes = int64(headerLen)
		return syncDir(s.dir)
	}

	hdr := make([]byte, headerLen)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return fmt.Errorf("wal: %s is shorter than its header; it is not a canal log: %w", path, err)
	}
	if string(hdr[:len(magic)]) != magic {
		return fmt.Errorf("wal: %s does not begin with the canal magic; refusing to touch it", path)
	}
	if v := uint16(hdr[len(magic)])<<8 | uint16(hdr[len(magic)+1]); v != formatVersion {
		// ADR 0020: refuse rather than guess. A store written by a newer binary is not something an
		// older one may reinterpret.
		return fmt.Errorf("wal: %s is format version %d, this binary writes %d", path, v, formatVersion)
	}

	offset := int64(headerLen)
	for {
		p, err := readFrame(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errTornTail) {
			// Everything from here on never completed. Truncate so the next append is frame-aligned.
			if err := f.Truncate(offset); err != nil {
				return fmt.Errorf("wal: truncating a torn tail at %d: %w", offset, err)
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("wal: syncing after truncation: %w", err)
			}
			break
		}
		if err != nil {
			return err
		}

		e, derr := decode(p)
		if derr != nil {
			// A frame whose CRC matched but whose contents do not parse is not a torn write — it is
			// a real inconsistency, and silently dropping it would lose committed state.
			return fmt.Errorf("wal: frame at %d passed its checksum but does not parse: %w", offset, derr)
		}
		s.apply(e)
		offset += int64(frameHeader + len(p))
	}

	s.bytes = offset
	s.live = s.liveBytes()
	return nil
}

// apply folds one decoded entry into the index. Used by replay only; the write path updates the
// index directly so it can reuse the values it already holds.
func (s *StateStore) apply(e entry) {
	for _, w := range e.writes {
		s.data[w.key.String()] = store.Versioned{Key: w.key, Value: w.value, Version: w.version}
		if w.epochSeen > s.epochs[w.key.String()] {
			s.epochs[w.key.String()] = w.epochSeen
		}
	}
	for _, k := range e.deletes {
		delete(s.data, k.String())
	}
}

// Get reads several keys. An absent key is simply missing from the result.
func (s *StateStore) Get(_ context.Context, keys []store.Key) (map[string]store.Versioned, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	out := make(map[string]store.Versioned, len(keys))
	for _, k := range keys {
		if v, ok := s.data[k.String()]; ok {
			out[k.String()] = clone(v)
		}
	}
	return out, nil
}

// Range iterates every key under a prefix, in key order.
//
// The result is a snapshot taken under the lock, so iteration cannot observe a half-applied batch
// and the caller may write while ranging.
func (s *StateStore) Range(_ context.Context, prefix store.Key) (iter.Seq2[store.Key, store.Versioned], error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errClosed
	}
	var names []string
	for name, v := range s.data {
		if v.Key.Prefix(prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	snapshot := make([]store.Versioned, 0, len(names))
	for _, n := range names {
		snapshot = append(snapshot, clone(s.data[n]))
	}
	s.mu.Unlock()

	return func(yield func(store.Key, store.Versioned) bool) {
		for _, v := range snapshot {
			if !yield(v.Key, v) {
				return
			}
		}
	}, nil
}

// Set applies the whole batch or none of it, durably.
//
// The order is: check every precondition, encode, append, fsync, and only then mutate the index. A
// failed append therefore leaves the in-memory state exactly as it was, so the store never claims a
// write the log does not have.
func (s *StateStore) Set(_ context.Context, w store.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	// Pass one: every precondition, before anything is written. A store that cannot do this cannot
	// back any tier above at-least-once.
	for name, v := range w.Writes {
		// Per-key epoch, falling back to the batch's. Using the batch epoch alone would leave a
		// multi-lane atomic write unfenced, which is the only reason StateHandle.SetMany exists:
		// a worker holding 32 lanes at 32 different lease epochs has no single number to offer.
		if seen, ok := s.epochs[name]; ok && w.EpochFor(v) < seen {
			return fault.ErrFenced
		}
		cur, exists := s.data[name]
		switch {
		case v.IfVersion == 0 && exists:
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("wal: %s exists but the write required it not to", name))
		case v.IfVersion != 0 && (!exists || cur.Version != v.IfVersion):
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("wal: %s is at version %d, not %d", name, cur.Version, v.IfVersion))
		}
	}

	// Pass two: build the entry that records what WILL be true, in a deterministic key order so two
	// runs of the same batch produce byte-identical frames.
	names := make([]string, 0, len(w.Writes))
	for name := range w.Writes {
		names = append(names, name)
	}
	sort.Strings(names)

	e := entry{writes: make([]appliedWrite, 0, len(names)), deletes: w.Deletes}
	for _, name := range names {
		v := w.Writes[name]
		epoch := w.EpochFor(v)
		if seen := s.epochs[name]; seen > epoch {
			epoch = seen
		}
		e.writes = append(e.writes, appliedWrite{
			key:       v.Key,
			value:     v.Value,
			version:   s.data[name].Version + 1,
			epochSeen: epoch,
		})
	}

	n, err := s.append(encode(e))
	if err != nil {
		return err
	}

	s.apply(e)
	s.bytes += n
	s.live = s.liveBytes()
	return s.maybeCompact()
}

// Delete removes keys unconditionally, durably.
func (s *StateStore) Delete(_ context.Context, keys []store.Key) error {
	if len(keys) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	e := entry{deletes: keys}
	n, err := s.append(encode(e))
	if err != nil {
		return err
	}
	s.apply(e)
	s.bytes += n
	s.live = s.liveBytes()
	return s.maybeCompact()
}

// append writes one frame and fsyncs it. The caller holds the mutex.
func (s *StateStore) append(frame []byte) (int64, error) {
	if _, err := s.f.Write(frame); err != nil {
		return 0, fault.Unknown(fault.OpPersist, fmt.Errorf("wal: appending: %w", err))
	}
	// The fsync is the whole promise. Without it Set returns before the bytes are durable, which is
	// design rule R4's original violation and breaks phase two of the commit protocol.
	if err := s.f.Sync(); err != nil {
		return 0, fault.Unknown(fault.OpPersist, fmt.Errorf("wal: syncing: %w", err))
	}
	return int64(len(frame)), nil
}

// Capabilities reports what this store can honestly promise.
func (s *StateStore) Capabilities() store.StoreCaps { return Caps() }

// Caps is what every WAL store promises, as a value that needs no open store.
//
// It is a package-level constant rather than only a method because negotiation is a pure function
// of config, and a tool that reports the contract a WAL-backed deployment WILL get must not have to
// open — and therefore exclusively lock — the state directory of a pipeline that may be running.
// Asking "what would this spec negotiate" should never be able to disturb a running pipeline.
//
// TestCapabilitiesNeedNoOpenStore pins this to the method, so the two cannot drift into reporting
// different tiers for the same store.
func Caps() store.StoreCaps {
	return store.StoreCaps{
		// One frame is one atomic unit, so a batch spanning cursors, schema epoch, pending
		// committables and dedupe additions cannot be torn between them.
		AtomicMultiKey: true,
		CAS:            true,
		EpochFencing:   true,
		// The bytes survive the process and the machine. They do not survive the disk, and no
		// cluster claim is made: another worker cannot read this directory.
		Durability:     connector.DurabilityNode,
		FlushIsDurable: true,
	}
}

// Close flushes and releases the store. It is safe to call twice.
func (s *StateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	if s.f != nil {
		if err := s.f.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := s.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.lock != nil {
		if err := releaseLock(s.lock, filepath.Join(s.dir, lockName)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

var errClosed = errors.New("wal: the store is closed")

// clone returns a copy whose Value cannot be mutated through the caller's slice.
//
// Design rule R13's specific historical defect was a read-model store handing back live mutable
// records, so a reader could edit committed state by accident.
func clone(v store.Versioned) store.Versioned {
	if v.Value != nil {
		b := make([]byte, len(v.Value))
		copy(b, v.Value)
		v.Value = b
	}
	v.Key.Parts = append([]string(nil), v.Key.Parts...)
	return v
}

// liveBytes estimates the bytes the current live set would occupy if written fresh. The caller holds
// the mutex.
func (s *StateStore) liveBytes() int64 {
	var n int64
	for _, v := range s.data {
		n += int64(len(v.Value)) + int64(len(v.Key.String())) + 32 // 32 covers the varints and framing
	}
	return n + int64(headerLen)
}

// maybeCompact rewrites the log when most of it is superseded. The caller holds the mutex.
//
// Without it a checkpoint store grows forever: it overwrites the same handful of keys for the life
// of the pipeline, so the live set stays kilobytes while the log reaches gigabytes.
func (s *StateStore) maybeCompact() error {
	if s.bytes < compactMinBytes || float64(s.live) >= float64(s.bytes)*compactLiveRatio {
		return nil
	}
	return s.compactLocked()
}

// compactLocked rewrites the log as one frame per live key, then renames it into place.
//
// The rename is the commit point and it is atomic on POSIX, so a crash at any moment leaves either
// the old complete log or the new complete one — never a half-written store. The directory fsync
// after the rename is what makes the rename itself durable; without it the entry can be lost while
// the file contents survive.
func (s *StateStore) compactLocked() error {
	tmp := filepath.Join(s.dir, compactName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: creating the compaction file: %w", err)
	}

	fail := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}

	hdr := append([]byte(magic), byte(formatVersion>>8), byte(formatVersion))
	if _, err := f.Write(hdr); err != nil {
		return fail(fmt.Errorf("wal: writing the compaction header: %w", err))
	}
	written := int64(headerLen)

	names := make([]string, 0, len(s.data))
	for name := range s.data {
		names = append(names, name)
	}
	sort.Strings(names)

	// One frame per key rather than one giant frame: a single 500 MB frame would exceed maxFrame and
	// would make replay allocate the whole live set at once.
	for _, name := range names {
		v := s.data[name]
		frame := encode(entry{writes: []appliedWrite{{
			key: v.Key, value: v.Value, version: v.Version, epochSeen: s.epochs[name],
		}}})
		if _, err := f.Write(frame); err != nil {
			return fail(fmt.Errorf("wal: writing a compacted frame: %w", err))
		}
		written += int64(len(frame))
	}

	if err := f.Sync(); err != nil {
		return fail(fmt.Errorf("wal: syncing the compaction file: %w", err))
	}
	if err := f.Close(); err != nil {
		return fail(fmt.Errorf("wal: closing the compaction file: %w", err))
	}

	// Swap. The old handle is closed first so Windows can rename over it.
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("wal: closing the live log before the swap: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logName)); err != nil {
		return fmt.Errorf("wal: renaming the compacted log into place: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}

	reopened, err := os.OpenFile(filepath.Join(s.dir, logName), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopening after compaction: %w", err)
	}
	s.f = reopened
	s.bytes = written
	s.live = s.liveBytes()
	return nil
}
