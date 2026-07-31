package stdoutsink

import (
	"os"
	"testing"
)

// TestSyncUnsupportedCoversTheRealTargets uses actual file descriptors rather than asserting against
// a list of errnos, because the errno differs per platform and per target and a hand-written list is
// exactly the thing that goes stale.
//
// The audit's finding: the guard tolerated only os.ErrInvalid, so `canal | head`, `canal > /dev/null`
// and running attached to a terminal each turned a successful write into an Indeterminate fault with
// Written=0 — on the happy path, every time.
func TestSyncUnsupportedCoversTheRealTargets(t *testing.T) {
	t.Run("/dev/null", func(t *testing.T) {
		f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			t.Skipf("cannot open %s: %v", os.DevNull, err)
		}
		defer f.Close()
		if _, err := f.WriteString("x"); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := f.Sync(); err != nil && !syncUnsupported(err) {
			t.Errorf("Sync on %s returned %v, which the guard treats as a real failure", os.DevNull, err)
		}
	})

	t.Run("pipe", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Skipf("cannot create a pipe: %v", err)
		}
		defer r.Close()
		defer w.Close()
		if _, err := w.WriteString("x"); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.Sync(); err != nil && !syncUnsupported(err) {
			t.Errorf("Sync on a pipe returned %v, which the guard treats as a real failure — this is the `canal | head` case", err)
		}
	})

	// The other direction: a regular file must still sync cleanly, so the guard is not hiding a real
	// durability failure on the one target where fsync actually means something.
	t.Run("regular file still syncs", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "sink")
		if err != nil {
			t.Fatalf("temp: %v", err)
		}
		defer f.Close()
		if _, err := f.WriteString("x"); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Errorf("Sync on a regular file failed: %v", err)
		}
	})

	t.Run("a genuine error is not swallowed", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "sink")
		if err != nil {
			t.Fatalf("temp: %v", err)
		}
		f.Close() // syncing a closed *os.File yields os.ErrClosed, which is not an fsync-unsupported answer
		err = f.Sync()
		if err == nil {
			t.Skip("this platform allows Sync on a closed file")
		}
		if syncUnsupported(err) {
			t.Errorf("the guard swallowed %v; it must only tolerate 'this fd cannot be synced'", err)
		}
	})
}
