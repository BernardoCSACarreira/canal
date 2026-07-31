//go:build unix

package wal

import (
	"fmt"
	"os"
	"syscall"
)

// syncDir fsyncs a directory, which is what makes a rename durable.
//
// Syncing the renamed FILE is not enough: the directory entry pointing at it is separate metadata,
// and a crash can leave the file's contents on disk with the old name still in the directory. This
// is the classic mistake in atomic-rename code and it only shows up on power loss, which is to say
// never in testing and once in production.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: opening %s to sync it: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: syncing %s: %w", dir, err)
	}
	return nil
}

// acquireLock takes an exclusive advisory lock on path.
//
// flock is used rather than a PID file because the kernel releases it when the process dies, however
// it dies. A PID file survives a kill -9 and then refuses to open a store that nothing is using —
// which, for a store whose whole purpose is surviving kill -9, would be an unfortunate way to fail.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening the lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: %s is already open in another process: %w", path, err)
	}
	return f, nil
}

// releaseLock drops the lock. The file is left in place: its existence means nothing, only the flock
// does, so removing it would race another process that has already opened it.
func releaseLock(f *os.File, _ string) error {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}
