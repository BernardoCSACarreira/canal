//go:build !unix

package wal

import (
	"fmt"
	"os"
)

// syncDir is a no-op off Unix.
//
// Windows has no directory handle to fsync, and NTFS orders metadata differently: a rename is
// committed through the filesystem's own journal rather than needing an explicit parent sync. There
// is nothing useful to call here, and pretending otherwise with an error would make the store
// unusable on a platform where the guarantee already holds.
func syncDir(string) error { return nil }

// acquireLock takes an exclusive lock by creating a file that must not already exist.
//
// This is weaker than the Unix flock path, and the difference is worth stating: the kernel does not
// release this on process death, so a crash leaves a stale lock file that the next Open refuses.
// The error names the file so the fix is obvious, but it IS a manual fix. A store whose reason for
// existing is surviving kill -9 deserves better than this on every platform, and closing that gap
// needs LockFileEx through golang.org/x/sys/windows — a third-party dependency the project does not
// take. Revisit if Windows becomes a real target rather than a compile check.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf(
			"wal: cannot take the lock at %s; another process holds the store, or a previous one "+
				"crashed and the file must be removed by hand: %w", path, err)
	}
	return f, nil
}

// releaseLock drops the lock by removing the file it is made of.
func releaseLock(f *os.File, path string) error {
	err := f.Close()
	if rmErr := os.Remove(path); rmErr != nil && err == nil {
		err = rmErr
	}
	return err
}
