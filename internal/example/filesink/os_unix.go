//go:build unix

package filesink

import (
	"fmt"
	"os"
	"path/filepath"
)

// syncParent fsyncs the directory holding path, which is what makes a newly created file durable.
//
// The file's contents and the directory entry naming them are separate metadata. Syncing only the
// file can leave a crash-recovered filesystem holding the bytes with nothing pointing at them. It
// is the same hazard pkg/store/wal's syncDir documents, and it costs one fsync at Open.
func syncParent(path string) error {
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("filesink: opening %s to sync it: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("filesink: syncing %s: %w", dir, err)
	}
	return nil
}
