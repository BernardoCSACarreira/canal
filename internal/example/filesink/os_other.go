//go:build !unix

package filesink

// syncParent is a no-op where a directory cannot be opened as a file.
//
// LABELLED SCAFFOLDING (design rule R10). On Windows a directory handle cannot be fsynced through
// os.Open, so the durability of a newly created file's DIRECTORY ENTRY is whatever the filesystem
// gives. The file's own contents are still fsynced by every Write; what is weaker is only the
// crash-window in which the file exists but may not yet be named.
//
// It is a no-op rather than an error because refusing to run on a platform over a narrow crash
// window would be worse than running with the window documented.
func syncParent(string) error { return nil }
