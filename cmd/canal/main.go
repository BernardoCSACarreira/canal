// Command canal runs one pipeline from a specification file.
//
// This is the standalone deployment shape: one process, one spec, a local write-ahead log for
// state. The horizontally scaled shape swaps the four stores in [engine.Deps] and changes nothing
// else — which is the claim this binary exists to keep testable, because a composition root that
// only ever runs in a test is a composition root nobody has checked.
//
// WHY THIS PACKAGE IS AS THIN AS IT IS. Everything here is wiring: read a file, open a store, build,
// run, handle a signal, choose an exit code. There is no policy in it. A decision that belongs to
// the engine and is made here instead is a decision the library cannot be trusted to make on its
// own, and the enterprise deployment would have to re-implement it.
//
// STDOUT BELONGS TO THE DATA. The stdout sink writes records to file descriptor 1, so every log
// line, diagnostic and summary this program produces goes to STDERR without exception. Writing one
// status line to stdout corrupts the output stream of every pipeline that ends in a byte sink, and
// it does so in a way that looks like a connector bug.
package main

import (
	"fmt"
	"os"
	"runtime/debug"
)

// version is overridable at link time with -ldflags "-X main.version=v1.2.3". When it is not set,
// buildVersion falls back to the VCS stamp the toolchain embeds.
var version string

const usage = `canal moves records from a source to a sink.

usage:
  canal run    --spec FILE --state DIR [flags]   run a pipeline until its input ends
  canal check  --spec FILE                       build a pipeline and report what it negotiated
  canal version                                  print the build version

run 'canal <command> -h' for the flags of a command.

exit codes:
  0  the pipeline finished and everything it read is durable
  1  the pipeline failed while running
  2  the command line or the spec file could not be read
  3  the spec was refused; the diagnostics say which field and why
`

// Exit codes are part of this program's interface: a supervisor decides whether to restart from
// them. A spec that will never build (3) must be distinguishable from a run that failed and might
// succeed on retry (1), or a crash loop is the operator's only feedback.
const (
	exitOK      = 0
	exitFailed  = 1
	exitUsage   = 2
	exitRefused = 3
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return exitUsage
	}

	switch args[0] {
	case "run":
		return cmdRun(args[1:])
	case "check":
		return cmdCheck(args[1:])
	case "version":
		fmt.Fprintln(os.Stderr, buildVersion())
		return exitOK
	case "-h", "--help", "help":
		fmt.Fprint(os.Stderr, usage)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "canal: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}

// buildVersion reports the version to stamp into checkpoints, so an operator reading state can tell
// which build wrote it.
//
// The link-time value wins. Failing that the VCS revision the Go toolchain embeds is used, with
// +dirty appended when the working tree had uncommitted changes — because "this state was written
// by an unreproducible build" is exactly the thing worth knowing while debugging one.
func buildVersion() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	var rev, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	if rev == "" {
		return "devel"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	return rev + dirty
}
