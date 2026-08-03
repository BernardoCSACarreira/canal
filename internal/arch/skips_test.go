package arch

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// AN UNCONDITIONALLY SKIPPED TEST IS A TEST THAT IS DECLARED AND NEVER RUNS, which is this module's
// own dominant defect class wearing a different hat.
//
// `go test` prints nothing for a skip unless somebody passes -v, so a suite full of them reports the
// same cheerful `ok` as a suite that runs. The two guards next door exist because a field nothing
// reads and a function nothing calls are invisible; a test nothing executes is invisible in exactly
// the same way, and it is worse, because its presence is what stops anybody writing the test that
// would have run.
//
// So an unconditional skip has to be DECLARED, with the same rule as the other two allowlists: say
// why, and say what would remove it. Deleting an entry is how a test graduates.
//
// CONDITIONAL SKIPS ARE NOT ON THE LIST, and the distinction is the whole difference between a
// useful check and a noisy one. Its first run reported three — two behind testing.Short and one
// behind "cannot open /dev/null" — and every one of them is a test that RUNS in the suite that
// matters, skipping only where it cannot be set up. Reporting those would have made this a check
// people learn to ignore. A skip that is the first statement of a test body is the one that never
// runs anywhere.

// knownSkips are the test files that skip, and why each is allowed to.
//
// Keyed by file rather than by line, because line numbers churn on every edit and a guard that
// fails on unrelated churn is a guard people delete.
// EMPTY IS THE GOAL STATE, and it is worth saying so rather than leaving a bare map. The last entry
// was internal/engine/revocation_test.go, whose sink held records by blocking Write — which starved
// the other lane and left three revocation rules unobservable. It holds by deferring durability
// through Flusher now, both lanes settle together, and the test runs.
var knownSkips = map[string]string{}

func TestEverySkippedTestIsDeclared(t *testing.T) {
	root := repoRoot(t)
	found := map[string]bool{}

	for _, top := range []string{"pkg", "internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if skipsUnconditionally(t, path) {
				rel, _ := filepath.Rel(root, path)
				found[filepath.ToSlash(rel)] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", top, err)
		}
	}

	for file := range found {
		if _, ok := knownSkips[file]; !ok {
			t.Errorf("%s skips a test and is not declared in knownSkips.\n"+
				"  A skip prints nothing without -v, so the suite reports the same ok as one that\n"+
				"  runs — and the skipped test is what stops anybody writing the one that would.\n"+
				"  Add it with why it skips and what would remove the entry, or make it run.", file)
		}
	}
	for file := range knownSkips {
		if !found[file] {
			t.Errorf("%s is declared in knownSkips and no longer skips anything.\n"+
				"  Delete its entry — a stale allowlist is how the next real one hides.", file)
		}
	}
}

// skipsUnconditionally reports whether a file skips a test outside any conditional.
//
// "Outside any conditional" is approximated as a skip appearing at the TOP LEVEL of a function
// body — not nested inside an if, a loop, a switch or a subtest closure. That is where a skip which
// disables a whole test is written, and every conditional skip in this module is nested inside the
// if that guards it.
func skipsUnconditionally(t *testing.T, path string) bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		for _, stmt := range fd.Body.List {
			es, ok := stmt.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := es.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && isSkipName(sel.Sel.Name) {
				return true
			}
		}
	}
	return false
}

func isSkipName(n string) bool {
	return n == "Skip" || n == "Skipf" || n == "SkipNow"
}

// The matcher has to work in both directions, for the same reason the other two do.
//
// BOTH PINS ARE FIXTURES NOW, not real files. The positive pin used to be the one entry in
// knownSkips, which made it a pin that expired the moment that test was fixed — the guard failed
// because the tree got BETTER, which is how a guard earns itself a deletion. testdata/skips holds
// one of each shape and cannot go stale.
func TestTheSkipMatcherWorksInBothDirections(t *testing.T) {
	data := filepath.Join(repoRoot(t), "internal/arch/testdata/skips")

	if !skipsUnconditionally(t, filepath.Join(data, "unconditional/sample.go")) {
		t.Error("the matcher did not find an unconditional skip in the fixture written to contain " +
			"one, so it would find none in the tree either and this whole check passes on a file " +
			"full of them")
	}

	// Two conditional skips, neither of which may be reported: one behind testing.Short and one
	// behind a missing input. A check that reports these reports every legitimate environment guard
	// in the module, and a guard that cries about everything gets muted.
	if skipsUnconditionally(t, filepath.Join(data, "guarded/sample.go")) {
		t.Error("a skip guarded by a condition read as unconditional")
	}
}
