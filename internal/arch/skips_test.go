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
var knownSkips = map[string]string{
	"internal/engine/revocation_test.go": "the revocation fixture. Two lanes are announced, both " +
		"are claimed, the run stays alive and the revocation is noticed — but the sink holds by " +
		"BLOCKING Write and the engine writes serially per sink node, so the first held record " +
		"starves the other lane. Removing this entry means holding by deferring durability through " +
		"Flusher instead, which is what lets both lanes settle together. Until then three revocation " +
		"rules have no test that can observe them.",
}

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

// The matcher has to work in both directions, for the same reason the other two do. This one is
// cheap to pin because it can check itself: this file contains no skip and the one it names does.
func TestTheSkipMatcherWorksInBothDirections(t *testing.T) {
	root := repoRoot(t)

	declared := filepath.Join(root, "internal/engine/revocation_test.go")
	if !skipsUnconditionally(t, declared) {
		t.Error("the file knownSkips names does not skip unconditionally; either the entry is stale " +
			"or the matcher is looking for the wrong thing, and both make this pass on a tree full " +
			"of skips")
	}

	// And the negative pin: a file whose only skips are behind a condition must NOT be reported.
	// cmd/canal skips two tests under -short, which is a test that runs in the suite that matters.
	if skipsUnconditionally(t, filepath.Join(root, "cmd/canal/main_test.go")) {
		t.Error("a skip guarded by testing.Short reads as unconditional; this check would then " +
			"report every legitimate environment guard in the module")
	}
}
