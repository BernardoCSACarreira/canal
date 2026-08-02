package arch

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// DECLARED AND UNREACHABLE is the inert defect one level up, and it is the one that has been getting
// through.
//
// inert_test.go catches a FIELD nothing reads. It cannot see a FUNCTION nothing calls, and every
// expensive find of the last several sessions has been exactly that:
//
//   - Ledger.Revoke. A complete fence — three places in the ledger consult the flag it sets — with no
//     callers anywhere in the module. The entire multi-worker safety argument rested on it, and it
//     was dead code for as long as it had existed.
//   - Record.MarkFailed's routing. The method set a flag, Record.Failed read it back, and nothing in
//     the engine ever asked: a source could declare its own record broken and the record was
//     delivered anyway.
//   - store.ConfigStore, before Deps.Config had a reader. That one the field check DID catch, which
//     is the point: the same defect is invisible the moment it hides behind a function instead.
//
// THE RULE. An exported function or method declared in the core, whose name is REFERENCED NOWHERE
// in any non-test file — not even by its own package — is unreachable.
//
// Not "no caller outside its package", which was the first version of this rule and was wrong. Most
// of what a large package exports is called by its own siblings: Checkpoint.WrittenByNewerBuild is
// called from commit.go, four files along, and the outside-the-package rule reported it as dead. The
// question that catches the real ones is whether anything calls it AT ALL.
//
// Tests are excluded deliberately and that is what makes this find anything: Ledger.Revoke had test
// coverage. A function exercised only by its own unit test is one no production path reaches — the
// test proves it WORKS while nothing proves it RUNS.
//
// The match is by NAME, like the field check next door, because resolving types needs a full
// type-check of the module and the honest alternative is a check that does not exist. Name matching
// is generous in one direction — an interface method is "called" by any same-named call anywhere,
// which is what keeps every connector implementation off the report — and that is the safe
// direction: this reports too little rather than too much, and what it does report is real.

// unreachableFuncs are the exported functions with no non-test caller outside their package, and why
// each is allowed to have none.
//
// Removing an entry is how a function graduates. Adding one should feel like a decision, and the
// reason should say what would call it and when.
var unreachableFuncs = map[string]string{
	"internal/ledger.Expand": "fan-out for a transform node, and no component of that kind is " +
		"registered anywhere in the module, so there is nothing to expand a group for. It is the " +
		"same gap config.FieldBatching is on the field allowlist for.",
	"internal/example/memstore.NewCoordinator": "scaffolding. The in-memory coordinator exists to " +
		"give the engine's lease tests a real placement protocol to run against; cmd/canal builds " +
		"the standalone shape, which has no coordinator at all.",
	"internal/example/memstore.NewStatus": "scaffolding, like NewCoordinator. The in-memory status " +
		"store exists to give a worker somewhere real to publish its read model; cmd/canal builds " +
		"the standalone shape, where the document is complete by construction and nothing aggregates.",
	"internal/example/memstore.Reports": "reads back what a worker published, which is the only " +
		"observation this store supports until Aggregate exists — and it is the point of the store: " +
		"a report nothing can read back is a write to /dev/null with extra steps.",
	"internal/example/memstore.Dropped": "the watch buffer's drop counter, and it is read only by " +
		"the test that asserts a full watcher loses events rather than blocking a Put. A drop " +
		"policy nothing can observe is indistinguishable from a delivery guarantee that happens to " +
		"hold on small inputs, which is why the accessor exists at all.",
}

// coreDirs are the packages this check holds to the rule.
//
// internal/ only. pkg/ is the CONNECTOR-FACING API: an exported function there with no in-module
// caller is a function third-party connectors call, which is its job rather than a defect, and
// holding pkg/ to this rule would produce an allowlist the size of the package. internal/stress is
// the conformance corpus — connectors that exist to be run BY tests — and internal/arch is this
// file.
func coreDirs(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		switch slash := filepath.ToSlash(rel); {
		case slash == "internal":
			return nil
		case slash == "internal/arch", strings.HasPrefix(slash, "internal/stress"):
			return filepath.SkipDir
		case strings.Contains(slash, "/testdata"):
			return filepath.SkipDir
		}
		if hasGoFiles(path) {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking internal: %v", err)
	}
	sort.Strings(out)
	return out
}

func hasGoFiles(dir string) bool {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
			return true
		}
	}
	return false
}

func TestNoExportedFunctionIsUnreachable(t *testing.T) {
	root := repoRoot(t)
	dirs := coreDirs(t, root)
	if len(dirs) == 0 {
		t.Fatal("no core packages were found, so this check proves nothing")
	}

	referenced := referencedIdents(t, root)
	for _, dir := range dirs {
		rel := filepath.ToSlash(mustRel(t, root, dir))
		declared := exportedFuncsIn(t, dir)
		if len(declared) == 0 {
			continue
		}
		used := referenced
		for _, fn := range declared {
			key := rel + "." + fn
			if _, allowed := unreachableFuncs[key]; allowed {
				if used[fn] {
					t.Errorf("%s is on the unreachable allowlist but something calls it now.\n"+
						"  Delete its entry — a stale allowlist hides the next real one", key)
				}
				continue
			}
			if !used[fn] {
				t.Errorf("%s is exported and nothing in the module references it.\n"+
					"  A function whose only exercise is its own test is one no production path\n"+
					"  reaches: the test proves it works while nothing proves it runs. That is how\n"+
					"  Ledger.Revoke — the fence the whole multi-worker safety argument rests on —\n"+
					"  spent its entire existence as dead code.\n"+
					"  Wire it up, unexport it, delete it, or add it to unreachableFuncs with the reason.",
					key)
			}
		}
	}
}

// exportedFuncsIn returns the exported function and method names a package declares in non-test files.
func exportedFuncsIn(t *testing.T, dir string) []string {
	t.Helper()
	seen := map[string]bool{}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || !fd.Name.IsExported() {
				continue
			}
			// New is a constructor convention whose name collides across every package, so matching it
			// would match anything.
			if fd.Name.Name == "New" && fd.Recv == nil {
				continue
			}
			// A METHOD ON AN UNEXPORTED TYPE CANNOT BE CALLED FROM OUTSIDE, so reporting it says
			// nothing about reachability — it is a style question about the method's own case, and
			// mixing the two would bury the finds that matter under a list of them.
			if fd.Recv != nil && !exportedReceiver(fd.Recv) {
				continue
			}
			seen[fd.Name.Name] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// exportedReceiver reports whether a method's receiver type is exported.
func exportedReceiver(recv *ast.FieldList) bool {
	if recv == nil || len(recv.List) == 0 {
		return false
	}
	t := recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	// A generic receiver is Name[T]; the type is the index expression's base.
	if idx, ok := t.(*ast.IndexExpr); ok {
		t = idx.X
	}
	id, ok := t.(*ast.Ident)
	return ok && id.IsExported()
}

// referencedIdents collects every identifier that is USED in a non-test file anywhere in the module.
//
// A function declaration's own name is skipped, which is the whole trick: without it every function
// references itself and nothing is ever reported. Everything else counts — a call, a method value, a
// name in a comment-free expression — because name matching cannot tell them apart and over-counting
// is the safe direction for a check whose failure mode should be silence rather than noise.
func referencedIdents(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if perr != nil {
				return perr
			}
			skip := map[*ast.Ident]bool{}
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok {
					skip[fd.Name] = true
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && !skip[id] {
					out[id.Name] = true
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", top, err)
		}
	}
	return out
}

func mustRel(t *testing.T, root, dir string) string {
	t.Helper()
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		t.Fatalf("relativising %s: %v", dir, err)
	}
	return rel
}

// The matcher has to work in both directions, for the same reason the field one does: a check that
// reported nothing, or everything, would pass just as quietly on an allowlist that happened to be
// complete.
//
// Pinned against two functions with known answers. Ledger.Revoke is the one this check was built
// after — dead code for its whole existence — and it is now called, so seeing it as unreferenced
// would mean the skip-your-own-declaration trick has stopped working. Ledger.Expand is genuinely
// unreferenced and is on the allowlist, so seeing it as referenced would mean the walk is picking up
// declarations or test files.
func TestTheUnreachableMatcherWorksInBothDirections(t *testing.T) {
	referenced := referencedIdents(t, repoRoot(t))

	if !referenced["Revoke"] {
		t.Error("Ledger.Revoke reads as unreferenced although the engine calls it; every result " +
			"from this file is now suspect, because a matcher that sees nothing reports everything")
	}
	if referenced["Expand"] {
		t.Error("Ledger.Expand reads as referenced although nothing calls it; the walk is counting " +
			"declarations or test files, and this check would then pass on an empty tree")
	}
}
