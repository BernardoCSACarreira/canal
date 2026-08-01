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

// DECLARED AND INERT is this module's most productive defect, by a wide margin.
//
// Something is declared where an operator or a connector author can see it — a field on a config
// form, a knob on Deps, a policy on spec.Spec — and nothing reads it. It is invisible by
// construction: the code compiles, the tests pass, the form renders the field, and the only symptom
// is that setting it changes nothing. Found so far, each by accident rather than by any check:
//
//   - Deps.FlushRecords, declared for the flush trigger and never read. Cost a 30-second test run.
//   - spec.Spec.WhenFull and the per-node when_full, offered in two places and read in neither, so
//     every admission blocked regardless of what the operator chose.
//   - connector.Runtime.Config, which returned (nil, nil) to every connector that ever called it,
//     because Build validated each node's config and then dropped it.
//   - Heartbeat, Backlog and Nack: fifteen implementations across the stress corpus, resolved by
//     the registry, reported by the negotiation as declared, and called from nowhere.
//   - The ledger's per-lane abandoned counter, written in two places and read in none.
//
// This file is the mechanism. It cannot find a field that is read WRONGLY, only one that is not
// read at all — which is precisely the shape every entry above had.
//
// THE ALLOWLIST IS THE POINT. A field that is legitimately unread must say so here, with a reason,
// which turns a silence into a declaration somebody had to write down.

// unreadFields are the struct fields with no reader, and why each is allowed to have none.
//
// Removing an entry is how a field graduates. Adding one should feel like a decision.
var unreadFields = map[string]string{
	"Deps.Status": "store.StatusStore.Report publishes the read model per worker and Aggregate " +
		"merges them. With one worker the document is complete by construction.",
	"spec.Spec.Title": "display only, for a frontend that does not exist. Nothing in the engine " +
		"should branch on it even when one does.",
}

// unreadStageFields are the stage-standard config fields nothing reads, and why.
//
// pkg/registry attaches these to EVERY node's config form, so each one reaches the generated JSON
// schema and the operator's submit screen. A field an operator can fill in and nothing consumes is
// the same defect as an unread Deps knob with a wider audience.
var unreadStageFields = map[string]string{
	"FieldBatching": "sink batching is not implemented: buildRequests emits one request per batch " +
		"as the source produced it, and there is no accumulator to configure.",
	"FieldMaxInFlight": "per-sink concurrency is not implemented. SinkCaps.MaxConcurrency is one " +
		"for every sink in the module and the engine writes serially per node.",
	"FieldDedupe": "no dedupe layer exists. canal_records_deduped_total is declared and unemitted " +
		"for the same reason.",
	"FieldCapacity": "there is no buffer node type, so nothing has a capacity to configure. It is " +
		"attached only to KindBuffer, which cannot be instantiated.",
}

// TestNoDeclaredFieldIsInert checks the two structs an operator configures the engine through.
func TestNoDeclaredFieldIsInert(t *testing.T) {
	root := repoRoot(t)

	readers := []string{"internal/engine", "internal/ledger", "internal/metrics", "cmd/canal"}

	for _, target := range []struct {
		file, typeName, prefix string
		// bases are the receiver spellings a read goes through: r.deps.X and d.X for Deps,
		// r.p.spec.X and s.X for a Spec. Matching the BASE and not just the field name is what
		// makes this check usable at all — .Config, .Status and .Coordinator all appear on other
		// types, so a bare name match reported three genuinely inert fields as read.
		bases []string
	}{
		{"internal/engine/build.go", "Deps", "Deps", []string{"deps", "d"}},
		{"pkg/spec/spec.go", "Spec", "spec.Spec", []string{"spec", "s"}},
	} {
		selected := selectorsIn(t, root, readers, target.bases)
		for _, field := range exportedFields(t, filepath.Join(root, target.file), target.typeName) {
			key := target.prefix + "." + field
			if _, allowed := unreadFields[key]; allowed {
				if selected[field] {
					t.Errorf("%s is on the unread allowlist but something reads it now.\n"+
						"  delete its entry in inert_test.go — the allowlist is a record of what is "+
						"deliberately inert, and a stale one hides the next real gap", key)
				}
				continue
			}
			if !selected[field] {
				t.Errorf("%s is declared and nothing reads it.\n"+
					"  Either wire it up, or add it to unreadFields in inert_test.go with the reason.\n"+
					"  A knob that changes nothing is worse than no knob: an operator sets it, sees no\n"+
					"  effect, and has no way to tell a broken pipeline from an ignored field.", key)
			}
		}
	}
}

// TestNoStageStandardFieldIsInert checks the config fields the registry puts on every node's form.
//
// These are keyed by STRING rather than selected as struct fields, so the check is different: a
// field is read if its config.Field* constant appears outside the two places that declare and
// attach it.
func TestNoStageStandardFieldIsInert(t *testing.T) {
	root := repoRoot(t)

	attached := stageStandardFields(t, filepath.Join(root, "pkg/registry/stage_standard.go"))
	if len(attached) == 0 {
		t.Fatal("no stage-standard fields were found, so this test proves nothing")
	}

	declaring := map[string]bool{
		"pkg/config/composites.go":            true,
		"pkg/registry/stage_standard.go":      true,
		"internal/arch/inert_test.go":         true,
		"pkg/registry/stage_standard_test.go": true,
	}
	used := identsIn(t, root, []string{"pkg", "internal", "cmd"}, declaring)

	for _, name := range attached {
		if _, allowed := unreadStageFields[name]; allowed {
			if used[name] {
				t.Errorf("config.%s is on the unread allowlist but something reads it now; "+
					"delete its entry in inert_test.go", name)
			}
			continue
		}
		if !used[name] {
			t.Errorf("config.%s is attached to every node's config form and nothing reads it.\n"+
				"  It reaches the generated JSON schema and the operator's submit screen, so somebody\n"+
				"  will set it and nothing will happen. Wire it up, stop offering it, or add it to\n"+
				"  unreadStageFields in inert_test.go with the reason.", name)
		}
	}
}

// --- the parsing ---------------------------------------------------------------------------------

// exportedFields returns the exported field names of one struct type in one file.
func exportedFields(t *testing.T, path, typeName string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != typeName {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, fld := range st.Fields.List {
			for _, name := range fld.Names {
				if name.IsExported() {
					out = append(out, name.Name)
				}
			}
		}
		return false
	})
	if len(out) == 0 {
		t.Fatalf("no exported fields found on %s in %s; the check would pass vacuously", typeName, path)
	}
	sort.Strings(out)
	return out
}

// selectorsIn collects every field name SELECTED in the given packages.
//
// Selectors only: a struct-literal key or a field declaration is not a read, which is the whole
// distinction this test turns on — Deps.FlushRecords was assigned by every caller and read by none.
//
// The match is by NAME AND BASE rather than by resolved type, because resolving types needs a full
// type-check of the module and the honest alternative is a check that does not exist. A read counts
// when the selector's base ends in one of the given receiver spellings, so r.deps.GracePeriod and
// d.GracePeriod count and rt.Config() does not.
//
// A reader spelled some other way would be MISSED, which shows up as a false alarm on a field that
// is genuinely wired — noisy, immediately obvious, and fixed by adding the spelling. That is the
// safe direction: the failure mode is a complaint, not a silence.
func selectorsIn(t *testing.T, root string, pkgs, bases []string) map[string]bool {
	t.Helper()
	allowed := map[string]bool{}
	for _, b := range bases {
		allowed[b] = true
	}
	out := map[string]bool{}
	for _, p := range pkgs {
		dir := filepath.Join(root, filepath.FromSlash(p))
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", p, err)
		}
		for _, e := range ents {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, e.Name()), nil, 0)
			if err != nil {
				t.Fatalf("parsing %s: %v", e.Name(), err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && allowed[baseName(sel.X)] {
					out[sel.Sel.Name] = true
				}
				return true
			})
		}
	}
	return out
}

// stageStandardFields returns the config.Field* constant names attached to every node's form.
func stageStandardFields(t *testing.T, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || !strings.HasPrefix(sel.Sel.Name, "Field") {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); ok && x.Name == "config" {
			out = append(out, sel.Sel.Name)
		}
		return true
	})
	sort.Strings(out)
	return out
}

// identsIn collects every identifier used across the given trees, skipping the files that declare
// the things being looked for.
func identsIn(t *testing.T, root string, tops []string, skip map[string]bool) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, top := range tops {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if skip[filepath.ToSlash(rel)] {
				return nil
			}
			f, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if perr != nil {
				return perr
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
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

// baseName returns the final identifier of a selector's base: "deps" for both deps.X and r.deps.X.
func baseName(x ast.Expr) string {
	switch e := x.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.ParenExpr:
		return baseName(e.X)
	case *ast.StarExpr:
		return baseName(e.X)
	case *ast.IndexExpr:
		return baseName(e.X)
	case *ast.CallExpr:
		return baseName(e.Fun)
	}
	return ""
}

// THE MATCHER NEEDS A TEST OF ITS OWN, in both directions.
//
// Everything above depends on selectorsIn telling the truth. If it reported every field as read the
// allowlist entries would catch it — they assert the inverse — but if it reported every field as
// UNREAD, or matched nothing at all, the whole check would still pass on an empty result set for a
// field that has no entry. So it is pinned against one field known to be read and one known not to
// be, and a change that breaks it fails here rather than going quiet.
func TestTheInertMatcherWorksInBothDirections(t *testing.T) {
	root := repoRoot(t)
	selected := selectorsIn(t, root,
		[]string{"internal/engine", "internal/ledger", "internal/metrics", "cmd/canal"},
		[]string{"deps", "d"})

	if !selected["GracePeriod"] {
		t.Error("Deps.GracePeriod is read all over the engine and the matcher did not see it; " +
			"every result from this file is now suspect")
	}
	// The negative pin was Deps.Coordinator until the lease work gave it a reader. Deps.Status is
	// the remaining one, and it is a better pin for the same reason Coordinator was: .Status is
	// spelled all over the engine on OTHER receivers — p.Status(), r.status — so a matcher whose
	// base-expression filter stopped filtering would report it as read.
	if selected["Status"] {
		t.Error("Deps.Status has no reader, and the matcher saw one: " +
			"the base-expression filter is not filtering")
	}
}

// unreadWriteOnly are the unexported fields a package assigns and never reads, and why.
//
// Same defect as an inert exported field, one level down and harder to see: nothing outside the
// package can even observe the field, so the only symptom is work being done for no reason — or, in
// the case that motivated this check, a number being computed correctly and thrown away while a
// consumer reported a different one under its name.
var unreadWriteOnly = map[string]string{}

// TestNoFieldIsWrittenAndNeverRead finds state a package maintains for nobody.
//
// It found three on its first run: record.Batch.bytes, declared and zeroed and never once
// incremented; the tracker's doubly-linked back pointer, assigned three times and followed never;
// and the ledger's recordsCommitted, which counted records acknowledged to the SOURCE and was
// discarded while the read model reported the sink's settled count under the name "committed".
//
// The check is per package and by name, so it cannot see a field read through an interface or in
// another package — which is fine, because an unexported field has neither.
func TestNoFieldIsWrittenAndNeverRead(t *testing.T) {
	root := repoRoot(t)
	for _, pkg := range []string{
		"pkg/record", "pkg/connector", "pkg/config", "pkg/spec", "pkg/store", "pkg/telemetry",
		"pkg/fault", "pkg/registry", "pkg/schema",
		"internal/ledger", "internal/engine", "internal/metrics",
	} {
		for _, field := range writeOnlyFields(t, filepath.Join(root, filepath.FromSlash(pkg))) {
			key := pkg + "." + field
			if _, allowed := unreadWriteOnly[key]; allowed {
				continue
			}
			t.Errorf("%s is assigned and never read.\n"+
				"  Either something should be reading it, or the writes are work done for nobody.\n"+
				"  Add it to unreadWriteOnly in inert_test.go with the reason if it is deliberate.", key)
		}
	}
}

// writeOnlyFields returns the unexported struct fields a package writes and never reads.
func writeOnlyFields(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	declared := map[string]bool{}
	reads, writes := map[string]int{}, map[string]int{}

	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}

		// Assignment targets are collected by POSITION first, so the second pass can tell a write
		// from a read of the same selector text. Without that, every write counts as a read and the
		// check reports nothing at all — which is exactly what the first version of it did.
		target := map[token.Pos]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch s := n.(type) {
			case *ast.AssignStmt:
				for _, l := range s.Lhs {
					if sel, ok := l.(*ast.SelectorExpr); ok {
						target[sel.Pos()] = true
						writes[sel.Sel.Name]++
					}
				}
			case *ast.IncDecStmt:
				if sel, ok := s.X.(*ast.SelectorExpr); ok {
					target[sel.Pos()] = true
					writes[sel.Sel.Name]++
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			switch s := n.(type) {
			case *ast.StructType:
				for _, fld := range s.Fields.List {
					for _, nm := range fld.Names {
						if !nm.IsExported() {
							declared[nm.Name] = true
						}
					}
				}
			case *ast.SelectorExpr:
				if !target[s.Pos()] {
					reads[s.Sel.Name]++
				}
			}
			return true
		})
	}

	var out []string
	for name := range declared {
		// A COMPOUND ASSIGNMENT IS A READ TOO. x.n += 1 parses as an AssignStmt with x.n on the left,
		// so it counts only as a write here — which is right for this question: a counter that is
		// only ever added to and never consulted is precisely the thing being looked for.
		if writes[name] > 0 && reads[name] == 0 {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// THE DETECTOR NEEDS ITS OWN FIXTURE, in both directions.
//
// writeOnlyFields reports nothing on a clean tree, and so does a broken version of it. The first
// draft of it was exactly that: it counted an assignment target as a read of itself, so every write
// looked like a read and it found nothing at all — on a tree that had three. A check whose passing
// state is indistinguishable from its broken state is not a check.
func TestTheWriteOnlyDetectorWorksInBothDirections(t *testing.T) {
	got := writeOnlyFields(t, filepath.Join(repoRoot(t), "internal/arch/testdata/writeonly"))
	found := map[string]bool{}
	for _, f := range got {
		found[f] = true
	}

	if !found["dropped"] {
		t.Error("the fixture's write-only field was not found; every clean result from this check is " +
			"now meaningless")
	}
	if !found["compound"] {
		t.Error("a field that is only ever added to was not found: x += n is a write, and a counter " +
			"nothing consults is the case that motivated this check")
	}
	if found["kept"] {
		t.Error("a field that is written and read was reported as write-only")
	}
}
