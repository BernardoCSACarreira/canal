// Package arch holds architecture tests: assertions about the SHAPE of the module rather than
// about the behaviour of any part of it.
//
// It exists because architecture.md §3 spent this project's whole life being wrong. It declared
// package paths the module did not have, an import table wrong in five of ten rows, and one edge
// (registry -> spec) that the code has REVERSED and which produces an import cycle if added. It
// also named a CI test called TestDependencyDirection that had never been written. The compliance
// audit rated all of that fatal, twice — once under R12 and once under C6.
//
// Rewriting the prose fixed the document for exactly as long as it takes someone to add an import.
// This file is the mechanism that keeps it fixed: the table below is the single source of truth,
// and it is compared against the real import graph parsed from the real source.
package arch

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/BernardoCSACarreira/canal"

// declared is the intra-module import graph, exactly as architecture.md §3 states it.
//
// Keep this table and §3 in agreement. Anything else — an edge here that the code does not have, or
// an edge the code has that is not here — fails the test, in both directions, so the document
// cannot drift from the code again in either direction.
var declared = map[string][]string{
	// pkg/ — the published surface. Strictly downward, schema at the bottom.
	"pkg/schema":    {},
	"pkg/record":    {"pkg/schema"},
	"pkg/fault":     {"pkg/record"},
	"pkg/config":    {"pkg/fault", "pkg/record"},
	"pkg/connector": {"pkg/config", "pkg/fault", "pkg/record", "pkg/schema"},
	"pkg/registry":  {"pkg/config", "pkg/connector"},
	// pkg/codec imports exactly what a third-party codec would, which is the same six-package
	// boundary a connector is held to. That is the point: nothing here is privileged.
	"pkg/codec":         {"pkg/config", "pkg/connector", "pkg/fault", "pkg/record", "pkg/registry"},
	"pkg/telemetry":     {"pkg/connector", "pkg/fault", "pkg/record"},
	"pkg/spec":          {"pkg/connector", "pkg/fault", "pkg/record", "pkg/registry", "pkg/schema", "pkg/telemetry"},
	"pkg/store":         {"pkg/connector", "pkg/record", "pkg/spec", "pkg/telemetry"},
	"pkg/store/wal":     {"pkg/connector", "pkg/fault", "pkg/record", "pkg/store"},
	"pkg/connectortest": {"pkg/config", "pkg/connector", "pkg/fault", "pkg/record", "pkg/schema"},
	// pkg/storetest is the conformance suite for store.StateStore, and it sits beside
	// pkg/connectortest for the same reason: a contract is only real if something INDEPENDENT of
	// the implementation can check it. Its imports are the contract's own surface and nothing else.
	"pkg/storetest": {"pkg/connector", "pkg/fault", "pkg/record", "pkg/store"},

	// internal/ — engine machinery. Nothing under pkg/ may import any of it.
	"internal/ledger": {"pkg/connector", "pkg/fault", "pkg/record"},
	// internal/metrics implements connector.Metrics and enforces pkg/telemetry's closed name and
	// label sets, so those two are its whole dependency surface. It must not reach the engine: a
	// registry that knows what a pipeline is would be a registry the enterprise deployment cannot
	// swap for a pusher.
	"internal/metrics": {"pkg/connector", "pkg/telemetry"},
	"internal/engine": {"internal/ledger", "internal/metrics", "pkg/config", "pkg/connector", "pkg/fault",
		"pkg/record", "pkg/registry", "pkg/schema", "pkg/spec", "pkg/store", "pkg/telemetry"},

	// cmd/ — the composition root, and the ONLY package allowed to import both internal/engine and
	// a concrete deployment assembly. It is where the standalone shape is chosen; the enterprise
	// shape is a different main with different stores and the same engine.
	//
	// The blank imports of the example connectors and pkg/codec are what make this build's
	// catalogue, and they are why this row is longer than any other. Nothing imports cmd.
	//
	// pkg/record is here because the composition root parses wire input into domain types: /status
	// takes ?stream=orders and a telemetry.StatusQuery holds a record.StreamName. It is not a new
	// direction — cmd/canal already handles record types through every spec.Spec it loads — it is
	// naming one it was already carrying transitively.
	"cmd/canal": {"internal/engine", "internal/example/filesink", "internal/example/linefile", "internal/example/stdoutsink",
		"internal/metrics", "pkg/codec", "pkg/config", "pkg/record", "pkg/registry", "pkg/spec",
		"pkg/store", "pkg/store/wal", "pkg/telemetry", "pkg/fault"},
}

// connectorPrefixes are the trees that stand in for third-party code: the worked examples and the
// hostile-connector corpus. Everything under them is held to the connector import boundary.
var connectorPrefixes = []string{"internal/example/", "internal/stress/"}

// notAConnector lists packages inside those trees that implement a DEPLOYMENT interface rather than
// a connector interface, and so are not bound by the six-package rule.
//
// internal/example/memstore is a store.StateStore. It is the in-memory scaffolding the compliance
// audit named — honest about being a map, and the reason no tier above at-least-once can be
// negotiated on it. It imports pkg/store because that is the interface it implements; holding it to
// the connector boundary would be a category error, not a finding.
var notAConnector = map[string]bool{
	"internal/example/memstore": true,
}

// connectorMayImport is the boundary from §3: the packages a connector author can reach.
//
// This is the structural reason the core cannot grow a switch on connector identity — the core's
// types are not reachable from a connector at all.
var connectorMayImport = map[string]bool{
	"pkg/config": true, "pkg/connector": true, "pkg/fault": true,
	"pkg/record": true, "pkg/registry": true, "pkg/schema": true,
}

// imports parses every non-test .go file under dir and returns the intra-module packages it imports.
func imports(t *testing.T, root, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, filepath.Join(root, dir), func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}
	seen := map[string]bool{}
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, imp := range f.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil || !strings.HasPrefix(path, modulePath+"/") {
					continue
				}
				seen[strings.TrimPrefix(path, modulePath+"/")] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// isConnectorPkg reports whether dir stands in for third-party connector code.
func isConnectorPkg(dir string) bool {
	if notAConnector[dir] {
		return false
	}
	for _, p := range connectorPrefixes {
		if strings.HasPrefix(dir+"/", p) {
			return true
		}
	}
	return false
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", root, err)
	}
	return root
}

// allPackageDirs walks the tree for directories holding at least one non-test .go file.
func allPackageDirs(t *testing.T, root string) []string {
	t.Helper()
	var dirs []string
	for _, top := range []string{"pkg", "internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			// testdata is not a package, by the toolchain's own rule: go build ignores it entirely.
			// This walk did not, so the first fixture directory anybody added — one for the
			// write-only field detector in inert_test.go — was reported as an undeclared package.
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			ents, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			for _, e := range ents {
				if strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
					rel, _ := filepath.Rel(root, path)
					dirs = append(dirs, filepath.ToSlash(rel))
					return nil
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", top, err)
		}
	}
	sort.Strings(dirs)
	return dirs
}

// TestDependencyDirection is the test architecture.md §3 has always claimed exists.
func TestDependencyDirection(t *testing.T) {
	root := repoRoot(t)

	for pkg, want := range declared {
		got := imports(t, root, pkg)
		wantSet := map[string]bool{}
		for _, w := range want {
			wantSet[w] = true
		}
		gotSet := map[string]bool{}
		for _, g := range got {
			gotSet[g] = true
		}
		for _, g := range got {
			if !wantSet[g] {
				t.Errorf("%s imports %s, which the declared table does not allow\n"+
					"  either the import is wrong, or the table and architecture.md §3 need updating together",
					pkg, g)
			}
		}
		for _, w := range want {
			if !gotSet[w] {
				t.Errorf("the declared table says %s imports %s, but it does not\n"+
					"  a stale declaration is how §3 drifted the first time; remove it here and in §3",
					pkg, w)
			}
		}
	}
}

// TestNoDeclaredPackageIsMissing catches a package added to the tree without a row in the table,
// which is the other way §3 goes stale.
func TestNoDeclaredPackageIsMissing(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range allPackageDirs(t, root) {
		if _, ok := declared[dir]; ok {
			continue
		}
		if !isConnectorPkg(dir) && !notAConnector[dir] {
			t.Errorf("package %s exists but has no row in the declared table (and is not a connector)", dir)
		}
	}
}

// TestPkgNeverImportsInternal is R11's boundary and constraint #3's: pkg/ is what a third party
// imports, and it cannot be allowed to drag engine machinery along behind it.
func TestPkgNeverImportsInternal(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range allPackageDirs(t, root) {
		if !strings.HasPrefix(dir, "pkg/") {
			continue
		}
		for _, imp := range imports(t, root, dir) {
			if strings.HasPrefix(imp, "internal/") {
				t.Errorf("%s imports %s; pkg/ is the published surface and must not reach into internal/", dir, imp)
			}
		}
	}
}

// TestConnectorsStayInsideTheBoundary is constraint #4 made mechanical.
//
// If a connector can reach the engine, someone eventually will, and the "zero core edits" property
// stops being structural and becomes a convention.
func TestConnectorsStayInsideTheBoundary(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range allPackageDirs(t, root) {
		if !isConnectorPkg(dir) {
			continue
		}
		for _, imp := range imports(t, root, dir) {
			// A connector may depend on a sibling within its own example or stress group; the
			// boundary being tested is the one around the CORE.
			if strings.HasPrefix(imp, "internal/example/") || strings.HasPrefix(imp, "internal/stress/") {
				continue
			}
			if !connectorMayImport[imp] {
				t.Errorf("connector package %s imports %s, which is outside the six-package boundary\n"+
					"  allowed: config, connector, fault, record, registry, schema", dir, imp)
			}
		}
	}
}
