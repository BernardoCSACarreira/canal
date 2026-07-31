// Documentation checks, as tests rather than as a CI-only script.
//
// R8's rule is that drift is prevented structurally rather than by discipline, and R12's historical
// defect includes "a README linked a design doc that did not exist". Both are cheap to make
// mechanical, and putting them in `go test` rather than in a YAML step means a contributor finds a
// broken link before pushing rather than after.
//
// What is NOT here: whether a mermaid diagram actually RENDERS. That needs a browser, so it lives in
// the CI workflow. These tests cover the failures that are checkable without one — an unbalanced
// fence, an unknown diagram type, an empty block — which is most of them.
package arch

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// markdownFiles returns every .md file in the repository, skipping .git.
func markdownFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".md") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return out
}

// Code spans have to go before links are matched, or Go's own syntax reads as markdown: a generic
// call such as Get[T any](c, "password") is an identifier followed by a bracket group followed by a
// paren group, which is exactly the shape of [text](target). Four such false positives appeared the
// first time this test ran, all of them real Go quoted in prose.
//
// Fenced blocks first, then inline spans; longest backtick runs first so “a `b` c“ is handled.
var (
	fencedBlocks = regexp.MustCompile("(?ms)^```.*?^```")
	inlineCode   = regexp.MustCompile("`{1,3}[^`\n]*`{1,3}")
)

var mdLink = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)

// TestRelativeLinksResolve is R12's "a README linked a design doc that did not exist", made
// mechanical. It covers every markdown file, not just the two that get read.
func TestRelativeLinksResolve(t *testing.T) {
	root := repoRoot(t)
	checked := 0

	for _, f := range markdownFiles(t, root) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		body := fencedBlocks.ReplaceAllString(string(b), "")
		body = inlineCode.ReplaceAllString(body, "")

		for _, m := range mdLink.FindAllStringSubmatch(body, -1) {
			target := m[1]
			if i := strings.IndexByte(target, '#'); i >= 0 {
				target = target[:i] // an in-page anchor; only the file part is checkable here
			}
			if target == "" || strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			// A path with a :line suffix is a citation, not a link target.
			if i := strings.LastIndexByte(target, ':'); i > 0 {
				if rest := target[i+1:]; rest != "" && strings.IndexFunc(rest, func(r rune) bool { return r < '0' || r > '9' }) == -1 {
					target = target[:i]
				}
			}
			checked++
			if _, err := os.Stat(filepath.Join(filepath.Dir(f), target)); err != nil {
				rel, _ := filepath.Rel(root, f)
				t.Errorf("%s links to %q, which does not exist", rel, m[1])
			}
		}
	}

	if checked == 0 {
		t.Fatal("no relative links were checked at all; the matcher is broken, not the docs")
	}
	t.Logf("%d relative links checked", checked)
}

// knownDiagramTypes are the mermaid grammars this repository uses. An unrecognised one is far more
// likely to be a typo than a new diagram kind, and a typo renders as an error box on GitHub.
var knownDiagramTypes = map[string]bool{
	"flowchart": true, "graph": true, "sequenceDiagram": true, "classDiagram": true,
	"stateDiagram-v2": true, "erDiagram": true, "journey": true, "gantt": true,
	"pie": true, "gitGraph": true, "mindmap": true, "timeline": true,
}

// TestMermaidBlocksAreWellFormed catches the failures that need no browser: an unclosed fence, an
// empty block, or a first line that is not a diagram type.
//
// Sixty diagrams landed in one change. A broken one is invisible until somebody opens the page.
func TestMermaidBlocksAreWellFormed(t *testing.T) {
	root := repoRoot(t)
	total := 0

	for _, f := range markdownFiles(t, root) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		rel, _ := filepath.Rel(root, f)
		lines := strings.Split(string(b), "\n")

		open := -1
		for i, ln := range lines {
			switch {
			case open < 0 && strings.TrimRight(ln, " \t") == "```mermaid":
				open = i
			case open >= 0 && strings.TrimRight(ln, " \t") == "```":
				block := lines[open+1 : i]
				total++
				checkBlock(t, rel, open+1, block)
				open = -1
			}
		}
		if open >= 0 {
			t.Errorf("%s:%d opens a ```mermaid fence that is never closed", rel, open+1)
		}
	}

	if total == 0 {
		t.Fatal("no mermaid blocks found; the scanner is broken, not the docs")
	}
	t.Logf("%d mermaid blocks checked", total)
}

func checkBlock(t *testing.T, file string, line int, block []string) {
	t.Helper()

	var first string
	for _, ln := range block {
		s := strings.TrimSpace(ln)
		if s == "" || strings.HasPrefix(s, "%%") { // %% is a mermaid comment
			continue
		}
		first = s
		break
	}
	if first == "" {
		t.Errorf("%s:%d is an empty mermaid block", file, line)
		return
	}

	word := first
	if i := strings.IndexAny(word, " \t"); i >= 0 {
		word = word[:i]
	}
	if !knownDiagramTypes[word] {
		t.Errorf("%s:%d declares diagram type %q, which mermaid does not know — it renders as an error box\n"+
			"  first line: %s", file, line, word, first)
	}
}

// TestNoHardcodedDiagramColours keeps the diagrams readable in both GitHub themes.
//
// A diagram that pins a fill or a font colour looks correct to whoever wrote it and unreadable to
// half the people who open it. Structure — subgraphs, node shapes, edge labels — carries the meaning
// instead.
func TestNoHardcodedDiagramColours(t *testing.T) {
	root := repoRoot(t)
	colour := regexp.MustCompile(`(?i)(fill|color|stroke|background)\s*:\s*#`)

	for _, f := range markdownFiles(t, root) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		rel, _ := filepath.Rel(root, f)
		inBlock := false
		for i, ln := range strings.Split(string(b), "\n") {
			switch {
			case !inBlock && strings.TrimRight(ln, " \t") == "```mermaid":
				inBlock = true
			case inBlock && strings.TrimRight(ln, " \t") == "```":
				inBlock = false
			case inBlock && colour.MatchString(ln):
				t.Errorf("%s:%d pins a colour in a mermaid diagram, which breaks one of the two GitHub themes\n  %s",
					rel, i+1, strings.TrimSpace(ln))
			}
		}
	}
}
