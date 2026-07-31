package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file holds the test design rule R3 has been waiting for.
//
// TestResumeAfterInterruption in internal/engine already proves resume across a restart, but it
// restarts a pipeline INSIDE one process: the same heap, the same runtime, a store that was closed
// politely. Everything in canal that claims to survive a crash — the fsync in the WAL, the
// directory sync behind the rename, the torn-tail reader, the flock the kernel drops when a process
// dies, the three-phase commit ordering — is a claim about a process that does NOT get to run its
// deferred functions.
//
// A SIGKILL is the only way to test that, and a SIGKILL needs a real binary. Until this file, every
// durability guarantee in this repository was a design rather than a fact.

var (
	buildOnce sync.Once
	builtPath string
	buildErr  error
)

// canalBinary builds cmd/canal once per test binary and returns the path.
func canalBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "canal-bin")
		if err != nil {
			buildErr = err
			return
		}
		builtPath = filepath.Join(dir, "canal")
		out, err := exec.Command("go", "build", "-o", builtPath, ".").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("building canal: %v", buildErr)
	}
	return builtPath
}

// writeSpec writes a line_file -> stdout pipeline reading input and returns the spec path.
func writeSpec(t *testing.T, dir, input string) string {
	t.Helper()
	s := map[string]any{
		"tenant":    "acme",
		"id":        "crash",
		"guarantee": "at_least_once",
		"graph": []any{
			map[string]any{"id": "in", "kind": "source", "name": "line_file",
				"config": map[string]any{"path": input}},
			map[string]any{"id": "out", "kind": "sink", "name": "stdout",
				"config": map[string]any{
					"codec": map[string]any{"encoder": "raw", "framer": "newline"},
				},
				"inputs": []any{map[string]any{"from": "in"}}},
		},
		"streams": []any{map[string]any{
			"stream": "lines", "read": []string{"scan"}, "write": "append"}},
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatalf("encoding the spec: %v", err)
	}
	path := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("writing the spec: %v", err)
	}
	return path
}

// crashLines is the input size. It is large enough that a kill lands mid-run rather than in the
// gap between "started" and "finished", and small enough to stay a few seconds in CI.
const crashLines = 300000

func writeInput(t *testing.T, path string, n int) {
	t.Helper()
	var b strings.Builder
	b.Grow(n * 12)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "line-%07d\n", i)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("writing the input: %v", err)
	}
}

// parseRound turns the bytes one round emitted into the line numbers it carried.
//
// A TRAILING PARTIAL LINE IS EXPECTED AND IGNORED. The sink writes a whole batch in one call, and a
// process killed inside that call can leave the last line half-written. That record's position was
// never committed — the cursor only advances after a settled write is flushed — so the next round
// re-emits it whole, and the seam check below is what proves it did.
func parseRound(t *testing.T, b []byte) []int {
	t.Helper()
	lines := strings.Split(string(b), "\n")
	if len(lines) > 0 {
		lines = lines[:len(lines)-1] // after a terminator-framed stream this is "" or a torn tail
	}
	out := make([]int, 0, len(lines))
	for i, ln := range lines {
		n, err := strconv.Atoi(strings.TrimPrefix(ln, "line-"))
		if !strings.HasPrefix(ln, "line-") || err != nil {
			t.Fatalf("line %d of this round is not a record this pipeline could have written: %q", i, ln)
		}
		out = append(out, n)
	}
	return out
}

// TestSurvivesKillNine kills canal three times mid-run and requires that nothing is lost.
//
// The guarantee under test is AT-LEAST-ONCE, so duplicates after a crash are correct and expected:
// records written but whose position had not yet been flushed are read again. What must never
// happen is a GAP — a record the sink never received because the cursor moved past it.
//
// The strongest assertion here is the seam check. Every restart must resume at or before the record
// after the last one it emitted. A first line of round N greater than lastLine(N-1)+1 means the
// cursor was durable for records the sink never got, which is exactly the failure the three-phase
// commit exists to prevent, and no amount of end-state counting would localise it.
//
// WHAT IT CATCHES, MEASURED. Both assertions were confirmed against injected defects rather than
// assumed to work. A sink write skipped while its cursor advanced was caught by the coverage check
// ("14000 of 300000 records never reached the sink; the first is line 9500"); a resume that seeked
// past its cursor was caught by the seam check, which named the round and the count for all three
// restarts.
//
// WHAT IT DOES NOT CATCH, ALSO MEASURED. Settling a record BEFORE its write returns — the ordering
// violation this whole protocol is built to prevent — was injected and this test passed anyway. The
// gap between the wrong settle and the write that follows it is microseconds, and a kill almost
// never lands there. So this test proves the commit ORDER produces no loss in practice; it is not a
// proof that the order is right. That proof has to be structural, and it lives in the ordering
// comments in flushOnce and deliver.
func TestSurvivesKillNine(t *testing.T) {
	if testing.Short() {
		t.Skip("kills a real process three times; too slow for -short")
	}

	bin := canalBinary(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	writeInput(t, input, crashLines)
	specPath := writeSpec(t, dir, input)
	stateDir := filepath.Join(dir, "state")
	outPath := filepath.Join(dir, "out.txt")

	// killAt is the number of bytes each round is allowed to produce before it is killed. The last
	// round has no entry: it runs to completion.
	killAt := []int64{600_000, 1_400_000, 2_200_000}

	var (
		rounds  [][]int
		lastEnd int64
	)

	for round := 0; round <= len(killAt); round++ {
		// O_APPEND, so a restart adds to the sink's output rather than truncating what the previous
		// round proved. This file is the durable record the assertions are made against.
		out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("opening the output: %v", err)
		}

		cmd := exec.Command(bin, "run",
			"--spec", specPath, "--state", stateDir, "--flush", "50ms", "--log", "warn")
		cmd.Stdout = out
		var stderr strings.Builder
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("round %d: starting canal: %v", round, err)
		}

		killed := round < len(killAt)
		if killed {
			waitForSize(t, outPath, killAt[round])
			// SIGKILL. Not SIGTERM: the point is a process that gets NO shutdown, no drain, no
			// final flush, no Close, and no chance to release the store lock politely.
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("round %d: kill: %v", round, err)
			}
		}
		err = cmd.Wait()
		out.Close()

		switch {
		case killed && err == nil:
			t.Fatalf("round %d: the process exited cleanly, so it was never actually killed", round)
		case !killed && err != nil:
			t.Fatalf("round %d: the final run failed: %v\n%s", round, err, stderr.String())
		}

		// Each round is parsed from its own slice of the file. A torn line belongs to the round
		// that tore it, and must not corrupt the parse of the round that follows.
		all, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("reading the output: %v", err)
		}
		got := parseRound(t, all[lastEnd:])
		lastEnd = int64(len(all))
		rounds = append(rounds, got)
		t.Logf("round %d: killed=%v, emitted %d records, total %d bytes", round, killed, len(got), lastEnd)
	}

	// 1. THE SEAM. No restart may skip a record.
	for i := 1; i < len(rounds); i++ {
		prev, cur := rounds[i-1], rounds[i]
		if len(prev) == 0 || len(cur) == 0 {
			continue
		}
		last, first := prev[len(prev)-1], cur[0]
		if first > last+1 {
			t.Errorf("round %d resumed at line %d but round %d stopped at line %d: "+
				"%d record(s) were lost across the crash",
				i, first, i-1, last, first-last-1)
		}
	}

	// 2. NOTHING LOST OVERALL. Every input line reached the sink at least once.
	seen := make(map[int]int, crashLines)
	for _, r := range rounds {
		for _, n := range r {
			seen[n]++
		}
	}
	missing := 0
	firstMissing := -1
	for i := 0; i < crashLines; i++ {
		if seen[i] == 0 {
			if firstMissing < 0 {
				firstMissing = i
			}
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%d of %d records never reached the sink; the first is line %d",
			missing, crashLines, firstMissing)
	}

	// 3. RESUME DID REAL WORK. At-least-once permits duplicates, but a pipeline that restarted from
	// the beginning every time would also satisfy the two checks above while having no durable
	// cursor at all. Three restarts from scratch would duplicate 3x crashLines; the bound here is
	// far below that and far above the flush interval's worth of replay.
	dupes := 0
	for _, c := range seen {
		dupes += c - 1
	}
	if dupes > crashLines/4 {
		t.Errorf("%d duplicate records out of %d: too many for a cursor that survived the crash",
			dupes, crashLines)
	}
	if !t.Failed() {
		t.Logf("no loss across three SIGKILLs; %d duplicates (%.2f%%), which at-least-once permits",
			dupes, 100*float64(dupes)/float64(crashLines))
	}
}

// waitForSize blocks until path is at least n bytes.
func waitForSize(t *testing.T, path string, n int64) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if fi, err := os.Stat(path); err == nil && fi.Size() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s never reached %d bytes; the pipeline is not producing", path, n)
}

// TestKilledProcessDoesNotKeepTheStoreLocked is the flock claim in pkg/store/wal, tested against a
// process that really died rather than against a Close that really ran.
//
// The comment on acquireLock says a PID file "survives a kill -9 and then refuses to open a store
// that nothing is using". This is the assertion behind that sentence.
//
// It is a unix assertion. The non-unix fallback in os_other.go is an O_EXCL file, which a killed
// process DOES leave behind, and that weakness is documented there rather than hidden. This test
// would fail on Windows, correctly; CI runs Linux and macOS and cross-compiles the rest.
func TestKilledProcessDoesNotKeepTheStoreLocked(t *testing.T) {
	if testing.Short() {
		t.Skip("starts and kills a real process")
	}
	bin := canalBinary(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "input.txt")
	writeInput(t, input, 200000)
	specPath := writeSpec(t, dir, input)
	stateDir := filepath.Join(dir, "state")
	outPath := filepath.Join(dir, "out.txt")

	out, err := os.Create(outPath)
	if err != nil {
		t.Fatalf("creating the output: %v", err)
	}
	cmd := exec.Command(bin, "run", "--spec", specPath, "--state", stateDir, "--flush", "20ms", "--log", "warn")
	cmd.Stdout = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting canal: %v", err)
	}
	waitForSize(t, outPath, 200_000)

	// While it is alive the directory is locked, and a second process must be refused rather than
	// interleaving writes into one write-ahead log.
	second := exec.Command(bin, "run", "--spec", specPath, "--state", stateDir, "--log", "warn")
	secondOut, err := second.CombinedOutput()
	if err == nil {
		t.Fatal("a second process opened a state directory that was already in use")
	}
	if !strings.Contains(string(secondOut), "already open in another process") {
		t.Errorf("the refusal did not say the directory was in use:\n%s", secondOut)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = cmd.Wait()
	out.Close()

	// The kernel drops the flock when the process dies. Nothing has to clean up, which is the whole
	// reason it is a flock.
	third := exec.Command(bin, "run", "--spec", specPath, "--state", stateDir, "--log", "warn")
	if outThird, err := third.CombinedOutput(); err != nil {
		t.Fatalf("the store stayed locked after the holder was killed: %v\n%s", err, outThird)
	}
}

// TestCheckRefusesAnUnbuildableSpec pins the exit code a supervisor reads. A spec that will never
// build must be distinguishable from a run that failed and might succeed on retry.
func TestCheckRefusesAnUnbuildableSpec(t *testing.T) {
	bin := canalBinary(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(path, []byte(`{
	  "tenant": "acme", "id": "bad", "guarantee": "at_least_once",
	  "graph": [
	    {"id": "in",  "kind": "source", "name": "no_such_source", "config": {}},
	    {"id": "out", "kind": "sink",   "name": "stdout", "inputs": [{"from": "in"}]}
	  ],
	  "streams": [{"stream": "lines", "read": ["scan"], "write": "append"}]
	}`), 0o644); err != nil {
		t.Fatalf("writing the spec: %v", err)
	}

	out, err := exec.Command(bin, "check", "--spec", path).CombinedOutput()
	code := exitCodeOf(t, err)
	if code != exitRefused {
		t.Errorf("exit code is %d, want %d (refused)\n%s", code, exitRefused, out)
	}
	if !strings.Contains(string(out), "no_such_source") {
		t.Errorf("the refusal did not name the component that is missing:\n%s", out)
	}
}

// TestUnknownSpecFieldIsRefused. A typo silently ignored is a setting the operator believes is in
// force, which is worse than a parse error by exactly the amount of confidence it creates.
func TestUnknownSpecFieldIsRefused(t *testing.T) {
	bin := canalBinary(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(path, []byte(`{"tenant":"acme","id":"t","streems":[]}`), 0o644); err != nil {
		t.Fatalf("writing the spec: %v", err)
	}

	out, err := exec.Command(bin, "check", "--spec", path).CombinedOutput()
	if code := exitCodeOf(t, err); code != exitUsage {
		t.Errorf("exit code is %d, want %d (usage)\n%s", code, exitUsage, out)
	}
	if !strings.Contains(string(out), "streems") {
		t.Errorf("the error did not quote the field that was not understood:\n%s", out)
	}
}

func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("the command failed without an exit code: %v", err)
	}
	return ee.ExitCode()
}
