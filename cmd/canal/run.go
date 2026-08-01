package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"

	// The binary can only run what is linked into it. These blank imports ARE the connector
	// catalogue of this build: a spec naming anything else is refused at build time with a
	// diagnostic that lists what is available, which is the honest failure for a static registry.
	_ "github.com/BernardoCSACarreira/canal/internal/example/linefile"
	_ "github.com/BernardoCSACarreira/canal/internal/example/stdoutsink"
	_ "github.com/BernardoCSACarreira/canal/pkg/codec"
)

// opts is the shared flag set of run and check.
type opts struct {
	spec  string
	state string

	worker string
	flush  time.Duration
	grace  time.Duration
	logLvl string
}

func (o *opts) bind(fs *flag.FlagSet, withState bool) {
	fs.StringVar(&o.spec, "spec", "", "path to the pipeline specification (JSON); - reads stdin")
	if withState {
		fs.StringVar(&o.state, "state", "", "directory for the write-ahead log holding lane cursors")
		fs.StringVar(&o.worker, "worker", "single", "this process's worker identity, recorded in every lane lease")
		fs.DurationVar(&o.flush, "flush", time.Second,
			"how often lane cursors are made durable; also the upper bound on how much replays after a crash")
		fs.DurationVar(&o.grace, "grace", 30*time.Second,
			"how long shutdown is given to drain in-flight records before it is reported as a drain timeout")
	}
	fs.StringVar(&o.logLvl, "log", "info", "log level: debug, info, warn or error")
}

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("canal run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var o opts
	o.bind(fs, true)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if o.spec == "" || o.state == "" {
		fmt.Fprintln(os.Stderr, "canal run: --spec and --state are both required")
		fs.PrintDefaults()
		return exitUsage
	}

	log, err := logger(o.logLvl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canal run: %v\n", err)
		return exitUsage
	}

	s, err := loadSpec(o.spec, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canal run: %v\n", err)
		return exitUsage
	}

	// The store is opened BEFORE the build, because an exclusive lock already held by another
	// process is the failure most worth reporting early: building first would construct every
	// connector and then discard it.
	st, err := wal.Open(o.state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canal run: opening the state store at %s: %v\n", o.state, err)
		return exitFailed
	}
	// Close is deliberately not deferred: the exit paths below each need it to happen before the
	// process leaves, and a deferred close does not run under os.Exit.
	closeStore := func() {
		if err := st.Close(); err != nil {
			log.Error("closing the state store", "error", err)
		}
	}

	deps := engine.Deps{
		State:         st,
		Worker:        store.WorkerID(o.worker),
		Log:           log,
		Version:       buildVersion(),
		FlushInterval: o.flush,
		GracePeriod:   o.grace,
	}

	// Signals are wired up BEFORE Build so a Ctrl-C during a slow build is not ignored, and the
	// handler is installed before anything can block.
	ctx, stop := signalContext(log)
	defer stop()

	p, neg, diags := engine.Build(ctx, registry.Default, s, deps)
	printDiagnostics(diags)
	if diags.HasErrors() {
		closeStore()
		return exitRefused
	}
	printNegotiated(neg)

	runErr := p.Run(ctx)

	// Close gets a context that is NOT the run context: it is called precisely because the run
	// ended, and cancellation is one of the ways a run ends. Handing Close a cancelled context
	// would abandon the shutdown at the moment it matters.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.grace)
	closeErr := p.Close(closeCtx)
	cancel()
	closeStore()

	switch {
	case runErr != nil && !errors.Is(runErr, context.Canceled):
		log.Error("the pipeline stopped on an error", "error", runErr)
		return exitFailed
	case closeErr != nil:
		log.Error("the pipeline did not shut down cleanly", "error", closeErr)
		return exitFailed
	case ctx.Err() != nil:
		// An interrupted run is not a failure. It drained, it flushed, and what it read is
		// durable — the only difference from a completed run is that the input still has more.
		log.Info("stopped on a signal; every record it acknowledged is durable")
		return exitOK
	default:
		log.Info("the input ended and every record is durable")
		return exitOK
	}
}

func cmdCheck(args []string) int {
	fs := flag.NewFlagSet("canal check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var o opts
	o.bind(fs, false)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if o.spec == "" {
		fmt.Fprintln(os.Stderr, "canal check: --spec is required")
		fs.PrintDefaults()
		return exitUsage
	}

	log, err := logger(o.logLvl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canal check: %v\n", err)
		return exitUsage
	}
	s, err := loadSpec(o.spec, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canal check: %v\n", err)
		return exitUsage
	}

	// check does no I/O and touches no state directory. That is the whole point of Build and Run
	// being separate calls: an operator can see the negotiated contract of a pipeline before
	// anything connects to anything, and before a state directory exists. See capsOnlyStore for why
	// the answer is still the real one.
	p, neg, diags := engine.Build(context.Background(), registry.Default, s,
		engine.Deps{State: capsOnlyStore{}, Log: log, Version: buildVersion()})
	printDiagnostics(diags)
	if diags.HasErrors() {
		return exitRefused
	}
	_ = p.Close(context.Background())
	printNegotiated(neg)
	return exitOK
}

// signalContext returns a context cancelled by the first SIGINT or SIGTERM.
//
// The SECOND signal exits immediately. A graceful drain is bounded by --grace, but an operator who
// presses Ctrl-C twice has decided not to wait for it, and a program that ignores that is a program
// people learn to SIGKILL by reflex. The exit code says the shutdown was skipped.
func signalContext(log *slog.Logger) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-ch:
			log.Info("draining; press it again to stop immediately", "signal", sig.String())
			cancel()
		case <-ctx.Done():
			return
		}
		select {
		case <-ch:
			log.Warn("stopping without draining; in-flight records will replay on restart")
			os.Exit(exitFailed)
		case <-ctx.Done():
		}
	}()

	return ctx, func() { signal.Stop(ch); cancel() }
}

// loadSpec reads a specification file and fills in what the operator left out.
//
// DURATIONS IN THIS FILE ARE NANOSECONDS, because spec.Spec is encoded by encoding/json and
// time.Duration is an int64 to it. That is unfriendly to write by hand and it is a known rough
// edge; the defaults below exist so the common case never has to write one.
func loadSpec(path string, log *slog.Logger) (spec.Spec, error) {
	var (
		data []byte
		err  error
	)
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return spec.Spec{}, fmt.Errorf("reading the spec: %w", err)
	}

	var s spec.Spec
	dec := json.NewDecoder(bytes.NewReader(data))
	// An unknown field is a typo, and a typo silently ignored is a setting the operator believes
	// is in force. Refusing is the only reading of "streems": [...] that does not lie.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return spec.Spec{}, fmt.Errorf("parsing %s: %w", path, err)
	}

	if s.Retry == (fault.RetryPolicy{}) {
		s.Retry = fault.DefaultRetry
		log.Debug("spec has no retry policy; using the default", "max_attempts", s.Retry.MaxAttempts)
	}
	return s, nil
}

// printDiagnostics writes every diagnostic to stderr, errors and warnings alike.
//
// All of them, always: validation returns the complete set precisely so an operator fixes a spec in
// one pass instead of one field per run.
func printDiagnostics(d config.Diagnostics) {
	for i := range d {
		g := &d[i]
		where := "spec"
		if g.Node != "" {
			where = "node " + string(g.Node)
		}
		if len(g.Path) > 0 {
			where += "." + joinPath(g.Path)
		}
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", g.Severity, where, g.Message)
		if g.Hint != "" {
			fmt.Fprintf(os.Stderr, "  hint: %s\n", g.Hint)
		}
		if g.Iface != "" {
			fmt.Fprintf(os.Stderr, "  the connector would need to implement: %s\n", g.Iface)
		}
	}
}

// printNegotiated writes the delivery contract the pipeline actually got.
//
// This is printed on every run rather than only on request, because the gap between the guarantee
// an operator ASKED FOR and the one the components can actually deliver is the most consequential
// fact about a pipeline, and negotiation makes it knowable before a record moves. Printing it only
// under a flag would make the default behaviour "do not mention the downgrade".
//
// Why, Defaults and Downgrades are each printed in full. Why is the derivation; Defaults is design
// rule R10's labelling of every value the core supplied rather than the operator; Downgrades is the
// list of waivers in force, which is the one thing nobody should have to go looking for.
func printNegotiated(n telemetry.Negotiated) {
	w := os.Stderr
	fmt.Fprintf(w, "negotiated: %s (durability edge: %s, ack point: %s, replay budget: %d)\n",
		n.Guarantee, n.DurabilityEdge, n.AckPoint, n.ReplayBudget)
	for id, c := range n.Nodes {
		fmt.Fprintf(w, "  node %s: %s\n", id, c.Guarantee)
	}
	for _, why := range n.Why {
		fmt.Fprintf(w, "  because: %s\n", why)
	}
	for _, d := range n.Defaults {
		fmt.Fprintf(w, "  default: %s = %v (%s)\n", joinPath(d.Path), d.Value, d.From)
	}
	for _, d := range n.Downgrades {
		fmt.Fprintf(w, "  waiver: node %s %s -> %s, missing %v, acknowledged by %s\n",
			d.Node, d.Requested, d.Effective, d.Missing, d.AcknowledgedBy)
	}
}

func joinPath(p []string) string {
	out := ""
	for i, seg := range p {
		if i > 0 {
			out += "."
		}
		out += seg
	}
	return out
}

func logger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("unknown log level %q: use debug, info, warn or error", level)
	}
	// STDERR, always. See the package comment: stdout carries records.
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})), nil
}
