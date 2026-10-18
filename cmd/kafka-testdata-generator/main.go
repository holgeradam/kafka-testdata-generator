// Command kafka-testdata-generator generates test data from an AsyncAPI spec
// and produces it to Kafka, or prints it in dry run.
//
// This file is the process edge and nothing else: argv becomes a Run plan
// (internal/runplan), the run's sink and encoder are built from it, the
// Pipeline is driven, and an exit code comes back. Every rule about which flags
// are valid lives in the plan, where a table test can reach it (ADR-0010).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/logging"
	"github.com/holgeradam/kafka-testdata-generator/internal/logging/zaplog"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/runplan"
)

func main() {
	os.Exit(run(context.Background(), os.Args[0], os.Args[1:], os.Stdout, os.Stderr))
}

// buildSink is the seam tests replace to observe cleanup; production always
// builds the plan's own sink.
var buildSink = func(ctx context.Context, r *runplan.Run, stdout, stderr io.Writer) (pipeline.Sink, error) {
	return r.NewSink(ctx, stdout, stderr)
}

// run executes one invocation and returns its exit code. It never calls
// os.Exit, so every deferred cleanup runs on every path: the Kafka producer is
// flushed and closed whether the run succeeds, fails to encode, or is
// interrupted (ADR-0010).
func run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) int {
	// The signal goroutine logs while the run writes its own lines.
	stderr = &lockedWriter{w: stderr}
	// Until a plan names the level, diagnostics go out at the default one.
	log := zaplog.New(stderr, logging.Info)
	plan, err := runplan.Plan(args)
	if errors.Is(err, flag.ErrHelp) {
		runplan.Usage(stdout, name)
		return 0
	}
	if err != nil {
		fatal(stderr, log, err)
		// A rule about the flags themselves is worth a reminder of the flag
		// surface; a rejected spec, avsc or key path is not.
		var pe *runplan.Error
		if errors.As(err, &pe) && pe.Usage {
			runplan.Usage(stderr, name)
			return exitUsage
		}
		return exitFailure
	}
	log = zaplog.New(stderr, plan.LogLevel)
	for _, d := range plan.Diagnostics {
		d.Emit(log)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	var interrupted atomic.Bool
	go func() {
		select {
		case <-sigCh:
			interrupted.Store(true)
			log.Warn("Interrupted, shutting down...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// The broker is dialed before the registry is contacted, so a run that
	// cannot produce never registers a schema (ADR-0010).
	sink, err := buildSink(ctx, plan, stdout, stderr)
	if err != nil {
		fatal(stderr, log, err)
		return failureCode(&interrupted)
	}
	defer sink.Close()

	enc, err := plan.NewEncoder(ctx)
	if err != nil {
		fatal(stderr, log, err)
		return failureCode(&interrupted)
	}

	cfg := plan.Config
	cfg.Encoder = enc
	cfg.Logger = log

	stats, err := pipeline.New(cfg, sink).Run(ctx)
	if err != nil {
		fatal(stderr, log, err)
	}
	printStats(stderr, stats, plan.DryRun)
	if interrupted.Load() {
		return exitInterrupted
	}
	if err != nil || stats.Failed > 0 {
		return exitFailure
	}
	return 0
}

// failureCode is the exit code for a failure during setup: an interrupt that
// cut the dial short is still an interrupt.
func failureCode(interrupted *atomic.Bool) int {
	if interrupted.Load() {
		return exitInterrupted
	}
	return exitFailure
}

// Exit codes: 0 is success, a runtime failure (a rejected spec, a failed
// record, a generation error) is 1, a misuse of the flags is 2 (#120), and an
// interrupt is 128 plus SIGINT, as shells report it.
const (
	exitFailure     = 1
	exitUsage       = 2
	exitInterrupted = 130
)

// lockedWriter serialises writes, so diagnostics from the signal goroutine
// never interleave with, or race, the run's own lines.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// fatal reports a run-ending error: the plain Error line, whatever the log
// level, and an error-level record beside it (#121 decision 7).
func fatal(stderr io.Writer, log logging.Logger, err error) {
	fmt.Fprintf(stderr, "Error: %v\n", err)
	log.Error("run failed", "error", err)
}

func printStats(w io.Writer, s pipeline.Stats, dryRun bool) {
	mode := "kafka"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Fprintf(w, "\nStats [%s]: total=%d acked=%d failed=%d elapsed=%s\n",
		mode, s.Total, s.Acked, s.Failed, s.Elapsed.Round(time.Millisecond))
}
