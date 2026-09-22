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
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	plan, err := runplan.Plan(args)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		// A rule about the flags themselves is worth a reminder of the flag
		// surface; a rejected spec, avsc or key path is not.
		var pe *runplan.Error
		if errors.As(err, &pe) && pe.Err == nil {
			runplan.Usage(stderr, name)
		}
		return 1
	}
	for _, w := range plan.Warnings {
		fmt.Fprintln(stderr, w)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			fmt.Fprintln(stderr, "\nInterrupted, shutting down...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// The broker is dialed before the registry is contacted, so a run that
	// cannot produce never registers a schema (ADR-0010).
	sink, err := buildSink(ctx, plan, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	defer sink.Close()

	enc, err := plan.NewEncoder(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}

	cfg := plan.Config
	cfg.Encoder = enc
	cfg.Warn = stderr

	stats, err := pipeline.New(cfg, sink).Run(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	printStats(stderr, stats, plan.DryRun)
	return 0
}

func printStats(w io.Writer, s pipeline.Stats, dryRun bool) {
	mode := "kafka"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Fprintf(w, "\nStats [%s]: total=%d acked=%d failed=%d elapsed=%s\n",
		mode, s.Total, s.Acked, s.Failed, s.Elapsed.Round(time.Millisecond))
}
