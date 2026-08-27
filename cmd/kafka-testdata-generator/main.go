package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
)

func main() {
	specPath := flag.String("spec", "", "Path to AsyncAPI spec file (required)")
	channel := flag.String("channel", "", "Kafka topic/channel to produce to (required)")
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	count := flag.Int("count", 10, "Number of payloads to generate (0 = infinite)")
	rateLimit := flag.Duration("rate", 10*time.Millisecond, "Minimum time between messages")
	keyField := flag.String("key", "", "Field name to extract as Kafka message key")
	dryRun := flag.Bool("dry-run", false, "Generate payloads without producing to Kafka")
	seed := flag.Int64("seed", time.Now().UnixNano(), "Random seed for reproducibility")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Generates test data from AsyncAPI specs and produces to Kafka.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -spec order.yaml -channel orders.created\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -spec order.yaml -channel orders.created -dry-run -count 5\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -spec order.yaml -channel orders.created -count 0\n", os.Args[0])
	}

	flag.Parse()

	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -spec is required")
		flag.Usage()
		os.Exit(1)
	}

	if *channel == "" {
		fmt.Fprintln(os.Stderr, "Error: -channel is required")
		flag.Usage()
		os.Exit(1)
	}

	if *dryRun && (*broker != "localhost:9092" || *keyField != "") {
		fmt.Fprintln(os.Stderr, "Warning: dry-run mode disregards Kafka options")
	}

	doc, err := asyncapi.Load(*specPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading spec: %v\n", err)
		os.Exit(1)
	}

	schema, err := doc.PayloadSchema(*channel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error extracting schema: %v\n", err)
		os.Exit(1)
	}

	gen := generator.New(*seed)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nInterrupted, shutting down...")
		cancel()
	}()

	var sink pipeline.Sink
	if *dryRun {
		sink = pipeline.NewStdoutSink(os.Stdout, os.Stderr)
	} else {
		prod, err := producer.New(*broker)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error connecting to Kafka: %v\n", err)
			os.Exit(1)
		}
		if err := prod.Ping(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: broker %s unreachable: %v\n", *broker, err)
			os.Exit(1)
		}
		sink = pipeline.NewKafkaSink(*channel, prod)
	}
	defer sink.Close()

	p := pipeline.New(pipeline.Config{
		Generator: gen,
		Schema:    schema,
		Count:     *count,
		RateLimit: *rateLimit,
		KeyField:  *keyField,
		Warn:      os.Stderr,
	}, sink)

	stats := p.Run(ctx)
	printStats(stats, *dryRun)
}

func printStats(s pipeline.Stats, dryRun bool) {
	mode := "kafka"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Fprintf(os.Stderr, "\nStats [%s]: total=%d acked=%d failed=%d elapsed=%s\n",
		mode, s.Total, s.Acked, s.Failed, s.Elapsed.Round(time.Millisecond))
}
