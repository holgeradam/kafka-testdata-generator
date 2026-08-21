package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
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

	if *dryRun {
		runDryRun(ctx, gen, schema, *channel, *count, *rateLimit, *keyField)
	} else {
		runProduce(ctx, gen, schema, *channel, *broker, *count, *rateLimit, *keyField)
	}
}

func runDryRun(ctx context.Context, gen *generator.Generator, schema map[string]any, channel string, count int, rateLimit time.Duration, keyField string) {
	var total, acked, failed int64
	start := time.Now()

	for {
		if count > 0 && total >= int64(count) {
			break
		}

		select {
		case <-ctx.Done():
			break
		default:
		}

		payload := gen.Value(schema, "")
		total++

		if keyField != "" {
			if key, ok := payload.(map[string]any)[keyField]; ok {
				fmt.Fprintf(os.Stderr, "Key: %v\n", key)
			} else {
				fmt.Fprintf(os.Stderr, "Warning: field %q not found in payload\n", keyField)
				failed++
				continue
			}
		}

		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling payload: %v\n", err)
			failed++
			continue
		}

		fmt.Println(string(data))
		acked++

		time.Sleep(rateLimit)
	}

	elapsed := time.Since(start)
	printStats(total, acked, failed, elapsed, true)
}

func runProduce(ctx context.Context, gen *generator.Generator, schema map[string]any, channel, broker string, count int, rateLimit time.Duration, keyField string) {
	prod, err := producer.New(broker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to Kafka: %v\n", err)
		os.Exit(1)
	}
	defer prod.Close()

	var total, acked, failed int64
	start := time.Now()

	for {
		if count > 0 && total >= int64(count) {
			break
		}

		select {
		case <-ctx.Done():
			break
		default:
		}

		payload := gen.Value(schema, "")
		total++

		var key []byte
		if keyField != "" {
			if k, ok := payload.(map[string]any)[keyField]; ok {
				keyData, _ := json.Marshal(k)
				key = keyData
			} else {
				fmt.Fprintf(os.Stderr, "Warning: field %q not found in payload, skipping\n", keyField)
				failed++
				continue
			}
		}

		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling payload: %v\n", err)
			failed++
			continue
		}

		if err := prod.Send(ctx, channel, key, data); err != nil {
			fmt.Fprintf(os.Stderr, "Error producing message: %v\n", err)
			failed++
			continue
		}

		acked++
		time.Sleep(rateLimit)
	}

	elapsed := time.Since(start)
	printStats(total, acked, failed, elapsed, false)
}

func printStats(total, acked, failed int64, elapsed time.Duration, dryRun bool) {
	mode := "kafka"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Fprintf(os.Stderr, "\nStats [%s]: total=%d acked=%d failed=%d elapsed=%s\n",
		mode, total, acked, failed, elapsed.Round(time.Millisecond))
}

func firstOrEmpty(s []string) string {
	if len(s) > 0 {
		return s[0]
	}
	return ""
}

func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
