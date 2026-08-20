package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
)

func main() {
	var (
		specPath = flag.String("spec", "", "Path to the AsyncAPI specification, in YAML or JSON format")
		channel  = flag.String("channel", "", "Channel name to generate records for")
		message  = flag.String("message", "", "Message name to generate records for")
		count    = flag.Int("count", 1, "Number of records to generate")
		seed     = flag.Int64("seed", time.Now().UnixNano(), "Random seed")
		pretty   = flag.Bool("pretty", false, "Pretty-print generated JSON records")
	)
	flag.Parse()

	if *specPath == "" {
		exitf("missing required -spec")
	}
	if *count < 1 {
		exitf("-count must be greater than 0")
	}

	doc, err := asyncapi.Load(*specPath)
	if err != nil {
		exitf("load spec: %v", err)
	}

	schema, selection, err := doc.PayloadSchema(asyncapi.Selection{
		Channel: *channel,
		Message: *message,
	})
	if err != nil {
		exitf("select payload schema: %v", err)
	}

	rng := rand.New(rand.NewSource(*seed))
	gen := generator.New(rng)

	encoder := json.NewEncoder(os.Stdout)
	if *pretty {
		encoder.SetIndent("", "  ")
	}

	for i := 0; i < *count; i++ {
		record, err := gen.Value(schema, "")
		if err != nil {
			exitf("generate record %d: %v", i+1, err)
		}
		if err := encoder.Encode(record); err != nil {
			exitf("write record %d: %v", i+1, err)
		}
	}

	fmt.Fprintf(os.Stderr, "generated %d record(s) from channel %q", *count, selection.Channel)
	if selection.Message != "" {
		fmt.Fprintf(os.Stderr, ", message %q", selection.Message)
	}
	fmt.Fprintln(os.Stderr)
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, strings.TrimRight(format, "\n")+"\n", args...)
	os.Exit(1)
}
