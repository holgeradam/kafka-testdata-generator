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
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
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
	nowFlag := newNowFlag()
	flag.Var(nowFlag, "now", "Clock for date fields (RFC3339; default now)")
	acksFlag := newAcksFlag()
	flag.Var(acksFlag, "acks", "Acks level: 1 (leader) or all (all in-sync replicas)")
	formatFlag := newFormatFlag()
	flag.Var(formatFlag, "format", "Output wire format: json (default) or avro")
	avroSchemaPath := flag.String("avro-schema", "", "Path to value avsc file (required with -format avro)")
	avroKeySchemaPath := flag.String("avro-key-schema", "", "Path to key avsc file (mutually exclusive with -key under -format avro)")
	registryURL := flag.String("registry", "", "Confluent Schema Registry base URL (required with -format avro when producing)")

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

	// AVRO flag surface (ADR-0007 decision 6): the avro flags are invalid for
	// json; under avro, -avro-schema is required and -avro-key-schema excludes
	// -key. Validation happens before any file is loaded.
	if formatFlag.format == "avro" {
		if *avroSchemaPath == "" {
			fmt.Fprintln(os.Stderr, "Error: -avro-schema is required with -format avro")
			flag.Usage()
			os.Exit(1)
		}
		if *avroKeySchemaPath != "" && *keyField != "" {
			fmt.Fprintln(os.Stderr, "Error: -avro-key-schema and -key are mutually exclusive under -format avro")
			flag.Usage()
			os.Exit(1)
		}
		// Producing AVRO data needs Confluent framing (magic byte + registry
		// schema ID, ADR-0007 decision 2), and registration of the value avsc is
		// how that ID comes to exist. Dry-run never touches a registry.
		if !*dryRun && *registryURL == "" {
			fmt.Fprintln(os.Stderr, "Error: -registry is required with -format avro when producing")
			flag.Usage()
			os.Exit(1)
		}
	} else {
		if *avroSchemaPath != "" || *avroKeySchemaPath != "" {
			fmt.Fprintln(os.Stderr, "Error: -avro-schema and -avro-key-schema are only valid with -format avro")
			flag.Usage()
			os.Exit(1)
		}
		if *registryURL != "" {
			fmt.Fprintln(os.Stderr, "Error: -registry is only valid with -format avro")
			flag.Usage()
			os.Exit(1)
		}
	}

	if *dryRun && (*broker != "localhost:9092" || *keyField != "" || acksFlag.set || *registryURL != "") {
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

	keyBinding, err := doc.KeyBinding(*channel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error extracting key binding: %v\n", err)
		os.Exit(1)
	}

	// Key bindings declare a JSON-schema-shaped key, but under -format avro the
	// Key is either extracted from the AVRO-native payload (via -key) or stays
	// null. Generating a JSON-shaped key value would silently violate the avsc
	// key contract, so bindings are ignored under avro with a warning.
	if formatFlag.format == "avro" && keyBinding != nil {
		fmt.Fprintln(os.Stderr, "Warning: key bindings are ignored under -format avro")
		keyBinding = nil
	}

	gen := generator.New(*seed, nowFlag.now)
	gen.SetRefResolver(doc.ResolveRef)

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
		prod, err := producer.New(*broker, producer.Options{Acks: acksFlag.acks})
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

	var avroModel *avro.Schema
	if formatFlag.format == "avro" {
		// Parse the value avsc (and key avsc when supplied) up front so a
		// malformed schema surfaces a typed error before any pipeline work
		// (ADR-0007 decision 4). The value model drives AVRO generation; its
		// raw avsc is what the AvroEncoder registers with the registry.
		avroModel, err = loadAvroSchema(*avroSchemaPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if *avroKeySchemaPath != "" {
			if _, err := loadAvroSchema(*avroKeySchemaPath); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			// Key avsc encoding is a later vertical; parsing still validates it.
			fmt.Fprintln(os.Stderr, "Warning: -avro-key-schema encoding is not yet implemented; keys follow the plain-scalar or null contract")
		}
	}

	// AVRO generation follows the value avsc model instead of the JSON Schema
	// (ADR-0007 decision 3); the adapter keeps the Pipeline on the same seam by
	// ignoring the JSON schema argument.
	valueGenerator := pipeline.ValueGenerator(gen)
	if formatFlag.format == "avro" {
		valueGenerator = &avroValueGenerator{generator: avro.NewGenerator(*seed, nowFlag.now), model: avroModel}
	}

	enc, err := newEncoder(ctx, formatFlag.format, *registryURL, *channel, avroModel, *dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	p := pipeline.New(pipeline.Config{
		Generator:  valueGenerator,
		Schema:     schema,
		Count:      *count,
		RateLimit:  *rateLimit,
		KeyField:   *keyField,
		KeyBinding: keyBinding,
		Encoder:    enc,
		Warn:       os.Stderr,
	}, sink)

	stats, err := p.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	printStats(stats, *dryRun)
}

// avroValueGenerator is a pipeline.ValueGenerator adapter: AVRO generation
// follows the parsed value avsc model, so the JSON schema argument from the
// Pipeline is ignored and every Value honours the model (ADR-0007 decision 3).
type avroValueGenerator struct {
	generator *avro.Generator
	model     *avro.Schema
}

func (g *avroValueGenerator) Value(_ map[string]any) (any, error) {
	return g.generator.Value(g.model.Root)
}

// newEncoder constructs the Encoder for the given wire format. json marshals
// generated values directly; avro returns an AvroEncoder that registers the
// exact value avsc under <topic>-value and frames payloads with the
// registry-assigned schema ID. Dry-run avro returns the AvroDisplayEncoder,
// which renders each value in the Avro JSON encoding - readable text, with
// logical types in their human-readable form - from the local avsc and never
// opens a registry connection (ADR-0007 decision 7).
func newEncoder(ctx context.Context, format, registryURL, topic string, valueModel *avro.Schema, dryRun bool) (pipeline.Encoder, error) {
	switch format {
	case "json":
		return pipeline.JsonEncoder{}, nil
	case "avro":
		if valueModel == nil {
			return nil, fmt.Errorf("-format avro requires the value avsc")
		}
		if dryRun {
			// Display renders from the parsed avsc alone; the registry flag is
			// disregarded with the standard dry-run warning in main.
			return pipeline.NewAvroDisplayEncoder(valueModel), nil
		}
		return pipeline.NewAvroEncoder(ctx, registryURL, topic+"-value", string(valueModel.Raw()))
	default:
		return nil, fmt.Errorf("unknown format %q (supported: json, avro)", format)
	}
}

// loadAvroSchema reads an avsc file and parses it into the Avro model,
// wrapping read failures and propagating the typed *avro.ParseError.
func loadAvroSchema(path string) (*avro.Schema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading avsc %s: %w", path, err)
	}
	return avro.Parse(b)
}

func printStats(s pipeline.Stats, dryRun bool) {
	mode := "kafka"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Fprintf(os.Stderr, "\nStats [%s]: total=%d acked=%d failed=%d elapsed=%s\n",
		mode, s.Total, s.Acked, s.Failed, s.Elapsed.Round(time.Millisecond))
}

// acksFlag is a flag.Value accepting "1" or "all" (case-insensitive) for the
// Kafka acknowledgement level. Invalid values fail at parse time with a hint.
type acksFlag struct {
	acks producer.Acks
	set  bool
}

func newAcksFlag() *acksFlag {
	return &acksFlag{acks: producer.AcksLeader}
}

// Set parses the -acks value case-insensitively via producer.ParseAcks; the
// flag package reports the to/from string.
func (a *acksFlag) Set(v string) error {
	acks, err := producer.ParseAcks(v)
	if err != nil {
		return fmt.Errorf("invalid -acks: %w", err)
	}
	a.acks = acks
	a.set = true
	return nil
}

// String satisfies flag.Value and is used for the flag default and usage.
func (a *acksFlag) String() string {
	return a.acks.String()
}

// nowFlag is a flag.Value accepting an RFC3339 timestamp for the generator's
// explicit clock. It defaults to wall-clock so omitting -now still works; an
// invalid value fails at parse time with a hint.
type nowFlag struct {
	now time.Time
}

func newNowFlag() *nowFlag {
	return &nowFlag{now: time.Now()}
}

// Set parses the -now value as RFC3339; the flag package reports parse errors
// at flag.Parse time.
func (n *nowFlag) Set(v string) error {
	parsed, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return fmt.Errorf("invalid -now: %w", err)
	}
	n.now = parsed
	return nil
}

// String satisfies flag.Value and is used for the flag default and usage.
func (n *nowFlag) String() string {
	return n.now.Format(time.RFC3339)
}

// formatFlag is a flag.Value accepting "json" or "avro" (case-sensitive) for
// the output wire format. Invalid values fail at parse time with a hint.
type formatFlag struct {
	format string
}

func newFormatFlag() *formatFlag {
	return &formatFlag{format: "json"}
}

// Set parses the -format value; the flag package reports parse errors.
func (f *formatFlag) Set(v string) error {
	switch v {
	case "json", "avro":
		f.format = v
	default:
		return fmt.Errorf("invalid -format %q (supported: json, avro)", v)
	}
	return nil
}

// String satisfies flag.Value and is used for the flag default and usage.
func (f *formatFlag) String() string {
	return f.format
}
