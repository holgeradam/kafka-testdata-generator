// Package runplan turns argv into a validated Run: the plan of one generation
// run (ADR-0010). Planning is pure - flags, validation, spec loading, the Wire
// format's parts and the Key plan - so every rule is reachable from a table
// test without spawning a process or dialing anything. The two constructors
// that do I/O, NewSink and NewEncoder, are called by the process edge
// afterwards.
package runplan

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/avrowire"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/jsonwire"
)

// Error reports a run the planner rejects. Flag names the option at fault
// (without its dash) when one rule owns the failure, and Err carries the typed
// cause when the rejection came from another module: a *keyplan.PathError, an
// *avro.ParseError, a *generator.UnsupportedSchemaError. Tests assert on those
// rather than on message text. It is the Wire formats' error type, so a rule
// reports the same shape whether runplan or a format owns it.
type Error = wire.Error

// formats are the Wire format adapters, by the -format value that selects them.
var formats = map[string]wire.Format{}

func init() {
	for _, f := range []wire.Format{jsonwire.Format{}, avrowire.Format{}} {
		formats[f.Name()] = f
	}
}

// Run is a validated plan: everything one generation run needs, with the two
// pieces that touch the network left to NewSink and NewEncoder. Config.Warn is
// left nil for the caller to point at its own stderr.
type Run struct {
	// Config drives the Pipeline; its Encoder is supplied by NewEncoder.
	Config pipeline.Config
	// DryRun selects the stdout sink over a Kafka producer.
	DryRun bool
	// Broker, Acks: the produce path's Kafka options.
	Broker string
	Acks   producer.Acks
	// Format is the active wire format, Topic the channel produced to.
	Format string
	Topic  string
	// RegistryURL is the Confluent Schema Registry base URL (AVRO produce).
	RegistryURL string
	// Warnings are diagnostics the run carries, for the caller to print:
	// disregarded Kafka options in dry run, a key binding ignored under AVRO.
	Warnings []string

	encoder func(ctx context.Context) (pipeline.Encoder, error)
}

// flags holds the parsed flag surface, so the rules below read as a checklist
// rather than a pointer soup.
type flags struct {
	set                                    *flag.FlagSet
	specPath, channel, broker              *string
	count                                  *int
	rateLimit                              *time.Duration
	keyPath, renamedKey                    *string
	dryRun                                 *bool
	seed                                   *int64
	now                                    *nowFlag
	acks                                   *acksFlag
	format                                 *formatFlag
	avroSchema, avroKeySchema, registryURL *string
}

// newFlags defines the CLI surface. It is one function so Plan and Usage
// describe the same flags.
func newFlags() *flags {
	fs := flag.NewFlagSet("kafka-testdata-generator", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	f := &flags{
		set:        fs,
		specPath:   fs.String("spec", "", "Path to AsyncAPI spec file (required)"),
		channel:    fs.String("channel", "", "Kafka topic/channel to produce to (required)"),
		broker:     fs.String("broker", "localhost:9092", "Kafka broker address"),
		count:      fs.Int("count", 10, "Number of payloads to generate (0 = infinite)"),
		rateLimit:  fs.Duration("rate", 10*time.Millisecond, "Minimum time between messages"),
		keyPath:    fs.String("keyPath", "", "Path in the payload where the generated Key is planted, e.g. customer.id or items[0].sku (requires a key schema)"),
		renamedKey: fs.String("key", "", "deprecated: renamed to -keyPath"),
		dryRun:     fs.Bool("dry-run", false, "Generate payloads without producing to Kafka"),
		// 0 stands in for "random" so help states no seed that will not be used;
		// Plan draws the real one when -seed is not given.
		seed:          fs.Int64("seed", 0, "Random seed for reproducibility (default: random)"),
		now:           newNowFlag(),
		acks:          newAcksFlag(),
		format:        newFormatFlag(),
		avroSchema:    fs.String("avro-schema", "", "Path to value avsc file (required with -format avro)"),
		avroKeySchema: fs.String("avro-key-schema", "", "Path to key avsc file (the AVRO key schema)"),
		registryURL:   fs.String("registry", "", "Confluent Schema Registry base URL (required with -format avro when producing)"),
	}
	// A back-quoted word names the value in help, in place of "value".
	fs.Var(f.now, "now", "Clock for date fields, as an RFC3339 `time` (default: the current time)")
	fs.Var(f.acks, "acks", "Acks `level`: 1 (leader) or all (all in-sync replicas)")
	fs.Var(f.format, "format", "Output wire format `name`: json or avro")
	return f
}

// isSet reports whether the named flag was given on the command line.
func (f *flags) isSet(name string) bool {
	set := false
	f.set.Visit(func(fl *flag.Flag) {
		if fl.Name == name {
			set = true
		}
	})
	return set
}

// Usage writes the help block for the CLI to w, naming the binary as invoked.
func Usage(w io.Writer, name string) {
	f := newFlags()
	f.set.SetOutput(w)
	fmt.Fprintf(w, "Usage: %s [options]\n\n", name)
	fmt.Fprintf(w, "Generates test data from AsyncAPI specs and produces to Kafka.\n\n")
	fmt.Fprintf(w, "Options:\n")
	f.set.PrintDefaults()
	fmt.Fprintf(w, "\nExamples:\n")
	fmt.Fprintf(w, "  %s -spec order.yaml -channel orders.created\n", name)
	fmt.Fprintf(w, "  %s -spec order.yaml -channel orders.created -dry-run -count 5\n", name)
	fmt.Fprintf(w, "  %s -spec order.yaml -channel orders.created -count 0\n", name)
}

// Plan validates args and builds the run they describe, or returns the first
// rule they break. It performs no network I/O: only argv, the spec file and the
// avsc files are read. -h and -help return flag.ErrHelp, a request for Usage
// rather than a rule broken.
func Plan(args []string) (*Run, error) {
	f := newFlags()
	if err := f.set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		// An unknown or unparsable flag is a rule about the flags themselves,
		// so it carries no cause.
		return nil, &Error{Detail: err.Error()}
	}
	if !f.isSet("seed") {
		*f.seed = time.Now().UnixNano()
	}

	if *f.specPath == "" {
		return nil, &Error{Flag: "spec", Detail: "-spec is required"}
	}
	if *f.channel == "" {
		return nil, &Error{Flag: "channel", Detail: "-channel is required"}
	}

	// -key extracted a field from the Payload; -keyPath plants the generated
	// Key into it (ADR-0009). The meaning changed, so an old invocation stops
	// with guidance rather than the flag package's bare "not defined".
	if *f.renamedKey != "" {
		return nil, &Error{Flag: "key", Detail: "-key was renamed to -keyPath and changed meaning: the Key is generated from the key schema and planted into the payload at that path, never extracted from it. Use -keyPath, together with a key schema (bindings.kafka.key in JSON mode, -avro-key-schema under -format avro)."}
	}

	run := &Run{
		DryRun:      *f.dryRun,
		Broker:      *f.broker,
		Acks:        f.acks.acks,
		Format:      f.format.format,
		Topic:       *f.channel,
		RegistryURL: *f.registryURL,
	}

	if err := run.checkFormatFlags(f); err != nil {
		return nil, err
	}
	run.warnDisregardedOptions(f)

	if err := run.loadSchemas(f); err != nil {
		return nil, err
	}
	return run, nil
}

// checkFormatFlags applies the AVRO flag surface (ADR-0007 decision 6, amended
// by ADR-0009): the avro flags are invalid for json; under avro, -avro-schema
// is required and the Key comes from -avro-key-schema, which -keyPath requires.
// It runs before any file is read.
func (r *Run) checkFormatFlags(f *flags) error {
	if r.Format == "avro" {
		if *f.avroSchema == "" {
			return &Error{Flag: "avro-schema", Detail: "-avro-schema is required with -format avro"}
		}
		if *f.keyPath != "" && *f.avroKeySchema == "" {
			return &Error{Flag: "keyPath", Detail: "-keyPath requires -avro-key-schema under -format avro, so there is a Key to plant"}
		}
		// Producing AVRO data needs Confluent framing (magic byte + registry
		// schema ID, ADR-0007 decision 2), and registration of the value avsc is
		// how that ID comes to exist. Dry run never touches a registry.
		if !r.DryRun && r.RegistryURL == "" {
			return &Error{Flag: "registry", Detail: "-registry is required with -format avro when producing"}
		}
		return nil
	}
	if *f.avroSchema != "" || *f.avroKeySchema != "" {
		return &Error{Flag: "avro-schema", Detail: "-avro-schema and -avro-key-schema are only valid with -format avro"}
	}
	if r.RegistryURL != "" {
		return &Error{Flag: "registry", Detail: "-registry is only valid with -format avro"}
	}
	return nil
}

// warnDisregardedOptions records the dry-run diagnostic. -keyPath is honoured
// in dry run (the Key is echoed), so only the options that reach Kafka or the
// registry count as disregarded.
func (r *Run) warnDisregardedOptions(f *flags) {
	if r.DryRun && (f.isSet("broker") || f.isSet("acks") || r.RegistryURL != "") {
		r.Warnings = append(r.Warnings, "Warning: dry-run mode disregards Kafka options")
	}
}

// loadSchemas reads the spec, then has the Wire format wire the run's
// generation, Key source and encoder from it and its own files.
func (r *Run) loadSchemas(f *flags) error {
	doc, err := asyncapi.Load(*f.specPath)
	if err != nil {
		return &Error{Flag: "spec", Detail: "loading spec", Err: err}
	}
	schema, err := doc.PayloadSchema(*f.channel)
	if err != nil {
		return &Error{Flag: "channel", Detail: "extracting schema", Err: err}
	}
	keyBinding, err := doc.KeyBinding(*f.channel)
	if err != nil {
		return &Error{Flag: "channel", Detail: "extracting key binding", Err: err}
	}

	if *f.keyPath != "" && r.Format != "avro" && keyBinding == nil {
		return &Error{Flag: "keyPath", Detail: "-keyPath requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to plant"}
	}

	// Key bindings declare a JSON-schema-shaped key, but under -format avro the
	// Key comes from the key avsc or stays null. Generating a JSON-shaped key
	// value would silently violate the avsc key contract, so bindings are
	// ignored under avro with a warning.
	if r.Format == "avro" && keyBinding != nil {
		r.Warnings = append(r.Warnings, "Warning: key bindings are ignored under -format avro")
		keyBinding = nil
	}

	parts, err := formats[r.Format].Build(wire.Options{
		DryRun:        r.DryRun,
		Topic:         r.Topic,
		KeyPath:       *f.keyPath,
		RegistryURL:   r.RegistryURL,
		AvroSchema:    *f.avroSchema,
		AvroKeySchema: *f.avroKeySchema,
		// One Synthesizer per run: the Payload and the Key draw from one
		// shared stream in both wire formats (ADR-0008 decision 4).
		Synth:      synth.New(*f.seed, f.now.now),
		Schema:     schema,
		KeyBinding: keyBinding,
		ResolveRef: doc.ResolveRef,
	})
	if err != nil {
		return err
	}
	r.encoder = parts.Encoder

	// The Key plan owns the Key of the run: the key schema generates it, and
	// -keyPath says where it is planted into the Payload (ADR-0009). Its checks
	// run here, so an unusable path stops the run before a record exists.
	var keyPlan pipeline.KeyPlan
	if parts.KeyGen != nil {
		plan, err := keyplan.New(parts.KeyGen, parts.Checker, *f.keyPath)
		if err != nil {
			return &Error{Flag: "keyPath", Err: err}
		}
		keyPlan = plan
	}

	r.Config = pipeline.Config{
		Generator: parts.Values,
		Schema:    schema,
		Count:     *f.count,
		RateLimit: *f.rateLimit,
		KeyPlan:   keyPlan,
	}
	return nil
}

// NewSink builds the run's destination: stdout in dry run, otherwise a Kafka
// producer, which is dialed and pinged here rather than during planning. The
// broker is contacted before any registry work, so a run that cannot produce
// never registers a schema (ADR-0010).
func (r *Run) NewSink(ctx context.Context, stdout, stderr io.Writer) (pipeline.Sink, error) {
	if r.DryRun {
		return pipeline.NewStdoutSink(stdout, stderr), nil
	}
	prod, err := producer.New(r.Broker, producer.Options{Acks: r.Acks})
	if err != nil {
		return nil, &Error{Flag: "broker", Detail: "connecting to Kafka", Err: err}
	}
	if err := prod.Ping(ctx); err != nil {
		return nil, &Error{Flag: "broker", Detail: fmt.Sprintf("broker %s unreachable", r.Broker), Err: err}
	}
	return pipeline.NewKafkaSink(r.Topic, prod), nil
}

// NewEncoder builds the Encoder the Wire format chose for the run's mode. Only
// AVRO produce contacts a network: it registers the avsc files with the schema
// registry, so it is called after NewSink has reached the broker.
func (r *Run) NewEncoder(ctx context.Context) (pipeline.Encoder, error) {
	return r.encoder(ctx)
}

// acksFlag is a flag.Value accepting "1" or "all" (case-insensitive) for the
// Kafka acknowledgement level. Invalid values fail at parse time with a hint.
type acksFlag struct {
	acks producer.Acks
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
	return nil
}

// String satisfies flag.Value and is used for the flag default and usage. The
// unset zero value renders empty, so help shows the real default beside it.
func (a *acksFlag) String() string {
	if a == nil || a.acks == 0 {
		return ""
	}
	return a.acks.String()
}

// nowFlag is a flag.Value accepting an RFC3339 timestamp for the generator's
// explicit clock. It defaults to wall-clock so omitting -now still works; an
// invalid value fails at parse time with a hint.
type nowFlag struct {
	now time.Time
	set bool
}

func newNowFlag() *nowFlag {
	return &nowFlag{now: time.Now()}
}

// Set parses the -now value as RFC3339; the flag package reports parse errors
// at parse time.
func (n *nowFlag) Set(v string) error {
	parsed, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return fmt.Errorf("invalid -now: %w", err)
	}
	n.now = parsed
	n.set = true
	return nil
}

// String satisfies flag.Value. The wall-clock default renders empty, so help
// states no moment that the run will not use.
func (n *nowFlag) String() string {
	if n == nil || !n.set {
		return ""
	}
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

// String satisfies flag.Value and is used for the flag default and usage. The
// zero value renders empty, so help shows the json default beside it.
func (f *formatFlag) String() string {
	if f == nil {
		return ""
	}
	return f.format
}
