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
	"strings"
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
	// Format is the active wire format, Topic the Kafka topic produced to.
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
	specPath, topic, broker                *string
	count, recordsPerKey                   *int
	rateLimit                              *time.Duration
	keyPath, renamedKey, renamedChannel    *string
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
		set:            fs,
		specPath:       fs.String("spec", "", "Path to AsyncAPI spec file (required)"),
		topic:          fs.String("topic", "", "Kafka topic to produce to (required)"),
		broker:         fs.String("broker", "localhost:9092", "Kafka broker address"),
		count:          fs.Int("count", 10, "Number of payloads to generate (0 = infinite)"),
		recordsPerKey:  fs.Int("records-per-key", 1, "Average number of records sharing one Key, i.e. one Entity (requires a key schema)"),
		rateLimit:      fs.Duration("rate", 10*time.Millisecond, "Minimum time between messages"),
		keyPath:        fs.String("keyPath", "", "Path in the payload where the generated Key is planted, e.g. customer.id or items[0].sku (requires a key schema)"),
		renamedKey:     fs.String("key", "", "deprecated: renamed to -keyPath"),
		renamedChannel: fs.String("channel", "", "deprecated: renamed to -topic"),
		dryRun:         fs.Bool("dry-run", false, "Generate payloads without producing to Kafka"),
		// 0 stands in for "random" so help states no seed that will not be used;
		// Plan draws the real one when -seed is not given.
		seed:          fs.Int64("seed", 0, "Random seed for reproducibility (default: random)"),
		now:           newNowFlag(),
		acks:          newAcksFlag(),
		format:        newFormatFlag(),
		avroSchema:    fs.String("avro-schema", "", "Path to value avsc file, for a spec whose payloads are JSON Schema; makes the run AVRO"),
		avroKeySchema: fs.String("avro-key-schema", "", "Path to key avsc file (the AVRO key schema), unless the spec declares an Avro Key binding"),
		registryURL:   fs.String("registry", "", "Confluent Schema Registry base URL (required to produce AVRO)"),
	}
	// A back-quoted word names the value in help, in place of "value".
	fs.Var(f.now, "now", "Clock for date fields, as an RFC3339 `time` (default: the current time)")
	fs.Var(f.acks, "acks", "Acks `level`: 1 (leader) or all (all in-sync replicas)")
	fs.Var(f.format, "format", "Wire format `name`: json or avro (default: avro when the spec's payloads are Avro or -avro-schema is given, else json)")
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
	fmt.Fprintf(w, "  %s -spec order.yaml -topic orders.created\n", name)
	fmt.Fprintf(w, "  %s -spec order.yaml -topic orders.created -dry-run -count 5\n", name)
	fmt.Fprintf(w, "  %s -spec order.yaml -topic orders.created -count 0\n", name)
}

// Plan validates args and builds the run they describe, or returns the first
// rule they break. It performs no network I/O: only argv, the spec file and the
// avsc files are read. The Wire format's rules run once the spec is read, since
// the spec's payloads decide the Wire format. -h and -help return flag.ErrHelp, a request for Usage
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
	// -channel named the spec's channels: entry; -topic names the Kafka topic,
	// which the entry may bind under another key (bindings.kafka.topic). An old
	// invocation stops with guidance, as -key does.
	if *f.renamedChannel != "" {
		return nil, &Error{Flag: "channel", Detail: "-channel was renamed to -topic: it names the Kafka topic to produce to, which the spec's entry may declare under another key through bindings.kafka.topic. Use -topic."}
	}
	if *f.topic == "" {
		return nil, &Error{Flag: "topic", Detail: "-topic is required"}
	}
	if *f.recordsPerKey < 1 {
		return nil, &Error{Flag: "records-per-key", Detail: "-records-per-key must be at least 1"}
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
		Topic:       *f.topic,
		RegistryURL: *f.registryURL,
	}

	doc, err := asyncapi.Load(*f.specPath)
	if err != nil {
		return nil, &Error{Flag: "spec", Detail: "loading spec", Err: err}
	}
	topic, err := doc.Topic(*f.topic)
	if err != nil {
		return nil, &Error{Flag: "topic", Detail: "reading the spec", Err: err}
	}

	// The Wire format follows from the spec and the flags; the format then
	// owns the rules about its flags, which it judges against the spec.
	if run.Format, err = wireFormat(f, run.Topic, topic.MessageTypes); err != nil {
		return nil, err
	}
	format := formats[run.Format]
	opts := wire.Options{
		DryRun:          run.DryRun,
		Topic:           run.Topic,
		KeyPath:         *f.keyPath,
		RegistryURL:     run.RegistryURL,
		AvroSchema:      *f.avroSchema,
		AvroKeySchema:   *f.avroKeySchema,
		MessageTypes:    topic.MessageTypes,
		TopicParameters: topic.Parameters,
	}
	if err := format.Check(opts); err != nil {
		return nil, err
	}
	run.warnDisregardedOptions(f)

	if err := run.build(f, format, opts); err != nil {
		return nil, err
	}
	return run, nil
}

// wireFormat infers the run's Wire format from where the Payload's schema
// comes from (#84 decisions 2, 3 and 9; ADR-0011): Avro payloads in the spec,
// or -avro-schema, mean avro, otherwise json. -format only confirms it, and
// stops the run when it disagrees; it never converts. A Kafka topic is
// produced in one Wire format, so its Message types must share one payload
// format.
func wireFormat(f *flags, topic string, types []asyncapi.MessageType) (string, error) {
	var avroTypes, jsonTypes []string
	for _, mt := range types {
		if mt.Avsc != nil {
			avroTypes = append(avroTypes, mt.Name)
		} else {
			jsonTypes = append(jsonTypes, mt.Name)
		}
	}
	declared := f.format.format
	switch {
	case len(avroTypes) > 0 && len(jsonTypes) > 0:
		return "", &Error{Flag: "topic", Detail: fmt.Sprintf("Kafka topic %q mixes payload formats: Avro (%s) and JSON Schema (%s); a Kafka topic is produced in one Wire format", topic, strings.Join(avroTypes, ", "), strings.Join(jsonTypes, ", "))}
	case len(avroTypes) > 0:
		if declared == "json" {
			return "", &Error{Flag: "format", Detail: fmt.Sprintf("payload of %s is Avro, so the Wire format is avro; drop -format json", avroTypes[0])}
		}
		return "avro", nil
	case *f.avroSchema != "":
		if declared == "json" {
			return "", &Error{Flag: "format", Detail: "-avro-schema makes the Wire format avro; drop -format json"}
		}
		return "avro", nil
	case declared != "":
		return declared, nil
	}
	return "json", nil
}

// warnDisregardedOptions records the dry-run diagnostic. -keyPath is honoured
// in dry run (the Key is echoed), so only the options that reach Kafka or the
// registry count as disregarded.
func (r *Run) warnDisregardedOptions(f *flags) {
	if r.DryRun && (f.isSet("broker") || f.isSet("acks") || r.RegistryURL != "") {
		r.Warnings = append(r.Warnings, "Warning: dry-run mode disregards Kafka options")
	}
}

// build has the Wire format wire the run's generation, Key source and encoder
// from the spec and its own files.
func (r *Run) build(f *flags, format wire.Format, opts wire.Options) error {
	// One Synthesizer per run: the Payload and the Key draw from one shared
	// stream in both wire formats (ADR-0008 decision 4), and so do the Key
	// reuse decisions.
	opts.Synth = synth.New(*f.seed, f.now.now)
	parts, err := format.Build(opts)
	if err != nil {
		return err
	}
	r.encoder = parts.Encoder
	r.Warnings = append(r.Warnings, parts.Warnings...)

	// The Key plan owns the Key of the run: the key schema generates it, and
	// -keyPath says where it is planted into the Payload (ADR-0009). Its checks
	// run here, so an unusable path stops the run before a record exists.
	// With -records-per-key above 1 its Keys identify Entities that recur
	// across records, which needs a Key schema to generate them from.
	if *f.recordsPerKey > 1 && parts.KeyGen == nil {
		return &Error{Flag: "records-per-key", Detail: "-records-per-key above 1 requires a key schema: declare message.bindings.kafka.key in the spec, or pass -avro-key-schema under AVRO, so there is a Key to reuse"}
	}
	var keyPlan pipeline.KeyPlan
	if parts.KeyGen != nil {
		keyGen := keyplan.Reuse(parts.KeyGen, *f.recordsPerKey, opts.Synth)
		plan, err := keyplan.New(keyGen, parts.Checker, *f.keyPath)
		if err != nil {
			return &Error{Flag: "keyPath", Err: err}
		}
		keyPlan = plan
	}

	r.Config = pipeline.Config{
		Generator: parts.Values,
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

// formatFlag is a flag.Value accepting the name of a Wire format
// (case-sensitive). Invalid values fail at parse time with a hint. Unset, it
// is empty and the Wire format is inferred.
type formatFlag struct {
	format string
}

func newFormatFlag() *formatFlag {
	return &formatFlag{}
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
// unset value renders empty, so help states the inferred default beside it.
func (f *formatFlag) String() string {
	if f == nil {
		return ""
	}
	return f.format
}
