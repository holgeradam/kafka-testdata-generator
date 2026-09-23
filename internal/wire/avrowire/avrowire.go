// Package avrowire is the AVRO Wire format adapter (ADR-0007): the value avsc
// generates the Payload, the key avsc the Key, and produce frames both
// Confluent-style under registry-assigned IDs (AvroEncoder) while Dry run
// renders the Avro JSON encoding without a registry (AvroDisplayEncoder).
package avrowire

import (
	"context"
	"fmt"
	"os"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the AVRO adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects AVRO.
func (Format) Name() string { return "avro" }

// Check applies the AVRO flag surface (ADR-0007 decision 6, amended by
// ADR-0009): -avro-schema is required, the Key comes from -avro-key-schema,
// which -keyPath therefore requires, and producing needs a registry.
func (Format) Check(opts wire.Options) error {
	if opts.AvroSchema == "" {
		return &wire.Error{Flag: "avro-schema", Detail: "-avro-schema is required with -format avro"}
	}
	if opts.KeyPath != "" && opts.AvroKeySchema == "" {
		return &wire.Error{Flag: "keyPath", Detail: "-keyPath requires -avro-key-schema under -format avro, so there is a Key to plant"}
	}
	// Producing AVRO data needs Confluent framing (magic byte + registry
	// schema ID, ADR-0007 decision 2), and registration of the value avsc is
	// how that ID comes to exist. Dry run never touches a registry.
	if !opts.DryRun && opts.RegistryURL == "" {
		return &wire.Error{Flag: "registry", Detail: "-registry is required with -format avro when producing"}
	}
	return nil
}

// Build parses the value avsc, and the key avsc when one is given, up front so
// a malformed schema surfaces a typed error before any pipeline work (ADR-0007
// decision 4). The models drive generation; their raw avsc is what the
// AvroEncoder registers. The Message schema and key binding are not consulted:
// under AVRO the avsc governs.
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	value, err := loadAvsc(opts.AvroSchema)
	if err != nil {
		return nil, &wire.Error{Flag: "avro-schema", Err: err}
	}
	var key *avro.Schema
	if opts.AvroKeySchema != "" {
		if key, err = loadAvsc(opts.AvroKeySchema); err != nil {
			return nil, &wire.Error{Flag: "avro-key-schema", Err: err}
		}
	}

	gen := avro.NewGenerator(opts.Synth)
	parts := &wire.Parts{
		Values:  &boundGenerator{gen: gen, model: value},
		Encoder: encoderFor(opts, value, key),
	}
	// A key binding declares a JSON-schema-shaped Key; generating one would
	// silently violate the avsc key contract, so it is ignored, out loud.
	if opts.KeyBinding != nil {
		parts.Warnings = append(parts.Warnings, "Warning: key bindings are ignored under -format avro")
	}
	if key != nil {
		parts.KeyGen = &boundGenerator{gen: gen, model: key}
		if opts.KeyPath != "" {
			parts.Checker = avro.NewKeyChecker(value, key)
		}
	}
	return parts, nil
}

// encoderFor picks the Encoder for the run's mode: Dry run renders from the
// local avsc and never opens a registry connection (ADR-0007 decision 7);
// produce registers the value avsc under <topic>-value and the key avsc under
// <topic>-key, then frames records with the assigned IDs.
func encoderFor(opts wire.Options, value, key *avro.Schema) func(context.Context) (pipeline.Encoder, error) {
	if opts.DryRun {
		return func(context.Context) (pipeline.Encoder, error) {
			return NewAvroDisplayEncoder(value, key), nil
		}
	}
	return func(ctx context.Context) (pipeline.Encoder, error) {
		var keyAvsc string
		if key != nil {
			keyAvsc = string(key.Raw())
		}
		enc, err := NewAvroEncoder(ctx, opts.RegistryURL, opts.Topic, string(value.Raw()), keyAvsc)
		if err != nil {
			return nil, err
		}
		return enc, nil
	}
}

// loadAvsc reads an avsc file and parses it into the Avro model, wrapping read
// failures and propagating the typed *avro.ParseError.
func loadAvsc(path string) (*avro.Schema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading avsc %s: %w", path, err)
	}
	return avro.Parse(b)
}

// boundGenerator binds an avsc model to the generator: the value avsc for the
// Payload, the key avsc for the Key (ADR-0007 decision 3).
type boundGenerator struct {
	gen   *avro.Generator
	model *avro.Schema
}

func (g *boundGenerator) Value() (any, error) { return g.gen.Value(g.model.Root) }
