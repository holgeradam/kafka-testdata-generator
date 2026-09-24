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
// under AVRO the avsc governs. A Topic parameter's payload location is walked
// through the value avsc instead, and its value planted into every Payload
// (#83).
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

	plants, err := topicParameters(value, opts)
	if err != nil {
		return nil, err
	}

	gen := avro.NewGenerator(opts.Synth)
	parts := &wire.Parts{
		Values:  &boundGenerator{gen: gen, model: value, plants: plants},
		Encoder: encoderFor(opts, value, key),
	}
	// A key binding declares a JSON-schema-shaped Key; generating one would
	// silently violate the avsc key contract, so it is ignored, out loud. The
	// spec's Message types play no other part: the avsc governs the Payload.
	for _, mt := range opts.MessageTypes {
		if mt.KeyBinding != nil {
			parts.Warnings = append(parts.Warnings, "Warning: key bindings are ignored under -format avro")
			break
		}
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
		enc, err := NewAvroEncoder(ctx, opts.RegistryURL, opts.Topic, value, key)
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

// topicParameters checks each Topic parameter's payload location against the
// value avsc, before any record exists: the location must be guaranteed, as a
// -keyPath must under AVRO, the type there must hold the value, and no two
// plantings, the Key's included, may land in the same field. It returns what
// to plant into each Payload.
func topicParameters(value *avro.Schema, opts wire.Options) (wire.Plants, error) {
	var plants wire.Plants
	for _, tp := range opts.TopicParameters {
		if tp.Pointer == nil {
			continue
		}
		steps, at, err := avro.Locate(value, tp.Pointer)
		if err != nil {
			return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: location %s: %v", tp.Name, tp.Location, err)}
		}
		if err := avro.HoldsString(at, tp.Value, tp.Location); err != nil {
			return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: %v", tp.Name, err)}
		}
		if plants, err = plants.Add(tp, steps, opts.KeyPath); err != nil {
			return nil, err
		}
	}
	return plants, nil
}

// boundGenerator binds an avsc model to the generator: the value avsc for the
// Payload, with the Topic parameter values planted into each, the key avsc
// for the Key (ADR-0007 decision 3).
type boundGenerator struct {
	gen    *avro.Generator
	model  *avro.Schema
	plants wire.Plants
}

func (g *boundGenerator) Value() (any, error) {
	v, err := g.gen.Value(g.model.Root)
	if err != nil {
		return nil, err
	}
	if err := g.plants.Apply(v); err != nil {
		return nil, err
	}
	return v, nil
}
