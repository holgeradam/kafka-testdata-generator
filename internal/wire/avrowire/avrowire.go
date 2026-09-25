// Package avrowire is the AVRO Wire format adapter (ADR-0007): the value avsc
// generates the Payload, the key avsc the Key, each from the spec or a file, and produce frames both
// Confluent-style under registry-assigned IDs (AvroEncoder) while Dry run
// renders the Avro JSON encoding without a registry (AvroDisplayEncoder).
package avrowire

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the AVRO adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects AVRO.
func (Format) Name() string { return "avro" }

// Check applies the AVRO flag surface (ADR-0007 decision 6, amended by
// ADR-0009 and ADR-0011), judged against the spec. The Payload's avsc comes
// from the spec's Avro payload or from -avro-schema, never both; the Key's
// from the spec's Avro Key binding or from -avro-key-schema, never both, and
// -keyPath needs one. Producing needs a registry. One Avro Message type is
// read until #91, and the Kafka message binding's registry fields must be
// ones the Confluent framing honours (#84 decision 8).
func (Format) Check(opts wire.Options) error {
	spec := specAvro(opts.MessageTypes)
	if len(spec) > 1 {
		return &wire.Error{Flag: "topic", Detail: fmt.Sprintf("several Avro Message types (%s) are not supported yet", names(spec))}
	}
	switch {
	case len(spec) == 1 && opts.AvroSchema != "":
		return &wire.Error{Flag: "avro-schema", Detail: fmt.Sprintf("the spec declares the Payload's avsc (payload of %s is Avro) and -avro-schema gives another; drop -avro-schema, so the run has one source of truth", spec[0].Name)}
	case len(spec) == 0 && opts.AvroSchema == "":
		return &wire.Error{Flag: "avro-schema", Detail: "-avro-schema is required with -format avro when the spec's payloads are JSON Schema"}
	}
	specKey := len(spec) == 1 && spec[0].KeyAvsc != nil
	if specKey && opts.AvroKeySchema != "" {
		return &wire.Error{Flag: "avro-key-schema", Detail: fmt.Sprintf("the spec declares the Key's avsc (bindings.kafka.key of %s) and -avro-key-schema gives another; drop -avro-key-schema, so the run has one source of truth", spec[0].Name)}
	}
	if opts.KeyPath != "" && opts.AvroKeySchema == "" && !specKey {
		return &wire.Error{Flag: "keyPath", Detail: "-keyPath requires a key avsc: -avro-key-schema, or a Key binding beside the spec's Avro payload, so there is a Key to plant"}
	}
	// Producing AVRO data needs Confluent framing (magic byte + registry
	// schema ID, ADR-0007 decision 2), and registration of the value avsc is
	// how that ID comes to exist. Dry run never touches a registry.
	if !opts.DryRun && opts.RegistryURL == "" {
		return &wire.Error{Flag: "registry", Detail: "-registry is required to produce with the avro Wire format"}
	}
	for _, mt := range opts.MessageTypes {
		if err := checkRegistry(mt); err != nil {
			return err
		}
	}
	return nil
}

// checkRegistry refuses a Kafka message binding whose registry fields the
// tool does not honour: its framing puts a Confluent schema ID in the payload,
// registered under <topic>-value, so consumers told otherwise could not read
// the records.
func checkRegistry(mt asyncapi.MessageType) error {
	r := mt.Registry
	refuse := func(field, value, why string) error {
		return &wire.Error{Flag: "topic", Detail: fmt.Sprintf("message %s: bindings.kafka.%s is %s; %s", mt.Name, field, value, why)}
	}
	switch {
	case r.SchemaIDLocation != "" && r.SchemaIDLocation != "payload":
		return refuse("schemaIdLocation", r.SchemaIDLocation, "the tool frames the schema ID in the payload (Confluent wire format), so it honours only payload")
	case r.SchemaIDPayloadEncoding != "" && r.SchemaIDPayloadEncoding != "confluent" && r.SchemaIDPayloadEncoding != "4":
		return refuse("schemaIdPayloadEncoding", r.SchemaIDPayloadEncoding, "the tool encodes the schema ID the Confluent way, so it honours only confluent or 4")
	case r.SchemaLookupStrategy != "" && r.SchemaLookupStrategy != "TopicNameStrategy" && r.SchemaLookupStrategy != "TopicIdStrategy":
		return refuse("schemaLookupStrategy", r.SchemaLookupStrategy, "the tool registers under <topic>-value, so it honours only TopicNameStrategy or TopicIdStrategy")
	}
	return nil
}

// specAvro returns the Message types whose payloads the spec declares in
// Avro. The Run plan has already refused a Kafka topic mixing payload
// formats, so these are all of them or none.
func specAvro(types []asyncapi.MessageType) []asyncapi.MessageType {
	var out []asyncapi.MessageType
	for _, mt := range types {
		if mt.Avsc != nil {
			out = append(out, mt)
		}
	}
	return out
}

func names(types []asyncapi.MessageType) string {
	n := make([]string, len(types))
	for i, mt := range types {
		n[i] = mt.Name
	}
	return strings.Join(n, ", ")
}

// Build parses the value avsc, and the key avsc when the run has one, up front
// so a malformed schema surfaces a typed error before any pipeline work
// (ADR-0007 decision 4). Each comes from the spec, when it declares Avro, or
// from its file. The models drive generation; their raw avsc is what the
// AvroEncoder registers. A spec whose payloads are JSON Schema is not
// consulted: -avro-schema governs. A Topic parameter's payload location is
// walked through the value avsc instead, and its value planted into every
// Payload (#83).
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	spec := specAvro(opts.MessageTypes)
	value, err := valueAvsc(opts, spec)
	if err != nil {
		return nil, err
	}
	key, err := keyAvsc(opts, spec)
	if err != nil {
		return nil, err
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
	// A JSON Schema payload's key binding declares a JSON-schema-shaped Key;
	// generating one would silently violate the avsc key contract, so it is
	// ignored, out loud. The spec's Message types play no other part: the
	// avsc governs the Payload.
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

// valueAvsc parses the Payload's avsc: the spec's, else -avro-schema's.
func valueAvsc(opts wire.Options, spec []asyncapi.MessageType) (*avro.Schema, error) {
	if len(spec) > 0 {
		value, err := avro.Parse(spec[0].Avsc)
		if err != nil {
			return nil, &wire.Error{Flag: "topic", Detail: "payload of " + spec[0].Name, Err: err}
		}
		return value, nil
	}
	value, err := loadAvsc(opts.AvroSchema)
	if err != nil {
		return nil, &wire.Error{Flag: "avro-schema", Err: err}
	}
	return value, nil
}

// keyAvsc parses the Key's avsc: the spec's Key binding, else
// -avro-key-schema's; nil when the run has neither.
func keyAvsc(opts wire.Options, spec []asyncapi.MessageType) (*avro.Schema, error) {
	if len(spec) > 0 && spec[0].KeyAvsc != nil {
		key, err := avro.Parse(spec[0].KeyAvsc)
		if err != nil {
			return nil, &wire.Error{Flag: "topic", Detail: "bindings.kafka.key of " + spec[0].Name, Err: err}
		}
		return key, nil
	}
	if opts.AvroKeySchema == "" {
		return nil, nil
	}
	key, err := loadAvsc(opts.AvroKeySchema)
	if err != nil {
		return nil, &wire.Error{Flag: "avro-key-schema", Err: err}
	}
	return key, nil
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
