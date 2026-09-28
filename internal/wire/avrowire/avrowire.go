// Package avrowire is the AVRO Wire format adapter (ADR-0007): the value avsc
// generates the Payload, the key avsc the Key, each from the spec or a file, and produce frames both
// Confluent-style under registry-assigned IDs (AvroEncoder) while Dry run
// renders the Avro JSON encoding without a registry (AvroDisplayEncoder).
package avrowire

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the AVRO adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects AVRO.
func (Format) Name() string { return "avro" }

// checkFlags applies the AVRO flag surface (ADR-0007 decision 6, amended by
// ADR-0009 and ADR-0011), judged against the spec before any avsc is read.
// The Payload's avsc comes from the spec's Avro payloads or from
// -avro-schema, never both; the Key's from the spec's Avro Key binding or
// from -avro-key-schema, never both, and -keyPath and Key reuse need one.
// Producing needs a registry. The Kafka message binding's registry fields
// must be ones the Confluent framing honours (#84 decision 8).
func checkFlags(flags wire.Flags, topic *asyncapi.Topic) error {
	spec := specAvro(topic)
	switch {
	case len(spec) > 0 && flags.AvroSchema != "":
		return &wire.Error{Usage: true, Flag: "avro-schema", Detail: fmt.Sprintf("the spec declares the Payload's avsc (payload of %s is Avro) and -avro-schema gives another; drop -avro-schema, so the run has one source of truth", spec[0].Name)}
	case len(spec) == 0 && flags.AvroSchema == "":
		return &wire.Error{Usage: true, Flag: "avro-schema", Detail: "-avro-schema is required with -format avro when the spec's payloads are JSON Schema"}
	}
	keyed := slices.IndexFunc(spec, func(mt asyncapi.MessageType) bool { return mt.Key != nil })
	if keyed >= 0 && flags.AvroKeySchema != "" {
		return &wire.Error{Usage: true, Flag: "avro-key-schema", Detail: fmt.Sprintf("the spec declares the Key's avsc (bindings.kafka.key of %s) and -avro-key-schema gives another; drop -avro-key-schema, so the run has one source of truth", spec[keyed].Name)}
	}
	if flags.KeyPath != "" && flags.AvroKeySchema == "" && keyed < 0 {
		return &wire.Error{Usage: true, Flag: "keyPath", Detail: "-keyPath requires a key avsc: -avro-key-schema, or a Key binding beside the spec's Avro payload, so there is a Key to plant"}
	}
	// Producing AVRO data needs Confluent framing (magic byte + registry
	// schema ID, ADR-0007 decision 2), and registration of the value avsc is
	// how that ID comes to exist. Dry run never touches a registry.
	if !flags.DryRun && flags.RegistryURL == "" {
		return &wire.Error{Usage: true, Flag: "registry", Detail: "-registry is required to produce with the avro Wire format"}
	}
	for _, mt := range topic.MessageTypes {
		if err := checkRegistry(mt); err != nil {
			return err
		}
	}
	// With -records-per-key above 1 Keys identify Entities that recur across
	// records, which needs a key avsc to generate them from (#75).
	if flags.RecordsPerKey > 1 && flags.AvroKeySchema == "" && keyed < 0 {
		return &wire.Error{Usage: true, Flag: "records-per-key", Detail: "-records-per-key above 1 requires a key schema: pass -avro-key-schema, or declare a Key binding beside the spec's Avro payload, so there is a Key to reuse"}
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
// Avro: all of the Kafka topic's, or none, since the spec reader refuses a
// Kafka topic mixing payload formats. Their Payload and Key are the Avsc case.
func specAvro(topic *asyncapi.Topic) []asyncapi.MessageType {
	if topic.Format != asyncapi.AvroFormat {
		return nil
	}
	return topic.MessageTypes
}

// Build parses each value avsc, and the key avsc when the run has one, up
// front so a malformed schema surfaces a typed error before any pipeline work
// (ADR-0007 decision 4). The value avscs are the spec's Avro payloads, one per
// Message type, else the -avro-schema file; the key avsc is the spec's Key
// binding, which the Message types must share, else the -avro-key-schema
// file. The models drive generation; their raw avsc is what the AvroEncoder
// registers. A spec whose payloads are JSON Schema is not consulted:
// -avro-schema governs. Every Planting - the Key at -keyPath, each Topic
// parameter at its location - is walked through every value avsc instead
// (#83, #108).
//
// Several Avro Message types are mixed per record, as JSON mode mixes (#74),
// and register as a union under <topic>-value (#84 decision 4). -keyPath and
// every Topic parameter must hold in each of them. Every rule about the flags
// is judged first, before any avsc is read (checkFlags).
func (Format) Build(flags wire.Flags, topic *asyncapi.Topic, s *synth.Synthesizer) (*wire.Parts, error) {
	if err := checkFlags(flags, topic); err != nil {
		return nil, err
	}
	spec := specAvro(topic)
	values, err := valueAvscs(flags, spec)
	if err != nil {
		return nil, err
	}
	key, err := keyAvsc(flags, spec)
	if err != nil {
		return nil, err
	}
	// Records from -avro-schema are of no Message type in the spec, so they
	// have no Headers to plant into.
	if len(spec) == 0 {
		for _, tp := range topic.Parameters {
			if tp.InHeaders {
				return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: location %s: under -avro-schema the records are of no Message type in the spec, so they have no Headers", tp.Name, tp.Location)}
			}
		}
	}

	gen := avro.NewGenerator(s)
	bound := make([]wire.Bound[encoding], len(values))
	for i, v := range values {
		bound[i] = wire.Bound[encoding]{
			Payload:  &boundGenerator{gen: gen, model: v},
			Walk:     avro.NewWalk(v, key),
			Encoding: encoding{value: v},
		}
		if len(spec) > 0 {
			bound[i].Name, bound[i].Headers = spec[i].Name, spec[i].Headers
		}
	}
	if err := wire.Plant(bound, flags.KeyPath, topic.Parameters); err != nil {
		return nil, err
	}
	var u *union
	if len(values) > 1 {
		if u, err = newUnion(spec); err != nil {
			return nil, err
		}
		for i := range bound {
			bound[i].Encoding.branch = u.plan.Branches[i]
		}
	}

	var keyGen keyplan.Generator
	if key != nil {
		keyGen = keyplan.Reuse(&boundGenerator{gen: gen, model: key}, flags.RecordsPerKey, s)
	}
	parts := &wire.Parts{
		Values:  wire.NewMix(s, bound, keyGen),
		Keyed:   key != nil,
		Encoder: encoderFor(flags, bound, u, key),
	}
	// Records from -avro-schema are of no Message type in the spec, so no
	// Message type's headers apply to them: they are ignored, out loud.
	if len(spec) == 0 && slices.ContainsFunc(topic.MessageTypes, func(mt asyncapi.MessageType) bool { return mt.Headers != nil }) {
		parts.Warnings = append(parts.Warnings, "Warning: headers are ignored under -avro-schema: its records are of no Message type in the spec")
	}
	// A JSON Schema payload's key binding declares a JSON-schema-shaped Key;
	// generating one would silently violate the avsc key contract, so it is
	// ignored, out loud. The spec's Message types play no other part: the
	// avsc governs the Payload.
	if topic.Format == asyncapi.JSONSchemaFormat && slices.ContainsFunc(topic.MessageTypes, func(mt asyncapi.MessageType) bool { return mt.Key != nil }) {
		parts.Warnings = append(parts.Warnings, "Warning: key bindings are ignored under -format avro")
	}
	return parts, nil
}

// encoding is what the AVRO Encoders need of a Message type: its value avsc,
// and its branch of the union when several Message types register as one -
// the record's full name, which names it in a union's generic value; empty
// otherwise.
type encoding struct {
	value  *avro.Schema
	branch string
}

// valueAvscs parses the Payload's avscs, one per Message type: the spec's,
// else -avro-schema's alone.
func valueAvscs(flags wire.Flags, spec []asyncapi.MessageType) ([]*avro.Schema, error) {
	if len(spec) == 0 {
		value, err := loadAvsc(flags.AvroSchema)
		if err != nil {
			return nil, &wire.Error{Flag: "avro-schema", Err: err}
		}
		return []*avro.Schema{value}, nil
	}
	values := make([]*avro.Schema, len(spec))
	for i, mt := range spec {
		value, err := avro.Parse(mt.Payload.(asyncapi.Avsc))
		if err != nil {
			return nil, &wire.Error{Flag: "topic", Detail: "payload of " + mt.Name, Err: err}
		}
		values[i] = value
	}
	return values, nil
}

// keyAvsc parses the Key's avsc: the Key binding the spec's Message types
// share, else -avro-key-schema's; nil when the run has neither. A Key
// identifies one Entity across the Message types, so they must declare the
// same Key binding, or none (#34 decision 3).
func keyAvsc(flags wire.Flags, spec []asyncapi.MessageType) (*avro.Schema, error) {
	if len(spec) > 0 {
		shared, err := wire.SharedKeyBinding(spec)
		if err != nil {
			return nil, err
		}
		if shared != nil {
			key, err := avro.Parse(shared.(asyncapi.Avsc))
			if err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: "bindings.kafka.key of " + spec[0].Name, Err: err}
			}
			return key, nil
		}
	}
	if flags.AvroKeySchema == "" {
		return nil, nil
	}
	key, err := loadAvsc(flags.AvroKeySchema)
	if err != nil {
		return nil, &wire.Error{Flag: "avro-key-schema", Err: err}
	}
	return key, nil
}

// union is how several Avro Message types register and encode: the plan of
// subjects, and the union avsc every record is encoded against.
type union struct {
	plan   *unionPlan
	schema *avro.Schema
}

// newUnion composes the spec's Avro Message types into a union.
func newUnion(spec []asyncapi.MessageType) (*union, error) {
	members := make([]unionMember, len(spec))
	for i, mt := range spec {
		members[i] = unionMember{name: mt.Name, avsc: mt.Payload.(asyncapi.Avsc)}
	}
	plan, err := planUnion(members)
	if err != nil {
		return nil, &wire.Error{Flag: "topic", Err: err}
	}
	schema, err := avro.Parse(plan.Union)
	if err != nil {
		return nil, &wire.Error{Flag: "topic", Detail: "the union of the Avro Message types", Err: err}
	}
	return &union{plan: plan, schema: schema}, nil
}

// encoderFor picks the Encoder for the run's mode: Dry run renders from the
// local avscs and never opens a registry connection (ADR-0007 decision 7);
// produce registers the value avsc under <topic>-value, or the union of
// several (#84 decision 4), and the key avsc under <topic>-key, then frames
// records with the assigned IDs.
func encoderFor(flags wire.Flags, bound []wire.Bound[encoding], u *union, key *avro.Schema) func(context.Context) (pipeline.Encoder, error) {
	if flags.DryRun {
		return func(context.Context) (pipeline.Encoder, error) {
			return &AvroDisplayEncoder{types: bound, key: key}, nil
		}
	}
	return func(ctx context.Context) (pipeline.Encoder, error) {
		if u != nil {
			return NewAvroUnionEncoder(ctx, flags.RegistryURL, flags.Topic, u.schema, u.plan, bound, key)
		}
		return NewAvroEncoder(ctx, flags.RegistryURL, flags.Topic, bound[0].Encoding.value, key)
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
