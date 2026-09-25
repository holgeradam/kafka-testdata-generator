// Package avrowire is the AVRO Wire format adapter (ADR-0007): the value avsc
// generates the Payload, the key avsc the Key, each from the spec or a file, and produce frames both
// Confluent-style under registry-assigned IDs (AvroEncoder) while Dry run
// renders the Avro JSON encoding without a registry (AvroDisplayEncoder).
package avrowire

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the AVRO adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects AVRO.
func (Format) Name() string { return "avro" }

// Check applies the AVRO flag surface (ADR-0007 decision 6, amended by
// ADR-0009 and ADR-0011), judged against the spec. The Payload's avsc comes
// from the spec's Avro payloads or from -avro-schema, never both; the Key's
// from the spec's Avro Key binding or from -avro-key-schema, never both, and
// -keyPath needs one. Producing needs a registry. The Kafka message binding's
// registry fields must be ones the Confluent framing honours (#84 decision
// 8).
func (Format) Check(opts wire.Options) error {
	spec := specAvro(opts.MessageTypes)
	switch {
	case len(spec) > 0 && opts.AvroSchema != "":
		return &wire.Error{Flag: "avro-schema", Detail: fmt.Sprintf("the spec declares the Payload's avsc (payload of %s is Avro) and -avro-schema gives another; drop -avro-schema, so the run has one source of truth", spec[0].Name)}
	case len(spec) == 0 && opts.AvroSchema == "":
		return &wire.Error{Flag: "avro-schema", Detail: "-avro-schema is required with -format avro when the spec's payloads are JSON Schema"}
	}
	keyed := slices.IndexFunc(spec, func(mt asyncapi.MessageType) bool { return mt.KeyAvsc != nil })
	if keyed >= 0 && opts.AvroKeySchema != "" {
		return &wire.Error{Flag: "avro-key-schema", Detail: fmt.Sprintf("the spec declares the Key's avsc (bindings.kafka.key of %s) and -avro-key-schema gives another; drop -avro-key-schema, so the run has one source of truth", spec[keyed].Name)}
	}
	if opts.KeyPath != "" && opts.AvroKeySchema == "" && keyed < 0 {
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

// Build parses each value avsc, and the key avsc when the run has one, up
// front so a malformed schema surfaces a typed error before any pipeline work
// (ADR-0007 decision 4). The value avscs are the spec's Avro payloads, one per
// Message type, else the -avro-schema file; the key avsc is the spec's Key
// binding, which the Message types must share, else the -avro-key-schema
// file. The models drive generation; their raw avsc is what the AvroEncoder
// registers. A spec whose payloads are JSON Schema is not consulted:
// -avro-schema governs. A Topic parameter's payload location is walked
// through every value avsc instead, and its value planted into every Payload
// (#83).
//
// Several Avro Message types are mixed per record, as JSON mode mixes (#74),
// and register as a union under <topic>-value (#84 decision 4). -keyPath and
// every Topic parameter must hold in each of them.
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	spec := specAvro(opts.MessageTypes)
	values, names, err := valueAvscs(opts, spec)
	if err != nil {
		return nil, err
	}
	key, err := keyAvsc(opts, spec)
	if err != nil {
		return nil, err
	}
	plants, err := topicParameters(values, names, opts)
	if err != nil {
		return nil, err
	}
	var u *union
	if len(values) > 1 {
		if u, err = newUnion(spec); err != nil {
			return nil, err
		}
	}

	gen := avro.NewGenerator(opts.Synth)
	payloads := make([]*boundGenerator, len(values))
	for i, v := range values {
		payloads[i] = &boundGenerator{gen: gen, model: v, plants: plants[i]}
	}
	parts := &wire.Parts{
		Values:  &mix{synth: opts.Synth, types: payloads},
		Encoder: encoderFor(opts, values, u, key),
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
			checker := make(wire.EveryType, len(values))
			for i, v := range values {
				checker[i] = wire.NamedChecker{Name: names[i], Checker: avro.NewKeyChecker(v, key)}
			}
			parts.Checker = checker
		}
	}
	return parts, nil
}

// valueAvscs parses the Payload's avscs, one per Message type, with the
// Message types' names: the spec's, else -avro-schema's alone.
func valueAvscs(opts wire.Options, spec []asyncapi.MessageType) ([]*avro.Schema, []string, error) {
	if len(spec) == 0 {
		value, err := loadAvsc(opts.AvroSchema)
		if err != nil {
			return nil, nil, &wire.Error{Flag: "avro-schema", Err: err}
		}
		return []*avro.Schema{value}, []string{""}, nil
	}
	values := make([]*avro.Schema, len(spec))
	names := make([]string, len(spec))
	for i, mt := range spec {
		value, err := avro.Parse(mt.Avsc)
		if err != nil {
			return nil, nil, &wire.Error{Flag: "topic", Detail: "payload of " + mt.Name, Err: err}
		}
		values[i], names[i] = value, mt.Name
	}
	return values, names, nil
}

// keyAvsc parses the Key's avsc: the Key binding the spec's Message types
// share, else -avro-key-schema's; nil when the run has neither. A Key
// identifies one Entity across the Message types, so they must declare the
// same Key binding, or none (#34 decision 3).
func keyAvsc(opts wire.Options, spec []asyncapi.MessageType) (*avro.Schema, error) {
	if len(spec) > 0 {
		if err := sameKey(spec); err != nil {
			return nil, err
		}
		if spec[0].KeyAvsc != nil {
			key, err := avro.Parse(spec[0].KeyAvsc)
			if err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: "bindings.kafka.key of " + spec[0].Name, Err: err}
			}
			return key, nil
		}
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

// sameKey refuses Message types that declare different Key bindings, naming
// them grouped by binding.
func sameKey(spec []asyncapi.MessageType) error {
	var keys [][]byte
	var groups [][]string
	for _, mt := range spec {
		i := slices.IndexFunc(keys, func(k []byte) bool { return bytes.Equal(k, mt.KeyAvsc) })
		if i < 0 {
			keys = append(keys, mt.KeyAvsc)
			groups = append(groups, nil)
			i = len(keys) - 1
		}
		groups[i] = append(groups[i], mt.Name)
	}
	if len(keys) == 1 {
		return nil
	}
	described := make([]string, len(groups))
	for i, g := range groups {
		described[i] = strings.Join(g, ", ")
		if keys[i] == nil {
			described[i] += " (none)"
		}
	}
	return &wire.Error{Flag: "topic", Detail: fmt.Sprintf("the Message types of the Kafka topic declare different Key bindings (%s); a Key identifies one Entity across them, so they must declare the same one, or none", strings.Join(described, " vs "))}
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
		members[i] = unionMember{name: mt.Name, avsc: mt.Avsc}
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
func encoderFor(opts wire.Options, values []*avro.Schema, u *union, key *avro.Schema) func(context.Context) (pipeline.Encoder, error) {
	if opts.DryRun {
		return func(context.Context) (pipeline.Encoder, error) {
			return newAvroDisplayEncoder(values, key), nil
		}
	}
	return func(ctx context.Context) (pipeline.Encoder, error) {
		if u != nil {
			return NewAvroUnionEncoder(ctx, opts.RegistryURL, opts.Topic, u.schema, u.plan, key)
		}
		return NewAvroEncoder(ctx, opts.RegistryURL, opts.Topic, values[0], key)
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

// topicParameters checks each Topic parameter's payload location against
// every value avsc, before any record exists: the location must be
// guaranteed, as a -keyPath must under AVRO, the type there must hold the
// value, and no two plantings, the Key's included, may land in the same
// field. It returns what to plant into each Message type's Payloads.
func topicParameters(values []*avro.Schema, names []string, opts wire.Options) ([]wire.Plants, error) {
	plants := make([]wire.Plants, len(values))
	for _, tp := range opts.TopicParameters {
		if tp.Pointer == nil {
			continue
		}
		for i, value := range values {
			inType := ""
			if len(values) > 1 {
				inType = "in Message type " + names[i] + ": "
			}
			steps, at, err := avro.Locate(value, tp.Pointer)
			if err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: location %s: %s%v", tp.Name, tp.Location, inType, err)}
			}
			if err := avro.HoldsString(at, tp.Value, tp.Location); err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: %s%v", tp.Name, inType, err)}
			}
			if plants[i], err = plants[i].Add(tp, steps, opts.KeyPath); err != nil {
				return nil, err
			}
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

// mix generates each Payload from one Message type picked from the seeded
// stream, as JSON mode's mix does. A single Message type draws no pick, so
// its output is exactly what generating from it alone gives.
type mix struct {
	synth *synth.Synthesizer
	types []*boundGenerator
}

func (m *mix) Generate() (pipeline.Generated, error) {
	i := 0
	if len(m.types) > 1 {
		i = m.synth.Pick(len(m.types))
	}
	v, err := m.types[i].Value()
	return pipeline.Generated{Type: i, Payload: v}, err
}
