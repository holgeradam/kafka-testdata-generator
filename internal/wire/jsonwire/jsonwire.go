// Package jsonwire is the JSON Wire format adapter: the Kafka topic's Message
// types generate the Payloads, mixed per record, bindings.kafka.key generates
// the Key, and both are encoded as JSON by JsonEncoder.
package jsonwire

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the JSON adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects JSON.
func (Format) Name() string { return "json" }

// Check rejects the AVRO flags: under JSON the Message schema governs and
// nothing is registered.
func (Format) Check(opts wire.Options) error {
	if opts.AvroSchema != "" || opts.AvroKeySchema != "" {
		return &wire.Error{Flag: "avro-schema", Detail: "-avro-schema and -avro-key-schema are only valid with -format avro"}
	}
	if opts.RegistryURL != "" {
		return &wire.Error{Flag: "registry", Detail: "-registry is only valid with -format avro"}
	}
	return nil
}

// Build wires the JSON Schema generator for Payload and Key. Each Payload is
// of one of the Kafka topic's Message types, picked per record from the
// seeded stream (#74). The Key comes from the Key binding, which every Message
// type must declare identically, since a Key identifies one Entity across them
// (#34 decision 3), and which -keyPath therefore requires. A -keyPath is checked
// against every Message type's Payload schema, and so is each Topic
// parameter's location, whose value is planted into every Payload (#83).
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	types := opts.MessageTypes
	if len(types) == 0 {
		return nil, &wire.Error{Flag: "topic", Detail: "the spec declares no Message type for the Kafka topic"}
	}
	keyBinding, err := sharedKeyBinding(types)
	if err != nil {
		return nil, err
	}
	if opts.KeyPath != "" && keyBinding == nil {
		return nil, &wire.Error{Flag: "keyPath", Detail: "-keyPath requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to plant"}
	}

	plants, err := topicParameters(types, opts)
	if err != nil {
		return nil, err
	}

	gen := generator.New(opts.Synth)
	payloads := make([]*boundGenerator, len(types))
	for i, mt := range types {
		payloads[i] = &boundGenerator{gen: gen, schema: mt.Payload, plants: plants[i]}
	}

	parts := &wire.Parts{
		Values: &mix{synth: opts.Synth, types: payloads},
		Encoder: func(context.Context) (pipeline.Encoder, error) {
			return JsonEncoder{}, nil
		},
	}
	if keyBinding != nil {
		parts.KeyGen = &boundGenerator{gen: gen, schema: keyBinding}
		if opts.KeyPath != "" {
			parts.Checker = everyType(types, keyBinding)
		}
	}
	return parts, nil
}

// sharedKeyBinding returns the Key binding every Message type declares, nil
// when none does, or an error naming the Message types that disagree.
func sharedKeyBinding(types []asyncapi.MessageType) (map[string]any, error) {
	var groups [][]string
	var bindings []map[string]any
	for _, mt := range types {
		found := false
		for i, b := range bindings {
			if reflect.DeepEqual(b, mt.KeyBinding) {
				groups[i] = append(groups[i], mt.Name)
				found = true
				break
			}
		}
		if !found {
			bindings = append(bindings, mt.KeyBinding)
			groups = append(groups, []string{mt.Name})
		}
	}
	if len(bindings) == 1 {
		return bindings[0], nil
	}
	described := make([]string, len(groups))
	for i, g := range groups {
		described[i] = strings.Join(g, ", ")
		if bindings[i] == nil {
			described[i] += " (none)"
		}
	}
	return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("the Message types of the Kafka topic declare different Key bindings (%s); a Key identifies one Entity across them, so they must declare the same one, or none", strings.Join(described, " vs "))}
}

// mix generates each Payload from one Message type picked from the seeded
// stream. A single Message type draws no pick, so its output is exactly what
// generating from it alone gives.
type mix struct {
	synth *synth.Synthesizer
	types []*boundGenerator
}

func (m *mix) Value() (any, error) {
	if len(m.types) == 1 {
		return m.types[0].Value()
	}
	return m.types[m.synth.Pick(len(m.types))].Value()
}

// everyType checks a key path against each Message type's Payload schema,
// naming the Message type a rejection comes from.
func everyType(types []asyncapi.MessageType, keyBinding map[string]any) keyplan.Checker {
	c := make(checkers, len(types))
	for i, mt := range types {
		c[i] = namedChecker{name: mt.Name, checker: generator.NewKeyChecker(mt.Payload, keyBinding)}
	}
	return c
}

type namedChecker struct {
	name    string
	checker keyplan.Checker
}

type checkers []namedChecker

func (c checkers) Check(path []keyplan.Step) error {
	for _, nc := range c {
		if err := nc.checker.Check(path); err != nil {
			if len(c) == 1 {
				return err
			}
			return fmt.Errorf("in Message type %s: %w", nc.name, err)
		}
	}
	return nil
}

// topicParameters checks each Topic parameter's payload location against
// every Message type, before any record exists: the location must be
// guaranteed, as a -keyPath must, the value must conform to the field's schema
// there, and no two plantings, the Key's included, may land in the same
// field. It returns what to plant into each Message type's Payloads.
func topicParameters(types []asyncapi.MessageType, opts wire.Options) ([]wire.Plants, error) {
	plants := make([]wire.Plants, len(types))
	for _, tp := range opts.TopicParameters {
		if tp.Pointer == nil {
			continue
		}
		for i, mt := range types {
			inType := ""
			if len(types) > 1 {
				inType = "in Message type " + mt.Name + ": "
			}
			steps, field, err := generator.Locate(mt.Payload, tp.Pointer)
			if err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: location %s: %s%v", tp.Name, tp.Location, inType, err)}
			}
			if err := generator.Conforms(field, tp.Value); err != nil {
				return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("Topic parameter %s: value %s does not conform to the Payload field at %s: %s%v", tp.Name, tp.Value, tp.Location, inType, err)}
			}
			if plants[i], err = plants[i].Add(tp, steps, opts.KeyPath); err != nil {
				return nil, err
			}
		}
	}
	return plants, nil
}

// boundGenerator binds a JSON Schema to the generator: a Message type's
// Payload schema, with the Topic parameter values planted into each Payload,
// or the Key binding.
type boundGenerator struct {
	gen    *generator.Generator
	schema map[string]any
	plants wire.Plants
}

func (g *boundGenerator) Value() (any, error) {
	v, err := g.gen.Value(g.schema)
	if err != nil {
		return nil, err
	}
	if err := g.plants.Apply(v); err != nil {
		return nil, err
	}
	return v, nil
}
