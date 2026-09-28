// Package jsonwire is the JSON Wire format adapter: the Kafka topic's Message
// types generate the Payloads, mixed per record, bindings.kafka.key generates
// the Key, and both are encoded as JSON by JsonEncoder.
package jsonwire

import (
	"context"
	"fmt"

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

// Build wires the JSON Schema generator for Payload and Key. Each Payload is
// of one of the Kafka topic's Message types, picked per record from the
// seeded stream (#74). The Key comes from the Key binding, which every Message
// type must declare identically, since a Key identifies one Entity across them
// (#34 decision 3), and which -keyPath and Key reuse therefore require. Every
// Planting - the Key at -keyPath, each Topic parameter at its location - is
// checked against every Message type before any record exists (#83, #108).
//
// The AVRO flags are refused: under JSON the Message schema governs and
// nothing is registered. -avro-schema never reaches here, since it makes the
// Wire format avro. JSON reads no files of its own, so its rules come in the
// order their data is at hand.
func (Format) Build(flags wire.Flags, topic *asyncapi.Topic, s *synth.Synthesizer) (*wire.Parts, error) {
	if flags.AvroKeySchema != "" {
		return nil, &wire.Error{Usage: true, Flag: "avro-key-schema", Detail: "-avro-key-schema needs the avro Wire format: pass -avro-schema, or use a spec with Avro payloads"}
	}
	if flags.RegistryURL != "" {
		return nil, &wire.Error{Usage: true, Flag: "registry", Detail: "-registry is only valid with the avro Wire format"}
	}
	types := topic.MessageTypes
	if len(types) == 0 {
		return nil, &wire.Error{Flag: "topic", Detail: "the spec declares no Message type for the Kafka topic"}
	}
	// The Run plan picks JSON only for a Kafka topic whose Message schemas
	// are JSON Schema, so each Payload and the Key are the JSONSchema case.
	if topic.Format != asyncapi.JSONSchemaFormat {
		return nil, &wire.Error{Flag: "topic", Detail: fmt.Sprintf("the Kafka topic's payloads are %s, which the json Wire format does not generate", topic.Format)}
	}
	shared, err := wire.SharedKeyBinding(types)
	if err != nil {
		return nil, err
	}
	keyBinding, _ := shared.(asyncapi.JSONSchema)
	if flags.KeyPath != "" && keyBinding == nil {
		return nil, &wire.Error{Usage: true, Flag: "keyPath", Detail: "-keyPath requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to plant"}
	}
	// With -records-per-key above 1 Keys identify Entities that recur across
	// records, which needs a Key schema to generate them from (#75).
	if flags.RecordsPerKey > 1 && keyBinding == nil {
		return nil, &wire.Error{Usage: true, Flag: "records-per-key", Detail: "-records-per-key above 1 requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to reuse"}
	}

	gen := generator.New(s)
	// JSON encodes each Message type alike, from the ordered values its
	// generator emits (#112), so a bound type carries no encoding data.
	bound := make([]wire.Bound[struct{}], len(types))
	for i, mt := range types {
		payload := mt.Payload.(asyncapi.JSONSchema)
		bound[i] = wire.Bound[struct{}]{
			Name:    mt.Name,
			Payload: &boundGenerator{gen: gen, schema: payload},
			Walk:    generator.NewWalk(payload, keyBinding),
			Headers: mt.Headers,
		}
	}
	if err := wire.Plant(bound, flags.KeyPath, topic.Parameters); err != nil {
		return nil, err
	}
	var key keyplan.Generator
	if keyBinding != nil {
		key = keyplan.Reuse(&boundGenerator{gen: gen, schema: keyBinding}, flags.RecordsPerKey, s)
	}
	return &wire.Parts{
		Values: wire.NewMix(s, bound, key),
		Keyed:  keyBinding != nil,
		Encoder: func(context.Context) (pipeline.Encoder, error) {
			return JsonEncoder{}, nil
		},
	}, nil
}

// boundGenerator binds a JSON Schema to the generator: a Message type's
// Payload schema, or the Key binding.
type boundGenerator struct {
	gen    *generator.Generator
	schema map[string]any
}

func (g *boundGenerator) Value() (any, error) { return g.gen.Value(g.schema) }
