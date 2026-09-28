// Package jsonwire is the JSON Wire format adapter: the Kafka topic's Message
// types generate the Payloads, mixed per record, bindings.kafka.key generates
// the Key, and both are encoded as JSON by JsonEncoder.
package jsonwire

import (
	"context"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// Format is the JSON adapter on the wire.Format seam.
type Format struct{}

// Name is the -format value that selects JSON.
func (Format) Name() string { return "json" }

// Check rejects the AVRO flags: under JSON the Message schema governs and
// nothing is registered. -avro-schema never reaches here, since it makes the
// Wire format avro.
func (Format) Check(opts wire.Options) error {
	if opts.AvroKeySchema != "" {
		return &wire.Error{Usage: true, Flag: "avro-key-schema", Detail: "-avro-key-schema needs the avro Wire format: pass -avro-schema, or use a spec with Avro payloads"}
	}
	if opts.RegistryURL != "" {
		return &wire.Error{Usage: true, Flag: "registry", Detail: "-registry is only valid with the avro Wire format"}
	}
	return nil
}

// Build wires the JSON Schema generator for Payload and Key. Each Payload is
// of one of the Kafka topic's Message types, picked per record from the
// seeded stream (#74). The Key comes from the Key binding, which every Message
// type must declare identically, since a Key identifies one Entity across them
// (#34 decision 3), and which -keyPath therefore requires. Every Planting - the
// Key at -keyPath, each Topic parameter at its location - is checked against
// every Message type before any record exists (#83, #108).
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	types := opts.MessageTypes
	if len(types) == 0 {
		return nil, &wire.Error{Flag: "topic", Detail: "the spec declares no Message type for the Kafka topic"}
	}
	keyBinding, err := wire.SharedKeyBinding(types, func(mt asyncapi.MessageType) map[string]any { return mt.KeyBinding })
	if err != nil {
		return nil, err
	}
	if opts.KeyPath != "" && keyBinding == nil {
		return nil, &wire.Error{Usage: true, Flag: "keyPath", Detail: "-keyPath requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to plant"}
	}

	gen := generator.New(opts.Synth)
	bound := make([]wire.Bound[encoding], len(types))
	for i, mt := range types {
		bound[i] = wire.Bound[encoding]{
			Name:     mt.Name,
			Payload:  &boundGenerator{gen: gen, schema: mt.Payload},
			Walk:     generator.NewWalk(mt.Payload, keyBinding),
			Headers:  mt.Headers,
			Encoding: encoding{payload: mt.Payload},
		}
	}
	if err := wire.Plant(bound, opts.KeyPath, opts.TopicParameters); err != nil {
		return nil, err
	}
	var key keyplan.Generator
	if keyBinding != nil {
		key = keyplan.Reuse(&boundGenerator{gen: gen, schema: keyBinding}, opts.RecordsPerKey, opts.Synth)
	}
	return &wire.Parts{
		Values: wire.NewMix(opts.Synth, bound, key),
		Keyed:  keyBinding != nil,
		Encoder: func(context.Context) (pipeline.Encoder, error) {
			return JsonEncoder{types: bound, key: keyBinding}, nil
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
