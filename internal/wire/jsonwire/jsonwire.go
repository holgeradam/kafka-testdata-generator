// Package jsonwire is the JSON Wire format adapter: the Message schema
// generates the Payload, bindings.kafka.key generates the Key, and both are
// encoded as JSON by JsonEncoder.
package jsonwire

import (
	"context"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
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

// Build wires the JSON Schema generator for Payload and Key. The Key comes
// from the key binding, which -keyPath therefore requires, and a -keyPath is
// checked against the Message schema.
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	if opts.KeyPath != "" && opts.KeyBinding == nil {
		return nil, &wire.Error{Flag: "keyPath", Detail: "-keyPath requires a key schema: declare message.bindings.kafka.key in the spec, so there is a Key to plant"}
	}

	gen := generator.New(opts.Synth)
	gen.SetRefResolver(opts.ResolveRef)

	parts := &wire.Parts{
		Values: &boundGenerator{gen: gen, schema: opts.Schema},
		Encoder: func(context.Context) (pipeline.Encoder, error) {
			return JsonEncoder{}, nil
		},
	}
	if opts.KeyBinding != nil {
		parts.KeyGen = &boundGenerator{gen: gen, schema: opts.KeyBinding}
		if opts.KeyPath != "" {
			parts.Checker = generator.NewKeyChecker(opts.Schema, opts.KeyBinding, opts.ResolveRef)
		}
	}
	return parts, nil
}

// boundGenerator binds a JSON Schema to the generator: the Message schema for
// the Payload, the key binding for the Key.
type boundGenerator struct {
	gen    *generator.Generator
	schema map[string]any
}

func (g *boundGenerator) Value() (any, error) { return g.gen.Value(g.schema) }
