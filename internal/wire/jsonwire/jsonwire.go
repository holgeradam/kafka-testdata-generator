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

// Build wires the JSON Schema generator for Payload and Key. The Key comes
// from the key binding, and a -keyPath is checked against the Message schema.
func (Format) Build(opts wire.Options) (*wire.Parts, error) {
	gen := generator.New(opts.Synth)
	gen.SetRefResolver(opts.ResolveRef)

	parts := &wire.Parts{
		Values: gen,
		Encoder: func(context.Context) (pipeline.Encoder, error) {
			return JsonEncoder{}, nil
		},
	}
	if opts.KeyBinding != nil {
		parts.KeyGen = &bindingKeyGenerator{gen: gen, schema: opts.KeyBinding}
		if opts.KeyPath != "" {
			parts.Checker = generator.NewKeyChecker(opts.Schema, opts.KeyBinding, opts.ResolveRef)
		}
	}
	return parts, nil
}

// bindingKeyGenerator binds the key binding's schema to the generator, giving
// the Key plan its no-argument Generator.
type bindingKeyGenerator struct {
	gen    *generator.Generator
	schema map[string]any
}

func (g *bindingKeyGenerator) Value() (any, error) { return g.gen.Value(g.schema) }
