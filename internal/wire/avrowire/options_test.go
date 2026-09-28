package avrowire

import (
	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// buildOptions gathers what a test hands Build in one place: the flags, what the
// spec declares for the Kafka topic, and the run's Synthesizer.
type buildOptions struct {
	DryRun          bool
	Topic           string
	KeyPath         string
	RecordsPerKey   int
	RegistryURL     string
	AvroSchema      string
	AvroKeySchema   string
	Synth           *synth.Synthesizer
	MessageTypes    []asyncapi.MessageType
	TopicParameters []asyncapi.TopicParameter
}

// build runs the format's Build on o, as a run does.
func build(o buildOptions) (*wire.Parts, error) {
	flags := wire.Flags{DryRun: o.DryRun, Topic: o.Topic, KeyPath: o.KeyPath, RecordsPerKey: o.RecordsPerKey, RegistryURL: o.RegistryURL, AvroSchema: o.AvroSchema, AvroKeySchema: o.AvroKeySchema}
	return Format{}.Build(flags, &asyncapi.Topic{Format: formatOf(o.MessageTypes), MessageTypes: o.MessageTypes, Parameters: o.TopicParameters}, o.Synth)
}

// formatOf is the payload format of the Message types, as the spec reader
// states it for a Kafka topic: every one declares the same.
func formatOf(types []asyncapi.MessageType) asyncapi.PayloadFormat {
	if len(types) == 0 || types[0].Payload == nil {
		return asyncapi.JSONSchemaFormat
	}
	return types[0].Payload.Format()
}
