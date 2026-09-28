// Package wire is the Wire format seam: one adapter per format owns the rules
// about its flags, how a run generates its Payload and Key, and which Encoder
// turns them into bytes, so neither the Run plan nor the Pipeline branches on
// the format (issue #29).
// The adapters live in internal/wire/jsonwire and internal/wire/avrowire; this
// package holds only what they share.
package wire

import (
	"context"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// Format is one Wire format adapter.
type Format interface {
	// Name is the -format value that selects the adapter.
	Name() string
	// Check applies the format's rules about its flags, judged against what
	// the spec declares. It runs once the spec is read and before the
	// format's own files are, so every field of opts but Synth is set.
	Check(opts Options) error
	// Build reads the format's own files and wires the run from them and
	// the spec. It performs no network I/O: the Encoder it returns connects
	// when called.
	Build(opts Options) (*Parts, error)
}

// Options is what a run hands its Wire format: the relevant flags, what the
// AsyncAPI spec declares for the Kafka topic, and, at Build, the Synthesizer
// shared by Payload and Key.
type Options struct {
	// DryRun is a property of the run; each format decides what it means.
	DryRun bool
	// Topic is the Kafka topic produced to.
	Topic string
	// KeyPath is -keyPath; empty when the Key is not planted.
	KeyPath string
	// RecordsPerKey is -records-per-key: how many records an Entity's Key
	// carries on average (#75).
	RecordsPerKey int
	// RegistryURL, AvroSchema and AvroKeySchema are the AVRO flags.
	RegistryURL   string
	AvroSchema    string
	AvroKeySchema string

	// Synth is the run's one Synthesizer (ADR-0008 decision 4).
	Synth *synth.Synthesizer
	// MessageTypes are the Message types the spec declares for the Kafka
	// topic, each with a self-contained Payload schema and Key binding, all
	// in one payload format. How they are used is the format's business:
	// JSON mixes them, AVRO reads its avsc from them when they are Avro, and
	// follows -avro-schema instead when they are not.
	MessageTypes []asyncapi.MessageType
	// TopicParameters are the values -topic fills a templated address with.
	// Each one with a location is planted into every Payload or Headers, in
	// either format, once Plant has checked that the location is guaranteed
	// and holds the value (#83, #93).
	TopicParameters []asyncapi.TopicParameter
}

// Parts is a run as its Wire format wires it.
type Parts struct {
	// Values generates each message: its Payload, Headers and Key, every
	// Planting in place, with the Message type it is of.
	Values pipeline.PayloadGenerator
	// Keyed says the run has a key schema; without one every record carries
	// a null Key.
	Keyed bool
	// Encoder builds the run's Encoder. It is called after the sink exists, so
	// a run that cannot reach its broker never contacts a registry (ADR-0010).
	Encoder func(ctx context.Context) (pipeline.Encoder, error)
	// Warnings are diagnostics for the caller to print, such as a spec
	// declaration the format ignores.
	Warnings []string
}

// Error reports a run a Wire format rejects. Flag names the option at fault
// (without its dash) when one rule owns the failure, and Err carries the typed
// cause. runplan.Error is this type, so callers see one shape wherever a rule
// lives.
type Error struct {
	Flag   string
	Detail string
	Err    error
	// Usage marks a rule about the flags themselves - a missing, renamed or
	// conflicting flag - for which the flag surface is worth a reminder. A
	// refusal of what the spec or an avsc declares leaves it false: its
	// message already says what is wrong (#101).
	Usage bool
}

func (e *Error) Error() string {
	switch {
	case e.Detail != "" && e.Err != nil:
		return e.Detail + ": " + e.Err.Error()
	case e.Detail != "":
		return e.Detail
	case e.Err != nil:
		return e.Err.Error()
	default:
		return "invalid run"
	}
}

// Unwrap exposes the cause for errors.Is/As.
func (e *Error) Unwrap() error { return e.Err }
