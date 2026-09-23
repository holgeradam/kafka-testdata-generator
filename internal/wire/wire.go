// Package wire is the Wire format seam: one adapter per format owns the rules
// about its flags, how a run generates its Payload and Key, and which Encoder
// turns them into bytes, so neither the Run plan nor the Pipeline branches on
// the format (issue #29).
// The adapters live in internal/wire/jsonwire and internal/wire/avrowire; this
// package holds only what they share.
package wire

import (
	"context"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// Format is one Wire format adapter.
type Format interface {
	// Name is the -format value that selects the adapter.
	Name() string
	// Check applies the format's rules about its flags. It runs before any
	// file is read, so only the flag fields of opts are set.
	Check(opts Options) error
	// Build reads the format's own files and wires the run from them, with
	// the spec's fields of opts set too. It performs no network I/O: the
	// Encoder it returns connects when called.
	Build(opts Options) (*Parts, error)
}

// Options is what a run hands its Wire format: the relevant flags, then, from
// Build on, the Synthesizer shared by Payload and Key and what the AsyncAPI
// spec declares for the Kafka topic.
type Options struct {
	// DryRun is a property of the run; each format decides what it means.
	DryRun bool
	// Topic is the Kafka topic produced to.
	Topic string
	// KeyPath is -keyPath; empty when the Key is not planted.
	KeyPath string
	// RegistryURL, AvroSchema and AvroKeySchema are the AVRO flags.
	RegistryURL   string
	AvroSchema    string
	AvroKeySchema string

	// Synth is the run's one Synthesizer (ADR-0008 decision 4).
	Synth *synth.Synthesizer
	// Schema is the Kafka topic's Message schema, KeyBinding its
	// bindings.kafka.key (nil when absent), and ResolveRef resolves the $refs
	// both may hold.
	Schema     map[string]any
	KeyBinding map[string]any
	ResolveRef generator.RefResolver
}

// Parts is a run as its Wire format wires it.
type Parts struct {
	// Values generates each Payload.
	Values pipeline.ValueGenerator
	// KeyGen generates each Key; nil means records carry a null Key.
	KeyGen keyplan.Generator
	// Checker validates -keyPath against the Payload schema; nil when the run
	// has no -keyPath.
	Checker keyplan.Checker
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
