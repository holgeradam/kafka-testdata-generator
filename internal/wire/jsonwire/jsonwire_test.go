package jsonwire

import (
	"context"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

var _ wire.Format = Format{}

func options() wire.Options {
	return wire.Options{
		Synth: synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		Schema: map[string]any{
			"type":       "object",
			"required":   []any{"orderId"},
			"properties": map[string]any{"orderId": map[string]any{"type": "string"}},
		},
	}
}

// TestBuildGeneratesFromMessageSchema proves the Payload honours the Message
// schema the Pipeline passes in.
func TestBuildGeneratesFromMessageSchema(t *testing.T) {
	opts := options()
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v, err := parts.Values.Value(opts.Schema)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if _, ok := v.(map[string]any)["orderId"].(string); !ok {
		t.Errorf("payload = %#v, want an object with a string orderId", v)
	}
}

// TestBuildKey proves the Key comes from the key binding: none means a null
// Key, and a Checker exists only when -keyPath asks for planting.
func TestBuildKey(t *testing.T) {
	opts := options()
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.KeyGen != nil || parts.Checker != nil {
		t.Error("no key binding: want a null Key and no Checker")
	}

	opts.KeyBinding = map[string]any{"type": "string"}
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.KeyGen == nil {
		t.Fatal("a key binding must produce a Key generator")
	}
	k, err := parts.KeyGen.Value()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if _, ok := k.(string); !ok {
		t.Errorf("key = %T, want a string from the binding", k)
	}
	if parts.Checker != nil {
		t.Error("no -keyPath: want no Checker")
	}

	opts.KeyPath = "orderId"
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.Checker == nil {
		t.Error("-keyPath with a key binding must produce a Checker")
	}
}

// TestBuildEncoderIgnoresDryRun proves JSON encodes the same way in Dry run and
// produce.
func TestBuildEncoderIgnoresDryRun(t *testing.T) {
	for _, dry := range []bool{false, true} {
		opts := options()
		opts.DryRun = dry
		parts, err := Format{}.Build(opts)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		enc, err := parts.Encoder(context.Background())
		if err != nil {
			t.Fatalf("Encoder: %v", err)
		}
		if _, ok := enc.(JsonEncoder); !ok {
			t.Errorf("dry run %v: encoder = %T, want JsonEncoder", dry, enc)
		}
	}
}
