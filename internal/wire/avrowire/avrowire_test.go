package avrowire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

var _ wire.Format = Format{}

func testNow() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

// writeAvsc puts an avsc in a temp file and returns its path.
func writeAvsc(t *testing.T, avsc string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.avsc")
	if err := os.WriteFile(p, []byte(avsc), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

const orderAvsc = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`

func options(t *testing.T) wire.Options {
	return wire.Options{
		Topic:      "orders",
		AvroSchema: writeAvsc(t, orderAvsc),
		Synth:      synth.New(1, testNow()),
		// The Message schema and binding must not reach AVRO generation.
		Schema:     map[string]any{"type": "integer"},
		KeyBinding: map[string]any{"type": "integer"},
	}
}

// TestBuildGeneratesFromValueAvsc proves the Payload follows the value avsc,
// whatever Message schema the Pipeline hands the generator.
func TestBuildGeneratesFromValueAvsc(t *testing.T) {
	parts, err := Format{}.Build(options(t))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v, err := parts.Values.Value(map[string]any{"type": "integer"})
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if _, ok := v.(map[string]any)["id"].(string); !ok {
		t.Errorf("payload = %#v, want an Order record with a string id", v)
	}
}

// TestBuildKey proves the Key comes from the key avsc alone: without one the
// Key is null even when the spec declares a binding, and a Checker exists only
// when -keyPath asks for planting.
func TestBuildKey(t *testing.T) {
	opts := options(t)
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.KeyGen != nil || parts.Checker != nil {
		t.Error("no key avsc: want a null Key and no Checker, whatever the binding says")
	}

	opts.AvroKeySchema = writeAvsc(t, `{"type":"long"}`)
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.KeyGen == nil {
		t.Fatal("a key avsc must produce a Key generator")
	}
	k, err := parts.KeyGen.Value()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if _, ok := k.(int64); !ok {
		t.Errorf("key = %T, want int64 from the long key avsc", k)
	}
	if parts.Checker != nil {
		t.Error("no -keyPath: want no Checker")
	}

	opts.KeyPath = "id"
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.Checker == nil {
		t.Error("-keyPath with a key avsc must produce a Checker")
	}
}

// TestBuildRejectsBadAvsc proves an unreadable or malformed avsc stops the run
// naming the flag that pointed at it, with the typed cause preserved.
func TestBuildRejectsBadAvsc(t *testing.T) {
	bad := writeAvsc(t, `{"type":"nope"}`)
	cases := []struct {
		name, flag string
		mutate     func(*wire.Options)
	}{
		{"malformed value", "avro-schema", func(o *wire.Options) { o.AvroSchema = bad }},
		{"missing value", "avro-schema", func(o *wire.Options) { o.AvroSchema = filepath.Join(t.TempDir(), "absent.avsc") }},
		{"malformed key", "avro-key-schema", func(o *wire.Options) { o.AvroKeySchema = bad }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := options(t)
			tc.mutate(&opts)
			_, err := Format{}.Build(opts)
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != tc.flag {
				t.Fatalf("err = %v, want a *wire.Error on -%s", err, tc.flag)
			}
			var pe *avro.ParseError
			if tc.name != "missing value" && !errors.As(err, &pe) {
				t.Errorf("err = %v, want the *avro.ParseError preserved", err)
			}
		})
	}
}

// TestBuildEncoderPerMode proves Dry run gets the display encoder and never a
// registry, and produce gets the framing encoder, which registers when called.
func TestBuildEncoderPerMode(t *testing.T) {
	opts := options(t)
	opts.DryRun = true
	opts.RegistryURL = "http://127.0.0.1:1"
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	enc, err := parts.Encoder(context.Background())
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	if _, ok := enc.(*AvroDisplayEncoder); !ok {
		t.Errorf("dry-run encoder = %T, want *AvroDisplayEncoder", enc)
	}

	srv, calls := fakeRegistry(t, map[string]int{"orders-value": 7})
	opts.DryRun = false
	opts.RegistryURL = srv.URL
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatal("Build must not contact the registry; the Encoder does when called")
	}
	enc, err = parts.Encoder(context.Background())
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	if _, ok := enc.(*AvroEncoder); !ok {
		t.Errorf("produce encoder = %T, want *AvroEncoder", enc)
	}
	if len(*calls) != 1 || (*calls)[0].subject != "orders-value" {
		t.Errorf("registry calls = %+v, want one registration under orders-value", *calls)
	}
}
