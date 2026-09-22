package runplan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
)

// write puts content in a temp file and returns its path.
func write(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

const plainSpec = `
asyncapi: '2.6.0'
info: {title: Plain, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required: [orderId]
          properties:
            orderId: {type: string}
`

const bindingSpec = `
asyncapi: '2.6.0'
info: {title: Bound, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key: {type: string}
        payload:
          type: object
          required: [orderId]
          properties:
            orderId: {type: string}
            nickname: {type: string}
`

const valueAvsc = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
const keyAvsc = `{"type":"string"}`

// TestPlanAccepts covers the runs that are valid, asserting what the plan
// carries rather than what the binary prints.
func TestPlanAccepts(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	bound := write(t, "bound.yaml", bindingSpec)
	value := write(t, "value.avsc", valueAvsc)
	key := write(t, "key.avsc", keyAvsc)

	cases := []struct {
		name  string
		args  []string
		check func(*testing.T, *Run)
	}{
		{
			name: "json dry run",
			args: []string{"-spec", spec, "-channel", "orders", "-dry-run"},
			check: func(t *testing.T, r *Run) {
				if !r.DryRun || r.Format != "json" || r.Topic != "orders" {
					t.Errorf("got %+v, want a json dry run on orders", r)
				}
				if r.Config.Count != 10 {
					t.Errorf("Count = %d, want the default 10", r.Config.Count)
				}
				if r.Config.KeyPlan != nil {
					t.Error("no key schema: KeyPlan must be nil, so records carry a null Key")
				}
				if r.Config.Schema == nil || r.Config.Generator == nil {
					t.Error("plan must carry the Message schema and a generator")
				}
			},
		},
		{
			name: "key plan from binding",
			args: []string{"-spec", bound, "-channel", "orders", "-dry-run", "-keyPath", "orderId"},
			check: func(t *testing.T, r *Run) {
				if r.Config.KeyPlan == nil {
					t.Fatal("a key binding and -keyPath must produce a KeyPlan")
				}
				payload := map[string]any{"orderId": "before"}
				k, err := r.Config.KeyPlan.Apply(payload)
				if err != nil {
					t.Fatalf("Apply: %v", err)
				}
				if payload["orderId"] != k {
					t.Errorf("planted %v, key %v; want the Key planted at -keyPath", payload["orderId"], k)
				}
			},
		},
		{
			name: "binding without a path still keys",
			args: []string{"-spec", bound, "-channel", "orders", "-dry-run"},
			check: func(t *testing.T, r *Run) {
				if r.Config.KeyPlan == nil {
					t.Error("a key binding alone must still produce a KeyPlan")
				}
			},
		},
		{
			name: "avro dry run",
			args: []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value},
			check: func(t *testing.T, r *Run) {
				if r.Format != "avro" {
					t.Errorf("Format = %q, want avro", r.Format)
				}
				if r.Config.KeyPlan != nil {
					t.Error("no key avsc: KeyPlan must be nil")
				}
			},
		},
		{
			name: "avro key avsc",
			args: []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-avro-key-schema", key},
			check: func(t *testing.T, r *Run) {
				if r.Config.KeyPlan == nil {
					t.Error("a key avsc must produce a KeyPlan")
				}
			},
		},
		{
			name: "acks accepts any case",
			args: []string{"-spec", spec, "-channel", "orders", "-dry-run", "-acks", "aLL"},
			check: func(t *testing.T, r *Run) {
				if r.Acks != producer.AcksAll {
					t.Errorf("Acks = %v, want all", r.Acks)
				}
			},
		},
		{
			name: "explicit json format matches the default",
			args: []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "json"},
			check: func(t *testing.T, r *Run) {
				if r.Format != "json" {
					t.Errorf("Format = %q, want json", r.Format)
				}
			},
		},
		{
			name: "kafka options",
			args: []string{"-spec", spec, "-channel", "orders", "-broker", "kafka:9092", "-acks", "all", "-count", "3"},
			check: func(t *testing.T, r *Run) {
				if r.DryRun {
					t.Error("DryRun must be false without -dry-run")
				}
				if r.Broker != "kafka:9092" || r.Acks != producer.AcksAll || r.Config.Count != 3 {
					t.Errorf("got broker %q acks %v count %d", r.Broker, r.Acks, r.Config.Count)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := Plan(c.args)
			if err != nil {
				t.Fatalf("Plan(%v) = %v, want a plan", c.args, err)
			}
			c.check(t, r)
		})
	}
}

// TestPlanWarnings proves the diagnostics a run carries are data on the plan,
// not writes to stderr during planning.
func TestPlanWarnings(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	bound := write(t, "bound.yaml", bindingSpec)
	value := write(t, "value.avsc", valueAvsc)

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"dry run disregards kafka options", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-broker", "other:9092"}, "dry-run mode disregards Kafka options"},
		{"dry run disregards acks", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-acks", "all"}, "dry-run mode disregards Kafka options"},
		{"binding ignored under avro", []string{"-spec", bound, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value}, "key bindings are ignored under -format avro"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := Plan(c.args)
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			if !slicesContain(r.Warnings, c.want) {
				t.Errorf("Warnings = %q, want one mentioning %q", r.Warnings, c.want)
			}
		})
	}

	r, err := Plan([]string{"-spec", spec, "-channel", "orders", "-dry-run"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(r.Warnings) != 0 {
		t.Errorf("a plain dry run must warn about nothing, got %q", r.Warnings)
	}
}

func slicesContain(all []string, want string) bool {
	for _, s := range all {
		if strings.Contains(s, want) {
			return true
		}
	}
	return false
}

// TestPlanRejects is the rule table: one row per rejected run, asserting the
// flag at fault and, where one exists, the typed cause. No process is spawned
// and nothing is dialed.
func TestPlanRejects(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	bound := write(t, "bound.yaml", bindingSpec)
	value := write(t, "value.avsc", valueAvsc)
	key := write(t, "key.avsc", keyAvsc)
	broken := write(t, "broken.avsc", `{"type":"record","name":"X","fields":[{"name":"n","type":"nope"}]}`)

	cases := []struct {
		name     string
		args     []string
		wantFlag string
		wantText string
		wantAs   any
	}{
		{"no spec", []string{"-channel", "orders"}, "spec", "-spec is required", nil},
		{"no channel", []string{"-spec", spec}, "channel", "-channel is required", nil},
		{"renamed key flag", []string{"-spec", spec, "-channel", "orders", "-key", "orderId"}, "key", "-key was renamed to -keyPath", nil},
		{"avro without value avsc", []string{"-spec", spec, "-channel", "orders", "-format", "avro"}, "avro-schema", "-avro-schema is required with -format avro", nil},
		{"avro key path without key avsc", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-keyPath", "id"}, "keyPath", "-keyPath requires -avro-key-schema", nil},
		{"avro produce without registry", []string{"-spec", spec, "-channel", "orders", "-format", "avro", "-avro-schema", value}, "registry", "-registry is required", nil},
		{"avro flags under json", []string{"-spec", spec, "-channel", "orders", "-avro-schema", value}, "avro-schema", "only valid with -format avro", nil},
		{"registry under json", []string{"-spec", spec, "-channel", "orders", "-registry", "http://localhost:8081"}, "registry", "-registry is only valid with -format avro", nil},
		{"key path without key schema", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-keyPath", "orderId"}, "keyPath", "-keyPath requires a key schema", nil},
		{"spec file missing", []string{"-spec", filepath.Join(t.TempDir(), "gone.yaml"), "-channel", "orders"}, "spec", "", nil},
		{"channel missing from spec", []string{"-spec", spec, "-channel", "nope", "-dry-run"}, "channel", "", nil},
		{"malformed avsc", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", broken}, "avro-schema", "", new(*avro.ParseError)},
		{"key path not guaranteed", []string{"-spec", bound, "-channel", "orders", "-dry-run", "-keyPath", "nickname"}, "keyPath", "not required", new(*keyplan.PathError)},
		{"unknown flag", []string{"-spec", spec, "-channel", "orders", "-nope"}, "", "not defined", nil},
		{"invalid format", []string{"-spec", spec, "-channel", "orders", "-format", "xml"}, "", "invalid -format", nil},
		{"invalid acks", []string{"-spec", spec, "-channel", "orders", "-acks", "two"}, "", "invalid -acks", nil},
		{"invalid now", []string{"-spec", spec, "-channel", "orders", "-now", "yesterday"}, "", "invalid -now", nil},
		{"avro key path not guaranteed", []string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-avro-key-schema", key, "-keyPath", "missing"}, "keyPath", "no field", new(*keyplan.PathError)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := Plan(c.args)
			if err == nil {
				t.Fatalf("Plan(%v) = %+v, want an error", c.args, r)
			}
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("error %v is %T, want *runplan.Error", err, err)
			}
			if pe.Flag != c.wantFlag {
				t.Errorf("Flag = %q, want %q", pe.Flag, c.wantFlag)
			}
			if c.wantText != "" && !strings.Contains(err.Error(), c.wantText) {
				t.Errorf("error %v, want it to mention %q", err, c.wantText)
			}
			if c.wantAs != nil && !errors.As(err, c.wantAs) {
				t.Errorf("error %v does not wrap %T", err, c.wantAs)
			}
		})
	}
}

// TestPlanIsDeterministic proves two plans with the same seed and clock
// generate the same payloads, so the seed reaches the generator.
func TestPlanIsDeterministic(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	args := []string{"-spec", spec, "-channel", "orders", "-dry-run", "-seed", "42", "-now", "2026-09-22T00:00:00Z"}

	first, err := Plan(args)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	second, err := Plan(args)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	a, err := first.Config.Generator.Value(first.Config.Schema)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	b, err := second.Config.Generator.Value(second.Config.Schema)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("same seed and now generated %v and %v", a, b)
	}
}

// TestNewSink proves the sink is built on demand: dry run writes to the given
// writers, and only the produce path dials a broker.
func TestNewSink(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	r, err := Plan([]string{"-spec", spec, "-channel", "orders", "-dry-run"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	var stdout, stderr strings.Builder
	sink, err := r.NewSink(context.Background(), &stdout, &stderr)
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	defer sink.Close()
	if err := sink.Send(context.Background(), pipeline.Outgoing{Payload: []byte(`{"a":1}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := stdout.String(); !strings.Contains(got, `{"a":1}`) {
		t.Errorf("dry-run sink wrote %q to the given stdout, want the payload", got)
	}
}

// TestNewEncoder proves the encoder is chosen per format and mode, and that
// only the produce path contacts a registry.
func TestNewEncoder(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	value := write(t, "value.avsc", valueAvsc)

	json, err := Plan([]string{"-spec", spec, "-channel", "orders", "-dry-run"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err := json.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(pipeline.JsonEncoder); !ok {
		t.Errorf("json encoder = %T, want pipeline.JsonEncoder", enc)
	}

	avroDry, err := Plan([]string{"-spec", spec, "-channel", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-registry", "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err = avroDry.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(*pipeline.AvroDisplayEncoder); !ok {
		t.Errorf("avro dry-run encoder = %T, want *pipeline.AvroDisplayEncoder (no registry contact)", enc)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprint(w, `{"id":7}`)
	}))
	defer srv.Close()
	produce, err := Plan([]string{"-spec", spec, "-channel", "orders", "-format", "avro", "-avro-schema", value, "-registry", srv.URL})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err = produce.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(*pipeline.AvroEncoder); !ok {
		t.Errorf("avro produce encoder = %T, want *pipeline.AvroEncoder", enc)
	}

	dead, err := Plan([]string{"-spec", spec, "-channel", "orders", "-format", "avro", "-avro-schema", value, "-registry", "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := dead.NewEncoder(context.Background()); err == nil {
		t.Error("an unreachable registry must fail encoder construction")
	} else {
		var re *pipeline.RegistryError
		if !errors.As(err, &re) {
			t.Errorf("error %v is %T, want *pipeline.RegistryError", err, err)
		}
	}
}

// TestUsage proves the usage block is written to the caller's writer, not to a
// package-level stderr.
func TestUsage(t *testing.T) {
	var buf strings.Builder
	Usage(&buf, "ktg")
	out := buf.String()
	for _, want := range []string{"Usage: ktg", "-keyPath", "-avro-schema", "Examples:"} {
		if !strings.Contains(out, want) {
			t.Errorf("usage is missing %q:\n%s", want, out)
		}
	}
	var discard io.Writer = io.Discard
	Usage(discard, "ktg") // must not panic on a plain writer
}
