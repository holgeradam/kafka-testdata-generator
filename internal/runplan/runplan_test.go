package runplan

import (
	"context"
	"errors"
	"flag"
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
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/avrowire"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/jsonwire"
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

// twoEntriesSpec binds two spec entries to one Kafka topic, whose two Message
// types the run mixes (#74).
const twoEntriesSpec = `
asyncapi: '2.6.0'
info: {title: Two, version: '1.0.0'}
channels:
  orders-v1:
    bindings: {kafka: {topic: orders}}
    publish: {message: {payload: {type: object}}}
  orders-v2:
    bindings: {kafka: {topic: orders}}
    publish: {message: {payload: {type: object}}}
`

const valueAvsc = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
const keyAvsc = `{"type":"string"}`

// TestPlanAccepts covers the runs that are valid, asserting what the plan
// carries rather than what the binary prints.
func TestPlanAccepts(t *testing.T) {
	spec := write(t, "spec.yaml", plainSpec)
	twoEntries := write(t, "two.yaml", twoEntriesSpec)
	bound := write(t, "bound.yaml", bindingSpec)
	value := write(t, "value.avsc", valueAvsc)
	key := write(t, "key.avsc", keyAvsc)

	cases := []struct {
		name  string
		args  []string
		check func(*testing.T, *Run)
	}{
		{
			name: "two Message types mix",
			args: []string{"-spec", twoEntries, "-topic", "orders", "-dry-run"},
			check: func(t *testing.T, r *Run) {
				if _, err := generate(r); err != nil {
					t.Errorf("generating from the mix: %v", err)
				}
			},
		},
		{
			name: "avro ignores the Message types",
			args: []string{"-spec", twoEntries, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value},
			check: func(t *testing.T, r *Run) {
				v, err := generate(r)
				if err != nil {
					t.Fatalf("generating: %v", err)
				}
				if _, ok := v.(map[string]any)["id"]; !ok {
					t.Errorf("payload = %v, want the value avsc's record", v)
				}
			},
		},
		{
			name: "json dry run",
			args: []string{"-spec", spec, "-topic", "orders", "-dry-run"},
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
				if r.Config.Generator == nil {
					t.Error("plan must carry a generator")
				}
			},
		},
		{
			name: "key plan from binding",
			args: []string{"-spec", bound, "-topic", "orders", "-dry-run", "-keyPath", "orderId"},
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
			name: "keys reused across records",
			args: []string{"-spec", bound, "-topic", "orders", "-dry-run", "-keyPath", "orderId", "-records-per-key", "4", "-seed", "3"},
			check: func(t *testing.T, r *Run) {
				distinct := map[any]bool{}
				for i := 0; i < 400; i++ {
					payload := map[string]any{"orderId": "before"}
					k, err := r.Config.KeyPlan.Apply(payload)
					if err != nil {
						t.Fatalf("Apply: %v", err)
					}
					if payload["orderId"] != k {
						t.Fatalf("record %d: planted %v, key %v; want the reused Key planted", i, payload["orderId"], k)
					}
					distinct[k] = true
				}
				if avg := 400.0 / float64(len(distinct)); avg < 3 || avg > 5 {
					t.Errorf("400 records over %d Keys = %.1f per Key, want about 4", len(distinct), avg)
				}
			},
		},
		{
			name: "binding without a path still keys",
			args: []string{"-spec", bound, "-topic", "orders", "-dry-run"},
			check: func(t *testing.T, r *Run) {
				if r.Config.KeyPlan == nil {
					t.Error("a key binding alone must still produce a KeyPlan")
				}
			},
		},
		{
			name: "avro dry run",
			args: []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value},
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
			args: []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-avro-key-schema", key},
			check: func(t *testing.T, r *Run) {
				if r.Config.KeyPlan == nil {
					t.Error("a key avsc must produce a KeyPlan")
				}
			},
		},
		{
			name: "acks accepts any case",
			args: []string{"-spec", spec, "-topic", "orders", "-dry-run", "-acks", "aLL"},
			check: func(t *testing.T, r *Run) {
				if r.Acks != producer.AcksAll {
					t.Errorf("Acks = %v, want all", r.Acks)
				}
			},
		},
		{
			name: "explicit json format matches the default",
			args: []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "json"},
			check: func(t *testing.T, r *Run) {
				if r.Format != "json" {
					t.Errorf("Format = %q, want json", r.Format)
				}
			},
		},
		{
			name: "kafka options",
			args: []string{"-spec", spec, "-topic", "orders", "-broker", "kafka:9092", "-acks", "all", "-count", "3"},
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
		{"dry run disregards kafka options", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-broker", "other:9092"}, "dry-run mode disregards Kafka options"},
		{"dry run disregards acks", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-acks", "all"}, "dry-run mode disregards Kafka options"},
		{"binding ignored under avro", []string{"-spec", bound, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value}, "key bindings are ignored under -format avro"},
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

	r, err := Plan([]string{"-spec", spec, "-topic", "orders", "-dry-run"})
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
	mixedKeys := write(t, "mixed.yaml", `
asyncapi: '2.6.0'
info: {title: Mixed, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: OrderCreated, bindings: {kafka: {key: {type: string}}}, payload: {type: object}}
          - {name: OrderUpdated, payload: {type: object}}
`)
	keyedMix := write(t, "keyed.yaml", `
asyncapi: '2.6.0'
info: {title: Keyed, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: OrderCreated, bindings: {kafka: {key: {type: string}}}, payload: {type: object, required: [orderId], properties: {orderId: {type: string}}}}
          - {name: OrderUpdated, bindings: {kafka: {key: {type: string}}}, payload: {type: object, properties: {orderId: {type: string}}}}
`)
	badBinding := write(t, "bad.yaml", `
asyncapi: '2.6.0'
info: {title: Bad, version: '1.0.0'}
channels:
  orders:
    publish: {message: {bindings: {kafka: {key: string}}, payload: {type: object}}}
`)
	broken := write(t, "broken.avsc", `{"type":"record","name":"X","fields":[{"name":"n","type":"nope"}]}`)

	cases := []struct {
		name     string
		args     []string
		wantFlag string
		wantText string
		wantAs   any
	}{
		{"no spec", []string{"-topic", "orders"}, "spec", "-spec is required", nil},
		{"no topic", []string{"-spec", spec}, "topic", "-topic is required", nil},
		{"renamed channel flag", []string{"-spec", spec, "-channel", "orders"}, "channel", "-channel was renamed to -topic", nil},
		{"Message types with different Key bindings", []string{"-spec", mixedKeys, "-topic", "orders", "-dry-run"}, "topic", "different Key bindings (OrderCreated vs OrderUpdated (none))", nil},
		{"key path missing in one Message type", []string{"-spec", keyedMix, "-topic", "orders", "-dry-run", "-keyPath", "orderId"}, "keyPath", "in Message type OrderUpdated", new(*keyplan.PathError)},
		{"unusable key binding", []string{"-spec", badBinding, "-topic", "orders", "-dry-run"}, "topic", "bindings.kafka.key must be a schema object", nil},
		{"records per key below 1", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-records-per-key", "0"}, "records-per-key", "-records-per-key must be at least 1", nil},
		{"key reuse without a key schema", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-records-per-key", "2"}, "records-per-key", "-records-per-key above 1 requires a key schema", nil},
		{"avro key reuse without a key avsc", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-records-per-key", "2"}, "records-per-key", "requires a key schema", nil},
		{"renamed key flag", []string{"-spec", spec, "-topic", "orders", "-key", "orderId"}, "key", "-key was renamed to -keyPath", nil},
		{"avro without value avsc", []string{"-spec", spec, "-topic", "orders", "-format", "avro"}, "avro-schema", "-avro-schema is required with -format avro", nil},
		{"avro key path without key avsc", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-keyPath", "id"}, "keyPath", "-keyPath requires a key avsc", nil},
		{"avro produce without registry", []string{"-spec", spec, "-topic", "orders", "-format", "avro", "-avro-schema", value}, "registry", "-registry is required", nil},
		{"avro flags under json", []string{"-spec", spec, "-topic", "orders", "-format", "json", "-avro-schema", value}, "format", "drop -format json", nil},
		{"registry under json", []string{"-spec", spec, "-topic", "orders", "-registry", "http://localhost:8081"}, "registry", "-registry is only valid with the avro Wire format", nil},
		{"key path without key schema", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-keyPath", "orderId"}, "keyPath", "-keyPath requires a key schema", nil},
		{"spec file missing", []string{"-spec", filepath.Join(t.TempDir(), "gone.yaml"), "-topic", "orders"}, "spec", "", nil},
		{"Kafka topic missing from spec", []string{"-spec", spec, "-topic", "nope", "-dry-run"}, "topic", "", nil},
		{"malformed avsc", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", broken}, "avro-schema", "", new(*avro.ParseError)},
		{"key path not guaranteed", []string{"-spec", bound, "-topic", "orders", "-dry-run", "-keyPath", "nickname"}, "keyPath", "not required", new(*keyplan.PathError)},
		{"unknown flag", []string{"-spec", spec, "-topic", "orders", "-nope"}, "", "not defined", nil},
		{"invalid format", []string{"-spec", spec, "-topic", "orders", "-format", "xml"}, "", "invalid -format", nil},
		{"invalid acks", []string{"-spec", spec, "-topic", "orders", "-acks", "two"}, "", "invalid -acks", nil},
		{"invalid now", []string{"-spec", spec, "-topic", "orders", "-now", "yesterday"}, "", "invalid -now", nil},
		{"avro key path not guaranteed", []string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-avro-key-schema", key, "-keyPath", "missing"}, "keyPath", "no field", new(*keyplan.PathError)},
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
	args := []string{"-spec", spec, "-topic", "orders", "-dry-run", "-seed", "42", "-now", "2026-09-22T00:00:00Z"}

	first, err := Plan(args)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	second, err := Plan(args)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	a, err := generate(first)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	b, err := generate(second)
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
	r, err := Plan([]string{"-spec", spec, "-topic", "orders", "-dry-run"})
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

	json, err := Plan([]string{"-spec", spec, "-topic", "orders", "-dry-run"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err := json.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(jsonwire.JsonEncoder); !ok {
		t.Errorf("json encoder = %T, want jsonwire.JsonEncoder", enc)
	}

	avroDry, err := Plan([]string{"-spec", spec, "-topic", "orders", "-dry-run", "-format", "avro", "-avro-schema", value, "-registry", "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err = avroDry.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(*avrowire.AvroDisplayEncoder); !ok {
		t.Errorf("avro dry-run encoder = %T, want *avrowire.AvroDisplayEncoder (no registry contact)", enc)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprint(w, `{"id":7}`)
	}))
	defer srv.Close()
	produce, err := Plan([]string{"-spec", spec, "-topic", "orders", "-format", "avro", "-avro-schema", value, "-registry", srv.URL})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	enc, err = produce.NewEncoder(context.Background())
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if _, ok := enc.(*avrowire.AvroEncoder); !ok {
		t.Errorf("avro produce encoder = %T, want *avrowire.AvroEncoder", enc)
	}

	dead, err := Plan([]string{"-spec", spec, "-topic", "orders", "-format", "avro", "-avro-schema", value, "-registry", "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := dead.NewEncoder(context.Background()); err == nil {
		t.Error("an unreachable registry must fail encoder construction")
	} else {
		var re *avrowire.RegistryError
		if !errors.As(err, &re) {
			t.Errorf("error %v is %T, want *avrowire.RegistryError", err, err)
		}
	}
}

// TestUsage proves the usage block is written to the caller's writer, not to a
// package-level stderr.
func TestUsage(t *testing.T) {
	var buf strings.Builder
	Usage(&buf, "ktg")
	out := buf.String()
	for _, want := range []string{"Usage: ktg", "-topic", "-keyPath", "-records-per-key", "-avro-schema", "Examples:", "ktg -spec order.yaml -topic orders.created"} {
		if !strings.Contains(out, want) {
			t.Errorf("usage is missing %q:\n%s", want, out)
		}
	}
	var discard io.Writer = io.Discard
	Usage(discard, "ktg") // must not panic on a plain writer
}

// TestUsageStatesEachDefaultOnceAndTruly proves every default in the help block
// is stated once and holds for every invocation: no default printed by the
// flag package on top of one the usage text already names, no timestamp or
// random number frozen at the moment help was printed, and a placeholder that
// says what the value is.
func TestUsageStatesEachDefaultOnceAndTruly(t *testing.T) {
	var buf strings.Builder
	Usage(&buf, "ktg")
	out := buf.String()
	for _, want := range []string{
		"  -acks level\n    \tAcks level: 1 (leader) or all (all in-sync replicas) (default 1)\n",
		"  -format name\n    \tWire format name: json or avro (default: avro when the spec's payloads are Avro or -avro-schema is given, else json)\n",
		"  -now time\n    \tClock for date fields, as an RFC3339 time (default: the current time)\n",
		"  -seed int\n    \tRandom seed for reproducibility (default: random)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("usage is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, " value\n") {
		t.Errorf("a flag shows the generic placeholder \"value\":\n%s", out)
	}
}

// TestPlanHelp proves -h reaches the caller as flag.ErrHelp, so the process
// edge can treat it as a request rather than a failure.
func TestPlanHelp(t *testing.T) {
	for _, arg := range []string{"-h", "-help"} {
		if _, err := Plan([]string{arg}); !errors.Is(err, flag.ErrHelp) {
			t.Errorf("Plan(%s) error = %v, want flag.ErrHelp", arg, err)
		}
	}
}

// generate draws one Payload from the plan's generator.
func generate(r *Run) (any, error) {
	g, err := r.Config.Generator.Generate()
	return g.Payload, err
}
