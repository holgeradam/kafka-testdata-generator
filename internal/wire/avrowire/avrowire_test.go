package avrowire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
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
		MessageTypes: []asyncapi.MessageType{{
			Name:       "Order",
			Payload:    map[string]any{"type": "integer"},
			KeyBinding: map[string]any{"type": "integer"},
		}},
	}
}

// TestBuildGeneratesFromValueAvsc proves the Payload follows the value avsc,
// whatever Message schema the spec declares.
func TestBuildGeneratesFromValueAvsc(t *testing.T) {
	parts, err := Format{}.Build(options(t))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v, err := parts.Values.Value()
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

// TestCheck proves the AVRO flag rules, judged against the spec: one value
// avsc, from the spec or -avro-schema; one key avsc, from the spec's Key
// binding or -avro-key-schema, for -keyPath; a registry only when producing;
// one Avro Message type until #91; and registry bindings the tool honours.
func TestCheck(t *testing.T) {
	cases := []struct {
		name, flag string
		opts       wire.Options
	}{
		{"dry run", "", wire.Options{AvroSchema: "v.avsc", DryRun: true}},
		{"produce", "", wire.Options{AvroSchema: "v.avsc", RegistryURL: "http://localhost:8081"}},
		{"planted key", "", wire.Options{AvroSchema: "v.avsc", AvroKeySchema: "k.avsc", KeyPath: "id", DryRun: true}},
		{"no value avsc", "avro-schema", wire.Options{DryRun: true}},
		{"key path without key avsc", "keyPath", wire.Options{AvroSchema: "v.avsc", KeyPath: "id", DryRun: true}},
		{"produce without registry", "registry", wire.Options{AvroSchema: "v.avsc"}},
		{"spec avsc", "", wire.Options{MessageTypes: avroTypes("A"), DryRun: true}},
		{"spec key planted", "", wire.Options{MessageTypes: keyedAvroTypes("A"), KeyPath: "id", DryRun: true}},
		{"spec avsc, key avsc planted", "", wire.Options{MessageTypes: avroTypes("A"), AvroKeySchema: "k.avsc", KeyPath: "id", DryRun: true}},
		{"spec avsc and value avsc", "avro-schema", wire.Options{MessageTypes: avroTypes("A"), AvroSchema: "v.avsc", DryRun: true}},
		{"spec key and key avsc", "avro-key-schema", wire.Options{MessageTypes: keyedAvroTypes("A"), AvroKeySchema: "k.avsc", DryRun: true}},
		{"spec avsc, key path without a key", "keyPath", wire.Options{MessageTypes: avroTypes("A"), KeyPath: "id", DryRun: true}},
		{"several spec avscs", "topic", wire.Options{MessageTypes: avroTypes("A", "B"), DryRun: true}},
		{"registry binding", "topic", wire.Options{MessageTypes: []asyncapi.MessageType{{Name: "A", Payload: map[string]any{}, Registry: asyncapi.RegistryBinding{SchemaIDLocation: "header"}}}, AvroSchema: "v.avsc", DryRun: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Format{}.Check(tc.opts)
			if tc.flag == "" {
				if err != nil {
					t.Fatalf("Check: %v, want nil", err)
				}
				return
			}
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != tc.flag {
				t.Fatalf("err = %v, want a *wire.Error on -%s", err, tc.flag)
			}
		})
	}
}

// TestBuildWarnsOfIgnoredBinding proves a spec's key binding is reported as
// ignored under AVRO, and stays silent when there is none.
func TestBuildWarnsOfIgnoredBinding(t *testing.T) {
	opts := options(t)
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(parts.Warnings) != 1 || parts.Warnings[0] != "Warning: key bindings are ignored under -format avro" {
		t.Errorf("warnings = %q, want the ignored-binding warning", parts.Warnings)
	}

	opts.MessageTypes[0].KeyBinding = nil
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(parts.Warnings) != 0 {
		t.Errorf("warnings = %q, want none without a binding", parts.Warnings)
	}
}

const regionAvsc = `{"type":"record","name":"Order","fields":[
	{"name":"id","type":"string"},
	{"name":"region","type":"string"},
	{"name":"zone","type":{"type":"enum","name":"Zone","symbols":["eu","us"]}},
	{"name":"ref","type":{"type":"string","logicalType":"uuid"}},
	{"name":"note","type":["null","string"]},
	{"name":"count","type":"int"},
	{"name":"meta","type":{"type":"record","name":"Meta","fields":[{"name":"tenant","type":"string"}]}}
]}`

func parameter(name, value string, pointer ...string) asyncapi.TopicParameter {
	return asyncapi.TopicParameter{Name: name, Value: value, Location: "$message.payload#/" + strings.Join(pointer, "/"), Pointer: pointer}
}

// TestBuildPlantsTopicParameters proves under AVRO a Topic parameter's value
// lands at its location in every Payload, walked through the value avsc
// (#83): a string, an enum symbol, a uuid and a nested record field.
func TestBuildPlantsTopicParameters(t *testing.T) {
	opts := options(t)
	opts.AvroSchema = writeAvsc(t, regionAvsc)
	const uuid = "0b7e8c3a-6f2d-4a51-9c1e-2d3f4a5b6c7d"
	opts.TopicParameters = []asyncapi.TopicParameter{
		parameter("region", "eu", "region"),
		parameter("zone", "us", "zone"),
		parameter("ref", uuid, "ref"),
		parameter("tenant", "acme", "meta", "tenant"),
		{Name: "env", Value: "prod"},
	}
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for i := 0; i < 30; i++ {
		v, err := parts.Values.Value()
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		p := v.(map[string]any)
		if p["region"] != "eu" || p["zone"] != "us" || p["ref"] != uuid || p["meta"].(map[string]any)["tenant"] != "acme" {
			t.Fatalf("record %d = %v, want every Topic parameter planted", i, p)
		}
	}
}

// TestBuildRejectsTopicParameters proves under AVRO each Topic parameter the
// avsc cannot hold is refused before a record exists.
func TestBuildRejectsTopicParameters(t *testing.T) {
	cases := map[string]struct {
		param   asyncapi.TopicParameter
		keyPath string
		flag    string
		want    string
	}{
		"union": {parameter("note", "x", "note"), "", "topic",
			`Topic parameter note: location $message.payload#/note: at "/note": the schema here is a union, so the branch differs per record and the value may be null`},
		"missing field": {parameter("x", "x", "nope"), "", "topic",
			`Topic parameter x: location $message.payload#/nope: at "/nope": record Order has no field "nope"`},
		"not a string": {parameter("count", "7", "count"), "", "topic",
			"Topic parameter count: the Payload field at $message.payload#/count is int, which cannot hold the parameter's string value"},
		"not a symbol": {parameter("zone", "apac", "zone"), "", "topic",
			"Topic parameter zone: value apac is not a symbol of enum Zone [eu, us]"},
		"not a uuid": {parameter("ref", "eu", "ref"), "", "topic",
			"Topic parameter ref: value eu is not a uuid, which the Payload field at $message.payload#/ref (string (uuid)) requires"},
		"clash with -keyPath": {parameter("tenant", "acme", "meta", "tenant"), "meta", "keyPath",
			"Topic parameter tenant: location $message.payload#/meta/tenant overlaps -keyPath meta; both would plant into the same field"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			opts := options(t)
			opts.AvroSchema = writeAvsc(t, regionAvsc)
			opts.TopicParameters = []asyncapi.TopicParameter{c.param}
			if c.keyPath != "" {
				opts.KeyPath = c.keyPath
				opts.AvroKeySchema = writeAvsc(t, `{"type":"record","name":"Meta","fields":[{"name":"tenant","type":"string"}]}`)
			}
			_, err := Format{}.Build(opts)
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != c.flag {
				t.Fatalf("err = %v, want a *wire.Error on -%s", err, c.flag)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

// avroTypes are Message types whose payloads are Avro, one per name.
func avroTypes(names ...string) []asyncapi.MessageType {
	var types []asyncapi.MessageType
	for _, n := range names {
		types = append(types, asyncapi.MessageType{Name: n, Avsc: []byte(`{"type":"record","name":"` + n + `","fields":[{"name":"id","type":"string"}]}`)})
	}
	return types
}

// keyedAvroTypes are avroTypes with an Avro Key binding.
func keyedAvroTypes(names ...string) []asyncapi.MessageType {
	types := avroTypes(names...)
	for i := range types {
		types[i].KeyAvsc = []byte(`"string"`)
	}
	return types
}
