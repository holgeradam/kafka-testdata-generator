package jsonwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

var _ wire.Format = Format{}

func options() wire.Options {
	return wire.Options{
		Synth:        synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		MessageTypes: []asyncapi.MessageType{{Name: "Order", Payload: orderSchema()}},
	}
}

func orderSchema() map[string]any {
	return map[string]any{
		"type":       "object",
		"required":   []any{"orderId"},
		"properties": map[string]any{"orderId": map[string]any{"type": "string"}},
	}
}

// kindSchema is a Payload schema whose kind field is the constant name, so a
// generated Payload shows which Message type it is of.
func kindSchema(name string, required ...string) map[string]any {
	props := map[string]any{"kind": map[string]any{"const": name}}
	req := []any{"kind"}
	for _, r := range required {
		props[r] = map[string]any{"type": "string"}
		req = append(req, r)
	}
	return map[string]any{"type": "object", "required": req, "properties": props}
}

// TestBuildGeneratesFromMessageSchema proves the Payload honours the Message
// schema the format bound at Build.
func TestBuildGeneratesFromMessageSchema(t *testing.T) {
	opts := options()
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v, err := parts.Values.Value()
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

	opts.MessageTypes[0].KeyBinding = map[string]any{"type": "string"}
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

// TestCheck proves JSON rejects the AVRO flags before any file is read,
// naming the flag at fault.
func TestCheck(t *testing.T) {
	cases := []struct {
		name, flag string
		opts       wire.Options
	}{
		{"plain run", "", wire.Options{Topic: "orders"}},
		{"value avsc", "avro-schema", wire.Options{AvroSchema: "v.avsc"}},
		{"key avsc", "avro-schema", wire.Options{AvroKeySchema: "k.avsc"}},
		{"registry", "registry", wire.Options{RegistryURL: "http://localhost:8081"}},
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

// TestBuildRejectsKeyPathWithoutBinding proves -keyPath needs a key binding to
// generate the Key it plants.
func TestBuildRejectsKeyPathWithoutBinding(t *testing.T) {
	opts := options()
	opts.KeyPath = "orderId"
	_, err := Format{}.Build(opts)
	var we *wire.Error
	if !errors.As(err, &we) || we.Flag != "keyPath" {
		t.Fatalf("err = %v, want a *wire.Error on -keyPath", err)
	}
}

// mixOptions builds options for a Kafka topic with the given Message types.
func mixOptions(seed int64, types ...asyncapi.MessageType) wire.Options {
	return wire.Options{
		Synth:        synth.New(seed, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		MessageTypes: types,
	}
}

// kinds generates n Payloads and returns their kind fields in order.
func kinds(t *testing.T, opts wire.Options, n int) []string {
	t.Helper()
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	out := make([]string, n)
	for i := range out {
		v, err := parts.Values.Value()
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		out[i] = fmt.Sprint(v.(map[string]any)["kind"])
	}
	return out
}

// TestBuildMixesMessageTypes proves each Payload is of one Message type picked
// from the seeded stream (#74): every type appears, and the same seed repeats
// the same sequence.
func TestBuildMixesMessageTypes(t *testing.T) {
	types := []asyncapi.MessageType{
		{Name: "OrderCreated", Payload: kindSchema("created")},
		{Name: "OrderUpdated", Payload: kindSchema("updated")},
		{Name: "OrderCancelled", Payload: kindSchema("cancelled")},
	}
	first := kinds(t, mixOptions(7, types...), 60)
	seen := map[string]int{}
	for _, k := range first {
		seen[k]++
	}
	for _, want := range []string{"created", "updated", "cancelled"} {
		if seen[want] == 0 {
			t.Errorf("no %s Payload in 60 records: %v", want, seen)
		}
	}
	if again := kinds(t, mixOptions(7, types...), 60); strings.Join(again, ",") != strings.Join(first, ",") {
		t.Errorf("same seed gave a different sequence:\n%v\n%v", first, again)
	}
}

// TestBuildSingleTypeDrawsNothingExtra proves a Kafka topic with one Message
// type generates exactly as before the mix: no pick is drawn from the stream.
func TestBuildSingleTypeDrawsNothingExtra(t *testing.T) {
	opts := mixOptions(3, asyncapi.MessageType{Name: "Order", Payload: orderSchema()})
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	direct := generator.New(synth.New(3, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	for i := 0; i < 5; i++ {
		got, _ := parts.Values.Value()
		want, _ := direct.Value(orderSchema())
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("record %d: %v, want %v as generated without a mix", i, got, want)
		}
	}
}

// TestBuildRejectsDifferingKeyBindings proves the Message types of a Kafka
// topic share one Key binding, or none, since a Key identifies one Entity
// across them (#34 decision 3).
func TestBuildRejectsDifferingKeyBindings(t *testing.T) {
	uuidKey := map[string]any{"type": "string", "format": "uuid"}
	cases := map[string][]asyncapi.MessageType{
		"different schemas": {
			{Name: "OrderCreated", Payload: kindSchema("created"), KeyBinding: uuidKey},
			{Name: "OrderUpdated", Payload: kindSchema("updated"), KeyBinding: map[string]any{"type": "integer"}},
		},
		"one without": {
			{Name: "OrderCreated", Payload: kindSchema("created"), KeyBinding: uuidKey},
			{Name: "OrderUpdated", Payload: kindSchema("updated")},
		},
	}
	for name, types := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Format{}.Build(mixOptions(1, types...))
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != "topic" {
				t.Fatalf("err = %v, want a *wire.Error on -topic", err)
			}
			for _, n := range []string{"OrderCreated", "OrderUpdated"} {
				if !strings.Contains(err.Error(), n) {
					t.Errorf("err = %v, want it to name %s", err, n)
				}
			}
		})
	}

	same := []asyncapi.MessageType{
		{Name: "OrderCreated", Payload: kindSchema("created"), KeyBinding: uuidKey},
		{Name: "OrderUpdated", Payload: kindSchema("updated"), KeyBinding: map[string]any{"type": "string", "format": "uuid"}},
	}
	parts, err := Format{}.Build(mixOptions(1, same...))
	if err != nil {
		t.Fatalf("identical Key bindings: Build = %v, want nil", err)
	}
	if parts.KeyGen == nil {
		t.Error("identical Key bindings must produce a Key generator")
	}
}

// TestBuildChecksKeyPathInEveryType proves -keyPath must be guaranteed in
// every Message type's Payload, and a rejection names the type it fails in.
func TestBuildChecksKeyPathInEveryType(t *testing.T) {
	key := map[string]any{"type": "string"}
	opts := mixOptions(1,
		asyncapi.MessageType{Name: "OrderCreated", Payload: kindSchema("created", "orderId"), KeyBinding: key},
		asyncapi.MessageType{Name: "OrderUpdated", Payload: kindSchema("updated"), KeyBinding: key},
	)
	opts.KeyPath = "orderId"
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	steps, _ := keyplan.ParsePath("orderId")
	err = parts.Checker.Check(steps)
	var pe *keyplan.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *keyplan.PathError", err)
	}
	if !strings.Contains(err.Error(), "OrderUpdated") || strings.Contains(err.Error(), "OrderCreated") {
		t.Errorf("err = %v, want it to name OrderUpdated only", err)
	}
}

// regionSchema is a Payload schema of one Message type, kind name, whose
// required region field only takes two lowercase letters.
func regionSchema(name string) map[string]any {
	s := kindSchema(name)
	s["required"] = append(s["required"].([]any), "region", "meta")
	props := s["properties"].(map[string]any)
	props["region"] = map[string]any{"type": "string", "pattern": "^[a-z]{2}$"}
	props["meta"] = map[string]any{
		"type":       "object",
		"properties": map[string]any{"tenant": map[string]any{"type": "string"}},
	}
	return s
}

func regionParameter(value string) asyncapi.TopicParameter {
	return asyncapi.TopicParameter{Name: "region", Value: value, Location: "$message.payload#/region", Pointer: []string{"region"}}
}

// TestBuildPlantsTopicParameters proves a Topic parameter's value lands at its
// location in every Payload of every Message type (#83), and a parameter with
// no location plants nothing.
func TestBuildPlantsTopicParameters(t *testing.T) {
	opts := mixOptions(3,
		asyncapi.MessageType{Name: "OrderCreated", Payload: regionSchema("created")},
		asyncapi.MessageType{Name: "OrderUpdated", Payload: regionSchema("updated")},
	)
	opts.TopicParameters = []asyncapi.TopicParameter{regionParameter("eu"), {Name: "env", Value: "prod"}}
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	seen := map[any]bool{}
	for i := 0; i < 50; i++ {
		v, err := parts.Values.Value()
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		p := v.(map[string]any)
		seen[p["kind"]] = true
		if p["region"] != "eu" {
			t.Fatalf("record %d: region = %v, want the Topic parameter's eu", i, p["region"])
		}
	}
	if len(seen) != 2 {
		t.Errorf("Message types seen = %v, want both", seen)
	}
}

// TestBuildRejectsTopicParameters proves each Topic parameter the run cannot
// honour is refused before a record exists, each with its own error.
func TestBuildRejectsTopicParameters(t *testing.T) {
	created := asyncapi.MessageType{Name: "OrderCreated", Payload: regionSchema("created")}
	bare := asyncapi.MessageType{Name: "OrderCancelled", Payload: kindSchema("cancelled")}
	cases := map[string]struct {
		types   []asyncapi.MessageType
		param   asyncapi.TopicParameter
		keyPath string
		flag    string
		want    string
	}{
		"unguaranteed": {
			[]asyncapi.MessageType{created}, asyncapi.TopicParameter{Name: "tenant", Value: "acme", Location: "$message.payload#/meta/tenant", Pointer: []string{"meta", "tenant"}}, "", "topic",
			`Topic parameter tenant: location $message.payload#/meta/tenant: at "/meta/tenant": property "tenant" is not required`,
		},
		"missing in one Message type": {
			[]asyncapi.MessageType{created, bare}, regionParameter("eu"), "", "topic",
			`Topic parameter region: location $message.payload#/region: in Message type OrderCancelled: at "/region": the object has no property "region"`,
		},
		"value breaks the field": {
			[]asyncapi.MessageType{created}, regionParameter("EU"), "", "topic",
			"Topic parameter region: value EU does not conform to the Payload field at $message.payload#/region: does not match pattern",
		},
		"clash with -keyPath": {
			[]asyncapi.MessageType{created}, regionParameter("eu"), "region", "keyPath",
			"Topic parameter region: location $message.payload#/region overlaps -keyPath region; both would plant into the same field",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			for i := range c.types {
				c.types[i].KeyBinding = map[string]any{"type": "string"}
			}
			opts := mixOptions(1, c.types...)
			opts.TopicParameters = []asyncapi.TopicParameter{c.param}
			opts.KeyPath = c.keyPath
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

// TestBuildRejectsParametersPlantingTogether proves two Topic parameters whose
// locations overlap are refused, rather than one silently overwriting the
// other.
func TestBuildRejectsParametersPlantingTogether(t *testing.T) {
	opts := mixOptions(1, asyncapi.MessageType{Name: "OrderCreated", Payload: regionSchema("created")})
	opts.TopicParameters = []asyncapi.TopicParameter{
		regionParameter("eu"),
		{Name: "area", Value: "us", Location: "$message.payload#/region", Pointer: []string{"region"}},
	}
	_, err := Format{}.Build(opts)
	want := "Topic parameters region and area plant into the same field ($message.payload#/region and $message.payload#/region)"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}
