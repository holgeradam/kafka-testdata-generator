package jsonwire

import (
	"reflect"

	"context"
	"errors"
	"fmt"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

var _ wire.Format = Format{}

func options() buildOptions {
	return buildOptions{
		Synth:        synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		MessageTypes: []asyncapi.MessageType{{Name: "Order", Payload: orderSchema()}},
	}
}

func orderSchema() asyncapi.JSONSchema {
	return map[string]any{
		"type":       "object",
		"required":   []any{"orderId"},
		"properties": map[string]any{"orderId": map[string]any{"type": "string"}},
	}
}

// kindSchema is a Payload schema whose kind field is the constant name, so a
// generated Payload shows which Message type it is of.
func kindSchema(name string, required ...string) asyncapi.JSONSchema {
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
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v, err := generate(parts)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if _, ok := v.(map[string]any)["orderId"].(string); !ok {
		t.Errorf("payload = %#v, want an object with a string orderId", v)
	}
}

// TestBuildKey proves the Key comes from the key binding: none means a null
// Key, and with -keyPath the Key is planted into the Payload.
func TestBuildKey(t *testing.T) {
	opts := options()
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if g, err := parts.Values.Generate(); parts.Keyed || err != nil || g.Key != nil {
		t.Errorf("no key binding: Keyed %v, Key %v, %v; want a null Key", parts.Keyed, g.Key, err)
	}

	opts.MessageTypes[0].Key = asyncapi.JSONSchema{"type": "string"}
	parts, err = build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	g, err := parts.Values.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := g.Key.(string); !parts.Keyed || !ok {
		t.Errorf("key binding: Keyed %v, Key %T; want a string Key from the binding", parts.Keyed, g.Key)
	}
	if g.Payload.(map[string]any)["orderId"] == g.Key {
		t.Error("no -keyPath: the Key must not be planted")
	}

	opts.KeyPath = "orderId"
	parts, err = build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if g, err = parts.Values.Generate(); err != nil || g.Payload.(map[string]any)["orderId"] != g.Key {
		t.Errorf("-keyPath orderId: Key %v, Payload %v, %v; want the Key planted", g.Key, g.Payload, err)
	}
}

// TestBuildEncoderIgnoresDryRun proves JSON encodes the same way in Dry run and
// produce.
func TestBuildEncoderIgnoresDryRun(t *testing.T) {
	for _, dry := range []bool{false, true} {
		opts := options()
		opts.DryRun = dry
		parts, err := build(opts)
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

// TestBuildFlagRules proves JSON's rules about its flags: the AVRO flags are
// refused, naming the flag at fault - -avro-schema never reaches it, since it
// makes the Wire format avro - and Key reuse needs a Key binding (#75). A rule
// about the flags wins over what the spec declares and over a Planting.
func TestBuildFlagRules(t *testing.T) {
	bound := func(o buildOptions) buildOptions {
		o.MessageTypes[0].Key = asyncapi.JSONSchema{"type": "string"}
		return o
	}
	with := func(change func(*buildOptions)) buildOptions {
		o := options()
		change(&o)
		return o
	}
	unplantable := []asyncapi.TopicParameter{{Name: "region", Value: "eu", Location: "$message.payload#/nope", Pointer: []string{"nope"}}}
	cases := []struct {
		name, flag string
		opts       buildOptions
	}{
		{"plain run", "", options()},
		{"key avsc", "avro-key-schema", with(func(o *buildOptions) { o.AvroKeySchema = "k.avsc" })},
		{"registry", "registry", with(func(o *buildOptions) { o.RegistryURL = "http://localhost:8081" })},
		{"key avsc before the spec", "avro-key-schema", with(func(o *buildOptions) { o.AvroKeySchema = "k.avsc"; o.MessageTypes = nil })},
		{"key reuse without a binding", "records-per-key", with(func(o *buildOptions) { o.RecordsPerKey = 2 })},
		{"key reuse with a binding", "", bound(with(func(o *buildOptions) { o.RecordsPerKey = 2 }))},
		{"key reuse before a Planting", "records-per-key", with(func(o *buildOptions) { o.RecordsPerKey = 2; o.TopicParameters = unplantable })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := build(tc.opts)
			if tc.flag == "" {
				if err != nil {
					t.Fatalf("Build: %v, want nil", err)
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
	_, err := build(opts)
	var we *wire.Error
	if !errors.As(err, &we) || we.Flag != "keyPath" {
		t.Fatalf("err = %v, want a *wire.Error on -keyPath", err)
	}
}

// mixOptions builds options for a Kafka topic with the given Message types.
func mixOptions(seed int64, types ...asyncapi.MessageType) buildOptions {
	return buildOptions{
		Synth:        synth.New(seed, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		MessageTypes: types,
	}
}

// kinds generates n Payloads and returns their kind fields in order.
func kinds(t *testing.T, opts buildOptions, n int) []string {
	t.Helper()
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	out := make([]string, n)
	for i := range out {
		v, err := generate(parts)
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
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	direct := generator.New(synth.New(3, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	for i := 0; i < 5; i++ {
		got, _ := generate(parts)
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
	uuidKey := asyncapi.JSONSchema{"type": "string", "format": "uuid"}
	cases := map[string][]asyncapi.MessageType{
		"different schemas": {
			{Name: "OrderCreated", Payload: kindSchema("created"), Key: uuidKey},
			{Name: "OrderUpdated", Payload: kindSchema("updated"), Key: asyncapi.JSONSchema{"type": "integer"}},
		},
		"one without": {
			{Name: "OrderCreated", Payload: kindSchema("created"), Key: uuidKey},
			{Name: "OrderUpdated", Payload: kindSchema("updated")},
		},
	}
	for name, types := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := build(mixOptions(1, types...))
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
		{Name: "OrderCreated", Payload: kindSchema("created"), Key: uuidKey},
		{Name: "OrderUpdated", Payload: kindSchema("updated"), Key: asyncapi.JSONSchema{"type": "string", "format": "uuid"}},
	}
	parts, err := build(mixOptions(1, same...))
	if err != nil {
		t.Fatalf("identical Key bindings: Build = %v, want nil", err)
	}
	if !parts.Keyed {
		t.Error("identical Key bindings must give the run a Key")
	}
}

// TestBuildChecksKeyPathInEveryType proves -keyPath must be guaranteed in
// every Message type's Payload, and a rejection names the type it fails in.
func TestBuildChecksKeyPathInEveryType(t *testing.T) {
	key := asyncapi.JSONSchema{"type": "string"}
	opts := mixOptions(1,
		asyncapi.MessageType{Name: "OrderCreated", Payload: kindSchema("created", "orderId"), Key: key},
		asyncapi.MessageType{Name: "OrderUpdated", Payload: kindSchema("updated"), Key: key},
	)
	opts.KeyPath = "orderId"
	_, err := build(opts)
	var we *wire.Error
	var pe *planting.PathError
	if !errors.As(err, &we) || we.Flag != "keyPath" || !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *planting.PathError on -keyPath", err)
	}
	if !strings.Contains(err.Error(), "OrderUpdated") || strings.Contains(err.Error(), "OrderCreated") {
		t.Errorf("err = %v, want it to name OrderUpdated only", err)
	}
}

// regionSchema is a Payload schema of one Message type, kind name, whose
// required region field only takes two lowercase letters.
func regionSchema(name string) asyncapi.JSONSchema {
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
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	seen := map[any]bool{}
	for i := 0; i < 50; i++ {
		v, err := generate(parts)
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
				c.types[i].Key = asyncapi.JSONSchema{"type": "string"}
			}
			opts := mixOptions(1, c.types...)
			opts.TopicParameters = []asyncapi.TopicParameter{c.param}
			opts.KeyPath = c.keyPath
			_, err := build(opts)
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
	_, err := build(opts)
	want := "Topic parameters region and area plant into the same field ($message.payload#/region and $message.payload#/region)"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}

// generate draws one Payload from the parts' generator.
func generate(parts *wire.Parts) (any, error) {
	g, err := parts.Values.Generate()
	return g.Payload, err
}

// tenantHeaders is a headers schema with one constant header, tenant=name.
func tenantHeaders(name string) asyncapi.JSONSchema {
	return map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{"tenant": map[string]any{"const": name}}}
}

// TestBuildGeneratesHeaders proves each record carries the Headers of its own
// Message type, and none when its type declares none (#92).
func TestBuildGeneratesHeaders(t *testing.T) {
	opts := options()
	opts.MessageTypes = []asyncapi.MessageType{
		{Name: "OrderCreated", Payload: kindSchema("created"), Headers: tenantHeaders("acme")},
		{Name: "OrderPaid", Payload: kindSchema("paid")},
	}
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	seen := map[int]bool{}
	for i := 0; i < 50; i++ {
		g, err := parts.Values.Generate()
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		seen[g.Type] = true
		var want []pipeline.Header
		if g.Type == 0 {
			want = []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}
		}
		if !reflect.DeepEqual(g.Headers, want) {
			t.Fatalf("record %d of type %d: headers %v, want %v", i, g.Type, g.Headers, want)
		}
	}
	if len(seen) != 2 {
		t.Errorf("Message types = %v, want both", seen)
	}
}

// TestEncoderFollowsDeclaredOrder proves a run's JSON encoder writes each
// Payload in the order its own Message type declares its properties, and an
// object Key in the order its Key binding does (#96).
func TestEncoderFollowsDeclaredOrder(t *testing.T) {
	str := map[string]any{"type": "string", "const": "v"}
	schema := func(names ...string) asyncapi.JSONSchema {
		props := map[string]any{}
		var order []any
		for _, n := range names {
			props[n] = str
			order = append(order, n)
		}
		return map[string]any{"type": "object", "required": order, "properties": props, ordered.Keyword: order}
	}
	opts := options()
	opts.MessageTypes = []asyncapi.MessageType{
		{Name: "A", Payload: schema("zeta", "alpha"), Key: schema("tenant", "id")},
		{Name: "B", Payload: schema("mid", "beta", "zulu"), Key: schema("tenant", "id")},
	}
	parts, err := build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	enc, err := parts.Encoder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"zeta":"v","alpha":"v"}`, `{"mid":"v","beta":"v","zulu":"v"}`}
	for i := 0; i < 20; i++ {
		g, err := parts.Values.Generate()
		if err != nil {
			t.Fatal(err)
		}
		keyBytes, payload, err := enc.Encode(g)
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != want[g.Type] {
			t.Fatalf("type %d: payload %s, want %s", g.Type, payload, want[g.Type])
		}
		if string(keyBytes) != `{"tenant":"v","id":"v"}` {
			t.Fatalf("key %s, want tenant before id", keyBytes)
		}
	}
}
