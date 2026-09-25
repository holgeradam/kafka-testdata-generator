package wire

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// TestEncodeHeaders proves each property becomes one Kafka record header,
// sorted by name, its value plain-scalar as a JSON Key is, null as a null
// header (#85 decision 2).
func TestEncodeHeaders(t *testing.T) {
	got, err := EncodeHeaders(map[string]any{
		"tenant":  "acme",
		"attempt": float64(3),
		"ratio":   0.5,
		"retry":   true,
		"trace":   nil,
		"tags":    []any{"a", "b"},
		"origin":  map[string]any{"zone": "eu"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []pipeline.Header{
		{Name: "attempt", Value: []byte("3")},
		{Name: "origin", Value: []byte(`{"zone":"eu"}`)},
		{Name: "ratio", Value: []byte("0.5")},
		{Name: "retry", Value: []byte("true")},
		{Name: "tags", Value: []byte(`["a","b"]`)},
		{Name: "tenant", Value: []byte("acme")},
		{Name: "trace", Value: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("headers = %v, want %v", got, want)
	}
}

// TestHeaderSource proves each record's Headers come from its own Message
// type's headers schema, none for a type that declares none, and that no
// source exists when no type declares any, so such runs draw nothing more
// from the seeded stream.
func TestHeaderSource(t *testing.T) {
	s := synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if mustHeaderSource(t, s, []asyncapi.MessageType{{Name: "A"}}) != nil {
		t.Error("no Message type declares headers: want no source")
	}
	hs := mustHeaderSource(t, s, []asyncapi.MessageType{
		{Name: "A", Headers: map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{"tenant": map[string]any{"type": "string", "enum": []any{"acme"}}}}},
		{Name: "B"},
	})
	got, err := hs.Generate(0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}; !reflect.DeepEqual(got, want) {
		t.Errorf("A's headers = %v, want %v", got, want)
	}
	if got, err := hs.Generate(1); err != nil || got != nil {
		t.Errorf("B's headers = %v, %v; want none", got, err)
	}

	bad := mustHeaderSource(t, s, []asyncapi.MessageType{{Name: "A", Headers: map[string]any{"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "wat"}}}}})
	if _, err := bad.Generate(0); err == nil || !strings.Contains(err.Error(), "headers of A") {
		t.Errorf("err = %v, want a generation error naming the headers of A", err)
	}
}

func mustHeaderSource(t *testing.T, s *synth.Synthesizer, types []asyncapi.MessageType, params ...asyncapi.TopicParameter) *HeaderSource {
	t.Helper()
	hs, err := NewHeaderSource(s, types, params)
	if err != nil {
		t.Fatalf("NewHeaderSource: %v", err)
	}
	return hs
}

// tenantHeaders is a headers schema whose tenant header is required, of
// lower-case letters, and whose trace header is optional.
func tenantHeaders() map[string]any {
	return map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{
		"tenant": map[string]any{"type": "string", "pattern": "^[a-z]+$"},
		"trace":  map[string]any{"type": "string"},
	}}
}

func headerParam(name, value, pointer string) asyncapi.TopicParameter {
	return asyncapi.TopicParameter{Name: name, Value: value, Location: "$message.header#/" + pointer, Pointer: []string{pointer}, InHeaders: true}
}

// TestHeaderSourcePlants proves a Topic parameter with a header location has
// its value planted into the Headers of every record of every Message type,
// before they are encoded, and that a payload location is not its business
// (#93).
func TestHeaderSourcePlants(t *testing.T) {
	s := synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	payloadParam := asyncapi.TopicParameter{Name: "region", Value: "eu", Location: "$message.payload#/region", Pointer: []string{"region"}}
	hs := mustHeaderSource(t, s, []asyncapi.MessageType{{Name: "A", Headers: tenantHeaders()}, {Name: "B", Headers: tenantHeaders()}}, headerParam("tenant", "acme", "tenant"), payloadParam)
	for i := 0; i < 2; i++ {
		for n := 0; n < 10; n++ {
			got, err := hs.Generate(i)
			if err != nil {
				t.Fatal(err)
			}
			if got[len(got)-1].Name == "trace" {
				got = got[:len(got)-1]
			}
			if want := []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("type %d: headers %v, want tenant=acme planted", i, got)
			}
		}
	}
}

// TestHeaderSourceRefusesPlantings proves every header planting the run
// cannot honour stops it before any record exists, with its own error.
func TestHeaderSourceRefusesPlantings(t *testing.T) {
	s := synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	both := []asyncapi.MessageType{{Name: "A", Headers: tenantHeaders()}, {Name: "B", Headers: tenantHeaders()}}
	cases := map[string]struct {
		types  []asyncapi.MessageType
		params []asyncapi.TopicParameter
		want   string
	}{
		"a Message type without headers": {[]asyncapi.MessageType{{Name: "A", Headers: tenantHeaders()}, {Name: "B"}},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: in Message type B: B declares no headers"},
		"no Message type declares headers": {[]asyncapi.MessageType{{Name: "A"}},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: A declares no headers"},
		"records of no Message type": {nil,
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: under -avro-schema the records are of no Message type in the spec, so they have no Headers"},
		"not guaranteed": {both,
			[]asyncapi.TopicParameter{headerParam("trace", "t1", "trace")},
			"Topic parameter trace: location $message.header#/trace: in Message type A:"},
		"value the header refuses": {both,
			[]asyncapi.TopicParameter{headerParam("tenant", "ACME", "tenant")},
			"Topic parameter tenant: value ACME does not conform to the header at $message.header#/tenant: in Message type A:"},
		"two plantings in one header": {both,
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant"), headerParam("org", "acme", "tenant")},
			"Topic parameters tenant and org plant into the same field"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewHeaderSource(s, c.types, c.params)
			var we *Error
			if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want a topic error mentioning %q", err, c.want)
			}
		})
	}
}
