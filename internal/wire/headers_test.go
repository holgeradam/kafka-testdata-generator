package wire

import (
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// TestEncodeHeaders proves each property becomes one Kafka record header, its
// value plain-scalar as a JSON Key is, null as a null header (#85 decision
// 2); without a recorded order they come by name.
func TestEncodeHeaders(t *testing.T) {
	got, err := EncodeHeaders(nil, map[string]any{
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
	if NewHeaderSource(s, []asyncapi.MessageType{{Name: "A"}}) != nil {
		t.Error("no Message type declares headers: want no source")
	}
	hs := NewHeaderSource(s, []asyncapi.MessageType{
		{Name: "A", Headers: map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{"tenant": map[string]any{"type": "string", "enum": []any{"acme"}}}}},
		{Name: "B"},
	})
	got, err := generateHeaders(hs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}; !reflect.DeepEqual(got, want) {
		t.Errorf("A's headers = %v, want %v", got, want)
	}
	if got, err := generateHeaders(hs, 1); err != nil || got != nil {
		t.Errorf("B's headers = %v, %v; want none", got, err)
	}

	bad := NewHeaderSource(s, []asyncapi.MessageType{{Name: "A", Headers: map[string]any{"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "wat"}}}}})
	if _, err := generateHeaders(bad, 0); err == nil || !strings.Contains(err.Error(), "headers of A") {
		t.Errorf("err = %v, want a generation error naming the headers of A", err)
	}
}

// generateHeaders draws and encodes the Headers of a record of Message type
// i, as the Mix does when nothing is planted into them.
func generateHeaders(hs *HeaderSource, i int) ([]pipeline.Header, error) {
	v, err := hs.value(i)
	if err != nil {
		return nil, err
	}
	return hs.encode(i, v)
}

// TestHeadersFollowDeclaredOrder proves Headers come in the order the headers
// schema declares its properties, an object header's JSON text in its own
// declared order (#96); a schema without a recorded order keeps name order.
func TestHeadersFollowDeclaredOrder(t *testing.T) {
	s := synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	schema := map[string]any{"type": "object", "required": []any{"zone", "origin", "attempt"}, "properties": map[string]any{
		"zone":    map[string]any{"const": "eu"},
		"origin":  map[string]any{"type": "object", "required": []any{"z", "a"}, "properties": map[string]any{"z": map[string]any{"const": 1}, "a": map[string]any{"const": 2}}, generator.OrderKeyword: []any{"z", "a"}},
		"attempt": map[string]any{"const": 3},
	}, generator.OrderKeyword: []any{"zone", "origin", "attempt"}}
	got, err := generateHeaders(NewHeaderSource(s, []asyncapi.MessageType{{Name: "A", Headers: schema}}), 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []pipeline.Header{{Name: "zone", Value: []byte("eu")}, {Name: "origin", Value: []byte(`{"z":1,"a":2}`)}, {Name: "attempt", Value: []byte("3")}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("headers = %s, want %s", got, want)
	}
}
