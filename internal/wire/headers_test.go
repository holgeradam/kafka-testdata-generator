package wire

import (
	"reflect"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
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
