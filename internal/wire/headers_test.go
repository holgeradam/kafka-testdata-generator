package wire

import (
	"reflect"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
)

// TestEncodeHeaders proves each property becomes one Kafka record header, in
// the order the object has them, its value plain-scalar as a JSON Key is, an
// object as its JSON text in its own order, null as a null header (#85
// decision 2).
func TestEncodeHeaders(t *testing.T) {
	var origin, obj ordered.Object
	origin.Add("zone", "eu")
	origin.Add("az", 2.0)
	for _, kv := range []struct {
		k string
		v any
	}{{"tenant", "acme"}, {"attempt", float64(3)}, {"ratio", 0.5}, {"retry", true}, {"trace", nil}, {"tags", []any{"a", "b"}}, {"origin", origin}} {
		obj.Add(kv.k, kv.v)
	}
	got, err := EncodeHeaders(obj)
	if err != nil {
		t.Fatal(err)
	}
	want := []pipeline.Header{
		{Name: "tenant", Value: []byte("acme")},
		{Name: "attempt", Value: []byte("3")},
		{Name: "ratio", Value: []byte("0.5")},
		{Name: "retry", Value: []byte("true")},
		{Name: "trace", Value: nil},
		{Name: "tags", Value: []byte(`["a","b"]`)},
		{Name: "origin", Value: []byte(`{"zone":"eu","az":2}`)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("headers = %v, want %v", got, want)
	}
}
