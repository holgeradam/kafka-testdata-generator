package asyncapi

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
)

// orderIn is the property order recorded on a schema object.
func orderIn(schema map[string]any) any {
	return schema[generator.OrderKeyword]
}

// TestPropertyOrderRecorded proves the reader records the order a spec
// writes each schema's properties in, at every level and through $refs, for
// the Payload, the Key binding and the Headers (#96).
func TestPropertyOrderRecorded(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {key: {type: object, properties: {tenant: {type: string}, id: {type: string}}}}}
        headers: {type: object, properties: {zone: {type: string}, attempt: {type: integer}}}
        payload:
          type: object
          properties:
            zeta: {type: string}
            billing: {$ref: '#/components/schemas/Address'}
            alpha: {type: string}
components:
  schemas:
    Address: {type: object, properties: {street: {type: string}, city: {type: string}}}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]struct {
		schema map[string]any
		want   []any
	}{
		"payload":              {mt.Payload, []any{"zeta", "billing", "alpha"}},
		"payload through $ref": {mt.Payload["properties"].(map[string]any)["billing"].(map[string]any), []any{"street", "city"}},
		"key":                  {mt.KeyBinding, []any{"tenant", "id"}},
		"headers":              {mt.Headers, []any{"zone", "attempt"}},
	}
	for name, c := range checks {
		if got := orderIn(c.schema); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: order %v, want %v", name, got, c.want)
		}
	}
}

// TestPropertyOrderRecordedFromJSON proves a JSON spec records its order too.
func TestPropertyOrderRecordedFromJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spec.json")
	spec := `{"asyncapi":"2.6.0","info":{"title":"T","version":"1"},"channels":{"orders":{"publish":{"message":{"payload":{"type":"object","properties":{"zeta":{"type":"string"},"alpha":{"type":"string"}}}}}}}}`
	if err := os.WriteFile(path, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}
	doc, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := orderIn(mt.Payload), []any{"zeta", "alpha"}; !reflect.DeepEqual(got, want) {
		t.Errorf("order %v, want %v", got, want)
	}
}

// TestPropertyOrderKeptOutOfAvsc proves the recorded order never reaches an
// avsc, which registers exactly as declared.
func TestPropertyOrderKeptOutOfAvsc(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {type: record, name: R, properties: {b: 1, a: 2}, fields: [{name: id, type: string}]}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mt.Avsc), generator.OrderKeyword) {
		t.Errorf("avsc %s carries the recorded order", mt.Avsc)
	}
}

// withoutOrder is a schema without the property order the reader records,
// for tests about everything else a schema holds.
func withoutOrder(schema map[string]any) map[string]any {
	var strip func(v any) any
	strip = func(v any) any {
		switch v := v.(type) {
		case map[string]any:
			out := make(map[string]any, len(v))
			for k, e := range v {
				if k != generator.OrderKeyword {
					out[k] = strip(e)
				}
			}
			return out
		case []any:
			out := make([]any, len(v))
			for i, e := range v {
				out[i] = strip(e)
			}
			return out
		}
		return v
	}
	return strip(schema).(map[string]any)
}
