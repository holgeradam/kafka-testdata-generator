package asyncapi_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
)

// orderIn is the property order recorded on a schema object.
func orderIn(schema map[string]any) any {
	return schema[ordered.Keyword]
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
		"payload":              {jsonPayload(mt), []any{"zeta", "billing", "alpha"}},
		"payload through $ref": {jsonPayload(mt)["properties"].(map[string]any)["billing"].(map[string]any), []any{"street", "city"}},
		"key":                  {jsonKey(mt), []any{"tenant", "id"}},
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
	doc, err := asyncapi.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := orderIn(jsonPayload(mt)), []any{"zeta", "alpha"}; !reflect.DeepEqual(got, want) {
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
	if strings.Contains(string(avroPayload(mt)), ordered.Keyword) {
		t.Errorf("avsc %s carries the recorded order", avroPayload(mt))
	}
}

// tree builds an order tree: an Object of the keys given, each with its
// subtree, as the reader records a literal's written order.
func tree(kv ...any) ordered.Object {
	var o ordered.Object
	for i := 0; i < len(kv); i += 2 {
		o.Add(kv[i].(string), kv[i+1])
	}
	return o
}

// TestLiteralOrderRecorded proves the reader records the order each literal
// the generator returns as-is is written in - an example, examples, a const,
// an enum member - as an order tree beside it, nested objects and array items
// included, and records nothing for a literal without an object (#112).
func TestLiteralOrderRecorded(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            customer: {type: object, example: {name: Acme, id: 7, address: {street: Main, city: Oslo}}}
            tags: {type: array, examples: [[{b: 1, a: 2}], []]}
            fixed: {const: {z: 1, y: 2}}
            choice: {enum: [{q: 1, p: 2}, plain]}
            note: {type: string, example: hello}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	props := jsonPayload(mt)["properties"].(map[string]any)
	literalOrder := func(name string) any { return props[name].(map[string]any)[ordered.LiteralKeyword] }
	checks := map[string]any{
		"customer": map[string]any{"example": tree("name", nil, "id", nil, "address", tree("street", nil, "city", nil))},
		"tags":     map[string]any{"examples": []any{[]any{tree("b", nil, "a", nil)}, []any{}}},
		"fixed":    map[string]any{"const": tree("z", nil, "y", nil)},
		"choice":   map[string]any{"enum": []any{tree("q", nil, "p", nil), nil}},
		"note":     nil,
	}
	for name, want := range checks {
		if got := literalOrder(name); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: literal order %#v, want %#v", name, got, want)
		}
	}
}

// TestLiteralOrderKeptOutOfAvsc proves no literal order reaches an avsc.
func TestLiteralOrderKeptOutOfAvsc(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {type: record, name: R, fields: [{name: id, type: string, default: x, example: {b: 1, a: 2}}]}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(avroPayload(mt)), ordered.LiteralKeyword) {
		t.Errorf("avsc %s carries a recorded literal order", avroPayload(mt))
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
				if k != ordered.Keyword && k != ordered.LiteralKeyword {
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

// TestOrderStampedOnSchemaObjectsOnly proves the recorded orders sit on
// schema objects only: a property named like a keyword - properties,
// example - gets no stamp in the properties object, which would read as a
// property of its own, while its own schema is stamped as any other.
func TestOrderStampedOnSchemaObjectsOnly(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            properties: {type: object, properties: {b: {type: string}, a: {type: string}}}
            example: {type: object, properties: {d: {type: string}, c: {type: string}}, example: {d: x, c: y}}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	props := jsonPayload(mt)["properties"].(map[string]any)
	if len(props) != 2 {
		t.Fatalf("properties = %v, want exactly properties and example", props)
	}
	if got, want := orderIn(props["properties"].(map[string]any)), []any{"b", "a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("property properties: order %v, want %v", got, want)
	}
	example := props["example"].(map[string]any)
	if got, want := orderIn(example), []any{"d", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("property example: order %v, want %v", got, want)
	}
	if got, want := example[ordered.LiteralKeyword], map[string]any{"example": tree("d", nil, "c", nil)}; !reflect.DeepEqual(got, want) {
		t.Errorf("property example: literal order %v, want %v", got, want)
	}
}
