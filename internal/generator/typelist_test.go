package generator_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// kindOf names the JSON Schema type of a generated value.
func kindOf(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case int64:
		return "integer"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		_ = v
		return "?"
	}
}

// draws generates n values from schema with one seeded generator and counts
// them by kind.
func draws(t *testing.T, schema map[string]any, n int) map[string]int {
	t.Helper()
	gen := generator.New(synth.New(7, fixedNow()))
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		v, err := plainValue(gen, schema)
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		counts[kindOf(v)]++
	}
	return counts
}

// TestTypeListDraw proves a type list draws as an Avro union does (#99
// decision 1): null with 30% chance when listed, otherwise one of the other
// types uniformly; without null, uniformly over the list.
func TestTypeListDraw(t *testing.T) {
	counts := draws(t, map[string]any{"type": []any{"string", "null"}}, 2000)
	if share := float64(counts["null"]) / 2000; share < 0.26 || share > 0.34 {
		t.Errorf("[string, null]: null share %.2f, want about 0.30 (%v)", share, counts)
	}
	if counts["string"]+counts["null"] != 2000 {
		t.Errorf("[string, null]: kinds %v, want strings and nulls only", counts)
	}

	counts = draws(t, map[string]any{"type": []any{"string", "integer", "null"}}, 3000)
	if share := float64(counts["null"]) / 3000; share < 0.26 || share > 0.34 {
		t.Errorf("[string, integer, null]: null share %.2f, want about 0.30 (%v)", share, counts)
	}
	if counts["string"] < 900 || counts["integer"] < 900 {
		t.Errorf("[string, integer, null]: %v, want strings and integers about equally", counts)
	}

	counts = draws(t, map[string]any{"type": []any{"boolean", "integer"}}, 2000)
	if counts["null"] != 0 || counts["boolean"] < 900 || counts["integer"] < 900 {
		t.Errorf("[boolean, integer]: %v, want both about equally and no null", counts)
	}
}

// TestTypeListOfOneType proves a one-type list generates exactly as the plain
// type, drawing nothing more from the seeded stream (#99 decision 3).
func TestTypeListOfOneType(t *testing.T) {
	sequence := func(schema map[string]any) []any {
		gen := generator.New(synth.New(3, fixedNow()))
		var out []any
		for i := 0; i < 50; i++ {
			v, err := plainValue(gen, schema)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, v)
		}
		return out
	}
	plain := map[string]any{"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "integer"}, "s": map[string]any{"type": "string"}}}
	listed := map[string]any{"type": []any{"object"}, "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": []any{"integer"}}, "s": map[string]any{"type": []any{"string"}}}}
	if !reflect.DeepEqual(sequence(plain), sequence(listed)) {
		t.Error("a one-type list must generate exactly as the plain type")
	}
}

// TestTypeListKeywordsPerDraw proves each draw honours its own type's
// keywords and ignores the others', so every value conforms (#99 decision 2).
func TestTypeListKeywordsPerDraw(t *testing.T) {
	schema := map[string]any{
		"type":      []any{"string", "integer", "number", "boolean", "object", "array", "null"},
		"minLength": float64(3), "maxLength": float64(8), "pattern": "^[a-z]{3,8}$",
		"minimum": float64(5), "maximum": float64(9),
		"required":   []any{"id"},
		"properties": map[string]any{"id": map[string]any{"type": []any{"string", "null"}, "format": "uuid"}},
		"items":      map[string]any{"type": []any{"integer", "null"}, "minimum": float64(1), "maximum": float64(2)},
		"minItems":   float64(1),
	}
	gen := generator.New(synth.New(11, fixedNow()))
	kinds := map[string]bool{}
	for i := 0; i < 500; i++ {
		v, err := plainValue(gen, schema)
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		kinds[kindOf(v)] = true
		if err := generator.Conforms(schema, v); err != nil {
			t.Fatalf("draw %d (%v) does not conform: %v", i, v, err)
		}
	}
	if len(kinds) != 7 {
		t.Errorf("kinds drawn %v, want all seven", kinds)
	}
}

// TestTypeListRecursionEndsInNull proves a recursion that exhausts the depth
// budget ends in null when the type list allows it, keeping the required
// field and so Conformance (#99 decision 4).
func TestTypeListRecursionEndsInNull(t *testing.T) {
	schema := map[string]any{
		"$ref": "#/$defs/Node",
		"$defs": map[string]any{"Node": map[string]any{
			"type": []any{"object", "null"}, "required": []any{"next"},
			"properties": map[string]any{"next": map[string]any{"$ref": "#/$defs/Node"}},
		}},
	}
	gen := generator.New(synth.New(1, fixedNow()))
	for i := 0; i < 50; i++ {
		v, err := plainValue(gen, schema)
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		if err := generator.Conforms(schema, v); err != nil {
			t.Fatalf("value %v does not conform: %v", v, err)
		}
	}
}

// TestTypeListAllOfIntersects proves allOf keeps the types every branch
// allows, and refuses branches that allow none in common (#99 decision 6).
func TestTypeListAllOfIntersects(t *testing.T) {
	counts := draws(t, map[string]any{"allOf": []any{
		map[string]any{"type": []any{"string", "null"}},
		map[string]any{"type": []any{"integer", "string"}, "minLength": float64(2)},
	}}, 200)
	if counts["string"] != 200 {
		t.Errorf("allOf of [string, null] and [integer, string]: %v, want strings only", counts)
	}
	gen := generator.New(synth.New(1, fixedNow()))
	_, err := plainValue(gen, map[string]any{"allOf": []any{map[string]any{"type": []any{"string", "null"}}, map[string]any{"type": "integer"}}})
	assertUnsupported(t, err, "allOf", generator.RootPath)
}

// TestTypeListMalformed proves a type list the generator cannot read stops
// the run naming the path (#99 decision 7).
func TestTypeListMalformed(t *testing.T) {
	for name, types := range map[string][]any{
		"empty":      {},
		"repeated":   {"string", "string"},
		"unknown":    {"string", "widget"},
		"not a name": {"string", float64(7)},
	} {
		t.Run(name, func(t *testing.T) {
			gen := generator.New(synth.New(1, fixedNow()))
			_, err := plainValue(gen, map[string]any{"type": "object", "required": []any{"f"}, "properties": map[string]any{"f": map[string]any{"type": types}}})
			assertUnsupported(t, err, "type", "$.f")
		})
	}
}

// TestTypeListPlanting proves -keyPath and a Topic parameter location accept
// a nullable field at the end of the path, and refuse one in the middle,
// which may be null with nothing to plant into (#99 decision 5).
func TestTypeListPlanting(t *testing.T) {
	payload := map[string]any{
		"type": "object", "required": []any{"id", "amount", "customer", "strict"},
		"properties": map[string]any{
			"id":       map[string]any{"type": []any{"string", "null"}},
			"amount":   map[string]any{"type": []any{"number", "null"}},
			"strict":   map[string]any{"type": "string"},
			"customer": map[string]any{"type": []any{"object", "null"}, "required": []any{"cid"}, "properties": map[string]any{"cid": map[string]any{"type": "string"}}},
		},
	}
	nullableKey := map[string]any{"type": []any{"string", "null"}}
	for _, c := range []struct {
		name, path string
		key        map[string]any
		want       string
	}{
		{"string Key at a nullable end", "id", stringKey(), ""},
		{"integer Key into a nullable number", "amount", map[string]any{"type": "integer"}, ""},
		{"nullable Key into a nullable field", "id", nullableKey, ""},
		{"nullable Key into a field without null", "strict", nullableKey, "the key schema may be null, but the schema here is a string"},
		{"through a nullable object", "customer.cid", stringKey(), "may be null, with nothing to plant into"},
		{"a type the field does not allow", "amount", stringKey(), "the schema here is one of number, null but the key schema is a string"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkPath(t, payload, c.key, c.path)
			switch {
			case c.want == "" && err != nil:
				t.Errorf("Check: %v, want the path accepted", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Errorf("Check: %v, want it to mention %q", err, c.want)
			}
		})
	}

	if _, field, err := locate(payload, []string{"id"}); err != nil || generator.Conforms(field, "eu") != nil {
		t.Errorf("Locate at a nullable end: %v, want the field accepting a string", err)
	}
	if _, _, err := locate(payload, []string{"customer", "cid"}); err == nil || !strings.Contains(err.Error(), "may be null") {
		t.Errorf("Locate through a nullable object: %v, want it refused", err)
	}
}
