package generator

import (
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// TestLocateGuaranteedFields proves a JSON Pointer into the Payload becomes
// the steps generation guarantees, a numeric token indexing an array, and
// returns the schema of the field there.
func TestLocateGuaranteedFields(t *testing.T) {
	cases := []struct {
		pointer []string
		steps   string
		typ     string
	}{
		{[]string{"id"}, "id", "string"},
		{[]string{"customer", "id"}, "customer.id", "string"},
		{[]string{"items", "1", "sku"}, "items[1].sku", "string"},
	}
	for _, c := range cases {
		steps, field, err := Locate(orderSchema(), c.pointer)
		if err != nil {
			t.Fatalf("Locate(%v): %v", c.pointer, err)
		}
		if got := keyplan.PathString(steps); got != c.steps {
			t.Errorf("Locate(%v) steps = %s, want %s", c.pointer, got, c.steps)
		}
		if field["type"] != c.typ {
			t.Errorf("Locate(%v) field = %v, want a %s", c.pointer, field, c.typ)
		}
	}
}

// TestLocateRefusesUnguaranteedFields proves a location generation does not
// fill in every record is refused, naming the failing step as a pointer.
func TestLocateRefusesUnguaranteedFields(t *testing.T) {
	cases := map[string]struct {
		pointer []string
		want    string
	}{
		"optional":       {[]string{"note"}, `at "/note": property "note" is not required`},
		"nested":         {[]string{"customer", "nickname"}, `at "/customer/nickname": property "nickname" is not required`},
		"beyond min":     {[]string{"items", "2", "sku"}, `at "/items/2": the array has minItems 2`},
		"missing":        {[]string{"nope"}, `at "/nope": the object has no property "nope"`},
		"field in array": {[]string{"items", "sku"}, `at "/items/sku": the schema here is an array, not an object`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := Locate(orderSchema(), c.pointer)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

// TestLocateFieldIsSelfContained proves the returned field schema carries the
// Payload schema's $defs, so a $ref inside it still resolves on its own.
func TestLocateFieldIsSelfContained(t *testing.T) {
	schema := map[string]any{
		"type":     "object",
		"required": []any{"region"},
		"properties": map[string]any{
			"region": map[string]any{"$ref": "#/$defs/Region"},
		},
		"$defs": map[string]any{
			"Region": map[string]any{"type": "object", "required": []any{"code"}, "properties": map[string]any{"code": map[string]any{"$ref": "#/$defs/Code"}}},
			"Code":   map[string]any{"type": "string", "pattern": "^[a-z]{2}$"},
		},
	}
	_, field, err := Locate(schema, []string{"region", "code"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(field["$defs"], schema["$defs"]) {
		t.Errorf("field = %v, want the Payload schema's $defs attached", field)
	}
	if err := Conforms(field, "eu"); err != nil {
		t.Errorf("Conforms(eu) = %v, want nil", err)
	}
	if err := Conforms(field, "EU"); err == nil {
		t.Error("Conforms(EU) = nil, want the pattern refused")
	}
}

// TestConforms proves the value check honours the constraints a Payload field
// declares, format included, and says which it breaks.
func TestConforms(t *testing.T) {
	cases := []struct {
		schema map[string]any
		value  string
		want   string // "" when the value conforms
	}{
		{map[string]any{"type": "string"}, "eu", ""},
		{map[string]any{"type": "string", "enum": []any{"eu", "us"}}, "eu", ""},
		{map[string]any{"type": "string", "enum": []any{"eu", "us"}}, "apac", "value must be one of"},
		{map[string]any{"type": "string", "pattern": "^[a-z]{2}$"}, "EU", "does not match pattern"},
		{map[string]any{"type": "string", "maxLength": float64(2)}, "apac", "length must be <= 2"},
		{map[string]any{"type": "string", "format": "uuid"}, "eu", "is not valid 'uuid'"},
		{map[string]any{"type": "integer"}, "7", "expected integer, but got string"},
		{map[string]any{"const": "eu"}, "us", "value must be"},
	}
	for _, c := range cases {
		err := Conforms(c.schema, c.value)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("Conforms(%v, %q) = %v, want nil", c.schema, c.value, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("Conforms(%v, %q) = %v, want it to mention %q", c.schema, c.value, err, c.want)
		}
	}
}
