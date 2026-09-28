package generator

import (
	"reflect"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// withCustomer is a Payload schema whose required customer object carries
// extra keywords, such as a literal the generator returns as-is.
func withCustomer(extra map[string]any) map[string]any {
	customer := map[string]any{
		"type": "object", "required": []any{"id"},
		"properties": map[string]any{"id": map[string]any{"type": "string"}},
	}
	for k, v := range extra {
		customer[k] = v
	}
	return map[string]any{
		"type": "object", "required": []any{"customer"},
		"properties": map[string]any{"customer": customer},
	}
}

// TestPlantingRefusesStepsIntoLiterals proves a path that steps into a schema
// Generator.value answers with a literal is refused, by the Key path check and
// by Locate alike, naming the keyword: the literal need not hold the next step
// (#107).
func TestPlantingRefusesStepsIntoLiterals(t *testing.T) {
	cases := map[string]struct {
		schema map[string]any
		want   string
	}{
		"example":  {withCustomer(map[string]any{"example": map[string]any{"name": "Acme"}}), "declares an example"},
		"examples": {withCustomer(map[string]any{"examples": []any{map[string]any{"name": "Acme"}}}), "declares examples"},
		"const":    {withCustomer(map[string]any{"const": map[string]any{"id": "c1"}}), "declares a const"},
		"enum":     {withCustomer(map[string]any{"enum": []any{map[string]any{"id": "c1"}}}), "declares an enum"},
		"behind a $ref": {map[string]any{
			"type": "object", "required": []any{"customer"},
			"properties": map[string]any{"customer": map[string]any{"$ref": "#/$defs/Customer"}},
			"$defs":      map[string]any{"Customer": withCustomer(map[string]any{"const": map[string]any{"id": "c1"}})["properties"].(map[string]any)["customer"]},
		}, "declares a const"},
		"from an allOf branch": {withCustomer(map[string]any{"allOf": []any{
			map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
			map[string]any{"example": map[string]any{"name": "Acme"}},
		}}), "declares an example"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := checkPath(t, c.schema, stringKey(), "customer.id"); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Check: %v, want it to mention %q", err, c.want)
			}
			if _, _, err := locate(c.schema, []string{"customer", "id"}); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Locate: %v, want it to mention %q", err, c.want)
			}
		})
	}
}

// TestPlantingIgnoresWhatGenerationIgnores proves the walk judges a schema as
// Generator.value reads it: empty examples and an empty enum are no literal,
// and an allOf is merged before any sibling oneOf is looked at.
func TestPlantingIgnoresWhatGenerationIgnores(t *testing.T) {
	for name, extra := range map[string]map[string]any{
		"empty examples": {"examples": []any{}},
		"empty enum":     {"enum": []any{}},
		"allOf beside a oneOf": {"allOf": []any{
			map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
		}, "oneOf": []any{map[string]any{"type": "string"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkPath(t, withCustomer(extra), stringKey(), "customer.id"); err != nil {
				t.Errorf("Check: %v, want the path accepted", err)
			}
		})
	}
}

// TestPlantingOverLiteralsAtThePathEnd proves a literal at the end of a path
// is accepted: the planted value replaces it, so only whether the field can
// hold that value matters (#107).
func TestPlantingOverLiteralsAtThePathEnd(t *testing.T) {
	schema := map[string]any{
		"type": "object", "required": []any{"region", "note"},
		"properties": map[string]any{
			"region": map[string]any{"type": "string", "enum": []any{"eu", "us"}},
			"note":   map[string]any{"type": "string", "example": "hello"},
		},
	}
	if err := checkPath(t, schema, stringKey(), "note"); err != nil {
		t.Errorf("Check(note): %v, want an example at the end accepted", err)
	}
	_, field, err := locate(schema, []string{"region"})
	if err != nil {
		t.Fatalf("locate(region): %v, want an enum at the end accepted", err)
	}
	if Conforms(field, "eu") != nil || Conforms(field, "asia") == nil {
		t.Errorf("locate(region) field = %v, want it to hold eu and refuse asia", field)
	}
}

// TestValueReturnsItsOwnLiteral proves a literal comes back as a copy, so
// planting into one record's value reaches neither the schema nor another
// record (#107).
func TestValueReturnsItsOwnLiteral(t *testing.T) {
	for name, schema := range map[string]map[string]any{
		"example":  {"type": "object", "example": map[string]any{"id": "x", "tags": []any{"a"}}},
		"examples": {"type": "object", "examples": []any{map[string]any{"id": "x", "tags": []any{"a"}}}},
		"const":    {"type": "object", "const": map[string]any{"id": "x", "tags": []any{"a"}}},
		"enum":     {"type": "object", "enum": []any{map[string]any{"id": "x", "tags": []any{"a"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			want := map[string]any{"id": "x", "tags": []any{"a"}}
			gen := New(synth.New(1, fixedNow()))
			first, err := gen.Value(schema)
			if err != nil {
				t.Fatal(err)
			}
			first.(map[string]any)["id"] = "planted"
			first.(map[string]any)["tags"].([]any)[0] = "planted"
			second, err := gen.Value(schema)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(second, want) {
				t.Errorf("second value = %v, want %v untouched by planting into the first", second, want)
			}
		})
	}
}
