package generator

import (
	"errors"
	"testing"
)

// assertUnsupported asserts err is an *UnsupportedSchemaError with the given
// keyword and exact Path. Each Value-level test lands on the same error shape,
// so the assertion lives once here rather than repeated per case.
func assertUnsupported(t *testing.T, err error, keyword, path string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var ue *UnsupportedSchemaError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UnsupportedSchemaError, got %T (%v)", err, err)
	}
	if ue.Keyword != keyword {
		t.Errorf("keyword = %q, want %q", ue.Keyword, keyword)
	}
	if ue.Path != path {
		t.Errorf("path = %q, want %q", ue.Path, path)
	}
}

func TestValueUnsupportedType(t *testing.T) {
	gen := New(42, fixedNow())
	_, err := gen.Value(map[string]any{"type": "widget"}, RootField)
	assertUnsupported(t, err, "type", RootField)
}

func TestValueNoType(t *testing.T) {
	gen := New(42, fixedNow())
	_, err := gen.Value(map[string]any{"minimum": 5}, RootField)
	assertUnsupported(t, err, "type", RootField)
}

func TestValueNestedUnsupportedPath(t *testing.T) {
	gen := New(42, fixedNow())
	schema := map[string]any{
		"type":     "object",
		"required": []any{"widget"},
		"properties": map[string]any{
			"widget": map[string]any{"type": "gadget"},
		},
	}
	_, err := gen.Value(schema, RootField)
	assertUnsupported(t, err, "type", RootField+".widget")
}

func TestValueArrayUnsupportedItemPath(t *testing.T) {
	gen := New(42, fixedNow())
	schema := map[string]any{
		"type":     "array",
		"items":    map[string]any{"type": "thing"},
		"minItems": float64(1),
		"maxItems": float64(3),
	}
	_, err := gen.Value(schema, RootField)
	var ue *UnsupportedSchemaError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UnsupportedSchemaError, got %T (%v)", err, err)
	}
	if ue.Keyword != "type" {
		t.Errorf("keyword = %q, want type", ue.Keyword)
	}
	if ue.Path[:len(RootField)+1] != RootField+"[" {
		t.Errorf("path %q should be an array element under %q", ue.Path, RootField)
	}
}

func TestValueNoPanicOnWeirdButValidShapes(t *testing.T) {
	gen := New(42, fixedNow())
	schemas := []map[string]any{
		{"type": "widget"},
		{"minimum": 5},
		{"type": []any{"string", "integer"}},
		{"type": "object", "properties": map[string]any{"a": "not-a-map"}},
		{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}}},
		{"oneOf": []any{"not-a-map"}},
		{"anyOf": []any{"not-a-map"}},
		{"type": "array", "items": "not-a-map"},
		{"allOf": []any{"not-a-map"}},
		{"type": "null"},
	}
	for i, s := range schemas {
		i := i
		s := s
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("schema[%d] panicked: %v", i, r)
				}
			}()
			_, _ = gen.Value(s, RootField)
		}()
	}
}

func TestValueNonMapPropertySchema(t *testing.T) {
	gen := New(42, fixedNow())
	schema := map[string]any{
		"type":     "object",
		"required": []any{"a"},
		"properties": map[string]any{
			"a": "not-a-schema-object",
		},
	}
	_, err := gen.Value(schema, RootField)
	assertUnsupported(t, err, "properties", RootField+".a")
}

func TestValueRefResolutionError(t *testing.T) {
	gen := New(1, fixedNow())
	gen.SetRefResolver(func(ref string) (map[string]any, error) {
		return nil, errors.New("no such definition")
	})
	_, err := gen.Value(map[string]any{"$ref": "#/defs/Missing"}, RootField)
	assertUnsupported(t, err, "$ref", RootField)
}

func TestValueRefMissingResolver(t *testing.T) {
	gen := New(1, fixedNow())
	_, err := gen.Value(map[string]any{"$ref": "#/defs/Node"}, RootField)
	assertUnsupported(t, err, "$ref", RootField)
}
