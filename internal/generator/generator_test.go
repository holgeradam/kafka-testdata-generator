package generator

import (
	"encoding/json"
	"testing"
)

func TestGenerateObject(t *testing.T) {
	gen := New(42)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer", "minimum": 0, "maximum": 120},
		},
		"required": []any{"name"},
	}

	result, _ := gen.Value(schema, RootField)
	obj, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	if _, ok := obj["name"]; !ok {
		t.Error("required field 'name' not generated")
	}

	if age, ok := obj["age"]; ok {
		ageNum, ok := age.(int64)
		if !ok {
			t.Errorf("age should be an int64, got %T", age)
		} else if ageNum < 0 || ageNum > 120 {
			t.Errorf("age = %v, want 0-120", ageNum)
		}
	}
}

func TestGenerateArray(t *testing.T) {
	gen := New(42)
	schema := map[string]any{
		"type":     "array",
		"items":    map[string]any{"type": "string"},
		"minItems": float64(2),
		"maxItems": float64(5),
	}

	result, _ := gen.Value(schema, RootField)
	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("expected array, got %T", result)
	}

	if len(arr) < 2 || len(arr) > 5 {
		t.Errorf("array length = %d, want 2-5", len(arr))
	}

	for i, item := range arr {
		if _, ok := item.(string); !ok {
			t.Errorf("item[%d] should be string, got %T", i, item)
		}
	}
}

func TestGenerateStringFormats(t *testing.T) {
	gen := New(42)

	tests := []struct {
		format string
		name   string
	}{
		{"uuid", "UUID"},
		{"email", "Email"},
		{"date-time", "DateTime"},
		{"date", "Date"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := map[string]any{
				"type":   "string",
				"format": tt.format,
			}
			result, _ := gen.Value(schema, RootField)
			if _, ok := result.(string); !ok {
				t.Errorf("expected string, got %T", result)
			}
		})
	}
}

func TestGenerateIntegerBounds(t *testing.T) {
	gen := New(42)
	schema := map[string]any{
		"type":    "integer",
		"minimum": float64(10),
		"maximum": float64(20),
	}

	for i := 0; i < 100; i++ {
		result, _ := gen.Value(schema, RootField)
		num, ok := result.(int64)
		if !ok {
			t.Fatalf("expected int64, got %T", result)
		}
		if num < 10 || num > 20 {
			t.Errorf("value = %d, want 10-20", num)
		}
	}
}

func TestGenerateEnum(t *testing.T) {
	gen := New(42)
	schema := map[string]any{
		"type": "string",
		"enum": []any{"a", "b", "c"},
	}

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		result, _ := gen.Value(schema, RootField)
		str, ok := result.(string)
		if !ok {
			t.Fatalf("expected string, got %T", result)
		}
		seen[str] = true
	}

	for _, v := range []string{"a", "b", "c"} {
		if !seen[v] {
			t.Errorf("enum value %q not generated in 50 iterations", v)
		}
	}
}

func TestDeterministic(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}

	gen1 := New(12345)
	gen2 := New(12345)

	for i := 0; i < 10; i++ {
		r1, _ := gen1.Value(schema, RootField)
		r2, _ := gen2.Value(schema, RootField)

		j1, _ := json.Marshal(r1)
		j2, _ := json.Marshal(r2)

		if string(j1) != string(j2) {
			t.Errorf("iteration %d: not deterministic\n  gen1: %s\n  gen2: %s", i, j1, j2)
		}
	}
}
