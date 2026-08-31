package generator

import (
	"testing"
)

func TestBasicDeterminism(t *testing.T) {
	gen1 := New(42)
	gen2 := New(42)

	for i := 0; i < 10; i++ {
		schema := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{"type": "string"},
				"age":  map[string]any{"type": "integer"},
			},
		}

		r1, err := gen1.Value(schema, RootField)
		if err != nil {
			t.Fatalf("gen1.Value error: %v", err)
		}
		r2, err := gen2.Value(schema, RootField)
		if err != nil {
			t.Fatalf("gen2.Value error: %v", err)
		}

		obj1 := r1.(map[string]any)
		obj2 := r2.(map[string]any)

		if obj1["name"] != obj2["name"] {
			t.Errorf("iteration %d: name differs: %v vs %v", i, obj1["name"], obj2["name"])
		}
		if obj1["age"] != obj2["age"] {
			t.Errorf("iteration %d: age differs: %v vs %v", i, obj1["age"], obj2["age"])
		}
	}
}
