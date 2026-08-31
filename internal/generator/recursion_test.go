package generator

import (
	"encoding/json"
	"fmt"
	"testing"
)

// selfRefSchema returns a schema map and resolver for a self-referential Node
// with a required "value" and an optional "child" that points back at Node.
func selfRefSchema() (map[string]any, func(string) (map[string]any, error)) {
	node := map[string]any{
		"type":     "object",
		"required": []any{"value"},
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
			"child": map[string]any{"$ref": "#/defs/Node"},
		},
	}
	resolver := func(ref string) (map[string]any, error) {
		if ref == "#/defs/Node" {
			return node, nil
		}
		return nil, fmt.Errorf("unknown ref %s", ref)
	}
	return node, resolver
}

// linkDepth returns how many "child" hops deep a generated recursive object
// linkDepth walks the self-referential "child" chain and returns the deepest
// node and how many hops it sits from the root.
func linkDepth(root any) (deepest map[string]any, depth int) {
	cur, ok := root.(map[string]any)
	for ok {
		child, hasChild := cur["child"].(map[string]any)
		if !hasChild {
			return cur, depth
		}
		depth++
		cur = child
	}
	return cur, depth
}

func TestValueRecursiveTerminates(t *testing.T) {
	root := map[string]any{"$ref": "#/defs/Node"}
	gen := New(42)
	_, resolver := selfRefSchema()
	gen.SetRefResolver(resolver)

	result := gen.Value(root, "")

	deepest, depth := linkDepth(result)
	if _, present := deepest["child"]; present {
		t.Error("deepest node should not carry a child (budget exhaustion)")
	}
	if depth == 0 {
		t.Error("expected recursion to produce nested children")
	}
	if depth > maxRecursionDepth {
		t.Errorf("link depth %d exceeds budget %d", depth, maxRecursionDepth)
	}
}

func TestValueRecursiveBudgetExhaustionSkippedField(t *testing.T) {
	gen := New(7)
	_, resolver := selfRefSchema()
	gen.SetRefResolver(resolver)

	result := gen.Value(map[string]any{"$ref": "#/defs/Node"}, "")

	// The deepest node must omit the exhausted child field.
	deepest, _ := linkDepth(result)
	if _, present := deepest["child"]; present {
		t.Error("deepest node should skip the exhausted child field")
	}
}

func TestValueRecursiveDeterministic(t *testing.T) {
	root := map[string]any{"$ref": "#/defs/Node"}

	gen1 := New(99)
	_, r1r := selfRefSchema()
	gen1.SetRefResolver(r1r)
	gen2 := New(99)
	_, r2r := selfRefSchema()
	gen2.SetRefResolver(r2r)

	for i := 0; i < 10; i++ {
		r1 := gen1.Value(root, "")
		r2 := gen2.Value(root, "")

		j1, _ := json.Marshal(r1)
		j2, _ := json.Marshal(r2)
		if string(j1) != string(j2) {
			t.Fatalf("iteration %d not deterministic:\n  gen1: %s\n  gen2: %s", i, j1, j2)
		}
	}
}

// TestValueRecursiveArrayEmpties covers an array of self-referential items:
// at budget exhaustion the deepest array becomes empty rather than growing.
func TestValueRecursiveArrayEmpties(t *testing.T) {
	node := map[string]any{
		"type":     "object",
		"required": []any{"children"},
		"properties": map[string]any{
			"children": map[string]any{
				"type":     "array",
				"minItems": float64(1),
				"maxItems": float64(2),
				"items":    map[string]any{"$ref": "#/defs/Node"},
			},
		},
	}
	gen := New(3)
	gen.SetRefResolver(func(ref string) (map[string]any, error) {
		if ref == "#/defs/Node" {
			return node, nil
		}
		return nil, fmt.Errorf("unknown ref %s", ref)
	})

	result := gen.Value(map[string]any{"$ref": "#/defs/Node"}, "")

	// Descend through first-child chains; the deepest array must be empty.
	cur := result.(map[string]any)
	seenEmpty := false
	for depth := 0; depth < maxRecursionDepth+2; depth++ {
		children, _ := cur["children"].([]any)
		if len(children) == 0 {
			seenEmpty = true
			break
		}
		first, ok := children[0].(map[string]any)
		if !ok {
			break
		}
		cur = first
	}
	if !seenEmpty {
		t.Error("expected an empty children array at budget exhaustion")
	}
}
