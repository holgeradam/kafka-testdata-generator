package generator

import (
	"testing"
)

// TestAllOfProperty drives several allOf schemas through the real Value
// pipeline and validates every generated payload against the full allOf schema.
// Each fixture isolates one merge rule from ADR-0006 Decision 5: bounds
// intersect without a later constraint silently overriding an earlier one,
// properties merge recursively, and required unions.
func TestAllOfProperty(t *testing.T) {
	stringType := map[string]any{"type": "string"}
	intType := map[string]any{"type": "integer"}

	fixtures := []struct {
		name   string
		schema map[string]any
	}{
		{
			// bounds intersect: max of minimums, min of maximums. The later
			// branch's relaxed minimum must not beat the earlier branch's tighter
			// one, so every payload sits in [20,50], not [10,50].
			name: "bounds-no-silent-override",
			schema: map[string]any{
				"allOf": []any{
					map[string]any{"type": "integer", "minimum": 20, "maximum": 100},
					map[string]any{"type": "integer", "minimum": 10, "maximum": 50},
				},
			},
		},
		{
			// properties merge recursively: x's constraints accumulate across
			// branches; y exists only in the second branch and is still present.
			name: "recursive-properties",
			schema: map[string]any{
				"allOf": []any{
					map[string]any{"type": "object", "properties": map[string]any{
						"x": map[string]any{"type": "integer", "minimum": 5},
					}},
					map[string]any{"type": "object", "properties": map[string]any{
						"x": map[string]any{"type": "integer", "maximum": 10},
						"y": stringType,
					}},
				},
			},
		},
		{
			// required unions: a and b are each required by only one branch; the
			// merged schema must require both.
			name: "required-union",
			schema: map[string]any{
				"allOf": []any{
					map[string]any{"type": "object", "required": []any{"a"}, "properties": map[string]any{
						"a": intType,
					}},
					map[string]any{"type": "object", "required": []any{"b"}, "properties": map[string]any{
						"b": stringType,
					}},
				},
			},
		},
	}

	for _, fx := range fixtures {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			gen := New(42, fixedNow())
			for i := 0; i < 50; i++ {
				v, err := gen.Value(fx.schema)
				if err != nil {
					t.Fatalf("iteration %d: Value error: %v", i, err)
				}
				mustConform(t, fx.schema, v)
			}
		})
	}
}

// TestAllOfConflict asserts that irreconcilable allOf branches - distinct const
// values or clashing type - surface a typed *UnsupportedSchemaError naming the
// allOf keyword, rather than silently producing one branch's value.
func TestAllOfConflict(t *testing.T) {
	cases := []struct {
		name   string
		schema map[string]any
	}{
		{
			"distinct-const",
			map[string]any{"allOf": []any{
				map[string]any{"const": "x"},
				map[string]any{"const": "y"},
			}},
		},
		{
			"clashing-type",
			map[string]any{"allOf": []any{
				map[string]any{"type": "string"},
				map[string]any{"type": "integer"},
			}},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gen := New(42, fixedNow())
			_, err := gen.Value(c.schema)
			assertUnsupported(t, err, "allOf", RootPath)
		})
	}
}
