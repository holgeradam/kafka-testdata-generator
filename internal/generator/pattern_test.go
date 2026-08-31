package generator

import (
	"errors"
	"testing"
	"time"
)

// mustValidate is the pattern-schema shim over the single validator wrapper
// mustConform (ADR-0006 Decision 3): it validates value against a string schema
// with the given pattern, failing the test if it does not conform.
func mustValidate(t *testing.T, pattern string, value any) {
	t.Helper()
	mustConform(t, map[string]any{
		"type":    "string",
		"pattern": pattern,
	}, value)
}

// TestPatternSupportProperty drives the documented subset through the real
// Value pipeline and validates every generated payload against its pattern
// schema. Each supported construct is covered by at least one fixture.
func TestPatternSupportProperty(t *testing.T) {
	fixtures := []struct {
		name    string
		pattern string
	}{
		{"literal", `^AB-C$`},
		{"class-upper", `[A-Z]{4}`},
		{"class-lower", `[a-z]{5}`},
		{"class-digit", `[0-9]{3}`},
		{"class-mixed-ranges", `[A-Za-z0-9]{3}`},
		{"escape-d", `\d{4}`},
		{"escape-w", `\w{5}`},
		{"escape-s", `a\s+b`},
		{"quant-exact", `[A-Z]{3}`},
		{"quant-range", `[0-9]{2,5}`},
		{"quant-star", `ab*c`},
		{"quant-plus", `a+`},
		{"quant-optional", `colou?r`},
		{"group", `(ab)+c`},
		{"alternation", `^(cat|dog)$`},
		{"combined-sku", `^[A-Z]{3}-[A-Z]{2}-\d{4}$`},
	}

	for _, fx := range fixtures {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			gen := New(42, fixedNow())
			schema := map[string]any{
				"type":    "string",
				"pattern": fx.pattern,
			}
			for i := 0; i < 50; i++ {
				v, err := gen.Value(schema, RootField)
				if err != nil {
					t.Fatalf("iteration %d: Value error: %v", i, err)
				}
				s, ok := v.(string)
				if !ok {
					t.Fatalf("iteration %d: expected string, got %T", i, v)
				}
				mustValidate(t, fx.pattern, s)
			}
		})
	}
}

// TestPatternUnsupportedError asserts that constructs outside the documented
// subset surface an *UnsupportedPatternError naming the construct, rather than
// a silently-nonconforming string.
func TestPatternUnsupportedError(t *testing.T) {
	fixtures := []struct {
		name      string
		pattern   string
		construct string
	}{
		{"dot-any", `a.c`, `.`},
		{"negated-class", `[^a-z]{2}`, `[^a-z]`},
		{"escape-D", `\D{2}`, `\D`},
		{"escape-b", `\bword\b`, `\b`},
		{"unbounded-range", `[0-9]{2,}`, `{2,}`},
		{"descending-range", `[0-9]{3,1}`, `{3,1}`},
	}

	for _, fx := range fixtures {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			gen := New(42, fixedNow())
			schema := map[string]any{
				"type":    "string",
				"pattern": fx.pattern,
			}
			_, err := gen.Value(schema, RootField)
			if err == nil {
				t.Fatalf("expected error for unsupported pattern %q", fx.pattern)
			}
			var pe *UnsupportedPatternError
			if !errors.As(err, &pe) {
				t.Fatalf("expected *UnsupportedPatternError, got %T (%v)", err, err)
			}
			if pe.Pattern != fx.pattern {
				t.Errorf("Pattern = %q, want %q", pe.Pattern, fx.pattern)
			}
			if pe.Construct != fx.construct {
				t.Errorf("Construct = %q, want %q", pe.Construct, fx.construct)
			}
		})
	}
}

// TestPatternDeterministic asserts a fixed (seed, now) yields an identical
// synthesized string for the same pattern across repeated generation.
func TestPatternDeterministic(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pattern := `^[A-Z]{3}-[A-Z]{2}-\d{4}$`
	schema := map[string]any{"type": "string", "pattern": pattern}

	gen1 := New(99, now)
	gen2 := New(99, now)

	a, err := gen1.Value(schema, RootField)
	if err != nil {
		t.Fatalf("gen1.Value error: %v", err)
	}
	b, err := gen2.Value(schema, RootField)
	if err != nil {
		t.Fatalf("gen2.Value error: %v", err)
	}
	if a != b {
		t.Errorf("pattern output differs for same (seed, now): %q vs %q", a, b)
	}
}
