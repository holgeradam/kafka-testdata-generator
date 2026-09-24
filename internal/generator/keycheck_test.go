package generator

import (
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// checkPath is the shim these tests drive: parse a -keyPath, then validate it
// against a Message schema and a key schema exactly as the process edge does.
func checkPath(t *testing.T, payload, key map[string]any, path string) error {
	t.Helper()
	steps, err := keyplan.ParsePath(path)
	if err != nil {
		t.Fatalf("ParsePath(%q): %v", path, err)
	}
	return NewKeyChecker(payload, key).Check(steps)
}

func stringKey() map[string]any { return map[string]any{"type": "string"} }

// orderSchema has a required nested object, a required array with minItems, and
// an optional field, so one fixture covers guaranteed and unguaranteed steps.
func orderSchema() map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []any{"id", "customer", "items"},
		"properties": map[string]any{
			"id": map[string]any{"type": "string"},
			"customer": map[string]any{
				"type":     "object",
				"required": []any{"id"},
				"properties": map[string]any{
					"id":       map[string]any{"type": "string"},
					"nickname": map[string]any{"type": "string"},
				},
			},
			"items": map[string]any{
				"type":     "array",
				"minItems": float64(2),
				"items": map[string]any{
					"type":       "object",
					"required":   []any{"sku"},
					"properties": map[string]any{"sku": map[string]any{"type": "string"}},
				},
			},
			"note": map[string]any{"type": "string"},
		},
	}
}

func TestKeyCheckerAcceptsGuaranteedPaths(t *testing.T) {
	for _, path := range []string{"id", "customer.id", "items[0].sku", "items[1].sku"} {
		if err := checkPath(t, orderSchema(), stringKey(), path); err != nil {
			t.Errorf("Check(%q) = %v, want nil", path, err)
		}
	}
}

func TestKeyCheckerRejectsUnguaranteedPaths(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"note", "not required"},
		{"customer.nickname", "not required"},
		{"items[2].sku", "minItems"},
		{"missing", "no property"},
		{"customer.id.deeper", "not an object"},
		{"id[0]", "not an array"},
	}
	for _, c := range cases {
		err := checkPath(t, orderSchema(), stringKey(), c.path)
		if err == nil {
			t.Errorf("Check(%q) = nil, want an error mentioning %q", c.path, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("Check(%q) error = %v, want it to mention %q", c.path, err, c.want)
		}
	}
}

// TestKeyCheckerRejectsAlternatives proves a step whose schema is a oneOf or
// anyOf is rejected: the generator picks a branch per record, so no step under
// it is guaranteed.
func TestKeyCheckerRejectsAlternatives(t *testing.T) {
	schema := map[string]any{
		"type":     "object",
		"required": []any{"payment"},
		"properties": map[string]any{
			"payment": map[string]any{"oneOf": []any{
				map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
				map[string]any{"type": "object", "required": []any{"ref"}, "properties": map[string]any{"ref": map[string]any{"type": "string"}}},
			}},
		},
	}
	err := checkPath(t, schema, stringKey(), "payment.id")
	if err == nil || !strings.Contains(err.Error(), "oneOf") {
		t.Errorf("Check through a oneOf = %v, want an error mentioning oneOf", err)
	}
}

// TestKeyCheckerMergesAllOf proves allOf is walked the way generation walks it:
// the branches merge, so a field required by one branch is guaranteed.
func TestKeyCheckerMergesAllOf(t *testing.T) {
	schema := map[string]any{
		"type":     "object",
		"required": []any{"order"},
		"properties": map[string]any{
			"order": map[string]any{"allOf": []any{
				map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
				map[string]any{"type": "object", "properties": map[string]any{"extra": map[string]any{"type": "string"}}},
			}},
		},
	}
	if err := checkPath(t, schema, stringKey(), "order.id"); err != nil {
		t.Errorf("Check through allOf = %v, want nil", err)
	}
}

// TestKeyCheckerFollowsRefs proves a $ref step resolves inside the schema's
// own $defs (#73), and that a path deeper than the generator's budget is
// rejected rather than silently truncated at run time (ADR-0005).
func TestKeyCheckerFollowsRefs(t *testing.T) {
	node := map[string]any{
		"type":     "object",
		"required": []any{"name", "child"},
		"properties": map[string]any{
			"name":  map[string]any{"type": "string"},
			"child": map[string]any{"$ref": "#/$defs/Node"},
		},
	}
	schema := map[string]any{
		"type":       "object",
		"required":   []any{"root"},
		"properties": map[string]any{"root": map[string]any{"$ref": "#/$defs/Node"}},
		"$defs":      map[string]any{"Node": node},
	}

	if err := checkPath(t, schema, stringKey(), "root.name"); err != nil {
		t.Errorf("Check through a $ref = %v, want nil", err)
	}

	deep := "root" + strings.Repeat(".child", maxRecursionDepth+1) + ".name"
	err := checkPath(t, schema, stringKey(), deep)
	if err == nil || !strings.Contains(err.Error(), "depth") {
		t.Errorf("Check(%q) = %v, want an error mentioning the depth budget", deep, err)
	}

	delete(schema, "$defs")
	if err := checkPath(t, schema, stringKey(), "root.child.name"); err == nil {
		t.Errorf("Check through a $ref the schema does not define = nil, want an error")
	}
}

// TestKeyCheckerTypeCompatibility proves the type at the path must be able to
// hold the Key the key schema produces.
func TestKeyCheckerTypeCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		key     map[string]any
		path    string
		wantErr bool
	}{
		{"string into string", map[string]any{"type": "string"}, "id", false},
		{"integer into string", map[string]any{"type": "integer"}, "id", true},
		{"object into string", map[string]any{"type": "object", "properties": map[string]any{}}, "id", true},
		{"integer into number", map[string]any{"type": "integer"}, "total", false},
		{"number into integer", map[string]any{"type": "number"}, "count", true},
	}
	schema := map[string]any{
		"type":     "object",
		"required": []any{"id", "total", "count"},
		"properties": map[string]any{
			"id":    map[string]any{"type": "string"},
			"total": map[string]any{"type": "number"},
			"count": map[string]any{"type": "integer"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkPath(t, schema, c.key, c.path)
			if c.wantErr && err == nil {
				t.Errorf("Check = nil, want a type error")
			}
			if !c.wantErr && err != nil {
				t.Errorf("Check = %v, want nil", err)
			}
			if c.wantErr && err != nil && !strings.Contains(err.Error(), "key schema") {
				t.Errorf("Check error = %v, want it to name the key schema", err)
			}
		})
	}
}

// TestKeyCheckerNamesTypesWithTheirArticle pins the article: "an array", "an
// object", "an integer", never "a array".
func TestKeyCheckerNamesTypesWithTheirArticle(t *testing.T) {
	err := checkPath(t, orderSchema(), stringKey(), "items.sku")
	if err == nil || !strings.Contains(err.Error(), "the schema here is an array, not an object") {
		t.Errorf("err = %v, want it to say an array", err)
	}
	err = checkPath(t, orderSchema(), stringKey(), "id.x")
	if err == nil || !strings.Contains(err.Error(), "the schema here is a string, not an object") {
		t.Errorf("err = %v, want it to say a string", err)
	}
}
