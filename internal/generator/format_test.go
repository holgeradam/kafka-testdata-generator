package generator

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// standardFormats are the string formats JSON Schema 2020-12 defines. Every one
// of them is honoured (ADR-0008 amendment, #44).
var standardFormats = []string{
	"date-time", "date", "time", "duration",
	"email", "idn-email", "hostname", "idn-hostname",
	"ipv4", "ipv6", "uri", "uri-reference", "iri", "iri-reference",
	"uuid", "uri-template", "json-pointer", "relative-json-pointer", "regex",
}

// mustConformFormat validates value against a string schema carrying format,
// with format assertion switched on (it is annotation-only by default from
// draft 2019-09). The validator ships no idn-email or idn-hostname checker, so
// those map to the ASCII checkers, which is the stricter assertion and exactly
// what the Synthesizer produces.
func mustConformFormat(t *testing.T, format string, value any) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type":    "string",
		"format":  format,
	})
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat = true
	c.Formats = map[string]func(any) bool{
		"idn-email":    jsonschema.Formats["email"],
		"idn-hostname": jsonschema.Formats["hostname"],
	}
	if err := c.AddResource("schema.json", bytes.NewReader(raw)); err != nil {
		t.Fatalf("add resource: %v", err)
	}
	sch, err := c.Compile("schema.json")
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	if err := sch.Validate(value); err != nil {
		t.Errorf("format %q: value %#v does not conform: %v", format, value, err)
	}
}

// TestStandardFormatsProperty generates for every standard format and validates
// each value with format assertion on.
func TestStandardFormatsProperty(t *testing.T) {
	for _, format := range standardFormats {
		t.Run(format, func(t *testing.T) {
			gen := New(synth.New(42, fixedNow()))
			schema := map[string]any{"type": "string", "format": format}
			for i := 0; i < 30; i++ {
				v, err := gen.Value(schema)
				if err != nil {
					t.Fatalf("Value error: %v", err)
				}
				s, ok := v.(string)
				if !ok {
					t.Fatalf("expected string, got %T", v)
				}
				mustConformFormat(t, format, s)
			}
		})
	}
}

// TestNonStandardFormatIgnored proves a format outside the standard set is an
// annotation: generation succeeds and falls through to pattern, then to the
// field-name heuristics (#31 decision 10).
func TestNonStandardFormatIgnored(t *testing.T) {
	gen := New(synth.New(42, fixedNow()))
	schema := map[string]any{
		"type":     "object",
		"required": []any{"secret", "code", "city"},
		"properties": map[string]any{
			"secret": map[string]any{"type": "string", "format": "password"},
			"code":   map[string]any{"type": "string", "format": "byte", "pattern": `^[A-Z]{4}$`},
			"city":   map[string]any{"type": "string", "format": "currency-code"},
		},
	}
	v, err := gen.Value(schema)
	if err != nil {
		t.Fatalf("a non-standard format must not fail generation: %v", err)
	}
	obj := v.(map[string]any)
	mustConform(t, schema, obj)
	if got := obj["code"].(string); len(got) != 4 {
		t.Errorf("code = %q, want the pattern honoured under an ignored format", got)
	}
	if got := obj["city"].(string); got == "" {
		t.Errorf("city = %q, want a heuristic value", got)
	}
}
