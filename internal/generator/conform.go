package generator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Conforms reports whether value honours schema, format included, naming each
// constraint it breaks. It is the one production use of a JSON Schema
// validator (ADR-0006, amended by #83): a check made once at startup on a
// value the run did not generate, such as a Topic parameter's, never in the
// generation path, where Conformance holds by construction.
func Conforms(schema map[string]any, value any) error {
	doc := maps.Clone(schema) // adding $schema leaves the caller's schema intact
	doc["$schema"] = "http://json-schema.org/draft-07/schema#"
	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encoding the schema: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft7
	c.AssertFormat = true
	if err := c.AddResource("field.json", bytes.NewReader(raw)); err != nil {
		return fmt.Errorf("reading the schema: %w", err)
	}
	compiled, err := c.Compile("field.json")
	if err != nil {
		return fmt.Errorf("compiling the schema: %w", err)
	}
	err = compiled.Validate(value)
	var invalid *jsonschema.ValidationError
	if errors.As(err, &invalid) {
		return errors.New(strings.Join(leafMessages(invalid), "; "))
	}
	return err
}

// leafMessages collects the validator's innermost messages, which name the
// broken constraint, rather than its wrapper's.
func leafMessages(e *jsonschema.ValidationError) []string {
	if len(e.Causes) == 0 {
		return []string{e.Message}
	}
	var out []string
	for _, cause := range e.Causes {
		out = append(out, leafMessages(cause)...)
	}
	return out
}
