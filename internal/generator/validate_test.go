package generator

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// mustConform is the single test wrapper around the third-party JSON Schema
// validator (ADR-0006 Decision 3). It validates value against the given schema
// and fails the test if it does not conform. Swapping the validator
// implementation costs exactly this one helper; everything else delegates to it.
func mustConform(t *testing.T, schema map[string]any, value any) {
	t.Helper()
	doc := map[string]any{"$schema": "http://json-schema.org/draft-07/schema#"}
	for k, v := range schema {
		doc[k] = v
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}

	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft7
	if err := c.AddResource("schema.json", bytes.NewReader(raw)); err != nil {
		t.Fatalf("add resource: %v", err)
	}
	sch, err := c.Compile("schema.json")
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	if err := sch.Validate(value); err != nil {
		if jErr, ok := err.(*jsonschema.ValidationError); ok {
			t.Errorf("value %#v does not conform to schema %s: %s",
				value, raw, jErr.Causes)
			return
		}
		t.Errorf("validate %#v against schema: %v", value, err)
	}
}
