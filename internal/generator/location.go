package generator

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// Locate walks a JSON Pointer into a Payload schema, as the location of a
// Topic parameter (#83), and returns the steps to plant along and the schema
// of the field there. Every step must be one generation guarantees, exactly
// as for a key path (KeyChecker): a required property, an index below
// minItems, no alternative on the way, within the $ref budget. A token indexes
// an array where the schema is one, and names a property elsewhere.
//
// The field schema comes back self-contained, carrying the Payload schema's
// $defs, so it can be validated against on its own.
func Locate(schema map[string]any, pointer []string) ([]Step, map[string]any, error) {
	defs, _ := schema["$defs"].(map[string]any)
	steps := make([]Step, len(pointer))
	current, depth := schema, 0
	for i, token := range pointer {
		resolved, d, err := resolveGuaranteed(defs, current, depth)
		if err != nil {
			return nil, nil, locationError(pointer, i, err)
		}
		depth = d
		steps[i] = Step{Field: token, Index: -1}
		if n, err := strconv.Atoi(token); err == nil && n >= 0 && strconv.Itoa(n) == token && resolved["type"] == "array" {
			steps[i] = Step{Index: n}
		}
		if current, err = descend(resolved, steps[i]); err != nil {
			return nil, nil, locationError(pointer, i, err)
		}
	}
	field, _, err := resolveGuaranteed(defs, current, depth)
	if err != nil {
		return nil, nil, locationError(pointer, len(pointer)-1, err)
	}
	if defs != nil {
		field = maps.Clone(field)
		field["$defs"] = defs
	}
	return steps, field, nil
}

// locationError names the token that failed, as the pointer up to it.
func locationError(pointer []string, i int, err error) error {
	escaped := make([]string, i+1)
	for j, token := range pointer[:i+1] {
		escaped[j] = strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	}
	return fmt.Errorf("at %q: %w", "/"+strings.Join(escaped, "/"), err)
}
