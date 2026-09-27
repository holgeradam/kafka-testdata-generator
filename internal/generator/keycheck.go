package generator

import (
	"fmt"
	"slices"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// KeyChecker validates a key path against a Message schema: that generation
// puts a value at that path in every record, and that the type there can hold
// the Key the key schema produces (ADR-0009). It is the JSON Schema adapter of
// keyplan.Checker; the avsc adapter lives in internal/avro.
//
// "Guaranteed" means what this generator guarantees: a property listed in its
// parent's required, an array index below minItems, no alternative branch along
// the way, and a depth within the $ref budget (ADR-0005). Anything else is
// present in some records and absent in others, which would leave the Key and
// the Payload disagreeing, so it is refused before the run starts.
type KeyChecker struct {
	schema    map[string]any
	keySchema map[string]any
	defs      map[string]any
}

// NewKeyChecker builds the checker from the Message schema and the key schema
// (the resolved bindings.kafka.key node). Both are self-contained: a $ref in
// the Message schema points into its own $defs (#73).
func NewKeyChecker(schema, keySchema map[string]any) *KeyChecker {
	defs, _ := schema["$defs"].(map[string]any)
	return &KeyChecker{schema: schema, keySchema: keySchema, defs: defs}
}

// Check reports whether path is usable, naming the offending step otherwise.
func (c *KeyChecker) Check(path []keyplan.Step) error {
	current := c.schema
	depth := 0
	for i, step := range path {
		resolved, d, err := resolveGuaranteed(c.defs, current, depth)
		if err != nil {
			return pathError(path, i, err)
		}
		depth = d
		next, err := descend(resolved, step)
		if err != nil {
			return pathError(path, i, err)
		}
		current = next
	}
	final, _, err := resolveGuaranteed(c.defs, current, depth)
	if err != nil {
		return pathError(path, len(path)-1, err)
	}
	if err := c.holds(final); err != nil {
		return pathError(path, len(path)-1, err)
	}
	return nil
}

// resolveGuaranteed follows $ref nodes into defs and merges allOf, so the
// caller sees the schema generation actually walks. It refuses alternatives,
// whose branch is chosen per record, and a $ref chain past the depth budget,
// where generation truncates the subtree instead of producing the field.
func resolveGuaranteed(defs, schema map[string]any, depth int) (map[string]any, int, error) {
	for {
		if _, ok := schema["oneOf"]; ok {
			return nil, depth, fmt.Errorf("the schema here is a oneOf, so the branch differs per record")
		}
		if _, ok := schema["anyOf"]; ok {
			return nil, depth, fmt.Errorf("the schema here is an anyOf, so the branch differs per record")
		}
		if allOf, ok := schema["allOf"].([]any); ok {
			merged, err := mergeSchemas(allOf, RootPath)
			if err != nil {
				return nil, depth, fmt.Errorf("the allOf here cannot be merged: %w", err)
			}
			schema = merged
			continue
		}
		ref, ok := schema["$ref"].(string)
		if !ok {
			return schema, depth, nil
		}
		target, err := lookupDef(defs, ref)
		if err != nil {
			return nil, depth, err
		}
		if depth >= maxRecursionDepth {
			return nil, depth, fmt.Errorf("the path goes beyond the $ref depth budget of %d, where generation truncates the subtree", maxRecursionDepth)
		}
		schema, depth = target, depth+1
	}
}

// descend takes one step into the schema, requiring the step to be guaranteed.
func descend(schema map[string]any, step Step) (map[string]any, error) {
	if step.Index >= 0 {
		if err := always(schema, "array"); err != nil {
			return nil, err
		}
		minItems := 1
		if v, ok := schema["minItems"].(float64); ok {
			minItems = int(v)
		}
		if step.Index >= minItems {
			return nil, fmt.Errorf("the array has minItems %d, so index %d is generated only sometimes", minItems, step.Index)
		}
		items, ok := schema["items"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("the array declares no items schema")
		}
		return items, nil
	}

	if err := always(schema, "object"); err != nil {
		return nil, err
	}
	props, _ := schema["properties"].(map[string]any)
	field, ok := props[step.Field].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the object has no property %q", step.Field)
	}
	if !isRequired(schema, step.Field) {
		return nil, fmt.Errorf("property %q is not required, so it is generated only sometimes", step.Field)
	}
	return field, nil
}

// Step is keyplan's path step, aliased so this file reads as one walk.
type Step = keyplan.Step

func isRequired(schema map[string]any, field string) bool {
	required, _ := schema["required"].([]any)
	for _, r := range required {
		if name, ok := r.(string); ok && name == field {
			return true
		}
	}
	return false
}

// always requires every value of schema to be of type want, so a path can
// step into it: a type list that also allows null may hold nothing to plant
// into, and one with other types holds want only sometimes (#99 decision 5).
func always(schema map[string]any, want string) error {
	types, err := typeList(schema)
	switch {
	case err == nil && len(types) == 1 && types[0] == want:
		return nil
	case err == nil && len(types) > 1 && slices.Contains(types, want) && slices.Contains(types, "null"):
		return fmt.Errorf("the schema here is %s, so it may be null, with nothing to plant into", describeType(schema))
	case err == nil && len(types) > 1 && slices.Contains(types, want):
		return fmt.Errorf("the schema here is %s, so it is %s only sometimes", describeType(schema), withArticle(want))
	}
	return fmt.Errorf("the schema here is %s, not %s", describeType(schema), withArticle(want))
}

// holds reports whether a value generated from the key schema conforms to the
// schema at the key path: every type the Key may take must be one the field
// allows, an integer Key also fitting a number field. A field whose type list
// allows null takes a planted Key of another type it lists (#99 decision 5).
func (c *KeyChecker) holds(at map[string]any) error {
	keyTypes, err := typeList(c.keySchema)
	if err != nil {
		return fmt.Errorf("the key schema declares no type, so the Key cannot be planted")
	}
	fieldTypes, err := typeList(at)
	if err != nil {
		return fmt.Errorf("the schema here declares no type, so the Key cannot be planted")
	}
	for _, kt := range keyTypes {
		if slices.Contains(fieldTypes, kt) || (kt == "integer" && slices.Contains(fieldTypes, "number")) {
			continue
		}
		if kt == "null" {
			return fmt.Errorf("the key schema may be null, but the schema here is %s", describeType(at))
		}
		return fmt.Errorf("the schema here is %s but the key schema is %s", describeType(at), describeType(c.keySchema))
	}
	return nil
}

// describeType names what a schema node is, for error messages, with its
// article: "an array", "a string".
func describeType(schema map[string]any) string {
	types, err := typeList(schema)
	switch {
	case err != nil:
		return "untyped"
	case len(types) > 1:
		return "one of " + strings.Join(types, ", ")
	}
	return withArticle(types[0])
}

// withArticle names a type with its article: "an array", "a string".
func withArticle(typ string) string {
	if typ == "array" || typ == "object" || typ == "integer" {
		return "an " + typ
	}
	return "a " + typ
}

// pathError names the step that failed, so the message points at the part of
// the path that is wrong rather than the whole of it.
func pathError(path []Step, i int, err error) error {
	return &keyplan.PathError{
		Path:   keyplan.PathString(path),
		Detail: fmt.Sprintf("at %q: %v", keyplan.PathString(path[:i+1]), err),
	}
}
