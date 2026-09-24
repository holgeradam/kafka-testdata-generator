package generator

import (
	"fmt"

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
	typ, _ := schema["type"].(string)
	if step.Index >= 0 {
		if typ != "array" {
			return nil, fmt.Errorf("the schema here is %s, not an array", describeType(schema))
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

	if typ != "object" {
		return nil, fmt.Errorf("the schema here is %s, not an object", describeType(schema))
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

// holds reports whether a value generated from the key schema conforms to the
// schema at the key path. Types must match, except that an integer Key also
// conforms to a number field.
func (c *KeyChecker) holds(at map[string]any) error {
	keyType, _ := c.keySchema["type"].(string)
	fieldType, _ := at["type"].(string)
	switch {
	case keyType == "":
		return fmt.Errorf("the key schema declares no type, so the Key cannot be planted")
	case fieldType == "":
		return fmt.Errorf("the schema here declares no type, so the Key cannot be planted")
	case keyType == fieldType:
		return nil
	case keyType == "integer" && fieldType == "number":
		return nil
	default:
		return fmt.Errorf("the schema here is %s but the key schema is %s", fieldType, keyType)
	}
}

// describeType names what a schema node is, for error messages, with its
// article: "an array", "a string".
func describeType(schema map[string]any) string {
	typ, ok := schema["type"].(string)
	switch {
	case !ok:
		return "untyped"
	case typ == "array" || typ == "object" || typ == "integer":
		return "an " + typ
	default:
		return "a " + typ
	}
}

// pathError names the step that failed, so the message points at the part of
// the path that is wrong rather than the whole of it.
func pathError(path []Step, i int, err error) error {
	return &keyplan.PathError{
		Path:   keyplan.PathString(path),
		Detail: fmt.Sprintf("at %q: %v", keyplan.PathString(path[:i+1]), err),
	}
}
