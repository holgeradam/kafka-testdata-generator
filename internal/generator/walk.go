package generator

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
)

// Walk is the JSON Schema adapter of planting.Walk: where generation puts a
// value in every record of a Message schema, or of a headers schema, and
// whether the field there holds what is planted into it (ADR-0009, #83).
//
// "Guaranteed" means what this generator guarantees, read as Generator.value
// reads the schema: a property listed in its parent's required, an array index
// below minItems, no alternative branch along the way, no literal the
// generator returns as-is (#107), and a depth within the $ref budget
// (ADR-0005). Anything else is present in some records and absent in others,
// so it is refused before the run starts.
type Walk struct {
	schema map[string]any
	key    map[string]any
	defs   map[string]any
}

// NewWalk walks schema, a Message schema or a headers schema, judging the
// Key against key, the resolved bindings.kafka.key node, which is nil where no
// Key is planted. Both are self-contained: a $ref in schema points into its
// own $defs (#73).
func NewWalk(schema, key map[string]any) *Walk {
	defs, _ := schema["$defs"].(map[string]any)
	return &Walk{schema: schema, key: key, defs: defs}
}

// Locate follows path through the schema. A token indexes an array where the
// schema is one, and names a property elsewhere.
func (w *Walk) Locate(path []planting.Step) ([]planting.Step, planting.Field, error) {
	steps := make([]planting.Step, len(path))
	current, depth := w.schema, 0
	for i, step := range path {
		resolved, d, err := resolveGuaranteed(w.defs, current, depth, true)
		if err != nil {
			return nil, nil, &planting.StepError{Step: i, Err: err}
		}
		depth = d
		steps[i] = step
		if step.Token {
			steps[i] = planting.Step{Field: step.Field, Index: -1}
			if n, err := strconv.Atoi(step.Field); err == nil && n >= 0 && strconv.Itoa(n) == step.Field && allows(resolved, "array") {
				steps[i] = planting.Step{Index: n}
			}
		}
		if current, err = descend(resolved, steps[i]); err != nil {
			return nil, nil, &planting.StepError{Step: i, Err: err}
		}
	}
	final, _, err := resolveGuaranteed(w.defs, current, depth, false)
	if err != nil {
		return nil, nil, &planting.StepError{Step: len(path) - 1, Err: err}
	}
	return steps, &field{schema: final, walk: w}, nil
}

// field is the schema at the end of a located path.
type field struct {
	schema map[string]any
	walk   *Walk
}

// Holds validates value against the field, carrying the walked schema's
// $defs, so a $ref inside the field still resolves.
func (f *field) Holds(value string) error {
	return Conforms(f.selfContained(), value)
}

// selfContained is the field's schema with the walked schema's $defs.
func (f *field) selfContained() map[string]any {
	if f.walk.defs == nil {
		return f.schema
	}
	s := maps.Clone(f.schema)
	s["$defs"] = f.walk.defs
	return s
}

// resolveGuaranteed follows $ref nodes into defs and merges allOf, so the
// caller sees the schema generation actually walks, taking the keywords in
// the order Generator.value does. It refuses alternatives, whose branch is
// chosen per record, and a $ref chain past the depth budget, where generation
// truncates the subtree instead of producing the field. When the path steps
// into the schema, it also refuses one the generator answers with a literal,
// which need not hold the next step (#107); at the end of the path a literal
// is no obstacle, since the planted value replaces it.
func resolveGuaranteed(defs, schema map[string]any, depth int, into bool) (map[string]any, int, error) {
	for {
		if ref, ok := schema["$ref"].(string); ok {
			target, err := lookupDef(defs, ref)
			if err != nil {
				return nil, depth, err
			}
			if depth >= maxRecursionDepth {
				return nil, depth, fmt.Errorf("the path goes beyond the $ref depth budget of %d, where generation truncates the subtree", maxRecursionDepth)
			}
			schema, depth = target, depth+1
			continue
		}
		if keyword := literalKeyword(schema); into && keyword != "" {
			return nil, depth, fmt.Errorf("the schema here declares %s, which generation returns as-is, so a path cannot step into it", keyword)
		}
		if allOf, ok := schema["allOf"].([]any); ok {
			merged, err := mergeSchemas(allOf, RootPath)
			if err != nil {
				return nil, depth, fmt.Errorf("the allOf here cannot be merged: %w", err)
			}
			schema = merged
			continue
		}
		if _, ok := schema["oneOf"]; ok {
			return nil, depth, fmt.Errorf("the schema here is a oneOf, so the branch differs per record")
		}
		if _, ok := schema["anyOf"]; ok {
			return nil, depth, fmt.Errorf("the schema here is an anyOf, so the branch differs per record")
		}
		return schema, depth, nil
	}
}

// literalKeyword names the keyword Generator.value answers schema with as-is,
// with its article, or "" when it builds the value from the schema instead.
// It mirrors value's short-circuits: an example, the first of non-empty
// examples, a const, a member of a non-empty enum.
func literalKeyword(schema map[string]any) string {
	if _, ok := schema["example"]; ok {
		return "an example"
	}
	if ex, ok := schema["examples"].([]any); ok && len(ex) > 0 {
		return "examples"
	}
	if _, ok := schema["const"]; ok {
		return "a const"
	}
	if enum, ok := schema["enum"].([]any); ok && len(enum) > 0 {
		return "an enum"
	}
	return ""
}

// descend takes one step into the schema, requiring the step to be guaranteed.
func descend(schema map[string]any, step planting.Step) (map[string]any, error) {
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

// HoldsKey reports whether a value generated from the key schema conforms to
// the field: every type the Key may take must be one the field allows, an
// integer Key also fitting a number field. A field whose type list allows
// null takes a planted Key of another type it lists (#99 decision 5).
func (f *field) HoldsKey() error {
	keyTypes, err := typeList(f.walk.key)
	if err != nil {
		return fmt.Errorf("the key schema declares no type, so the Key cannot be planted")
	}
	fieldTypes, err := typeList(f.schema)
	if err != nil {
		return fmt.Errorf("the schema here declares no type, so the Key cannot be planted")
	}
	for _, kt := range keyTypes {
		if slices.Contains(fieldTypes, kt) || (kt == "integer" && slices.Contains(fieldTypes, "number")) {
			continue
		}
		if kt == "null" {
			return fmt.Errorf("the key schema may be null, but the schema here is %s", describeType(f.schema))
		}
		return fmt.Errorf("the schema here is %s but the key schema is %s", describeType(f.schema), describeType(f.walk.key))
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

// allows reports whether the schema's type is typ or lists it.
func allows(schema map[string]any, typ string) bool {
	types, err := typeList(schema)
	return err == nil && slices.Contains(types, typ)
}
