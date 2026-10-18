package generator

import (
	"fmt"
	"reflect"
	"slices"
)

// The keywords a field may declare beyond its type, by how a key schema can
// entail them. A Key is planted only when the key schema provably guarantees
// every one the field declares (#113); a keyword listed nowhere here is an
// annotation or is judged by type alone.
var (
	// sameKeywords are satisfied only when the key schema declares the same,
	// annotations aside.
	sameKeywords = []string{
		"pattern", "format", "multipleOf", "uniqueItems", "not",
		"required", "properties", "additionalProperties", "patternProperties", "propertyNames",
		"items", "prefixItems", "contains", "minContains", "maxContains",
		"dependentRequired", "dependentSchemas", "if", "then", "else",
	}
	// sizeBounds are satisfied by a Key bound of the same kind at least as
	// strict: a lower bound at least as high, an upper one at most as high.
	sizeLower = []string{"minLength", "minItems", "minProperties"}
	sizeUpper = []string{"maxLength", "maxItems", "maxProperties"}
)

// annotations never constrain a value, so two schemas differing only in them
// accept the same values.
var annotations = []string{"description", "title", "example", "examples", "default", "deprecated", "readOnly", "writeOnly", "$comment"}

// keyHeldBy reports why a Key generated from key may break a constraint the
// field declares beyond its type, nil when none can. It is a sound but
// incomplete proof: a keyword it cannot entail is refused, since a refused
// run is safer than a Payload that breaks Conformance.
func keyHeldBy(field, key map[string]any) error {
	for _, kw := range []string{"enum", "const"} {
		if _, ok := field[kw]; ok && !keyValuesWithin(field, key) {
			return keyViolation(kw)
		}
	}
	for _, kw := range sizeLower {
		if v, ok := field[kw]; ok && !atLeast(key[kw], v) {
			return keyViolation(kw)
		}
	}
	for _, kw := range sizeUpper {
		if v, ok := field[kw]; ok && !atMost(key[kw], v) {
			return keyViolation(kw)
		}
	}
	if kw := numericViolation(field, key); kw != "" {
		return keyViolation(kw)
	}
	for _, kw := range sameKeywords {
		v, ok := field[kw]
		// A keyword set to its permissive value constrains nothing: a true
		// schema, or uniqueItems false.
		if !ok || v == true || (kw == "uniqueItems" && v == false) {
			continue
		}
		if !reflect.DeepEqual(withoutAnnotations(v), withoutAnnotations(key[kw])) {
			return keyViolation(kw)
		}
	}
	return nil
}

// numericViolation names the numeric bound of field that key does not
// guarantee, "" when it guarantees all. An exclusive field bound needs a Key
// bound that excludes the same value: a higher inclusive one, or the same
// exclusive one. An inclusive field bound is met by either kind.
func numericViolation(field, key map[string]any) string {
	if v, ok := field["minimum"]; ok && !atLeast(key["minimum"], v) && !atLeast(key["exclusiveMinimum"], v) {
		return "minimum"
	}
	if v, ok := field["exclusiveMinimum"]; ok && !greater(key["minimum"], v) && !atLeast(key["exclusiveMinimum"], v) {
		return "exclusiveMinimum"
	}
	if v, ok := field["maximum"]; ok && !atMost(key["maximum"], v) && !atMost(key["exclusiveMaximum"], v) {
		return "maximum"
	}
	if v, ok := field["exclusiveMaximum"]; ok && !less(key["maximum"], v) && !atMost(key["exclusiveMaximum"], v) {
		return "exclusiveMaximum"
	}
	return ""
}

// keyValuesWithin reports whether the key schema fixes the Keys it generates
// to a set every member of which the field's enum or const allows. The set is
// what Generator.value answers the key schema with, read in its order.
func keyValuesWithin(field, key map[string]any) bool {
	var values []any
	switch {
	case key["example"] != nil:
		values = []any{key["example"]}
	case len(listOf(key["examples"])) > 0:
		values = key["examples"].([]any)[:1]
	case key["const"] != nil:
		values = []any{key["const"]}
	case len(listOf(key["enum"])) > 0:
		values = key["enum"].([]any)
	default:
		return false
	}
	for _, v := range values {
		if !allowedByField(field, v) {
			return false
		}
	}
	return true
}

func allowedByField(field map[string]any, v any) bool {
	if c, ok := field["const"]; ok && !reflect.DeepEqual(c, v) {
		return false
	}
	if e, ok := field["enum"].([]any); ok && !slices.ContainsFunc(e, func(m any) bool { return reflect.DeepEqual(m, v) }) {
		return false
	}
	return true
}

func listOf(v any) []any {
	l, _ := v.([]any)
	return l
}

// withoutAnnotations copies a schema value with its annotation keywords
// removed, at every depth.
func withoutAnnotations(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if !slices.Contains(annotations, k) {
				out[k] = withoutAnnotations(e)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = withoutAnnotations(e)
		}
		return out
	}
	return v
}

// The comparisons below take a Key bound and a field bound. A bound that is
// absent or not a number entails nothing, so they report false.

func atLeast(keyBound, fieldBound any) bool {
	k, f, ok := numbers(keyBound, fieldBound)
	return ok && k >= f
}

func atMost(keyBound, fieldBound any) bool {
	k, f, ok := numbers(keyBound, fieldBound)
	return ok && k <= f
}

func greater(keyBound, fieldBound any) bool {
	k, f, ok := numbers(keyBound, fieldBound)
	return ok && k > f
}

func less(keyBound, fieldBound any) bool {
	k, f, ok := numbers(keyBound, fieldBound)
	return ok && k < f
}

func numbers(a, b any) (x, y float64, ok bool) {
	x, aok := a.(float64)
	y, bok := b.(float64)
	return x, y, aok && bok
}

func keyViolation(keyword string) error {
	return fmt.Errorf("the schema here declares %s, which the key schema does not guarantee every Key satisfies", keyword)
}
