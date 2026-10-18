package generator

import (
	"fmt"
	"reflect"
	"slices"
)

// Constraint keywords by how a key schema can entail them. A field's keyword
// from these is satisfied by every Key only when the key schema provably
// guarantees it (#113); every other keyword is an annotation or is judged by
// type alone.
var (
	// lowerBounds are satisfied when the Key's own bound is at least as high.
	lowerBounds = []string{"minLength", "minItems", "minProperties", "minimum", "exclusiveMinimum"}
	// upperBounds are satisfied when the Key's own bound is at most as high.
	upperBounds = []string{"maxLength", "maxItems", "maxProperties", "maximum", "exclusiveMaximum"}
	// sameKeywords are satisfied only when the key schema declares the same.
	sameKeywords = []string{"pattern", "format", "multipleOf", "uniqueItems", "not"}
)

// holdsConstraints reports why a Key generated from key may break a
// constraint the field declares beyond its type, "" when none can. It is a
// sound but incomplete proof: a keyword it cannot entail is refused, since a
// refused run is safer than a Payload that breaks Conformance.
func holdsConstraints(field, key map[string]any) string {
	if _, ok := field["enum"]; ok {
		if !keyValuesWithin(field, key) {
			return "enum"
		}
	}
	if _, ok := field["const"]; ok {
		if !keyValuesWithin(field, key) {
			return "const"
		}
	}
	for _, kw := range lowerBounds {
		if v, ok := field[kw]; ok && !boundEntailed(v, key[kw], true) {
			return kw
		}
	}
	for _, kw := range upperBounds {
		if v, ok := field[kw]; ok && !boundEntailed(v, key[kw], false) {
			return kw
		}
	}
	for _, kw := range sameKeywords {
		v, ok := field[kw]
		if !ok || v == false {
			continue
		}
		if !reflect.DeepEqual(v, key[kw]) {
			return kw
		}
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
	case len(sliceOf(key["examples"])) > 0:
		values = key["examples"].([]any)[:1]
	case key["const"] != nil:
		values = []any{key["const"]}
	case len(sliceOf(key["enum"])) > 0:
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

func sliceOf(v any) []any {
	s, _ := v.([]any)
	return s
}

// boundEntailed reports whether the key schema's bound keeps every Key within
// the field's: at least as high for a lower bound, at most as high for an
// upper one. A key schema that declares no numeric bound entails nothing.
func boundEntailed(fieldBound, keyBound any, lower bool) bool {
	f, fok := fieldBound.(float64)
	k, kok := keyBound.(float64)
	if !fok || !kok {
		return false
	}
	if lower {
		return k >= f
	}
	return k <= f
}

func keyViolation(keyword string) error {
	return fmt.Errorf("the schema here declares %s, which the key schema does not guarantee every Key satisfies", keyword)
}
