package generator

import (
	"fmt"
	"math"
	"reflect"
)

// mergeAllOf folds every allOf branch into one schema using ADR-0006 Decision 5
// rules - numeric bounds intersect, required unions, properties merge
// recursively - then generates a value from the merged schema. Irreconcilable
// branches (distinct const values, clashing type) surface a typed
// UnsupportedSchemaError rather than a silently-overridden constraint.
func (g *Generator) mergeAllOf(allOf []any, path string, depth int) (any, error) {
	merged, err := mergeSchemas(allOf, path)
	if err != nil {
		return nil, err
	}
	return g.value(merged, path, depth)
}

// mergeSchemas folds the allOf branches into a single schema map. Each branch
// must be a schema object; anything else is an unsupported allOf construct.
func mergeSchemas(branches []any, path string) (map[string]any, error) {
	merged := make(map[string]any)
	for _, sub := range branches {
		m, ok := sub.(map[string]any)
		if !ok {
			return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path, Detail: "branch is not a schema object"}
		}
		var err error
		merged, err = mergeTwoSchemas(merged, m, path)
		if err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// mergeTwoSchemas intersects two schemas keyword by keyword (ADR-0006
// Decision 5). a is the accumulated merge so far, b the next branch.
func mergeTwoSchemas(a, b map[string]any, path string) (map[string]any, error) {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		existing, ok := out[k]
		switch k {
		case "minimum", "exclusiveMinimum":
			if ok {
				ev, eok := toFloat64(existing)
				nv, nok := toFloat64(v)
				if eok && nok {
					out[k] = math.Max(ev, nv)
					continue
				}
			}
			out[k] = v
		case "maximum", "exclusiveMaximum":
			if ok {
				ev, eok := toFloat64(existing)
				nv, nok := toFloat64(v)
				if eok && nok {
					out[k] = math.Min(ev, nv)
					continue
				}
			}
			out[k] = v
		case "required":
			out[k] = unionRequired(existing, v, ok)
		case "properties":
			merged, err := mergeProperties(existing, v, ok, path)
			if err != nil {
				return nil, err
			}
			out[k] = merged
		case "type":
			if ok {
				if !reflect.DeepEqual(existing, v) {
					return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path,
						Detail: fmt.Sprintf("clashing type %v and %v", existing, v)}
				}
				continue
			}
			out[k] = v
		case "const":
			if ok {
				if !reflect.DeepEqual(existing, v) {
					return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path,
						Detail: fmt.Sprintf("distinct const %v and %v", existing, v)}
				}
				continue
			}
			out[k] = v
		default:
			out[k] = v
		}
	}
	return out, nil
}

// mergeProperties recursively merges two properties maps. A property named in
// both branches is itself merged with mergeTwoSchemas; a property in only one
// branch is taken as-is.
func mergeProperties(existing, v any, exists bool, path string) (any, error) {
	if !exists {
		return v, nil
	}
	em, eok := existing.(map[string]any)
	vm, vok := v.(map[string]any)
	if !eok || !vok {
		return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path, Detail: "properties in allOf branch is not an object"}
	}
	out := make(map[string]any, len(em)+len(vm))
	for name, ps := range em {
		out[name] = ps
	}
	for name, ps := range vm {
		if ep, ok := out[name]; ok {
			epm, eok := ep.(map[string]any)
			psm, pok := ps.(map[string]any)
			if !eok || !pok {
				return nil, &UnsupportedSchemaError{Keyword: "allOf", Path: path + "." + name, Detail: "property schema in allOf branch is not an object"}
			}
			m, err := mergeTwoSchemas(epm, psm, path+"."+name)
			if err != nil {
				return nil, err
			}
			out[name] = m
		} else {
			out[name] = ps
		}
	}
	return out, nil
}

// unionRequired returns the set union of two required lists, preserving order.
func unionRequired(existing, v any, exists bool) any {
	if !exists {
		return v
	}
	ea, eok := existing.([]any)
	va, vok := v.([]any)
	if !eok || !vok {
		// One side is not a required list; fall back to what we already have.
		return existing
	}
	seen := make(map[string]bool, len(ea)+len(va))
	out := make([]any, 0, len(ea)+len(va))
	for _, x := range ea {
		s, ok := x.(string)
		if !ok || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	for _, x := range va {
		s, ok := x.(string)
		if !ok || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
