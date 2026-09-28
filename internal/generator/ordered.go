package generator

import (
	"sort"

	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
)

// declaredOrder is the order of props as the schema declares them: the
// recorded order (ordered.Keyword), then any property it misses, sorted.
func declaredOrder(schema, props map[string]any) []string {
	var names []string
	listed := map[string]bool{}
	recorded, _ := schema[ordered.Keyword].([]any)
	for _, n := range recorded {
		name, ok := n.(string)
		if _, declared := props[name]; ok && declared && !listed[name] {
			names = append(names, name)
			listed[name] = true
		}
	}
	var rest []string
	for name := range props {
		if !listed[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	return append(names, rest...)
}

// literalOf is the literal under keyword of schema, the value v - the
// index-th of a list of them when index is not negative - as a fresh copy in
// the order it is written in, from the order tree the spec reader records
// (ordered.LiteralKeyword).
func literalOf(schema map[string]any, keyword string, v any, index int) any {
	trees, _ := schema[ordered.LiteralKeyword].(map[string]any)
	tree := trees[keyword]
	if index >= 0 {
		list, _ := tree.([]any)
		tree = nil
		if index < len(list) {
			tree = list[index]
		}
	}
	return orderLiteral(v, tree)
}

// orderLiteral copies v with every object an ordered.Object: its keys in the
// order tree gives them, then any key tree does not know, sorted. Objects and
// arrays are fresh, scalars shared, being immutable.
func orderLiteral(v, tree any) any {
	switch v := v.(type) {
	case map[string]any:
		t, _ := tree.(ordered.Object)
		var obj ordered.Object
		listed := map[string]bool{}
		for i, k := range t.Keys {
			if e, ok := v[k]; ok && !listed[k] {
				obj.Add(k, orderLiteral(e, t.Values[i]))
				listed[k] = true
			}
		}
		var rest []string
		for k := range v {
			if !listed[k] {
				rest = append(rest, k)
			}
		}
		sort.Strings(rest)
		for _, k := range rest {
			obj.Add(k, orderLiteral(v[k], nil))
		}
		return obj
	case []any:
		t, _ := tree.([]any)
		out := make([]any, len(v))
		for i, e := range v {
			var sub any
			if i < len(t) {
				sub = t[i]
			}
			out[i] = orderLiteral(e, sub)
		}
		return out
	}
	return v
}
