package generator

import (
	"sort"

	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
)

// OrderKeyword is the extension keyword under which the spec reader records
// the order a schema declares its properties in (#96): a decoded map forgets
// the order its keys were written in. Validators ignore unknown keywords, so
// it changes nothing but the order records encode in.
const OrderKeyword = "x-kafka-testdata-generator-property-order"

// Ordered returns value ready for JSON encoding with every object's keys in
// the order its schema declares its properties, at every level: nested
// objects, array items, through $refs into the schema's $defs, and through
// allOf, oneOf and anyOf, whose declared properties count in branch order.
// Keys the schema does not declare follow, sorted, and so do all keys of a
// schema whose order was never recorded. Only the order changes: values are
// the ones generated.
func Ordered(schema map[string]any, value any) any {
	defs, _ := schema["$defs"].(map[string]any)
	return orderer{defs: defs}.value(schema, value)
}

type orderer struct {
	defs map[string]any
}

func (o orderer) value(schema map[string]any, v any) any {
	switch v := v.(type) {
	case map[string]any:
		names, children := o.properties(schema)
		var obj ordered.Object
		for _, name := range names {
			if val, ok := v[name]; ok {
				obj.Add(name, o.value(children[name], val))
			}
		}
		var rest []string
		for name := range v {
			if _, declared := children[name]; !declared {
				rest = append(rest, name)
			}
		}
		sort.Strings(rest)
		for _, name := range rest {
			obj.Add(name, o.value(nil, v[name]))
		}
		return obj
	case []any:
		items := o.items(schema)
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = o.value(items, e)
		}
		return out
	}
	return v
}

// properties collects the properties the schema declares, in order, with
// the schema of each, across its $ref and composition branches.
func (o orderer) properties(schema map[string]any) ([]string, map[string]map[string]any) {
	var names []string
	children := map[string]map[string]any{}
	for _, s := range o.branches(schema) {
		props, _ := s["properties"].(map[string]any)
		for _, name := range declaredOrder(s, props) {
			if _, seen := children[name]; seen {
				continue
			}
			child, _ := props[name].(map[string]any)
			children[name] = child
			names = append(names, name)
		}
	}
	return names, children
}

// items is the items schema the schema declares, across its branches.
func (o orderer) items(schema map[string]any) map[string]any {
	for _, s := range o.branches(schema) {
		if items, ok := s["items"].(map[string]any); ok {
			return items
		}
	}
	return nil
}

// branches are the schema and every schema it stands for through $ref,
// allOf, oneOf and anyOf, each once.
func (o orderer) branches(schema map[string]any) []map[string]any {
	var out []map[string]any
	seen := map[string]bool{}
	var walk func(s map[string]any)
	walk = func(s map[string]any) {
		if s == nil {
			return
		}
		if ref, ok := s["$ref"].(string); ok {
			if seen[ref] {
				return
			}
			seen[ref] = true
			target, err := lookupDef(o.defs, ref)
			if err == nil {
				walk(target)
			}
			return
		}
		out = append(out, s)
		for _, key := range []string{"allOf", "oneOf", "anyOf"} {
			list, _ := s[key].([]any)
			for _, b := range list {
				branch, _ := b.(map[string]any)
				walk(branch)
			}
		}
	}
	walk(schema)
	return out
}

// declaredOrder is the order of props as the schema declares them: the
// recorded order, then any property it misses, sorted.
func declaredOrder(schema, props map[string]any) []string {
	var names []string
	listed := map[string]bool{}
	recorded, _ := schema[OrderKeyword].([]any)
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
