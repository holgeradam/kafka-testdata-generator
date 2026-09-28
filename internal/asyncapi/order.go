package asyncapi

import (
	"reflect"

	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"gopkg.in/yaml.v3"
)

// keyOrder is the order each object of the spec is written in, by the
// identity of the map it decoded into: decoding into maps forgets the order,
// and generated records keep it (#96, #112). Nothing is written into the
// spec itself, whose objects stay exactly as decoded.
type keyOrder map[uintptr][]string

// keyOrders reads the spec a second time as YAML nodes, which keep their
// order, JSON being YAML too, and walks them beside the decoded document,
// recording each object's key order. A spec it cannot read that way records
// none, and keeps sorted order.
func keyOrders(data []byte, doc map[string]any) keyOrder {
	orders := keyOrder{}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return orders
	}
	var walk func(n *yaml.Node, v any)
	walk = func(n *yaml.Node, v any) {
		switch n.Kind {
		case yaml.DocumentNode:
			if len(n.Content) == 1 {
				walk(n.Content[0], v)
			}
		case yaml.AliasNode:
			walk(n.Alias, v)
		case yaml.MappingNode:
			m, ok := v.(map[string]any)
			if !ok {
				return
			}
			orders[identity(m)] = mappingKeys(n)
			for i := 0; i+1 < len(n.Content); i += 2 {
				if child, ok := m[n.Content[i].Value]; ok {
					walk(n.Content[i+1], child)
				}
			}
		case yaml.SequenceNode:
			list, ok := v.([]any)
			if !ok || len(list) != len(n.Content) {
				return
			}
			for i, item := range n.Content {
				walk(item, list[i])
			}
		}
	}
	walk(&root, doc)
	return orders
}

// identity tells one decoded map from another.
func identity(m map[string]any) uintptr { return reflect.ValueOf(m).Pointer() }

// mappingKeys lists a mapping node's keys in order; a merge key is not one.
func mappingKeys(n *yaml.Node) []string {
	names := []string{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if key := n.Content[i].Value; key != "<<" {
			names = append(names, key)
		}
	}
	return names
}

// place is where in a schema the resolver copies a node: a schema object,
// whose keys are keywords; an object whose keys are names, such as
// properties; or a literal, such as an example, whose content is data.
type place int

const (
	inSchema place = iota
	inNames
	inLiteral
)

// nameKeywords hold objects whose keys are names, each naming a schema.
var nameKeywords = map[string]bool{"properties": true, "patternProperties": true, "$defs": true, "definitions": true, "dependencies": true, "dependentSchemas": true}

// literalKeywords hold literals the generator returns as-is (#107); default
// holds one it does not, but is data just as well.
var literalKeywords = []string{"example", "examples", "const", "enum"}

// child is where the value under key of an object at p is.
func (p place) child(key string) place {
	switch {
	case p == inLiteral:
		return inLiteral
	case p == inNames:
		return inSchema
	case nameKeywords[key]:
		return inNames
	case key == "default" || isLiteralKeyword(key):
		return inLiteral
	}
	return inSchema
}

// referenced is where a $ref's target is: a schema, unless the $ref sits in
// a literal, whose content it expands.
func (p place) referenced() place {
	if p == inLiteral {
		return inLiteral
	}
	return inSchema
}

func isLiteralKeyword(key string) bool {
	for _, k := range literalKeywords {
		if k == key {
			return true
		}
	}
	return false
}

// stamp records, on out, the copy of a schema object n, the order n's
// properties are written in (#96) and the order tree of each literal it
// holds that has an object in it (#112), under the extension keywords
// validators ignore. Only a schema object is stamped: an object of names,
// such as properties, or a literal holds no keywords to stamp beside.
func (d *Document) stamp(n, out map[string]any, at place) {
	if at != inSchema {
		return
	}
	if props, ok := n["properties"].(map[string]any); ok {
		if names, ok := d.orders[identity(props)]; ok {
			recorded := make([]any, len(names))
			for i, name := range names {
				recorded[i] = name
			}
			out[ordered.Keyword] = recorded
		}
	}
	trees := map[string]any{}
	for _, k := range literalKeywords {
		if v, ok := n[k]; ok && holdsObject(v) {
			trees[k] = d.tree(v)
		}
	}
	if len(trees) > 0 {
		out[ordered.LiteralKeyword] = trees
	}
}

// tree is the order tree of a literal (ordered.LiteralKeyword): its keys in
// written order, each with the tree of its value; nil where no object is.
func (d *Document) tree(v any) any {
	switch v := v.(type) {
	case map[string]any:
		var t ordered.Object
		for _, k := range d.orders[identity(v)] {
			if e, ok := v[k]; ok {
				t.Add(k, d.tree(e))
			}
		}
		return t
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = d.tree(e)
		}
		return out
	}
	return nil
}

// holdsObject reports whether an object is anywhere in v.
func holdsObject(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		return true
	case []any:
		for _, e := range v {
			if holdsObject(e) {
				return true
			}
		}
	}
	return false
}
