package asyncapi

import (
	"encoding/json"
	"fmt"
	"strings"
)

// avsc reads an Avro schema the spec declares, a payload or a Key binding
// beside Avro payloads (#84), into avsc JSON. Its $refs are expanded in place,
// as a JSON Schema's are, with two Avro rules: a named type (record, enum,
// fixed) is defined once, so a $ref reaching one again becomes a reference by
// its full name; and a $ref cycle is refused, since Avro expresses recursion
// by name.
func (d *Document) avsc(node any) ([]byte, error) {
	x := &avscExpander{d: d, defined: map[string]string{}}
	out, err := x.node(node, nil, "")
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

// avscExpander expands one avsc, remembering the full name each $ref to a
// named type defined.
type avscExpander struct {
	d       *Document
	defined map[string]string
}

// node expands n. stack holds the $refs being expanded, and namespace is the
// one an unqualified name inherits there.
func (x *avscExpander) node(n any, stack []string, namespace string) (any, error) {
	switch n := n.(type) {
	case map[string]any:
		if ref := refOf(n); ref != "" {
			return x.ref(ref, stack, namespace)
		}
		if name, ok := namedType(n); ok {
			namespace = namespaceOf(name, n, namespace)
		}
		out := make(map[string]any, len(n))
		for k, v := range n {
			e, err := x.node(v, stack, namespace)
			if err != nil {
				return nil, err
			}
			out[k] = e
		}
		return out, nil
	case []any:
		out := make([]any, len(n))
		for i, v := range n {
			e, err := x.node(v, stack, namespace)
			if err != nil {
				return nil, err
			}
			out[i] = e
		}
		return out, nil
	default:
		return n, nil
	}
}

// ref expands a $ref: its target the first time, the named type's full name
// after that.
func (x *avscExpander) ref(ref string, stack []string, namespace string) (any, error) {
	for _, r := range stack {
		if r == ref {
			return nil, fmt.Errorf("$ref cycle through %s; in an avsc, refer to a named type by its name instead", ref)
		}
	}
	if full, ok := x.defined[ref]; ok {
		return full, nil
	}
	if len(stack) == maxRefChain {
		return nil, fmt.Errorf("$ref chain longer than %d", maxRefChain)
	}
	target, err := x.d.pointer(ref)
	if err != nil {
		return nil, err
	}
	out, err := x.node(target, append(stack, ref), namespace)
	if err != nil {
		return nil, err
	}
	if m, ok := target.(map[string]any); ok {
		if name, ok := namedType(m); ok {
			x.defined[ref] = fullName(name, namespaceOf(name, m, namespace))
		}
	}
	return out, nil
}

// namedType returns the name of a record, enum or fixed definition.
func namedType(n map[string]any) (string, bool) {
	switch n["type"] {
	case "record", "error", "enum", "fixed":
		name, ok := n["name"].(string)
		return name, ok && name != ""
	}
	return "", false
}

// namespaceOf is the namespace a named type's definition sits in, which its
// nested unqualified names inherit: the qualifier of a dotted name, else its
// own namespace, else the enclosing one.
func namespaceOf(name string, n map[string]any, enclosing string) string {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[:i]
	}
	if ns, ok := n["namespace"].(string); ok {
		return ns
	}
	return enclosing
}

// fullName qualifies a name with its namespace.
func fullName(name, namespace string) string {
	if strings.Contains(name, ".") || namespace == "" {
		return name
	}
	return namespace + "." + name
}
