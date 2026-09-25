package avrowire

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Several Avro Message types on one Kafka topic register as a union under
// <topic>-value, TopicNameStrategy's pattern for several event types in one
// Kafka topic (#84 decisions 4 and 5): each Message type's record registers
// under its full name, a named type several of them define identically
// registers once and is referenced, and <topic>-value holds the union of the
// records' names, referencing each.

// unionMember is one Avro Message type of the Kafka topic: its name and avsc.
type unionMember struct {
	name string
	avsc []byte
}

// unionPlan is how the Message types register and encode.
type unionPlan struct {
	// Branches are the full names of the Message types' records, in Message
	// type order: the union's branches.
	Branches []string
	// Subjects register in order, each after the subjects it references.
	Subjects []unionSubject
	// Value is the <topic>-value schema, the union of Branches by name; it
	// references each branch's subject.
	Value string
	// Union is the same union with every record and named type inline, each
	// defined once, for the codec to encode records against.
	Union []byte
}

// unionSubject is one schema registered under its full name.
type unionSubject struct {
	Name   string
	Schema string
	// References are the full names of the subjects it refers to by name.
	References []string
}

// avroPrimitives are the type names that never refer to a named type.
var avroPrimitives = map[string]bool{"null": true, "boolean": true, "int": true, "long": true, "float": true, "double": true, "bytes": true, "string": true}

// namedDef is a named type as the Message types define it, in its shallow
// form: its full namespace written out, and every named type inside it
// replaced by its full name.
type namedDef struct {
	namespace string
	shallow   map[string]any
	canonical string
	// definedBy are the Message types that define it, in order.
	definedBy []string
}

// planUnion composes the Message types' avscs into the union plan, or
// refuses a composition the registry cannot hold: a payload that is not a
// record, one record as the payload of two Message types, a named type the
// Message types define differently, and shared named types that refer to
// each other, which separate subjects cannot express.
func planUnion(members []unionMember) (*unionPlan, error) {
	c := &composer{defs: map[string]*namedDef{}}
	rootOf := map[string]string{}
	var branches []string
	for _, m := range members {
		var avsc any
		if err := json.Unmarshal(m.avsc, &avsc); err != nil {
			return nil, fmt.Errorf("the payload of %s: %w", m.name, err)
		}
		root, ok := avsc.(map[string]any)
		if !ok || (root["type"] != "record" && root["type"] != "error") {
			return nil, fmt.Errorf("the payload of %s is not an Avro record, so it cannot be a branch of the union registered under <topic>-value", m.name)
		}
		c.member = m.name
		name, err := c.shallow(root, "")
		if err != nil {
			return nil, err
		}
		full := name.(string)
		if other, ok := rootOf[full]; ok {
			return nil, fmt.Errorf("Message types %s and %s both have the record %s as payload; a union holds each record once", other, m.name, full)
		}
		rootOf[full] = m.name
		branches = append(branches, full)
	}

	registered := map[string]bool{}
	for _, b := range branches {
		registered[b] = true
	}
	for name, d := range c.defs {
		if len(d.definedBy) > 1 {
			registered[name] = true
		}
	}
	order, err := c.order(branches, registered)
	if err != nil {
		return nil, err
	}

	plan := &unionPlan{Branches: branches}
	for _, name := range order {
		r := &renderer{c: c, registered: registered, self: name, emitted: map[string]bool{name: true}}
		schema, err := r.definition(name)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(schema)
		plan.Subjects = append(plan.Subjects, unionSubject{Name: name, Schema: string(raw), References: r.refs})
	}
	value, _ := json.Marshal(branches)
	plan.Value = string(value)

	inline := &renderer{c: c, emitted: map[string]bool{}}
	var union []any
	for _, b := range branches {
		s, err := inline.ref(b, "")
		if err != nil {
			return nil, err
		}
		union = append(union, s)
	}
	plan.Union, _ = json.Marshal(union)
	return plan, nil
}

// composer collects the named types of the Message types.
type composer struct {
	defs   map[string]*namedDef
	member string
}

// shallow returns a schema node's shallow form under the enclosing
// namespace: a named type's definition is recorded and becomes its full name.
// A namespace of "" inherits the enclosing one, as the codec reads it.
func (c *composer) shallow(node any, namespace string) (any, error) {
	switch n := node.(type) {
	case string:
		return qualify(n, namespace), nil
	case []any:
		out := make([]any, len(n))
		for i, v := range n {
			s, err := c.shallow(v, namespace)
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
		return out, nil
	case map[string]any:
		switch t := n["type"].(type) {
		case string:
			switch t {
			case "record", "error", "enum", "fixed":
				return c.define(n, namespace)
			case "array":
				return c.with(n, "items", namespace)
			case "map":
				return c.with(n, "values", namespace)
			}
			return n, nil
		case map[string]any, []any:
			return c.shallow(t, namespace)
		}
	}
	return node, nil
}

// with copies n with the schema under key in shallow form.
func (c *composer) with(n map[string]any, key, namespace string) (any, error) {
	out := copyMap(n)
	s, err := c.shallow(n[key], namespace)
	if err != nil {
		return nil, err
	}
	out[key] = s
	return out, nil
}

// define records a named type's definition and returns its full name.
func (c *composer) define(n map[string]any, enclosing string) (any, error) {
	name, _ := n["name"].(string)
	namespace := enclosing
	if ns, ok := n["namespace"].(string); ok && ns != "" {
		namespace = ns
	}
	if i := strings.LastIndex(name, "."); i >= 0 {
		namespace, name = name[:i], name[i+1:]
	}
	full := qualify(name, namespace)

	def := copyMap(n)
	def["name"] = name
	delete(def, "namespace")
	if namespace != "" {
		def["namespace"] = namespace
	}
	if fields, ok := n["fields"].([]any); ok {
		out := make([]any, len(fields))
		for i, f := range fields {
			fm, ok := f.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("the payload of %s: record %s has a field that is not an object", c.member, full)
			}
			field := copyMap(fm)
			s, err := c.shallow(fm["type"], namespace)
			if err != nil {
				return nil, err
			}
			field["type"] = s
			out[i] = field
		}
		def["fields"] = out
	}

	raw, _ := json.Marshal(def)
	if have, ok := c.defs[full]; ok {
		if have.canonical != string(raw) {
			return nil, fmt.Errorf("named type %s is defined differently in Message types %s and %s; a named type is registered once, so its definitions must agree", full, have.definedBy[0], c.member)
		}
		if !slices.Contains(have.definedBy, c.member) {
			have.definedBy = append(have.definedBy, c.member)
		}
		return full, nil
	}
	c.defs[full] = &namedDef{namespace: namespace, shallow: def, canonical: string(raw), definedBy: []string{c.member}}
	return full, nil
}

// order puts the registered types in registration order, each after the
// registered types it refers to, starting from the branches in order.
func (c *composer) order(branches []string, registered map[string]bool) ([]string, error) {
	var order []string
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch state[name] {
		case 2:
			return nil
		case 1:
			return fmt.Errorf("named types %s refer to each other, so they cannot register as separate subjects; define them in one Message type only", strings.Join(cycleOf(path, name), " and "))
		}
		state[name] = 1
		for _, dep := range c.dependencies(name, registered) {
			if err := visit(dep, append(path, name)); err != nil {
				return err
			}
		}
		state[name] = 2
		order = append(order, name)
		return nil
	}
	for _, b := range branches {
		if err := visit(b, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// cycleOf is the part of path from name on, sorted, for a stable message.
func cycleOf(path []string, name string) []string {
	i := slices.Index(path, name)
	cycle := slices.Clone(path[i:])
	sort.Strings(cycle)
	return cycle
}

// dependencies are the registered types a registered type refers to, looking
// through the named types that stay inline in it.
func (c *composer) dependencies(name string, registered map[string]bool) []string {
	var deps []string
	seen := map[string]bool{name: true}
	var schema func(node any)
	def := func(d *namedDef) {
		fields, _ := d.shallow["fields"].([]any)
		for _, f := range fields {
			schema(f.(map[string]any)["type"])
		}
	}
	schema = func(node any) {
		switch n := node.(type) {
		case string:
			if seen[n] || c.defs[n] == nil {
				return
			}
			seen[n] = true
			if registered[n] {
				deps = append(deps, n)
				return
			}
			def(c.defs[n])
		case []any:
			for _, v := range n {
				schema(v)
			}
		case map[string]any:
			schema(n["items"])
			schema(n["values"])
		}
	}
	def(c.defs[name])
	return deps
}

// renderer writes shallow forms back out as avsc JSON: each named type
// defined at its first occurrence and named after that. A registered type
// other than self stays a name, and is recorded as a reference; the inline
// union has no registered types, so it defines each one once.
type renderer struct {
	c          *composer
	registered map[string]bool
	self       string
	emitted    map[string]bool
	refs       []string
}

// ref renders a reference to the named type full from inside namespace.
func (r *renderer) ref(full, namespace string) (any, error) {
	d := r.c.defs[full]
	if d == nil {
		return full, nil
	}
	if d.namespace == "" && namespace != "" {
		return nil, fmt.Errorf("named type %s has no namespace, so a namespaced record cannot refer to it by name; give it a namespace", full)
	}
	if full != r.self && r.registered[full] {
		if !slices.Contains(r.refs, full) {
			r.refs = append(r.refs, full)
		}
		return full, nil
	}
	if r.emitted[full] {
		return full, nil
	}
	r.emitted[full] = true
	return r.definition(full)
}

// definition renders the named type full's definition.
func (r *renderer) definition(full string) (any, error) {
	d := r.c.defs[full]
	out := copyMap(d.shallow)
	if fields, ok := d.shallow["fields"].([]any); ok {
		rendered := make([]any, len(fields))
		for i, f := range fields {
			field := copyMap(f.(map[string]any))
			t, err := r.schema(field["type"], d.namespace)
			if err != nil {
				return nil, err
			}
			field["type"] = t
			rendered[i] = field
		}
		out["fields"] = rendered
	}
	return out, nil
}

// schema renders a shallow schema node inside namespace.
func (r *renderer) schema(node any, namespace string) (any, error) {
	switch n := node.(type) {
	case string:
		if avroPrimitives[n] {
			return n, nil
		}
		return r.ref(n, namespace)
	case []any:
		out := make([]any, len(n))
		for i, v := range n {
			s, err := r.schema(v, namespace)
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
		return out, nil
	case map[string]any:
		out := copyMap(n)
		for _, key := range []string{"items", "values"} {
			if v, ok := n[key]; ok {
				s, err := r.schema(v, namespace)
				if err != nil {
					return nil, err
				}
				out[key] = s
			}
		}
		return out, nil
	}
	return node, nil
}

// qualify gives a type name its full name in namespace; primitives and
// dotted names are already full.
func qualify(name, namespace string) string {
	if avroPrimitives[name] || strings.Contains(name, ".") || namespace == "" {
		return name
	}
	return namespace + "." + name
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
