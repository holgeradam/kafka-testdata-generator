// Package asyncapi reads an AsyncAPI 2.x spec into what a run needs: the
// Message types the spec declares for a Kafka topic, each with a Payload schema
// and an optional Key binding. The spec is decoded once into a JSON-normalized
// map, and one walk resolves every $ref on the way - at an entry's bindings, an
// operation's message, a oneOf variant, a message's bindings, the kafka
// binding, the key, and inside the schemas (ADR-0005). A message's traits are
// merged into it before it is read, and its payload is read only in a JSON
// Schema format (#81). What the walk cannot read is an error naming the
// Message type, never a silent fallback.
package asyncapi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// maxRefChain bounds a chain of $refs that point at further $refs, so a spec
// whose refs form a loop without content stops with an error.
const maxRefChain = 32

// Document is a parsed AsyncAPI 2.x spec.
type Document struct {
	raw map[string]any
}

// MessageType is one kind of message the spec declares for a Kafka topic. Its
// schemas are self-contained: a cyclic $ref points into the schema's own
// $defs, so they are used without the Document.
type MessageType struct {
	// Name is the message's name, else its component key, else where it is
	// declared, e.g. "orders publish oneOf[1]".
	Name string
	// Payload is the JSON Schema the Payload honours.
	Payload map[string]any
	// KeyBinding is the bindings.kafka.key schema, nil when none is declared.
	KeyBinding map[string]any
}

// Load reads and parses an AsyncAPI 2.x specification from a YAML or JSON
// file. A 3.x document is refused rather than half-read.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec file: %w", err)
	}
	raw, err := unmarshalRaw(data, path)
	if err != nil {
		return nil, err
	}
	if v, _ := raw["asyncapi"].(string); strings.HasPrefix(v, "3.") {
		return nil, fmt.Errorf("AsyncAPI %s is not supported yet: the tool reads AsyncAPI 2.x specs", v)
	}
	return &Document{raw: raw}, nil
}

// unmarshalRaw decodes the spec bytes, choosing YAML or JSON by suffix, and
// normalizes them through a JSON round-trip so every number is float64
// whichever format the source was.
func unmarshalRaw(data []byte, path string) (map[string]any, error) {
	var decoded any
	var err error
	if strings.HasSuffix(path, ".json") {
		err = json.Unmarshal(data, &decoded)
	} else {
		err = yaml.Unmarshal(data, &decoded)
	}
	if err != nil {
		return nil, fmt.Errorf("parsing spec: %w", err)
	}
	buf, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("normalizing spec: %w", err)
	}
	var normalized map[string]any
	if err := json.Unmarshal(buf, &normalized); err != nil {
		return nil, fmt.Errorf("parsing spec: the document is not an object")
	}
	return normalized, nil
}

// MessageTypes returns every Message type the spec declares for a Kafka topic,
// in a stable order: across the spec's entries for it (sorted by key), the
// publish then the subscribe operation, and oneOf variants in order. A
// component message referenced more than once is one Message type.
func (d *Document) MessageTypes(topic string) ([]MessageType, error) {
	channels, _ := d.raw["channels"].(map[string]any)
	keys := make([]string, 0, len(channels))
	for key := range channels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	c := &collector{d: d, seen: map[string]bool{}}
	found := false
	for _, key := range keys {
		entry, err := d.object(channels[key], "spec entry "+key)
		if err != nil {
			return nil, err
		}
		bound, err := d.kafkaTopic(entry, key)
		if err != nil {
			return nil, err
		}
		if bound != topic {
			continue
		}
		found = true
		if entry["messages"] != nil {
			return nil, fmt.Errorf("spec entry %s: messages on a spec entry is AsyncAPI 3.0 syntax; in 2.x, declare them under publish or subscribe (several as message.oneOf)", key)
		}
		for _, op := range []string{"publish", "subscribe"} {
			if err := c.operation(entry[op], key+" "+op); err != nil {
				return nil, err
			}
		}
	}

	switch {
	case !found:
		if entry, ok := channels[topic]; ok {
			if e, err := d.object(entry, "spec entry "+topic); err == nil {
				if bound, err := d.kafkaTopic(e, topic); err == nil {
					return nil, fmt.Errorf("Kafka topic %q not found in spec: the spec entry %s binds Kafka topic %q", topic, topic, bound)
				}
			}
		}
		return nil, fmt.Errorf("Kafka topic %q not found in spec", topic)
	case len(c.types) == 0:
		return nil, fmt.Errorf("Kafka topic %q: the spec declares no message for it", topic)
	}
	return c.types, nil
}

// kafkaTopic is the Kafka topic a spec entry keyed key stands for: its
// bindings.kafka.topic when declared, else the key itself.
func (d *Document) kafkaTopic(entry map[string]any, key string) (string, error) {
	where := "spec entry " + key
	kafka, err := d.kafkaBinding(entry["bindings"], where)
	if err != nil || kafka == nil {
		return key, err
	}
	switch topic := kafka["topic"].(type) {
	case nil:
		return key, nil
	case string:
		return topic, nil
	default:
		return "", fmt.Errorf("%s: bindings.kafka.topic must be a string", where)
	}
}

// kafkaBinding resolves a bindings object and its kafka binding, either of
// which may be a $ref. It returns nil when no kafka binding is declared, and
// an error when one is declared but is not an object.
func (d *Document) kafkaBinding(bindings any, where string) (map[string]any, error) {
	if bindings == nil {
		return nil, nil
	}
	b, err := d.object(bindings, where+": bindings")
	if err != nil {
		return nil, err
	}
	if b["kafka"] == nil {
		return nil, nil
	}
	return d.object(b["kafka"], where+": bindings.kafka")
}

// collector gathers the Message types of one Kafka topic.
type collector struct {
	d     *Document
	seen  map[string]bool
	types []MessageType
}

// operation adds the Message types of one publish or subscribe operation.
func (c *collector) operation(node any, where string) error {
	if node == nil {
		return nil
	}
	op, err := c.d.object(node, where)
	if err != nil {
		return err
	}
	if op["message"] == nil {
		return nil
	}
	return c.message(op["message"], where)
}

// message adds one message, or each variant of a oneOf message.
func (c *collector) message(node any, where string) error {
	ref := refOf(node)
	if ref != "" {
		if c.seen[ref] {
			return nil
		}
		c.seen[ref] = true
	}
	msg, err := c.d.object(node, where)
	if err != nil {
		return err
	}
	if variants, ok := msg["oneOf"].([]any); ok {
		for i, v := range variants {
			if err := c.message(v, fmt.Sprintf("%s oneOf[%d]", where, i)); err != nil {
				return err
			}
		}
		return nil
	}

	declared := messageName(msg, ref, where)
	traits, err := c.d.traits(msg, declared)
	if err != nil {
		return err
	}
	for _, trait := range traits { // 2.x: a trait overrides the message's own field
		merged, err := c.d.mergePatch(msg, trait, "message "+declared, 0)
		if err != nil {
			return err
		}
		msg = merged.(map[string]any)
	}
	mt, err := c.d.messageType(msg, messageName(msg, ref, where))
	if err != nil {
		return err
	}
	c.types = append(c.types, mt)
	return nil
}

// messageName names a message: its name, else its component key, else where
// it is declared.
func messageName(msg map[string]any, ref, where string) string {
	if n, ok := msg["name"].(string); ok && n != "" {
		return n
	}
	if ref != "" {
		return lastToken(ref)
	}
	return where
}

// messageType reads a message, its traits already merged, into a Message
// type: its payload, in a format the tool reads, and its Key binding.
func (d *Document) messageType(msg map[string]any, name string) (MessageType, error) {
	if msg["payload"] == nil {
		return MessageType{}, fmt.Errorf("message %s declares no payload", name)
	}
	if err := checkSchemaFormat(msg, name); err != nil {
		return MessageType{}, err
	}
	payload, err := d.schema(msg["payload"])
	if err != nil {
		return MessageType{}, fmt.Errorf("message %s: payload: %w", name, err)
	}
	mt := MessageType{Name: name, Payload: payload}

	kafka, err := d.kafkaBinding(msg["bindings"], "message "+name)
	if err != nil {
		return MessageType{}, err
	}
	if kafka != nil && kafka["key"] != nil {
		if _, ok := kafka["key"].(map[string]any); !ok {
			return MessageType{}, fmt.Errorf("message %s: bindings.kafka.key must be a schema object", name)
		}
		if mt.KeyBinding, err = d.schema(kafka["key"]); err != nil {
			return MessageType{}, fmt.Errorf("message %s: bindings.kafka.key: %w", name, err)
		}
	}
	return mt, nil
}

// object resolves node through any $ref chain and requires an object there.
func (d *Document) object(node any, where string) (map[string]any, error) {
	for i := 0; ; i++ {
		m, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: must be an object", where)
		}
		ref := refOf(m)
		if ref == "" {
			return m, nil
		}
		if i == maxRefChain {
			return nil, fmt.Errorf("%s: $ref chain longer than %d", where, maxRefChain)
		}
		target, err := d.pointer(ref)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		node = target
	}
}

// refOf returns the $ref of a node, or "" when it is not a reference.
func refOf(node any) string {
	m, _ := node.(map[string]any)
	ref, _ := m["$ref"].(string)
	return ref
}

// pointer resolves an internal reference: a JSON Pointer (RFC 6901) in a URI
// fragment, so percent-encoding decodes and ~1 and ~0 unescape to / and ~.
// External references are rejected (ADR-0005 decision 4).
func (d *Document) pointer(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("resolving $ref %s: external refs not supported", ref)
	}
	fragment, err := url.PathUnescape(ref[1:])
	if err != nil {
		return nil, fmt.Errorf("resolving $ref %s: %w", ref, err)
	}
	var current any = d.raw
	for _, token := range strings.Split(fragment[1:], "/") {
		token = unescapeToken(token)
		switch node := current.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return nil, fmt.Errorf("resolving $ref %s: %q not found", ref, token)
			}
			current = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(node) {
				return nil, fmt.Errorf("resolving $ref %s: no item %q", ref, token)
			}
			current = node[i]
		default:
			return nil, fmt.Errorf("resolving $ref %s: cannot descend into %q", ref, token)
		}
	}
	return current, nil
}

// schema resolves a schema node into a self-contained schema: every non-cyclic
// $ref expanded in place, and every cycle preserved as a $ref into the
// schema's own $defs, which holds the cycle's resolved target (ADR-0005,
// amended by #73). Resolution walks each path with a stack of the refs being
// expanded, so a diamond resolves in full and only a true cycle is kept.
func (d *Document) schema(node any) (map[string]any, error) {
	r := &resolver{d: d, defs: map[string]any{}}
	out, err := r.node(node, nil)
	if err != nil {
		return nil, err
	}
	m, ok := out.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the schema must be an object")
	}
	if len(r.defs) > 0 {
		defs, _ := m["$defs"].(map[string]any)
		if defs == nil {
			defs = map[string]any{}
		}
		for name, target := range r.defs {
			defs[name] = target
		}
		m["$defs"] = defs
	}
	return m, nil
}

// resolver expands one schema, collecting its cycles' targets in defs.
type resolver struct {
	d    *Document
	defs map[string]any
}

func (r *resolver) node(node any, stack []string) (any, error) {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["$ref"].(string); ok {
			for _, open := range stack {
				if open == ref {
					return r.cycle(ref)
				}
			}
			target, err := r.d.pointer(ref)
			if err != nil {
				return nil, err
			}
			return r.node(target, append(stack, ref))
		}
		out := make(map[string]any, len(n))
		for k, v := range n {
			rv, err := r.node(v, stack)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(n))
		for i, item := range n {
			rv, err := r.node(item, stack)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return node, nil
	}
}

// cycle returns the local $ref that preserves a cycle through ref, defining
// ref's target the first time.
func (r *resolver) cycle(ref string) (any, error) {
	name, err := r.define(ref)
	if err != nil {
		return nil, err
	}
	return map[string]any{"$ref": "#/$defs/" + escapeToken(name)}, nil
}

// define puts ref's target into defs under the reference's own pointer, so two
// targets never collide. The target is copied with each $ref inside it
// rewritten to a local one rather than expanded, and each of those defined in
// turn: every $ref the generator follows inside a cycle then costs one step of
// its depth budget, exactly as when it followed the spec's refs (ADR-0005).
func (r *resolver) define(ref string) (string, error) {
	name, err := url.PathUnescape(strings.TrimPrefix(ref, "#/"))
	if err != nil {
		return "", fmt.Errorf("resolving $ref %s: %w", ref, err)
	}
	if _, done := r.defs[name]; done {
		return name, nil
	}
	r.defs[name] = nil // claimed, so the target's own cycles stop here
	target, err := r.d.pointer(ref)
	if err != nil {
		return "", err
	}
	local, err := r.local(target)
	if err != nil {
		return "", err
	}
	r.defs[name] = local
	return name, nil
}

// local copies a node, rewriting each $ref in it to a local one.
func (r *resolver) local(node any) (any, error) {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["$ref"].(string); ok {
			return r.cycle(ref)
		}
		out := make(map[string]any, len(n))
		for k, v := range n {
			lv, err := r.local(v)
			if err != nil {
				return nil, err
			}
			out[k] = lv
		}
		return out, nil
	case []any:
		out := make([]any, len(n))
		for i, item := range n {
			lv, err := r.local(item)
			if err != nil {
				return nil, err
			}
			out[i] = lv
		}
		return out, nil
	default:
		return node, nil
	}
}

// lastToken is the final, unescaped token of a JSON Pointer reference.
func lastToken(ref string) string {
	return unescapeToken(ref[strings.LastIndex(ref, "/")+1:])
}

func escapeToken(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

func unescapeToken(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
}
