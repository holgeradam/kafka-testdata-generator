package asyncapi

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Document represents a parsed AsyncAPI specification.
type Document struct {
	AsyncAPI string `yaml:"asyncapi" json:"asyncapi"`
	Info     Info   `yaml:"info" json:"info"`
	// Channels is AsyncAPI's channels: key, holding the spec's entries for
	// Kafka topics.
	Channels   map[string]ChannelItem `yaml:"channels" json:"channels"`
	Components *Components            `yaml:"components,omitempty" json:"components,omitempty"`

	// raw is the whole spec decoded once into a navigable, JSON-normalized map
	// that $ref resolution walks directly.
	raw map[string]any
}

type Info struct {
	Title       string `yaml:"title" json:"title"`
	Version     string `yaml:"version" json:"version"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

// ChannelItem is AsyncAPI's Channel Item Object: the spec's entry for a Kafka
// topic. Its key under channels: names the Kafka topic unless its Kafka
// binding declares another (bindings.kafka.topic).
type ChannelItem struct {
	Ref         string         `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	Bindings    map[string]any `yaml:"bindings,omitempty" json:"bindings,omitempty"`
	Messages    map[string]any `yaml:"messages,omitempty" json:"messages,omitempty"`
	Publish     *Operation     `yaml:"publish,omitempty" json:"publish,omitempty"`
	Subscribe   *Operation     `yaml:"subscribe,omitempty" json:"subscribe,omitempty"`
	Description string         `yaml:"description,omitempty" json:"description,omitempty"`
}

type Operation struct {
	Message any `yaml:"message,omitempty" json:"message,omitempty"`
}

type Message struct {
	Ref         string         `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	Name        string         `yaml:"name,omitempty" json:"name,omitempty"`
	Payload     map[string]any `yaml:"payload,omitempty" json:"payload,omitempty"`
	Bindings    map[string]any `yaml:"bindings,omitempty" json:"bindings,omitempty"`
	Description string         `yaml:"description,omitempty" json:"description,omitempty"`
}

type Components struct {
	Messages map[string]Message `yaml:"messages,omitempty" json:"messages,omitempty"`
	Schemas  map[string]any     `yaml:"schemas,omitempty" json:"schemas,omitempty"`
}

// Load reads and parses an AsyncAPI specification from a YAML or JSON file.
// The document is also decoded once into a navigable JSON-normalized map so
// that $ref resolution can walk it directly instead of re-marshalling per ref.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec file: %w", err)
	}

	var doc Document
	if err := decode(data, path, &doc); err != nil {
		return nil, fmt.Errorf("parsing spec: %w", err)
	}

	raw, err := unmarshalRaw(data, path)
	if err != nil {
		return nil, err
	}
	doc.raw = raw

	return &doc, nil
}

// decode unmarshals spec bytes into target, choosing YAML or JSON by suffix.
func decode(data []byte, path string, target any) error {
	if strings.HasSuffix(path, ".json") {
		return json.Unmarshal(data, target)
	}
	return yaml.Unmarshal(data, target)
}

// unmarshalRaw decodes the spec bytes into a navigable map and normalizes it
// through a JSON round-trip so every number is float64 regardless of whether
// the source was YAML (integers) or JSON. This keeps numbers consistent with
// the JSON round-trip the message structs already undergo.
func unmarshalRaw(data []byte, path string) (map[string]any, error) {
	var m map[string]any
	if err := decode(data, path, &m); err != nil {
		return nil, fmt.Errorf("parsing spec: %w", err)
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("normalizing spec: %w", err)
	}
	var normalized map[string]any
	if err := json.Unmarshal(buf, &normalized); err != nil {
		return nil, fmt.Errorf("normalizing spec: %w", err)
	}
	return normalized, nil
}

// KeyBinding extracts the message-level kafka key binding schema for the given
// Kafka topic. Returns nil when no binding is present. The returned schema has
// $ref nodes resolved so it can be fed directly to the Generator.
func (d *Document) KeyBinding(topic string) (map[string]any, error) {
	item, err := d.entryFor(topic)
	if err != nil {
		return nil, err
	}

	msg, err := d.resolveMessage(item)
	if err != nil {
		return nil, fmt.Errorf("Kafka topic %q: %w", topic, err)
	}

	if msg.Bindings == nil {
		return nil, nil
	}

	kafka, ok := msg.Bindings["kafka"].(map[string]any)
	if !ok {
		return nil, nil
	}

	keySchema, ok := kafka["key"].(map[string]any)
	if !ok {
		return nil, nil
	}

	resolved, err := d.resolveNode(keySchema, nil)
	if err != nil {
		return nil, fmt.Errorf("resolving key binding refs: %w", err)
	}

	out, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("key binding schema must be an object")
	}

	return out, nil
}

// PayloadSchema extracts the JSON Schema for the message payload of the given
// Kafka topic. If its spec entry has publish and subscribe operations, publish
// is preferred.
func (d *Document) PayloadSchema(topic string) (map[string]any, error) {
	item, err := d.entryFor(topic)
	if err != nil {
		return nil, err
	}

	msg, err := d.resolveMessage(item)
	if err != nil {
		return nil, fmt.Errorf("Kafka topic %q: %w", topic, err)
	}

	schema := msg.Payload
	if schema == nil {
		return nil, fmt.Errorf("no payload schema in message for Kafka topic %q", topic)
	}

	resolved, err := d.resolveNode(schema, nil)
	if err != nil {
		return nil, fmt.Errorf("resolving refs: %w", err)
	}
	out, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload schema for Kafka topic %q must be an object", topic)
	}

	// Cyclic refs are preserved as $ref nodes in `out`; the generator walks
	// them within its depth budget.
	return out, nil
}

// ResolveRef returns the schema node a $ref points at, for the generator to
// follow preserved cyclic $ref nodes within its depth budget.
func (d *Document) ResolveRef(ref string) (map[string]any, error) {
	v, err := d.resolveRef(ref)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("ref %s does not resolve to a schema object", ref)
	}
	return m, nil
}

// resolveNode expands $ref nodes along a single path. The stack holds the refs
// currently being expanded, so a ref that targets a node already on the stack
// (a cycle) is preserved as a $ref node; diamonds resolve correctly because
// each sibling starts a fresh path. Non-cyclic refs are fully expanded.
func (d *Document) resolveNode(node any, stack []string) (any, error) {
	if m, ok := node.(map[string]any); ok {
		if ref, isRef := m["$ref"].(string); isRef {
			if contains(stack, ref) {
				return deepCopy(m), nil
			}
			target, err := d.resolveRef(ref)
			if err != nil {
				return nil, fmt.Errorf("resolving $ref %s: %w", ref, err)
			}
			return d.resolveNode(target, append(stack, ref))
		}

		out := make(map[string]any, len(m))
		for k, v := range m {
			rv, err := d.resolveNode(v, stack)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	}

	if arr, ok := node.([]any); ok {
		out := make([]any, len(arr))
		for i, item := range arr {
			rv, err := d.resolveNode(item, stack)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	}

	return node, nil
}

func contains(stack []string, ref string) bool {
	for _, s := range stack {
		if s == ref {
			return true
		}
	}
	return false
}

// deepCopy clones a nested value built from maps, slices, and scalars.
func deepCopy(v any) any {
	switch n := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(n))
		for k, val := range n {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(n))
		for i, item := range n {
			out[i] = deepCopy(item)
		}
		return out
	default:
		return n
	}
}

// entryFor returns the spec's entry for a Kafka topic: the one entry whose
// Kafka binding names it, or whose key names it when it binds no other Kafka
// topic. Several entries bound to one Kafka topic are refused by name rather
// than one of them picked, until their Message types can be mixed (#74).
func (d *Document) entryFor(topic string) (ChannelItem, error) {
	var names []string
	for name, item := range d.Channels {
		if item.kafkaTopic(name) == topic {
			names = append(names, name)
		}
	}
	switch len(names) {
	case 0:
		if item, ok := d.Channels[topic]; ok {
			return ChannelItem{}, fmt.Errorf("Kafka topic %q not found in spec: the spec entry %s binds Kafka topic %q", topic, topic, item.kafkaTopic(topic))
		}
		return ChannelItem{}, fmt.Errorf("Kafka topic %q not found in spec", topic)
	case 1:
		return d.Channels[names[0]], nil
	}
	sort.Strings(names)
	return ChannelItem{}, fmt.Errorf("Kafka topic %q is bound by several spec entries (%s); mixing their Message types is not supported yet", topic, strings.Join(names, ", "))
}

// kafkaTopic is the Kafka topic an entry keyed name stands for: its
// bindings.kafka.topic when declared, else the key itself.
func (c ChannelItem) kafkaTopic(name string) string {
	if kafka, ok := c.Bindings["kafka"].(map[string]any); ok {
		if topic, ok := kafka["topic"].(string); ok && topic != "" {
			return topic
		}
	}
	return name
}

func (d *Document) resolveMessage(item ChannelItem) (*Message, error) {
	// Try publish first, then subscribe
	for _, op := range []*Operation{item.Publish, item.Subscribe} {
		if op == nil || op.Message == nil {
			continue
		}
		msgs, err := d.normalizeMessages(op.Message)
		if err != nil {
			continue
		}
		if len(msgs) > 0 {
			return msgs[0], nil
		}
	}

	// Try entry-level messages
	if item.Messages != nil {
		for _, v := range item.Messages {
			if m, ok := v.(map[string]any); ok {
				msg, err := messageFromMap(d, m)
				if err != nil {
					return nil, err
				}
				return msg, nil
			}
		}
	}

	return nil, fmt.Errorf("no message found in its spec entry")
}

func (d *Document) normalizeMessages(raw any) ([]*Message, error) {
	switch v := raw.(type) {
	case map[string]any:
		msg, err := messageFromMap(d, v)
		if err != nil {
			return nil, err
		}
		return []*Message{msg}, nil
	case []any:
		var msgs []*Message
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				msg, err := messageFromMap(d, m)
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, msg)
			}
		}
		return msgs, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %T", raw)
	}
}

// messageFromMap decodes one message map into a *Message, resolving a
// message-level $ref when present. Unlike resolveNode, which expands schema
// nodes into anonymous nested values, this works at the top-level message
// boundary and decodes into the typed Message struct.
func messageFromMap(d *Document, m map[string]any) (*Message, error) {
	msg := &Message{}
	if ref, ok := m["$ref"].(string); ok {
		resolved, err := d.resolveRef(ref)
		if err != nil {
			return nil, err
		}
		rm, ok := resolved.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("resolved message %s is not an object", ref)
		}
		if err := mapToStruct(rm, msg); err != nil {
			return nil, err
		}
	} else {
		if err := mapToStruct(m, msg); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// resolveRef resolves an internal (#/) reference by navigating the document's
// raw map directly. External refs are rejected.
func (d *Document) resolveRef(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("external refs not supported: %s", ref)
	}

	path := strings.TrimPrefix(ref, "#/")
	parts := strings.Split(path, "/")

	var current any = d.raw
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot navigate ref %s at %q", ref, part)
		}
		current = m[part]
		if current == nil {
			return nil, fmt.Errorf("ref %s not found at %q", ref, part)
		}
	}

	return current, nil
}

func mapToStruct(m map[string]any, target any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
