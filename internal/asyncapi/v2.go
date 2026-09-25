package asyncapi

import (
	"fmt"
	"slices"
	"sort"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
)

// The 2.x front end: a spec entry under channels: is a Kafka topic, its
// messages declared by its publish and subscribe operations.

// topic2 is Topic for a 2.x spec: across the spec entries bound to the Kafka
// topic (sorted by key), the publish then the subscribe operation, and oneOf
// variants in order. A templated key is bound when -topic fills it (#88), as
// a 3.0 address is; every bound entry must fill its template the same way,
// and that way gives the Topic parameters.
func (d *Document) topic2(name string) (*Topic, error) {
	channels, _ := d.raw["channels"].(map[string]any)
	keys := make([]string, 0, len(channels))
	for key := range channels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var entries []entry
	for _, key := range keys {
		e, err := d.object(channels[key], "spec entry "+key)
		if err != nil {
			return nil, err
		}
		bound, err := d.kafkaTopic2(e, key)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry{id: key, node: e, bound: bound})
	}
	matched, err := bind(name, entries)
	if err != nil {
		return nil, err
	}
	if len(matched) == 0 {
		return nil, d.notFound2(channels[name], name)
	}
	c := newCollector(d)
	messages := func(m match) error {
		if m.node["messages"] != nil {
			return fmt.Errorf("spec entry %s: messages on a spec entry is AsyncAPI 3.0 syntax; in 2.x, declare them under publish or subscribe (several as message.oneOf)", m.id)
		}
		for _, op := range []string{"publish", "subscribe"} {
			if err := c.operation(m.node[op], m.id+" "+op); err != nil {
				return err
			}
		}
		return nil
	}
	return d.topic(name, c, matched, messages, checkSchema)
}

// notFound2 is the error for a Kafka topic no spec entry is bound to, telling
// what a spec entry keyed by it stands for instead.
func (d *Document) notFound2(entry any, topic string) error {
	err := fmt.Errorf("Kafka topic %q not found in spec", topic)
	if entry == nil {
		return err
	}
	e, cerr := d.object(entry, "spec entry "+topic)
	if cerr != nil {
		return err
	}
	if bound, berr := d.kafkaTopic2(e, topic); berr == nil {
		return fmt.Errorf("%w: the spec entry %s binds Kafka topic %q", err, topic, bound.topic)
	}
	return err
}

// kafkaTopic2 is the Kafka topic a 2.x spec entry keyed key stands for: its
// bindings.kafka.topic when declared, taken literally, else the key itself,
// which may be a template.
func (d *Document) kafkaTopic2(entry map[string]any, key string) (boundTopic, error) {
	where := "spec entry " + key
	fromKey := boundTopic{topic: key, known: true, templated: templateParam.MatchString(key)}
	kafka, err := d.kafkaBinding(entry["bindings"], where)
	if err != nil || kafka == nil {
		return fromKey, err
	}
	switch topic := kafka["topic"].(type) {
	case nil:
		return fromKey, nil
	case string:
		return boundTopic{topic: topic, known: true}, nil
	default:
		return boundTopic{}, fmt.Errorf("%s: bindings.kafka.topic must be a string", where)
	}
}

// checkSchema refuses a value that does not conform to the parameter's
// declared schema, checked once at startup (ADR-0006 as amended by #83). A
// Topic parameter is a string, so a schema that does not allow one is refused
// whatever the value.
func checkSchema(d *Document, p map[string]any, v paramValue, where, topic string) error {
	if p["schema"] == nil {
		return nil
	}
	at := where + ": parameters." + v.name + ": schema"
	if _, err := d.object(p["schema"], at); err != nil {
		return err
	}
	schema, err := d.schema(p["schema"])
	if err != nil {
		return fmt.Errorf("%s: %w", at, err)
	}
	if !allowsString(schema["type"]) {
		return fmt.Errorf("%s: parameter %s: its schema does not allow a string, and a Topic parameter is a string", where, v.name)
	}
	if err := generator.Conforms(schema, v.value); err != nil {
		return fmt.Errorf("Kafka topic %q: %s value %s does not conform to the parameter's schema: %v", topic, v.name, v.value, err)
	}
	return nil
}

// allowsString reports whether a schema's type keyword admits a string: no
// type does, as does "string" alone or in a list.
func allowsString(t any) bool {
	switch t := t.(type) {
	case nil:
		return true
	case string:
		return t == "string"
	case []any:
		return slices.Contains(t, any("string"))
	}
	return false
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
	return c.message2(op["message"], where)
}

// message2 adds one 2.x message, or each variant of a oneOf message.
func (c *collector) message2(node any, where string) error {
	if !c.once(node) {
		return nil
	}
	ref := refOf(node)
	msg, err := c.d.object(node, where)
	if err != nil {
		return err
	}
	if variants, ok := msg["oneOf"].([]any); ok {
		for i, v := range variants {
			if err := c.message2(v, fmt.Sprintf("%s oneOf[%d]", where, i)); err != nil {
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
		merged, err := c.d.mergePatch(msg, trait, "message "+declared)
		if err != nil {
			return err
		}
		msg = merged.(map[string]any)
	}
	mt, err := c.d.messageType(msg, messageName(msg, ref, where), msg["payload"], msg["schemaFormat"])
	if err != nil {
		return err
	}
	c.types = append(c.types, mt)
	return nil
}
