package asyncapi

import (
	"fmt"
	"sort"
)

// The 2.x front end: a spec entry under channels: is a Kafka topic, its
// messages declared by its publish and subscribe operations.

// messageTypes2 is MessageTypes for a 2.x spec: across the spec's entries for
// the Kafka topic (sorted by key), the publish then the subscribe operation,
// and oneOf variants in order.
func (d *Document) messageTypes2(topic string) ([]MessageType, error) {
	channels, _ := d.raw["channels"].(map[string]any)
	keys := make([]string, 0, len(channels))
	for key := range channels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	c := newCollector(d)
	found := false
	for _, key := range keys {
		entry, err := d.object(channels[key], "spec entry "+key)
		if err != nil {
			return nil, err
		}
		bound, err := d.kafkaTopic2(entry, key)
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
				if bound, err := d.kafkaTopic2(e, topic); err == nil {
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

// kafkaTopic2 is the Kafka topic a 2.x spec entry keyed key stands for: its
// bindings.kafka.topic when declared, else the key itself.
func (d *Document) kafkaTopic2(entry map[string]any, key string) (string, error) {
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
