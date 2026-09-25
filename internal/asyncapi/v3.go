package asyncapi

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// The 3.0 front end: a channel under channels: stands for a Kafka topic, and
// its messages map holds every message sent to it (#76 decisions 1 and 2).
// Operations, and send versus receive, are never read.

// topic3 is Topic for a 3.0 spec: across the channels bound to the Kafka
// topic (sorted by id), each message in the channel's messages map (sorted by
// key). A channel referenced more than once is read once. A templated address
// is bound when -topic fills it (#83); every bound channel must fill its
// template the same way, and that way gives the Topic parameters.
func (d *Document) topic3(name string) (*Topic, error) {
	channels, _ := d.raw["channels"].(map[string]any)
	ids := make([]string, 0, len(channels))
	for id := range channels {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	c := newCollector(d)
	var entries []entry
	for _, id := range ids {
		if !c.once(channels[id]) {
			continue
		}
		ch, err := d.object(channels[id], "spec entry "+id)
		if err != nil {
			return nil, err
		}
		bound, err := d.kafkaTopic3(ch, id)
		if err != nil {
			return nil, err
		}
		if bound.known {
			entries = append(entries, entry{id: id, node: ch, bound: bound})
		}
	}
	matched, err := bind(name, entries)
	if err != nil {
		return nil, err
	}
	if len(matched) == 0 {
		return nil, d.notFound3(channels[name], name)
	}
	return d.topic(name, c, matched, func(m match) error { return c.channel(m.node, m.id) }, checkEnum)
}

// checkEnum refuses a value outside the parameter's declared enum.
func checkEnum(_ *Document, p map[string]any, v paramValue, where, topic string) error {
	if p["enum"] == nil {
		return nil
	}
	list, ok := p["enum"].([]any)
	if !ok {
		return fmt.Errorf("%s: parameter %s: enum must be a list of strings", where, v.name)
	}
	symbols := make([]string, len(list))
	for i, e := range list {
		if symbols[i], ok = e.(string); !ok {
			return fmt.Errorf("%s: parameter %s: enum must be a list of strings", where, v.name)
		}
	}
	if !slices.Contains(symbols, v.value) {
		return fmt.Errorf("Kafka topic %q: %s value %s is not in the parameter's enum [%s]", topic, v.name, v.value, strings.Join(symbols, ", "))
	}
	return nil
}

// notFound3 is the error for a Kafka topic no channel is bound to, telling
// what a channel whose id equals it stands for instead.
func (d *Document) notFound3(entry any, topic string) error {
	err := fmt.Errorf("Kafka topic %q not found in spec", topic)
	if entry == nil {
		return err
	}
	ch, cerr := d.object(entry, "spec entry "+topic)
	if cerr != nil {
		return err
	}
	bound, berr := d.kafkaTopic3(ch, topic)
	switch {
	case berr != nil:
		return err
	case !bound.known:
		return fmt.Errorf("%w: the spec entry %s has no address, so it names no Kafka topic", err, topic)
	default:
		return fmt.Errorf("%w: the spec entry %s names Kafka topic %q", err, topic, bound.topic)
	}
}

// kafkaTopic3 is the Kafka topic a 3.0 channel stands for: its
// bindings.kafka.topic when declared, else its address. The channel id never
// names a Kafka topic.
func (d *Document) kafkaTopic3(ch map[string]any, id string) (boundTopic, error) {
	where := "spec entry " + id
	kafka, err := d.kafkaBinding(ch["bindings"], where)
	if err != nil {
		return boundTopic{}, err
	}
	if kafka != nil && kafka["topic"] != nil {
		topic, ok := kafka["topic"].(string)
		if !ok {
			return boundTopic{}, fmt.Errorf("%s: bindings.kafka.topic must be a string", where)
		}
		return boundTopic{topic: topic, known: true}, nil
	}
	switch address := ch["address"].(type) {
	case nil:
		return boundTopic{}, nil
	case string:
		return boundTopic{topic: address, known: true, templated: templateParam.MatchString(address)}, nil
	default:
		return boundTopic{}, fmt.Errorf("%s: address must be a string", where)
	}
}

// channel adds the Message types of one 3.0 channel's messages map.
func (c *collector) channel(ch map[string]any, id string) error {
	where := "spec entry " + id
	for _, op := range []string{"publish", "subscribe"} {
		if ch[op] != nil {
			return fmt.Errorf("%s: %s is AsyncAPI 2.x syntax; in 3.0, declare the channel's messages under messages", where, op)
		}
	}
	if ch["messages"] == nil {
		return nil
	}
	messages, ok := ch["messages"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s: messages must be an object", where)
	}
	keys := make([]string, 0, len(messages))
	for key := range messages {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.message3(messages[key], where+": messages."+key, key); err != nil {
			return err
		}
	}
	return nil
}

// message3 adds one 3.0 message, named by its name, else its component key,
// else its key in the channel's messages map.
func (c *collector) message3(node any, where, key string) error {
	if !c.once(node) {
		return nil
	}
	msg, err := c.d.object(node, where)
	if err != nil {
		return err
	}
	name := messageName(msg, refOf(node), key)
	traits, err := c.d.traits(msg, name)
	if err != nil {
		return err
	}
	if len(traits) > 0 { // 3.0: traits merge in order, then the message wins
		var base any = map[string]any{}
		for _, trait := range traits {
			if base, err = c.d.mergePatch(base.(map[string]any), trait, "message "+name); err != nil {
				return err
			}
		}
		merged, err := c.d.underlay(msg, base, "message "+name)
		if err != nil {
			return err
		}
		msg = merged.(map[string]any)
	}
	payload, format, err := c.d.payload3(msg, name)
	if err != nil {
		return err
	}
	mt, err := c.d.messageType(msg, name, payload, format)
	if err != nil {
		return err
	}
	c.types = append(c.types, mt)
	return nil
}

// payload3 finds a 3.0 message's payload schema and its format. The payload
// is a Schema Object, or a Multi Format Schema Object {schemaFormat, schema};
// either may be a $ref. A plain schema is returned as declared, $ref and all,
// so its cycles resolve exactly as a 2.x payload's.
func (d *Document) payload3(msg map[string]any, name string) (payload, format any, err error) {
	if msg["schemaFormat"] != nil {
		return nil, nil, fmt.Errorf("message %s: schemaFormat on a message is AsyncAPI 2.x syntax; in 3.0, declare the payload as {schemaFormat, schema}", name)
	}
	if msg["payload"] == nil {
		return nil, nil, nil
	}
	resolved, err := d.object(msg["payload"], "message "+name+": payload")
	if err != nil {
		return nil, nil, err
	}
	if _, multi := resolved["schemaFormat"]; !multi {
		return msg["payload"], nil, nil
	}
	if resolved["schema"] == nil {
		return nil, nil, fmt.Errorf("message %s: payload declares a schemaFormat but no schema", name)
	}
	return resolved["schema"], resolved["schemaFormat"], nil
}
