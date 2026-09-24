package asyncapi

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// The 3.0 front end: a channel under channels: stands for a Kafka topic, and
// its messages map holds every message sent to it (#76 decisions 1 and 2).
// Operations, and send versus receive, are never read.

// templateParam is one {parameter} of a templated channel address.
var templateParam = regexp.MustCompile(`\{[^{}]*\}`)

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
	var matched []match
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
		if !bound.known {
			continue
		}
		m := match{id: id, channel: ch, address: bound.topic}
		if bound.templated {
			ways := fill(bound.topic, name)
			if len(ways) > 1 {
				return nil, fmt.Errorf("Kafka topic %q fills the address %s of spec entry %s in more than one way, so its Topic parameters are ambiguous", name, bound.topic, id)
			}
			if len(ways) == 0 {
				continue
			}
			m.values = ways[0]
		} else if bound.topic != name {
			continue
		}
		matched = append(matched, m)
	}
	if len(matched) == 0 {
		return nil, d.notFound3(channels[name], name)
	}
	if err := sameWay(name, matched); err != nil {
		return nil, err
	}

	var params []TopicParameter
	for _, m := range matched {
		if err := c.channel(m.channel, m.id); err != nil {
			return nil, err
		}
		found, err := d.parameters(m, name)
		if err != nil {
			return nil, err
		}
		params = mergeParameters(params, found)
	}
	if len(c.types) == 0 {
		return nil, fmt.Errorf("Kafka topic %q: the spec declares no message for it", name)
	}
	return &Topic{MessageTypes: c.types, Parameters: params}, nil
}

// match is a channel bound to the Kafka topic, with the Topic parameter
// values -topic fills its address with, in address order (none for a literal
// address).
type match struct {
	id      string
	channel map[string]any
	address string
	values  []paramValue
}

type paramValue struct{ name, value string }

// describe names how the channel was matched, e.g. "byRegion (orders.{region}:
// region=eu)".
func (m match) describe() string {
	if len(m.values) == 0 {
		return fmt.Sprintf("%s (%s: no Topic parameters)", m.id, m.address)
	}
	parts := make([]string, len(m.values))
	for i, v := range m.values {
		parts[i] = v.name + "=" + v.value
	}
	return fmt.Sprintf("%s (%s: %s)", m.id, m.address, strings.Join(parts, ", "))
}

// sameWay refuses channels that fill their templates differently: the run
// would not know which Topic parameter values hold.
func sameWay(name string, matched []match) error {
	first := assignment(matched[0].values)
	for _, m := range matched[1:] {
		if !maps.Equal(first, assignment(m.values)) {
			described := make([]string, len(matched))
			for i, m := range matched {
				described[i] = m.describe()
			}
			return fmt.Errorf("Kafka topic %q matches spec entries in different ways: %s; its Topic parameters are ambiguous", name, strings.Join(described, ", "))
		}
	}
	return nil
}

func assignment(values []paramValue) map[string]string {
	out := make(map[string]string, len(values))
	for _, v := range values {
		out[v.name] = v.value
	}
	return out
}

// fill returns the ways topic fills a templated address, each parameter
// taking one or more characters, and stops at two: more than one way is
// already ambiguous. A parameter named twice must take one value.
func fill(address, topic string) [][]paramValue {
	literals := templateParam.Split(address, -1)
	names := templateParam.FindAllString(address, -1)
	for i, n := range names {
		names[i] = n[1 : len(n)-1]
	}
	if !strings.HasPrefix(topic, literals[0]) {
		return nil
	}
	var ways [][]paramValue
	var walk func(k, pos int, values []paramValue)
	walk = func(k, pos int, values []paramValue) {
		if len(ways) > 1 {
			return
		}
		if k == len(names) {
			if pos == len(topic) {
				ways = append(ways, slices.Clone(values))
			}
			return
		}
		next := literals[k+1]
		for end := pos + 1; end <= len(topic); end++ {
			if !strings.HasPrefix(topic[end:], next) {
				continue
			}
			value := topic[pos:end]
			if prior, ok := assignment(values)[names[k]]; ok && prior != value {
				continue
			}
			walk(k+1, end+len(next), append(values, paramValue{names[k], value}))
		}
	}
	walk(0, len(literals[0]), nil)
	return ways
}

// parameters reads the Topic parameters of one bound channel: each value
// -topic filled, checked against the parameter's enum, with its payload
// location when it declares one.
func (d *Document) parameters(m match, topic string) ([]TopicParameter, error) {
	if len(m.values) == 0 {
		return nil, nil
	}
	where := "spec entry " + m.id
	declared := map[string]any{}
	if m.channel["parameters"] != nil {
		var err error
		if declared, err = d.object(m.channel["parameters"], where+": parameters"); err != nil {
			return nil, err
		}
	}
	var out []TopicParameter
	seen := map[string]bool{}
	for _, v := range m.values {
		if seen[v.name] {
			continue
		}
		seen[v.name] = true
		tp := TopicParameter{Name: v.name, Value: v.value}
		if declared[v.name] != nil {
			p, err := d.object(declared[v.name], where+": parameters."+v.name)
			if err != nil {
				return nil, err
			}
			if err := checkEnum(p, v, where, topic); err != nil {
				return nil, err
			}
			if tp.Location, tp.Pointer, err = payloadLocation(p, v.name, where); err != nil {
				return nil, err
			}
		}
		out = append(out, tp)
	}
	return out, nil
}

// checkEnum refuses a value outside the parameter's declared enum.
func checkEnum(p map[string]any, v paramValue, where, topic string) error {
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

// payloadLocation reads a parameter's location, a runtime expression. A
// payload location gives the JSON Pointer tokens into the Payload; a header
// location stops the run, since the tool generates no Kafka record headers.
func payloadLocation(p map[string]any, name, where string) (string, []string, error) {
	if p["location"] == nil {
		return "", nil, nil
	}
	location, ok := p["location"].(string)
	if !ok {
		return "", nil, fmt.Errorf("%s: parameter %s: location must be a string", where, name)
	}
	const payload = "$message.payload"
	switch {
	case strings.HasPrefix(location, "$message.header"):
		return "", nil, fmt.Errorf("%s: parameter %s lives in message headers, which the tool does not generate", where, name)
	case location == payload || location == payload+"#":
		return "", nil, fmt.Errorf("%s: parameter %s: location %s names the whole Payload, not a field in it", where, name, location)
	case !strings.HasPrefix(location, payload+"#/"):
		return "", nil, fmt.Errorf("%s: parameter %s: location %s is not a $message.payload#/... runtime expression", where, name, location)
	}
	tokens := strings.Split(strings.TrimPrefix(location, payload+"#/"), "/")
	for i, t := range tokens {
		tokens[i] = unescapeToken(t)
	}
	return location, tokens, nil
}

// mergeParameters adds a channel's Topic parameters to those already found:
// each name once, and once more per further distinct location.
func mergeParameters(have, found []TopicParameter) []TopicParameter {
	for _, f := range found {
		i := slices.IndexFunc(have, func(h TopicParameter) bool { return h.Name == f.Name })
		switch {
		case i < 0:
			have = append(have, f)
		case f.Location == "" || slices.ContainsFunc(have, func(h TopicParameter) bool { return h.Name == f.Name && h.Location == f.Location }):
			// nothing new: the name is known, and so is this location
		case have[i].Location == "":
			have[i] = f
		default:
			have = append(have, f)
		}
	}
	return have
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

// boundTopic is what a 3.0 channel says about its Kafka topic.
type boundTopic struct {
	topic string
	// known is false when the channel names no Kafka topic: no binding, and a
	// null or absent address, which 3.0 says "MUST be interpreted as unknown".
	known bool
	// templated is an address with Topic parameters, e.g. orders.{region}.
	templated bool
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
