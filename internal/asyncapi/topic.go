package asyncapi

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// The back end both front ends share for finding a Kafka topic's spec entries
// (#76 decision 3): each front end lists its spec entries with the Kafka topic
// each stands for, and a templated one is bound when -topic fills it (#83 for
// 3.0 addresses, #88 for 2.x keys). Only reading the messages and checking a
// Topic parameter's value differ between the versions.

// templateParam is one {parameter} of a templated address.
var templateParam = regexp.MustCompile(`\{[^{}]*\}`)

// entry is one spec entry as a front end reads it: a 3.0 channel under its
// id, or a 2.x channel item under its key, with the Kafka topic it stands for.
type entry struct {
	id    string
	node  map[string]any
	bound boundTopic
}

// valueCheck is a front end's check of the value -topic fills a Topic
// parameter with against the parameter's declared Parameter Object p: 3.0's
// enum, 2.x's schema.
type valueCheck func(d *Document, p map[string]any, v paramValue, where, topic string) error

// bind returns the spec entries bound to the Kafka topic name, with the Topic
// parameter values each fills its template with. It refuses a template filled
// in more than one way, and entries that fill theirs differently.
func bind(name string, entries []entry) ([]match, error) {
	var matched []match
	for _, e := range entries {
		m := match{entry: e}
		if e.bound.templated {
			ways := fill(e.bound.topic, name)
			if len(ways) > 1 {
				return nil, fmt.Errorf("Kafka topic %q fills the address %s of spec entry %s in more than one way, so its Topic parameters are ambiguous", name, e.bound.topic, e.id)
			}
			if len(ways) == 0 {
				continue
			}
			m.values = ways[0]
		} else if e.bound.topic != name {
			continue
		}
		matched = append(matched, m)
	}
	if len(matched) > 0 {
		if err := sameWay(name, matched); err != nil {
			return nil, err
		}
	}
	return matched, nil
}

// topic reads the Kafka topic name from the spec entries bound to it: the
// Message types messages adds for each entry, and the Topic parameters, each
// value checked by check.
func (d *Document) topic(name string, c *collector, matched []match, messages func(match) error, check valueCheck) (*Topic, error) {
	var params []TopicParameter
	for _, m := range matched {
		if err := messages(m); err != nil {
			return nil, err
		}
		found, err := d.parameters(m, name, check)
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

// match is a spec entry bound to the Kafka topic, with the Topic parameter
// values -topic fills its template with, in template order (none for a
// literal one).
type match struct {
	entry
	values []paramValue
}

type paramValue struct{ name, value string }

// describe names how the spec entry was matched, e.g. "byRegion (orders.{region}:
// region=eu)".
func (m match) describe() string {
	if len(m.values) == 0 {
		return fmt.Sprintf("%s (%s: no Topic parameters)", m.id, m.bound.topic)
	}
	parts := make([]string, len(m.values))
	for i, v := range m.values {
		parts[i] = v.name + "=" + v.value
	}
	return fmt.Sprintf("%s (%s: %s)", m.id, m.bound.topic, strings.Join(parts, ", "))
}

// sameWay refuses spec entries that fill their templates differently: the run
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

// parameters reads the Topic parameters of one bound spec entry: each value
// -topic filled, checked by check against the declared parameter, with its
// payload location when it declares one.
func (d *Document) parameters(m match, topic string, check valueCheck) ([]TopicParameter, error) {
	if len(m.values) == 0 {
		return nil, nil
	}
	where := "spec entry " + m.id
	declared := map[string]any{}
	if m.node["parameters"] != nil {
		var err error
		if declared, err = d.object(m.node["parameters"], where+": parameters"); err != nil {
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
			if err := check(d, p, v, where, topic); err != nil {
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

// mergeParameters adds a spec entry's Topic parameters to those already found:
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

// boundTopic is what a spec entry says about its Kafka topic.
type boundTopic struct {
	topic string
	// known is false when a 3.0 channel names no Kafka topic: no binding, and
	// a null or absent address, which 3.0 says "MUST be interpreted as
	// unknown". A 2.x spec entry always names one.
	known bool
	// templated is a 3.0 address or 2.x key with Topic parameters, e.g.
	// orders.{region}. A bindings.kafka.topic is never templated.
	templated bool
}
