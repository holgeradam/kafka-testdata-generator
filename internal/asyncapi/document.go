// Package asyncapi reads an AsyncAPI 2.x or 3.0 spec into what a run needs:
// the Message types the spec declares for a Kafka topic, each with a Payload
// schema and an optional Key binding. The spec is decoded once into a
// JSON-normalized map. A front end per version finds the spec entries for a
// Kafka topic and their messages (v2.go, v3.go); both hand each message to one
// back end that merges its traits, refuses a payload format the tool does not
// read (#81), and resolves every $ref on the way - at bindings, messages, the
// key and inside the schemas (ADR-0005). What the walk cannot read is an error
// naming the Message type, never a silent fallback.
package asyncapi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// maxRefChain bounds a chain of $refs that point at further $refs, so a spec
// whose refs form a loop without content stops with an error.
const maxRefChain = 32

// Document is a parsed AsyncAPI 2.x or 3.x spec.
type Document struct {
	raw map[string]any
	// major is the spec's AsyncAPI major version, 2 or 3.
	major int
}

// MessageType is one kind of message the spec declares for a Kafka topic. Its
// schemas are self-contained: a cyclic $ref points into the schema's own
// $defs, so they are used without the Document.
type MessageType struct {
	// Name is the message's name, else its component key, else where it is
	// declared, e.g. "orders publish oneOf[1]".
	Name string
	// Payload is the JSON Schema the Payload honours; nil when the payload is
	// Avro, and Avsc holds it.
	Payload map[string]any
	// KeyBinding is the bindings.kafka.key schema, nil when none is declared
	// or the payload is Avro.
	KeyBinding map[string]any
	// Avsc is the Payload's avsc when the message declares an Avro
	// schemaFormat (#84), as JSON with its $refs expanded; nil for a JSON
	// Schema payload.
	Avsc []byte
	// KeyAvsc is bindings.kafka.key read as an avsc beside an Avro payload
	// (#84 decision 7); nil when none is declared or the payload is JSON
	// Schema.
	KeyAvsc []byte
	// Registry is what the Kafka message binding declares about the schema
	// registry, for the AVRO Wire format to judge (#84 decision 8).
	Registry RegistryBinding
	// Headers is the JSON Schema of the message's headers, an object whose
	// properties are the Kafka record headers (#85); nil when none are
	// declared.
	Headers map[string]any
}

// RegistryBinding holds the schema-registry fields of a Kafka message binding
// as written, a number as its decimal text; each empty when absent.
type RegistryBinding struct {
	SchemaIDLocation        string
	SchemaIDPayloadEncoding string
	SchemaLookupStrategy    string
}

// Load reads and parses an AsyncAPI 2.x or 3.x specification from a YAML or
// JSON file.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec file: %w", err)
	}
	raw, err := unmarshalRaw(data, path)
	if err != nil {
		return nil, err
	}
	major := 2
	if v, _ := raw["asyncapi"].(string); strings.HasPrefix(v, "3.") {
		major = 3
	}
	return &Document{raw: raw, major: major}, nil
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

// Topic is what the spec declares for one Kafka topic.
type Topic struct {
	// MessageTypes are the Message types of the Kafka topic, in a stable
	// order. A component message referenced more than once is one.
	MessageTypes []MessageType
	// Parameters are the Topic parameters -topic fills in a templated 3.0
	// address or 2.x spec entry key, in template order; none for a literal
	// one.
	Parameters []TopicParameter
}

// TopicParameter is a named placeholder in a Kafka topic's address template,
// such as region in orders.{region}, with the value -topic fills it with.
type TopicParameter struct {
	Name  string
	Value string
	// Location is the parameter's payload location as written, e.g.
	// $message.payload#/region, and Pointer its JSON Pointer tokens into the
	// Payload; both empty when it declares none. The same parameter appears
	// once per distinct location the spec entries declare for it.
	Location string
	Pointer  []string
}

// Topic reads what the spec declares for a Kafka topic.
func (d *Document) Topic(name string) (*Topic, error) {
	if d.major == 3 {
		return d.topic3(name)
	}
	return d.topic2(name)
}

// collector gathers the Message types of one Kafka topic, reading each
// referenced component once.
type collector struct {
	d     *Document
	seen  map[string]bool
	types []MessageType
}

func newCollector(d *Document) *collector {
	return &collector{d: d, seen: map[string]bool{}}
}

// once reports whether node should be read: true unless it is a $ref already
// read.
func (c *collector) once(node any) bool {
	ref := refOf(node)
	if ref == "" {
		return true
	}
	if c.seen[ref] {
		return false
	}
	c.seen[ref] = true
	return true
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

// messageName names a message: its name, else its component key, else the
// front end's fallback.
func messageName(msg map[string]any, ref, fallback string) string {
	if n, ok := msg["name"].(string); ok && n != "" {
		return n
	}
	if ref != "" {
		return lastToken(ref)
	}
	return fallback
}

// messageType reads a message, its traits already merged, into a Message
// type: its payload schema, which the front end found with its schemaFormat
// (nil when none is declared), its Key binding, read in the payload's format,
// and the binding's registry fields.
func (d *Document) messageType(msg map[string]any, name string, payloadNode, format any) (MessageType, error) {
	if payloadNode == nil {
		return MessageType{}, fmt.Errorf("message %s declares no payload", name)
	}
	pf, err := d.payloadFormatOf(format, name)
	if err != nil {
		return MessageType{}, err
	}
	kafka, err := d.kafkaBinding(msg["bindings"], "message "+name)
	if err != nil {
		return MessageType{}, err
	}
	mt := MessageType{Name: name}
	if kafka != nil {
		mt.Registry = RegistryBinding{text(kafka["schemaIdLocation"]), text(kafka["schemaIdPayloadEncoding"]), text(kafka["schemaLookupStrategy"])}
	}
	if mt.Headers, err = d.headers(msg, name); err != nil {
		return MessageType{}, err
	}

	if pf == avroSchema {
		if mt.Avsc, err = d.avsc(payloadNode); err != nil {
			return MessageType{}, fmt.Errorf("message %s: payload: %w", name, err)
		}
		if kafka != nil && kafka["key"] != nil {
			if mt.KeyAvsc, err = d.avsc(kafka["key"]); err != nil {
				return MessageType{}, fmt.Errorf("message %s: bindings.kafka.key: %w", name, err)
			}
		}
		return mt, nil
	}

	if mt.Payload, err = d.schema(payloadNode); err != nil {
		return MessageType{}, fmt.Errorf("message %s: payload: %w", name, err)
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

// headers reads a message's headers schema, which both versions require to
// be an object: each of its properties is one Kafka record header (#85). A
// 3.0 message may declare it as a Multi Format Schema, which must be JSON
// Schema; in 2.x, schemaFormat speaks of the payload only.
func (d *Document) headers(msg map[string]any, name string) (map[string]any, error) {
	node := msg["headers"]
	if node == nil {
		return nil, nil
	}
	if d.major == 3 {
		resolved, err := d.object(node, "message "+name+": headers")
		if err != nil {
			return nil, err
		}
		if format, multi := resolved["schemaFormat"]; multi {
			if resolved["schema"] == nil {
				return nil, fmt.Errorf("message %s: headers declare a schemaFormat but no schema", name)
			}
			f, _ := format.(string)
			if pf, ok := readsSchemaFormat(f, d.major); !ok || pf != jsonSchema {
				return nil, fmt.Errorf("headers of %s are %v; the tool reads headers in JSON Schema only", name, format)
			}
			node = resolved["schema"]
		}
	}
	schema, err := d.schema(node)
	if err != nil {
		return nil, fmt.Errorf("message %s: headers: %w", name, err)
	}
	if schema["type"] != "object" {
		return nil, fmt.Errorf("message %s: headers must be a schema of type object, whose properties are the Kafka record headers", name)
	}
	return schema, nil
}

// text is a declared scalar as written, "" when absent.
func text(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
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
