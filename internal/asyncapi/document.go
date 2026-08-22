package asyncapi

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Document represents a parsed AsyncAPI specification.
type Document struct {
	AsyncAPI    string              `yaml:"asyncapi" json:"asyncapi"`
	Info        Info                `yaml:"info" json:"info"`
	Channels    map[string]Channel  `yaml:"channels" json:"channels"`
	Components  *Components         `yaml:"components,omitempty" json:"components,omitempty"`
}

type Info struct {
	Title       string `yaml:"title" json:"title"`
	Version     string `yaml:"version" json:"version"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

type Channel struct {
	Ref         string            `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	Messages    map[string]any    `yaml:"messages,omitempty" json:"messages,omitempty"`
	Publish     *Operation        `yaml:"publish,omitempty" json:"publish,omitempty"`
	Subscribe   *Operation        `yaml:"subscribe,omitempty" json:"subscribe,omitempty"`
	Description string            `yaml:"description,omitempty" json:"description,omitempty"`
}

type Operation struct {
	Message     any    `yaml:"message,omitempty" json:"message,omitempty"`
}

type Message struct {
	Ref         string         `yaml:"$ref,omitempty" json:"$ref,omitempty"`
	Name        string         `yaml:"name,omitempty" json:"name,omitempty"`
	Payload     map[string]any `yaml:"payload,omitempty" json:"payload,omitempty"`
	Description string         `yaml:"description,omitempty" json:"description,omitempty"`
}

type Components struct {
	Messages map[string]Message `yaml:"messages,omitempty" json:"messages,omitempty"`
	Schemas  map[string]any     `yaml:"schemas,omitempty" json:"schemas,omitempty"`
}

// Load reads and parses an AsyncAPI specification from a YAML or JSON file.
func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec file: %w", err)
	}

	var doc Document
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parsing JSON spec: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parsing YAML spec: %w", err)
		}
	}

	return &doc, nil
}

// PayloadSchema extracts the JSON Schema for the message payload of the given channel.
// If the channel has publish and subscribe operations, publish is preferred.
// For oneOf messages, the first variant is returned.
func (d *Document) PayloadSchema(channel string) (map[string]any, error) {
	ch, ok := d.Channels[channel]
	if !ok {
		return nil, fmt.Errorf("channel %q not found in spec", channel)
	}

	msg, err := d.resolveChannelMessage(ch)
	if err != nil {
		return nil, err
	}

	schema := msg.Payload
	if schema == nil {
		return nil, fmt.Errorf("no payload schema in message for channel %q", channel)
	}

	schema = normalizeMap(schema)
	if err := d.resolveRefs(schema); err != nil {
		return nil, fmt.Errorf("resolving refs: %w", err)
	}

	return schema, nil
}

func (d *Document) resolveChannelMessage(ch Channel) (*Message, error) {
	// Try publish first, then subscribe
	for _, op := range []*Operation{ch.Publish, ch.Subscribe} {
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

	// Try channel-level messages
	if ch.Messages != nil {
		for _, v := range ch.Messages {
			if m, ok := v.(map[string]any); ok {
				msg := &Message{}
				if ref, ok := m["$ref"].(string); ok {
					resolved, err := d.resolveRef(ref)
					if err != nil {
						return nil, err
					}
					if rm, ok := resolved.(map[string]any); ok {
						if err := mapToStruct(rm, msg); err != nil {
							return nil, err
						}
					}
				} else {
					if err := mapToStruct(m, msg); err != nil {
						return nil, err
					}
				}
				return msg, nil
			}
		}
	}

	return nil, fmt.Errorf("no message found for channel")
}

func (d *Document) normalizeMessages(raw any) ([]*Message, error) {
	switch v := raw.(type) {
	case map[string]any:
		msg := &Message{}
		if ref, ok := v["$ref"].(string); ok {
			resolved, err := d.resolveRef(ref)
			if err != nil {
				return nil, err
			}
			if rm, ok := resolved.(map[string]any); ok {
				if err := mapToStruct(rm, msg); err != nil {
					return nil, err
				}
			}
		} else {
			if err := mapToStruct(v, msg); err != nil {
				return nil, err
			}
		}
		return []*Message{msg}, nil
	case []any:
		var msgs []*Message
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				msg := &Message{}
				if ref, ok := m["$ref"].(string); ok {
					resolved, err := d.resolveRef(ref)
					if err != nil {
						return nil, err
					}
					if rm, ok := resolved.(map[string]any); ok {
						if err := mapToStruct(rm, msg); err != nil {
							return nil, err
						}
					}
				} else {
					if err := mapToStruct(m, msg); err != nil {
						return nil, err
					}
				}
				msgs = append(msgs, msg)
			}
		}
		return msgs, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %T", raw)
	}
}

func (d *Document) resolveRef(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("external refs not supported: %s", ref)
	}

	// Convert document to map for navigation
	data, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("marshaling document: %w", err)
	}
	var docMap map[string]any
	if err := json.Unmarshal(data, &docMap); err != nil {
		return nil, fmt.Errorf("unmarshaling document: %w", err)
	}

	path := strings.TrimPrefix(ref, "#/")
	parts := strings.Split(path, "/")

	var current any = docMap
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

func (d *Document) resolveRefs(schema map[string]any) error {
	return d.resolveRefsWithSeen(schema, make(map[string]bool))
}

func (d *Document) resolveRefsWithSeen(schema map[string]any, seen map[string]bool) error {
	for key, val := range schema {
		if key == "$ref" {
			if ref, ok := val.(string); ok {
				if seen[ref] {
					delete(schema, "$ref")
					continue
				}
				seen[ref] = true
				resolved, err := d.resolveRef(ref)
				if err != nil {
					return fmt.Errorf("resolving $ref %s: %w", ref, err)
				}
				if rm, ok := resolved.(map[string]any); ok {
					for k, v := range rm {
						schema[k] = v
					}
					delete(schema, "$ref")
					return d.resolveRefsWithSeen(schema, seen)
				}
			}
		}

		if nested, ok := val.(map[string]any); ok {
			if err := d.resolveRefsWithSeen(nested, seen); err != nil {
				return err
			}
		}

		if arr, ok := val.([]any); ok {
			for _, item := range arr {
				if nested, ok := item.(map[string]any); ok {
					if err := d.resolveRefsWithSeen(nested, seen); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func normalizeMap(raw map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range raw {
		result[k] = v
	}
	return result
}

func mapToStruct(m map[string]any, target any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
