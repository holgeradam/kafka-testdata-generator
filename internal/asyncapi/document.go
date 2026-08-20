package asyncapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type Document struct {
	root map[string]any
}

type Selection struct {
	Channel string
	Message string
}

func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var root any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}

	normalized := normalize(root)
	object, ok := normalized.(map[string]any)
	if !ok {
		return nil, errors.New("spec root must be an object")
	}

	return &Document{root: object}, nil
}

func (d *Document) PayloadSchema(selection Selection) (map[string]any, Selection, error) {
	channels, ok := objectAt(d.root, "channels")
	if !ok || len(channels) == 0 {
		return nil, Selection{}, errors.New("spec does not contain channels")
	}

	channelName := selection.Channel
	if channelName == "" {
		channelName = firstKey(channels)
	}

	channel, ok := channels[channelName].(map[string]any)
	if !ok {
		return nil, Selection{}, fmt.Errorf("channel %q not found or not an object", channelName)
	}

	messageValue, messageName, err := selectMessage(channel, selection.Message)
	if err != nil {
		return nil, Selection{}, fmt.Errorf("channel %q: %w", channelName, err)
	}

	messageValue, err = d.resolve(messageValue)
	if err != nil {
		return nil, Selection{}, err
	}

	messageObject, ok := messageValue.(map[string]any)
	if !ok {
		return nil, Selection{}, fmt.Errorf("message %q is not an object", messageName)
	}

	payload, ok := messageObject["payload"]
	if !ok {
		return nil, Selection{}, fmt.Errorf("message %q does not define a payload", messageName)
	}

	resolvedPayload, err := d.resolveDeep(payload, map[string]struct{}{})
	if err != nil {
		return nil, Selection{}, err
	}

	schema, ok := resolvedPayload.(map[string]any)
	if !ok {
		return nil, Selection{}, fmt.Errorf("message %q payload is not a schema object", messageName)
	}

	return schema, Selection{Channel: channelName, Message: messageName}, nil
}

func selectMessage(channel map[string]any, requested string) (any, string, error) {
	if messages, ok := channel["messages"].(map[string]any); ok {
		return selectMessageMap(messages, requested)
	}

	for _, operation := range []string{"publish", "subscribe"} {
		op, ok := channel[operation].(map[string]any)
		if !ok {
			continue
		}

		message, ok := op["message"]
		if !ok {
			continue
		}

		return selectMessageValue(message, requested)
	}

	return nil, "", errors.New("no channel message found")
}

func selectMessageMap(messages map[string]any, requested string) (any, string, error) {
	if len(messages) == 0 {
		return nil, "", errors.New("channel messages map is empty")
	}

	if requested != "" {
		if message, ok := messages[requested]; ok {
			return message, requested, nil
		}
		for _, key := range sortedKeys(messages) {
			message := messages[key]
			if inferredMessageName(message, key) == requested {
				return message, requested, nil
			}
		}
		return nil, "", fmt.Errorf("message %q not found in channel messages", requested)
	}

	key := firstKey(messages)
	return messages[key], inferredMessageName(messages[key], key), nil
}

func selectMessageValue(message any, requested string) (any, string, error) {
	messageObject, _ := message.(map[string]any)
	if oneOf, ok := messageObject["oneOf"].([]any); ok {
		if len(oneOf) == 0 {
			return nil, "", errors.New("message oneOf list is empty")
		}
		if requested == "" {
			return oneOf[0], inferredMessageName(oneOf[0], "0"), nil
		}
		for i, candidate := range oneOf {
			if inferredMessageName(candidate, fmt.Sprint(i)) == requested {
				return candidate, requested, nil
			}
		}
		return nil, "", fmt.Errorf("message %q not found in oneOf list", requested)
	}

	name := inferredMessageName(message, "")
	if requested != "" && requested != name {
		return nil, "", fmt.Errorf("message %q not found; available message is %q", requested, name)
	}

	return message, name, nil
}

func inferredMessageName(message any, fallback string) string {
	object, ok := message.(map[string]any)
	if !ok {
		return fallback
	}

	for _, field := range []string{"name", "title"} {
		if value, ok := object[field].(string); ok && value != "" {
			return value
		}
	}

	if ref, ok := object["$ref"].(string); ok {
		parts := strings.Split(ref, "/")
		return parts[len(parts)-1]
	}

	return fallback
}

func (d *Document) resolve(value any) (any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return value, nil
	}

	ref, ok := object["$ref"].(string)
	if !ok {
		return value, nil
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("only internal references are supported, got %q", ref)
	}

	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	var current any = d.root
	for _, rawPart := range parts {
		part := strings.ReplaceAll(strings.ReplaceAll(rawPart, "~1", "/"), "~0", "~")
		currentObject, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("reference %q traverses through a non-object", ref)
		}
		current, ok = currentObject[part]
		if !ok {
			return nil, fmt.Errorf("reference %q could not resolve %q", ref, part)
		}
	}

	return current, nil
}

func (d *Document) resolveDeep(value any, seen map[string]struct{}) (any, error) {
	resolved, err := d.resolve(value)
	if err != nil {
		return nil, err
	}

	if object, ok := value.(map[string]any); ok {
		if ref, ok := object["$ref"].(string); ok {
			if _, exists := seen[ref]; exists {
				return nil, fmt.Errorf("circular reference detected at %q", ref)
			}
			seen[ref] = struct{}{}
			defer delete(seen, ref)
		}
	}

	switch typed := resolved.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			resolvedChild, err := d.resolveDeep(child, seen)
			if err != nil {
				return nil, err
			}
			out[key] = resolvedChild
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			resolvedChild, err := d.resolveDeep(child, seen)
			if err != nil {
				return nil, err
			}
			out[i] = resolvedChild
		}
		return out, nil
	default:
		return resolved, nil
	}
}

func objectAt(root map[string]any, key string) (map[string]any, bool) {
	value, ok := root[key].(map[string]any)
	return value, ok
}

func firstKey(values map[string]any) string {
	if len(values) == 0 {
		return ""
	}
	return sortedKeys(values)[0]
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func normalize(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalize(value)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = normalize(value)
		}
		return out
	case []any:
		for i, item := range typed {
			typed[i] = normalize(item)
		}
		return typed
	default:
		var decoded any
		bytes, err := json.Marshal(typed)
		if err == nil && json.Unmarshal(bytes, &decoded) == nil {
			return decoded
		}
		return typed
	}
}
