package asyncapi

import (
	"fmt"
	"maps"
)

// maxMergeDepth bounds how deep a trait merge descends, so a message and a
// trait naming different refs of one cycle stop with an error instead of
// following it forever.
const maxMergeDepth = 64

// traits resolves the message traits of msg, in the order listed. Each may be
// a $ref. A message trait holds every message field except payload and traits
// (AsyncAPI 2.6.0 and 3.0.0), so a trait declaring either is refused.
func (d *Document) traits(msg map[string]any, name string) ([]map[string]any, error) {
	if msg["traits"] == nil {
		return nil, nil
	}
	list, ok := msg["traits"].([]any)
	if !ok {
		return nil, fmt.Errorf("message %s: traits must be a list", name)
	}
	out := make([]map[string]any, len(list))
	for i, node := range list {
		trait, err := d.object(node, fmt.Sprintf("message %s: traits[%d]", name, i))
		if err != nil {
			return nil, err
		}
		for _, field := range []string{"payload", "traits"} {
			if _, ok := trait[field]; ok {
				return nil, fmt.Errorf("message %s: traits[%d] declares %s, which a message trait may not", name, i, field)
			}
		}
		out[i] = trait
	}
	return out, nil
}

// mergePatch applies patch to target with JSON Merge Patch (RFC 7386), the
// algorithm AsyncAPI merges traits with: an object merges key by key, a null
// removes the key, anything else replaces. Neither input is modified. Where
// both sides hold an object, a $ref on either side is resolved first, so a
// patch extends a referenced object instead of being shadowed by its $ref.
func (d *Document) mergePatch(target map[string]any, patch any, where string, depth int) (any, error) {
	p, ok := patch.(map[string]any)
	if !ok {
		return patch, nil
	}
	if depth == maxMergeDepth {
		return nil, fmt.Errorf("%s: traits merge deeper than %d levels", where, maxMergeDepth)
	}
	if len(target) > 0 {
		var err error
		if p, err = d.object(p, where); err != nil {
			return nil, err
		}
	}
	out := maps.Clone(target)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range p {
		if value == nil {
			delete(out, key)
			continue
		}
		at := where + "." + key
		sub, _ := out[key].(map[string]any)
		if _, isObject := value.(map[string]any); isObject && sub != nil {
			if ref := refOf(sub); ref != "" && ref == refOf(value) {
				continue // the same object on both sides: merging changes nothing
			}
			var err error
			if sub, err = d.object(sub, at); err != nil {
				return nil, err
			}
		}
		merged, err := d.mergePatch(sub, value, at, depth+1)
		if err != nil {
			return nil, err
		}
		out[key] = merged
	}
	return out, nil
}
