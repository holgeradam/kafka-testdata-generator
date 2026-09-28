// Package ordered holds a JSON object whose keys keep the order they were
// given in when encoded, so generated records read in the order their schema
// declares their fields (#96), and a literal in the order it is written
// (#112). encoding/json sorts a map's keys. The JSON Schema generator emits
// every object as an Object, so no consumer needs a schema to order it.
package ordered

import (
	"bytes"
	"encoding/json"
)

// Keyword is the extension keyword under which the spec reader records the
// order a schema declares its properties in (#96): a decoded map forgets the
// order its keys were written in. Validators ignore unknown keywords, so it
// changes nothing but the order records encode in.
const Keyword = "x-kafka-testdata-generator-property-order"

// LiteralKeyword is the extension keyword under which the spec reader
// records the order a literal the generator returns as-is - an example,
// examples, a const, an enum - is written in (#112). Its value maps each such
// keyword of the schema to an order tree of its literal: an Object of the
// literal's keys in written order, each value the tree of that key's value;
// an array of the trees of its items; nil for anything else. The tree holds
// no values of its own, only their order.
const LiteralKeyword = "x-kafka-testdata-generator-literal-order"

// Object is a JSON object whose keys encode in order. Values may be Objects
// themselves, or anything encoding/json encodes.
type Object struct {
	Keys   []string
	Values []any
}

// Add appends a key and its value.
func (o *Object) Add(key string, value any) {
	o.Keys = append(o.Keys, key)
	o.Values = append(o.Values, value)
}

// Get returns the value under key, and whether the object has the key.
func (o Object) Get(key string) (any, bool) {
	for i, k := range o.Keys {
		if k == key {
			return o.Values[i], true
		}
	}
	return nil, false
}

// Set replaces the value under key, in place: every copy of the Object
// shares its values. It adds no key, reporting false when key is absent.
func (o Object) Set(key string, value any) bool {
	for i, k := range o.Keys {
		if k == key {
			o.Values[i] = value
			return true
		}
	}
	return false
}

// Plain returns v with every Object turned into a map, recursively, for a
// consumer that reads plain JSON values, such as a validator. Order is lost.
func Plain(v any) any {
	switch v := v.(type) {
	case Object:
		m := make(map[string]any, len(v.Keys))
		for i, k := range v.Keys {
			m[k] = Plain(v.Values[i])
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, e := range v {
			m[k] = Plain(e)
		}
		return m
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = Plain(e)
		}
		return out
	}
	return v
}

// MarshalJSON encodes the object with its keys in order, each key and value
// exactly as encoding/json encodes them.
func (o Object) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range o.Keys {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		value, err := json.Marshal(o.Values[i])
		if err != nil {
			return nil, err
		}
		b.Write(value)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}
