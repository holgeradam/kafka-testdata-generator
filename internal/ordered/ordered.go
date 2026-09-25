// Package ordered holds a JSON object whose keys keep the order they were
// given in when encoded, so generated records read in the order their schema
// declares their fields (#96). encoding/json sorts a map's keys.
package ordered

import (
	"bytes"
	"encoding/json"
)

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
