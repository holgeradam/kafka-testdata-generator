package jsonwire

import (
	"encoding/json"
	"fmt"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// JsonEncoder encodes records as JSON (NDJSON-compatible). The Payload is
// json.Marshal'd. The Key follows the plain-scalar contract (CONTEXT.md Key
// entry): a string maps to UTF-8 bytes, a number to decimal text, and a
// structured value (object/array) to JSON - never a JSON-wrapped scalar. The
// same encoding serves Dry run and produce.
//
// Objects encode in the order their schema declares their properties (#96):
// each Payload by its own Message type's Payload schema, a structured Key by
// the Key binding. The zero value orders by name.
type JsonEncoder struct {
	// types are the run's bound Message types, the very slice its Mix
	// generates from, so a record's Type finds its Payload schema.
	types []wire.Bound[encoding]
	// key is the Key binding.
	key map[string]any
}

// encoding is what JsonEncoder needs of a Message type: the Payload schema,
// whose declared order the Payload encodes in.
type encoding struct {
	payload map[string]any
}

// Encode marshals the Payload to JSON and the Key to plain-scalar bytes. When
// the Key is nil the returned keyBytes is nil: the record carries a null Key.
func (e JsonEncoder) Encode(generated pipeline.Generated) ([]byte, []byte, error) {
	keyBytes, err := plainScalarKey(generator.Ordered(e.key, generated.Key))
	if err != nil {
		return nil, nil, err
	}
	var schema map[string]any
	if e.types != nil {
		schema = e.types[generated.Type].Encoding.payload
	}
	payloadBytes, err := json.Marshal(generator.Ordered(schema, generated.Payload))
	if err != nil {
		return nil, nil, err
	}
	return keyBytes, payloadBytes, nil
}

// plainScalarKey renders a Key by JSON mode's plain-scalar contract
// (CONTEXT.md Key), which Headers follow too. A nil Key yields nil bytes, so
// the record carries a null Key. AVRO shows its Key in the Avro JSON encoding
// of the key avsc instead.
func plainScalarKey(key any) ([]byte, error) {
	b, err := wire.PlainScalar(key)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	return b, nil
}
