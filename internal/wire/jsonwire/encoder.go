package jsonwire

import (
	"encoding/json"
	"fmt"

	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// JsonEncoder encodes records as JSON (NDJSON-compatible). The Payload is
// json.Marshal'd. The Key follows the plain-scalar contract (CONTEXT.md Key
// entry): a string maps to UTF-8 bytes, a number to decimal text, and a
// structured value (object/array) to JSON - never a JSON-wrapped scalar. The
// same encoding serves Dry run and produce.
//
// Objects encode in the order they were generated in: the order their schema
// declares their properties (#96), a literal as it is written (#112). The
// encoder needs no schema for that, so its zero value is the whole encoder.
type JsonEncoder struct{}

// Encode marshals the Payload to JSON and the Key to plain-scalar bytes. When
// the Key is nil the returned keyBytes is nil: the record carries a null Key.
func (JsonEncoder) Encode(generated pipeline.Generated) ([]byte, []byte, error) {
	keyBytes, err := plainScalarKey(generated.Key)
	if err != nil {
		return nil, nil, err
	}
	payloadBytes, err := json.Marshal(generated.Payload)
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
