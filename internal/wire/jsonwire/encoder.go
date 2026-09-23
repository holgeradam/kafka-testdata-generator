package jsonwire

import (
	"encoding/json"

	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

// JsonEncoder encodes records as JSON (NDJSON-compatible). The Payload is
// json.Marshal'd. The Key follows the plain-scalar contract (CONTEXT.md Key
// entry): a string maps to UTF-8 bytes, a number to decimal text, and a
// structured value (object/array) to JSON - never a JSON-wrapped scalar. The
// same encoding serves Dry run and produce.
type JsonEncoder struct{}

// Encode marshals the payload to JSON and the key to plain-scalar bytes. When
// key is nil the returned keyBytes is nil (the pipeline skips sending).
func (e JsonEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	keyBytes, err := wire.PlainScalarKey(key)
	if err != nil {
		return nil, nil, err
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	return keyBytes, payloadBytes, nil
}
