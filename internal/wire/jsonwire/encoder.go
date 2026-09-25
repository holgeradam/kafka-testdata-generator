package jsonwire

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
)

// JsonEncoder encodes records as JSON (NDJSON-compatible). The Payload is
// json.Marshal'd. The Key follows the plain-scalar contract (CONTEXT.md Key
// entry): a string maps to UTF-8 bytes, a number to decimal text, and a
// structured value (object/array) to JSON - never a JSON-wrapped scalar. The
// same encoding serves Dry run and produce.
type JsonEncoder struct{}

// Encode marshals the payload to JSON and the key to plain-scalar bytes. When
// key is nil the returned keyBytes is nil (the pipeline skips sending). Every
// Message type encodes the same way, so the type is not consulted.
func (e JsonEncoder) Encode(key any, generated pipeline.Generated) ([]byte, []byte, error) {
	keyBytes, err := plainScalarKey(key)
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
// (CONTEXT.md Key): a string as UTF-8, a number as decimal text, and a
// structured value as JSON - never a JSON-wrapped scalar. A nil Key yields nil
// bytes, so the record carries a null Key. AVRO shows its Key in the Avro JSON
// encoding of the key avsc instead.
func plainScalarKey(key any) ([]byte, error) {
	switch v := key.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(v), nil
	case float64:
		// The JSON generator produces numbers as float64; any other numeric
		// type falls through to JSON, which renders integers as decimal text.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("key: cannot encode non-finite number %v", v)
		}
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		return []byte(strconv.FormatBool(v)), nil
	case []byte:
		return v, nil
	default:
		// Objects, arrays, and any other structured value become JSON.
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("key: %w", err)
		}
		return b, nil
	}
}
