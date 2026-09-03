package pipeline

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// Encoder is the wire-format seam at the Pipeline's single marshal call site.
// One adapter exists per wire format: JsonEncoder for JSON mode, AvroEncoder
// (later vertical) for AVRO mode. The Pipeline never knows which adapter it
// holds; it calls Encode for every record and the adapter owns both Key and
// Payload byte encoding. See ADR-0007.
type Encoder interface {
	// Encode turns a generated record into wire-format bytes. key is the
	// extracted key value from the payload (may be nil when no key is
	// configured); payload is the generated record as an in-memory value.
	// Returns the encoded Key bytes (nil when key is nil) and Payload bytes.
	Encode(key any, payload any) (keyBytes []byte, payloadBytes []byte, err error)
}

// JsonEncoder encodes records as JSON (NDJSON-compatible). The Payload is
// json.Marshal'd. The Key follows the plain-scalar contract (CONTEXT.md Key
// entry): a string maps to UTF-8 bytes, a number to decimal text, and a
// structured value (object/array) to JSON - never a JSON-wrapped scalar.
type JsonEncoder struct{}

// Encode marshals the payload to JSON and the key to plain-scalar bytes. When
// key is nil the returned keyBytes is nil (the pipeline skips sending).
func (e JsonEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	var keyBytes []byte
	if key != nil {
		var err error
		keyBytes, err = plainScalarKey(key)
		if err != nil {
			return nil, nil, err
		}
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	return keyBytes, payloadBytes, nil
}

// plainScalarKey renders a key value as plain-scalar bytes: strings as UTF-8,
// numbers as decimal text, and structured values as JSON (CONTEXT.md Key).
// The generator produces numbers as float64, so that is the one numeric case;
// any other value falls through to JSON.
func plainScalarKey(key any) ([]byte, error) {
	switch v := key.(type) {
	case string:
		return []byte(v), nil
	case float64:
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
