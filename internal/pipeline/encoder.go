package pipeline

import "encoding/json"

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

// JsonEncoder encodes records as JSON (NDJSON-compatible). It preserves the
// exact byte output of the original pipeline: payload via json.Marshal,
// key as the JSON encoding of the extracted field value.
type JsonEncoder struct{}

// Encode marshals key and payload to JSON. When key is nil the returned
// keyBytes is nil (the pipeline skips sending in that case).
func (e JsonEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	var keyBytes []byte
	if key != nil {
		var err error
		keyBytes, err = json.Marshal(key)
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
