package avrowire

import (
	"fmt"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
)

// AvroDisplayEncoder is the Dry-run AVRO adapter on the Encoder seam: it
// renders each generated record readably, Payload and Key alike, in the Avro
// JSON encoding of their avsc - but it never touches a schema registry
// (ADR-0007 decision 7). One adapter per concern keeps the framing/registry
// AvroEncoder solely for producing, where registry interaction belongs.
type AvroDisplayEncoder struct {
	value *avro.Schema
	// key is the key avsc model, nil when records carry a null Key.
	key *avro.Schema
}

// NewAvroDisplayEncoder returns a Dry-run encoder that renders values
// honouring the already-parsed value avsc and, when the run has one, the key
// avsc. It requires no registry URL and makes no network calls; the models
// alone drive the rendering.
func NewAvroDisplayEncoder(value, key *avro.Schema) *AvroDisplayEncoder {
	return &AvroDisplayEncoder{value: value, key: key}
}

// Encode renders the payload and the key as the canonical Avro JSON encoding
// (the readable spec-defined text form of a datum), each against its own avsc
// (#64). As for the AvroEncoder, a Key exists exactly when a key avsc does; a
// mismatch is a programming error, rejected rather than shown.
func (e *AvroDisplayEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	var keyBytes []byte
	switch {
	case e.key == nil && key != nil:
		return nil, nil, fmt.Errorf("avro: a message key was provided but no key avsc is configured (-avro-key-schema)")
	case e.key != nil && key == nil:
		return nil, nil, fmt.Errorf("avro: a key avsc is configured but no key value was generated")
	case e.key != nil:
		var err error
		if keyBytes, err = avro.RenderJSON(e.key.Root, key); err != nil {
			return nil, nil, fmt.Errorf("avro: rendering key: %w", err)
		}
	}
	payloadBytes, err := avro.RenderJSON(e.value.Root, payload)
	if err != nil {
		return nil, nil, err
	}
	return keyBytes, payloadBytes, nil
}
