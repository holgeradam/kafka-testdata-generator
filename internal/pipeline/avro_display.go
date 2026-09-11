package pipeline

import (
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
)

// AvroDisplayEncoder is the Dry-run AVRO adapter on the Encoder seam: it
// renders each generated record in the readable Avro JSON encoding on stdout
// and echoes the Key by the plain-scalar contract, exactly like produce-time
// adapters encode bytes - but it never touches a schema registry (ADR-0007
// decision 7). One adapter per concern keeps the framing/registry AvroEncoder
// solely for producing, where registry interaction belongs.
type AvroDisplayEncoder struct {
	model *avro.Schema
}

// NewAvroDisplayEncoder returns a Dry-run encoder that renders values
// honouring the already-parsed avsc model. It requires no registry URL and
// makes no network calls; the model alone drives the rendering.
func NewAvroDisplayEncoder(model *avro.Schema) *AvroDisplayEncoder {
	return &AvroDisplayEncoder{model: model}
}

// Encode renders the payload as the canonical Avro JSON encoding (the
// readable spec-defined text form of the datum) and the key as plain-scalar
// bytes (shared contract, CONTEXT.md Key).
func (e *AvroDisplayEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	keyBytes, err := encodeKeyBytes(key)
	if err != nil {
		return nil, nil, err
	}
	payloadBytes, err := avro.RenderJSON(e.model.Root, payload)
	if err != nil {
		return nil, nil, err
	}
	return keyBytes, payloadBytes, nil
}