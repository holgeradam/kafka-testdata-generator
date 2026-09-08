package pipeline

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
)

// confluentMagicByte is the leading byte of every Confluent-framed record
// (ADR-0007 decision 2): magic byte 0x00, then the 4-byte big-endian schema ID
// the registry assigned, then the Avro binary datum.
const confluentMagicByte = 0x00

// RegistryError reports a schema-registry interaction the AvroEncoder needs
// but cannot complete: the registry is unreachable or rejected the schema.
// It wraps the underlying transport error or the registry's *registry.Error
// so callers can distinguish a dead registry from an unregistrable schema.
type RegistryError struct {
	// URL is the registry base URL that failed.
	URL string
	// Err is the underlying registry or transport error.
	Err error
}

func (e *RegistryError) Error() string {
	msg := "avro: schema registry "
	if e.URL != "" {
		msg += e.URL
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap exposes the underlying error for errors.Is/As.
func (e *RegistryError) Unwrap() error {
	return e.Err
}

// AvroEncoder encodes records as Confluent-framed Avro: the magic byte, the
// registry-assigned schema ID, then the Avro binary for the Payload. It owns
// both the schema-registry interaction and the encoding (ADR-0007 decision 5),
// so the registry client is confined to this adapter and franz-go stays the
// producer. The explicit avsc is what gets registered (decision 3), and the
// generic marshaller honours it, so what the encoder frames is always
// registry-valid wire data a Confluent/AVRO-aware consumer can deserialize.
type AvroEncoder struct {
	api    avro.API
	schema avro.Schema
	id     int
}

// NewAvroEncoder registers the explicit avsc with the schema registry under
// subject and returns an encoder that frames payloads with the
// registry-assigned schema ID. Registration happens up front so an unreachable
// or rejecting registry stops the run with a typed *RegistryError before any
// produce work starts (ADR-0007 decision 4: fail fast, never a silent nil).
func NewAvroEncoder(ctx context.Context, registryURL, subject, avsc string) (*AvroEncoder, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	schema, err := avro.Parse(avsc)
	if err != nil {
		return nil, fmt.Errorf("avro: parsing avsc for encoding: %w", err)
	}

	client, err := registry.NewClient(registryURL)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}
	id, _, err := client.CreateSchema(ctx, subject, avsc)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}

	return &AvroEncoder{
		api:    avro.Config{}.Freeze(),
		schema: schema,
		id:     id,
	}, nil
}

// Encode turns a generated Avro value into Confluent wire bytes. The Key
// follows the plain-scalar contract shared with JsonEncoder (CONTEXT.md Key
// entry); the Payload is framed as magic byte + schema ID + Avro binary.
func (e *AvroEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	keyBytes, err := encodeKeyBytes(key)
	if err != nil {
		return nil, nil, err
	}

	body, err := e.api.Marshal(e.schema, payload)
	if err != nil {
		return nil, nil, fmt.Errorf("avro: encoding payload %T: %w", payload, err)
	}

	wire := make([]byte, 0, 5+len(body))
	wire = append(wire, confluentMagicByte, byte(e.id>>24), byte(e.id>>16), byte(e.id>>8), byte(e.id))
	wire = append(wire, body...)
	return keyBytes, wire, nil
}
