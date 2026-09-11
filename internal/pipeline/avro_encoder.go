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
// registry-assigned schema ID, then the Avro binary for the Payload - and,
// when a key avsc is configured, the same framing for the Key under its own
// (topic-key) schema ID (ADR-0007 decision 3). It owns both the schema-registry
// interaction and the encoding (ADR-0007 decision 5), so the registry client is
// confined to this adapter and franz-go stays the producer. The explicit avsc
// files are what get registered (decision 3), and the generic marshaller
// honours them, so what the encoder frames is always registry-valid wire data a
// Confluent/AVRO-aware consumer can deserialize.
type AvroEncoder struct {
	api    avro.API
	schema avro.Schema
	id     int
	// keySchema is the parsed key avsc, nil when the run produces payload-only
	// records; keyID is the ID the registry assigned to the <topic>-key subject.
	keySchema avro.Schema
	keyID     int
}

// NewAvroEncoder registers the explicit value avsc under <topic>-value and,
// when keyAvsc is given, the key avsc under <topic>-key, then returns an
// encoder that frames payloads and keys with the registry-assigned schema IDs.
// Registration happens up front so an unreachable or rejecting registry stops
// the run with a typed *RegistryError before any produce work starts
// (ADR-0007 decision 4: fail fast, never a silent nil).
func NewAvroEncoder(ctx context.Context, registryURL, topic, valueAvsc, keyAvsc string) (*AvroEncoder, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	schema, err := avro.Parse(valueAvsc)
	if err != nil {
		return nil, fmt.Errorf("avro: parsing value avsc for encoding: %w", err)
	}

	client, err := registry.NewClient(registryURL)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}

	var keySchema avro.Schema
	if keyAvsc != "" {
		keySchema, err = avro.Parse(keyAvsc)
		if err != nil {
			return nil, fmt.Errorf("avro: parsing key avsc for encoding: %w", err)
		}
	}

	enc := &AvroEncoder{
		api:       avro.Config{}.Freeze(),
		schema:    schema,
		keySchema: keySchema,
	}

	enc.id, _, err = client.CreateSchema(ctx, topic+"-value", valueAvsc)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}
	if enc.keySchema != nil {
		enc.keyID, _, err = client.CreateSchema(ctx, topic+"-key", keyAvsc)
		if err != nil {
			return nil, &RegistryError{URL: registryURL, Err: err}
		}
	}

	return enc, nil
}

// Encode turns generated values into Confluent wire bytes. The Key contract
// (issue #24) is strict: a key is only ever emitted when a key avsc is
// registered, and it is then framed under the key schema's own registry ID so a
// consumer decodes it against the key avsc. A key without a key avsc, or a
// registered key avsc without a generated key, is a programming error - rejected
// rather than silently dropping data or sending a null key where the schema
// contract promises one.
func (e *AvroEncoder) Encode(key any, payload any) ([]byte, []byte, error) {
	if e.keySchema == nil {
		if key != nil {
			return nil, nil, fmt.Errorf("avro: a message key was provided but no key avsc is registered (-avro-key-schema)")
		}
	} else if key == nil {
		return nil, nil, fmt.Errorf("avro: a key avsc is registered but no key value was generated")
	}

	var keyBytes []byte
	if e.keySchema != nil {
		keyBody, err := e.api.Marshal(e.keySchema, key)
		if err != nil {
			return nil, nil, fmt.Errorf("avro: encoding key %T: %w", key, err)
		}
		keyBytes = frameConfluent(e.keyID, keyBody)
	}

	body, err := e.api.Marshal(e.schema, payload)
	if err != nil {
		return nil, nil, fmt.Errorf("avro: encoding payload %T: %w", payload, err)
	}

	return keyBytes, frameConfluent(e.id, body), nil
}

// frameConfluent prefixes an Avro binary datum with the Confluent wire-format
// header: magic byte 0x00 then the 4-byte big-endian schema ID (ADR-0007
// decision 2).
func frameConfluent(id int, body []byte) []byte {
	wire := make([]byte, 0, 5+len(body))
	wire = append(wire, confluentMagicByte, byte(id>>24), byte(id>>16), byte(id>>8), byte(id))
	return append(wire, body...)
}
