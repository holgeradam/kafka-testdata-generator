package avrowire

import (
	"context"
	"fmt"

	codec "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
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
// honours the codec schema they were parsed into, so what the encoder frames is
// always registry-valid wire data a Confluent/AVRO-aware consumer can
// deserialize.
type AvroEncoder struct {
	api    codec.API
	schema codec.Schema
	id     int
	// keySchema is the key avsc's codec schema, nil when the run produces
	// payload-only records; keyID is the ID the registry assigned to the
	// <topic>-key subject.
	keySchema codec.Schema
	keyID     int
	// branches are the full names of the union's records, by Message type,
	// when several Avro Message types register as a union; nil otherwise.
	branches []string
}

// NewAvroEncoder registers the exact value avsc under <topic>-value and, when
// key is non-nil, the key avsc under <topic>-key, then returns an encoder that
// frames payloads and keys with the registry-assigned schema IDs. It encodes
// against the codec schema each model already carries, so the avsc is never
// parsed twice (#65). Registration happens up front so an unreachable or
// rejecting registry stops the run with a typed *RegistryError before any
// produce work starts (ADR-0007 decision 4: fail fast, never a silent nil).
func NewAvroEncoder(ctx context.Context, registryURL, topic string, value, key *avro.Schema) (*AvroEncoder, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	client, err := registry.NewClient(registryURL)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}

	enc := &AvroEncoder{
		api:    codec.Config{}.Freeze(),
		schema: value.Codec(),
	}

	enc.id, _, err = client.CreateSchema(ctx, topic+"-value", string(value.Raw()))
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}
	if key != nil {
		enc.keySchema = key.Codec()
		enc.keyID, _, err = client.CreateSchema(ctx, topic+"-key", string(key.Raw()))
		if err != nil {
			return nil, &RegistryError{URL: registryURL, Err: err}
		}
	}

	return enc, nil
}

// NewAvroUnionEncoder registers several Avro Message types as a union under
// <topic>-value (#84 decision 4): each subject of plan under its full name,
// shared named types first, each referencing the subjects it names at the
// version the registry holds them at; then the union of the records' names
// under <topic>-value, referencing each record. The key avsc, when non-nil,
// registers under <topic>-key. Every record is then encoded against the union
// and framed with the union's ID.
func NewAvroUnionEncoder(ctx context.Context, registryURL, topic string, union *avro.Schema, plan *unionPlan, key *avro.Schema) (*AvroEncoder, error) {
	client, err := registry.NewClient(registryURL)
	if err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}
	enc := &AvroEncoder{
		api:      codec.Config{}.Freeze(),
		schema:   union.Codec(),
		branches: plan.Branches,
	}

	versions := map[string]int{}
	references := func(names []string) []registry.SchemaReference {
		refs := make([]registry.SchemaReference, len(names))
		for i, n := range names {
			refs[i] = registry.SchemaReference{Name: n, Subject: n, Version: versions[n]}
		}
		return refs
	}
	for _, s := range plan.Subjects {
		id, _, err := client.CreateSchema(ctx, s.Name, s.Schema, references(s.References)...)
		if err != nil {
			return nil, &RegistryError{URL: registryURL, Err: err}
		}
		if versions[s.Name], err = versionOf(ctx, client, s.Name, id); err != nil {
			return nil, &RegistryError{URL: registryURL, Err: err}
		}
	}
	if enc.id, _, err = client.CreateSchema(ctx, topic+"-value", plan.Value, references(plan.Branches)...); err != nil {
		return nil, &RegistryError{URL: registryURL, Err: err}
	}
	if key != nil {
		enc.keySchema = key.Codec()
		if enc.keyID, _, err = client.CreateSchema(ctx, topic+"-key", string(key.Raw())); err != nil {
			return nil, &RegistryError{URL: registryURL, Err: err}
		}
	}
	return enc, nil
}

// versionOf finds the version under which subject holds the schema with ID
// id, newest first: a reference names a subject at a version, and
// registration answers only the ID.
func versionOf(ctx context.Context, client *registry.Client, subject string, id int) (int, error) {
	versions, err := client.GetVersions(ctx, subject)
	if err != nil {
		return 0, err
	}
	for i := len(versions) - 1; i >= 0; i-- {
		info, err := client.GetSchemaInfo(ctx, subject, versions[i])
		if err != nil {
			return 0, err
		}
		if info.ID == id {
			return versions[i], nil
		}
	}
	return 0, fmt.Errorf("subject %s lists no version with schema ID %d", subject, id)
}

// Encode turns generated values into Confluent wire bytes. The Key contract
// (issue #24) is strict: a key is only ever emitted when a key avsc is
// registered, and it is then framed under the key schema's own registry ID so a
// consumer decodes it against the key avsc. A key without a key avsc, or a
// registered key avsc without a generated key, is a programming error - rejected
// rather than silently dropping data or sending a null key where the schema
// contract promises one.
func (e *AvroEncoder) Encode(key any, generated pipeline.Generated) ([]byte, []byte, error) {
	payload := generated.Payload
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

	if e.branches != nil {
		// A union's generic value names its branch: the record's full name.
		payload = map[string]any{e.branches[generated.Type]: payload}
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
