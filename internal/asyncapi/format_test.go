package asyncapi

import (
	"errors"
	"reflect"
	"testing"
)

const avroFormat = `'application/vnd.apache.avro;version=1.9.0'`

// TestSchemaCarriesItsFormat proves a Message type's Payload and Key come as
// the schema case of the format they are declared in, the Key read in its
// Payload's format, and the Topic states that format once (#111).
func TestSchemaCarriesItsFormat(t *testing.T) {
	json := readTopic(t, loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {key: {type: string}}}
        payload: {type: object}
`), "orders")
	if json.Format != JSONSchemaFormat {
		t.Errorf("JSON Schema payloads: Topic.Format = %v, want JSONSchemaFormat", json.Format)
	}
	mt := json.MessageTypes[0]
	if p, ok := mt.Payload.(JSONSchema); !ok || p["type"] != "object" {
		t.Errorf("Payload = %#v, want the JSONSchema", mt.Payload)
	}
	if k, ok := mt.Key.(JSONSchema); !ok || k["type"] != "string" {
		t.Errorf("Key = %#v, want the JSONSchema", mt.Key)
	}

	avro := readTopic(t, loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created:
        bindings: {kafka: {key: {type: string}}}
        payload: {schemaFormat: `+avroFormat+`, schema: {type: record, name: Order, fields: []}}
      paid:
        payload: {schemaFormat: `+avroFormat+`, schema: {type: record, name: Paid, fields: []}}
`), "orders")
	if avro.Format != AvroFormat {
		t.Errorf("Avro payloads: Topic.Format = %v, want AvroFormat", avro.Format)
	}
	created, paid := avro.MessageTypes[0], avro.MessageTypes[1]
	if _, ok := created.Payload.(Avsc); !ok {
		t.Errorf("Payload = %#v, want an Avsc", created.Payload)
	}
	if k, ok := created.Key.(Avsc); !ok || string(k) != `{"type":"string"}` {
		t.Errorf("Key = %s, want the Avsc {\"type\":\"string\"}", created.Key)
	}
	if paid.Key != nil {
		t.Errorf("no Key binding: Key = %#v, want nil", paid.Key)
	}
}

// TestTopicRefusesMixedFormats proves a Kafka topic whose Message types mix
// payload formats is refused by the spec reader, naming them: a Kafka topic
// is produced in one Wire format.
func TestTopicRefusesMixedFormats(t *testing.T) {
	_, err := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: A, schemaFormat: `+avroFormat+`, payload: {type: record, name: A, fields: []}}
          - {name: B, payload: {type: object}}
          - {name: C, schemaFormat: `+avroFormat+`, payload: {type: record, name: C, fields: []}}
`).Topic("orders")
	var mixed *MixedFormatsError
	if !errors.As(err, &mixed) {
		t.Fatalf("err = %v, want a *MixedFormatsError", err)
	}
	if want := `Kafka topic "orders" mixes payload formats: Avro (A, C) and JSON Schema (B); a Kafka topic is produced in one Wire format`; err.Error() != want {
		t.Errorf("err = %q, want %q", err, want)
	}
	if !reflect.DeepEqual(mixed.Avro, []string{"A", "C"}) || !reflect.DeepEqual(mixed.JSONSchema, []string{"B"}) {
		t.Errorf("mixed = %+v, want Avro A, C and JSON Schema B", mixed)
	}
}

// jsonPayload is a Message type's Payload as a JSON Schema map, nil when it is
// not the JSONSchema case.
func jsonPayload(mt MessageType) map[string]any {
	p, _ := mt.Payload.(JSONSchema)
	return p
}

// jsonKey is a Message type's Key binding as a JSON Schema map, nil when it
// declares none or it is not the JSONSchema case.
func jsonKey(mt MessageType) map[string]any {
	k, _ := mt.Key.(JSONSchema)
	return k
}

// avroPayload is a Message type's Payload avsc, nil when it is not the Avsc
// case.
func avroPayload(mt MessageType) []byte {
	p, _ := mt.Payload.(Avsc)
	return p
}

// avroKey is a Message type's Key binding avsc, nil when it declares none or
// it is not the Avsc case.
func avroKey(mt MessageType) []byte {
	k, _ := mt.Key.(Avsc)
	return k
}
