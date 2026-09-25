package asyncapi

import (
	"encoding/json"
	"reflect"
	"testing"
)

// avscOf decodes an avsc the reader produced, so tests compare structure
// rather than JSON text.
func avscOf(t *testing.T, raw []byte) any {
	t.Helper()
	if raw == nil {
		t.Fatal("avsc is nil")
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("avsc %s is not JSON: %v", raw, err)
	}
	return v
}

// wantAvsc is the avsc every Avro test below declares for OrderCreated.
func wantAvsc(t *testing.T) any {
	t.Helper()
	return avscOf(t, []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"id","type":"string"}]}`))
}

// TestAvroPayload2 proves a 2.x message whose schemaFormat is Avro, declared
// on the message or by a trait, in each Avro media type, reads to an avsc
// rather than a refusal (#84).
func TestAvroPayload2(t *testing.T) {
	cases := map[string]string{
		"avro":             `schemaFormat: 'application/vnd.apache.avro;version=1.9.0'`,
		"avro+json":        `schemaFormat: 'application/vnd.apache.avro+json;version=1.11.1'`,
		"avro+yaml":        `schemaFormat: 'application/vnd.apache.avro+yaml;version=1.8.2'`,
		"case-insensitive": `schemaFormat: 'Application/Vnd.Apache.Avro;version=1.9.0'`,
		"in a trait":       `traits: [{schemaFormat: 'application/vnd.apache.avro;version=1.9.0'}]`,
	}
	for name, field := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        `+field+`
        payload: {type: record, name: OrderCreated, fields: [{name: id, type: string}]}
`)
			mt, err := onlyType(doc, "orders")
			if err != nil {
				t.Fatal(err)
			}
			if mt.Payload != nil {
				t.Errorf("Payload = %v, want nil: an Avro payload is no JSON Schema", mt.Payload)
			}
			if got := avscOf(t, mt.Avsc); !reflect.DeepEqual(got, wantAvsc(t)) {
				t.Errorf("avsc = %v, want %v", got, wantAvsc(t))
			}
		})
	}
}

// TestAvroPayload3 proves a 3.0 Multi Format Schema in Avro reads to an avsc,
// the schema itself reachable through a $ref.
func TestAvroPayload3(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created:
        name: OrderCreated
        payload:
          schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
          schema: {$ref: '#/components/schemas/OrderCreated'}
components:
  schemas:
    OrderCreated: {type: record, name: OrderCreated, fields: [{name: id, type: string}]}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got := avscOf(t, mt.Avsc); !reflect.DeepEqual(got, wantAvsc(t)) {
		t.Errorf("avsc = %v, want %v", got, wantAvsc(t))
	}
}

// TestAvroVersionRefused proves only Avro 1.x formats are read: another major
// version stops the run naming the format, as any unread format does.
func TestAvroVersionRefused(t *testing.T) {
	for _, format := range []string{"application/vnd.apache.avro;version=2.0.0", "application/vnd.apache.avro", "application/vnd.apache.avro;version=1.9"} {
		doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: '`+format+`'
        payload: {type: record, name: OrderCreated, fields: []}
`)
		_, err := messageTypes(doc, "orders")
		wantErr(t, err, "payload of OrderCreated is "+format+", which the tool does not read")
	}
}

// TestAvroRefsExpanded proves $refs inside an avsc are expanded, and that a
// named type reached a second time becomes a reference by its full name,
// which Avro requires: a named type is defined once. The full name takes the
// namespace the first definition inherits.
func TestAvroRefsExpanded(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload:
          type: record
          name: OrderCreated
          namespace: com.acme
          fields:
            - {name: billing, type: {$ref: '#/components/schemas/Address'}}
            - {name: shipping, type: {$ref: '#/components/schemas/Address'}}
            - {name: status, type: {$ref: '#/components/schemas/Status'}}
components:
  schemas:
    Address: {type: record, name: Address, fields: [{name: city, type: string}]}
    Status: {type: enum, name: Status, namespace: com.acme.status, symbols: [NEW, PAID]}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	want := avscOf(t, []byte(`{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[
		{"name":"billing","type":{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}},
		{"name":"shipping","type":"com.acme.Address"},
		{"name":"status","type":{"type":"enum","name":"Status","namespace":"com.acme.status","symbols":["NEW","PAID"]}}]}`))
	if got := avscOf(t, mt.Avsc); !reflect.DeepEqual(got, want) {
		t.Errorf("avsc = %v\nwant %v", got, want)
	}
}

// TestAvroRefCycleRefused proves a $ref cycle in an avsc stops the run: Avro
// expresses recursion by name, never by $ref.
func TestAvroRefCycleRefused(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {$ref: '#/components/schemas/Node'}
components:
  schemas:
    Node: {type: record, name: Node, fields: [{name: next, type: ['null', {$ref: '#/components/schemas/Node'}]}]}
`)
	_, err := messageTypes(doc, "orders")
	wantErr(t, err, "message OrderCreated: payload", "$ref cycle through #/components/schemas/Node", "refer to a named type by its name")
}

// TestAvroKeyBinding proves the Key binding beside an Avro payload is read as
// an avsc, primitive or named, and that beside a JSON Schema payload it stays
// a JSON Schema (#84 decision 7).
func TestAvroKeyBinding(t *testing.T) {
	for name, c := range map[string]struct{ key, want string }{
		"primitive": {`{type: string}`, `{"type":"string"}`},
		"bare name": {`string`, `"string"`},
		"record":    {`{type: record, name: OrderKey, fields: [{name: id, type: long}]}`, `{"type":"record","name":"OrderKey","fields":[{"name":"id","type":"long"}]}`},
		"ref":       {`{$ref: '#/components/schemas/Key'}`, `{"type":"string","logicalType":"uuid"}`},
	} {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        bindings: {kafka: {key: `+c.key+`}}
        payload: {type: record, name: OrderCreated, fields: [{name: id, type: string}]}
components:
  schemas:
    Key: {type: string, logicalType: uuid}
`)
			mt, err := onlyType(doc, "orders")
			if err != nil {
				t.Fatal(err)
			}
			if mt.KeyBinding != nil {
				t.Errorf("KeyBinding = %v, want nil beside an Avro payload", mt.KeyBinding)
			}
			if got, want := avscOf(t, mt.KeyAvsc), avscOf(t, []byte(c.want)); !reflect.DeepEqual(got, want) {
				t.Errorf("key avsc = %v, want %v", got, want)
			}
		})
	}

	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {key: {type: string}}}
        payload: {type: object}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if mt.KeyAvsc != nil || mt.Avsc != nil {
		t.Errorf("a JSON Schema message has an avsc: payload %s, key %s", mt.Avsc, mt.KeyAvsc)
	}
}

// TestRegistryBinding proves the Kafka message binding's registry fields are
// read as declared, for the AVRO Wire format to judge (#84 decision 8).
func TestRegistryBinding(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {schemaIdLocation: header, schemaIdPayloadEncoding: 4, schemaLookupStrategy: RecordNameStrategy}}
        payload: {type: object}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	want := RegistryBinding{SchemaIDLocation: "header", SchemaIDPayloadEncoding: "4", SchemaLookupStrategy: "RecordNameStrategy"}
	if !reflect.DeepEqual(mt.Registry, want) {
		t.Errorf("Registry = %+v, want %+v", mt.Registry, want)
	}
}

// TestAvroEmptyNamespaceInherits proves an empty namespace inherits the
// enclosing one, as the codec that parses the avsc reads it, so a named type
// reached again is named by the full name the codec gives it.
func TestAvroEmptyNamespaceInherits(t *testing.T) {
	doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload:
          type: record
          name: OrderCreated
          namespace: com.acme
          fields:
            - {name: a, type: {$ref: '#/components/schemas/Tag'}}
            - {name: b, type: {$ref: '#/components/schemas/Tag'}}
components:
  schemas:
    Tag: {type: enum, name: Tag, namespace: '', symbols: [X]}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	fields := avscOf(t, mt.Avsc).(map[string]any)["fields"].([]any)
	if got := fields[1].(map[string]any)["type"]; got != "com.acme.Tag" {
		t.Errorf("second Tag = %v, want com.acme.Tag, the codec's full name for it", got)
	}
}
