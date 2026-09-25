package asyncapi

import (
	"reflect"
	"testing"
)

// wantHeaders is the headers schema every reading test below declares.
var wantHeaders = map[string]any{
	"type":       "object",
	"required":   []any{"tenant"},
	"properties": map[string]any{"tenant": map[string]any{"type": "string"}},
}

// TestHeaders2 proves a 2.x message's headers schema is read, declared on the
// message, through a $ref, or by a trait (#92).
func TestHeaders2(t *testing.T) {
	for name, field := range map[string]string{
		"inline":   `headers: {type: object, required: [tenant], properties: {tenant: {type: string}}}`,
		"ref":      `headers: {$ref: '#/components/schemas/Headers'}`,
		"in trait": `traits: [{headers: {$ref: '#/components/schemas/Headers'}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head2+`
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        `+field+`
        payload: {type: object}
components:
  schemas:
    Headers: {type: object, required: [tenant], properties: {tenant: {type: string}}}
`)
			mt, err := onlyType(doc, "orders")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(withoutOrder(mt.Headers), wantHeaders) {
				t.Errorf("Headers = %v, want %v", mt.Headers, wantHeaders)
			}
		})
	}
}

// TestHeaders3 proves a 3.0 message's headers are read as a Schema Object or
// a Multi Format Schema in JSON Schema, and that a message without headers
// has none.
func TestHeaders3(t *testing.T) {
	for name, headers := range map[string]string{
		"schema":       `{type: object, required: [tenant], properties: {tenant: {type: string}}}`,
		"multi format": `{schemaFormat: 'application/schema+json;version=draft-07', schema: {$ref: '#/components/schemas/Headers'}}`,
	} {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created: {name: OrderCreated, headers: `+headers+`, payload: {type: object}}
      paid: {name: OrderPaid, payload: {type: object}}
components:
  schemas:
    Headers: {type: object, required: [tenant], properties: {tenant: {type: string}}}
`)
			types, err := messageTypes(doc, "orders")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(withoutOrder(types[0].Headers), wantHeaders) {
				t.Errorf("OrderCreated Headers = %v, want %v", types[0].Headers, wantHeaders)
			}
			if types[1].Headers != nil {
				t.Errorf("OrderPaid Headers = %v, want none", types[1].Headers)
			}
		})
	}
}

// TestHeadersRefused proves headers the tool cannot generate one Kafka record
// header per property from stop the run naming the Message type.
func TestHeadersRefused(t *testing.T) {
	for name, c := range map[string]struct{ spec, want string }{
		"not an object, 2.x": {head2 + `
channels:
  orders:
    publish: {message: {name: OrderCreated, headers: {type: string}, payload: {type: object}}}
`, "message OrderCreated: headers must be a schema of type object, whose properties are the Kafka record headers"},
		"no type, 3.0": {head3 + `
channels:
  orders:
    address: orders
    messages: {created: {name: OrderCreated, headers: {properties: {tenant: {type: string}}}, payload: {type: object}}}
`, "message OrderCreated: headers must be a schema of type object"},
		"avro headers": {head3 + `
channels:
  orders:
    address: orders
    messages: {created: {name: OrderCreated, headers: {schemaFormat: 'application/vnd.apache.avro;version=1.9.0', schema: {type: record, name: H, fields: []}}, payload: {type: object}}}
`, "headers of OrderCreated are application/vnd.apache.avro;version=1.9.0; the tool reads headers in JSON Schema only"},
		"multi format without schema": {head3 + `
channels:
  orders:
    address: orders
    messages: {created: {name: OrderCreated, headers: {schemaFormat: 'application/schema+json;version=draft-07'}, payload: {type: object}}}
`, "message OrderCreated: headers declare a schemaFormat but no schema"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := messageTypes(loadSpec(t, c.spec), "orders")
			wantErr(t, err, c.want)
		})
	}
}
