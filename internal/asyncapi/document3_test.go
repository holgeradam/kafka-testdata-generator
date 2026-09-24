package asyncapi

import (
	"path/filepath"
	"reflect"
	"testing"
)

const head3 = "asyncapi: '3.0.0'\ninfo: {title: T, version: '1'}\n"

// TestV3ExampleMatchesV2 proves the 3.0 example reads to the same Message
// schema and Key binding as the 2.x example it restates, so both produce the
// same seeded output.
func TestV3ExampleMatchesV2(t *testing.T) {
	read := func(name string) MessageType {
		t.Helper()
		doc, err := Load(filepath.Join("..", "..", "examples", name))
		if err != nil {
			t.Fatalf("Load %s: %v", name, err)
		}
		mt, err := onlyType(doc, "orders.created")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return mt
	}
	v2, v3 := read("order.asyncapi.yaml"), read("order.asyncapi.v3.yaml")
	if !reflect.DeepEqual(v2.Payload, v3.Payload) {
		t.Errorf("payloads differ:\n2.x: %v\n3.0: %v", v2.Payload, v3.Payload)
	}
	if !reflect.DeepEqual(v2.KeyBinding, v3.KeyBinding) {
		t.Errorf("Key bindings differ: 2.x %v, 3.0 %v", v2.KeyBinding, v3.KeyBinding)
	}
	if v3.Name != "OrderCreated" {
		t.Errorf("name = %s, want OrderCreated", v3.Name)
	}
}

// TestV3KafkaTopic proves a 3.0 channel's Kafka topic is bindings.kafka.topic
// when declared, else its address, and never the channel id.
func TestV3KafkaTopic(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  bound:
    address: orders.v1
    bindings: {kafka: {topic: orders}}
    messages: {Order: {payload: {type: object}}}
  addressed:
    address: payments
    messages: {Payment: {payload: {type: object}}}
`)
	for topic, want := range map[string]string{"orders": "Order", "payments": "Payment"} {
		mt, err := onlyType(doc, topic)
		if err != nil {
			t.Fatalf("%s: %v", topic, err)
		}
		if mt.Name != want {
			t.Errorf("%s: Message type %s, want %s", topic, mt.Name, want)
		}
	}
	for topic, hint := range map[string]string{
		"orders.v1": `Kafka topic "orders.v1" not found`,
		"bound":     `the spec entry bound names Kafka topic "orders"`,
		"addressed": `the spec entry addressed names Kafka topic "payments"`,
	} {
		_, err := messageTypes(doc, topic)
		wantErr(t, err, hint)
	}
}

// TestV3UnknownAddress proves a channel with a null or absent address names
// no Kafka topic, as 3.0 says it "MUST be interpreted as unknown", and a
// -topic equal to its id says so.
func TestV3UnknownAddress(t *testing.T) {
	for name, address := range map[string]string{"null": "address: ~", "absent": ""} {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+`
channels:
  orders:
    `+address+`
    messages: {Order: {payload: {type: object}}}
`)
			_, err := messageTypes(doc, "orders")
			wantErr(t, err, `Kafka topic "orders" not found`, "the spec entry orders has no address, so it names no Kafka topic")
		})
	}
}

// TestV3Refs proves channels, their bindings and messages resolve through
// components.
func TestV3Refs(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  orders: {$ref: '#/components/channels/orders'}
components:
  channels:
    orders:
      address: orders
      bindings: {$ref: '#/components/channelBindings/orders'}
      messages:
        created: {$ref: '#/components/messages/OrderCreated'}
  channelBindings:
    orders: {kafka: {topic: orders.all}}
  messages:
    OrderCreated:
      bindings: {kafka: {key: {$ref: '#/components/schemas/Id'}}}
      payload: {$ref: '#/components/schemas/Order'}
  schemas:
    Id: {type: string, format: uuid}
    Order: {type: object, properties: {id: {$ref: '#/components/schemas/Id'}}}
`)
	mt, err := onlyType(doc, "orders.all")
	if err != nil {
		t.Fatal(err)
	}
	if mt.Name != "OrderCreated" {
		t.Errorf("name = %s, want the component key OrderCreated", mt.Name)
	}
	id := map[string]any{"type": "string", "format": "uuid"}
	if !reflect.DeepEqual(mt.KeyBinding, id) {
		t.Errorf("key binding = %v, want %v", mt.KeyBinding, id)
	}
	want := map[string]any{"type": "object", "properties": map[string]any{"id": id}}
	if !reflect.DeepEqual(mt.Payload, want) {
		t.Errorf("payload = %v, want %v", mt.Payload, want)
	}
}

// TestV3MessageTypes proves every message of every channel bound to the Kafka
// topic is a Message type, in a stable order (channels by id, messages by
// key), a component message referenced twice counting once, and a channel
// referenced twice read once. Operations are never read.
func TestV3MessageTypes(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  b-orders:
    address: orders
    messages:
      updated: {$ref: '#/components/messages/OrderUpdated'}
      created: {name: OrderCreated, payload: {type: object}}
  a-orders:
    address: orders
    messages:
      cancelled: {payload: {type: object}}
      again: {$ref: '#/components/messages/OrderUpdated'}
  c-orders: {$ref: '#/components/channels/shared'}
  d-orders: {$ref: '#/components/channels/shared'}
  other:
    address: payments
    messages: {paid: {payload: {type: object}}}
operations:
  send:
    action: send
    channel: {$ref: '#/channels/other'}
components:
  channels:
    shared:
      address: orders
      messages: {shipped: {payload: {type: object}}}
  messages:
    OrderUpdated: {payload: {type: object}}
`)
	for i := 0; i < 20; i++ {
		types, err := messageTypes(doc, "orders")
		if err != nil {
			t.Fatalf("MessageTypes: %v", err)
		}
		want := "OrderUpdated, cancelled, OrderCreated, shipped"
		if got := names(types); got != want {
			t.Fatalf("run %d: Message types = %s, want %s", i, got, want)
		}
	}
}

// TestV3TraitsMessageWins proves 3.0 trait semantics: traits merge in order,
// a later trait over an earlier one, but "a property on a trait MUST NOT
// override the same property on the target object", so the message wins.
// The message's own nulls are data, kept as written.
func TestV3TraitsMessageWins(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created:
        name: Own
        traits:
          - {$ref: '#/components/messageTraits/first'}
          - name: Second
            bindings: {kafka: {key: {minLength: 3, pattern: '^a'}}}
        bindings: {kafka: {key: {type: string, maxLength: 5}}}
        payload: {type: object, properties: {note: {type: ['string', 'null'], default: ~}}}
components:
  messageTraits:
    first: {name: First, bindings: {kafka: {key: {maxLength: 9, minLength: 2}}}}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if mt.Name != "Own" {
		t.Errorf("name = %s, want the message's own Own", mt.Name)
	}
	want := map[string]any{"type": "string", "maxLength": 5.0, "minLength": 3.0, "pattern": "^a"}
	if !reflect.DeepEqual(mt.KeyBinding, want) {
		t.Errorf("key binding = %v, want %v", mt.KeyBinding, want)
	}
	note := mt.Payload["properties"].(map[string]any)["note"].(map[string]any)
	if v, ok := note["default"]; !ok || v != nil {
		t.Errorf("note = %v, want its default: null kept", note)
	}
}

// TestV3TraitCarriesKeyBinding proves a 3.0 trait fills a Key binding the
// message does not declare.
func TestV3TraitCarriesKeyBinding(t *testing.T) {
	doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created:
        traits: [{bindings: {kafka: {key: {type: string, format: uuid}}}}]
        payload: {type: object}
`)
	key, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"type": "string", "format": "uuid"}; !reflect.DeepEqual(key, want) {
		t.Errorf("key binding = %v, want %v", key, want)
	}
}

// TestV3MultiFormatSchemaRead proves a payload declared as a Multi Format
// Schema Object in a JSON Schema format is read, inline or through a $ref,
// and a plain Schema Object needs no format.
func TestV3MultiFormatSchemaRead(t *testing.T) {
	cases := map[string]string{
		"no format":         `{type: object, properties: {id: {type: string}}}`,
		"asyncapi":          `{schemaFormat: 'application/vnd.aai.asyncapi;version=3.0.0', schema: {type: object, properties: {id: {type: string}}}}`,
		"asyncapi+json":     `{schemaFormat: 'application/vnd.aai.asyncapi+json;version=3.0.0', schema: {type: object, properties: {id: {type: string}}}}`,
		"asyncapi+yaml":     `{schemaFormat: 'application/vnd.aai.asyncapi+yaml;version=3.0.0', schema: {type: object, properties: {id: {type: string}}}}`,
		"draft-07 json":     `{schemaFormat: 'application/schema+json;version=draft-07', schema: {type: object, properties: {id: {type: string}}}}`,
		"draft-07 yaml":     `{schemaFormat: 'application/schema+yaml;version=draft-07', schema: {type: object, properties: {id: {type: string}}}}`,
		"multi format $ref": `{$ref: '#/components/schemas/Multi'}`,
		"schema $ref":       `{schemaFormat: 'application/schema+json;version=draft-07', schema: {$ref: '#/components/schemas/Order'}}`,
		"plain $ref":        `{$ref: '#/components/schemas/Order'}`,
	}
	want := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string"}}}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages: {created: {payload: `+payload+`}}
components:
  schemas:
    Order: {type: object, properties: {id: {type: string}}}
    Multi: {schemaFormat: 'application/schema+json;version=draft-07', schema: {$ref: '#/components/schemas/Order'}}
`)
			got, err := payloadSchema(doc, "orders")
			if err != nil {
				t.Fatalf("payloadSchema: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("payload = %v, want %v", got, want)
			}
		})
	}
}

// TestV3MultiFormatSchemaRefused proves a Multi Format Schema in a format the
// tool does not read stops the run naming the format and the Message type.
func TestV3MultiFormatSchemaRefused(t *testing.T) {
	for name, format := range map[string]string{
		"avro":         "application/vnd.apache.avro;version=1.9.0",
		"protobuf":     "application/vnd.google.protobuf;version=3",
		"raml":         "application/raml+yaml;version=1.0",
		"asyncapi 2.x": "application/vnd.aai.asyncapi+json;version=2.6.0",
	} {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+`
channels:
  orders:
    address: orders
    messages:
      created:
        name: OrderCreated
        payload: {schemaFormat: '`+format+`', schema: {type: record, name: OrderCreated, fields: []}}
`)
			_, err := messageTypes(doc, "orders")
			wantErr(t, err, "payload of OrderCreated is "+format+", which the tool does not read")
		})
	}
}

// TestV3RejectMistakes proves 2.x syntax in a 3.0 document, and a malformed
// 3.0 structure, stop the run naming where, instead of being skipped.
func TestV3RejectMistakes(t *testing.T) {
	cases := map[string]struct{ channel, want string }{
		"publish on a channel": {`
    address: orders
    publish: {message: {payload: {type: object}}}`, "spec entry orders: publish is AsyncAPI 2.x syntax; in 3.0, declare the channel's messages under messages"},
		"schemaFormat on a message": {`
    address: orders
    messages: {created: {name: OrderCreated, schemaFormat: 'application/schema+json;version=draft-07', payload: {type: object}}}`,
			"message OrderCreated: schemaFormat on a message is AsyncAPI 2.x syntax; in 3.0, declare the payload as {schemaFormat, schema}"},
		"multi format without schema": {`
    address: orders
    messages: {created: {name: OrderCreated, payload: {schemaFormat: 'application/schema+json;version=draft-07'}}}`,
			"message OrderCreated: payload declares a schemaFormat but no schema"},
		"messages not a map": {`
    address: orders
    messages: [{payload: {type: object}}]`, "spec entry orders: messages must be an object"},
		"address not a string": {`
    address: 7
    messages: {created: {payload: {type: object}}}`, "spec entry orders: address must be a string"},
		"message trait sets payload": {`
    address: orders
    messages: {created: {name: OrderCreated, traits: [{payload: {}}], payload: {type: object}}}`,
			"traits[0] declares payload, which a message trait may not"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, head3+"channels:\n  orders:"+c.channel+"\n")
			_, err := messageTypes(doc, "orders")
			wantErr(t, err, c.want)
		})
	}
}
