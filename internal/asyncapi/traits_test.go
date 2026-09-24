package asyncapi

import (
	"reflect"
	"testing"
)

// TestTraitCarriesKeyBinding reproduces #81: a Key binding declared in a
// message trait was never applied, so the run silently used a null Key. It
// must apply whether the trait is inline or a $ref.
func TestTraitCarriesKeyBinding(t *testing.T) {
	cases := map[string]string{
		"inline": `{bindings: {kafka: {key: {type: string, format: uuid}}}}`,
		"$ref":   `{$ref: '#/components/messageTraits/keyed'}`,
	}
	for name, trait := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish: {message: {$ref: '#/components/messages/OrderCreated'}}
components:
  messages:
    OrderCreated:
      traits: [`+trait+`]
      payload: {type: object}
  messageTraits:
    keyed: {bindings: {kafka: {key: {type: string, format: uuid}}}}
`)
			key, err := keyBinding(doc, "orders")
			if err != nil {
				t.Fatalf("keyBinding: %v", err)
			}
			want := map[string]any{"type": "string", "format": "uuid"}
			if !reflect.DeepEqual(key, want) {
				t.Errorf("key binding = %v, want the trait's %v", key, want)
			}
		})
	}
}

// TestTraitsMergeAsJSONMergePatch proves 2.x trait semantics (AsyncAPI 2.6.0):
// traits merge into the message with JSON Merge Patch (RFC 7386) in the order
// listed, so a trait overrides the message, a later trait overrides an earlier
// one, objects merge key by key, and a null removes a field.
func TestTraitsMergeAsJSONMergePatch(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: Own
        traits:
          - name: First
            bindings: {kafka: {key: {maxLength: 9, minLength: 2, pattern: '^a'}}}
          - name: Second
            bindings: {kafka: {key: {minLength: ~}}}
        bindings: {kafka: {key: {type: string, maxLength: 5}}}
        payload: {type: object}
`)
	mt, err := onlyType(doc, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if mt.Name != "Second" {
		t.Errorf("name = %s, want the last trait's Second", mt.Name)
	}
	want := map[string]any{"type": "string", "maxLength": 9.0, "pattern": "^a"}
	if !reflect.DeepEqual(mt.KeyBinding, want) {
		t.Errorf("key binding = %v, want %v", mt.KeyBinding, want)
	}
}

// TestTraitsMergeThroughRefs proves a merge follows a $ref wherever both the
// message and the trait declare an object: a trait adding to a referenced
// binding extends it instead of being shadowed by the $ref.
func TestTraitsMergeThroughRefs(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        traits: [{bindings: {kafka: {key: {format: uuid}}}}]
        bindings: {$ref: '#/components/messageBindings/keyed'}
        payload: {type: object}
components:
  messageBindings:
    keyed: {kafka: {key: {$ref: '#/components/schemas/Key'}}}
  schemas:
    Key: {type: string}
`)
	key, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("keyBinding: %v", err)
	}
	want := map[string]any{"type": "string", "format": "uuid"}
	if !reflect.DeepEqual(key, want) {
		t.Errorf("key binding = %v, want %v", key, want)
	}
}

// TestTraitsMergeLeaveTheSpecIntact proves merging copies: a component
// message used by two Kafka topics, one through a trait-bearing wrapper, is
// read unchanged by the other.
func TestTraitsMergeLeaveTheSpecIntact(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        traits: [{bindings: {kafka: {key: {format: uuid}}}}]
        bindings: {$ref: '#/components/messageBindings/keyed'}
        payload: {type: object}
  audit:
    publish:
      message:
        bindings: {$ref: '#/components/messageBindings/keyed'}
        payload: {type: object}
components:
  messageBindings:
    keyed: {kafka: {key: {type: string}}}
`)
	if _, err := keyBinding(doc, "orders"); err != nil {
		t.Fatal(err)
	}
	key, err := keyBinding(doc, "audit")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"type": "string"}; !reflect.DeepEqual(key, want) {
		t.Errorf("audit key binding = %v, want the untouched %v", key, want)
	}
}

// TestTraitsMergeSameCyclicRef proves a message and a trait naming the same
// cyclic schema merge to that schema instead of following the cycle forever.
func TestTraitsMergeSameCyclicRef(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        traits: [{bindings: {kafka: {key: {$ref: '#/components/schemas/Node'}}}}]
        bindings: {kafka: {key: {$ref: '#/components/schemas/Node'}}}
        payload: {type: object}
components:
  schemas:
    Node:
      type: object
      properties: {next: {$ref: '#/components/schemas/Node'}}
`)
	key, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("keyBinding: %v", err)
	}
	if key["type"] != "object" {
		t.Errorf("key binding = %v, want the Node schema", key)
	}
}

// TestTraitsRejectMistakes proves an unusable trait stops the run naming the
// message, rather than being skipped.
func TestTraitsRejectMistakes(t *testing.T) {
	cases := map[string]struct{ traits, want string }{
		"traits not a list":   {`traits: {bindings: {}}`, "traits must be a list"},
		"trait not an object": {`traits: [keyed]`, "traits[0]: must be an object"},
		"unresolvable $ref":   {`traits: [{$ref: '#/components/messageTraits/nope'}]`, `"nope" not found`},
		"trait with payload":  {`traits: [{}, {payload: {type: string}}]`, "traits[1] declares payload, which a message trait may not"},
		"trait with traits":   {`traits: [{traits: []}]`, "traits[0] declares traits, which a message trait may not"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        `+c.traits+`
        payload: {type: object}
components:
  messageTraits: {}
`)
			_, err := doc.MessageTypes("orders")
			wantErr(t, err, "OrderCreated", c.want)
		})
	}
}

// TestSchemaFormatsRead proves every payload format AsyncAPI 2.6.0 requires
// an implementation to support is read as JSON Schema.
func TestSchemaFormatsRead(t *testing.T) {
	for _, format := range []string{
		"application/vnd.aai.asyncapi;version=2.6.0",
		"application/vnd.aai.asyncapi+json;version=2.6.0",
		"application/vnd.aai.asyncapi+yaml;version=2.0.0",
		"application/schema+json;version=draft-07",
		"application/schema+yaml;version=draft-07",
		"Application/Schema+JSON; version=draft-07",
	} {
		t.Run(format, func(t *testing.T) {
			doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        schemaFormat: '`+format+`'
        payload: {type: object, properties: {id: {type: string}}}
`)
			payload, err := payloadSchema(doc, "orders")
			if err != nil {
				t.Fatalf("payloadSchema: %v", err)
			}
			if payload["type"] != "object" {
				t.Errorf("payload = %v, want the declared schema", payload)
			}
		})
	}
}

// TestSchemaFormatsRefused reproduces #81: an Avro payload was read as JSON
// Schema and failed with a puzzling unsupported type "record". Every format
// the tool does not read stops the run naming the format and the Message
// type, whether the message or a trait declares it.
func TestSchemaFormatsRefused(t *testing.T) {
	cases := map[string]string{
		"avro":             `schemaFormat: 'application/vnd.apache.avro;version=1.9.0'`,
		"avro in a trait":  `traits: [{schemaFormat: 'application/vnd.apache.avro;version=1.9.0'}]`,
		"protobuf":         `schemaFormat: 'application/vnd.google.protobuf;version=3'`,
		"unknown":          `schemaFormat: 'application/x-made-up'`,
		"other draft":      `schemaFormat: 'application/schema+json;version=draft-2020-12'`,
		"asyncapi 3":       `schemaFormat: 'application/vnd.aai.asyncapi+json;version=3.0.0'`,
		"no version":       `schemaFormat: 'application/schema+json'`,
		"not a media type": `schemaFormat: ';;'`,
	}
	for name, field := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        `+field+`
        payload: {type: record, name: OrderCreated, fields: []}
`)
			_, err := doc.MessageTypes("orders")
			wantErr(t, err, "payload of OrderCreated is ", ", which the tool does not read")
		})
	}
}

// TestSchemaFormatRefusalNamesTheFormat pins the whole message, the format
// written as the spec declares it.
func TestSchemaFormatRefusalNamesTheFormat(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
        payload: {type: record, name: OrderCreated, fields: []}
`)
	_, err := doc.MessageTypes("orders")
	want := "payload of OrderCreated is application/vnd.apache.avro;version=1.9.0, which the tool does not read"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}

// TestSchemaFormatMustBeAString proves a malformed schemaFormat is an error,
// not an absent one.
func TestSchemaFormatMustBeAString(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: OrderCreated
        schemaFormat: 7
        payload: {type: object}
`)
	_, err := doc.MessageTypes("orders")
	wantErr(t, err, "OrderCreated", "schemaFormat must be a string")
}
