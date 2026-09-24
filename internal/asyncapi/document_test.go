package asyncapi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeSpec writes a spec string to a temp .yaml file and returns its path.
func writeSpec(t *testing.T, spec string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}
	return tmpFile
}

// payloadSchema and keyBinding read the one Message type of a Kafka topic,
// the shape every single-message test here uses.
func payloadSchema(doc *Document, topic string) (map[string]any, error) {
	mt, err := onlyType(doc, topic)
	if err != nil {
		return nil, err
	}
	return mt.Payload, nil
}

func keyBinding(doc *Document, topic string) (map[string]any, error) {
	mt, err := onlyType(doc, topic)
	if err != nil {
		return nil, err
	}
	return mt.KeyBinding, nil
}

func onlyType(doc *Document, topic string) (MessageType, error) {
	types, err := doc.MessageTypes(topic)
	if err != nil {
		return MessageType{}, err
	}
	if len(types) != 1 {
		return MessageType{}, fmt.Errorf("Kafka topic %q has %d Message types (%s), want 1", topic, len(types), names(types))
	}
	return types[0], nil
}

// names lists Message type names the way the tests compare them.
func names(types []MessageType) string {
	var out []string
	for _, mt := range types {
		out = append(out, mt.Name)
	}
	return strings.Join(out, ", ")
}

// writeJSONSpec is writeSpec for JSON input, covering the JSON decode path.
func writeJSONSpec(t *testing.T, spec string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "test.json")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}
	return tmpFile
}

// TestLoadJSONSpec covers the JSON decode path and confirms the raw map is
// JSON-normalized (numbers are float64) so ref-spliced schemas stay consistent.
func TestLoadJSONSpec(t *testing.T) {
	spec := `{
  "asyncapi": "2.6.0",
  "info": {"title": "Test", "version": "1.0.0"},
  "components": {
    "schemas": {
      "Addr": {
        "type": "object",
        "properties": {"n": {"type": "integer", "minimum": 1}}
      }
    }
  },
  "channels": {
    "test": {
      "publish": {
        "message": {
          "payload": {"$ref": "#/components/schemas/Addr"}
        }
      }
    }
  }
}`
	doc, err := Load(writeJSONSpec(t, spec))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	schema, err := payloadSchema(doc, "test")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	props, _ := schema["properties"].(map[string]any)
	n, _ := props["n"].(map[string]any)
	min, ok := n["minimum"].(float64)
	if !ok {
		t.Fatalf("expected JSON number normalized to float64, got %T", props["n"])
	}
	if min != 1 {
		t.Errorf("expected minimum 1, got %v", min)
	}
}

func TestLoadValidYAML(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels:
  test:
    publish:
      message:
        payload:
          type: object
          properties:
            id:
              type: string
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if _, err := payloadSchema(doc, "test"); err != nil {
		t.Errorf("payloadSchema: %v", err)
	}
}

func objectProp(schema map[string]any, name string) map[string]any {
	props, _ := schema["properties"].(map[string]any)
	m, _ := props[name].(map[string]any)
	return m
}

// hasRef reports whether any $ref remains anywhere in a resolved schema node.
func hasRef(v any) bool {
	switch n := v.(type) {
	case map[string]any:
		if _, ok := n["$ref"].(string); ok {
			return true
		}
		for _, child := range n {
			if hasRef(child) {
				return true
			}
		}
	case []any:
		for _, item := range n {
			if hasRef(item) {
				return true
			}
		}
	}
	return false
}

func TestPayloadSchema(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            orderId:
              type: string
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := payloadSchema(doc, "orders")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	if schema["type"] != "object" {
		t.Errorf("expected type object, got %v", schema["type"])
	}
}

func TestMissingKafkaTopic(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels: {}
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	_, err = payloadSchema(doc, "nonexistent")
	if err == nil || !strings.Contains(err.Error(), `Kafka topic "nonexistent" not found in spec`) {
		t.Errorf("err = %v, want the missing Kafka topic named", err)
	}
}

// TestPayloadSchemaRefResolution exercises the per-path expansion stack.
func TestPayloadSchemaRefResolution(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas:
    Address:
      type: object
      properties:
        street:
          type: string
channels:
  test:
    publish:
      message:
        payload:
          type: object
          properties:
            home:
              $ref: '#/components/schemas/Address'
            work:
              $ref: '#/components/schemas/Address'
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := payloadSchema(doc, "test")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	// Diamond: both siblings resolve to the same component independently.
	home := objectProp(schema, "home")
	work := objectProp(schema, "work")
	if home == nil || home["type"] != "object" {
		t.Error("expected 'home' sibling expanded to Address object")
	}
	if work == nil || work["type"] != "object" {
		t.Error("expected 'work' sibling expanded to Address object")
	}
	if objectProp(home, "street") == nil || objectProp(work, "street") == nil {
		t.Error("expected both siblings to keep their nested street property")
	}
	if hasRef(schema) {
		t.Error("expected no $ref to remain in the resolved diamond schema")
	}
}

func TestPayloadSchemaNestedRefs(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas:
    Tag:
      type: object
      properties:
        id:
          type: string
    Envelope:
      type: object
      properties:
        tags:
          type: array
          items:
            $ref: '#/components/schemas/Tag'
        combined:
          allOf:
            - $ref: '#/components/schemas/Tag'
channels:
  test:
    publish:
      message:
        payload:
          $ref: '#/components/schemas/Envelope'
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := payloadSchema(doc, "test")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	tags := objectProp(schema, "tags")
	items, _ := tags["items"].(map[string]any)
	if items["type"] != "object" {
		t.Errorf("expected array items ref expanded to Tag object, got %v", items["type"])
	}

	combined := objectProp(schema, "combined")
	allOf, _ := combined["allOf"].([]any)
	if len(allOf) != 1 {
		t.Fatalf("expected 1 allOf entry, got %d", len(allOf))
	}
	first, _ := allOf[0].(map[string]any)
	if first["type"] != "object" {
		t.Errorf("expected allOf ref expanded to Tag object, got %v", first["type"])
	}

	if hasRef(schema) {
		t.Error("expected no $ref to remain in the resolved nested schema")
	}
}

// TestPayloadSchemaRefErrors covers ref shapes that must fail loudly: a
// missing target and an external (non-#/) ref.
func TestPayloadSchemaRefErrors(t *testing.T) {
	cases := []struct {
		name        string
		spec        string
		wantContain string
	}{
		{
			name:        "missing-target",
			wantContain: "not found",
			spec: `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas: {}
channels:
  test:
    publish:
      message:
        payload:
          $ref: '#/components/schemas/DoesNotExist'
`,
		},
		{
			name:        "external-ref",
			wantContain: "external",
			spec: `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels:
  test:
    publish:
      message:
        payload:
          $ref: 'http://other/spec.yaml#/components/schemas/Foo'
`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := Load(writeSpec(t, tt.spec))
			if err != nil {
				t.Fatal(err)
			}

			_, err = payloadSchema(doc, "test")
			if err == nil {
				t.Fatal("expected an error for this ref shape")
			}
			if !strings.Contains(err.Error(), tt.wantContain) {
				t.Errorf("error should contain %q, got %q", tt.wantContain, err.Error())
			}
		})
	}
}

// TestPayloadSchemaCyclicPreserved covers self and mutual cycles: they do NOT
// error (the generator walks them with a depth budget); instead the cycle is
// preserved as a $ref node in the resolved schema.
func TestPayloadSchemaCyclicPreserved(t *testing.T) {
	cases := []struct {
		name string
		spec string
		// ref is the preserved $ref path expected in the resolved schema.
		ref string
	}{
		{
			name: "self-cycle",
			ref:  "#/$defs/components~1schemas~1Node",
			spec: `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas:
    Node:
      type: object
      properties:
        value:
          type: string
        child:
          $ref: '#/components/schemas/Node'
channels:
  test:
    publish:
      message:
        payload:
          $ref: '#/components/schemas/Node'
`,
		},
		{
			name: "mutual-cycle",
			ref:  "#/$defs/components~1schemas~1A",
			spec: `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas:
    A:
      type: object
      properties:
        b:
          $ref: '#/components/schemas/B'
    B:
      type: object
      properties:
        a:
          $ref: '#/components/schemas/A'
channels:
  test:
    publish:
      message:
        payload:
          $ref: '#/components/schemas/A'
`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := Load(writeSpec(t, tt.spec))
			if err != nil {
				t.Fatal(err)
			}

			schema, err := payloadSchema(doc, "test")
			if err != nil {
				t.Fatalf("cyclic schema should not error, got %v", err)
			}
			if schema["type"] != "object" {
				t.Errorf("expected payload to resolve, got type %v", schema["type"])
			}
			found := findRefPath(schema, tt.ref)
			if !found {
				t.Errorf("expected the cycle preserved as $ref %q in the resolved schema", tt.ref)
			}
			// Self-contained (#73): the cycle's target travels in the schema's
			// own $defs, and the cycle closes there, so no callback into the
			// spec is needed.
			defs, _ := schema["$defs"].(map[string]any)
			name := strings.ReplaceAll(strings.TrimPrefix(tt.ref, "#/$defs/"), "~1", "/")
			target, _ := defs[name].(map[string]any)
			if target["type"] != "object" || !findRefPath(defs, tt.ref) {
				t.Errorf("$defs = %v, want %q defined and the cycle closing inside $defs", defs, name)
			}
		})
	}
}

// findRefPath reports whether a $ref equal to path appears anywhere in a node.
func findRefPath(v any, path string) bool {
	switch n := v.(type) {
	case map[string]any:
		if n["$ref"] == path {
			return true
		}
		for _, child := range n {
			if findRefPath(child, path) {
				return true
			}
		}
	case []any:
		for _, item := range n {
			if findRefPath(item, path) {
				return true
			}
		}
	}
	return false
}

// TestPayloadSchemaMessageRef guards the collapsed message-extraction helper:
// a message-level $ref through components.messages still resolves its payload.
func TestPayloadSchemaMessageRef(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  messages:
    OrderMessage:
      payload:
        type: object
        properties:
          orderId:
            type: string
channels:
  orders:
    publish:
      message:
        $ref: '#/components/messages/OrderMessage'
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := payloadSchema(doc, "orders")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}
	if schema["type"] != "object" {
		t.Errorf("expected message-level $ref to resolve payload, got type %v", schema["type"])
	}
}

func TestKeyBindingPresent(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              type: string
        payload:
          type: object
          properties:
            id:
              type: string
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("KeyBinding failed: %v", err)
	}
	if binding == nil {
		t.Fatal("expected non-nil binding")
	}
	if binding["type"] != "string" {
		t.Errorf("expected binding type string, got %v", binding["type"])
	}
}

func TestKeyBindingAbsent(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            id:
              type: string
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("KeyBinding failed: %v", err)
	}
	if binding != nil {
		t.Errorf("expected nil binding when absent, got %v", binding)
	}
}

func TestKeyBindingResolvesRef(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
components:
  schemas:
    OrderKey:
      type: string
      format: uuid
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              $ref: '#/components/schemas/OrderKey'
        payload:
          type: object
          properties:
            id:
              type: string
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	binding, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("KeyBinding failed: %v", err)
	}
	if binding == nil {
		t.Fatal("expected non-nil binding")
	}
	if binding["type"] != "string" {
		t.Errorf("expected resolved binding type string, got %v", binding["type"])
	}
	if binding["format"] != "uuid" {
		t.Errorf("expected resolved binding format uuid, got %v", binding["format"])
	}
	if hasRef(binding) {
		t.Error("expected no $ref to remain in resolved binding")
	}
}

func TestKeyBindingMissingKafkaTopic(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info:
  title: Test
  version: '1.0.0'
channels: {}
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	_, err = keyBinding(doc, "nonexistent")
	if err == nil {
		t.Error("expected error for missing Kafka topic")
	}
}

// TestKafkaTopicFromBinding proves the spec entry for a Kafka topic is found
// through its bindings.kafka.topic, which may differ from the entry's key, and
// that an entry binding another Kafka topic is not found by its key (#72).
func TestKafkaTopicFromBinding(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info: {title: Test, version: '1.0.0'}
channels:
  orders-v1:
    bindings: {kafka: {topic: orders}}
    publish:
      message:
        payload: {type: object, properties: {orderId: {type: string}}}
  payments:
    bindings: {kafka: {bindingVersion: '0.4.0'}}
    publish:
      message:
        payload: {type: object, properties: {paymentId: {type: string}}}
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := payloadSchema(doc, "orders")
	if err != nil {
		t.Fatalf("PayloadSchema(orders): %v", err)
	}
	if _, ok := schema["properties"].(map[string]any)["orderId"]; !ok {
		t.Errorf("schema = %v, want the orders-v1 entry's Payload", schema)
	}
	// The entry's key is what a user may type; the error points to the
	// Kafka topic the entry binds.
	_, err = payloadSchema(doc, "orders-v1")
	if err == nil || !strings.Contains(err.Error(), `the spec entry orders-v1 binds Kafka topic "orders"`) {
		t.Errorf("err = %v, want the bound Kafka topic named", err)
	}
	// A kafka binding without a topic leaves the entry's key as the Kafka topic.
	if _, err := payloadSchema(doc, "payments"); err != nil {
		t.Errorf("PayloadSchema(payments): %v", err)
	}
}

// TestKafkaTopicWithTwoEntries proves two spec entries bound to one Kafka
// topic both contribute their Message types.
func TestKafkaTopicWithTwoEntries(t *testing.T) {
	spec := `
asyncapi: '2.6.0'
info: {title: Test, version: '1.0.0'}
channels:
  orders-v2:
    bindings: {kafka: {topic: orders}}
    publish: {message: {payload: {type: object}}}
  orders:
    publish: {message: {payload: {type: object}}}
`
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatal(err)
	}
	// Both entries belong to the Kafka topic, so both inline messages are
	// its Message types, named by where they are declared (#34 decision 2).
	types, err := doc.MessageTypes("orders")
	if err != nil {
		t.Fatalf("MessageTypes: %v", err)
	}
	if got := names(types); got != "orders publish, orders-v2 publish" {
		t.Errorf("Message types = %s, want both entries' messages", got)
	}
}

// loadSpec writes and loads a spec, failing the test on a load error.
func loadSpec(t *testing.T, spec string) *Document {
	t.Helper()
	doc, err := Load(writeSpec(t, spec))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return doc
}

// wantErr asserts err is non-nil and mentions every fragment.
func wantErr(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want an error mentioning %q", fragments)
	}
	for _, f := range fragments {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("err = %v, want it to mention %q", err, f)
		}
	}
}

// TestMessageTypesNeverFallBack reproduces #73's silent fallbacks: a broken
// message $ref is reported as itself, never replaced by another operation's
// message or hidden behind "no message found".
func TestMessageTypesNeverFallBack(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message: {$ref: '#/components/messages/Typo'}
    subscribe:
      message: {payload: {type: object}}
components:
  messages:
    Order: {payload: {type: object}}
`)
	_, err := doc.MessageTypes("orders")
	wantErr(t, err, "#/components/messages/Typo", "orders publish")
}

// TestMessageTypesListsEvery proves every Message type of a Kafka topic is
// read - oneOf variants, publish and subscribe - in a stable order, named by
// the message's name, else its component key, else where it is declared.
func TestMessageTypesListsEvery(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: OrderCreated, payload: {type: object}}
          - {$ref: '#/components/messages/OrderUpdated'}
          - {payload: {type: object}}
    subscribe:
      message: {payload: {type: object}}
components:
  messages:
    OrderUpdated: {payload: {type: object}}
`)
	for i := 0; i < 20; i++ {
		types, err := doc.MessageTypes("orders")
		if err != nil {
			t.Fatalf("MessageTypes: %v", err)
		}
		want := "OrderCreated, OrderUpdated, orders publish oneOf[2], orders subscribe"
		if got := names(types); got != want {
			t.Fatalf("run %d: Message types = %s, want %s", i, got, want)
		}
	}
}

// TestMessageTypesDeduplicates proves a component message referenced by both
// operations is one Message type, not two.
func TestMessageTypesDeduplicates(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish: {message: {$ref: '#/components/messages/Order'}}
    subscribe: {message: {$ref: '#/components/messages/Order'}}
components:
  messages:
    Order: {payload: {type: object}}
`)
	types, err := doc.MessageTypes("orders")
	if err != nil {
		t.Fatalf("MessageTypes: %v", err)
	}
	if got := names(types); got != "Order" {
		t.Errorf("Message types = %s, want the one component message", got)
	}
}

// TestMessageTypesFollowRefsAtEveryLevel proves a $ref resolves wherever the
// spec may use one: the entry's bindings, the message, its bindings, the
// kafka binding and the key schema.
func TestMessageTypesFollowRefsAtEveryLevel(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders-v1:
    bindings: {$ref: '#/components/channelBindings/orders'}
    publish: {message: {$ref: '#/components/messages/Order'}}
components:
  channelBindings:
    orders: {kafka: {topic: orders}}
  messageBindings:
    keyed: {kafka: {$ref: '#/components/kafka/keyed'}}
  kafka:
    keyed: {key: {$ref: '#/components/schemas/Key'}}
  schemas:
    Key: {type: string, format: uuid}
  messages:
    Order:
      bindings: {$ref: '#/components/messageBindings/keyed'}
      payload: {type: object}
`)
	key, err := keyBinding(doc, "orders")
	if err != nil {
		t.Fatalf("keyBinding: %v", err)
	}
	if key["type"] != "string" || key["format"] != "uuid" {
		t.Errorf("key binding = %v, want the referenced Key schema", key)
	}
}

// TestMessageTypesRejectUnusableBinding proves a declared but unusable key
// binding stops the run, naming the message, instead of a silent null Key.
func TestMessageTypesRejectUnusableBinding(t *testing.T) {
	cases := map[string]string{
		"bindings not an object": `bindings: yes`,
		"kafka not an object":    `bindings: {kafka: yes}`,
		"key not a schema":       `bindings: {kafka: {key: string}}`,
	}
	for name, binding := range cases {
		t.Run(name, func(t *testing.T) {
			doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        name: Order
        `+binding+`
        payload: {type: object}
`)
			_, err := doc.MessageTypes("orders")
			wantErr(t, err, "Order")
		})
	}
}

// TestRefEscapes proves $refs are JSON Pointers (RFC 6901): ~1 and ~0 unescape
// to / and ~, and percent-encoding in the fragment decodes.
func TestRefEscapes(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            a: {$ref: '#/components/schemas/a~1b'}
            b: {$ref: '#/components/schemas/c~0d'}
            c: {$ref: '#/components/schemas/e%20f'}
components:
  schemas:
    a/b: {type: string}
    c~d: {type: integer}
    e f: {type: boolean}
`)
	schema, err := payloadSchema(doc, "orders")
	if err != nil {
		t.Fatalf("payloadSchema: %v", err)
	}
	props := schema["properties"].(map[string]any)
	for field, want := range map[string]string{"a": "string", "b": "integer", "c": "boolean"} {
		if got := props[field].(map[string]any)["type"]; got != want {
			t.Errorf("%s resolved to type %v, want %s", field, got, want)
		}
	}
}

// TestEntryLevelMessagesNamed proves a 2.x spec entry that declares its
// messages the 3.0 way is told so, instead of reading the map in random order
// or reporting no message.
func TestEntryLevelMessagesNamed(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  orders:
    messages:
      created: {payload: {type: object}}
      cancelled: {payload: {type: object}}
`)
	_, err := doc.MessageTypes("orders")
	wantErr(t, err, "spec entry orders", "messages", "AsyncAPI 3.0")
}

// TestCycleTargetsKeepTheirRefs proves a cycle's $defs target is the original
// schema with each $ref rewritten to a local one, not expanded: every $ref the
// generator follows inside a cycle still costs one step of its depth budget,
// as it did when it followed the spec's refs (ADR-0005). Expanding B into A's
// definition would halve the cost of an A-B-A cycle and double how deep it
// nests.
func TestCycleTargetsKeepTheirRefs(t *testing.T) {
	doc := loadSpec(t, `
asyncapi: '2.6.0'
info: {title: T, version: '1'}
channels:
  t: {publish: {message: {payload: {$ref: '#/components/schemas/A'}}}}
components:
  schemas:
    A: {type: object, properties: {b: {$ref: '#/components/schemas/B'}}}
    B: {type: object, properties: {a: {$ref: '#/components/schemas/A'}}}
`)
	schema, err := payloadSchema(doc, "t")
	if err != nil {
		t.Fatalf("payloadSchema: %v", err)
	}
	defs := schema["$defs"].(map[string]any)
	a, _ := defs["components/schemas/A"].(map[string]any)
	b, _ := defs["components/schemas/B"].(map[string]any)
	aToB := a["properties"].(map[string]any)["b"].(map[string]any)["$ref"]
	bToA := b["properties"].(map[string]any)["a"].(map[string]any)["$ref"]
	if aToB != "#/$defs/components~1schemas~1B" || bToA != "#/$defs/components~1schemas~1A" {
		t.Errorf("$defs A.b = %v, B.a = %v; want both kept as local $refs", aToB, bToA)
	}
}
