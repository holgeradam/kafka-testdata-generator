package asyncapi

import (
	"errors"
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

	schema, err := doc.PayloadSchema("test")
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

	if doc.AsyncAPI != "2.6.0" {
		t.Errorf("expected asyncapi 2.6.0, got %s", doc.AsyncAPI)
	}
}

func objectProp(schema map[string]any, name string) map[string]any {
	props, _ := schema["properties"].(map[string]any)
	m, _ := props[name].(map[string]any)
	return m
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

	schema, err := doc.PayloadSchema("orders")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	if schema["type"] != "object" {
		t.Errorf("expected type object, got %v", schema["type"])
	}
}

func TestMissingChannel(t *testing.T) {
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

	_, err = doc.PayloadSchema("nonexistent")
	if err == nil {
		t.Error("expected error for missing channel")
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

	schema, err := doc.PayloadSchema("test")
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
	if ref := findPreservedRef(schema); ref != "" {
		t.Errorf("expected no $ref to remain in the resolved diamond schema, found %s", ref)
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

	schema, err := doc.PayloadSchema("test")
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

	if ref := findPreservedRef(schema); ref != "" {
		t.Errorf("expected no $ref to remain in the resolved nested schema, found %s", ref)
	}
}

// TestPayloadSchemaRefErrors covers every ref shape that must fail loudly:
// self and mutual cycles (typed UnsupportedRecursionError naming the ref), a
// missing target, and an external (non-#/) ref.
func TestPayloadSchemaRefErrors(t *testing.T) {
	cases := []struct {
		name string
		spec string
		// wantRecursion means the failure must be a typed UnsupportedRecursionError.
		wantRecursion bool
		// wantContain, when non-empty, must appear in the error text.
		wantContain string
	}{
		{
			name:         "self-cycle",
			wantRecursion: true,
			wantContain:  "#/components/schemas/Node",
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
			name:         "mutual-cycle",
			wantRecursion: true,
			wantContain:  "#/components/schemas/A",
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
		{
			name:         "missing-target",
			wantContain:  "not found",
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
			name:         "external-ref",
			wantContain:  "external",
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

			_, err = doc.PayloadSchema("test")
			if err == nil {
				t.Fatal("expected an error for this ref shape")
			}
			if tt.wantRecursion {
				var rec *UnsupportedRecursionError
				if !errors.As(err, &rec) {
					t.Fatalf("expected *UnsupportedRecursionError, got %T: %v", err, err)
				}
			}
			if tt.wantContain != "" && !strings.Contains(err.Error(), tt.wantContain) {
				t.Errorf("error should contain %q, got %q", tt.wantContain, err.Error())
			}
		})
	}
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

	schema, err := doc.PayloadSchema("orders")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}
	if schema["type"] != "object" {
		t.Errorf("expected message-level $ref to resolve payload, got type %v", schema["type"])
	}
}
