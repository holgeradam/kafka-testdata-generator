package asyncapi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAML(t *testing.T) {
	content := `asyncapi: 2.6.0
info:
  title: Test API
  version: 1.0.0
channels:
  orders.created:
    publish:
      message:
        payload:
          type: object
          properties:
            orderId:
              type: string
              format: uuid
            amount:
              type: number
              minimum: 0
              maximum: 1000
`
	tmpDir := t.TempDir()
	specPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(specPath, []byte(content), 0644); err != nil {
		t.Fatalf("writing spec: %v", err)
	}

	doc, err := Load(specPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if doc.AsyncAPI != "2.6.0" {
		t.Errorf("AsyncAPI = %q, want %q", doc.AsyncAPI, "2.6.0")
	}

	if doc.Info.Title != "Test API" {
		t.Errorf("Info.Title = %q, want %q", doc.Info.Title, "Test API")
	}

	if _, ok := doc.Channels["orders.created"]; !ok {
		t.Error("channel orders.created not found")
	}
}

func TestPayloadSchema(t *testing.T) {
	content := `asyncapi: 2.6.0
info:
  title: Test API
  version: 1.0.0
channels:
  orders.created:
    publish:
      message:
        payload:
          type: object
          properties:
            orderId:
              type: string
`
	tmpDir := t.TempDir()
	specPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(specPath, []byte(content), 0644); err != nil {
		t.Fatalf("writing spec: %v", err)
	}

	doc, err := Load(specPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	schema, err := doc.PayloadSchema("orders.created")
	if err != nil {
		t.Fatalf("PayloadSchema() error: %v", err)
	}

	if schema["type"] != "object" {
		t.Errorf("schema type = %v, want object", schema["type"])
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not found or not a map")
	}

	if _, ok := props["orderId"]; !ok {
		t.Error("orderId property not found")
	}
}

func TestPayloadSchemaRef(t *testing.T) {
	content := `asyncapi: 2.6.0
info:
  title: Test API
  version: 1.0.0
channels:
  users.created:
    publish:
      message:
        $ref: '#/components/messages/UserCreated'
components:
  messages:
    UserCreated:
      payload:
        type: object
        properties:
          userId:
            type: string
`
	tmpDir := t.TempDir()
	specPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(specPath, []byte(content), 0644); err != nil {
		t.Fatalf("writing spec: %v", err)
	}

	doc, err := Load(specPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	schema, err := doc.PayloadSchema("users.created")
	if err != nil {
		t.Fatalf("PayloadSchema() error: %v", err)
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not found or not a map")
	}

	if _, ok := props["userId"]; !ok {
		t.Error("userId property not found")
	}
}
