package asyncapi

import (
	"os"
	"path/filepath"
	"testing"
)

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
	tmpFile := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}

	doc, err := Load(tmpFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if doc.AsyncAPI != "2.6.0" {
		t.Errorf("expected asyncapi 2.6.0, got %s", doc.AsyncAPI)
	}
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
	tmpFile := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}

	doc, err := Load(tmpFile)
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
	tmpFile := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}

	doc, err := Load(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	_, err = doc.PayloadSchema("nonexistent")
	if err == nil {
		t.Error("expected error for missing channel")
	}
}

func TestRecursiveRefNoStackOverflow(t *testing.T) {
	spec := `
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
`
	tmpFile := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(tmpFile, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}

	doc, err := Load(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	schema, err := doc.PayloadSchema("test")
	if err != nil {
		t.Fatalf("PayloadSchema failed: %v", err)
	}

	if schema["type"] != "object" {
		t.Errorf("expected type object, got %v", schema["type"])
	}
}
