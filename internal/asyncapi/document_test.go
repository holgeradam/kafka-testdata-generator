package asyncapi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPayloadSchemaResolvesMessageAndNestedSchemaReferences(t *testing.T) {
	spec := []byte(`
asyncapi: 2.6.0
info:
  title: Test
  version: 1.0.0
channels:
  users.created:
    publish:
      message:
        $ref: '#/components/messages/UserCreated'
components:
  messages:
    UserCreated:
      name: UserCreated
      payload:
        $ref: '#/components/schemas/User'
  schemas:
    User:
      type: object
      properties:
        id:
          type: string
          format: uuid
        profile:
          $ref: '#/components/schemas/Profile'
    Profile:
      type: object
      properties:
        email:
          type: string
          format: email
`)
	path := filepath.Join(t.TempDir(), "spec.yaml")
	if err := os.WriteFile(path, spec, 0o600); err != nil {
		t.Fatal(err)
	}

	doc, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	schema, selection, err := doc.PayloadSchema(Selection{Channel: "users.created"})
	if err != nil {
		t.Fatalf("PayloadSchema() error = %v", err)
	}

	if selection.Message != "UserCreated" {
		t.Fatalf("message = %q, want UserCreated", selection.Message)
	}

	properties := schema["properties"].(map[string]any)
	profile := properties["profile"].(map[string]any)
	if _, exists := profile["$ref"]; exists {
		t.Fatal("profile reference was not resolved")
	}
}
