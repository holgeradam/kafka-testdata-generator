package generator

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestGeneratesRealisticObjectFromSchema(t *testing.T) {
	gen := New(rand.New(rand.NewSource(7)))
	schema := map[string]any{
		"type": "object",
		"required": []any{
			"customerId",
			"email",
			"status",
			"totalAmount",
			"items",
		},
		"properties": map[string]any{
			"customerId": map[string]any{"type": "string", "format": "uuid"},
			"email":      map[string]any{"type": "string", "format": "email"},
			"status":     map[string]any{"type": "string", "enum": []any{"pending", "paid"}},
			"totalAmount": map[string]any{
				"type":    "number",
				"minimum": float64(10),
				"maximum": float64(20),
			},
			"items": map[string]any{
				"type":     "array",
				"minItems": float64(2),
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"quantity": map[string]any{"type": "integer"},
					},
				},
			},
		},
	}

	value, err := gen.Value(schema, "")
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	object := value.(map[string]any)
	if object["customerId"] == "" {
		t.Fatal("expected customerId")
	}
	if object["email"] == "" {
		t.Fatal("expected email")
	}
	if object["status"] != "pending" && object["status"] != "paid" {
		t.Fatalf("unexpected status: %v", object["status"])
	}
	if amount := object["totalAmount"].(float64); amount < 10 || amount > 20 {
		t.Fatalf("amount out of bounds: %v", amount)
	}
	if len(object["items"].([]any)) < 2 {
		t.Fatal("expected at least two items")
	}
}

func TestSameSeedGeneratesSameValue(t *testing.T) {
	schema := map[string]any{
		"type":     "object",
		"required": []any{"createdAt", "id", "totalAmount"},
		"properties": map[string]any{
			"id":          map[string]any{"type": "string", "format": "uuid"},
			"createdAt":   map[string]any{"type": "string", "format": "date-time"},
			"totalAmount": map[string]any{"type": "number"},
			"city":        map[string]any{"type": "string"},
		},
	}

	first, err := New(rand.New(rand.NewSource(42))).Value(schema, "")
	if err != nil {
		t.Fatalf("first Value() error = %v", err)
	}
	second, err := New(rand.New(rand.NewSource(42))).Value(schema, "")
	if err != nil {
		t.Fatalf("second Value() error = %v", err)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed generated different values:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}
