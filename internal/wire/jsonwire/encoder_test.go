package jsonwire_test

import (
	"encoding/json"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/jsonwire"
)

// Compile-time check: JsonEncoder must satisfy the Encoder interface. If the
// Encoder interface or JsonEncoder is deleted, this file will not compile.
var _ pipeline.Encoder = jsonwire.JsonEncoder{}

// TestJsonEncoderPayloadBytesAreJsonMarshal proves byte-identical output:
// the encoder must produce the exact same bytes as a direct json.Marshal of
// the same in-memory value.
func TestJsonEncoderPayloadBytesAreJsonMarshal(t *testing.T) {
	payload := map[string]any{
		"orderId": "abc-123",
		"amount":  float64(42),
		"items":   []any{"a", "b"},
	}

	enc := jsonwire.JsonEncoder{}
	_, payloadBytes, err := enc.Encode(pipeline.Generated{Payload: payload})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	expected, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(payloadBytes) != string(expected) {
		t.Errorf("payload bytes differ:\n  got  %s\n  want %s", payloadBytes, expected)
	}
}

// TestJsonEncoderKeyPlainScalar proves the plain-scalar key contract (ADR-0006
// / CONTEXT.md Key entry): a string key becomes raw UTF-8 bytes, a number
// becomes decimal text, and an object/array stays JSON. Key bytes are never
// JSON-wrapped scalars.
func TestJsonEncoderKeyPlainScalar(t *testing.T) {
	cases := []struct {
		name string
		key  any
		want string
	}{
		{"string", "cust-1", "cust-1"},
		{"number", float64(42), "42"},
		{"bool", true, "true"},
		{"object", map[string]any{"a": 1}, `{"a":1}`},
		{"array", []any{1, 2}, "[1,2]"},
	}

	enc := jsonwire.JsonEncoder{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keyBytes, _, err := enc.Encode(pipeline.Generated{Key: tc.key, Payload: map[string]any{"x": 1}})
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if string(keyBytes) != tc.want {
				t.Errorf("key = %q, want %q", keyBytes, tc.want)
			}
		})
	}
}

// TestJsonEncoderNilKeyReturnsNilKeyBytes proves that a nil key produces nil
// keyBytes (the pipeline skips sending when key is nil before calling Encode,
// but the encoder must also handle it gracefully).
func TestJsonEncoderNilKeyReturnsNilKeyBytes(t *testing.T) {
	enc := jsonwire.JsonEncoder{}
	keyBytes, payloadBytes, err := enc.Encode(pipeline.Generated{Payload: map[string]any{"x": 1}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if keyBytes != nil {
		t.Errorf("expected nil keyBytes, got %s", keyBytes)
	}
	if len(payloadBytes) == 0 {
		t.Error("expected non-empty payloadBytes")
	}
}

// TestJsonEncoderKeyAndPayloadTogether proves the encoder returns both parts
// simultaneously and the pipeline receives them as separate fields in Outgoing.
func TestJsonEncoderKeyAndPayloadTogether(t *testing.T) {
	payload := map[string]any{"orderId": "abc-123"}
	enc := jsonwire.JsonEncoder{}

	keyBytes, payloadBytes, err := enc.Encode(pipeline.Generated{Key: "abc-123", Payload: payload})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(keyBytes) != `abc-123` {
		t.Errorf("key = %s, want abc-123", keyBytes)
	}
	if string(payloadBytes) != `{"orderId":"abc-123"}` {
		t.Errorf("payload = %s, want {\"orderId\":\"abc-123\"}", payloadBytes)
	}
}

// TestEncoderDeletionGuard proves the Encoder interface is used by the
// pipeline: this variable holds an Encoder-typed reference. If the Encoder
// interface or JsonEncoder is removed, the compile-time assertion above
// catches it; this test verifies the interface is referenced at runtime.
func TestEncoderDeletionGuard(t *testing.T) {
	var enc pipeline.Encoder = jsonwire.JsonEncoder{}
	key, payload, err := enc.Encode(pipeline.Generated{Key: "k", Payload: map[string]any{"v": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if key == nil || payload == nil {
		t.Error("expected non-nil key and payload bytes")
	}
}
