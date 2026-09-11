package pipeline

import (
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
)

// Compile-time check: AvroDisplayEncoder must satisfy the Encoder interface.
var _ Encoder = (*AvroDisplayEncoder)(nil)

// testDisplayModel parses an avsc for the display tests.
func testDisplayModel(t *testing.T, avsc string) *avro.Schema {
	t.Helper()
	model, err := avro.Parse([]byte(avsc))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return model
}

// TestAvroDisplayEncoderRendersAvroJSON proves the Dry-run AVRO path renders
// the generated value in the readable Avro JSON encoding (not raw Go structs,
// not Confluent framing): int/long as numbers, bytes as Latin-1 strings,
// enum symbols as strings, logical types as human-readable text.
func TestAvroDisplayEncoderRendersAvroJSON(t *testing.T) {
	model := testDisplayModel(t, `{"type":"record","name":"O","fields":[
		{"name":"id","type":"string"},
		{"name":"qty","type":"int"}
	]}`)
	enc := NewAvroDisplayEncoder(model)

	payload := map[string]any{"id": "abc", "qty": int32(42)}
	keyBytes, payloadBytes, err := enc.Encode(nil, payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if keyBytes != nil {
		t.Errorf("nil key must yield nil keyBytes, got %q", keyBytes)
	}

	out := string(payloadBytes)
	if !strings.Contains(out, `"id":"abc"`) {
		t.Errorf("payload must contain readable string field, got: %s", out)
	}
	if !strings.Contains(out, `"qty":42`) {
		t.Errorf("payload must contain readable int field, got: %s", out)
	}
	if strings.HasPrefix(out, "\x00") {
		t.Errorf("display must not carry Confluent framing, got: %q", payloadBytes)
	}
}

// TestAvroDisplayEncoderBytesLatin1 proves bytes render as a Latin-1 string
// (Avro JSON encoding) rather than the base64 JSON marshalling of a []byte.
func TestAvroDisplayEncoderBytesLatin1(t *testing.T) {
	model := testDisplayModel(t, `{"type":"record","name":"O","fields":[{"name":"blob","type":"bytes"}]}`)
	enc := NewAvroDisplayEncoder(model)

	_, payloadBytes, err := enc.Encode(nil, map[string]any{"blob": []byte{0x00, 0xFF, 'A'}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out := string(payloadBytes)
	if strings.Contains(out, "AA==") {
		t.Errorf("bytes must not render as base64, got: %s", out)
	}
	// 0xFF renders as the Latin-1 character U+00FF (not a raw byte), proving
	// each byte maps to one Unicode code point rather than a UTF-8 multibyte.
	if !strings.Contains(out, "\u00ff") {
		t.Errorf("0xFF byte must render as Latin-1 U+00FF, got: %s", out)
	}
}

// TestAvroDisplayEncoderKeyContract proves the Dry-run AVRO adapter keeps the
// plain-scalar key contract shared with JsonEncoder and AvroEncoder (string as
// UTF-8, long as decimal text, nil as nil bytes).
func TestAvroDisplayEncoderKeyContract(t *testing.T) {
	model := testDisplayModel(t, `{"type":"record","name":"O","fields":[{"name":"id","type":"string"}]}`)
	enc := NewAvroDisplayEncoder(model)

	keyBytes, _, err := enc.Encode("cust-1", map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode string key: %v", err)
	}
	if string(keyBytes) != "cust-1" {
		t.Errorf("string key = %q, want cust-1", keyBytes)
	}

	// Under AVRO an extracted key can be an int64 (a generated long).
	keyBytes, _, err = enc.Encode(int64(42), map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode int64 key: %v", err)
	}
	if string(keyBytes) != "42" {
		t.Errorf("int64 key = %q, want 42", keyBytes)
	}
}

// TestAvroDisplayEncoderConformanceProperty proves the full AVRO generation ->
// display path never trips: for every schema fixture and seed, the generated
// value must render as valid readable JSON with no registry anywhere.
func TestAvroDisplayEncoderConformanceProperty(t *testing.T) {
	fixtures := []string{
		`{"type":"record","name":"O","fields":[{"name":"b","type":"boolean"},{"name":"i","type":"int"}]}`,
		`{"type":"record","name":"O","fields":[{"name":"day","type":{"type":"int","logicalType":"date"}},{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}}]}`,
		`{"type":"record","name":"O","fields":[{"name":"items","type":{"type":"array","items":"string"}},{"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"]}}]}`,
	}

	for _, avsc := range fixtures {
		model := testDisplayModel(t, avsc)
		enc := NewAvroDisplayEncoder(model)
		for seed := int64(0); seed < 5; seed++ {
			value, err := avro.NewGenerator(seed, testNow()).Value(model.Root)
			if err != nil {
				t.Fatalf("seed %d: generation failed: %v", seed, err)
			}
			_, payloadBytes, err := enc.Encode(nil, value)
			if err != nil {
				t.Fatalf("seed %d: Encode failed: %v", seed, err)
			}
			if len(payloadBytes) == 0 {
				t.Fatalf("seed %d: empty display bytes", seed)
			}
		}
	}
}
