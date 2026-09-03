package pipeline

import "testing"

// samplePayload is a generated Payload exercising the path shapes #25 asks for:
// nested object traversal and array indexing.
func samplePayload() map[string]any {
	return map[string]any{
		"customer": map[string]any{
			"id": "cust-1",
		},
		"items": []any{
			map[string]any{"sku": "ABC-DE-0001"},
			map[string]any{"sku": "ABC-DE-0002"},
		},
		"name": "alice",
	}
}

func TestExtractKeyTopLevelField(t *testing.T) {
	payload := samplePayload()
	if got := extractKey(payload, "name"); got != "alice" {
		t.Errorf("top-level extract = %v, want alice", got)
	}
}

func TestExtractKeyNestedField(t *testing.T) {
	payload := samplePayload()
	if got := extractKey(payload, "customer.id"); got != "cust-1" {
		t.Errorf("nested extract = %v, want cust-1", got)
	}
}

func TestExtractKeyArrayIndex(t *testing.T) {
	payload := samplePayload()
	got := extractKey(payload, "items[0].sku")
	want := "ABC-DE-0001"
	if got != want {
		t.Errorf("array index extract = %v, want %v", got, want)
	}
}

func TestExtractKeyMissingSegment(t *testing.T) {
	payload := samplePayload()
	if got := extractKey(payload, "customer.missing"); got != nil {
		t.Errorf("missing segment = %v, want nil", got)
	}
}

func TestExtractKeyMissingTopLevel(t *testing.T) {
	payload := samplePayload()
	if got := extractKey(payload, "nope.deep.field"); got != nil {
		t.Errorf("missing top-level = %v, want nil", got)
	}
}

func TestExtractKeyIndexOutOfRange(t *testing.T) {
	payload := samplePayload()
	if got := extractKey(payload, "items[5].sku"); got != nil {
		t.Errorf("index out of range = %v, want nil", got)
	}
}

func TestExtractKeyTraverseThroughArray(t *testing.T) {
	payload := samplePayload()
	got := extractKey(payload, "items[1].sku")
	want := "ABC-DE-0002"
	if got != want {
		t.Errorf("traverse array index = %v, want %v", got, want)
	}
}
