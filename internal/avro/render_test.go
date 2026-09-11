package avro

import (
	"encoding/json"
	"math/big"
	"reflect"
	"testing"
	"time"
)

func TestRenderPrimitives(t *testing.T) {
	avsc := `{"type":"record","name":"O","fields":[
		{"name":"n","type":"null"},
		{"name":"b","type":"boolean"},
		{"name":"i","type":"int"},
		{"name":"l","type":"long"},
		{"name":"f","type":"float"},
		{"name":"d","type":"double"},
		{"name":"by","type":"bytes"},
		{"name":"s","type":"string"}
	]}`
	model := mustParse(t, avsc)

	rec := map[string]any{
		"n":  nil,
		"b":  true,
		"i":  int32(42),
		"l":  int64(99),
		"f":  float32(3.14),
		"d":  float64(2.718281828),
		"by": []byte{0x00, 0xFF, 'A'},
		"s":  "hello",
	}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, b)
	}

	if got["n"] != nil {
		t.Errorf("null = %v, want null (nil)", got["n"])
	}
	if got["b"] != true {
		t.Errorf("boolean = %v, want true", got["b"])
	}
	if int32(got["i"].(float64)) != 42 {
		t.Errorf("int = %v, want 42", got["i"])
	}
	if int64(got["l"].(float64)) != 99 {
		t.Errorf("long = %v, want 99", got["l"])
	}
	// float/double: just check they are numbers
	if _, ok := got["f"].(float64); !ok {
		t.Errorf("float = %T %v, want float64", got["f"], got["f"])
	}
	if _, ok := got["d"].(float64); !ok {
		t.Errorf("double = %T %v, want float64", got["d"], got["d"])
	}
	// bytes rendered as Latin-1 string: json.Unmarshal decodes each byte as
	// one Unicode code point (0xFF -> U+00FF, rendered "ÿ"), not a raw byte.
	byStr, ok := got["by"].(string)
	if !ok {
		t.Fatalf("bytes = %T, want string", got["by"])
	}
	if byStr != "\x00\u00ffA" {
		t.Errorf("bytes = %q, want %q", byStr, "\x00\u00ffA")
	}
	if got["s"] != "hello" {
		t.Errorf("string = %v, want hello", got["s"])
	}
}

func TestRenderNullPrimitive(t *testing.T) {
	model := mustParse(t, `{"type":"null"}`)
	b, err := RenderJSON(model.Root, nil)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	if string(b) != "null" {
		t.Errorf("null = %s, want null", b)
	}
}

func TestRenderEnum(t *testing.T) {
	model := mustParse(t, `{"type":"enum","name":"State","symbols":["NEW","DONE"]}`)
	b, err := RenderJSON(model.Root, "NEW")
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	if string(b) != `"NEW"` {
		t.Errorf("enum = %s, want \"NEW\"", b)
	}
}

func TestRenderArray(t *testing.T) {
	model := mustParse(t, `{"type":"array","items":"int"}`)
	b, err := RenderJSON(model.Root, []any{int32(1), int32(2), int32(3)})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got []float64
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON array: %v\n%s", err, b)
	}
	if len(got) != 3 || int32(got[0]) != 1 || int32(got[1]) != 2 || int32(got[2]) != 3 {
		t.Errorf("array = %v, want [1 2 3]", got)
	}
}

func TestRenderMap(t *testing.T) {
	model := mustParse(t, `{"type":"map","values":"long"}`)
	b, err := RenderJSON(model.Root, map[string]any{"k0": int64(10), "k1": int64(20)})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]float64
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON object: %v\n%s", err, b)
	}
	if int64(got["k0"]) != 10 || int64(got["k1"]) != 20 {
		t.Errorf("map = %v, want {k0:10, k1:20}", got)
	}
}

func TestRenderUnionNullBranch(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"u","type":["null","string"]}]}`)
	b, err := RenderJSON(model.Root, map[string]any{"u": map[string]any{"null": nil}})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	if got["u"] != nil {
		t.Errorf("union null = %v, want null", got["u"])
	}
}

func TestRenderUnionStringBranch(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"u","type":["null","string"]}]}`)
	b, err := RenderJSON(model.Root, map[string]any{"u": map[string]any{"string": "foo"}})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	if got["u"] != "foo" {
		t.Errorf("union string = %v, want foo", got["u"])
	}
}

func TestRenderFixed(t *testing.T) {
	model := mustParse(t, `{"type":"fixed","name":"Code","size":4}`)
	// Fixed values come from the generator as [N]byte (via reflect).
	arr := [4]byte{0x01, 0x02, 0x03, 0x04}
	b, err := RenderJSON(model.Root, arr)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("fixed = %s, not a JSON string: %v", b, err)
	}
	if got != "\x01\x02\x03\x04" {
		t.Errorf("fixed = %q, want latin1 bytes", got)
	}
}

func TestRenderFixedDecimal(t *testing.T) {
	model := mustParse(t, `{"type":"fixed","name":"Amt","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
	r := new(big.Rat).SetFrac(big.NewInt(12345), big.NewInt(100))
	b, err := RenderJSON(model.Root, r)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("decimal = %s, not a JSON string: %v", b, err)
	}
	// FloatString(scale=2) produces "123.45"
	if got != "123.45" {
		t.Errorf("decimal = %q, want 123.45", got)
	}
}

func TestRenderDate(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}}]}`)
	ts := time.Date(2026, 9, 15, 12, 30, 0, 0, time.UTC)
	rec := map[string]any{"d": ts}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	dateStr, ok := got["d"].(string)
	if !ok {
		t.Fatalf("date = %T %v, want string", got["d"], got["d"])
	}
	if dateStr != "2026-09-15" {
		t.Errorf("date = %q, want 2026-09-15", dateStr)
	}
}

func TestRenderTimestampMillis(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
	ts := time.Date(2026, 9, 15, 14, 30, 45, 0, time.UTC)
	rec := map[string]any{"ts": ts}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	tsStr, ok := got["ts"].(string)
	if !ok {
		t.Fatalf("timestamp = %T %v, want string", got["ts"], got["ts"])
	}
	if tsStr != "2026-09-15T14:30:45Z" {
		t.Errorf("timestamp = %q, want 2026-09-15T14:30:45Z", tsStr)
	}
}

func TestRenderTimestampMicros(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}}]}`)
	ts := time.Date(2026, 1, 1, 0, 0, 0, 123000, time.UTC) // 123 microseconds
	rec := map[string]any{"ts": ts}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	// Timestamp renders as an ISO 8601 instant, keeping sub-second precision.
	tsStr := got["ts"].(string)
	if tsStr != "2026-01-01T00:00:00.000123Z" {
		t.Errorf("timestamp-micros = %q, want 2026-01-01T00:00:00.000123Z", tsStr)
	}
}

func TestRenderTimeMillis(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"t","type":{"type":"int","logicalType":"time-millis"}}]}`)
	d := 13*time.Hour + 45*time.Minute + 30*time.Second + 123*time.Millisecond
	rec := map[string]any{"t": d}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	timeStr, ok := got["t"].(string)
	if !ok {
		t.Fatalf("time = %T %v, want string", got["t"], got["t"])
	}
	if timeStr != "13:45:30.123" {
		t.Errorf("time = %q, want 13:45:30.123", timeStr)
	}
}

func TestRenderTimeMicros(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"t","type":{"type":"long","logicalType":"time-micros"}}]}`)
	d := 14*time.Hour + 30*time.Minute + 15*time.Second + 456000*time.Microsecond
	rec := map[string]any{"t": d}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	timeStr := got["t"].(string)
	if timeStr != "14:30:15.456000" {
		t.Errorf("time-micros = %q, want 14:30:15.456000", timeStr)
	}
}

func TestRenderBytesDecimal(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`)
	r := new(big.Rat).SetFrac(big.NewInt(9999), big.NewInt(100))
	rec := map[string]any{"amt": r}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	amtStr := got["amt"].(string)
	if amtStr != "99.99" {
		t.Errorf("decimal = %q, want 99.99", amtStr)
	}
}

func TestRenderNestedRecord(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[
		{"name":"id","type":"string"},
		{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}}
	]}`)
	rec := map[string]any{
		"id":    "abc",
		"inner": map[string]any{"x": int32(7)},
	}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	inner := got["inner"].(map[string]any)
	if int32(inner["x"].(float64)) != 7 {
		t.Errorf("nested x = %v, want 7", inner["x"])
	}
}

func TestRenderNestedArray(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[
		{"name":"matrix","type":{"type":"array","items":{"type":"array","items":"long"}}}
	]}`)
	rec := map[string]any{
		"matrix": []any{
			[]any{int64(1), int64(2)},
			[]any{int64(3)},
		},
	}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	matrix := got["matrix"].([]any)
	if len(matrix) != 2 {
		t.Fatalf("matrix rows = %d, want 2", len(matrix))
	}
	inner := matrix[0].([]any)
	if int64(inner[0].(float64)) != 1 || int64(inner[1].(float64)) != 2 {
		t.Errorf("matrix[0] = %v, want [1,2]", inner)
	}
}

func TestRenderUnionWithRecordBranch(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","namespace":"com.acme","fields":[
		{"name":"maybe","type":["null",{"type":"record","name":"R","fields":[{"name":"y","type":"int"}]}]}
	]}`)
	rec := map[string]any{
		"maybe": map[string]any{"com.acme.R": map[string]any{"y": int32(99)}},
	}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	// Active branch is com.acme.R; Avro JSON encoding renders just the inner value.
	maybe := got["maybe"].(map[string]any)
	if int32(maybe["y"].(float64)) != 99 {
		t.Errorf("union record y = %v, want 99", maybe["y"])
	}
}

func TestRenderUnionWithLogicalPrimitiveBranch(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[
		{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
	]}`)
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rec := map[string]any{
		"ts": map[string]any{"long.timestamp-millis": ts},
	}

	b, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("output not JSON: %v\n%s", err, b)
	}
	tsStr := got["ts"].(string)
	if tsStr != "2026-01-01T12:00:00Z" {
		t.Errorf("union timestamp = %q, want 2026-01-01T12:00:00Z", tsStr)
	}
}

func TestRenderJSONDeterministic(t *testing.T) {
	model := mustParse(t, `{"type":"record","name":"O","fields":[
		{"name":"id","type":"string"},
		{"name":"qty","type":"int"},
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
	]}`)
	ts := time.Date(2026, 9, 15, 14, 30, 45, 0, time.UTC)
	rec := map[string]any{"id": "abc", "qty": int32(5), "ts": ts}

	b1, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	b2, err := RenderJSON(model.Root, rec)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	if !reflect.DeepEqual(b1, b2) {
		t.Errorf("non-deterministic output:\n  1: %s\n  2: %s", b1, b2)
	}
}

// TestRenderConformanceProperty proves every value the AVRO generator emits
// can be rendered in the Avro JSON encoding at any seed: a value that honours
// the avsc (ADR-0007 decision 3) must never trip the display path. Fixtures
// mirror the encoder conformance suite so dry-run display and produced bytes
// cover the same schema space.
func TestRenderConformanceProperty(t *testing.T) {
	fixtures := []struct {
		name string
		avsc string
	}{
		{"primitives", `{"type":"record","name":"O","fields":[
			{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},
			{"name":"f","type":"float"},{"name":"d","type":"double"},{"name":"by","type":"bytes"},
			{"name":"s","type":"string"},{"name":"n","type":"null"}]}`},
		{"logical", `{"type":"record","name":"O","fields":[
			{"name":"day","type":{"type":"int","logicalType":"date"}},
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
			{"name":"tms","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
			{"name":"tmu","type":{"type":"long","logicalType":"time-micros"}},
			{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
			{"name":"price","type":{"type":"fixed","name":"Price","size":8,"logicalType":"decimal","precision":18,"scale":4}}]}`},
		{"collections", `{"type":"record","name":"O","fields":[
			{"name":"items","type":{"type":"array","items":"int"}},
			{"name":"attrs","type":{"type":"map","values":"long"}},
			{"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"]}},
			{"name":"code","type":{"type":"fixed","name":"Code","size":4}},
			{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"x","type":"long"}]}}]}`},
		{"unions", `{"type":"record","name":"O","namespace":"com.acme","fields":[
			{"name":"maybe","type":["null","string"]},
			{"name":"choice","type":["string","int"]},
			{"name":"optionalDate","type":["null",{"type":"int","logicalType":"date"}]},
			{"name":"optDecimal","type":["null",{"type":"bytes","logicalType":"decimal","precision":6,"scale":3}]},
			{"name":"nested","type":["null",{"type":"record","name":"R","fields":[{"name":"y","type":"int"}]}]}]}`},
		{"nested-collections", `{"type":"record","name":"O","fields":[
			{"name":"matrix","type":{"type":"array","items":{"type":"array","items":"long"}}},
			{"name":"dict","type":{"type":"map","values":{"type":"record","name":"V","fields":[{"name":"z","type":"double"}]}}},
			{"name":"tags","type":{"type":"array","items":{"type":"map","values":"boolean"}}}]}`},
		{"recursive-linked", `{"type":"record","name":"Node","fields":[
			{"name":"value","type":"long"},
			{"name":"next","type":["null","Node"]}]}`},
	}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			root := mustParse(t, fx.avsc).Root
			for seed := int64(0); seed < 6; seed++ {
				value, err := NewGenerator(seed, now).Value(root)
				if err != nil {
					t.Fatalf("seed %d: generation failed: %v", seed, err)
				}
				b, err := RenderJSON(root, value)
				if err != nil {
					t.Fatalf("seed %d: RenderJSON failed: %v", seed, err)
				}
				if !json.Valid(b) {
					t.Fatalf("seed %d: rendered output is not valid JSON: %s", seed, b)
				}
			}
		})
	}
}
