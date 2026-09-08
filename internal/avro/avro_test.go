package avro

import (
	"errors"
	"reflect"
	"testing"

	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	gogen "github.com/actgardner/gogen-avro/v10/schema"
)

// assertParseError asserts err is an *ParseError, so every malformed-avsc test
// lands on the same typed-error shape (ADR-0007 decision 4).
func assertParseError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *ParseError, got %T (%v)", err, err)
	}
	if pe.Error() == "" {
		t.Error("ParseError must carry a message")
	}
}

func TestParsePrimitiveRoot(t *testing.T) {
	cases := []struct {
		name string
		avsc string
		kind PrimitiveKind
	}{
		{"null", `"null"`, KindNull},
		{"boolean", `{"type": "boolean"}`, KindBoolean},
		{"int", `{"type": "int"}`, KindInt},
		{"long", `{"type": "long"}`, KindLong},
		{"float", `{"type": "float"}`, KindFloat},
		{"double", `{"type": "double"}`, KindDouble},
		{"bytes", `{"type": "bytes"}`, KindBytes},
		{"string", `{"type": "string"}`, KindString},
		{"bare string", `"string"`, KindString},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse([]byte(c.avsc))
			if err != nil {
				t.Fatalf("Parse(%s) failed: %v", c.avsc, err)
			}
			p, ok := s.Root.(*Primitive)
			if !ok {
				t.Fatalf("Root = %T, want *Primitive", s.Root)
			}
			if p.Kind != c.kind {
				t.Errorf("Kind = %q, want %q", p.Kind, c.kind)
			}
			if p.Logical != nil {
				t.Errorf("Logical = %+v, want nil for plain %s", p.Logical, c.kind)
			}
		})
	}
}

func TestParseComplexRecord(t *testing.T) {
	avsc := `{
		"type": "record",
		"name": "Order",
		"namespace": "com.example",
		"fields": [
			{"name": "orderId", "type": "string"},
			{"name": "active", "type": "boolean", "default": true},
			{"name": "customer", "type": {"type": "record", "name": "Customer", "namespace": "com.example",
				"fields": [{"name": "id", "type": "long"}, {"name": "email", "type": "string"}]}},
			{"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "Item", "namespace": "com.example",
				"fields": [{"name": "sku", "type": "string"}, {"name": "qty", "type": "int"}]}}},
			{"name": "labels", "type": {"type": "map", "values": "string"}},
			{"name": "status", "type": {"type": "enum", "name": "Status", "namespace": "com.example",
				"symbols": ["PLACED", "SHIPPED", "DELIVERED"], "default": "PLACED"}},
			{"name": "blob", "type": {"type": "fixed", "name": "Digest", "namespace": "com.example", "size": 16}},
			{"name": "note", "type": ["null", "string"], "default": null}
		]
	}`

	s, err := Parse([]byte(avsc))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	rec, ok := s.Root.(*Record)
	if !ok {
		t.Fatalf("Root = %T, want *Record", s.Root)
	}
	if rec.Name != "Order" || rec.Namespace != "com.example" {
		t.Errorf("Name/Namespace = %q/%q, want Order/com.example", rec.Name, rec.Namespace)
	}

	// Field order is preserved, and each field's type lands on the expected kind.
	wantKinds := []TypeKind{
		KindString, KindBoolean, KindRecord, KindArray, KindMap, KindEnum, KindFixed, KindUnion,
	}
	if len(rec.Fields) != len(wantKinds) {
		t.Fatalf("got %d fields, want %d", len(rec.Fields), len(wantKinds))
	}
	for i, f := range rec.Fields {
		if got := typeKind(f.Type); got != wantKinds[i] {
			t.Errorf("field %q: type kind = %v, want %v", f.Name, got, wantKinds[i])
		}
	}

	customer := rec.Fields[2].Type.(*Record)
	if customer.FullName() != "com.example.Customer" {
		t.Errorf("customer fullname = %q, want com.example.Customer", customer.FullName())
	}
	if customer.Fields[0].Name != "id" || typeKind(customer.Fields[0].Type) != KindLong {
		t.Errorf("customer.id = kind %v, want long", typeKind(customer.Fields[0].Type))
	}

	items := rec.Fields[3].Type.(*Array)
	if typeKind(items.Items) != KindRecord {
		t.Errorf("array items kind = %v, want record", typeKind(items.Items))
	}

	status := rec.Fields[5].Type.(*Enum)
	if !reflect.DeepEqual(status.Symbols, []string{"PLACED", "SHIPPED", "DELIVERED"}) {
		t.Errorf("enum symbols = %v", status.Symbols)
	}
	if status.Default == nil || *status.Default != "PLACED" {
		t.Errorf("enum default = %v, want PLACED", status.Default)
	}

	blob := rec.Fields[6].Type.(*Fixed)
	if blob.Size != 16 {
		t.Errorf("fixed size = %d, want 16", blob.Size)
	}

	note := rec.Fields[7].Type.(*Union)
	if note.NullIndex() != 0 {
		t.Errorf("union NullIndex = %d, want 0", note.NullIndex())
	}
	if len(note.Branches) != 2 || typeKind(note.Branches[1]) != KindString {
		t.Errorf("union branches = %v, want [null string]", note.Branches)
	}
}

func TestParseConstructRoots(t *testing.T) {
	cases := []struct {
		name string
		avsc string
	}{{
		"union", `["null", "string"]`,
	}, {
		"array", `{"type": "array", "items": "int"}`,
	}, {
		"map", `{"type": "map", "values": "long"}`,
	}, {
		"enum", `{"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}`,
	}, {
		"fixed", `{"type": "fixed", "name": "Checksum", "size": 4}`,
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Parse([]byte(c.avsc)); err != nil {
				t.Fatalf("Parse(%s) failed: %v", c.avsc, err)
			}
		})
	}
}

func TestParseNamedTypeSharing(t *testing.T) {
	// A named type referenced twice (here the record recursively) resolves to
	// the same model node, so the generator walks one shared definition and
	// cycles terminate under a depth budget - mirroring the JSON $ref walk.
	avsc := `{
		"type": "record",
		"name": "Tree",
		"fields": [
			{"name": "label", "type": "string"},
			{"name": "left", "type": "Tree"},
			{"name": "right", "type": "Tree"}
		]
	}`
	s, err := Parse([]byte(avsc))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	rec, ok := s.Root.(*Record)
	if !ok {
		t.Fatalf("Root = %T, want *Record", s.Root)
	}
	if rec.Fields[0].Name != "label" || rec.Fields[2].Name != "right" {
		t.Fatalf("unexpected field layout: %v", rec.Fields)
	}
	left, okLeft := rec.Fields[1].Type.(*Record)
	right, okRight := rec.Fields[2].Type.(*Record)
	if !okLeft || !okRight {
		t.Fatalf("recursive field types = %T/%T, want *Record/*Record",
			rec.Fields[1].Type, rec.Fields[2].Type)
	}
	if left != rec || right != rec {
		t.Error("named references must share the root *Record node")
	}
	if !reflect.DeepEqual(left, right) {
		t.Error("shared node must deep-equal itself")
	}
}

func TestParseLogicalTypes(t *testing.T) {
	cases := []struct {
		name    string
		avsc    string
		base    PrimitiveKind
		logical LogicalTypeKind
	}{
		{"timestamp-millis", `{"type": "long", "logicalType": "timestamp-millis"}`, KindLong, LogicalTimestampMillis},
		{"timestamp-micros", `{"type": "long", "logicalType": "timestamp-micros"}`, KindLong, LogicalTimestampMicros},
		{"date", `{"type": "int", "logicalType": "date"}`, KindInt, LogicalDate},
		{"time-millis", `{"type": "int", "logicalType": "time-millis"}`, KindInt, LogicalTimeMillis},
		{"time-micros", `{"type": "long", "logicalType": "time-micros"}`, KindLong, LogicalTimeMicros},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse([]byte(c.avsc))
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			p, ok := s.Root.(*Primitive)
			if !ok {
				t.Fatalf("Root = %T, want *Primitive", s.Root)
			}
			if p.Kind != c.base {
				t.Errorf("base kind = %q, want %q", p.Kind, c.base)
			}
			if p.Logical == nil || p.Logical.Kind != c.logical {
				t.Errorf("logical type = %+v, want %q", p.Logical, c.logical)
			}
		})
	}
}

func TestParseDecimalLogicalType(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		avsc := `{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}`
		s, err := Parse([]byte(avsc))
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		p := s.Root.(*Primitive)
		if p.Logical.Kind != LogicalDecimal || p.Logical.Precision != 10 || p.Logical.Scale != 2 {
			t.Errorf("decimal = %+v, want precision 10 scale 2", p.Logical)
		}
	})

	t.Run("fixed with default scale", func(t *testing.T) {
		avsc := `{"type": "fixed", "name": "Price", "size": 8, "logicalType": "decimal", "precision": 18}`
		s, err := Parse([]byte(avsc))
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		f := s.Root.(*Fixed)
		if f.Logical == nil || f.Logical.Kind != LogicalDecimal || f.Logical.Scale != 0 {
			t.Errorf("fixed decimal = %+v, want kind decimal scale 0", f.Logical)
		}
	})

	t.Run("missing precision", func(t *testing.T) {
		_, err := Parse([]byte(`{"type": "bytes", "logicalType": "decimal"}`))
		assertParseError(t, err)
	})
}

func TestParseRejectsUnsupportedLogicalType(t *testing.T) {
	cases := []string{
		`{"type": "long", "logicalType": "date"}`,
		`{"type": "string", "logicalType": "uuid"}`,
		`{"type": "string", "logicalType": "duration"}`,
		`{"type": "int", "logicalType": "timestamp-millis"}`,
	}
	for _, avsc := range cases {
		_, err := Parse([]byte(avsc))
		assertParseError(t, err)
	}
}

func TestParseRejectsMalformedAvsc(t *testing.T) {
	cases := []struct {
		name string
		avsc string
	}{
		{"empty", ""},
		{"whitespace", "   \n\t"},
		{"invalid json", `{"type": "record",`},
		{"json scalar", `42`},
		{"empty object", `{}`},
		{"unknown type", `{"type": "widget"}`},
		{"record missing name", `{"type": "record", "fields": []}`},
		{"record missing fields", `{"type": "record", "name": "R"}`},
		{"field without type", `{"type": "record", "name": "R", "fields": [{"name": "f"}]}`},
		{"unresolvable root reference", `"Order"`},
		{"duplicate named definition",
			`{"type": "record", "name": "R", "fields": [
				{"name": "a", "type": {"type": "enum", "name": "E", "symbols": ["X"]}},
				{"name": "b", "type": {"type": "enum", "name": "E", "symbols": ["Y"]}}
			]}`},
		{"enum missing symbols", `{"type": "enum", "name": "E"}`},
		{"enum symbols not strings", `{"type": "enum", "name": "E", "symbols": [1, 2]}`},
		{"fixed missing size", `{"type": "fixed", "name": "F"}`},
		{"fixed non-numeric size", `{"type": "fixed", "name": "F", "size": "16"}`},
		{"union branch invalid", `["null", {"type": "bogus"}]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.avsc))
			assertParseError(t, err)
		})
	}
}

func TestParseNoPanicOnWeirdInput(t *testing.T) {
	inputs := []string{
		"null",
		"true",
		`{"type": null}`,
		`{"type": ["string", "integer"]}`,
		`{"type": []}`,
		`[`,
		`"`,
		`{"fields": []}`,
		"[]",
		"[[]]",
		`{"type": "array"}`,
		`{"type": "map"}`,
		"null",
		"0",
		`{"type":"long","logicalType":""}`,
	}
	for i, in := range inputs {
		in := in
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("input[%d] %q panicked: %v", i, in, r)
				}
			}()
			_, _ = Parse([]byte(in))
		}()
	}
}

// TestParseMatchesSerializer checks the parse-model's acceptance matches the
// exact gogen-avro parse path confluent-kafka-go's Avro serde uses, so the
// avsc the serializer accepts parses identically (issue #21 acceptance 4).
func TestParseMatchesSerializer(t *testing.T) {
	fixtures := []string{
		`{"type": "record", "name": "O", "fields": [{"name": "a", "type": "int"}]}`,
		`{"type": "record", "name": "O", "fields": [{"name": "a", "type": ["null", "long"]}]}`,
		`{"type": "array", "items": "string"}`,
		`{"type": "map", "values": {"type": "record", "name": "M", "fields": []}}`,
		`{"type": "enum", "name": "E", "symbols": ["A", "B"]}`,
		`{"type": "fixed", "name": "F", "size": 2}`,
		`{"type": "long", "logicalType": "timestamp-millis"}`,
		`{"type": "widget"}`,
		`{"type": "record"}`,
		`"nope"`,
		"not json",
	}
	for _, avsc := range fixtures {
		_, ourErr := Parse([]byte(avsc))
		_, serErr := parseLikeSerializer([]byte(avsc))
		if (ourErr == nil) != (serErr == nil) {
			t.Errorf("avsc %q: model parse error = %v, serializer parse error = %v (must agree)",
				avsc, ourErr, serErr)
		}
	}
}

// parseLikeSerializer reproduces the gogen-avro parse sequence confluent's
// Avro serde runs (TypeForSchema + resolver over roots), used by the parity
// test above; production code never calls this.
func parseLikeSerializer(avsc []byte) (gogen.AvroType, error) {
	ns := parser.NewNamespace(false)
	root, err := ns.TypeForSchema(avsc)
	if err != nil {
		return nil, err
	}
	for _, def := range ns.Roots {
		if err := resolver.ResolveDefinition(def, ns.Definitions); err != nil {
			return nil, err
		}
	}
	return root, nil
}

// typeKind extracts the model kind of a Type for concise assertions.
func typeKind(t Type) TypeKind {
	switch x := t.(type) {
	case *Primitive:
		return x.Kind
	case *Record:
		return KindRecord
	case *Union:
		return KindUnion
	case *Array:
		return KindArray
	case *Map:
		return KindMap
	case *Enum:
		return KindEnum
	case *Fixed:
		return KindFixed
	default:
		panic("unknown Type")
	}
}
