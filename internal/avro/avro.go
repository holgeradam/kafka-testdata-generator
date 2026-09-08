// Package avro parses Apache Avro schemas (avsc) into a generation model the
// AVRO wire format drives values from (ADR-0007 decision 3: generation follows
// the avsc). Parsing delegates to the same gogen-avro schema parser the
// Confluent Go Avro serde uses, so the avsc the serializer accepts parses
// identically into this model (issue #21).
//
// The model covers the Avro constructs the generator will need - records,
// primitives, unions, arrays, maps, enums, fixed, and the in-scope logical
// types (timestamp-millis/micros, date, time-millis/micros, decimal).
// Anything the model cannot honour - malformed avsc, unresolvable references,
// unknown or mis-typed logical types - stops Parse with a *ParseError instead
// of producing a model that would generate non-conforming data.
package avro

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	gogen "github.com/actgardner/gogen-avro/v10/schema"
)

// TypeKind enumerates the kinds a model node can hold.
type TypeKind string

const (
	KindNull    TypeKind = "null"
	KindBoolean TypeKind = "boolean"
	KindInt     TypeKind = "int"
	KindLong    TypeKind = "long"
	KindFloat   TypeKind = "float"
	KindDouble  TypeKind = "double"
	KindBytes   TypeKind = "bytes"
	KindString  TypeKind = "string"
	KindRecord  TypeKind = "record"
	KindEnum    TypeKind = "enum"
	KindFixed   TypeKind = "fixed"
	KindUnion   TypeKind = "union"
	KindArray   TypeKind = "array"
	KindMap     TypeKind = "map"
)

// PrimitiveKind is the subset of TypeKind a Primitive node can carry.
type PrimitiveKind = TypeKind

// LogicalTypeKind enumerates the Avro logical types in the documented scope.
type LogicalTypeKind string

const (
	LogicalTimestampMillis LogicalTypeKind = "timestamp-millis"
	LogicalTimestampMicros LogicalTypeKind = "timestamp-micros"
	LogicalDate            LogicalTypeKind = "date"
	LogicalTimeMillis      LogicalTypeKind = "time-millis"
	LogicalTimeMicros      LogicalTypeKind = "time-micros"
	LogicalDecimal         LogicalTypeKind = "decimal"
)

// logicalBaseOK maps each supported logical kind to the base type Avro
// requires for it (decimal is validated separately because bytes and fixed are
// both legal). Unknown kinds are rejected at parse time.
var logicalBaseOK = map[LogicalTypeKind]TypeKind{
	LogicalTimestampMillis: KindLong,
	LogicalTimestampMicros: KindLong,
	LogicalDate:            KindInt,
	LogicalTimeMillis:      KindInt,
	LogicalTimeMicros:      KindLong,
}

// LogicalType is the semantic overlay an avsc applies to a base type: the
// temporal kinds reinterpret an int/long, and decimal reinterprets bytes or
// fixed. Recorded here so the AVRO generator can synthesise conforming values
// (date as an int day anchor, timestamp-millis as a millisecond instant,
// decimal as scale-aware bytes).
type LogicalType struct {
	// Kind is the logical type name, e.g. "timestamp-millis".
	Kind LogicalTypeKind
	// Precision is the number of digits of precision; decimal only, and the
	// Avro spec requires it when the logical type is decimal.
	Precision int
	// Scale is the number of fractional digits; decimal only, defaulting to 0
	// when unspecified.
	Scale int
}

// Type is a node in the Avro schema model. The concrete kinds below implement
// it; the generator type-switches over them to produce values. The parameter
// space is sealed so the model stays a closed union.
type Type interface {
	isType()
}

// Schema is a parsed avsc: the Root type plus the raw bytes it was built from.
// The raw avsc is retained so the AVRO path can register or compare the exact
// schema the serializer will encode against (ADR-0007 decision 5).
type Schema struct {
	// Root is the top-level Avro type of the schema.
	Root Type

	raw []byte
}

// Raw returns the avsc bytes this model was parsed from, with leading and
// trailing whitespace stripped.
func (s *Schema) Raw() []byte {
	return s.raw
}

// ParseError reports an avsc that cannot be parsed into the Avro model. It
// names the problem (missing key, unresolvable reference, unsupported logical
// type, ...) so a malformed schema never silently becomes a model that would
// generate non-conforming data (ADR-0007 decision 4).
type ParseError struct {
	// Detail is a human-readable explanation of the offending construct.
	Detail string
	// Err is the underlying library parse error, when one exists.
	Err error
}

func (e *ParseError) Error() string {
	msg := "avro: invalid avsc"
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// Unwrap exposes the underlying library error for errors.Is/As.
func (e *ParseError) Unwrap() error {
	return e.Err
}

// Primitive is one of the eight Avro primitive types, with an optional logical
// overlay (e.g. int + date).
type Primitive struct {
	Kind    PrimitiveKind
	Logical *LogicalType
}

func (*Primitive) isType() {}

// Field is a record field: its name, the type it carries, and the Avro
// default (present for any field declaring one, including an explicit null).
type Field struct {
	Name       string
	Type       Type
	HasDefault bool
	Default    any
}

// Record is a named Avro record: an ordered list of fields.
type Record struct {
	Name      string
	Namespace string
	Fields    []*Field
}

func (*Record) isType() {}

// FullName returns the namespace-qualified record name.
func (r *Record) FullName() string {
	return fullName(r.Namespace, r.Name)
}

// Union is an ordered list of branch types. Avro requires at most one null
// branch; NullIndex locates it for the null/optional idiom.
type Union struct {
	Branches []Type
}

func (*Union) isType() {}

// NullIndex returns the index of the null branch, or -1 when the union allows
// no null.
func (u *Union) NullIndex() int {
	for i, b := range u.Branches {
		if p, ok := b.(*Primitive); ok && p.Kind == KindNull {
			return i
		}
	}
	return -1
}

// Array is a homogeneous Avro array; Items is the element type.
type Array struct {
	Items Type
}

func (*Array) isType() {}

// Map is an Avro map from string keys to Values.
type Map struct {
	Values Type
}

func (*Map) isType() {}

// Enum is a named Avro enum; Symbols are the allowed values in declaration
// order, Default names the symbol selected when no value is produced (nil when
// the avsc declares none).
type Enum struct {
	Name      string
	Namespace string
	Symbols   []string
	Default   *string
}

func (*Enum) isType() {}

// FullName returns the namespace-qualified enum name.
func (e *Enum) FullName() string {
	return fullName(e.Namespace, e.Name)
}

// Fixed is a named Avro fixed of Size bytes, with an optional logical overlay
// (decimal reinterprets the fixed bytes as an unscaled integer).
type Fixed struct {
	Name      string
	Namespace string
	Size      int
	Logical   *LogicalType
}

func (*Fixed) isType() {}

// FullName returns the namespace-qualified fixed name.
func (f *Fixed) FullName() string {
	return fullName(f.Namespace, f.Name)
}

// Parse parses avsc bytes into the Avro model, or returns a *ParseError naming
// the offending construct. It drives gogen-avro's parser through exactly the
// sequence the Confluent Go Avro serde runs, so the avsc the serializer
// accepts parses identically here.
func Parse(avsc []byte) (s *Schema, err error) {
	// The library parser operates on untrusted input; a panic must surface as
	// a typed error rather than crashing the run.
	defer func() {
		if r := recover(); r != nil {
			s, err = nil, &ParseError{Detail: fmt.Sprintf("schema parser panicked: %v", r)}
		}
	}()

	avsc = bytes.TrimSpace(avsc)
	if len(avsc) == 0 {
		return nil, &ParseError{Detail: "avsc is empty"}
	}

	ns := parser.NewNamespace(false)
	root, pErr := ns.TypeForSchema(avsc)
	if pErr != nil {
		return nil, parseErr(pErr)
	}
	for _, def := range ns.Roots {
		if rErr := resolver.ResolveDefinition(def, ns.Definitions); rErr != nil {
			return nil, parseErr(rErr)
		}
	}

	model, mErr := buildModel(root, ns.Definitions)
	if mErr != nil {
		return nil, mErr
	}
	return &Schema{Root: model, raw: avsc}, nil
}

// parseErr wraps a library parse error as a typed *ParseError.
func parseErr(err error) error {
	if err == nil {
		return nil
	}
	return &ParseError{Detail: err.Error(), Err: err}
}

// buildModel turns gogen-avro's resolved schema tree into the model. Named
// types (record, enum, fixed) become shared nodes keyed by their fullname; the
// two-phase build keeps recursive records acyclic in memory while letting a
// reference to a named type yield the same node everywhere it appears.
func buildModel(root gogen.AvroType, defs map[gogen.QualifiedName]gogen.Definition) (Type, error) {
	// Deterministic iteration over the definition map (fullnames sorted) so
	// repeated parses of the same avsc always build byte-identical models.
	names := make([]gogen.QualifiedName, 0, len(defs))
	for qn := range defs {
		names = append(names, qn)
	}
	sort.Slice(names, func(i, j int) bool { return names[i].String() < names[j].String() })

	named := make(map[gogen.QualifiedName]Type, len(defs))
	for _, qn := range names {
		switch d := defs[qn].(type) {
		case *gogen.RecordDefinition:
			named[qn] = &Record{Name: qn.Name, Namespace: qn.Namespace}
		case *gogen.EnumDefinition:
			named[qn] = &Enum{Name: qn.Name, Namespace: qn.Namespace, Symbols: d.Symbols(), Default: enumDefault(d)}
		case *gogen.FixedDefinition:
			named[qn] = &Fixed{Name: qn.Name, Namespace: qn.Namespace, Size: d.SizeBytes()}
		}
	}

	for _, qn := range names {
		switch d := defs[qn].(type) {
		case *gogen.RecordDefinition:
			rec := named[qn].(*Record)
			for _, f := range d.Fields() {
				ft, err := buildType(f.Type(), named)
				if err != nil {
					return nil, err
				}
				rec.Fields = append(rec.Fields, &Field{
					Name:       f.Name(),
					Type:       ft,
					HasDefault: f.HasDefault(),
					Default:    f.Default(),
				})
			}
		case *gogen.FixedDefinition:
			fixed := named[qn].(*Fixed)
			lt, err := parseLogical(d.Attribute("logicalType"), KindFixed, d.Attribute("precision"), d.Attribute("scale"))
			if err != nil {
				return nil, err
			}
			fixed.Logical = lt
		}
	}

	return buildType(root, named)
}

// buildType converts one non-named schema node (or a reference to a named one)
// into a model Type.
func buildType(t gogen.AvroType, named map[gogen.QualifiedName]Type) (Type, error) {
	switch tt := t.(type) {
	case *gogen.Reference:
		node, ok := named[tt.TypeName]
		if !ok {
			return nil, &ParseError{Detail: fmt.Sprintf("reference to %s is not defined in this avsc", tt.TypeName)}
		}
		return node, nil
	case *gogen.UnionField:
		branches := make([]Type, 0, len(tt.AvroTypes()))
		for _, b := range tt.AvroTypes() {
			bt, err := buildType(b, named)
			if err != nil {
				return nil, err
			}
			branches = append(branches, bt)
		}
		return &Union{Branches: branches}, nil
	case *gogen.ArrayField:
		items, err := buildType(tt.ItemType(), named)
		if err != nil {
			return nil, err
		}
		return &Array{Items: items}, nil
	case *gogen.MapField:
		values, err := buildType(tt.ItemType(), named)
		if err != nil {
			return nil, err
		}
		return &Map{Values: values}, nil
	default:
		return buildPrimitive(tt)
	}
}

// buildPrimitive converts a primitive node, reading any logical-type overlay
// off its declaring attributes.
func buildPrimitive(t gogen.AvroType) (Type, error) {
	var kind PrimitiveKind
	var pr *gogen.PrimitiveField
	switch tt := t.(type) {
	case *gogen.NullField:
		kind, pr = KindNull, &tt.PrimitiveField
	case *gogen.BoolField:
		kind, pr = KindBoolean, &tt.PrimitiveField
	case *gogen.IntField:
		kind, pr = KindInt, &tt.PrimitiveField
	case *gogen.LongField:
		kind, pr = KindLong, &tt.PrimitiveField
	case *gogen.FloatField:
		kind, pr = KindFloat, &tt.PrimitiveField
	case *gogen.DoubleField:
		kind, pr = KindDouble, &tt.PrimitiveField
	case *gogen.BytesField:
		kind, pr = KindBytes, &tt.PrimitiveField
	case *gogen.StringField:
		kind, pr = KindString, &tt.PrimitiveField
	default:
		return nil, &ParseError{Detail: fmt.Sprintf("unexpected schema node of type %T", t)}
	}
	lt, err := parseLogical(pr.Attribute("logicalType"), kind, pr.Attribute("precision"), pr.Attribute("scale"))
	if err != nil {
		return nil, err
	}
	return &Primitive{Kind: kind, Logical: lt}, nil
}

// enumDefault returns the declared default symbol, or nil when the avsc names
// none (an empty default symbol and an absent one are distinguishable here).
func enumDefault(e *gogen.EnumDefinition) *string {
	if v, ok := e.Attribute("default").(string); ok {
		return &v
	}
	return nil
}

// parseLogical validates a declared logicalType against its base kind and
// returns the overlay, or a typed error when the combination cannot feed the
// generator (ADR-0007 decision 4: unknown or mis-typed overlays stop the run).
// A nil attr means no logical type is declared.
func parseLogical(attr any, base TypeKind, precision, scale any) (*LogicalType, error) {
	name, ok := attr.(string)
	if !ok || name == "" {
		return nil, nil
	}
	kind := LogicalTypeKind(name)

	if kind == LogicalDecimal {
		if base != KindBytes && base != KindFixed {
			return nil, &ParseError{Detail: fmt.Sprintf("logicalType %q requires a bytes or fixed base type, got %s", name, base)}
		}
		p, err := decimalPrecision(precision)
		if err != nil {
			return nil, err
		}
		return &LogicalType{Kind: kind, Precision: p, Scale: decimalScale(scale)}, nil
	}

	want, known := logicalBaseOK[kind]
	if !known {
		return nil, &ParseError{Detail: fmt.Sprintf("unsupported logicalType %q (supported: timestamp-millis, timestamp-micros, date, time-millis, time-micros, decimal)", name)}
	}
	if base != want {
		return nil, &ParseError{Detail: fmt.Sprintf("logicalType %q requires base type %s, got %s", name, want, base)}
	}
	return &LogicalType{Kind: kind}, nil
}

// decimalPrecision requires the precision attribute the Avro spec makes
// mandatory for decimal.
func decimalPrecision(v any) (int, error) {
	f, ok := v.(float64)
	if !ok {
		return 0, &ParseError{Detail: "logicalType \"decimal\" requires an integer precision attribute"}
	}
	return int(f), nil
}

// decimalScale defaults to 0 when the scale attribute is absent.
func decimalScale(v any) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func fullName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}
