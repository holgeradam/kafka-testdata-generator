// Package avro parses Apache Avro schemas (avsc) into a generation model the
// AVRO wire format drives values from (ADR-0007 decision 3: generation follows
// the avsc). The avsc is parsed once, by confluent-avro-go - the codec the
// AvroEncoder marshals with - and the model is built from that parse, which it
// carries for the encoder (#65).
//
// The model covers the Avro constructs the generator will need - records,
// primitives, unions, arrays, maps, enums, fixed, and the in-scope logical
// types (timestamp-millis/micros, date, time-millis/micros, decimal).
// Anything the model cannot honour - malformed avsc, unresolvable references,
// mis-typed logical types - stops Parse with a *ParseError instead of producing
// a model that would generate non-conforming data.
package avro

import (
	"bytes"
	"fmt"

	codec "github.com/confluentinc/confluent-avro-go/v2"
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

// Schema is a parsed avsc: the Root type, the raw bytes it was built from, and
// the codec's parse of them. The raw avsc is what the AVRO path registers
// (ADR-0007 decision 3: the exact avsc); the codec schema is what it encodes
// against, so nothing parses the avsc a second time.
type Schema struct {
	// Root is the top-level Avro type of the schema.
	Root Type

	raw   []byte
	codec codec.Schema
}

// Codec returns the codec's parsed schema the model was built from, for
// marshalling values against.
func (s *Schema) Codec() codec.Schema {
	return s.codec
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
// the offending construct. The avsc is parsed once, by the codec the AvroEncoder
// marshals with (#65), and the model is built from that parse, so what the
// generator honours and what the encoder accepts can never disagree.
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

	// A fresh cache per parse: the codec's default cache is process-global, so
	// named types from one avsc would resolve bare references in the next.
	parsed, pErr := codec.ParseBytesWithCache(avsc, "", &codec.SchemaCache{})
	if pErr != nil {
		return nil, &ParseError{Detail: pErr.Error(), Err: pErr}
	}

	b := &builder{named: map[string]Type{}}
	root, mErr := b.build(parsed)
	if mErr != nil {
		return nil, mErr
	}
	return &Schema{Root: root, raw: avsc, codec: parsed}, nil
}

// builder turns the codec's parsed schema into the model. Named types (record,
// enum, fixed) become one shared node per fullname, registered before their
// fields are built so a recursive record refers back to itself.
type builder struct {
	named map[string]Type
}

func (b *builder) build(s codec.Schema) (Type, error) {
	switch t := s.(type) {
	case *codec.RefSchema:
		return b.build(t.Schema())
	case *codec.RecordSchema:
		if n, ok := b.named[t.FullName()]; ok {
			return n, nil
		}
		rec := &Record{Name: t.Name(), Namespace: t.Namespace()}
		b.named[t.FullName()] = rec
		for _, f := range t.Fields() {
			ft, err := b.build(f.Type())
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
		return rec, nil
	case *codec.EnumSchema:
		if n, ok := b.named[t.FullName()]; ok {
			return n, nil
		}
		enum := &Enum{Name: t.Name(), Namespace: t.Namespace(), Symbols: t.Symbols()}
		if t.HasDefault() {
			d := t.Default()
			enum.Default = &d
		}
		b.named[t.FullName()] = enum
		return enum, nil
	case *codec.FixedSchema:
		if n, ok := b.named[t.FullName()]; ok {
			return n, nil
		}
		lt, err := logicalOf(t.Logical(), t.Prop("logicalType"), KindFixed)
		if err != nil {
			return nil, err
		}
		fixed := &Fixed{Name: t.Name(), Namespace: t.Namespace(), Size: t.Size(), Logical: lt}
		b.named[t.FullName()] = fixed
		return fixed, nil
	case *codec.UnionSchema:
		branches := make([]Type, 0, len(t.Types()))
		for _, br := range t.Types() {
			bt, err := b.build(br)
			if err != nil {
				return nil, err
			}
			branches = append(branches, bt)
		}
		return &Union{Branches: branches}, nil
	case *codec.ArraySchema:
		items, err := b.build(t.Items())
		if err != nil {
			return nil, err
		}
		return &Array{Items: items}, nil
	case *codec.MapSchema:
		values, err := b.build(t.Values())
		if err != nil {
			return nil, err
		}
		return &Map{Values: values}, nil
	case *codec.NullSchema:
		return &Primitive{Kind: KindNull}, nil
	case *codec.PrimitiveSchema:
		kind := TypeKind(t.Type())
		lt, err := logicalOf(t.Logical(), t.Prop("logicalType"), kind)
		if err != nil {
			return nil, err
		}
		return &Primitive{Kind: kind, Logical: lt}, nil
	default:
		return nil, &ParseError{Detail: fmt.Sprintf("unexpected schema node of type %T", s)}
	}
}

// logicalOf returns the model's overlay for a node. honoured is the logical
// type the codec recognised; declared is the raw logicalType attribute, which
// the codec leaves in place when it does not honour it. A logical type the
// model does not know is ignored and the base type governs, as the Avro spec
// requires of readers - the codec treats it as its base type too. A known
// logical type the codec did not honour (wrong base type, a decimal whose
// precision or scale is invalid or does not fit its fixed size) is a malformed
// avsc: keeping the overlay would generate values the encoder rejects, so Parse
// stops with a typed error (ADR-0007 decision 4).
func logicalOf(honoured codec.LogicalSchema, declared any, base TypeKind) (*LogicalType, error) {
	var name LogicalTypeKind
	if honoured != nil {
		name = LogicalTypeKind(honoured.Type())
	} else if d, ok := declared.(string); ok {
		name = LogicalTypeKind(d)
	}
	l := lookupLogical(name)
	switch {
	case l == nil:
		return nil, nil
	case !l.allows(base):
		return nil, &ParseError{Detail: fmt.Sprintf("logicalType %q requires base type %s, got %s", name, l.basesString(), base)}
	case honoured == nil && l.constraint != nil:
		return nil, &ParseError{Detail: fmt.Sprintf("logicalType %q requires %s", name, l.constraint(base))}
	case honoured == nil:
		return nil, &ParseError{Detail: fmt.Sprintf("logicalType %q is not honoured on base type %s", name, base)}
	}
	lt := &LogicalType{Kind: name}
	if d, ok := honoured.(*codec.DecimalLogicalSchema); ok {
		lt.Precision, lt.Scale = d.Precision(), d.Scale()
	}
	return lt, nil
}

func fullName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}
