package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"
)

// RenderError reports a generated value the renderer cannot display in the
// Avro JSON encoding - a shape mismatch between the model and the value. The
// generator honours the model, so this is defensive: a value that cannot be
// rendered stops the Dry-run display with a typed error rather than emitting
// garbage.
type RenderError struct {
	// Detail is a human-readable explanation of the offending construct.
	Detail string
}

func (e *RenderError) Error() string {
	return "avro: cannot render datum: " + e.Detail
}

// RenderJSON returns the canonical Avro JSON encoding (the spec-defined text
// form of a datum) for a generated value honouring model type t, the readable
// display the AVRO Dry run prints. It walks the model and the value together so
// each construct maps to its readable JSON form: bytes/ fixed as Latin-1
// strings, enums as strings, unions by their active branch (Avro JSON encodes
// a union datum directly, not wrapped), and logical types to their
// human-readable text (ADR-0007 decision 7 renders Dry run from the local
// avsc, never a registry).
func RenderJSON(t Type, v any) ([]byte, error) {
	jv, err := renderDatum(t, v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jv)
}

// renderDatum converts (model node, generated value) into a JSON-marshalable
// value in the Avro JSON encoding.
func renderDatum(t Type, v any) (any, error) {
	switch s := t.(type) {
	case *Primitive:
		return renderPrimitive(s, v)
	case *Record:
		return renderRecord(s, v)
	case *Union:
		return renderUnion(s, v)
	case *Array:
		return renderArray(s, v)
	case *Map:
		return renderMap(s, v)
	case *Enum:
		str, ok := v.(string)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("enum %s value is %T, want string", s.FullName(), v)}
		}
		return str, nil
	case *Fixed:
		return renderFixed(s, v)
	default:
		return nil, &RenderError{Detail: fmt.Sprintf("unsupported model node %T", t)}
	}
}

func renderPrimitive(p *Primitive, v any) (any, error) {
	if p.Logical != nil {
		return renderLogical(p.Logical, v)
	}
	switch p.Kind {
	case KindNull:
		return nil, nil
	case KindBoolean:
		if _, ok := v.(bool); !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("boolean value is %T, want bool", v)}
		}
		return v, nil
	case KindInt:
		if _, ok := v.(int32); !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("int value is %T, want int32", v)}
		}
		return v, nil
	case KindLong:
		if _, ok := v.(int64); !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("long value is %T, want int64", v)}
		}
		return v, nil
	case KindFloat:
		f, ok := v.(float32)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("float value is %T, want float32", v)}
		}
		return jsonNumber(float64(f)), nil
	case KindDouble:
		f, ok := v.(float64)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("double value is %T, want float64", v)}
		}
		return jsonNumber(f), nil
	case KindBytes:
		b, ok := v.([]byte)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("bytes value is %T, want []byte", v)}
		}
		return latin1String(b), nil
	case KindString:
		str, ok := v.(string)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("string value is %T, want string", v)}
		}
		return str, nil
	default:
		return nil, &RenderError{Detail: fmt.Sprintf("unsupported primitive kind %q", p.Kind)}
	}
}

// jsonNumber maps a float to its Avro JSON form: an ordinary JSON number, or
// (for the non-finite IEEE values JSON cannot express) the spec's string
// spellings "NaN", "Infinity", and "-Infinity".
func jsonNumber(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	default:
		return f
	}
}

// renderLogical converts a logical-type value to its human-readable Avro JSON
// text form: dates as calendar days, timestamps as RFC 3339 instants,
// time-of-day as clock strings, and decimals as base-10 strings.
func renderLogical(lt *LogicalType, v any) (any, error) {
	switch lt.Kind {
	case LogicalDate:
		ts, ok := v.(time.Time)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("date value is %T, want time.Time", v)}
		}
		return ts.Format("2006-01-02"), nil
	case LogicalTimestampMillis, LogicalTimestampMicros:
		ts, ok := v.(time.Time)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("%s value is %T, want time.Time", lt.Kind, v)}
		}
		return ts, nil
	case LogicalTimeMillis, LogicalTimeMicros:
		d, ok := v.(time.Duration)
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("%s value is %T, want time.Duration", lt.Kind, v)}
		}
		return clockString(d, lt.Kind), nil
	case LogicalDecimal:
		return decimalString(lt.Scale, v)
	default:
		return nil, &RenderError{Detail: fmt.Sprintf("unsupported logical type %q", lt.Kind)}
	}
}

// decimalString renders a decimal value as the spec's base-10 text form, e.g.
// "123.45" for scale 2. Shared by the bytes/fixed decimal logical types.
func decimalString(scale int, v any) (any, error) {
	r, ok := v.(*big.Rat)
	if !ok {
		return nil, &RenderError{Detail: fmt.Sprintf("decimal value is %T, want *big.Rat", v)}
	}
	return r.FloatString(scale), nil
}

// clockString renders a time-of-day duration as the Avro spec's readable text
// form: HH:MM:SS.mmm for time-millis, HH:MM:SS.uuuuuu for time-micros.
func clockString(d time.Duration, kind LogicalTypeKind) string {
	hours := int64(d / time.Hour)
	minutes := int64(d%time.Hour) / int64(time.Minute)
	seconds := int64(d%time.Minute) / int64(time.Second)
	if kind == LogicalTimeMillis {
		ms := int64(d%time.Second) / int64(time.Millisecond)
		return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, ms)
	}
	us := int64(d%time.Second) / int64(time.Microsecond)
	return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, us)
}

func renderRecord(rec *Record, v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, &RenderError{Detail: fmt.Sprintf("record %s value is %T, want map[string]any", rec.FullName(), v)}
	}
	out := make(map[string]any, len(rec.Fields))
	for _, f := range rec.Fields {
		fv, ok := m[f.Name]
		if !ok {
			return nil, &RenderError{Detail: fmt.Sprintf("record %s lacks field %q", rec.FullName(), f.Name)}
		}
		rv, err := renderDatum(f.Type, fv)
		if err != nil {
			return nil, err
		}
		out[f.Name] = rv
	}
	return out, nil
}

func renderUnion(u *Union, v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, &RenderError{Detail: fmt.Sprintf("union value is %T, want map[string]any (branch wrapper)", v)}
	}
	for branchName, bv := range m {
		for _, b := range u.Branches {
			if unionBranchName(b) == branchName {
				// Avro JSON encodes the active union branch directly, not as
				// the generator's single-entry wrapper map.
				return renderDatum(b, bv)
			}
		}
		return nil, &RenderError{Detail: fmt.Sprintf("union branch %q is not in the schema", branchName)}
	}
	return nil, &RenderError{Detail: "union value carries no active branch (empty wrapper map)"}
}

func renderArray(arr *Array, v any) (any, error) {
	items, ok := v.([]any)
	if !ok {
		return nil, &RenderError{Detail: fmt.Sprintf("array value is %T, want []any", v)}
	}
	out := make([]any, len(items))
	for i, item := range items {
		iv, err := renderDatum(arr.Items, item)
		if err != nil {
			return nil, err
		}
		out[i] = iv
	}
	return out, nil
}

func renderMap(m *Map, v any) (any, error) {
	kv, ok := v.(map[string]any)
	if !ok {
		return nil, &RenderError{Detail: fmt.Sprintf("map value is %T, want map[string]any", v)}
	}
	out := make(map[string]any, len(kv))
	for k, val := range kv {
		vv, err := renderDatum(m.Values, val)
		if err != nil {
			return nil, err
		}
		out[k] = vv
	}
	return out, nil
}

func renderFixed(f *Fixed, v any) (any, error) {
	if f.Logical != nil && f.Logical.Kind == LogicalDecimal {
		return decimalString(f.Logical.Scale, v)
	}
	b, err := fixedBytes(v)
	if err != nil {
		return nil, err
	}
	return latin1String(b), nil
}

// fixedBytes coerces a fixed value (produced by the generator as a [N]byte
// array via reflect) into a []byte for Latin-1 rendering.
func fixedBytes(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Array || rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil, &RenderError{Detail: fmt.Sprintf("fixed value is %T, want [N]byte", v)}
	}
	out := make([]byte, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface().(byte)
	}
	return out, nil
}

// latin1String converts raw bytes into the Latin-1 string the Avro JSON
// encoding specifies for bytes and fixed values: each byte is one character
// (json.Marshal then escapes non-printable ones as \u00XX).
func latin1String(b []byte) string {
	rs := make([]rune, len(b))
	for i, by := range b {
		rs[i] = rune(by)
	}
	return string(rs)
}
