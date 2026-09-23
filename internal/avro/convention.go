package avro

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// The AVRO value convention: the Go value each Avro construct is represented
// by between the Generator, RenderJSON and the codec's generic marshaller.
// It is the codec's own native convention, so generated values encode as they
// are: int32 for int, int64 for long, float32 for float, float64 for double,
// []byte for bytes, [N]byte for fixed, string for string and enum symbols,
// map[string]any for records and maps, []any for arrays, and a single-entry
// map keyed by unionBranchName for a union. Logical types are one entry each
// in logicalTypes below. TestConventionProperty holds all three sides to it.

// LogicalTypeKind names an Avro logical type.
type LogicalTypeKind string

// logicalType is one entry of the convention: which base types a logical
// type overlays, what value the Synthesizer produces for it, and how that
// value renders in the Avro JSON encoding. Parse, the Generator and RenderJSON
// all look logical types up here, so adding one is adding an entry.
type logicalType struct {
	kind  LogicalTypeKind
	bases []TypeKind
	// constraint names what the overlay needs beyond its base type, for the
	// error when the codec refuses it; nil when the base type alone decides.
	constraint func(base TypeKind) string
	synth      func(s *synth.Synthesizer, lt *LogicalType) (any, error)
	render     func(lt *LogicalType, v any) (any, error)
}

// allows reports whether the logical type may overlay base.
func (l *logicalType) allows(base TypeKind) bool {
	for _, b := range l.bases {
		if b == base {
			return true
		}
	}
	return false
}

// logicalTypes is the convention for every logical type the model honours. A
// logical type outside it is ignored and its base type governs, as the Avro
// spec requires of readers.
var logicalTypes = []*logicalType{
	{
		kind:  "date",
		bases: []TypeKind{KindInt},
		synth: func(s *synth.Synthesizer, _ *LogicalType) (any, error) {
			return s.Instant().UTC().Truncate(24 * time.Hour), nil
		},
		render: func(lt *LogicalType, v any) (any, error) {
			ts, err := as[time.Time](lt, v)
			if err != nil {
				return nil, err
			}
			return ts.Format("2006-01-02"), nil
		},
	},
	{
		kind:  "time-millis",
		bases: []TypeKind{KindInt},
		synth: func(s *synth.Synthesizer, _ *LogicalType) (any, error) {
			return time.Duration(s.Int(0, 86400*1000-1)) * time.Millisecond, nil
		},
		render: func(lt *LogicalType, v any) (any, error) {
			d, err := as[time.Duration](lt, v)
			if err != nil {
				return nil, err
			}
			return clockString(d, 3), nil
		},
	},
	{
		kind:  "time-micros",
		bases: []TypeKind{KindLong},
		synth: func(s *synth.Synthesizer, _ *LogicalType) (any, error) {
			return time.Duration(s.Int(0, 86400*1000_000-1)) * time.Microsecond, nil
		},
		render: func(lt *LogicalType, v any) (any, error) {
			d, err := as[time.Duration](lt, v)
			if err != nil {
				return nil, err
			}
			return clockString(d, 6), nil
		},
	},
	instant("timestamp-millis", time.Millisecond),
	instant("timestamp-micros", time.Microsecond),
	instant("local-timestamp-millis", time.Millisecond),
	instant("local-timestamp-micros", time.Microsecond),
	{
		kind:  "uuid",
		bases: []TypeKind{KindString},
		synth: func(s *synth.Synthesizer, _ *LogicalType) (any, error) {
			return s.Semantic(synth.UUID), nil
		},
		render: func(lt *LogicalType, v any) (any, error) {
			return as[string](lt, v)
		},
	},
	{
		kind:  "decimal",
		bases: []TypeKind{KindBytes, KindFixed},
		constraint: func(base TypeKind) string {
			return fmt.Sprintf("a precision above 0 that fits the %s, and a scale from 0 to the precision", base)
		},
		synth: decimalValue,
		render: func(lt *LogicalType, v any) (any, error) {
			r, err := as[*big.Rat](lt, v)
			if err != nil {
				return nil, err
			}
			return r.FloatString(lt.Scale), nil
		},
	},
}

// lookupLogical returns the convention entry for kind, or nil when the model
// does not honour it.
func lookupLogical(kind LogicalTypeKind) *logicalType {
	for _, l := range logicalTypes {
		if l.kind == kind {
			return l
		}
	}
	return nil
}

// instant is the convention shared by the timestamp logical types, UTC and
// local alike: a time.Time truncated to the unit, rendered as an RFC 3339
// instant.
func instant(kind LogicalTypeKind, unit time.Duration) *logicalType {
	return &logicalType{
		kind:  kind,
		bases: []TypeKind{KindLong},
		synth: func(s *synth.Synthesizer, _ *LogicalType) (any, error) {
			return s.Instant().UTC().Truncate(unit), nil
		},
		render: func(lt *LogicalType, v any) (any, error) {
			return as[time.Time](lt, v)
		},
	}
}

// as asserts a generated value has the Go type the convention names for lt,
// or reports the mismatch as a *RenderError.
func as[T any](lt *LogicalType, v any) (T, error) {
	t, ok := v.(T)
	if !ok {
		return t, &RenderError{Detail: fmt.Sprintf("%s value is %T, want %T", lt.Kind, v, t)}
	}
	return t, nil
}

// decimalValue synthesizes a *big.Rat honouring precision/scale: a mantissa of
// at most Precision digits scaled by 10^-Scale, so the marshaller's precision
// check is always satisfied.
func decimalValue(s *synth.Synthesizer, lt *LogicalType) (any, error) {
	digits := lt.Precision
	if digits > 15 {
		digits = 15
	}
	if digits < 1 {
		return nil, &GenerateError{Detail: fmt.Sprintf("decimal precision %d is out of range", lt.Precision)}
	}
	b := make([]byte, digits)
	b[0] = byte('1' + s.Pick(9))
	for i := 1; i < digits; i++ {
		b[i] = byte('0' + s.Pick(10))
	}
	mantissa, ok := new(big.Int).SetString(string(b), 10)
	if !ok {
		return nil, &GenerateError{Detail: fmt.Sprintf("generating decimal mantissa for precision %d", lt.Precision)}
	}
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(lt.Scale)), nil)
	return new(big.Rat).SetFrac(mantissa, scaleFactor), nil
}

// clockString renders a time-of-day duration as the Avro spec's readable text
// form, HH:MM:SS with the given number of fractional digits: 3 for
// time-millis, 6 for time-micros.
func clockString(d time.Duration, digits int) string {
	hours := int64(d / time.Hour)
	minutes := int64(d%time.Hour) / int64(time.Minute)
	seconds := int64(d%time.Minute) / int64(time.Second)
	if digits == 3 {
		ms := int64(d%time.Second) / int64(time.Millisecond)
		return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, ms)
	}
	us := int64(d%time.Second) / int64(time.Microsecond)
	return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, us)
}

// basesString names the base types a logical type allows, for errors.
func (l *logicalType) basesString() string {
	names := make([]string, len(l.bases))
	for i, b := range l.bases {
		names[i] = string(b)
	}
	return strings.Join(names, " or ")
}

// unionBranchName is the convention's key for a union value: the single-entry
// map the Generator produces is keyed by the name confluent-avro-go's generic
// union encoder resolves a branch by - the logical-qualified primitive name,
// the fullname of a named type, or "array"/"map". RenderJSON reads the same
// key to find the active branch.
func unionBranchName(t Type) string {
	switch x := t.(type) {
	case *Primitive:
		if x.Logical != nil {
			return string(x.Kind) + "." + string(x.Logical.Kind)
		}
		return string(x.Kind)
	case *Record:
		return x.FullName()
	case *Enum:
		return x.FullName()
	case *Fixed:
		return x.FullName()
	case *Array:
		return "array"
	case *Map:
		return "map"
	default:
		return fmt.Sprintf("%T", t)
	}
}
